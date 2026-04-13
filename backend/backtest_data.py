"""
NSE EDGE v5 — Backtest Data Layer
Downloads and stores 3-year historical data:
  - NIFTY OHLCV + VIX daily  (Kite historical_data API)
  - Option chain PCR daily    (NSE FO bhavcopy CSV)
  - Live signal log           (appended each run by signals.py)
"""

import csv
import io
import logging
import os
import sqlite3
import time
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
import pytz

logger = logging.getLogger("backtest_data")
IST = pytz.timezone("Asia/Kolkata")

DB_PATH = Path(__file__).parent / "data" / "backtest.db"


# ─── DB INIT ──────────────────────────────────────────────────────────────────
def init_db():
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            date TEXT PRIMARY KEY,
            open REAL, high REAL, low REAL, close REAL, volume INTEGER
        );
        CREATE TABLE IF NOT EXISTS vix_daily (
            date TEXT PRIMARY KEY,
            vix REAL, vix_chg REAL
        );
        CREATE TABLE IF NOT EXISTS chain_daily (
            date TEXT PRIMARY KEY,
            pcr REAL, total_call_oi INTEGER, total_put_oi INTEGER,
            max_pain_proxy INTEGER, ul_price REAL
        );
        CREATE TABLE IF NOT EXISTS fii_daily (
            date TEXT PRIMARY KEY,
            fii_net REAL, dii_net REAL
        );
        CREATE TABLE IF NOT EXISTS signal_log (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            date  TEXT, session TEXT,
            g1 TEXT, g2 TEXT, g3 TEXT, g4 TEXT, g5 TEXT,
            g1_score INTEGER, g2_score INTEGER, g3_score INTEGER,
            g4_score INTEGER, g5_score INTEGER,
            verdict TEXT, pass_count INTEGER,
            nifty REAL, vix REAL, pcr REAL, fii_net REAL,
            nifty_next REAL, outcome_pts REAL, outcome TEXT,
            ts REAL
        );
        CREATE TABLE IF NOT EXISTS gate_weights (
            gate INTEGER PRIMARY KEY,
            name TEXT, weight REAL, win_rate REAL, sample_size INTEGER
        );
        CREATE TABLE IF NOT EXISTS live_signal_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_key TEXT UNIQUE,
            trade_date TEXT,
            symbol TEXT,
            signal_type TEXT,
            trigger TEXT,
            strength TEXT,
            signal_time TEXT,
            entry_price REAL,
            stop_loss REAL,
            target_price REAL,
            exit_price REAL,
            exit_time TEXT,
            status TEXT,
            outcome TEXT,
            pnl_pts REAL,
            pnl_pct REAL,
            hold_minutes INTEGER,
            gate_pass_count INTEGER,
            gate_snapshot TEXT,
            verdict TEXT,
            vix REAL,
            pcr REAL,
            created_ts REAL,
            updated_ts REAL
        );
        -- ADV-IDX-OPTIONS: isolated daily series (Kite NIFTY + VIX + computed IV/gamma context). Does not replace ohlcv/vix_daily.
        CREATE TABLE IF NOT EXISTS adv_idx_options_daily (
            date TEXT PRIMARY KEY,
            nifty_open REAL, nifty_high REAL, nifty_low REAL, nifty_close REAL, nifty_volume INTEGER,
            vix REAL NOT NULL DEFAULT 0,
            vix_chg REAL,
            iv_rank_proxy REAL,
            iv_zone TEXT,
            gamma_elevated INTEGER NOT NULL DEFAULT 0,
            weekday INTEGER,
            options_context_score REAL,
            source TEXT DEFAULT 'kite',
            updated_ts REAL
        );
    """)
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lsh_trade_date ON live_signal_history(trade_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lsh_verdict_date ON live_signal_history(verdict, trade_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_adv_idx_opt_date ON adv_idx_options_daily(date)"
        )
    except Exception:
        pass
    try:
        import playbook_design as _pbd

        _pbd.ensure_schema(conn)
    except Exception:
        pass
    conn.commit()
    conn.close()


def get_conn():
    return sqlite3.connect(str(DB_PATH), timeout=10)


# ─── KITE HISTORICAL OHLCV ────────────────────────────────────────────────────
def download_kite_history(kite, days: int = 1095):
    """Download NIFTY OHLCV and VIX daily data via Kite historical API."""
    from config import KITE_TOKENS
    to_dt   = datetime.now(IST).date()
    from_dt = to_dt - timedelta(days=days)

    conn = get_conn()

    # NIFTY OHLCV
    try:
        logger.info(f"Downloading NIFTY OHLCV {from_dt} → {to_dt} ...")
        data = kite.historical_data(KITE_TOKENS["NIFTY"], from_dt, to_dt, "day")
        rows = []
        for d in data:
            dt_str = d["date"].strftime("%Y-%m-%d") if hasattr(d["date"], "strftime") else str(d["date"])[:10]
            rows.append((dt_str, d["open"], d["high"], d["low"], d["close"], d.get("volume", 0)))
        conn.executemany("INSERT OR REPLACE INTO ohlcv VALUES (?,?,?,?,?,?)", rows)
        conn.commit()
        logger.info(f"  NIFTY OHLCV: {len(rows)} days stored")
    except Exception as e:
        logger.error(f"NIFTY OHLCV download failed: {e}")

    # VIX
    try:
        logger.info(f"Downloading VIX {from_dt} → {to_dt} ...")
        data   = kite.historical_data(KITE_TOKENS["INDIAVIX"], from_dt, to_dt, "day")
        sorted_data = sorted(data, key=lambda d: d["date"])
        rows   = []
        prev   = None
        for d in sorted_data:
            vix    = d["close"]
            chg    = round((vix - prev) / prev * 100, 2) if prev else 0.0
            dt_str = d["date"].strftime("%Y-%m-%d") if hasattr(d["date"], "strftime") else str(d["date"])[:10]
            rows.append((dt_str, vix, chg))
            prev = vix
        conn.executemany("INSERT OR REPLACE INTO vix_daily VALUES (?,?,?)", rows)
        conn.commit()
        logger.info(f"  VIX: {len(rows)} days stored")
    except Exception as e:
        logger.error(f"VIX download failed: {e}")

    conn.close()


# ─── NSE BHAVCOPY PCR ─────────────────────────────────────────────────────────
_NSE = requests.Session()
_NSE.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
    "Accept":     "*/*",
    "Referer":    "https://www.nseindia.com/",
})


def _nse_cookie():
    try:
        _NSE.get("https://www.nseindia.com", timeout=8)
    except Exception:
        pass


def download_chain_history(days: int = 1095):
    """Download NSE FO bhavcopy for each trading day → compute NIFTY PCR."""
    to_dt   = datetime.now(IST).date()
    from_dt = to_dt - timedelta(days=days)

    conn     = get_conn()
    existing = set(r[0] for r in conn.execute("SELECT date FROM chain_daily").fetchall())
    conn.close()

    _nse_cookie()

    cur_dt     = from_dt
    downloaded = 0
    failed     = 0
    while cur_dt <= to_dt:
        if cur_dt.weekday() < 5:          # skip weekends
            dt_str = cur_dt.strftime("%Y-%m-%d")
            if dt_str not in existing:
                result = _fetch_bhavcopy_pcr(cur_dt)
                if result:
                    conn = get_conn()
                    conn.execute(
                        "INSERT OR REPLACE INTO chain_daily VALUES (?,?,?,?,?,?)",
                        (dt_str, result["pcr"], result["total_call_oi"],
                         result["total_put_oi"], result["max_pain_proxy"], 0.0)
                    )
                    conn.commit()
                    conn.close()
                    downloaded += 1
                else:
                    failed += 1
                time.sleep(0.3)
        cur_dt += timedelta(days=1)

    logger.info(f"Chain history: {downloaded} downloaded, {failed} failed/missing")
    return {"downloaded": downloaded, "failed": failed}


def _fetch_bhavcopy_pcr(dt: date):
    dd   = dt.strftime("%d")
    mm   = dt.strftime("%m")
    yyyy = dt.strftime("%Y")
    mon  = dt.strftime("%b").upper()

    urls = [
        f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{dd}{mm}{yyyy}_F_0000.csv",
        f"https://archives.nseindia.com/content/historical/DERIVATIVES/{yyyy}/{mon}/fo{dd}{mon}{yyyy}bhav.csv.zip",
    ]
    for url in urls:
        try:
            resp = _NSE.get(url, timeout=12)
            if resp.status_code != 200:
                continue
            content = resp.content
            if url.endswith(".zip"):
                try:
                    with zipfile.ZipFile(io.BytesIO(content)) as z:
                        content = z.read(z.namelist()[0])
                except Exception:
                    continue
            result = _parse_pcr(content.decode("utf-8", errors="ignore"), ref_date=dt)
            if result:
                return result
        except Exception as e:
            logger.debug(f"Bhavcopy {dt} {url}: {e.__class__.__name__}")
    return None


def _parse_pcr(csv_text: str, ref_date=None):
    try:
        reader = csv.DictReader(io.StringIO(csv_text))
        rows   = []
        for row in reader:
            sym = (row.get("SYMBOL") or row.get("TradSym") or "").strip()
            opt = (row.get("OPTION_TYP") or row.get("OptTp") or "").strip()
            exp = (row.get("EXPIRY_DT") or row.get("XpryDt") or "").strip()
            oi  = int(float(row.get("OPEN_INT") or row.get("OpnIntrst") or 0))
            if sym == "NIFTY" and opt in ("CE", "PE") and oi > 0:
                rows.append({"opt": opt, "exp": exp, "oi": oi})

        if not rows:
            return None

        def _parse_exp(s):
            for fmt in ("%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d", "%d/%m/%Y"):
                try:
                    return datetime.strptime(s, fmt).date()
                except Exception:
                    pass
            return None

        # Use ref_date (the bhavcopy date) to find nearest expiry, not today
        anchor  = ref_date if ref_date else datetime.now().date()
        exps    = sorted(set(r["exp"] for r in rows))
        nearest = None
        for e in exps:
            d = _parse_exp(e)
            if d and d >= anchor:
                nearest = e
                break
        if not nearest:
            nearest = exps[-1] if exps else None
        if not nearest:
            return None

        filtered  = [r for r in rows if r["exp"] == nearest]
        if not filtered:
            filtered = rows

        call_oi = sum(r["oi"] for r in filtered if r["opt"] == "CE")
        put_oi  = sum(r["oi"] for r in filtered if r["opt"] == "PE")
        if call_oi == 0:
            return None

        return {
            "pcr":           round(put_oi / call_oi, 3),
            "total_call_oi": call_oi,
            "total_put_oi":  put_oi,
            "max_pain_proxy": 0,
        }
    except Exception as e:
        logger.debug(f"PCR parse error: {e}")
        return None


# ─── NSE PARTICIPANT OI — FII + PCR (works for all dates) ────────────────────
def download_participant_oi(days: int = 1095):
    """
    Download NSE participant-wise OI CSV for each trading day.
    URL: archives.nseindia.com/content/nsccl/fao_participant_oi_DDMMYYYY.csv
    Extracts:
      - FII net index futures position → stored in fii_daily
      - Total NIFTY index option Put/Call OI → PCR stored in chain_daily
    Works for all dates including post-Jul 2024.
    """
    to_dt   = datetime.now(IST).date()
    from_dt = to_dt - timedelta(days=days)

    conn = get_conn()
    existing_fii   = set(r[0] for r in conn.execute("SELECT date FROM fii_daily").fetchall())
    existing_chain = set(r[0] for r in conn.execute("SELECT date FROM chain_daily").fetchall())
    conn.close()

    _nse_cookie()

    dl_fii = dl_pcr = failed = 0
    cur = from_dt

    while cur <= to_dt:
        if cur.weekday() < 5:
            dt_str  = cur.strftime("%Y-%m-%d")
            dt_file = cur.strftime("%d%m%Y")
            need_fii   = dt_str not in existing_fii
            need_chain = dt_str not in existing_chain

            if need_fii or need_chain:
                url = f"https://archives.nseindia.com/content/nsccl/fao_participant_oi_{dt_file}.csv"
                try:
                    resp = _NSE.get(url, timeout=12)
                    if resp.status_code == 200 and len(resp.content) > 200:
                        result = _parse_participant_oi(resp.text)
                        if result:
                            conn = get_conn()
                            if need_fii:
                                conn.execute(
                                    "INSERT OR REPLACE INTO fii_daily VALUES (?,?,?)",
                                    (dt_str, result["fii_net"], result["dii_net"])
                                )
                                existing_fii.add(dt_str)
                                dl_fii += 1
                            if need_chain and result["pcr"] > 0:
                                conn.execute(
                                    "INSERT OR REPLACE INTO chain_daily VALUES (?,?,?,?,?,?)",
                                    (dt_str, result["pcr"], result["call_oi"],
                                     result["put_oi"], 0, 0.0)
                                )
                                existing_chain.add(dt_str)
                                dl_pcr += 1
                            conn.commit()
                            conn.close()
                    else:
                        failed += 1
                except Exception as e:
                    logger.debug(f"Participant OI {dt_str}: {e.__class__.__name__}")
                    failed += 1
                time.sleep(0.3)

        cur += timedelta(days=1)

    logger.info(
        f"Participant OI: FII={dl_fii} PCR={dl_pcr} downloaded, {failed} failed/missing"
    )
    return {"fii_downloaded": dl_fii, "pcr_downloaded": dl_pcr, "failed": failed}


def _parse_participant_oi(csv_text: str) -> dict:
    """Parse participant OI CSV → extract FII net futures + total index option OI."""
    try:
        lines  = [l.strip() for l in csv_text.splitlines() if l.strip()]
        header = None
        data   = {}
        for line in lines:
            cols = [c.strip().strip('"') for c in line.split(",")]
            if cols[0] == "Client Type":
                header = cols
                continue
            if header and len(cols) >= 9:
                client = cols[0].upper()
                try:
                    row = {header[i]: int(cols[i].replace("\t","").replace(" ","") or 0)
                           for i in range(1, min(len(header), len(cols)))}
                    data[client] = row
                except Exception:
                    pass

        if not data:
            return None

        # FII net index futures = long - short
        fii = data.get("FII", {})
        dii = data.get("DII", {})
        fii_net = fii.get("Future Index Long", 0) - fii.get("Future Index Short", 0)
        dii_net = dii.get("Future Index Long", 0) - dii.get("Future Index Short", 0)

        # PCR from total index options (all participants)
        total = data.get("TOTAL", {})
        call_oi = total.get("Option Index Call Long", 0)
        put_oi  = total.get("Option Index Put Long",  0)
        pcr     = round(put_oi / call_oi, 3) if call_oi > 0 else 0.0

        return {
            "fii_net":  float(fii_net),
            "dii_net":  float(dii_net),
            "pcr":      pcr,
            "call_oi":  call_oi,
            "put_oi":   put_oi,
        }
    except Exception as e:
        logger.debug(f"Participant OI parse error: {e}")
        return None


def download_fii_history(days: int = 1095):
    """Alias — use participant OI which gives both FII and PCR."""
    return download_participant_oi(days)


# ─── DATA SUMMARY ─────────────────────────────────────────────────────────────
def get_data_summary() -> dict:
    conn = get_conn()
    def _cr(table, col="date"):
        r = conn.execute(f"SELECT COUNT(*), MIN({col}), MAX({col}) FROM {table}").fetchone()
        return {"count": r[0] or 0, "from": r[1], "to": r[2]}
    s = {
        "ohlcv":      _cr("ohlcv"),
        "vix":        _cr("vix_daily"),
        "chain":      _cr("chain_daily"),
        "fii":        _cr("fii_daily"),
        "signal_log": _cr("signal_log"),
    }
    conn.close()
    return s


# ─── LIVE SIGNAL LOGGING ──────────────────────────────────────────────────────
def log_signal(gates: dict, verdict: str, pass_count: int,
               indices: dict, chain, fii):
    """Append every live signal verdict to the DB for future analysis."""
    try:
        conn = get_conn()
        now  = datetime.now(IST)
        conn.execute("""
            INSERT INTO signal_log
            (date, session, g1, g2, g3, g4, g5,
             g1_score, g2_score, g3_score, g4_score, g5_score,
             verdict, pass_count, nifty, vix, pcr, fii_net,
             nifty_next, outcome_pts, outcome, ts)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now.strftime("%Y-%m-%d"), now.strftime("%H:%M"),
            gates.get(1, {}).get("state", ""),
            gates.get(2, {}).get("state", ""),
            gates.get(3, {}).get("state", ""),
            gates.get(4, {}).get("state", ""),
            gates.get(5, {}).get("state", ""),
            gates.get(1, {}).get("score", 0),
            gates.get(2, {}).get("score", 0),
            gates.get(3, {}).get("score", 0),
            gates.get(4, {}).get("score", 0),
            gates.get(5, {}).get("score", 0),
            verdict, pass_count,
            indices.get("nifty", 0) if indices else 0,
            indices.get("vix", 0)   if indices else 0,
            chain.get("pcr", 0)     if chain   else 0,
            fii.get("fii_net", 0)   if fii     else 0,
            None, None, None,
            time.time(),
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"Signal log write: {e}")


def fill_outcomes():
    """Fill outcome_pts for live signals using next-day OHLCV close."""
    conn    = get_conn()
    pending = conn.execute(
        "SELECT id, date, nifty FROM signal_log "
        "WHERE outcome IS NULL AND nifty > 0 ORDER BY date, ts"
    ).fetchall()
    updated = 0
    for sig_id, sig_date, sig_price in pending:
        nxt = conn.execute(
            "SELECT close FROM ohlcv WHERE date > ? ORDER BY date LIMIT 1",
            (sig_date,)
        ).fetchone()
        if nxt:
            pts     = round(nxt[0] - sig_price, 2)
            outcome = "WIN" if pts >= 30 else "LOSS" if pts <= -30 else "NEUTRAL"
            conn.execute(
                "UPDATE signal_log SET nifty_next=?, outcome_pts=?, outcome=? WHERE id=?",
                (nxt[0], pts, outcome, sig_id)
            )
            updated += 1
    conn.commit()
    conn.close()
    return updated


def _gate_snapshot_json(gates: dict) -> str:
    try:
        import json
        snap = {}
        for k, g in (gates or {}).items():
            kk = str(k)
            snap[kk] = {
                "state": g.get("state", ""),
                "score": g.get("score", 0),
                "name": g.get("name", f"G{kk}"),
            }
        return json.dumps(snap)
    except Exception:
        return "{}"


def swing_radar_candidates(
    stocks: list,
    indices: dict,
    chain: dict,
    _verdict: str,
    _pass_count: int,
    min_score: int = 60,
) -> list:
    """
    Core Swing Radar stock filtering (same rules as live persist). Returns up to 12 picks.
    """
    if not stocks:
        return []
    try:
        from config import SWING_RADAR as SWR
    except Exception:
        SWR = {
            "min_score_log": 58,
            "min_rr": 1.72,
            "vix_strict_above": 22.0,
            "vix_extra_min_score": 3,
            "nifty_against_threshold": 0.45,
            "counter_trend_min_pc": 4,
            "rs_long_min_vs_nifty": 0.06,
            "rs_short_max_vs_nifty": -0.06,
            "pcr_soft_long_min": 0.82,
            "pcr_soft_short_max": 1.22,
            "pcr_soft_min_pc": 4,
            "vol_breakout_min": 1.02,
            "vol_breakout_min_vix": 1.10,
            "oi_long_breakout_max_neg": -7.0,
            "no_trade_bypass_min_score": 76,
            "no_trade_bypass_min_pc": 3,
            "weak_symbol_penalty": 8,
            "recovery_vol_min": 1.0,
        }

    weak_syms: set[str] = set()
    try:
        weak_syms = {str(x).upper() for x in (get_signal_accuracy_filters().get("weak_symbols") or [])}
    except Exception:
        pass

    vix = float((indices or {}).get("vix", 0) or 0)
    pcr = float((chain or {}).get("pcr", 0) or 0)
    nifty_chg = float((indices or {}).get("nifty_chg", 0) or 0)
    verdict_raw = str(_verdict or "").upper().strip().replace(" ", "_")

    min_log = int(SWR.get("min_score_log", min_score))
    if vix >= float(SWR.get("vix_strict_above", 22.0)):
        min_log += int(SWR.get("vix_extra_min_score", 5))

    picks = []
    skip_syms = {"NIFTY", "BANKNIFTY", "INDIAVIX"}
    for s in stocks:
        sym = str(s.get("symbol", "") or "").upper()
        if not sym or sym in skip_syms:
            continue
        price = float(s.get("price", 0) or 0)
        if price <= 0:
            continue
        chg = float(s.get("chg_pct", 0) or 0)
        raw_score = int(s.get("score", 0) or 0)
        penalty = int(SWR.get("weak_symbol_penalty", 10)) if sym in weak_syms else 0
        rank_score = raw_score - penalty
        if rank_score < min_log:
            continue

        pc = int(s.get("pc", 0) or 0)
        vol_r = float(s.get("vol_ratio", 0) or 0)
        rs = float(s.get("rs_pct", 0) or 0)
        oi_p = float(s.get("oi_chg_pct", 0) or 0)
        atr_pct = float(s.get("atr_pct", 0) or max(abs(chg) * 0.8, 1.2))
        atr = max(price * atr_pct / 100.0, price * 0.006)
        direction = "LONG" if chg >= 0 else "SHORT"
        entry = price
        if direction == "LONG":
            stop = entry - atr * 1.5
            target = entry + atr * 3
        else:
            stop = entry + atr * 1.5
            target = entry - atr * 3
        risk = abs(entry - stop)
        reward = abs(target - entry)
        rr = (reward / risk) if risk > 1e-9 else 0.0
        if rr < float(SWR.get("min_rr", 1.85)):
            continue

        if verdict_raw == "NO_TRADE":
            if raw_score < int(SWR.get("no_trade_bypass_min_score", 82)):
                continue
            if pc < int(SWR.get("no_trade_bypass_min_pc", 4)):
                continue

        nth = float(SWR.get("nifty_against_threshold", 0.38))
        ctp = int(SWR.get("counter_trend_min_pc", 4))
        if direction == "LONG" and nifty_chg <= -nth:
            if rs < float(SWR.get("rs_long_min_vs_nifty", 0.12)) or pc < ctp:
                continue
        if direction == "SHORT" and nifty_chg >= nth:
            if rs > float(SWR.get("rs_short_max_vs_nifty", -0.12)) or pc < ctp:
                continue

        if direction == "LONG" and pcr < float(SWR.get("pcr_soft_long_min", 0.88)):
            if pc < int(SWR.get("pcr_soft_min_pc", 4)):
                continue
        if direction == "SHORT" and pcr > float(SWR.get("pcr_soft_short_max", 1.18)):
            if pc < int(SWR.get("pcr_soft_min_pc", 4)):
                continue

        if chg >= 0.3:
            setup = "Breakout"
        elif chg <= -0.3:
            setup = "Pullback"
        else:
            setup = "Recovery"

        if setup == "Breakout":
            v_need = float(
                SWR.get("vol_breakout_min_vix", 1.22)
                if vix >= float(SWR.get("vix_strict_above", 22.0))
                else SWR.get("vol_breakout_min", 1.12)
            )
            if vol_r < v_need:
                continue
            if direction == "LONG" and oi_p < float(SWR.get("oi_long_breakout_max_neg", -5.5)):
                continue
        elif setup == "Recovery":
            if vol_r < float(SWR.get("recovery_vol_min", 1.02)):
                continue

        sig_lbl = "EXECUTE" if pc >= 4 else "WATCH" if pc >= 3 else "SCAN"
        picks.append(
            {
                "sym": sym,
                "score": raw_score,
                "rank_score": rank_score,
                "pc": pc,
                "price": price,
                "chg": chg,
                "vol_r": vol_r,
                "entry": entry,
                "stop": stop,
                "target": target,
                "setup": setup,
                "direction": direction,
                "sig_lbl": sig_lbl,
            }
        )

    picks.sort(key=lambda x: -x["rank_score"])
    return picks[:12]


def log_swing_radar_triggers(
    stocks: list,
    gates: dict,
    _verdict: str,
    _pass_count: int,
    indices: dict,
    chain: dict,
    min_score: int = 60,
) -> int:
    """
    Persist Swing Radar setups into live_signal_history (quality-filtered).

    Filters: min R:R, index vs stock RS when fighting tape, PCR soft alignment, VIX/volume on
    breakouts, weak-symbol penalty from live outcome stats, NO_TRADE gate (bypass only
    exceptional names). Mirrors frontend _renderSwingLive when SWING_Q is in sync.

    Dedup: one row per (trade_date, symbol, setup, 2h_bucket) via UNIQUE signal_key + INSERT OR IGNORE.
    """
    if not stocks:
        return 0

    now = datetime.now(IST)
    now_ts = time.time()
    trade_date = now.strftime("%Y-%m-%d")
    signal_time = now.strftime("%H:%M")
    vix = float((indices or {}).get("vix", 0) or 0)
    pcr = float((chain or {}).get("pcr", 0) or 0)
    gate_json = _gate_snapshot_json(gates)
    bucket_2h = int(now_ts // 7200)

    picks = swing_radar_candidates(stocks, indices, chain, _verdict, _pass_count, min_score)

    conn = get_conn()
    inserted = 0
    try:
        for p in picks:
            signal_key = f"SWING|{trade_date}|{p['sym']}|{p['setup']}|{bucket_2h}"
            trig = (
                f"Swing {p['setup']} | {p['sig_lbl']} | {p['chg']:+.2f}% | Vol×{p['vol_r']:.1f} | "
                f"{p['pc']}/5 gates | score {p['score']}"
            )
            if p.get("rank_score", p["score"]) < p["score"]:
                trig += f" | rank {p['rank_score']}"
            strength = "hi" if p["score"] >= 80 else "md" if p["score"] >= 65 else "lo"
            try:
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO live_signal_history
                    (signal_key, trade_date, symbol, signal_type, trigger, strength, signal_time,
                     entry_price, stop_loss, target_price, status, outcome, pnl_pts, pnl_pct,
                     hold_minutes, gate_pass_count, gate_snapshot, verdict, vix, pcr, created_ts, updated_ts)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        signal_key,
                        trade_date,
                        p["sym"],
                        p["direction"],
                        trig,
                        strength,
                        signal_time,
                        round(p["entry"], 2),
                        round(p["stop"], 2),
                        round(p["target"], 2),
                        "OPEN",
                        "OPEN",
                        None,
                        None,
                        0,
                        p["pc"],
                        gate_json,
                        "SWING_RADAR",
                        vix,
                        pcr,
                        now_ts,
                        now_ts,
                    ),
                )
                if cur.rowcount == 1:
                    inserted += 1
            except Exception:
                continue
        conn.commit()
    finally:
        conn.close()
    return inserted


def log_live_spikes(spikes: list, gates: dict, verdict: str, pass_count: int, indices: dict, chain: dict):
    """Persist new live spike events with entry/SL/target snapshot."""
    if not spikes:
        return 0
    now = datetime.now(IST)
    now_ts = time.time()
    trade_date = now.strftime("%Y-%m-%d")
    signal_time = now.strftime("%H:%M")
    gate_json = _gate_snapshot_json(gates)
    vix = float((indices or {}).get("vix", 0) or 0)
    pcr = float((chain or {}).get("pcr", 0) or 0)
    conn = get_conn()
    inserted = 0
    try:
        for sp in spikes:
            sym = str(sp.get("symbol", "") or "").upper()
            sig = str(sp.get("signal", "") or "")
            if not sym or not sig:
                continue
            price = float(sp.get("price", 0) or 0)
            if price <= 0:
                continue
            direction = "LONG" if sig in ("LONG", "OI BUILD", "VOL SPIKE") or sp.get("type") == "buy" else "SHORT"
            stop_loss = round(price * (0.994 if direction == "LONG" else 1.006), 2)
            target = round(price * (1.012 if direction == "LONG" else 0.988), 2)
            signal_key = f"{trade_date}|{sym}|{sig}|{sp.get('time') or signal_time}|{round(price,2)}"
            exists = conn.execute("SELECT 1 FROM live_signal_history WHERE signal_key=?", (signal_key,)).fetchone()
            if exists:
                continue
            conn.execute(
                """
                INSERT INTO live_signal_history
                (signal_key, trade_date, symbol, signal_type, trigger, strength, signal_time,
                 entry_price, stop_loss, target_price, status, outcome, pnl_pts, pnl_pct,
                 hold_minutes, gate_pass_count, gate_snapshot, verdict, vix, pcr, created_ts, updated_ts)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    signal_key, trade_date, sym, direction, sp.get("trigger", ""), sp.get("strength", "lo"), sp.get("time") or signal_time,
                    price, stop_loss, target, "OPEN", "OPEN", None, None,
                    0, int(pass_count or 0), gate_json, verdict or "WAIT", vix, pcr, now_ts, now_ts,
                )
            )
            inserted += 1
        conn.commit()
    finally:
        conn.close()
    return inserted


def update_live_signal_outcomes(
    prices: dict, max_hold_minutes: int = 90, swing_max_hold_minutes: int = 10080
):
    """Update open signals against current market prices (intraday spikes + swing radar)."""
    if not prices:
        return 0
    conn = get_conn()
    now = datetime.now(IST)
    now_ts = time.time()
    rows = conn.execute(
        """SELECT id, symbol, signal_type, signal_time, entry_price, stop_loss, target_price,
                  verdict, trade_date
           FROM live_signal_history WHERE status='OPEN'"""
    ).fetchall()
    updated = 0
    for (
        sig_id,
        symbol,
        signal_type,
        signal_time,
        entry_price,
        stop_loss,
        target_price,
        verdict,
        trade_date,
    ) in rows:
        px = prices.get(symbol, {}) or {}
        last_price = float(px.get("price", 0) or 0)
        if last_price <= 0:
            continue
        direction = str(signal_type or "LONG").upper()
        status = None
        outcome = None
        if direction == "SHORT":
            if last_price <= float(target_price or 0):
                status = "CLOSED"
                outcome = "TARGET HIT"
            elif last_price >= float(stop_loss or 0):
                status = "CLOSED"
                outcome = "SL HIT"
        else:
            if last_price >= float(target_price or 0):
                status = "CLOSED"
                outcome = "TARGET HIT"
            elif last_price <= float(stop_loss or 0):
                status = "CLOSED"
                outcome = "SL HIT"

        is_swing = str(verdict or "") == "SWING_RADAR"
        max_hold = swing_max_hold_minutes if is_swing else max_hold_minutes
        hold_minutes = 0
        try:
            if is_swing and trade_date:
                sig_dt = datetime.strptime(
                    f"{str(trade_date).strip()} {signal_time}", "%Y-%m-%d %H:%M"
                )
                if sig_dt.tzinfo is None:
                    sig_dt = IST.localize(sig_dt)
                hold_minutes = int(max((now - sig_dt).total_seconds() / 60, 0))
            else:
                sig_dt = datetime.strptime(
                    f"{now.strftime('%Y-%m-%d')} {signal_time}", "%Y-%m-%d %H:%M"
                )
                hold_minutes = int(
                    max((now - IST.localize(sig_dt)).total_seconds() / 60, 0)
                )
        except Exception:
            hold_minutes = 0

        # Production-grade quick exits for intraday spikes only.
        if not status and not is_swing:
            try:
                from config import GATE as TH
            except Exception:
                TH = {}
            ef_min = int(TH.get("spike_live_early_fail_min", 8) or 8)
            ef_adv = float(TH.get("spike_live_early_fail_adverse_pct", 0.18) or 0.18)
            nf_min = int(TH.get("spike_live_no_ft_min", 15) or 15)
            nf_fav = float(TH.get("spike_live_no_ft_min_fav_pct", 0.12) or 0.12)
            if entry_price and entry_price > 0:
                if direction == "SHORT":
                    fav_pct = ((entry_price - last_price) / entry_price) * 100
                    adv_pct = ((last_price - entry_price) / entry_price) * 100
                else:
                    fav_pct = ((last_price - entry_price) / entry_price) * 100
                    adv_pct = ((entry_price - last_price) / entry_price) * 100
                if hold_minutes >= max(1, ef_min) and adv_pct >= max(0.05, ef_adv):
                    status = "CLOSED"
                    outcome = "EARLY_FAIL"
                elif hold_minutes >= max(3, nf_min) and fav_pct < max(0.05, nf_fav):
                    status = "CLOSED"
                    outcome = "NO_FOLLOW"

        if not status and hold_minutes >= max_hold:
            status = "CLOSED"
            outcome = "EXPIRED"

        if not status:
            continue

        pnl_pts = round((entry_price - last_price), 2) if direction == "SHORT" else round((last_price - entry_price), 2)
        pnl_pct = round((pnl_pts / entry_price) * 100, 2) if entry_price else 0
        conn.execute(
            "UPDATE live_signal_history SET exit_price=?, exit_time=?, status=?, outcome=?, pnl_pts=?, pnl_pct=?, hold_minutes=?, updated_ts=? WHERE id=?",
            (last_price, now.strftime("%H:%M"), status, outcome, pnl_pts, pnl_pct, hold_minutes, now_ts, sig_id)
        )
        updated += 1
    conn.commit()
    conn.close()
    return updated


def _clean_signal_text(val):
    if val is None:
        return val
    s = str(val)
    for bad in ("A�", "Â·", "·", "•", "�"):
        s = s.replace(bad, " | ")
    while "  " in s:
        s = s.replace("  ", " ")
    return s.strip(" |")


def wipe_live_signal_history():
    """Delete all rows from live_signal_history (spikes + swing + backfills)."""
    conn = get_conn()
    try:
        conn.execute("DELETE FROM live_signal_history")
        conn.commit()
    finally:
        conn.close()


def wipe_index_signal_history():
    """Delete all rows from index_signal_history (NIFTY/BANKNIFTY radar)."""
    conn = get_conn()
    try:
        conn.execute("DELETE FROM index_signal_history")
        conn.commit()
    finally:
        conn.close()


def replace_index_signal_rows(sigs: list, trade_date: str | None = None) -> int:
    """
    Insert index radar rows (same field shape as scheduler._ix_db_upsert).
    Intended after a full wipe; uses explicit trade_date (default: today IST).
    """
    if not sigs:
        return 0
    td = (trade_date or datetime.now(IST).strftime("%Y-%m-%d")).strip()[:10]
    conn = get_conn()
    n = 0
    now_wall = time.time()
    try:
        for sig in sigs:
            sid = str(sig.get("id") or sig.get("sig_id") or "").strip()
            if not sid:
                continue
            tm = str(sig.get("time") or sig.get("signal_time") or "").strip()
            ts = float(sig.get("ts") or now_wall)
            conn.execute(
                """
                INSERT INTO index_signal_history
                  (sig_id, trade_date, symbol, type, signal_time, ts,
                   index_px, strike, entry, sl, t1, t2, rr, lot_sz, lot_pnl_t1,
                   chg_pct, strength, vix, quality, pcr, option_expiry, option_week,
                   outcome, exit_time, created_ts, updated_ts)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    sid,
                    td,
                    sig.get("symbol"),
                    sig.get("type"),
                    tm,
                    ts,
                    float(sig.get("index_px") or 0),
                    int(sig.get("strike") or 0),
                    float(sig.get("entry") or 0),
                    float(sig.get("sl") or 0),
                    float(sig.get("t1") or 0),
                    float(sig.get("t2") or 0),
                    float(sig.get("rr") or 0),
                    int(sig.get("lot_sz") or 0),
                    float(sig.get("lot_pnl_t1") or 0),
                    float(sig.get("chg_pct") or 0),
                    sig.get("strength", "md"),
                    float(sig.get("vix") or 0),
                    sig.get("quality"),
                    sig.get("pcr"),
                    sig.get("option_expiry"),
                    sig.get("option_week"),
                    sig.get("outcome"),
                    sig.get("exit_time") or sig.get("outcome_time"),
                    ts,
                    now_wall,
                ),
            )
            n += 1
        conn.commit()
    finally:
        conn.close()
    return n


def get_live_signal_history(
    limit: int = 100,
    status: str | None = None,
    verdict: str | None = None,
    min_trade_date: str | None = None,
):
    conn = get_conn()
    try:
        sql = "SELECT id, trade_date, symbol, signal_type, trigger, strength, signal_time, entry_price, stop_loss, target_price, exit_price, exit_time, status, outcome, pnl_pts, pnl_pct, hold_minutes, gate_pass_count, verdict, vix, pcr FROM live_signal_history"
        params = []
        where = []
        if status and status.upper() != "ALL":
            f = status.upper()
            if f in ("OPEN", "CLOSED"):
                where.append("status=?")
                params.append(f)
            elif f in ("TARGET HIT", "SL HIT", "EXPIRED", "OPEN"):
                where.append("outcome=?")
                params.append(f)
            elif f == "BACKFILLED":
                where.append("verdict=?")
                params.append(f)
        if verdict:
            vu = str(verdict).strip().upper()
            if vu == "SWING_RADAR":
                where.append("(verdict = ? OR IFNULL(signal_key,'') LIKE 'SWING|%')")
                params.append("SWING_RADAR")
            else:
                where.append("verdict=?")
                params.append(verdict)
        if min_trade_date:
            where.append("trade_date >= ?")
            params.append(str(min_trade_date).strip()[:10])
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_ts DESC LIMIT ?"
        params.append(int(limit))
        rows = conn.execute(sql, params).fetchall()
        cols = ["id","trade_date","symbol","signal_type","trigger","strength","signal_time","entry_price","stop_loss","target_price","exit_price","exit_time","status","outcome","pnl_pts","pnl_pct","hold_minutes","gate_pass_count","verdict","vix","pcr"]
        out = []
        for r in rows:
            row = dict(zip(cols, r))
            row["trigger"] = _clean_signal_text(row.get("trigger"))
            row["outcome"] = _clean_signal_text(row.get("outcome"))
            out.append(row)
        return out
    finally:
        conn.close()


def import_historical_spike_results(results: list):
    """Backfill historical spike backtest results into live signal history."""
    if not results:
        return 0
    conn = get_conn()
    inserted = 0
    try:
        for r in results:
            dt_raw = str(r.get("time", "") or "")
            trade_date = dt_raw[:10] if len(dt_raw) >= 10 else datetime.now(IST).strftime("%Y-%m-%d")
            signal_time = dt_raw[11:16] if len(dt_raw) >= 16 else "09:15"
            symbol = str(r.get("symbol", "") or "").upper()
            if not symbol:
                continue
            signal_type = "LONG" if str(r.get("type", "BUY")).upper() == "BUY" else "SHORT"
            entry = float(r.get("entry", 0) or 0)
            sl = float(r.get("sl", 0) or 0)
            t1 = float(r.get("t1", 0) or 0)
            t2 = float(r.get("t2", 0) or 0)
            result = str(r.get("result", "EXPIRED") or "EXPIRED").upper()
            outcome = {"HIT_T1": "TARGET HIT", "HIT_T2": "TARGET HIT", "HIT_SL": "SL HIT", "EXPIRED": "EXPIRED"}.get(result, result)
            exit_price = t1 if result == "HIT_T1" else t2 if result == "HIT_T2" else sl if result == "HIT_SL" else None
            pnl_pct = float(r.get("pnl_pct", 0) or 0)
            pnl_pts = round(entry * pnl_pct / 100, 2) if entry and pnl_pct else None
            signal_key = f"BACKFILL|{trade_date}|{symbol}|{signal_time}|{round(entry,2)}|{signal_type}"
            exists = conn.execute("SELECT 1 FROM live_signal_history WHERE signal_key=?", (signal_key,)).fetchone()
            if exists:
                continue
            now_ts = time.time()
            conn.execute(
                """
                INSERT INTO live_signal_history
                (signal_key, trade_date, symbol, signal_type, trigger, strength, signal_time,
                 entry_price, stop_loss, target_price, exit_price, exit_time, status, outcome,
                 pnl_pts, pnl_pct, hold_minutes, gate_pass_count, gate_snapshot, verdict,
                 vix, pcr, created_ts, updated_ts)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    signal_key, trade_date, symbol, signal_type,
                    f"Price {float(r.get('chg_pct', 0) or 0):+.2f}% · Vol {float(r.get('vol_mult', 0) or 0):.1f}x",
                    "hi" if result == "HIT_T2" or float(r.get("score", 0) or 0) >= 70 else "md" if float(r.get("score", 0) or 0) >= 55 else "lo",
                    signal_time, entry, sl, t2 or t1, exit_price,
                    signal_time if exit_price is not None else None,
                    "CLOSED", outcome, pnl_pts, pnl_pct, 45, None, "{}", "BACKFILLED",
                    None, None, now_ts, now_ts,
                )
            )
            inserted += 1
        conn.commit()
    finally:
        conn.close()
    return inserted


def get_signal_accuracy_filters():
    """Return weak symbols/time buckets derived from stored signal outcomes."""
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT symbol, signal_time, outcome FROM live_signal_history
               WHERE status='CLOSED' AND outcome IN ('TARGET HIT','SL HIT','EXPIRED')
                 AND COALESCE(verdict,'') != 'SWING_RADAR'"""
        ).fetchall()
    finally:
        conn.close()

    symbol_stats = {}
    bucket_stats = {}

    def _bucket(hm: str):
        try:
            hh, mm = hm.split(':')
            mins = int(hh) * 60 + int(mm)
        except Exception:
            return "unknown"
        if mins < 570:
            return "open_915_930"
        if mins < 630:
            return "morning_930_1030"
        if mins < 780:
            return "midday_1030_1300"
        return "late_1300_plus"

    for sym, sig_time, outcome in rows:
        is_win = 1 if outcome == 'TARGET HIT' else 0
        ss = symbol_stats.setdefault(sym, {"n": 0, "w": 0})
        ss["n"] += 1
        ss["w"] += is_win
        bk = _bucket(sig_time or '')
        bs = bucket_stats.setdefault(bk, {"n": 0, "w": 0})
        bs["n"] += 1
        bs["w"] += is_win

    weak_symbols = {
        sym for sym, st in symbol_stats.items()
        if st["n"] >= 2 and (st["w"] / st["n"] * 100) <= 35
    }
    weak_buckets = {
        bk for bk, st in bucket_stats.items()
        if st["n"] >= 3 and (st["w"] / st["n"] * 100) <= 35
    }
    return {
        "weak_symbols": weak_symbols,
        "weak_buckets": weak_buckets,
        "symbol_stats": {k: {"sample": v["n"], "win_rate": round(v["w"] / v["n"] * 100, 1)} for k, v in symbol_stats.items()},
        "bucket_stats": {k: {"sample": v["n"], "win_rate": round(v["w"] / v["n"] * 100, 1)} for k, v in bucket_stats.items()},
    }
