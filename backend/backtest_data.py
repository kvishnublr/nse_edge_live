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
    """)
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


def update_live_signal_outcomes(prices: dict, max_hold_minutes: int = 90):
    """Update open spike signals against current market prices."""
    if not prices:
        return 0
    conn = get_conn()
    now = datetime.now(IST)
    now_ts = time.time()
    rows = conn.execute(
        "SELECT id, symbol, signal_type, signal_time, entry_price, stop_loss, target_price FROM live_signal_history WHERE status='OPEN'"
    ).fetchall()
    updated = 0
    for sig_id, symbol, signal_type, signal_time, entry_price, stop_loss, target_price in rows:
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

        try:
            sig_dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {signal_time}", "%Y-%m-%d %H:%M")
            hold_minutes = int(max((now - IST.localize(sig_dt)).total_seconds() / 60, 0))
        except Exception:
            hold_minutes = 0

        if not status and hold_minutes >= max_hold_minutes:
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


def get_live_signal_history(limit: int = 100, status: str | None = None):
    conn = get_conn()
    try:
        sql = "SELECT id, trade_date, symbol, signal_type, trigger, strength, signal_time, entry_price, stop_loss, target_price, exit_price, exit_time, status, outcome, pnl_pts, pnl_pct, hold_minutes, gate_pass_count, verdict, vix, pcr FROM live_signal_history"
        params = []
        if status and status.upper() != "ALL":
            f = status.upper()
            if f in ("OPEN", "CLOSED"):
                sql += " WHERE status=?"
                params.append(f)
            elif f in ("TARGET HIT", "SL HIT", "EXPIRED", "OPEN"):
                sql += " WHERE outcome=?"
                params.append(f)
            elif f == "BACKFILLED":
                sql += " WHERE verdict=?"
                params.append(f)
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
            "SELECT symbol, signal_time, outcome FROM live_signal_history WHERE status='CLOSED' AND outcome IN ('TARGET HIT','SL HIT','EXPIRED')"
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
