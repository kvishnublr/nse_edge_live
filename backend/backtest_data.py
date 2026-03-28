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
