from __future__ import annotations

import base64
import datetime as dt
import html
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import shutil
import smtplib
import sqlite3
import ssl
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from email.message import EmailMessage
from typing import Any, Optional
from urllib.parse import parse_qs, quote, urlencode, urlparse

import requests
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from dotenv import load_dotenv
from config import is_market_open, get_market_status

logger = logging.getLogger("saas")
router = APIRouter(tags=["saas-platform"])

# Zerodha Kite Connect redirect target (register this exact URL on the Kite app).
KITE_OAUTH_RETURN_PATH = "/kite-oauth-return"

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "saas.db"
TOKEN_SECRET = (os.getenv("SAAS_JWT_SECRET", "stockr-saas-local-secret") or "stockr-saas-local-secret").strip()
TOKEN_TTL_HOURS = int(os.getenv("SAAS_TOKEN_TTL_HOURS", "72") or 72)
BRAND_NAME = (os.getenv("APP_BRAND_NAME", "STOCKR.IN") or "STOCKR.IN").strip()
DEFAULT_ADMIN_EMAIL = (os.getenv("SAAS_ADMIN_EMAIL", "vishnualgo@gmail.com") or "vishnualgo@gmail.com").strip().lower()
DEFAULT_ADMIN_PASSWORD = (os.getenv("SAAS_ADMIN_PASSWORD", "NseEdge@123") or "NseEdge@123").strip()
WELCOME_CREDIT = float(os.getenv("SAAS_WELCOME_CREDIT", "500") or 500)
COUPON_PROFIT_CAP = float(os.getenv("SAAS_COUPON_PROFIT_CAP", "2000") or 2000)
GMAIL_SMTP_HOST = (os.getenv("GMAIL_SMTP_HOST", "smtp.gmail.com") or "smtp.gmail.com").strip()
GMAIL_SMTP_PORT = int(os.getenv("GMAIL_SMTP_PORT", "465") or 465)
GMAIL_USERNAME = (os.getenv("GMAIL_USERNAME", "") or "").strip()
GMAIL_APP_PASSWORD = (os.getenv("GMAIL_APP_PASSWORD", "") or "").strip()
GMAIL_FROM_EMAIL = (os.getenv("GMAIL_FROM_EMAIL", GMAIL_USERNAME) or GMAIL_USERNAME).strip()
GMAIL_FROM_NAME = (os.getenv("GMAIL_FROM_NAME", BRAND_NAME) or BRAND_NAME).strip()
GMAIL_NOTIFY_ON_SIGNUP = str(os.getenv("GMAIL_NOTIFY_ON_SIGNUP", "1") or "1").strip().lower() not in {"0", "false", "off", "no"}
GMAIL_NOTIFY_ON_PAYMENT = str(os.getenv("GMAIL_NOTIFY_ON_PAYMENT", "1") or "1").strip().lower() not in {"0", "false", "off", "no"}
GMAIL_NOTIFY_ON_SIGNAL = str(os.getenv("GMAIL_NOTIFY_ON_SIGNAL", "0") or "0").strip().lower() not in {"0", "false", "off", "no"}
GMAIL_OAUTH_CLIENT_ID = (os.getenv("GMAIL_OAUTH_CLIENT_ID", "") or "").strip()
GMAIL_OAUTH_CLIENT_SECRET = (os.getenv("GMAIL_OAUTH_CLIENT_SECRET", "") or "").strip()
GMAIL_OAUTH_REDIRECT_URI = (os.getenv("GMAIL_OAUTH_REDIRECT_URI", "") or "").strip()
GMAIL_OAUTH_SCOPES = str(
    os.getenv(
        "GMAIL_OAUTH_SCOPES",
        "https://www.googleapis.com/auth/gmail.send https://www.googleapis.com/auth/userinfo.email",
    )
    or "https://www.googleapis.com/auth/gmail.send https://www.googleapis.com/auth/userinfo.email"
).strip()
UPI_PAYEE = (os.getenv("UPI_PAYEE", "stockrin@upi") or "stockrin@upi").strip()
ADMIN_OTP_TTL_MINUTES = max(2, int(os.getenv("SAAS_ADMIN_OTP_TTL_MINUTES", "5") or 5))
ADMIN_OTP_MAX_ATTEMPTS = max(1, int(os.getenv("SAAS_ADMIN_OTP_MAX_ATTEMPTS", "5") or 5))
ADMIN_OTP_PHONE = (os.getenv("SAAS_ADMIN_OTP_PHONE", "9986238877") or "9986238877").strip()
ADMIN_OTP_WHATSAPP_APIKEY = (os.getenv("SAAS_ADMIN_OTP_WHATSAPP_APIKEY", "") or "").strip()

DEFAULT_STRATEGIES = [
    {"code": "SPIKE", "name": "Spike Hunt", "strategy_type": "INTRADAY", "description": "High-momentum stock spikes from the live scanner.", "active": 1, "theme": "pulse", "accent": "#ff7a18", "default_confidence": 72, "default_max_trades": 6},
    {"code": "INDEX", "name": "Index Hunt", "strategy_type": "INTRADAY", "description": "NIFTY and BANKNIFTY premium setups from INDEX HUNT.", "active": 1, "theme": "ice", "accent": "#38bdf8", "default_confidence": 70, "default_max_trades": 5},
    {"code": "SWING", "name": "Swing Radar", "strategy_type": "POSITIONAL", "description": "Multi-day swing and positional ideas from the live candidate engine.", "active": 1, "theme": "aurora", "accent": "#a78bfa", "default_confidence": 68, "default_max_trades": 3},
]

PLAN_CATALOG = [
    {"code": "STARTER", "name": "Starter", "price": 1000, "duration_days": 30, "wallet_credit": 0, "features": ["Signal access", "Strategy toggles", "Wallet", "Performance"]},
    {"code": "PRO", "name": "Pro", "price": 2500, "duration_days": 90, "wallet_credit": 250, "features": ["Everything in Starter", "Coupon boosts", "Priority inbox"]},
    {"code": "DESK", "name": "Desk", "price": 5000, "duration_days": 180, "wallet_credit": 750, "features": ["Everything in Pro", "Higher routing limits", "Longer retention"]},
]

BROKER_CATALOG = [
    {
        "code": "ZERODHA",
        "name": "Zerodha Kite",
        "tagline": "Real broker connection using your Kite API key and access token.",
        "supports_live": True,
        "supports_paper": True,
        "supports_auto": True,
        "accent": "#ffb347",
        "help": "Paste your Kite API key and a fresh access token. Use paper mode first, then turn live mode on once the test connection succeeds.",
    },
    {
        "code": "PAPER",
        "name": "Paper Router",
        "tagline": "Simulated execution inside STOCKR.IN without a live broker account.",
        "supports_live": False,
        "supports_paper": True,
        "supports_auto": True,
        "accent": "#67e8f9",
        "help": "Good for validating routing and automation before switching to a live broker.",
    },
]

KITE_INTERACTIVE_TIMEOUT_SECONDS = max(120, int(os.getenv("SAAS_KITE_INTERACTIVE_TIMEOUT_SECONDS", "300") or 300))
_kite_interactive_lock = threading.Lock()
_kite_interactive_sessions: dict[int, dict[str, Any]] = {}


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _utc_iso(value: Optional[dt.datetime] = None) -> str:
    return (value or _utc_now()).replace(microsecond=0).isoformat()


def _today_ist() -> dt.date:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=5, minutes=30))).date()


def _env_flag_true(key: str) -> bool:
    return str(os.getenv(key, "") or "").strip().lower() in ("1", "true", "yes", "on")


def _browser_session_expiry_iso() -> str:
    """UTC instant = start of next calendar day in IST (session valid for the full IST day)."""
    ist = dt.timezone(dt.timedelta(hours=5, minutes=30))
    now_ist = dt.datetime.now(ist)
    next_midnight_ist = dt.datetime.combine(now_ist.date() + dt.timedelta(days=1), dt.time.min, tzinfo=ist)
    return _utc_iso(next_midnight_ist.astimezone(dt.timezone.utc))


def _hash_browser_session_token(raw: str) -> str:
    return hashlib.sha256(str(raw).encode("utf-8")).hexdigest()


def _prune_expired_browser_sessions(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM saas_auth_sessions WHERE expires_at < ?", (_utc_iso(),))


def _issue_browser_session(conn: sqlite3.Connection, user_id: int) -> str:
    _prune_expired_browser_sessions(conn)
    raw = "nxa_" + secrets.token_urlsafe(36)
    th = _hash_browser_session_token(raw)
    now = _utc_iso()
    exp = _browser_session_expiry_iso()
    conn.execute(
        "INSERT INTO saas_auth_sessions(user_id,token_hash,created_at,expires_at,last_seen_at) VALUES(?,?,?,?,?)",
        (int(user_id), th, now, exp, now),
    )
    return raw


def _resolve_browser_session_user_id(conn: sqlite3.Connection, raw: str) -> Optional[int]:
    if not str(raw or "").startswith("nxa_"):
        return None
    th = _hash_browser_session_token(raw)
    now = _utc_iso()
    row = conn.execute(
        "SELECT user_id FROM saas_auth_sessions WHERE token_hash=? AND expires_at > ?",
        (th, now),
    ).fetchone()
    if row is None:
        return None
    conn.execute("UPDATE saas_auth_sessions SET last_seen_at=? WHERE token_hash=?", (now, th))
    return int(row["user_id"])


def _revoke_browser_session(conn: sqlite3.Connection, raw: str) -> None:
    if not str(raw or "").startswith("nxa_"):
        return
    th = _hash_browser_session_token(raw)
    conn.execute("DELETE FROM saas_auth_sessions WHERE token_hash=?", (th,))


def _normalize_login_email(value: str) -> str:
    email = str(value or "").strip().lower()
    if email.endswith("@stokr.in"):
        email = email[:-len("@stokr.in")] + "@stockr.in"
    return email


def _dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"))


def _loads(data: Any, fallback: Any) -> Any:
    if data in (None, ""):
        return fallback
    try:
        return json.loads(data)
    except Exception:
        return fallback


def _setting_get(conn: sqlite3.Connection, key: str, fallback: Any) -> Any:
    row = conn.execute("SELECT value_json FROM saas_app_settings WHERE key=?", (str(key or "").strip(),)).fetchone()
    if row is None:
        return fallback
    return _loads(row["value_json"], fallback)


def _setting_put(conn: sqlite3.Connection, key: str, value: Any, *, now: Optional[str] = None) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO saas_app_settings(key,value_json,updated_at) VALUES(?,?,?)",
        (str(key or "").strip(), _dumps(value), now or _utc_iso()),
    )


def _default_payment_profile() -> dict[str, Any]:
    return {
        "enabled": True,
        "payee_name": BRAND_NAME,
        "upi_id": UPI_PAYEE,
        "merchant_code": "",
        "support_phone": "",
        "support_email": DEFAULT_ADMIN_EMAIL,
        "instructions": "Pay the exact amount shown, then confirm inside the app so access updates immediately.",
        "theme_color": "#5ec8ff",
    }


def _normalize_payment_profile(value: Any) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    profile = _default_payment_profile()
    profile.update({k: v for k, v in raw.items() if v is not None})
    upi_id = str(profile.get("upi_id") or "").strip()
    payee_name = str(profile.get("payee_name") or BRAND_NAME).strip()[:80]
    merchant_code = str(profile.get("merchant_code") or "").strip()[:32]
    support_phone = _normalize_whatsapp_phone(profile.get("support_phone") or "")
    support_email = _normalize_login_email(profile.get("support_email") or DEFAULT_ADMIN_EMAIL)
    instructions = str(profile.get("instructions") or "").strip()[:280]
    theme_color = str(profile.get("theme_color") or "#5ec8ff").strip()[:24]
    enabled = bool(profile.get("enabled", True)) and bool(upi_id)
    return {
        "enabled": enabled,
        "payee_name": payee_name or BRAND_NAME,
        "upi_id": upi_id,
        "merchant_code": merchant_code,
        "support_phone": support_phone,
        "support_email": support_email if "@" in support_email else "",
        "instructions": instructions or "Pay the exact amount shown, then confirm inside the app so access updates immediately.",
        "theme_color": theme_color if theme_color.startswith("#") else "#5ec8ff",
    }


def _payment_profile(conn: sqlite3.Connection) -> dict[str, Any]:
    return _normalize_payment_profile(_setting_get(conn, "payment_profile", _default_payment_profile()))


def _public_payment_profile(conn: sqlite3.Connection) -> dict[str, Any]:
    profile = _payment_profile(conn)
    return {
        **profile,
        "can_auto_confirm": bool(_razorpay_ready() and (os.getenv("RAZORPAY_WEBHOOK_SECRET", "") or "").strip()),
        "mode": "gateway" if _razorpay_ready() else "direct_upi",
    }


def _upi_payload(profile: dict[str, Any], amount: float, plan_code: str, order_id: int) -> str:
    params = [
        f"pa={quote(str(profile.get('upi_id') or ''), safe='@._-')}",
        f"pn={quote(str(profile.get('payee_name') or BRAND_NAME), safe=' ')}".replace(" ", "%20"),
        f"tn={quote(str(plan_code or 'PLAN'), safe=' ')}".replace(" ", "%20"),
        f"am={amount:.2f}",
        "cu=INR",
        f"tr=LOCAL{int(order_id)}",
    ]
    merchant_code = str(profile.get("merchant_code") or "").strip()
    if merchant_code:
        params.append(f"mc={quote(merchant_code, safe='')}")
    return "upi://pay?" + "&".join(params)


def _pack_secret(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def _unpack_secret(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        return base64.urlsafe_b64decode(raw.encode("ascii")).decode("utf-8")
    except Exception:
        return raw


def _mask_secret(value: str, keep: int = 4) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if len(raw) <= keep:
        return "*" * len(raw)
    return ("*" * max(4, len(raw) - keep)) + raw[-keep:]


def _slug_tag(value: str, fallback: str = "AUTO") -> str:
    cleaned = re.sub(r"[^A-Z0-9]+", "", str(value or "").upper())
    return (cleaned or fallback)[:20]


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return float(default)
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return float(default)
    try:
        return float(match.group(0))
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(round(float(value)))
    except Exception:
        return int(default)


def _broker_catalog_map() -> dict[str, dict[str, Any]]:
    return {item["code"]: dict(item) for item in BROKER_CATALOG}


def _normalize_whatsapp_phone(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("+"):
        return "+" + re.sub(r"\D+", "", raw[1:])
    return re.sub(r"\D+", "", raw)


def _normalize_telegram_chat_id(value: Any) -> str:
    return str(value or "").strip()


def _strip_html_text(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _gmail_oauth_cfg() -> dict[str, Any]:
    return {
        "client_id": (os.getenv("GMAIL_OAUTH_CLIENT_ID", GMAIL_OAUTH_CLIENT_ID) or "").strip(),
        "client_secret": (os.getenv("GMAIL_OAUTH_CLIENT_SECRET", GMAIL_OAUTH_CLIENT_SECRET) or "").strip(),
        "redirect_uri": (os.getenv("GMAIL_OAUTH_REDIRECT_URI", GMAIL_OAUTH_REDIRECT_URI) or "").strip(),
        "scopes": [s for s in str(os.getenv("GMAIL_OAUTH_SCOPES", GMAIL_OAUTH_SCOPES) or GMAIL_OAUTH_SCOPES).split() if s],
    }


def _gmail_oauth_ready() -> bool:
    cfg = _gmail_oauth_cfg()
    return bool(cfg["client_id"] and cfg["client_secret"] and cfg["redirect_uri"] and cfg["scopes"])


def _gmail_oauth_state_sign(payload: dict[str, Any]) -> str:
    body = _dumps(payload).encode()
    sig = hmac.new(TOKEN_SECRET.encode(), body, hashlib.sha256).hexdigest()
    return _b64(body) + "." + sig


def _gmail_oauth_state_verify(token: str) -> dict[str, Any]:
    parts = str(token or "").split(".", 1)
    if len(parts) != 2:
        raise HTTPException(status_code=400, detail="Invalid OAuth state")
    body_b64, sig = parts
    try:
        body = _b64d(body_b64)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid OAuth state") from exc
    expected = hmac.new(TOKEN_SECRET.encode(), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, str(sig or "")):
        raise HTTPException(status_code=400, detail="Invalid OAuth state signature")
    data = _loads(body.decode("utf-8", errors="ignore"), {})
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Invalid OAuth state payload")
    exp = _safe_int(data.get("exp"), default=0)
    if exp <= int(time.time()):
        raise HTTPException(status_code=400, detail="OAuth state expired")
    return data


def _gmail_oauth_row(conn: sqlite3.Connection) -> dict[str, Any]:
    return _setting_get(
        conn,
        "gmail_oauth",
        {
            "connected": False,
            "email": "",
            "access_token": "",
            "refresh_token": "",
            "token_type": "Bearer",
            "scope": "",
            "expires_at": 0,
            "updated_at": "",
            "error": "",
        },
    )


def _gmail_oauth_save(conn: sqlite3.Connection, data: dict[str, Any]) -> None:
    now_ts = int(time.time())
    payload = {
        "connected": bool(data.get("connected", False)),
        "email": str(data.get("email") or "").strip().lower(),
        "access_token": str(data.get("access_token") or "").strip(),
        "refresh_token": str(data.get("refresh_token") or "").strip(),
        "token_type": str(data.get("token_type") or "Bearer").strip() or "Bearer",
        "scope": str(data.get("scope") or "").strip(),
        "expires_at": _safe_int(data.get("expires_at") or 0, default=0),
        "updated_at": str(data.get("updated_at") or _utc_iso()),
        "error": str(data.get("error") or "")[:500],
    }
    if payload["expires_at"] <= 0 and payload["access_token"]:
        payload["expires_at"] = now_ts + 3000
    _setting_put(conn, "gmail_oauth", payload)


def _gmail_oauth_refresh_if_needed(conn: sqlite3.Connection) -> dict[str, Any]:
    row = _gmail_oauth_row(conn)
    now_ts = int(time.time())
    if not row.get("connected") or not row.get("refresh_token"):
        return row
    if str(row.get("access_token") or "").strip() and _safe_int(row.get("expires_at"), default=0) > (now_ts + 90):
        return row
    cfg = _gmail_oauth_cfg()
    if not (cfg["client_id"] and cfg["client_secret"]):
        return row
    try:
        resp = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": cfg["client_id"],
                "client_secret": cfg["client_secret"],
                "refresh_token": str(row.get("refresh_token") or ""),
                "grant_type": "refresh_token",
            },
            timeout=20,
        )
        data = resp.json() if resp.content else {}
        if not resp.ok or not data.get("access_token"):
            row["error"] = str(data.get("error_description") or data.get("error") or f"OAuth refresh failed ({resp.status_code})")
            row["updated_at"] = _utc_iso()
            _gmail_oauth_save(conn, row)
            conn.commit()
            return row
        row["access_token"] = str(data.get("access_token") or "").strip()
        row["token_type"] = str(data.get("token_type") or row.get("token_type") or "Bearer")
        expires_in = max(300, _safe_int(data.get("expires_in"), default=3600))
        row["expires_at"] = now_ts + expires_in
        row["scope"] = str(data.get("scope") or row.get("scope") or "")
        row["connected"] = True
        row["error"] = ""
        row["updated_at"] = _utc_iso()
        _gmail_oauth_save(conn, row)
        conn.commit()
    except Exception as exc:
        row["error"] = str(exc)[:500]
        row["updated_at"] = _utc_iso()
        _gmail_oauth_save(conn, row)
        conn.commit()
    return row


def _gmail_oauth_user_email(access_token: str) -> str:
    token = str(access_token or "").strip()
    if not token:
        return ""
    try:
        resp = requests.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        data = resp.json() if resp.content else {}
        if not resp.ok:
            return ""
        return str(data.get("email") or "").strip().lower()
    except Exception:
        return ""


def _gmail_cfg() -> dict[str, Any]:
    """Load Gmail config from backend/.env at call time (no restart required)."""
    try:
        load_dotenv(BASE_DIR / ".env", override=True)
    except Exception:
        pass
    brand = (os.getenv("APP_BRAND_NAME", BRAND_NAME) or BRAND_NAME).strip()
    username = (os.getenv("GMAIL_USERNAME", GMAIL_USERNAME) or "").strip()
    app_password = (os.getenv("GMAIL_APP_PASSWORD", GMAIL_APP_PASSWORD) or "").strip()
    from_email = (os.getenv("GMAIL_FROM_EMAIL", username) or username).strip()
    from_name = (os.getenv("GMAIL_FROM_NAME", brand) or brand).strip()
    smtp_host = (os.getenv("GMAIL_SMTP_HOST", GMAIL_SMTP_HOST) or GMAIL_SMTP_HOST).strip()
    try:
        smtp_port = int(os.getenv("GMAIL_SMTP_PORT", str(GMAIL_SMTP_PORT)) or GMAIL_SMTP_PORT)
    except Exception:
        smtp_port = GMAIL_SMTP_PORT
    return {
        "brand": brand,
        "username": username,
        "app_password": app_password,
        "from_email": from_email,
        "from_name": from_name,
        "smtp_host": smtp_host,
        "smtp_port": smtp_port,
    }


def _gmail_runtime_status(conn: Optional[sqlite3.Connection] = None) -> dict[str, Any]:
    close_conn = False
    work_conn = conn
    if work_conn is None:
        work_conn = get_conn()
        close_conn = True
    try:
        oauth_row = _gmail_oauth_refresh_if_needed(work_conn)
        oauth_ready = bool(
            _gmail_oauth_ready()
            and oauth_row.get("connected")
            and str(oauth_row.get("access_token") or "").strip()
        )
        smtp_cfg = _gmail_cfg()
        smtp_ready = bool(smtp_cfg["username"] and smtp_cfg["app_password"] and smtp_cfg["from_email"])
        mode = "oauth" if oauth_ready else ("smtp" if smtp_ready else "none")
        return {
            "ready": bool(oauth_ready or smtp_ready),
            "mode": mode,
            "oauth_ready": bool(oauth_ready),
            "oauth_connected": bool(oauth_row.get("connected")),
            "oauth_email": str(oauth_row.get("email") or ""),
            "oauth_error": str(oauth_row.get("error") or ""),
            "smtp_ready": bool(smtp_ready),
            "smtp_from": smtp_cfg["from_email"],
            "smtp_host": smtp_cfg["smtp_host"],
            "smtp_port": smtp_cfg["smtp_port"],
        }
    finally:
        if close_conn:
            work_conn.close()


def _gmail_ready() -> bool:
    return bool(_gmail_runtime_status().get("ready"))


def _send_gmail_oauth(conn: sqlite3.Connection, to_email: str, subject: str, html_body: str, text_body: str = "") -> bool:
    row = _gmail_oauth_refresh_if_needed(conn)
    token = str(row.get("access_token") or "").strip()
    if not token:
        return False
    sender = str(row.get("email") or "").strip() or _gmail_cfg().get("from_email") or GMAIL_FROM_EMAIL
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{GMAIL_FROM_NAME} <{sender}>"
    msg["To"] = to_email
    msg.set_content(text_body or subject)
    msg.add_alternative(html_body, subtype="html")
    try:
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode().rstrip("=")
        resp = requests.post(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"raw": raw},
            timeout=25,
        )
        if resp.ok:
            if row.get("error"):
                row["error"] = ""
                row["updated_at"] = _utc_iso()
                _gmail_oauth_save(conn, row)
                conn.commit()
            return True
        body = {}
        try:
            body = resp.json() if resp.content else {}
        except Exception:
            body = {}
        row["error"] = str((body.get("error") or {}).get("message") or f"Gmail API send failed ({resp.status_code})")[:500]
        row["updated_at"] = _utc_iso()
        _gmail_oauth_save(conn, row)
        conn.commit()
        return False
    except Exception as exc:
        row["error"] = str(exc)[:500]
        row["updated_at"] = _utc_iso()
        _gmail_oauth_save(conn, row)
        conn.commit()
        return False


def _send_gmail(to_email: str, subject: str, html_body: str, text_body: str = "") -> bool:
    if not to_email:
        return False
    conn = get_conn()
    try:
        status = _gmail_runtime_status(conn)
        if status.get("oauth_ready"):
            return _send_gmail_oauth(conn, to_email, subject, html_body, text_body)
    except Exception as exc:
        logger.warning("gmail oauth send failed, fallback smtp: %s", exc)
    finally:
        conn.close()
    cfg = _gmail_cfg()
    if not (cfg["username"] and cfg["app_password"] and cfg["from_email"]):
        return False
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{cfg['from_name']} <{cfg['from_email']}>"
    msg["To"] = to_email
    msg.set_content(text_body or subject)
    msg.add_alternative(html_body, subtype="html")
    try:
        if int(cfg["smtp_port"]) == 465:
            with smtplib.SMTP_SSL(cfg["smtp_host"], int(cfg["smtp_port"]), context=ssl.create_default_context(), timeout=20) as smtp:
                smtp.login(cfg["username"], cfg["app_password"])
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(cfg["smtp_host"], int(cfg["smtp_port"]), timeout=20) as smtp:
                smtp.starttls(context=ssl.create_default_context())
                smtp.login(cfg["username"], cfg["app_password"])
                smtp.send_message(msg)
        return True
    except Exception as exc:
        logger.warning("gmail send failed: %s", exc)
        return False


def _send_gmail_async(to_email: str, subject: str, html_body: str, text_body: str = "") -> None:
    if not _gmail_ready() or not to_email:
        return
    threading.Thread(
        target=_send_gmail,
        args=(to_email, subject, html_body, text_body),
        daemon=True,
    ).start()


def _send_welcome_email(user_email: str, full_name: str) -> None:
    if not GMAIL_NOTIFY_ON_SIGNUP:
        return
    subject = f"Welcome to {BRAND_NAME}"
    html = f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;background:#07111d;color:#e5eefc;padding:24px">
      <div style="max-width:640px;margin:auto;background:#0d1b31;border:1px solid #1f3657;border-radius:20px;padding:28px">
        <div style="font-size:12px;letter-spacing:.24em;text-transform:uppercase;color:#67e8f9">{BRAND_NAME}</div>
        <h1 style="margin:14px 0 10px;font-size:28px;color:#f8fbff">Welcome, {full_name or "Trader"}</h1>
        <p style="line-height:1.7;color:#b8cae6">Your account is ready. Strategy controls, wallet, routed signals, and the admin-aware control hub are now available inside {BRAND_NAME}.</p>
        <div style="margin:18px 0;padding:16px;border-radius:16px;background:#112440;border:1px solid #24456d">
          <div style="font-weight:700;color:#ffd88f">Getting started</div>
          <div style="margin-top:8px;color:#d7e7ff;line-height:1.7">Open Nexus inside the app, enable strategies, redeem credits if needed, and watch routed signal ideas arrive in your inbox.</div>
        </div>
        <p style="margin:0;color:#94a8c6">This message was sent from Gmail integration configured for {BRAND_NAME}.</p>
      </div>
    </body></html>
    """
    text = f"Welcome to {BRAND_NAME}. Your account is ready and Nexus is available inside the app."
    _send_gmail_async(user_email, subject, html, text)


def _send_managed_password_email(user_email: str, full_name: str, temporary_password: str, role: str = "USER") -> bool:
    subject = f"{BRAND_NAME} desk access is ready"
    html_body = f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;background:#07111d;color:#e5eefc;padding:24px">
      <div style="max-width:700px;margin:auto;background:#0d1b31;border:1px solid #1f3657;border-radius:22px;padding:30px">
        <div style="font-size:12px;letter-spacing:.24em;text-transform:uppercase;color:#67e8f9">{html.escape(BRAND_NAME)}</div>
        <h1 style="margin:14px 0 10px;font-size:30px;color:#f8fbff">Your desk is ready</h1>
        <p style="line-height:1.7;color:#b8cae6">Hi {html.escape(full_name or "Trader")}, your account has been provisioned by the admin desk.</p>
        <div style="margin:18px 0;padding:18px;border-radius:18px;background:#112440;border:1px solid #24456d;color:#d7e7ff;line-height:1.9">
          Login email: <b>{html.escape(user_email)}</b><br>
          Temporary password: <b>{html.escape(temporary_password)}</b><br>
          Role: <b>{html.escape(role)}</b>
        </div>
        <div style="margin:18px 0;padding:16px;border-radius:16px;background:rgba(103,232,249,.08);border:1px solid rgba(103,232,249,.2)">
          <div style="font-weight:700;color:#9deeff">Next steps</div>
          <div style="margin-top:8px;color:#d7e7ff;line-height:1.7">Sign in, change your password, review wallet access, and confirm your notification channels inside Nexus.</div>
        </div>
      </div>
    </body></html>
    """
    text_body = f"{BRAND_NAME} desk access is ready. Email: {user_email}. Temporary password: {temporary_password}. Role: {role}. Please change the password after first login."
    return _send_gmail(user_email, subject, html_body, text_body)


def _send_user_account_summary_email(user: dict[str, Any], *, headline: str, intro: str, wallet_note: str = "", extra_lines: Optional[list[str]] = None) -> bool:
    email = str(user.get("email") or "").strip()
    if not email:
        return False
    contacts = user.get("contacts") or {}
    wallet = user.get("wallet") or {}
    controls = user.get("controls") or {}
    notifications = user.get("notifications") or {}
    lines = [
        f"Role: {user.get('role') or 'USER'}",
        f"Status: {user.get('status') or 'ACTIVE'}",
        f"Wallet type: {wallet.get('type') or 'COUPON'}",
        f"Wallet balance: Rs {float(wallet.get('balance') or 0):,.2f}",
        f"Daily loss limit: Rs {float(controls.get('daily_loss_limit') or 0):,.2f}",
        f"Max trades / day: {int(controls.get('max_trades_per_day') or 0)}",
        f"Max open signals: {int(controls.get('max_open_signals') or 0)}",
        "Notifications: "
        + ", ".join(
            [name for enabled, name in [
                (notifications.get("email"), "Email"),
                (notifications.get("telegram"), "Telegram"),
                (notifications.get("whatsapp"), "WhatsApp"),
                (notifications.get("token_reminder"), "Token reminders"),
            ] if enabled]
        ) if any([notifications.get("email"), notifications.get("telegram"), notifications.get("whatsapp"), notifications.get("token_reminder")]) else "Notifications: none",
        f"WhatsApp: {contacts.get('whatsapp_phone') or 'Not set'}",
        f"Telegram: {contacts.get('telegram_chat_id') or 'Not set'}",
    ]
    if wallet_note:
        lines.append(wallet_note)
    for line in (extra_lines or []):
        if line:
            lines.append(str(line))
    html_lines = "".join(f"<li style='margin:0 0 8px'>{html.escape(str(line))}</li>" for line in lines)
    html_body = f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;background:#07111d;color:#e5eefc;padding:24px">
      <div style="max-width:760px;margin:auto;background:#0d1b31;border:1px solid #1f3657;border-radius:22px;padding:30px">
        <div style="font-size:12px;letter-spacing:.24em;text-transform:uppercase;color:#67e8f9">{html.escape(BRAND_NAME)}</div>
        <h1 style="margin:14px 0 10px;font-size:30px;color:#f8fbff">{html.escape(headline)}</h1>
        <p style="line-height:1.7;color:#b8cae6">Hi {html.escape(user.get('full_name') or 'Trader')}, {html.escape(intro)}</p>
        <div style="margin:18px 0;padding:18px;border-radius:18px;background:#112440;border:1px solid #24456d">
          <div style="font-weight:700;color:#9deeff;margin-bottom:10px">Your account details</div>
          <ul style="margin:0;padding-left:20px;color:#d7e7ff;line-height:1.75">{html_lines}</ul>
        </div>
        <p style="margin:0;color:#94a8c6">If anything looks incorrect, reply to the admin or support contact configured for {html.escape(BRAND_NAME)}.</p>
      </div>
    </body></html>
    """
    text_body = headline + "\n" + intro + "\n" + "\n".join(lines)
    return _send_gmail(email, f"{BRAND_NAME} account update", html_body, text_body)


def _admin_otp_generate() -> str:
    return f"{secrets.randbelow(1000000):06d}"


def _send_admin_otp_email(to_email: str, otp_code: str) -> bool:
    subject = f"{BRAND_NAME} admin login OTP"
    ttl = int(ADMIN_OTP_TTL_MINUTES)
    html_body = f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;background:#07111d;color:#e5eefc;padding:24px">
      <div style="max-width:640px;margin:auto;background:#0d1b31;border:1px solid #1f3657;border-radius:20px;padding:28px">
        <div style="font-size:12px;letter-spacing:.24em;text-transform:uppercase;color:#67e8f9">{html.escape(BRAND_NAME)}</div>
        <h1 style="margin:14px 0 8px;font-size:26px;color:#f8fbff">Admin one-time code</h1>
        <p style="line-height:1.7;color:#b8cae6">Use this OTP to finish admin sign-in. It expires in <b>{ttl} minutes</b>.</p>
        <div style="font-size:36px;letter-spacing:.35em;font-weight:800;color:#7ee2ff;margin:18px 0">{html.escape(otp_code)}</div>
        <p style="font-size:13px;color:#9db4d8">If you did not request this, ignore this email.</p>
      </div>
    </body></html>
    """
    text_body = f"{BRAND_NAME} admin OTP: {otp_code}. Valid for {ttl} minutes."
    return _send_gmail(to_email, subject, html_body, text_body)


def _send_admin_otp_phone(otp_code: str) -> bool:
    phone = _normalize_whatsapp_phone(os.getenv("SAAS_ADMIN_OTP_PHONE", ADMIN_OTP_PHONE))
    apikey = (os.getenv("SAAS_ADMIN_OTP_WHATSAPP_APIKEY", ADMIN_OTP_WHATSAPP_APIKEY) or "").strip()
    if not phone or not apikey:
        return False
    text = f"{BRAND_NAME} admin OTP: {otp_code}. Valid for {int(ADMIN_OTP_TTL_MINUTES)} minutes."
    return _send_whatsapp_to_phone(phone, apikey, text)


def _send_payment_email(user_email: str, plan_name: str, amount: float, expires_at: str) -> None:
    if not GMAIL_NOTIFY_ON_PAYMENT:
        return
    subject = f"{BRAND_NAME} payment confirmed"
    html = f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;background:#07111d;color:#e5eefc;padding:24px">
      <div style="max-width:640px;margin:auto;background:#0d1b31;border:1px solid #1f3657;border-radius:20px;padding:28px">
        <div style="font-size:12px;letter-spacing:.24em;text-transform:uppercase;color:#67e8f9">{BRAND_NAME}</div>
        <h1 style="margin:14px 0 10px;font-size:28px;color:#f8fbff">Payment confirmed</h1>
        <p style="line-height:1.7;color:#b8cae6">Your <b>{plan_name}</b> access is active. Amount received: <b>Rs {amount:,.2f}</b>.</p>
        <div style="margin:18px 0;padding:16px;border-radius:16px;background:#112440;border:1px solid #24456d">
          <div style="font-weight:700;color:#ffd88f">Access details</div>
          <div style="margin-top:8px;color:#d7e7ff;line-height:1.7">Plan: {plan_name}<br>Valid until: {expires_at}</div>
        </div>
      </div>
    </body></html>
    """
    text = f"{BRAND_NAME} payment confirmed. Plan: {plan_name}. Valid until {expires_at}."
    _send_gmail_async(user_email, subject, html, text)


def _send_signal_email(user_email: str, headline: str, excerpt: str, strategy_code: str, confidence: float) -> None:
    if not GMAIL_NOTIFY_ON_SIGNAL:
        return
    subject = f"{BRAND_NAME} signal: {headline}"
    html = f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;background:#07111d;color:#e5eefc;padding:24px">
      <div style="max-width:640px;margin:auto;background:#0d1b31;border:1px solid #1f3657;border-radius:20px;padding:28px">
        <div style="font-size:12px;letter-spacing:.24em;text-transform:uppercase;color:#67e8f9">{BRAND_NAME}</div>
        <h1 style="margin:14px 0 10px;font-size:24px;color:#f8fbff">{headline}</h1>
        <p style="line-height:1.7;color:#b8cae6">{excerpt or "A new routed signal is available in your Nexus inbox."}</p>
        <div style="margin:18px 0;padding:16px;border-radius:16px;background:#112440;border:1px solid #24456d;color:#d7e7ff;line-height:1.7">
          Strategy: {strategy_code}<br>Confidence: {confidence:.2f}%
        </div>
      </div>
    </body></html>
    """
    text = f"{BRAND_NAME} signal: {headline}. Strategy {strategy_code}. Confidence {confidence:.2f}%."
    _send_gmail_async(user_email, subject, html, text)


def _send_admin_test_email(to_email: str) -> bool:
    cfg = _gmail_cfg()
    runtime = _gmail_runtime_status()
    mode = str(runtime.get("mode") or "none").upper()
    subject = f"{BRAND_NAME} Gmail test"
    html = f"""
    <html><body style="font-family:Segoe UI,Arial,sans-serif;background:#07111d;color:#e5eefc;padding:24px">
      <div style="max-width:640px;margin:auto;background:#0d1b31;border:1px solid #1f3657;border-radius:20px;padding:28px">
        <div style="font-size:12px;letter-spacing:.24em;text-transform:uppercase;color:#67e8f9">{BRAND_NAME}</div>
        <h1 style="margin:14px 0 10px;font-size:26px;color:#f8fbff">Mailer is live</h1>
        <p style="line-height:1.7;color:#b8cae6">This is a manual Gmail verification sent from the {BRAND_NAME} Nexus admin deck.</p>
        <div style="margin:18px 0;padding:16px;border-radius:16px;background:#112440;border:1px solid #24456d;color:#d7e7ff;line-height:1.7">
          Delivery mode: {html.escape(mode)}<br>SMTP host: {cfg["smtp_host"]}<br>Sender: {cfg["from_name"]} &lt;{cfg["from_email"]}&gt;
        </div>
      </div>
    </body></html>
    """
    text = f"{BRAND_NAME} Gmail test. Mode {mode}. Sender: {cfg['from_name']} <{cfg['from_email']}> via {cfg['smtp_host']}."
    return _send_gmail(to_email, subject, html, text)


def _send_telegram_to_chat(chat_id: str, message_html: str) -> bool:
    chat = str(chat_id or "").strip()
    if not chat:
        return False
    token = (os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
    if not token:
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": message_html, "parse_mode": "HTML"},
            timeout=12,
        )
        data = resp.json() if resp.content else {}
        return bool(resp.ok and data.get("ok", True))
    except Exception as exc:
        logger.warning("telegram user send failed: %s", exc)
        return False


def _send_whatsapp_to_phone(phone: str, apikey: str, text_message: str) -> bool:
    target_phone = _normalize_whatsapp_phone(phone)
    key = str(apikey or "").strip()
    if not target_phone or not key:
        return False
    try:
        import urllib.parse

        encoded = urllib.parse.quote(str(text_message or ""))
        resp = requests.get(
            f"https://api.callmebot.com/whatsapp.php?phone={target_phone}&text={encoded}&apikey={key}",
            timeout=12,
        )
        return bool(resp.ok)
    except Exception as exc:
        logger.warning("whatsapp user send failed: %s", exc)
        return False


def _dispatch_user_signal_notifications(user_row: sqlite3.Row, headline: str, excerpt: str, strategy_code: str, confidence: float) -> None:
    email_to = str(user_row["email"] or "").strip()
    telegram_id = _normalize_telegram_chat_id(user_row["telegram_chat_id"])
    whatsapp_phone = _normalize_whatsapp_phone(user_row["whatsapp_phone"])
    whatsapp_apikey = str(user_row["whatsapp_apikey"] or "").strip()
    if bool(user_row["notify_email"] or 0):
        _send_signal_email(email_to, headline, excerpt, strategy_code, confidence)
    if bool(user_row["notify_telegram"] or 0) and telegram_id:
        msg = (
            f"<b>{html.escape(BRAND_NAME)} signal call</b>\n"
            f"<b>{html.escape(headline)}</b>\n"
            f"{html.escape(excerpt or 'A new routed signal is available in your inbox.')}\n"
            f"Strategy: <b>{html.escape(strategy_code)}</b> · Confidence <b>{confidence:.2f}%</b>"
        )
        _send_telegram_to_chat(telegram_id, msg)
    if bool(user_row["notify_whatsapp"] or 0) and whatsapp_phone and whatsapp_apikey:
        msg = (
            f"{BRAND_NAME} signal call\n"
            f"{headline}\n"
            f"{_strip_html_text(excerpt or 'A new routed signal is available in your inbox.')}\n"
            f"Strategy: {strategy_code} | Confidence: {confidence:.2f}%"
        )
        _send_whatsapp_to_phone(whatsapp_phone, whatsapp_apikey, msg)


def send_token_validation_reminders() -> dict[str, int]:
    init_saas_db()
    today = _today_ist().isoformat()
    conn = get_conn()
    sent = skipped = 0
    try:
        rows = conn.execute(
            """
            SELECT u.*, b.broker_code, b.status AS broker_status, b.enabled AS broker_enabled, b.account_label
            FROM saas_users u
            LEFT JOIN saas_broker_accounts b ON b.user_id=u.id
            WHERE u.role='USER' AND u.status IN ('ACTIVE','LIMITED')
            ORDER BY u.id ASC
            """
        ).fetchall()
        for row in rows:
            if not bool(row["notify_token_reminder"] or 0):
                skipped += 1
                continue
            if str(row["last_token_reminder_at"] or "")[:10] == today:
                skipped += 1
                continue
            if not bool(row["broker_enabled"] or 0):
                skipped += 1
                continue
            broker_code = str(row["broker_code"] or "").upper()
            if broker_code in {"", "PAPER"}:
                skipped += 1
                continue
            account_label = str(row["account_label"] or broker_code or "broker").strip()
            reminder_html = (
                f"<b>{html.escape(BRAND_NAME)} broker token reminder</b>\n"
                f"Please validate your <b>{html.escape(account_label)}</b> token before <b>8:00 AM IST</b> so auto routing stays ready for today."
            )
            reminder_text = (
                f"{BRAND_NAME} broker token reminder\n"
                f"Please validate your {account_label} token before 8:00 AM IST so auto routing stays ready for today."
            )
            channel_ok = False
            if bool(row["notify_email"] or 0) and str(row["email"] or "").strip():
                subject = f"{BRAND_NAME} broker token reminder"
                html_body = (
                    "<html><body style=\"font-family:Segoe UI,Arial,sans-serif;background:#07111d;color:#e5eefc;padding:24px\">"
                    f"<div style=\"max-width:640px;margin:auto;background:#0d1b31;border:1px solid #1f3657;border-radius:20px;padding:28px\">"
                    f"<div style=\"font-size:12px;letter-spacing:.24em;text-transform:uppercase;color:#67e8f9\">{html.escape(BRAND_NAME)}</div>"
                    f"<h1 style=\"margin:14px 0 10px;font-size:26px;color:#f8fbff\">Broker token validation</h1>"
                    f"<p style=\"line-height:1.7;color:#b8cae6\">Please validate your <b>{html.escape(account_label)}</b> token before <b>8:00 AM IST</b> so automated routing remains ready for today.</p>"
                    "</div></body></html>"
                )
                channel_ok = _send_gmail(str(row["email"] or "").strip(), subject, html_body, reminder_text) or channel_ok
            if bool(row["notify_telegram"] or 0):
                channel_ok = _send_telegram_to_chat(_normalize_telegram_chat_id(row["telegram_chat_id"]), reminder_html) or channel_ok
            if bool(row["notify_whatsapp"] or 0):
                channel_ok = _send_whatsapp_to_phone(row["whatsapp_phone"], row["whatsapp_apikey"], reminder_text) or channel_ok
            if channel_ok:
                conn.execute("UPDATE saas_users SET last_token_reminder_at=?, updated_at=? WHERE id=?", (today, _utc_iso(), int(row["id"])))
                sent += 1
            else:
                skipped += 1
        conn.commit()
        return {"sent": sent, "skipped": skipped}
    finally:
        conn.close()


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _b64d(data: str) -> bytes:
    return base64.urlsafe_b64decode(data + ("=" * (-len(data) % 4)))


def _hash_password(password: str, salt: Optional[str] = None) -> tuple[str, str]:
    salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 120000)
    return base64.b64encode(digest).decode(), salt


def _verify_password(password: str, stored_hash: str, salt: str) -> bool:
    fresh, _ = _hash_password(password, salt)
    return hmac.compare_digest(fresh, stored_hash)


def _token_for(payload: dict[str, Any]) -> str:
    header = _b64(_dumps({"alg": "HS256", "typ": "JWT"}).encode())
    body = _b64(_dumps(payload).encode())
    sig = hmac.new(TOKEN_SECRET.encode(), f"{header}.{body}".encode(), hashlib.sha256).digest()
    return f"{header}.{body}.{_b64(sig)}"


def _decode_token(token: str) -> dict[str, Any]:
    try:
        head, body, sig = token.split(".", 2)
        expected = hmac.new(TOKEN_SECRET.encode(), f"{head}.{body}".encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(expected, _b64d(sig)):
            raise ValueError("bad signature")
        payload = json.loads(_b64d(body).decode())
        if int(payload.get("exp") or 0) < int(_utc_now().timestamp()):
            raise ValueError("expired")
        return payload
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Invalid token") from exc


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_saas_db() -> None:
    conn = get_conn()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS saas_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                full_name TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                password_salt TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'USER',
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                wallet_type TEXT NOT NULL DEFAULT 'COUPON',
                whatsapp_phone TEXT DEFAULT '',
                whatsapp_apikey TEXT DEFAULT '',
                telegram_chat_id TEXT DEFAULT '',
                notify_email INTEGER NOT NULL DEFAULT 1,
                notify_telegram INTEGER NOT NULL DEFAULT 1,
                notify_whatsapp INTEGER NOT NULL DEFAULT 0,
                notify_token_reminder INTEGER NOT NULL DEFAULT 1,
                daily_loss_limit REAL NOT NULL DEFAULT 2500,
                max_trades_per_day INTEGER NOT NULL DEFAULT 6,
                max_open_signals INTEGER NOT NULL DEFAULT 3,
                profit_share_pct REAL NOT NULL DEFAULT 25,
                coupon_profit_cap REAL NOT NULL DEFAULT 2000,
                auto_execute INTEGER NOT NULL DEFAULT 0,
                notes TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_login_at TEXT,
                last_token_reminder_at TEXT
            );
            CREATE TABLE IF NOT EXISTS saas_wallets (
                user_id INTEGER PRIMARY KEY,
                balance REAL NOT NULL DEFAULT 0,
                reserved_balance REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                wallet_type TEXT NOT NULL DEFAULT 'COUPON',
                realized_profit REAL NOT NULL DEFAULT 0,
                total_fees REAL NOT NULL DEFAULT 0,
                coupon_code TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES saas_users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS saas_wallet_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                amount REAL NOT NULL,
                reference_type TEXT,
                reference_id TEXT,
                note TEXT,
                meta_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES saas_users(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_wallet_ledger_user_created ON saas_wallet_ledger(user_id, created_at DESC);
            CREATE TABLE IF NOT EXISTS saas_strategies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                strategy_type TEXT NOT NULL,
                description TEXT DEFAULT '',
                active INTEGER NOT NULL DEFAULT 1,
                theme TEXT DEFAULT 'default',
                accent TEXT DEFAULT '#38bdf8',
                default_confidence REAL NOT NULL DEFAULT 70,
                default_max_trades INTEGER NOT NULL DEFAULT 5,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS saas_user_strategies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                strategy_code TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                min_confidence REAL NOT NULL DEFAULT 70,
                max_trades_per_day INTEGER NOT NULL DEFAULT 5,
                risk_level TEXT NOT NULL DEFAULT 'MEDIUM',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(user_id, strategy_code),
                FOREIGN KEY(user_id) REFERENCES saas_users(id) ON DELETE CASCADE,
                FOREIGN KEY(strategy_code) REFERENCES saas_strategies(code) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS saas_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                plan_code TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                amount REAL NOT NULL DEFAULT 0,
                source TEXT NOT NULL DEFAULT 'SYSTEM',
                started_at TEXT NOT NULL,
                expires_at TEXT,
                meta_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES saas_users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS saas_coupons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                credit REAL NOT NULL DEFAULT 0,
                max_profit REAL NOT NULL DEFAULT 2000,
                active INTEGER NOT NULL DEFAULT 1,
                usage_limit INTEGER NOT NULL DEFAULT 1,
                used_count INTEGER NOT NULL DEFAULT 0,
                expires_at TEXT,
                strategy_bundle TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                notes TEXT DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS saas_coupon_redemptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coupon_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                redeemed_at TEXT NOT NULL,
                credit REAL NOT NULL DEFAULT 0,
                UNIQUE(coupon_id, user_id),
                FOREIGN KEY(coupon_id) REFERENCES saas_coupons(id) ON DELETE CASCADE,
                FOREIGN KEY(user_id) REFERENCES saas_users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS saas_payment_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                provider TEXT NOT NULL DEFAULT 'LOCAL',
                provider_order_id TEXT,
                amount REAL NOT NULL,
                currency TEXT NOT NULL DEFAULT 'INR',
                status TEXT NOT NULL DEFAULT 'CREATED',
                plan_code TEXT,
                qr_payload TEXT,
                checkout_url TEXT,
                meta_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES saas_users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS saas_signal_inbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                strategy_code TEXT NOT NULL,
                signal_key TEXT NOT NULL,
                headline TEXT NOT NULL,
                excerpt TEXT DEFAULT '',
                confidence REAL NOT NULL DEFAULT 0,
                read_at TEXT,
                status TEXT NOT NULL DEFAULT 'NEW',
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(user_id, strategy_code, signal_key),
                FOREIGN KEY(user_id) REFERENCES saas_users(id) ON DELETE CASCADE,
                FOREIGN KEY(strategy_code) REFERENCES saas_strategies(code) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS saas_trade_journal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                strategy_code TEXT NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry_price REAL NOT NULL DEFAULT 0,
                exit_price REAL,
                quantity REAL NOT NULL DEFAULT 1,
                pnl REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'OPEN',
                source_signal_key TEXT,
                opened_at TEXT NOT NULL,
                closed_at TEXT,
                fee_amount REAL NOT NULL DEFAULT 0,
                notes TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES saas_users(id) ON DELETE CASCADE,
                FOREIGN KEY(strategy_code) REFERENCES saas_strategies(code) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS saas_broker_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL UNIQUE,
                broker_code TEXT NOT NULL DEFAULT 'PAPER',
                account_label TEXT DEFAULT '',
                broker_user_id TEXT DEFAULT '',
                api_key TEXT DEFAULT '',
                api_secret_enc TEXT DEFAULT '',
                access_token_enc TEXT DEFAULT '',
                refresh_token_enc TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'DISCONNECTED',
                enabled INTEGER NOT NULL DEFAULT 0,
                paper_mode INTEGER NOT NULL DEFAULT 1,
                live_mode INTEGER NOT NULL DEFAULT 0,
                default_quantity INTEGER NOT NULL DEFAULT 1,
                intraday_product TEXT NOT NULL DEFAULT 'MIS',
                positional_product TEXT NOT NULL DEFAULT 'CNC',
                order_variety TEXT NOT NULL DEFAULT 'regular',
                last_error TEXT DEFAULT '',
                profile_json TEXT,
                capabilities_json TEXT,
                connected_at TEXT,
                last_checked_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES saas_users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS saas_broker_order_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                broker_account_id INTEGER,
                strategy_code TEXT NOT NULL,
                signal_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                tradingsymbol TEXT NOT NULL,
                exchange TEXT NOT NULL DEFAULT 'NSE',
                transaction_type TEXT NOT NULL DEFAULT 'BUY',
                product TEXT NOT NULL DEFAULT 'MIS',
                order_type TEXT NOT NULL DEFAULT 'LIMIT',
                quantity INTEGER NOT NULL DEFAULT 1,
                requested_price REAL,
                trigger_price REAL,
                broker_order_id TEXT,
                broker_status TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'CREATED',
                live_mode INTEGER NOT NULL DEFAULT 0,
                request_json TEXT,
                response_json TEXT,
                error_text TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(user_id, signal_key, strategy_code),
                FOREIGN KEY(user_id) REFERENCES saas_users(id) ON DELETE CASCADE,
                FOREIGN KEY(broker_account_id) REFERENCES saas_broker_accounts(id) ON DELETE SET NULL
            );
            CREATE INDEX IF NOT EXISTS idx_broker_orders_user_created ON saas_broker_order_log(user_id, created_at DESC);
            CREATE TABLE IF NOT EXISTS saas_app_settings (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS saas_admin_otp (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                email TEXT NOT NULL,
                otp_code TEXT NOT NULL,
                purpose TEXT NOT NULL DEFAULT 'ADMIN_LOGIN',
                expires_at TEXT NOT NULL,
                used_at TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES saas_users(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_admin_otp_email_created ON saas_admin_otp(email, created_at DESC);
            CREATE TABLE IF NOT EXISTS saas_auth_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                last_seen_at TEXT,
                FOREIGN KEY(user_id) REFERENCES saas_users(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_saas_auth_sessions_token_hash ON saas_auth_sessions(token_hash);
            CREATE INDEX IF NOT EXISTS idx_saas_auth_sessions_expires ON saas_auth_sessions(expires_at);
            """
        )
        have = {str(r["name"] or "") for r in conn.execute("PRAGMA table_info(saas_users)").fetchall()}
        missing_user_cols = {
            "whatsapp_phone": "TEXT DEFAULT ''",
            "whatsapp_apikey": "TEXT DEFAULT ''",
            "telegram_chat_id": "TEXT DEFAULT ''",
            "notify_email": "INTEGER NOT NULL DEFAULT 1",
            "notify_telegram": "INTEGER NOT NULL DEFAULT 1",
            "notify_whatsapp": "INTEGER NOT NULL DEFAULT 0",
            "notify_token_reminder": "INTEGER NOT NULL DEFAULT 1",
            "last_token_reminder_at": "TEXT",
        }
        for name, spec in missing_user_cols.items():
            if name not in have:
                conn.execute(f"ALTER TABLE saas_users ADD COLUMN {name} {spec}")
        conn.commit()
    finally:
        conn.close()
    seed_defaults()


def seed_defaults() -> None:
    conn = get_conn()
    try:
        now = _utc_iso()
        for strat in DEFAULT_STRATEGIES:
            conn.execute(
                "INSERT INTO saas_strategies(code,name,strategy_type,description,active,theme,accent,default_confidence,default_max_trades,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(code) DO UPDATE SET name=excluded.name,strategy_type=excluded.strategy_type,description=excluded.description,active=excluded.active,theme=excluded.theme,accent=excluded.accent,default_confidence=excluded.default_confidence,default_max_trades=excluded.default_max_trades,updated_at=excluded.updated_at",
                (strat["code"], strat["name"], strat["strategy_type"], strat["description"], strat["active"], strat["theme"], strat["accent"], strat["default_confidence"], strat["default_max_trades"], now, now),
            )
        admin_row = conn.execute("SELECT id, email FROM saas_users WHERE role='ADMIN' ORDER BY id ASC LIMIT 1").fetchone()
        if admin_row is None:
            pw_hash, salt = _hash_password(DEFAULT_ADMIN_PASSWORD)
            cur = conn.execute("INSERT INTO saas_users(email,full_name,password_hash,password_salt,role,status,wallet_type,coupon_profit_cap,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)", (DEFAULT_ADMIN_EMAIL, f"{BRAND_NAME} Admin", pw_hash, salt, "ADMIN", "ACTIVE", "PAID", COUPON_PROFIT_CAP, now, now))
            uid = int(cur.lastrowid)
            conn.execute("INSERT INTO saas_wallets(user_id,balance,reserved_balance,status,wallet_type,realized_profit,total_fees,updated_at) VALUES(?,?,?,?,?,?,?,?)", (uid, 0, 0, "ACTIVE", "PAID", 0, 0, now))
            _ensure_broker_row(conn, uid)
            conn.execute("INSERT OR REPLACE INTO saas_app_settings(key,value_json,updated_at) VALUES(?,?,?)", ("bootstrap_admin", _dumps({"email": DEFAULT_ADMIN_EMAIL, "password": DEFAULT_ADMIN_PASSWORD}), now))
            for strat in DEFAULT_STRATEGIES:
                conn.execute("INSERT INTO saas_user_strategies(user_id,strategy_code,enabled,min_confidence,max_trades_per_day,risk_level,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)", (uid, strat["code"], 1, strat["default_confidence"], strat["default_max_trades"], "HIGH", now, now))
        else:
            current_admin_email = str(admin_row["email"] or "").strip().lower()
            env_admin_email = str(os.getenv("SAAS_ADMIN_EMAIL", "") or "").strip().lower()
            if current_admin_email in {"admin@nseedge.local", "admin@stockr.in"} or (env_admin_email and current_admin_email != DEFAULT_ADMIN_EMAIL):
                conn.execute("UPDATE saas_users SET email=?, full_name=?, updated_at=? WHERE id=?", (DEFAULT_ADMIN_EMAIL, f"{BRAND_NAME} Admin", now, int(admin_row["id"])))
            _ensure_broker_row(conn, int(admin_row["id"]))
        conn.execute("INSERT OR REPLACE INTO saas_app_settings(key,value_json,updated_at) VALUES(?,?,?)", ("bootstrap_admin", _dumps({"email": DEFAULT_ADMIN_EMAIL, "password": DEFAULT_ADMIN_PASSWORD}), now))
        conn.execute("INSERT OR REPLACE INTO saas_app_settings(key,value_json,updated_at) VALUES(?,?,?)", ("brand", _dumps({"name": BRAND_NAME}), now))
        gmail_status = _gmail_runtime_status(conn)
        conn.execute(
            "INSERT OR REPLACE INTO saas_app_settings(key,value_json,updated_at) VALUES(?,?,?)",
            (
                "gmail",
                _dumps(
                    {
                        "ready": bool(gmail_status.get("ready")),
                        "mode": gmail_status.get("mode"),
                        "oauth_connected": bool(gmail_status.get("oauth_connected")),
                        "oauth_email": gmail_status.get("oauth_email"),
                        "from": gmail_status.get("smtp_from") or GMAIL_FROM_EMAIL,
                        "host": gmail_status.get("smtp_host") or GMAIL_SMTP_HOST,
                        "port": gmail_status.get("smtp_port") or GMAIL_SMTP_PORT,
                    }
                ),
                now,
            ),
        )
        conn.execute("INSERT OR IGNORE INTO saas_app_settings(key,value_json,updated_at) VALUES(?,?,?)", ("payment_profile", _dumps(_default_payment_profile()), now))
        conn.execute("INSERT OR IGNORE INTO saas_coupons(code,credit,max_profit,active,usage_limit,used_count,expires_at,strategy_bundle,created_at,updated_at,notes) VALUES(?,?,?,?,?,?,?,?,?,?,?)", ("WELCOME500", 500, COUPON_PROFIT_CAP, 1, 9999, 0, None, "SPIKE,INDEX,SWING", now, now, "Default onboarding coupon"))
        conn.commit()
    finally:
        conn.close()

def _user_row(conn: sqlite3.Connection, user_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT u.*, w.balance, w.reserved_balance, w.status AS wallet_status, w.wallet_type AS wallet_kind, w.realized_profit, w.total_fees, w.coupon_code FROM saas_users u LEFT JOIN saas_wallets w ON w.user_id=u.id WHERE u.id=?", (user_id,)).fetchone()


def _latest_subscription(conn: sqlite3.Connection, user_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM saas_subscriptions WHERE user_id=? ORDER BY id DESC LIMIT 1", (user_id,)).fetchone()


def _strategy_rows(conn: sqlite3.Connection, user_id: int) -> list[sqlite3.Row]:
    return conn.execute("SELECT s.code,s.name,s.strategy_type,s.description,s.active,s.theme,s.accent,us.enabled,us.min_confidence,us.max_trades_per_day,us.risk_level FROM saas_strategies s LEFT JOIN saas_user_strategies us ON us.strategy_code=s.code AND us.user_id=? ORDER BY s.id ASC", (user_id,)).fetchall()


def _ensure_user_strategy_rows(conn: sqlite3.Connection, user_id: int) -> None:
    now = _utc_iso()
    for row in conn.execute("SELECT code, default_confidence, default_max_trades FROM saas_strategies").fetchall():
        conn.execute("INSERT OR IGNORE INTO saas_user_strategies(user_id,strategy_code,enabled,min_confidence,max_trades_per_day,risk_level,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)", (user_id, row["code"], 1, float(row["default_confidence"] or 70), int(row["default_max_trades"] or 5), "MEDIUM", now, now))
    conn.commit()


def _ensure_broker_row(conn: sqlite3.Connection, user_id: int) -> None:
    now = _utc_iso()
    conn.execute(
        """
        INSERT OR IGNORE INTO saas_broker_accounts(
            user_id,broker_code,account_label,broker_user_id,api_key,api_secret_enc,access_token_enc,refresh_token_enc,
            status,enabled,paper_mode,live_mode,default_quantity,intraday_product,positional_product,order_variety,
            last_error,profile_json,capabilities_json,connected_at,last_checked_at,created_at,updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            user_id, "PAPER", "Paper Router", "", "", "", "", "",
            "READY", 0, 1, 0, 1, "MIS", "CNC", "regular",
            "", _dumps({"mode": "paper"}), _dumps({"supports_live": False, "supports_paper": True, "supports_auto": True}),
            now, now, now, now,
        ),
    )
    conn.commit()


def _broker_row(conn: sqlite3.Connection, user_id: int) -> sqlite3.Row | None:
    _ensure_broker_row(conn, user_id)
    return conn.execute("SELECT * FROM saas_broker_accounts WHERE user_id=?", (user_id,)).fetchone()


def _row_int_bool(col: Any) -> bool:
    try:
        return int(col or 0) != 0
    except (TypeError, ValueError):
        s = str(col or "").strip().lower()
        return s in ("1", "true", "yes", "on")


def _body_mode_int(body: dict[str, Any], key: str, row_col: Any) -> int:
    """1/0 from JSON when key is present, else from DB (SQLite may return int or str)."""
    if key in body:
        v = body.get(key)
        if v is True:
            return 1
        if v is False or v is None:
            return 0
        try:
            return 1 if int(v) != 0 else 0
        except (TypeError, ValueError):
            s = str(v).strip().lower()
            return 1 if s in ("1", "true", "yes", "on") else 0
    return 1 if _row_int_bool(row_col) else 0


def _broker_effective_live(broker: Any) -> bool:
    """True only when this desk may send real exchange orders (live on and paper route off)."""
    if str(broker["broker_code"] or "").upper() == "PAPER":
        return False
    return _row_int_bool(broker["live_mode"]) and not _row_int_bool(broker["paper_mode"])


def _parse_kite_request_token(raw: str) -> str:
    """Accept full redirect URL, query string, or raw request_token (same idea as generate_token.py)."""
    s = str(raw or "").strip().strip('"').strip("'")
    if not s:
        return ""
    if "request_token=" not in s:
        return s
    url = s if "://" in s else f"https://127.0.0.1/{s.lstrip('?&')}"
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    toks = params.get("request_token", [])
    if not toks and parsed.fragment:
        toks = parse_qs(parsed.fragment).get("request_token", [])
    return toks[0] if toks else s


def _broker_login_url(account: sqlite3.Row | dict[str, Any] | None) -> Optional[str]:
    if not account:
        return None
    broker_code = str((account["broker_code"] if isinstance(account, sqlite3.Row) else account.get("broker_code")) or "").upper()
    api_key = str((account["api_key"] if isinstance(account, sqlite3.Row) else account.get("api_key")) or "").strip()
    if broker_code != "ZERODHA" or not api_key:
        return None
    try:
        from kiteconnect import KiteConnect

        kite = KiteConnect(api_key=api_key)
        return str(kite.login_url() or "")
    except Exception:
        return None


def _set_kite_interactive_session(user_id: int, **fields: Any) -> dict[str, Any]:
    with _kite_interactive_lock:
        session = dict(_kite_interactive_sessions.get(int(user_id), {}))
        session.update(fields)
        session["user_id"] = int(user_id)
        session["updated_at"] = _utc_iso()
        _kite_interactive_sessions[int(user_id)] = session
        return dict(session)


def _get_kite_interactive_session(user_id: int) -> dict[str, Any]:
    with _kite_interactive_lock:
        return dict(_kite_interactive_sessions.get(int(user_id), {}))


def _public_kite_interactive_session(user_id: int) -> dict[str, Any]:
    raw = _get_kite_interactive_session(user_id)
    if not raw:
        return {"active": False, "status": "IDLE", "message": "No interactive Kite login in progress."}
    return {
        "active": bool(raw.get("active", False)),
        "status": str(raw.get("status") or "IDLE"),
        "message": str(raw.get("message") or ""),
        "detail": str(raw.get("detail") or ""),
        "session_id": str(raw.get("session_id") or ""),
        "started_at": raw.get("started_at"),
        "updated_at": raw.get("updated_at"),
        "broker_preview": raw.get("broker_preview") or None,
    }


def _find_windows_browser() -> tuple[str, str]:
    candidates = [
        ("Microsoft Edge", shutil.which("msedge.exe")),
        ("Google Chrome", shutil.which("chrome.exe")),
        ("Microsoft Edge", r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
        ("Microsoft Edge", r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
        ("Google Chrome", r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
        ("Google Chrome", r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
    ]
    for label, path in candidates:
        if path and os.path.isfile(path):
            return label, path
    raise RuntimeError("Could not find Edge or Chrome on this machine")


def _start_interactive_kite_login(user_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    session_id = secrets.token_urlsafe(10)
    api_key = str(payload.get("api_key") or "").strip()
    api_secret = str(payload.get("api_secret") or "").strip()
    existing = _get_kite_interactive_session(user_id)
    ex_st = str(existing.get("status") or "").upper()
    if existing.get("active") and ex_st in {"STARTING", "WAITING"}:
        login_dup = ""
        try:
            from kiteconnect import KiteConnect

            login_dup = str(KiteConnect(api_key=api_key).login_url() or "")
        except Exception:
            pass
        return _public_kite_interactive_session(user_id) | {"login_url": login_dup, "session_reused": True}
    if not api_key or not api_secret:
        raise HTTPException(
            status_code=400,
            detail=(
                "One-time Kite login needs your Kite Connect API key and secret. "
                "Either save them on this desk once (Advanced → API key/secret → Validate & Save), "
                "or set KITE_API_KEY and KITE_API_SECRET in backend/.env on the server."
            ),
        )
    login_url = ""
    try:
        from kiteconnect import KiteConnect

        login_url = str(KiteConnect(api_key=api_key).login_url() or "")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not build Kite login URL: {exc}") from exc

    broker_preview = {
        "account_label": str(payload.get("account_label") or "Zerodha Desk"),
        "broker_code": "ZERODHA",
        "api_key_masked": _mask_secret(api_key),
    }
    _set_kite_interactive_session(
        user_id,
        active=True,
        status="STARTING",
        message="Opening official Kite login window...",
        detail="Complete login in the Zerodha window. Nexus will attach the session automatically.",
        session_id=session_id,
        started_at=_utc_iso(),
        broker_preview=broker_preview,
    )

    def _worker() -> None:
        final_url = ""
        browser_proc = None
        profile_dir = None
        captured_urls: list[str] = []

        def _capture(url: str) -> None:
            nonlocal final_url
            text = str(url or "").strip()
            if not text or "request_token" not in text:
                return
            if text not in captured_urls:
                captured_urls.append(text)
            if not final_url:
                final_url = text

        try:
            browser_name, browser_path = _find_windows_browser()
            debug_port = secrets.randbelow(1000) + 9223
            profile_dir = tempfile.mkdtemp(prefix="stockr_kite_login_")
            _set_kite_interactive_session(
                user_id,
                status="WAITING",
                message=f"{browser_name} Kite window opened.",
                detail="Log in there once. You do not need to paste the redirect URL manually.",
            )
            browser_proc = subprocess.Popen(
                [
                    browser_path,
                    f"--remote-debugging-port={debug_port}",
                    f"--user-data-dir={profile_dir}",
                    "--no-first-run",
                    "--new-window",
                    login_url,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            debug_url = f"http://127.0.0.1:{debug_port}/json"
            deadline = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=KITE_INTERACTIVE_TIMEOUT_SECONDS)
            while dt.datetime.now(dt.timezone.utc) < deadline:
                if final_url:
                    break
                if browser_proc.poll() is not None:
                    break
                try:
                    resp = requests.get(debug_url, timeout=2)
                    if resp.ok:
                        for target in resp.json() or []:
                            _capture((target or {}).get("url") or "")
                            if final_url:
                                break
                except Exception:
                    pass
                time.sleep(1)

            if not final_url and captured_urls:
                final_url = captured_urls[0]
            if not final_url and browser_proc.poll() is not None:
                _set_kite_interactive_session(
                    user_id,
                    active=False,
                    status="CANCELLED",
                    message="Kite window was closed before login finished.",
                    detail="Open One-Time Kite Login again when you want to retry.",
                )
                return
            if not final_url:
                _set_kite_interactive_session(
                    user_id,
                    active=False,
                    status="TIMEOUT",
                    message="Kite login timed out.",
                    detail="Try again and finish the login in the opened Zerodha window.",
                )
                return
        except Exception as exc:
            logger.warning("interactive kite login failed to launch: %s", exc)
            _set_kite_interactive_session(
                user_id,
                active=False,
                status="ERROR",
                message="Could not open interactive Kite login.",
                detail=str(exc),
            )
            return
        finally:
            try:
                if browser_proc and browser_proc.poll() is None:
                    browser_proc.terminate()
            except Exception:
                pass
            try:
                if profile_dir and os.path.isdir(profile_dir):
                    shutil.rmtree(profile_dir, ignore_errors=True)
            except Exception:
                pass

        request_token = _parse_kite_request_token(final_url)
        if not request_token:
            _set_kite_interactive_session(
                user_id,
                active=False,
                status="ERROR",
                message="Kite login completed, but request token was not captured.",
                detail="Check the Kite app redirect URL in Kite Connect and try again.",
            )
            return
        try:
            save_payload = dict(payload or {})
            save_payload["request_token"] = request_token
            save_payload["broker_code"] = "ZERODHA"
            # Interactive login implies an active desk; omit enabled so CONNECTED save can default routing on.
            save_payload.pop("enabled", None)
            conn = get_conn()
            try:
                broker = _save_broker_connection(conn, int(user_id), save_payload, test_only=False)
            finally:
                conn.close()
            _set_kite_interactive_session(
                user_id,
                active=False,
                status="CONNECTED",
                message="Zerodha login completed and broker session is attached.",
                detail=str(((broker.get("profile") or {}).get("user_id")) or ""),
                broker_preview={
                    "broker_code": broker.get("broker_code"),
                    "status": broker.get("status"),
                    "account_label": broker.get("account_label"),
                    "broker_name": broker.get("broker_name"),
                },
            )
        except Exception as exc:
            logger.warning("interactive kite login exchange failed: %s", exc)
            _set_kite_interactive_session(
                user_id,
                active=False,
                status="ERROR",
                message="Kite login was captured, but broker validation failed.",
                detail=str(exc),
            )

    threading.Thread(target=_worker, daemon=True).start()
    return _public_kite_interactive_session(user_id) | {"login_url": login_url}


def _public_broker(conn: sqlite3.Connection, user_id: int) -> dict[str, Any]:
    row = _broker_row(conn, user_id)
    catalog = _broker_catalog_map()
    if row is None:
        info = catalog.get("PAPER", {})
        return {
            "broker_code": "PAPER",
            "broker_name": info.get("name", "Paper Router"),
            "status": "READY",
            "enabled": False,
            "paper_mode": True,
            "live_mode": False,
            "effective_live": False,
            "default_quantity": 1,
            "intraday_product": "MIS",
            "positional_product": "CNC",
            "order_variety": "regular",
            "api_secret_masked": "",
            "catalog": BROKER_CATALOG,
            "recent_orders": [],
        }
    info = catalog.get(str(row["broker_code"] or "").upper(), {})
    profile = _loads(row["profile_json"], {})
    capabilities = _loads(row["capabilities_json"], {})
    recent = conn.execute("SELECT * FROM saas_broker_order_log WHERE user_id=? ORDER BY id DESC LIMIT 10", (user_id,)).fetchall()
    return {
        "id": int(row["id"]),
        "broker_code": row["broker_code"],
        "broker_name": info.get("name", row["broker_code"]),
        "tagline": info.get("tagline", ""),
        "help": info.get("help", ""),
        "status": row["status"],
        "enabled": bool(row["enabled"] or 0),
        "paper_mode": _row_int_bool(row["paper_mode"]),
        "live_mode": _row_int_bool(row["live_mode"]),
        "effective_live": _broker_effective_live(row),
        "default_quantity": int(row["default_quantity"] or 1),
        "intraday_product": row["intraday_product"] or "MIS",
        "positional_product": row["positional_product"] or "CNC",
        "order_variety": row["order_variety"] or "regular",
        "account_label": row["account_label"] or "",
        "broker_user_id": row["broker_user_id"] or "",
        "api_key_masked": _mask_secret(str(row["api_key"] or "")),
        "api_secret_masked": _mask_secret(_unpack_secret(row["api_secret_enc"])),
        "access_token_masked": _mask_secret(_unpack_secret(row["access_token_enc"])),
        "last_error": row["last_error"] or "",
        "profile": profile,
        "capabilities": capabilities,
        "connected_at": row["connected_at"],
        "last_checked_at": row["last_checked_at"],
        "login_url": _broker_login_url(row),
        "catalog": BROKER_CATALOG,
        "recent_orders": [
            {
                "id": int(r["id"]),
                "strategy_code": r["strategy_code"],
                "signal_key": r["signal_key"],
                "symbol": r["symbol"],
                "tradingsymbol": r["tradingsymbol"],
                "exchange": r["exchange"],
                "transaction_type": r["transaction_type"],
                "product": r["product"],
                "order_type": r["order_type"],
                "quantity": int(r["quantity"] or 0),
                "requested_price": float(r["requested_price"] or 0),
                "broker_order_id": r["broker_order_id"],
                "broker_status": r["broker_status"] or "",
                "status": r["status"],
                "live_mode": bool(r["live_mode"] or 0),
                "error_text": r["error_text"] or "",
                "created_at": r["created_at"],
            }
            for r in recent
        ],
    }


def _normalize_transaction_type(direction_hint: Any) -> str:
    text = str(direction_hint or "").upper()
    if any(flag in text for flag in ["SHORT", "SELL", "PUT", "BEAR", "DOWN"]):
        return "SELL"
    return "BUY"


def _signal_symbol(payload: dict[str, Any]) -> str:
    for key in ("symbol", "sym", "ticker", "name"):
        value = str(payload.get(key) or "").upper().strip()
        if value:
            return value
    return ""


def _order_price_from_payload(payload: dict[str, Any], *keys: str) -> float:
    for key in keys:
        value = payload.get(key)
        price = _safe_float(value, default=0.0)
        if price > 0:
            return round(price, 2)
    return 0.0


def _build_order_spec(strategy_code: str, payload: dict[str, Any], broker: sqlite3.Row) -> tuple[Optional[dict[str, Any]], str]:
    strategy = str(strategy_code or "").upper()
    symbol = _signal_symbol(payload)
    if strategy == "INDEX":
        return None, "Index auto execution needs option contract expiry mapping, so it stays inbox-only for now."
    if not symbol:
        return None, "Signal symbol missing."
    transaction_type = _normalize_transaction_type(payload.get("direction") or payload.get("signal") or payload.get("type") or payload.get("setup"))
    source_product = broker["intraday_product"] if strategy in {"SPIKE", "INDEX"} else broker["positional_product"]
    product = str(source_product or "MIS").upper()
    quantity = max(1, _safe_int(broker["default_quantity"] or 1, default=1))
    entry_price = 0.0
    if strategy == "SWING":
        entry_price = _order_price_from_payload(payload, "entry", "entry_price", "buy_above", "price")
    else:
        entry_price = _order_price_from_payload(payload, "entry", "trigger_price", "trigger", "ltp", "price", "close")
    order_type = "LIMIT" if entry_price > 0 else "MARKET"
    spec = {
        "symbol": symbol,
        "tradingsymbol": symbol,
        "exchange": "NSE",
        "transaction_type": transaction_type,
        "product": product,
        "order_type": order_type,
        "quantity": quantity,
        "price": round(entry_price, 2) if entry_price > 0 else None,
        "trigger_price": None,
        "direction": "LONG" if transaction_type == "BUY" else "SHORT",
    }
    return spec, "ok"


def _zerodha_test_connection(api_key: str, access_token: str) -> dict[str, Any]:
    from kiteconnect import KiteConnect

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    profile = kite.profile()
    margins = kite.margins()
    equity = (((margins or {}).get("equity") or {}).get("available") or {})
    return {
        "profile": profile,
        "balance_hint": equity.get("cash"),
    }


def _zerodha_place_order(account: sqlite3.Row, spec: dict[str, Any], strategy_code: str, *, variety_override: Optional[str] = None) -> dict[str, Any]:
    from kiteconnect import KiteConnect

    api_key = str(account["api_key"] or "").strip()
    access_token = _unpack_secret(account["access_token_enc"])
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    variety = str(variety_override or account["order_variety"] or "regular").strip().lower() or "regular"
    kwargs: dict[str, Any] = {
        "variety": variety,
        "exchange": spec["exchange"],
        "tradingsymbol": spec["tradingsymbol"],
        "transaction_type": spec["transaction_type"],
        "quantity": int(spec["quantity"]),
        "product": spec["product"],
        "order_type": spec["order_type"],
        "validity": "DAY",
        "tag": _slug_tag(f"{BRAND_NAME}-{strategy_code}", fallback="AUTO"),
    }
    if spec.get("price") is not None:
        kwargs["price"] = float(spec["price"])
    if spec.get("trigger_price") is not None:
        kwargs["trigger_price"] = float(spec["trigger_price"])
    order_id = kite.place_order(**kwargs)
    return {"order_id": order_id, "request": kwargs}


def _zerodha_order_snapshot(account: sqlite3.Row, order_id: str) -> dict[str, Any]:
    from kiteconnect import KiteConnect

    api_key = str(account["api_key"] or "").strip()
    access_token = _unpack_secret(account["access_token_enc"])
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    orders = kite.orders() or []
    for item in reversed(orders):
        if str(item.get("order_id") or "") == str(order_id):
            return dict(item)
    return {}


def _zerodha_cancel_order(account: sqlite3.Row, order_id: str, *, variety_override: Optional[str] = None) -> dict[str, Any]:
    from kiteconnect import KiteConnect

    api_key = str(account["api_key"] or "").strip()
    access_token = _unpack_secret(account["access_token_enc"])
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    variety = str(variety_override or account["order_variety"] or "regular").strip().lower() or "regular"
    resp = kite.cancel_order(variety=variety, order_id=order_id)
    return {"order_id": order_id, "cancel_response": resp}


def _upsert_broker_order_log(
    conn: sqlite3.Connection,
    *,
    user_id: int,
    broker_account_id: Optional[int],
    strategy_code: str,
    signal_key: str,
    spec: dict[str, Any],
    status: str,
    live_mode: bool,
    broker_status: str = "",
    broker_order_id: Optional[str] = None,
    error_text: str = "",
    request_data: Optional[dict[str, Any]] = None,
    response_data: Optional[dict[str, Any]] = None,
) -> None:
    now = _utc_iso()
    conn.execute(
        """
        INSERT INTO saas_broker_order_log(
            user_id,broker_account_id,strategy_code,signal_key,symbol,tradingsymbol,exchange,transaction_type,product,order_type,
            quantity,requested_price,trigger_price,broker_order_id,broker_status,status,live_mode,request_json,response_json,error_text,created_at,updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(user_id, signal_key, strategy_code) DO UPDATE SET
            broker_account_id=excluded.broker_account_id,
            symbol=excluded.symbol,
            tradingsymbol=excluded.tradingsymbol,
            exchange=excluded.exchange,
            transaction_type=excluded.transaction_type,
            product=excluded.product,
            order_type=excluded.order_type,
            quantity=excluded.quantity,
            requested_price=excluded.requested_price,
            trigger_price=excluded.trigger_price,
            broker_order_id=excluded.broker_order_id,
            broker_status=excluded.broker_status,
            status=excluded.status,
            live_mode=excluded.live_mode,
            request_json=excluded.request_json,
            response_json=excluded.response_json,
            error_text=excluded.error_text,
            updated_at=excluded.updated_at
        """,
        (
            user_id,
            broker_account_id,
            strategy_code,
            signal_key,
            spec.get("symbol") or "",
            spec.get("tradingsymbol") or spec.get("symbol") or "",
            spec.get("exchange") or "NSE",
            spec.get("transaction_type") or "BUY",
            spec.get("product") or "MIS",
            spec.get("order_type") or "LIMIT",
            int(spec.get("quantity") or 1),
            spec.get("price"),
            spec.get("trigger_price"),
            broker_order_id,
            broker_status,
            status,
            1 if live_mode else 0,
            _dumps(request_data or {}),
            _dumps(response_data or {}),
            str(error_text or "")[:600],
            now,
            now,
        ),
    )


def _auto_execute_signal_worker(user_id: int, user_email: str, strategy_code: str, signal_key: str, headline: str, payload: dict[str, Any], confidence: float) -> None:
    conn = get_conn()
    try:
        row = _user_row(conn, user_id)
        if row is None or not bool(row["auto_execute"] or 0):
            return
        broker = _broker_row(conn, user_id)
        if broker is None or not bool(broker["enabled"] or 0):
            return
        if str(broker["status"] or "").upper() not in {"CONNECTED", "READY"}:
            return
        ok, reason = _can_receive(conn, user_id, strategy_code, confidence)
        if not ok:
            conn.execute("UPDATE saas_signal_inbox SET status=? WHERE user_id=? AND strategy_code=? AND signal_key=?", (f"AUTO_{reason.upper()}"[:24], user_id, strategy_code, signal_key))
            conn.commit()
            return
        max_open = int(row["max_open_signals"] or 0)
        if max_open > 0:
            open_positions = int(conn.execute("SELECT COUNT(*) FROM saas_trade_journal WHERE user_id=? AND status='OPEN'", (user_id,)).fetchone()[0] or 0)
            if open_positions >= max_open:
                spec = {"symbol": _signal_symbol(payload), "tradingsymbol": _signal_symbol(payload), "exchange": "NSE", "transaction_type": "BUY", "product": "MIS", "order_type": "LIMIT", "quantity": 1}
                _upsert_broker_order_log(
                    conn,
                    user_id=user_id,
                    broker_account_id=int(broker["id"]),
                    strategy_code=strategy_code,
                    signal_key=signal_key,
                    spec=spec,
                    status="SKIPPED",
                    live_mode=_broker_effective_live(broker),
                    broker_status="max-open-signals",
                    error_text="Open position limit reached",
                )
                conn.execute("UPDATE saas_signal_inbox SET status='AUTO_SKIPPED' WHERE user_id=? AND strategy_code=? AND signal_key=?", (user_id, strategy_code, signal_key))
                conn.commit()
                return
        spec, message = _build_order_spec(strategy_code, payload, broker)
        if not spec:
            fallback = {"symbol": _signal_symbol(payload) or strategy_code, "tradingsymbol": _signal_symbol(payload) or strategy_code, "exchange": "NSE", "transaction_type": "BUY", "product": "MIS", "order_type": "LIMIT", "quantity": 1}
            _upsert_broker_order_log(
                conn,
                user_id=user_id,
                broker_account_id=int(broker["id"]),
                strategy_code=strategy_code,
                signal_key=signal_key,
                spec=fallback,
                status="SKIPPED",
                live_mode=_broker_effective_live(broker),
                broker_status="unsupported",
                error_text=message,
            )
            conn.execute("UPDATE saas_signal_inbox SET status='AUTO_SKIPPED' WHERE user_id=? AND strategy_code=? AND signal_key=?", (user_id, strategy_code, signal_key))
            conn.commit()
            return
        existing = conn.execute("SELECT id FROM saas_broker_order_log WHERE user_id=? AND signal_key=? AND strategy_code=?", (user_id, signal_key, strategy_code)).fetchone()
        if existing is not None:
            return
        live_mode = _broker_effective_live(broker)
        now = _utc_iso()
        if live_mode:
            try:
                placed = _zerodha_place_order(broker, spec, strategy_code)
                order_id = str(placed.get("order_id") or "")
                _upsert_broker_order_log(conn, user_id=user_id, broker_account_id=int(broker["id"]), strategy_code=strategy_code, signal_key=signal_key, spec=spec, status="PLACED", live_mode=True, broker_status="OPEN", broker_order_id=order_id, request_data=placed.get("request", {}), response_data=placed)
                notes = f"Auto-executed with {broker['broker_code']} order {order_id}".strip()
                conn.execute("UPDATE saas_signal_inbox SET status='AUTO_EXECUTED' WHERE user_id=? AND strategy_code=? AND signal_key=?", (user_id, strategy_code, signal_key))
            except Exception as exc:
                error_text = str(exc)
                conn.execute("UPDATE saas_broker_accounts SET status='ERROR', last_error=?, last_checked_at=?, updated_at=? WHERE id=?", (error_text[:500], now, now, int(broker["id"])))
                _upsert_broker_order_log(conn, user_id=user_id, broker_account_id=int(broker["id"]), strategy_code=strategy_code, signal_key=signal_key, spec=spec, status="FAILED", live_mode=True, broker_status="ERROR", error_text=error_text)
                conn.execute("UPDATE saas_signal_inbox SET status='AUTO_FAILED' WHERE user_id=? AND strategy_code=? AND signal_key=?", (user_id, strategy_code, signal_key))
                conn.commit()
                return
        else:
            order_id = f"PAPER-{strategy_code}-{int(_utc_now().timestamp())}-{user_id}"
            simulated = {"order_id": order_id, "mode": "paper", "broker_code": broker["broker_code"]}
            _upsert_broker_order_log(conn, user_id=user_id, broker_account_id=int(broker["id"]), strategy_code=strategy_code, signal_key=signal_key, spec=spec, status="SIMULATED", live_mode=False, broker_status="PAPER", broker_order_id=order_id, request_data=spec, response_data=simulated)
            notes = f"Paper-routed via {broker['broker_code']}"
            conn.execute("UPDATE saas_signal_inbox SET status='AUTO_SIMULATED' WHERE user_id=? AND strategy_code=? AND signal_key=?", (user_id, strategy_code, signal_key))
        existing_trade = conn.execute("SELECT id FROM saas_trade_journal WHERE user_id=? AND source_signal_key=? ORDER BY id DESC LIMIT 1", (user_id, signal_key)).fetchone()
        if existing_trade is None:
            conn.execute(
                "INSERT INTO saas_trade_journal(user_id,strategy_code,symbol,direction,entry_price,exit_price,quantity,pnl,status,source_signal_key,opened_at,closed_at,fee_amount,notes,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    user_id,
                    strategy_code,
                    spec["symbol"],
                    spec["direction"],
                    float(spec.get("price") or 0),
                    None,
                    float(spec.get("quantity") or 1),
                    0.0,
                    "OPEN",
                    signal_key,
                    now,
                    None,
                    0.0,
                    notes[:500],
                    now,
                    now,
                ),
            )
        conn.commit()
    except Exception as exc:
        logger.warning("auto execute failed for user %s %s: %s", user_id, signal_key, exc)
        try:
            conn.execute("UPDATE saas_signal_inbox SET status='AUTO_FAILED' WHERE user_id=? AND strategy_code=? AND signal_key=?", (user_id, strategy_code, signal_key))
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()


def _queue_auto_execution(user_id: int, user_email: str, strategy_code: str, signal_key: str, headline: str, payload: dict[str, Any], confidence: float) -> None:
    threading.Thread(
        target=_auto_execute_signal_worker,
        args=(user_id, user_email, strategy_code, signal_key, headline, dict(payload or {}), float(confidence or 0)),
        daemon=True,
    ).start()


def _public_user(conn: sqlite3.Connection, user_id: int) -> dict[str, Any]:
    row = _user_row(conn, user_id)
    if row is None:
        raise HTTPException(status_code=404, detail="User not found")
    sub = _latest_subscription(conn, user_id)
    unread = int(conn.execute("SELECT COUNT(*) FROM saas_signal_inbox WHERE user_id=? AND read_at IS NULL", (user_id,)).fetchone()[0] or 0)
    return {
        "id": int(row["id"]),
        "email": row["email"],
        "full_name": row["full_name"],
        "role": row["role"],
        "status": row["status"],
        "contacts": {
            "whatsapp_phone": _normalize_whatsapp_phone(row["whatsapp_phone"]),
            "whatsapp_apikey_masked": _mask_secret(str(row["whatsapp_apikey"] or ""), keep=4),
            "telegram_chat_id": row["telegram_chat_id"] or "",
        },
        "notifications": {
            "email": bool(row["notify_email"] or 0),
            "telegram": bool(row["notify_telegram"] or 0),
            "whatsapp": bool(row["notify_whatsapp"] or 0),
            "token_reminder": bool(row["notify_token_reminder"] or 0),
            "last_token_reminder_at": row["last_token_reminder_at"],
        },
        "wallet": {
            "balance": round(float(row["balance"] or 0), 2),
            "reserved_balance": round(float(row["reserved_balance"] or 0), 2),
            "status": row["wallet_status"] or "ACTIVE",
            "type": row["wallet_kind"] or row["wallet_type"],
            "realized_profit": round(float(row["realized_profit"] or 0), 2),
            "total_fees": round(float(row["total_fees"] or 0), 2),
            "coupon_code": row["coupon_code"],
            "coupon_profit_cap": round(float(row["coupon_profit_cap"] or COUPON_PROFIT_CAP), 2),
        },
        "subscription": {"plan_code": sub["plan_code"] if sub else None, "status": sub["status"] if sub else "NONE", "expires_at": sub["expires_at"] if sub else None, "amount": float(sub["amount"] or 0) if sub else 0},
        "controls": {"daily_loss_limit": float(row["daily_loss_limit"] or 0), "max_trades_per_day": int(row["max_trades_per_day"] or 0), "max_open_signals": int(row["max_open_signals"] or 0), "profit_share_pct": float(row["profit_share_pct"] or 0), "auto_execute": bool(row["auto_execute"] or 0)},
        "unread_signals": unread,
        "notes": row["notes"] or "",
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "last_login_at": row["last_login_at"],
    }


def _auth_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {"sub": int(row["id"]), "email": row["email"], "role": row["role"], "status": row["status"], "exp": int((_utc_now() + dt.timedelta(hours=TOKEN_TTL_HOURS)).timestamp())}


def _extract_token(auth: Optional[str]) -> str:
    auth = str(auth or "").strip()
    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    return auth.split(" ", 1)[1].strip()


def _require_user(auth: Optional[str]) -> dict[str, Any]:
    token = _extract_token(auth)
    conn = get_conn()
    try:
        user_id: Optional[int] = None
        if token.count(".") == 2:
            payload = _decode_token(token)
            user_id = int(payload["sub"])
        else:
            user_id = _resolve_browser_session_user_id(conn, token)
            if user_id is None:
                raise HTTPException(status_code=401, detail="Invalid or expired session")
        row = conn.execute("SELECT id,email,role,status FROM saas_users WHERE id=?", (int(user_id),)).fetchone()
        if row is None:
            raise HTTPException(status_code=401, detail="User not found")
        if str(row["status"] or "").upper() == "DISABLED":
            raise HTTPException(status_code=403, detail="User disabled")
        conn.commit()
        return {"id": int(row["id"]), "email": row["email"], "role": row["role"], "status": row["status"]}
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def _require_admin(auth: Optional[str]) -> dict[str, Any]:
    user = _require_user(auth)
    if str(user["role"] or "").upper() != "ADMIN":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


def _first_regular_user(conn: sqlite3.Connection) -> bool:
    return int(conn.execute("SELECT COUNT(*) FROM saas_users WHERE role='USER'").fetchone()[0] or 0) == 0


def _credit_wallet(conn: sqlite3.Connection, user_id: int, amount: float, kind: str, note: str, reference_type: str = "SYSTEM", reference_id: Optional[str] = None, meta: Optional[dict[str, Any]] = None) -> None:
    now = _utc_iso()
    conn.execute("UPDATE saas_wallets SET balance=COALESCE(balance,0)+?, updated_at=? WHERE user_id=?", (float(amount), now, user_id))
    conn.execute("INSERT INTO saas_wallet_ledger(user_id,kind,amount,reference_type,reference_id,note,meta_json,created_at) VALUES(?,?,?,?,?,?,?,?)", (user_id, kind, float(amount), reference_type, reference_id, note, _dumps(meta or {}), now))


def _debit_wallet(conn: sqlite3.Connection, user_id: int, amount: float, kind: str, note: str, reference_type: str = "SYSTEM", reference_id: Optional[str] = None, meta: Optional[dict[str, Any]] = None) -> None:
    _credit_wallet(conn, user_id, -abs(float(amount)), kind, note, reference_type, reference_id, meta)


def _maybe_block_coupon_wallet(conn: sqlite3.Connection, user_id: int) -> None:
    row = _user_row(conn, user_id)
    if row is None:
        return
    if str(row["wallet_kind"] or row["wallet_type"] or "").upper() != "COUPON":
        return
    if float(row["realized_profit"] or 0) < float(row["coupon_profit_cap"] or COUPON_PROFIT_CAP):
        return
    now = _utc_iso()
    conn.execute("UPDATE saas_wallets SET status='BLOCKED', updated_at=? WHERE user_id=?", (now, user_id))
    conn.execute("UPDATE saas_users SET status='LIMITED', updated_at=? WHERE id=?", (now, user_id))


def _can_receive(conn: sqlite3.Connection, user_id: int, strategy_code: str, confidence: float) -> tuple[bool, str]:
    row = _user_row(conn, user_id)
    if row is None:
        return False, "user-missing"
    if str(row["status"] or "").upper() not in {"ACTIVE", "LIMITED"}:
        return False, "user-inactive"
    if str(row["wallet_status"] or "ACTIVE").upper() == "BLOCKED":
        return False, "wallet-blocked"
    strat = conn.execute("SELECT us.*, s.active FROM saas_user_strategies us JOIN saas_strategies s ON s.code=us.strategy_code WHERE us.user_id=? AND us.strategy_code=?", (user_id, strategy_code)).fetchone()
    if strat is None or not int(strat["enabled"] or 0) or not int(strat["active"] or 0):
        return False, "strategy-disabled"
    if float(confidence or 0) < float(strat["min_confidence"] or 0):
        return False, "low-confidence"
    today = _today_ist().isoformat()
    max_trades = int(strat["max_trades_per_day"] or row["max_trades_per_day"] or 0)
    if max_trades > 0:
        traded = int(conn.execute("SELECT COUNT(*) FROM saas_trade_journal WHERE user_id=? AND strategy_code=? AND substr(opened_at,1,10)=?", (user_id, strategy_code, today)).fetchone()[0] or 0)
        if traded >= max_trades:
            return False, "trade-limit"
    daily_loss = float(row["daily_loss_limit"] or 0)
    if daily_loss > 0:
        pnl = float(conn.execute("SELECT COALESCE(SUM(pnl),0) FROM saas_trade_journal WHERE user_id=? AND substr(opened_at,1,10)=?", (user_id, today)).fetchone()[0] or 0)
        if pnl <= -abs(daily_loss):
            return False, "daily-loss-hit"
    return True, "ok"


def route_signal_event(strategy_code: str, signal_key: str, headline: str, payload: dict[str, Any], confidence: float = 0.0, excerpt: str = "") -> dict[str, int]:
    init_saas_db()
    conn = get_conn()
    try:
        delivered = skipped = 0
        now = _utc_iso()
        auto_tasks: list[tuple[int, str, str, str, str, dict[str, Any], float]] = []
        for row in conn.execute("SELECT * FROM saas_users WHERE role='USER'").fetchall():
            uid = int(row["id"])
            ok, _ = _can_receive(conn, uid, strategy_code.upper(), float(confidence or 0))
            if not ok:
                skipped += 1
                continue
            try:
                conn.execute("INSERT INTO saas_signal_inbox(user_id,strategy_code,signal_key,headline,excerpt,confidence,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?)", (uid, strategy_code.upper(), signal_key, headline, excerpt[:300], float(confidence or 0), _dumps(payload), now))
                delivered += 1
                auto_tasks.append((uid, str(row["email"] or "").strip(), strategy_code.upper(), signal_key, headline, dict(payload), float(confidence or 0)))
                _dispatch_user_signal_notifications(row, headline, excerpt[:300], strategy_code.upper(), float(confidence or 0))
            except sqlite3.IntegrityError:
                skipped += 1
        conn.commit()
        for task in auto_tasks:
            _queue_auto_execution(*task)
        return {"delivered": delivered, "skipped": skipped}
    finally:
        conn.close()


def route_market_state_snapshot(state: dict[str, Any]) -> dict[str, int]:
    from live_picks import compute_live_picks
    today = _today_ist().isoformat()
    out = {"SPIKE": 0, "SWING": 0}
    for spike in list(state.get("spikes") or [])[:10]:
        sym = str(spike.get("symbol") or "").upper()
        tm = str(spike.get("time") or "").replace(":", "")
        sig_key = f"{today}:SPIKE:{sym}:{spike.get('type')}:{tm}"
        sent = route_signal_event("SPIKE", sig_key, f"{sym} {spike.get('signal') or spike.get('type') or 'SPIKE'}", dict(spike), confidence=float(spike.get("score") or 0), excerpt=str(spike.get("trigger") or spike.get("note") or "Live spike opportunity"))
        out["SPIKE"] += int(sent["delivered"])
    picks = compute_live_picks(state).get("picks") or []
    for pick in picks[:6]:
        sym = str(pick.get("sym") or "").upper()
        sig_key = f"{today}:SWING:{sym}:{pick.get('setup')}:{pick.get('direction')}:{pick.get('entry')}"
        sent = route_signal_event("SWING", sig_key, f"{sym} {pick.get('setup')} {pick.get('direction')}", dict(pick), confidence=float(pick.get("score") or pick.get("raw_score") or 0), excerpt=str(pick.get("reason") or "Swing radar idea"))
        out["SWING"] += int(sent["delivered"])
    return out


def route_index_signal(sig: dict[str, Any]) -> dict[str, int]:
    today = _today_ist().isoformat()
    sym = str(sig.get("symbol") or "INDEX").upper()
    tm = str(sig.get("time") or "").replace(":", "")
    strike = str(sig.get("strike") or "")
    typ = str(sig.get("type") or "").upper()
    key = f"{today}:INDEX:{sym}:{typ}:{strike}:{tm}"
    excerpt = f"Entry {float(sig.get('entry') or 0):.2f} | T1 {float(sig.get('t1') or 0):.2f} | SL {float(sig.get('sl') or 0):.2f}"
    return route_signal_event("INDEX", key, f"{sym} {strike} {typ}".strip(), dict(sig), confidence=float(sig.get("quality") or 0), excerpt=excerpt)


def _dashboard_metrics(conn: sqlite3.Connection, user_id: int) -> dict[str, Any]:
    trades = conn.execute("SELECT COUNT(*) AS total, COALESCE(SUM(pnl),0) AS pnl, COALESCE(SUM(fee_amount),0) AS fees FROM saas_trade_journal WHERE user_id=?", (user_id,)).fetchone()
    today = conn.execute("SELECT COUNT(*) AS total, COALESCE(SUM(pnl),0) AS pnl FROM saas_trade_journal WHERE user_id=? AND substr(opened_at,1,10)=?", (user_id, _today_ist().isoformat())).fetchone()
    sigs = conn.execute("SELECT COUNT(*) AS total, SUM(CASE WHEN read_at IS NULL THEN 1 ELSE 0 END) AS unread FROM saas_signal_inbox WHERE user_id=?", (user_id,)).fetchone()
    return {"trades_total": int(trades["total"] or 0), "pnl_total": round(float(trades["pnl"] or 0), 2), "fees_total": round(float(trades["fees"] or 0), 2), "today_trades": int(today["total"] or 0), "today_pnl": round(float(today["pnl"] or 0), 2), "signals_total": int(sigs["total"] or 0), "signals_unread": int(sigs["unread"] or 0)}


def _signals_list(conn: sqlite3.Connection, user_id: int, limit: int = 40, unread_only: bool = False) -> list[dict[str, Any]]:
    q = "SELECT * FROM saas_signal_inbox WHERE user_id=?"
    params: list[Any] = [user_id]
    if unread_only:
        q += " AND read_at IS NULL"
    q += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    return [{"id": int(r["id"]), "strategy_code": r["strategy_code"], "signal_key": r["signal_key"], "headline": r["headline"], "excerpt": r["excerpt"], "confidence": round(float(r["confidence"] or 0), 2), "status": r["status"], "read": bool(r["read_at"]), "created_at": r["created_at"], "payload": _loads(r["payload_json"], {})} for r in conn.execute(q, tuple(params)).fetchall()]


def _wallet_snapshot(conn: sqlite3.Connection, user_id: int) -> dict[str, Any]:
    wallet = _public_user(conn, user_id)["wallet"]
    ledger = conn.execute("SELECT * FROM saas_wallet_ledger WHERE user_id=? ORDER BY id DESC LIMIT 30", (user_id,)).fetchall()
    wallet["ledger"] = [{"id": int(r["id"]), "kind": r["kind"], "amount": round(float(r["amount"] or 0), 2), "reference_type": r["reference_type"], "reference_id": r["reference_id"], "note": r["note"], "created_at": r["created_at"], "meta": _loads(r["meta_json"], {})} for r in ledger]
    return wallet


def _performance_snapshot(conn: sqlite3.Connection, user_id: int) -> dict[str, Any]:
    rows = conn.execute("SELECT * FROM saas_trade_journal WHERE user_id=? ORDER BY id DESC LIMIT 120", (user_id,)).fetchall()
    closed = [r for r in rows if str(r["status"] or "").upper() == "CLOSED"]
    wins = sum(1 for r in closed if float(r["pnl"] or 0) > 0)
    losses = sum(1 for r in closed if float(r["pnl"] or 0) < 0)
    curve: dict[str, float] = {}
    for r in rows:
        k = str(r["opened_at"] or "")[:10]
        curve[k] = round(curve.get(k, 0.0) + float(r["pnl"] or 0), 2)
    return {"summary": {"total_pnl": round(sum(float(r["pnl"] or 0) for r in rows), 2), "wins": wins, "losses": losses, "closed_trades": len(closed), "win_rate": round((wins / max(1, len(closed))) * 100, 2) if closed else 0.0}, "curve": [{"date": k, "pnl": v} for k, v in sorted(curve.items())][-30:], "trades": [dict(r) for r in rows[:40]]}


def _razorpay_ready() -> bool:
    return bool((os.getenv("RAZORPAY_KEY_ID", "") or "").strip() and (os.getenv("RAZORPAY_KEY_SECRET", "") or "").strip())


def _plan_spec(plan_code: str) -> dict[str, Any]:
    code = str(plan_code or "STARTER").upper().strip()
    for plan in PLAN_CATALOG:
        if plan["code"] == code:
            return plan
    return PLAN_CATALOG[0]


def _create_payment_order(conn: sqlite3.Connection, user_id: int, amount: float, plan_code: str) -> dict[str, Any]:
    now = _utc_iso()
    cur = conn.execute("INSERT INTO saas_payment_orders(user_id,provider,provider_order_id,amount,currency,status,plan_code,qr_payload,checkout_url,meta_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (user_id, "LOCAL", None, amount, "INR", "CREATED", plan_code, None, None, _dumps({}), now, now))
    order_id = int(cur.lastrowid)
    profile = _payment_profile(conn)
    qr_payload = _upi_payload(profile, amount, plan_code, order_id) if profile.get("enabled") and profile.get("upi_id") else ""
    meta = {"provider": "LOCAL", "payment_profile": profile}
    if _razorpay_ready():
        try:
            resp = requests.post("https://api.razorpay.com/v1/orders", auth=(os.getenv("RAZORPAY_KEY_ID", ""), os.getenv("RAZORPAY_KEY_SECRET", "")), json={"amount": int(round(amount * 100)), "currency": "INR", "payment_capture": 1, "notes": {"local_order_id": str(order_id), "user_id": str(user_id), "plan_code": plan_code}}, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            meta = {"provider": "RAZORPAY", "order": data, "key_id": os.getenv("RAZORPAY_KEY_ID", ""), "payment_profile": profile, "local_qr_payload": qr_payload}
            conn.execute("UPDATE saas_payment_orders SET provider='RAZORPAY', provider_order_id=?, status='PENDING', qr_payload=?, meta_json=?, updated_at=? WHERE id=?", (str(data.get("id") or ""), qr_payload or None, _dumps(meta), now, order_id))
        except Exception as exc:
            meta = {"provider": "LOCAL", "fallback_reason": str(exc), "qr_payload": qr_payload, "payment_profile": profile}
            conn.execute("UPDATE saas_payment_orders SET provider='LOCAL', qr_payload=?, meta_json=?, updated_at=? WHERE id=?", (qr_payload or None, _dumps(meta), now, order_id))
    else:
        meta = {"provider": "LOCAL", "qr_payload": qr_payload, "payment_profile": profile}
        conn.execute("UPDATE saas_payment_orders SET provider='LOCAL', qr_payload=?, meta_json=?, updated_at=? WHERE id=?", (qr_payload or None, _dumps(meta), now, order_id))
    conn.commit()
    row = conn.execute("SELECT * FROM saas_payment_orders WHERE id=?", (order_id,)).fetchone()
    return {"id": order_id, "status": row["status"], "provider": row["provider"], "plan_code": row["plan_code"], "amount": float(row["amount"] or 0), "currency": row["currency"], "provider_order_id": row["provider_order_id"], "checkout_url": row["checkout_url"], "qr_payload": row["qr_payload"], "meta": _loads(row["meta_json"], {})}


def _activate_payment(conn: sqlite3.Connection, order_id: int, payload: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM saas_payment_orders WHERE id=?", (order_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Payment order not found")
    if str(row["status"] or "").upper() == "PAID":
        return {"ok": True, "already_paid": True}
    plan_code = str(row["plan_code"] or "").upper().strip()
    is_custom = plan_code == "CUSTOM"
    plan = _plan_spec(plan_code if not is_custom else "STARTER")
    now = _utc_iso()
    expiry = (_utc_now() + dt.timedelta(days=int(plan["duration_days"]))).date().isoformat()
    conn.execute("UPDATE saas_payment_orders SET status='PAID', meta_json=?, updated_at=? WHERE id=?", (_dumps(payload or _loads(row["meta_json"], {})), now, order_id))
    bonus = 0.0
    if is_custom:
        # Custom pay behaves as wallet top-up on admin validation.
        _credit_wallet(conn, int(row["user_id"]), float(row["amount"] or 0), "TOPUP", "Custom payment validated by admin", reference_type="PAYMENT", reference_id=str(order_id))
        expiry = ""
    else:
        conn.execute("INSERT INTO saas_subscriptions(user_id,plan_code,status,amount,source,started_at,expires_at,meta_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)", (int(row["user_id"]), plan["code"], "ACTIVE", float(row["amount"] or 0), str(row["provider"] or "LOCAL"), now, expiry, _dumps({"payment_order_id": order_id}), now))
        bonus = float(plan.get("wallet_credit") or 0)
        if bonus > 0:
            _credit_wallet(conn, int(row["user_id"]), bonus, "PLAN_BONUS", f"{plan['name']} bonus credit", reference_type="PAYMENT", reference_id=str(order_id))
    conn.commit()
    user_row = _user_row(conn, int(row["user_id"]))
    if user_row is not None and not is_custom:
        _send_payment_email(str(user_row["email"] or "").strip(), str(plan["name"]), float(row["amount"] or 0), expiry)
    return {"ok": True, "plan": ("CUSTOM" if is_custom else plan["code"]), "expires_at": expiry, "bonus_credit": bonus, "wallet_topup": float(row["amount"] or 0) if is_custom else 0.0}


def _save_broker_connection(conn: sqlite3.Connection, user_id: int, body: dict[str, Any], test_only: bool = False) -> dict[str, Any]:
    _ensure_broker_row(conn, user_id)
    row = _broker_row(conn, user_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Broker workspace unavailable")
    broker_code = str(body.get("broker_code", row["broker_code"]) or row["broker_code"] or "PAPER").upper().strip()
    catalog = _broker_catalog_map()
    if broker_code not in catalog:
        raise HTTPException(status_code=400, detail="Unsupported broker")
    api_key = str(body.get("api_key", row["api_key"]) or row["api_key"] or "").strip()
    api_secret = str(body.get("api_secret") or "").strip() or _unpack_secret(row["api_secret_enc"])
    access_token = str(body.get("access_token") or "").strip()
    refresh_token = str(body.get("refresh_token") or "").strip() or _unpack_secret(row["refresh_token_enc"])
    account_label = str(body.get("account_label", row["account_label"]) or row["account_label"] or catalog[broker_code]["name"]).strip()
    broker_user_id = str(body.get("broker_user_id", row["broker_user_id"]) or row["broker_user_id"] or "").strip()
    enabled = 1 if body.get("enabled", bool(row["enabled"] or 0)) else 0
    default_quantity = max(1, _safe_int(body.get("default_quantity", row["default_quantity"] or 1), default=1))
    intraday_product = str(body.get("intraday_product", row["intraday_product"] or "MIS") or "MIS").upper()
    positional_product = str(body.get("positional_product", row["positional_product"] or "CNC") or "CNC").upper()
    order_variety = str(body.get("order_variety", row["order_variety"] or "regular") or "regular").lower()
    live_mode = _body_mode_int(body, "live_mode", row["live_mode"])
    paper_mode = _body_mode_int(body, "paper_mode", row["paper_mode"])
    now = _utc_iso()
    profile: dict[str, Any]
    capabilities = catalog[broker_code].copy()
    status = "READY" if broker_code == "PAPER" else "DISCONNECTED"
    last_error = ""
    connected_at = row["connected_at"]
    if broker_code == "PAPER":
        profile = {"name": "Paper Router", "email": "", "mode": "paper", "broker_user_id": broker_user_id or f"PAPER-{user_id}"}
        paper_mode = 1
        live_mode = 0
        enabled = 1 if body.get("enabled", True) else 0
        status = "CONNECTED"
        connected_at = now
    else:
        if not api_key:
            raise HTTPException(status_code=400, detail="API key required for broker")
        request_raw = str(body.get("request_token") or body.get("redirect_url") or "").strip()
        request_token = _parse_kite_request_token(request_raw) if request_raw else ""
        if request_token:
            secret_for_exchange = str(body.get("api_secret") or "").strip() or _unpack_secret(row["api_secret_enc"])
            if not secret_for_exchange:
                raise HTTPException(status_code=400, detail="API secret required to complete Zerodha login")
            try:
                from kiteconnect import KiteConnect

                kite = KiteConnect(api_key=api_key)
                sess = kite.generate_session(request_token, api_secret=secret_for_exchange)
                access_token = str(sess.get("access_token") or "")
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"Zerodha login exchange failed: {exc}") from exc
        if not access_token:
            access_token = _unpack_secret(row["access_token_enc"])
        if not access_token:
            if test_only:
                raise HTTPException(
                    status_code=400,
                    detail="Complete Zerodha login (paste redirect URL) or provide an access token",
                )
            profile = _loads(row["profile_json"], {}) or {}
            status = "DISCONNECTED"
            last_error = "Credentials saved. Complete Zerodha login (paste redirect URL) or add access token to finish connection."
        else:
            try:
                test_result = _zerodha_test_connection(api_key, access_token)
                profile = {
                    "name": str(((test_result.get("profile") or {}).get("user_name")) or broker_user_id or "Zerodha User"),
                    "email": str(((test_result.get("profile") or {}).get("email")) or ""),
                    "user_id": str(((test_result.get("profile") or {}).get("user_id")) or broker_user_id or ""),
                    "balance_hint": test_result.get("balance_hint"),
                }
                broker_user_id = profile["user_id"] or broker_user_id
                status = "CONNECTED"
                connected_at = now
                if "paper_mode" not in body and not test_only:
                    paper_mode = 0
            except Exception as exc:
                profile = _loads(row["profile_json"], {})
                status = "ERROR"
                last_error = str(exc)
                if test_only:
                    raise HTTPException(status_code=400, detail=f"Broker test failed: {exc}") from exc
    if broker_code != "PAPER":
        if live_mode:
            paper_mode = 0
        elif paper_mode:
            live_mode = 0
    if "auto_execute" in body:
        conn.execute("UPDATE saas_users SET auto_execute=?, updated_at=? WHERE id=?", (1 if body.get("auto_execute") else 0, now, user_id))
    if live_mode and broker_code == "PAPER":
        live_mode = 0
    if broker_code != "PAPER" and status == "CONNECTED" and body.get("enabled") is not False:
        enabled = 1
    conn.execute(
        """
        UPDATE saas_broker_accounts
        SET broker_code=?, account_label=?, broker_user_id=?, api_key=?, api_secret_enc=?, access_token_enc=?, refresh_token_enc=?,
            status=?, enabled=?, paper_mode=?, live_mode=?, default_quantity=?, intraday_product=?, positional_product=?, order_variety=?,
            last_error=?, profile_json=?, capabilities_json=?, connected_at=?, last_checked_at=?, updated_at=?
        WHERE user_id=?
        """,
        (
            broker_code,
            account_label[:80],
            broker_user_id[:80],
            api_key,
            _pack_secret(api_secret),
            _pack_secret(access_token),
            _pack_secret(refresh_token),
            status,
            enabled,
            paper_mode,
            live_mode,
            default_quantity,
            intraday_product,
            positional_product,
            order_variety,
            last_error[:500],
            _dumps(profile),
            _dumps(capabilities),
            connected_at,
            now,
            now,
            user_id,
        ),
    )
    conn.commit()
    return _public_broker(conn, user_id)

@router.get("/api/saas/ping")
def saas_ping() -> dict[str, Any]:
    init_saas_db()
    conn = get_conn()
    try:
        total_users = int(conn.execute("SELECT COUNT(*) FROM saas_users").fetchone()[0] or 0)
        gmail_status = _gmail_runtime_status(conn)
        return {
            "ok": True,
            "brand": BRAND_NAME,
            "gmail_ready": bool(gmail_status.get("ready")),
            "gmail": gmail_status,
            "db_path": str(DB_PATH),
            "users": total_users,
            "plans": PLAN_CATALOG,
            "payment_profile": _public_payment_profile(conn),
        }
    finally:
        conn.close()


@router.get(KITE_OAUTH_RETURN_PATH, response_class=HTMLResponse)
def kite_oauth_return_page() -> HTMLResponse:
    """
    After Kite login, Zerodha redirects the browser here with ?request_token=…&status=success.
    This page notifies the opener (Nexus popup flow) and closes itself.
    """
    brand_esc = html.escape(BRAND_NAME, quote=True)
    body = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Kite login — {brand_esc}</title>
  <style>body{{font-family:system-ui,-apple-system,sans-serif;margin:24px;color:#0f172a;background:#f1f5f9}}</style>
</head>
<body>
  <p id="kite-done">Finishing Zerodha login…</p>
  <p id="kite-hint" style="display:none;font-size:14px;color:#334155">You can close this tab. Return to Nexus — your desk should update in a few seconds.</p>
  <script>
(function(){{
  var q = new URLSearchParams(location.search || "");
  var rt = (q.get("request_token") || "").trim();
  var st = (q.get("status") || "").trim();
  var act = (q.get("action") || "").trim();
  if (!rt && location.hash && location.hash.indexOf("request_token=") !== -1) {{
    var qh = new URLSearchParams(location.hash.replace(/^#/, ""));
    rt = (qh.get("request_token") || rt).trim();
    st = (qh.get("status") || st).trim();
    act = (qh.get("action") || act).trim();
  }}
  var payload = {{
    type: "stockr_kite_oauth",
    request_token: rt,
    redirect_url: location.href,
    status: st,
    action: act
  }};
  try{{
    if (window.opener && !window.opener.closed) {{
      window.opener.postMessage(payload, "*");
    }}
  }} catch (e) {{}}
  try{{
    var bc = new BroadcastChannel("stockr_kite_oauth_v1");
    bc.postMessage(payload);
    bc.close();
  }} catch (e3) {{}}
  var hint = document.getElementById("kite-hint");
  var done = document.getElementById("kite-done");
  if (hint) hint.style.display = "block";
  if (done && rt) done.textContent = "Login data sent to Nexus. Closing…";
  setTimeout(function(){{ try {{ window.close(); }} catch (e2) {{}} }}, 500);
}})();
  </script>
</body>
</html>"""
    return HTMLResponse(content=body, status_code=200)


@router.get("/api/saas/bootstrap")
def saas_bootstrap(request: Request) -> dict[str, Any]:
    init_saas_db()
    conn = get_conn()
    try:
        admin = conn.execute("SELECT email, created_at FROM saas_users WHERE role='ADMIN' ORDER BY id ASC LIMIT 1").fetchone()
        gmail_status = _gmail_runtime_status(conn)
        public_base = str(request.base_url).rstrip("/")
        return {
            "ok": True,
            "brand": BRAND_NAME,
            "gmail_ready": bool(gmail_status.get("ready")),
            "gmail": gmail_status,
            "admin": {"email": admin["email"] if admin else DEFAULT_ADMIN_EMAIL, "created_at": admin["created_at"] if admin else None},
            "strategies": [dict(r) for r in conn.execute("SELECT code,name,strategy_type,description,active,theme,accent,default_confidence,default_max_trades FROM saas_strategies ORDER BY id ASC").fetchall()],
            "plans": PLAN_CATALOG,
            "coupon_code": "WELCOME500",
            "payment_profile": _public_payment_profile(conn),
            "kite_oauth_return_url": public_base + KITE_OAUTH_RETURN_PATH,
            "desk_feed_sync_enabled": _env_flag_true("ALLOW_DESK_TOKEN_TO_MAIN_FEED"),
        }
    finally:
        conn.close()


@router.post("/api/auth/signup")
async def auth_signup(request: Request) -> dict[str, Any]:
    init_saas_db()
    body = await request.json()
    email = _normalize_login_email(body.get("email") or "")
    password = str(body.get("password") or "").strip()
    full_name = str(body.get("full_name") or body.get("name") or "Trader").strip()[:80]
    whatsapp_phone = _normalize_whatsapp_phone(body.get("whatsapp_phone") or "")
    telegram_chat_id = _normalize_telegram_chat_id(body.get("telegram_chat_id") or "")
    notify_email = 1 if body.get("notify_email", True) else 0
    notify_telegram = 1 if body.get("notify_telegram", bool(telegram_chat_id)) else 0
    notify_whatsapp = 1 if body.get("notify_whatsapp", False) else 0
    notify_token_reminder = 1 if body.get("notify_token_reminder", True) else 0
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Valid email required")
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    conn = get_conn()
    try:
        if conn.execute("SELECT 1 FROM saas_users WHERE email=?", (email,)).fetchone():
            raise HTTPException(status_code=409, detail="Email already registered")
        now = _utc_iso()
        pw_hash, salt = _hash_password(password)
        is_first_user = _first_regular_user(conn)
        cur = conn.execute(
            "INSERT INTO saas_users(email,full_name,password_hash,password_salt,role,status,wallet_type,whatsapp_phone,telegram_chat_id,notify_email,notify_telegram,notify_whatsapp,notify_token_reminder,coupon_profit_cap,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (email, full_name or "Trader", pw_hash, salt, "USER", "ACTIVE", "COUPON", whatsapp_phone, telegram_chat_id, notify_email, notify_telegram, notify_whatsapp, notify_token_reminder, COUPON_PROFIT_CAP, now, now),
        )
        user_id = int(cur.lastrowid)
        conn.execute(
            "INSERT INTO saas_wallets(user_id,balance,reserved_balance,status,wallet_type,realized_profit,total_fees,coupon_code,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (user_id, 0, 0, "ACTIVE", "COUPON", 0, 0, None, now),
        )
        _ensure_user_strategy_rows(conn, user_id)
        _ensure_broker_row(conn, user_id)
        if is_first_user:
            _credit_wallet(conn, user_id, WELCOME_CREDIT, "WELCOME", "First-user welcome credit", reference_type="SYSTEM", reference_id="WELCOME")
        coupon = conn.execute("SELECT * FROM saas_coupons WHERE code='WELCOME500' AND active=1 LIMIT 1").fetchone()
        if coupon is not None:
            try:
                conn.execute("INSERT INTO saas_coupon_redemptions(coupon_id,user_id,redeemed_at,credit) VALUES(?,?,?,?)", (int(coupon["id"]), user_id, now, float(coupon["credit"] or 0)))
                conn.execute("UPDATE saas_coupons SET used_count=used_count+1, updated_at=? WHERE id=?", (now, int(coupon["id"])))
                conn.execute("UPDATE saas_wallets SET coupon_code=?, updated_at=? WHERE user_id=?", (coupon["code"], now, user_id))
                _credit_wallet(conn, user_id, float(coupon["credit"] or 0), "COUPON", f"Coupon {coupon['code']} applied", reference_type="COUPON", reference_id=str(coupon["id"]))
            except sqlite3.IntegrityError:
                pass
        conn.commit()
        row = conn.execute("SELECT * FROM saas_users WHERE id=?", (user_id,)).fetchone()
        token = _issue_browser_session(conn, user_id)
        conn.commit()
        _send_welcome_email(email, full_name or "Trader")
        return {"ok": True, "token": token, "user": _public_user(conn, user_id)}
    finally:
        conn.close()


@router.post("/api/auth/login")
async def auth_login(request: Request) -> dict[str, Any]:
    init_saas_db()
    body = await request.json()
    email = _normalize_login_email(body.get("email") or "")
    password = str(body.get("password") or "").strip()
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM saas_users WHERE email=?", (email,)).fetchone()
        if row is None or str(row["role"] or "").upper() != "USER":
            raise HTTPException(status_code=401, detail="Invalid credentials")
        if not _verify_password(password, row["password_hash"], row["password_salt"]):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        now = _utc_iso()
        conn.execute("UPDATE saas_users SET last_login_at=?, updated_at=? WHERE id=?", (now, now, int(row["id"])))
        token = _issue_browser_session(conn, int(row["id"]))
        conn.commit()
        return {"ok": True, "token": token, "user": _public_user(conn, int(row["id"]))}
    finally:
        conn.close()


@router.post("/api/admin/login")
async def admin_login(request: Request) -> dict[str, Any]:
    init_saas_db()
    body = await request.json()
    email = _normalize_login_email(body.get("email") or "")
    password = str(body.get("password") or "").strip()
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM saas_users WHERE email=?", (email,)).fetchone()
        if row is None or str(row["role"] or "").upper() != "ADMIN":
            raise HTTPException(status_code=401, detail="Invalid admin credentials")
        if not _verify_password(password, row["password_hash"], row["password_salt"]):
            raise HTTPException(status_code=401, detail="Invalid admin credentials")
        now = _utc_iso()
        conn.execute("UPDATE saas_users SET last_login_at=?, updated_at=? WHERE id=?", (now, now, int(row["id"])))
        token = _issue_browser_session(conn, int(row["id"]))
        conn.commit()
        return {"ok": True, "token": token, "user": _public_user(conn, int(row["id"]))}
    finally:
        conn.close()


@router.post("/api/admin/login/request-otp")
async def admin_login_request_otp(request: Request) -> dict[str, Any]:
    init_saas_db()
    body = await request.json()
    email = _normalize_login_email(body.get("email") or "")
    password = str(body.get("password") or "").strip()
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM saas_users WHERE email=?", (email,)).fetchone()
        if row is None or str(row["role"] or "").upper() != "ADMIN":
            raise HTTPException(status_code=401, detail="Invalid admin credentials")
        if not _verify_password(password, row["password_hash"], row["password_salt"]):
            raise HTTPException(status_code=401, detail="Invalid admin credentials")
        if not _gmail_ready():
            raise HTTPException(status_code=503, detail="Gmail not configured for OTP delivery")
        otp = _admin_otp_generate()
        now_dt = _utc_now()
        now = _utc_iso(now_dt)
        expires_at = _utc_iso(now_dt + dt.timedelta(minutes=int(ADMIN_OTP_TTL_MINUTES)))
        conn.execute(
            "INSERT INTO saas_admin_otp(user_id,email,otp_code,purpose,expires_at,used_at,attempts,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (int(row["id"]), email, otp, "ADMIN_LOGIN", expires_at, None, 0, now),
        )
        conn.commit()
        mailed = _send_admin_otp_email(email, otp)
        phone_sent = _send_admin_otp_phone(otp)
        if not mailed and not phone_sent:
            raise HTTPException(status_code=500, detail="Failed to deliver OTP (email/phone)")
        return {
            "ok": True,
            "otp_sent": True,
            "email": email,
            "phone": _normalize_whatsapp_phone(os.getenv("SAAS_ADMIN_OTP_PHONE", ADMIN_OTP_PHONE)),
            "channels": {"email": bool(mailed), "phone": bool(phone_sent)},
            "ttl_minutes": int(ADMIN_OTP_TTL_MINUTES),
        }
    finally:
        conn.close()


@router.post("/api/admin/login/verify-otp")
async def admin_login_verify_otp(request: Request) -> dict[str, Any]:
    init_saas_db()
    body = await request.json()
    email = _normalize_login_email(body.get("email") or "")
    otp = re.sub(r"\D+", "", str(body.get("otp") or ""))[:6]
    if len(otp) != 6:
        raise HTTPException(status_code=400, detail="Enter valid 6-digit OTP")
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM saas_users WHERE email=?", (email,)).fetchone()
        if row is None or str(row["role"] or "").upper() != "ADMIN":
            raise HTTPException(status_code=401, detail="Invalid admin credentials")
        otp_row = conn.execute(
            """
            SELECT * FROM saas_admin_otp
            WHERE email=? AND purpose='ADMIN_LOGIN'
            ORDER BY id DESC
            LIMIT 1
            """,
            (email,),
        ).fetchone()
        if otp_row is None:
            raise HTTPException(status_code=401, detail="OTP not requested")
        if str(otp_row["used_at"] or "").strip():
            raise HTTPException(status_code=401, detail="OTP already used")
        try:
            expires_dt = dt.datetime.fromisoformat(str(otp_row["expires_at"]))
        except Exception:
            expires_dt = _utc_now() - dt.timedelta(seconds=1)
        if _utc_now() > expires_dt:
            raise HTTPException(status_code=401, detail="OTP expired")
        attempts = int(otp_row["attempts"] or 0)
        if attempts >= int(ADMIN_OTP_MAX_ATTEMPTS):
            raise HTTPException(status_code=429, detail="OTP attempts exceeded. Request a new OTP.")
        if str(otp_row["otp_code"] or "") != otp:
            conn.execute("UPDATE saas_admin_otp SET attempts=attempts+1 WHERE id=?", (int(otp_row["id"]),))
            conn.commit()
            raise HTTPException(status_code=401, detail="Invalid OTP")
        now = _utc_iso()
        conn.execute("UPDATE saas_admin_otp SET used_at=?, attempts=attempts+1 WHERE id=?", (now, int(otp_row["id"])))
        conn.execute("UPDATE saas_users SET last_login_at=?, updated_at=? WHERE id=?", (now, now, int(row["id"])))
        token = _issue_browser_session(conn, int(row["id"]))
        conn.commit()
        return {"ok": True, "token": token, "user": _public_user(conn, int(row["id"]))}
    finally:
        conn.close()


@router.post("/api/auth/logout")
def auth_logout(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    """Revoke DB-backed session (nxa_…). JWT sessions are cleared client-side only."""
    try:
        token = _extract_token(authorization)
    except HTTPException:
        return {"ok": True}
    conn = get_conn()
    try:
        if token.count(".") != 2:
            _revoke_browser_session(conn, token)
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.get("/api/auth/me")
def auth_me(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    conn = get_conn()
    try:
        return {"ok": True, "user": _public_user(conn, int(user["id"]))}
    finally:
        conn.close()

@router.get("/api/user/dashboard")
def user_dashboard(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    conn = get_conn()
    try:
        return {
            "ok": True,
            "user": _public_user(conn, int(user["id"])),
            "metrics": _dashboard_metrics(conn, int(user["id"])),
            "strategies": [dict(r) for r in _strategy_rows(conn, int(user["id"]))],
            "signals": _signals_list(conn, int(user["id"]), limit=8),
            "wallet": _wallet_snapshot(conn, int(user["id"])),
            "broker": _public_broker(conn, int(user["id"])),
            "performance": _performance_snapshot(conn, int(user["id"])),
            "plans": PLAN_CATALOG,
            "payment_profile": _public_payment_profile(conn),
        }
    finally:
        conn.close()


@router.get("/api/user/broker")
def user_broker(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    conn = get_conn()
    try:
        return {"ok": True, "broker": _public_broker(conn, int(user["id"])), "controls": _public_user(conn, int(user["id"]))["controls"]}
    finally:
        conn.close()


@router.patch("/api/user/controls")
async def user_controls_update(request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    body = await request.json()
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM saas_users WHERE id=?", (int(user["id"]),)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="User not found")
        now = _utc_iso()
        auto_execute = 1 if body.get("auto_execute", bool(row["auto_execute"])) else 0
        max_open_signals = max(0, _safe_int(body.get("max_open_signals", row["max_open_signals"] or 0), default=int(row["max_open_signals"] or 0)))
        max_trades_per_day = max(0, _safe_int(body.get("max_trades_per_day", row["max_trades_per_day"] or 0), default=int(row["max_trades_per_day"] or 0)))
        daily_loss_limit = max(0.0, _safe_float(body.get("daily_loss_limit", row["daily_loss_limit"] or 0), default=float(row["daily_loss_limit"] or 0)))
        whatsapp_phone = _normalize_whatsapp_phone(body.get("whatsapp_phone", row["whatsapp_phone"]))
        telegram_chat_id = _normalize_telegram_chat_id(body.get("telegram_chat_id", row["telegram_chat_id"]))
        whatsapp_apikey = str(body.get("whatsapp_apikey") or "").strip() or str(row["whatsapp_apikey"] or "")
        notify_email = 1 if body.get("notify_email", bool(row["notify_email"])) else 0
        notify_telegram = 1 if body.get("notify_telegram", bool(row["notify_telegram"])) else 0
        notify_whatsapp = 1 if body.get("notify_whatsapp", bool(row["notify_whatsapp"])) else 0
        notify_token_reminder = 1 if body.get("notify_token_reminder", bool(row["notify_token_reminder"])) else 0
        conn.execute(
            "UPDATE saas_users SET auto_execute=?, max_open_signals=?, max_trades_per_day=?, daily_loss_limit=?, whatsapp_phone=?, whatsapp_apikey=?, telegram_chat_id=?, notify_email=?, notify_telegram=?, notify_whatsapp=?, notify_token_reminder=?, updated_at=? WHERE id=?",
            (auto_execute, max_open_signals, max_trades_per_day, daily_loss_limit, whatsapp_phone, whatsapp_apikey, telegram_chat_id, notify_email, notify_telegram, notify_whatsapp, notify_token_reminder, now, int(user["id"])),
        )
        conn.commit()
        return {"ok": True, "user": _public_user(conn, int(user["id"]))}
    finally:
        conn.close()


@router.post("/api/user/broker/connect")
async def user_broker_connect(request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    body = await request.json()
    conn = get_conn()
    try:
        broker = _save_broker_connection(conn, int(user["id"]), body, test_only=False)
        return {"ok": True, "broker": broker, "user": _public_user(conn, int(user["id"]))}
    finally:
        conn.close()


@router.post("/api/user/broker/test")
async def user_broker_test(request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    body = await request.json()
    conn = get_conn()
    try:
        broker = _save_broker_connection(conn, int(user["id"]), body, test_only=True)
        return {"ok": True, "broker": broker, "tested": True}
    finally:
        conn.close()


@router.post("/api/user/broker/kite-interactive-start")
async def user_broker_kite_interactive_start(request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    body = await request.json()
    conn = get_conn()
    try:
        row = _broker_row(conn, int(user["id"]))
    finally:
        conn.close()
    load_dotenv(BASE_DIR / ".env", override=True)
    payload = dict(body or {})
    payload["broker_code"] = "ZERODHA"
    payload["api_key"] = str(payload.get("api_key") or (row["api_key"] if row else "") or os.getenv("KITE_API_KEY", "")).strip()
    payload["api_secret"] = str(payload.get("api_secret") or (_unpack_secret(row["api_secret_enc"]) if row else "") or os.getenv("KITE_API_SECRET", "")).strip()
    if not str(payload.get("account_label") or "").strip():
        payload["account_label"] = str((row["account_label"] if row else "") or "Zerodha Desk")
    if "paper_mode" not in payload:
        payload["paper_mode"] = False
    if "enabled" not in payload:
        payload["enabled"] = True
    status = _start_interactive_kite_login(int(user["id"]), payload)
    return {"ok": True, "interactive": status}


@router.get("/api/user/broker/kite-interactive-status")
async def user_broker_kite_interactive_status(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    conn = get_conn()
    try:
        broker = _public_broker(conn, int(user["id"]))
    finally:
        conn.close()
    return {"ok": True, "interactive": _public_kite_interactive_session(int(user["id"])), "broker": broker}


@router.post("/api/user/broker/import-env-token")
async def user_broker_import_env_token(request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    body = await request.json()
    load_dotenv(BASE_DIR / ".env", override=True)
    api_key = str(os.getenv("KITE_API_KEY", "") or "").strip()
    api_secret = str(os.getenv("KITE_API_SECRET", "") or "").strip()
    access_token = str(os.getenv("KITE_ACCESS_TOKEN", "") or "").strip()
    broker_user_id = str(os.getenv("KITE_USER_ID", "") or "").strip()
    missing = [name for name, value in (("KITE_API_KEY", api_key), ("KITE_ACCESS_TOKEN", access_token)) if not value]
    if missing:
        raise HTTPException(
            status_code=400,
            detail="Missing env values: " + ", ".join(missing) + ". Refresh/load your Zerodha token first.",
        )
    payload = dict(body or {})
    payload.update(
        {
            "broker_code": "ZERODHA",
            "api_key": api_key,
            "api_secret": api_secret,
            "access_token": access_token,
            "broker_user_id": broker_user_id,
        }
    )
    if not str(payload.get("account_label") or "").strip():
        payload["account_label"] = "Zerodha Desk"
    if "paper_mode" not in payload:
        payload["paper_mode"] = False
    if "enabled" not in payload:
        payload["enabled"] = True
    conn = get_conn()
    try:
        broker = _save_broker_connection(conn, int(user["id"]), payload, test_only=False)
        status = str((broker or {}).get("status") or "").upper()
        if status not in {"CONNECTED", "READY"}:
            detail = str((broker or {}).get("last_error") or "").strip() or "Shared Zerodha session is not ready yet."
            raise HTTPException(status_code=400, detail=detail)
        return {"ok": True, "broker": broker, "imported": True, "user": _public_user(conn, int(user["id"]))}
    finally:
        conn.close()


@router.post("/api/user/broker/sync-main-feed-token")
async def user_broker_sync_main_feed_token(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    """
    Copy this desk's Zerodha access token into the main Trading OS Kite feed process (quotes / WS).
    Off by default — enable ALLOW_DESK_TOKEN_TO_MAIN_FEED=1 for local single-desk setups.
    """
    if not _env_flag_true("ALLOW_DESK_TOKEN_TO_MAIN_FEED"):
        raise HTTPException(
            status_code=403,
            detail="Disabled. Set ALLOW_DESK_TOKEN_TO_MAIN_FEED=1 in backend/.env and restart the server.",
        )
    user = _require_user(authorization)
    conn = get_conn()
    try:
        row = _broker_row(conn, int(user["id"]))
        if row is None:
            raise HTTPException(status_code=404, detail="Broker workspace unavailable")
        if str(row["broker_code"] or "").upper() != "ZERODHA":
            raise HTTPException(status_code=400, detail="Select Zerodha and connect this desk first")
        st = str(row["status"] or "").upper()
        if st not in {"CONNECTED", "READY"}:
            raise HTTPException(status_code=400, detail="Desk Kite session is not connected yet")
        access_token = _unpack_secret(row["access_token_enc"])
        if not access_token:
            raise HTTPException(status_code=400, detail="No access token stored on this desk")
        api_key = str(row["api_key"] or "").strip()
        if not api_key:
            raise HTTPException(status_code=400, detail="No API key stored on this desk")
        try:
            from scheduler import apply_kite_session_live

            prof = apply_kite_session_live(access_token, api_key)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Could not apply token to live feed: {exc}") from exc
        if _env_flag_true("DESK_SYNC_WRITE_DOTENV"):
            try:
                from dotenv import set_key

                env_path = BASE_DIR / ".env"
                if env_path.is_file():
                    set_key(str(env_path), "KITE_ACCESS_TOKEN", access_token)
                    set_key(str(env_path), "KITE_API_KEY", api_key)
            except Exception:
                logger.warning("sync-main-feed-token: could not write .env", exc_info=True)
        return {
            "ok": True,
            "kite_profile": prof,
            "msg": "Main Trading OS feed now uses this desk token. Hard-refresh the dashboard (Ctrl+F5) if widgets stay empty.",
        }
    finally:
        conn.close()


@router.post("/api/user/broker/sample-order")
async def user_broker_sample_order(request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    body = await request.json()
    conn = get_conn()
    try:
        broker = _broker_row(conn, int(user["id"]))
        if broker is None:
            raise HTTPException(status_code=404, detail="Broker workspace unavailable")
        if not bool(broker["enabled"] or 0):
            raise HTTPException(status_code=400, detail="Enable broker before sample order")
        status = str(broker["status"] or "").upper()
        if status not in {"CONNECTED", "READY"}:
            raise HTTPException(status_code=400, detail="Broker not connected")
        symbol = str(body.get("symbol") or "SBIN").upper().strip()
        if not re.fullmatch(r"[A-Z][A-Z0-9\-]{1,19}", symbol):
            raise HTTPException(status_code=400, detail="Invalid NSE symbol format for test trade")
        qty = max(1, _safe_int(body.get("quantity") or broker["default_quantity"] or 1, default=1))
        if qty > 5000:
            raise HTTPException(status_code=400, detail="Quantity too high for sample order")
        auto_cancel = bool(body.get("auto_cancel", True))
        now = _utc_iso()
        signal_key = f"SAMPLE-{int(_utc_now().timestamp())}-{int(user['id'])}"
        spec = {
            "symbol": symbol,
            "tradingsymbol": symbol,
            "exchange": "NSE",
            "transaction_type": "BUY",
            "product": str(broker["intraday_product"] or "MIS").upper(),
            "order_type": "MARKET",
            "quantity": qty,
            "price": None,
            "trigger_price": None,
            "direction": "LONG",
        }
        live_mode = _broker_effective_live(broker)
        if not live_mode:
            order_id = f"PAPER-SAMPLE-{int(_utc_now().timestamp())}-{int(user['id'])}"
            broker_status = "PAPER_DRILL" if auto_cancel else "PAPER_OPEN"
            # Not a failure: paper route never hits NSE; we simulate then clear by design.
            final_status = "SIMULATED_OK" if auto_cancel else "OPEN"
            log_status = "SIMULATED_OK" if auto_cancel else "SIMULATED"
            response_data = {
                "mode": "paper",
                "order_id": order_id,
                "auto_cancel": auto_cancel,
                "status": final_status,
                "note": "Desk routing check only — no exchange order. Simulated fill then cleared when auto_cancel is on.",
            }
            _upsert_broker_order_log(
                conn,
                user_id=int(user["id"]),
                broker_account_id=int(broker["id"]),
                strategy_code="SAMPLE",
                signal_key=signal_key,
                spec=spec,
                status=log_status,
                live_mode=False,
                broker_status=broker_status,
                broker_order_id=order_id,
                request_data=spec,
                response_data=response_data,
            )
            conn.commit()
            paper_msg = (
                "Paper sample succeeded: routing verified (simulated market BUY then cleared; nothing was sent to NSE)."
                if auto_cancel
                else "Paper sample: simulated order left OPEN for inspection (still no NSE order)."
            )
            route_hint = (
                "Real Kite orders need Live under Order firing and Paper route OFF (Advanced checkboxes sync when you save)."
                if _row_int_bool(broker["paper_mode"]) or not _row_int_bool(broker["live_mode"])
                else ""
            )
            return {
                "ok": True,
                "sample": {
                    "mode": "paper",
                    "order_id": order_id,
                    "symbol": symbol,
                    "quantity": qty,
                    "status": final_status,
                    "message": paper_msg,
                    "hint": route_hint,
                },
                "broker": _public_broker(conn, int(user["id"])),
            }
        market_open_now = bool(is_market_open())
        broker_variety = "regular" if market_open_now else "amo"
        session_hint = "market-open" if market_open_now else get_market_status()
        try:
            placed = _zerodha_place_order(broker, spec, "SAMPLE", variety_override=broker_variety)
            order_id = str(placed.get("order_id") or "")
            snapshot_before = _zerodha_order_snapshot(broker, order_id) if order_id else {}
            snapshot_after = snapshot_before
            cancelled = False
            cancel_error = ""
            if order_id and auto_cancel:
                try:
                    _zerodha_cancel_order(broker, order_id, variety_override=broker_variety)
                    cancelled = True
                    snapshot_after = _zerodha_order_snapshot(broker, order_id)
                except Exception as exc:
                    cancel_error = str(exc)
            final_state = str(snapshot_after.get("status") or snapshot_before.get("status") or ("CANCELLED" if cancelled else "OPEN")).upper()
            _upsert_broker_order_log(
                conn,
                user_id=int(user["id"]),
                broker_account_id=int(broker["id"]),
                strategy_code="SAMPLE",
                signal_key=signal_key,
                spec=spec,
                status=("CANCELLED" if "CANCEL" in final_state else ("EXECUTED" if "COMPLETE" in final_state else "PLACED")),
                live_mode=True,
                broker_status=final_state,
                broker_order_id=order_id,
                error_text=cancel_error,
                request_data={**(placed.get("request", {}) or {}), "session_hint": session_hint},
                response_data={"placed": placed, "order_before": snapshot_before, "order_after": snapshot_after, "cancelled": cancelled, "session_hint": session_hint},
            )
            conn.commit()
            live_msg = f"Live test trade submitted ({broker_variety.upper()}, {session_hint})"
            if cancelled and not cancel_error:
                live_msg += " — then auto-cancelled (sample drill; check order id in Kite console)."
            elif cancelled and cancel_error:
                live_msg += f" — cancel step reported: {cancel_error}"
            return {
                "ok": True,
                "sample": {
                    "mode": "live",
                    "order_id": order_id,
                    "symbol": symbol,
                    "quantity": qty,
                    "status": final_state,
                    "cancelled": cancelled,
                    "cancel_error": cancel_error,
                    "variety": broker_variety,
                    "session_hint": session_hint,
                    "message": live_msg,
                },
                "broker": _public_broker(conn, int(user["id"])),
            }
        except Exception as exc:
            error_text = str(exc)
            _upsert_broker_order_log(
                conn,
                user_id=int(user["id"]),
                broker_account_id=int(broker["id"]),
                strategy_code="SAMPLE",
                signal_key=signal_key,
                spec=spec,
                status="FAILED",
                live_mode=True,
                broker_status="REJECTED",
                error_text=error_text,
                request_data={"variety": broker_variety, **spec, "session_hint": session_hint},
                response_data={"error": error_text, "session_hint": session_hint},
            )
            conn.execute(
                "UPDATE saas_broker_accounts SET last_error=?, last_checked_at=?, updated_at=? WHERE id=?",
                (error_text[:500], now, now, int(broker["id"])),
            )
            conn.commit()
            return JSONResponse(
                {
                    "ok": False,
                    "detail": f"Sample order was not accepted by broker ({broker_variety.upper()}, {session_hint}). {error_text}",
                    "sample": {
                        "mode": "live",
                        "symbol": symbol,
                        "quantity": qty,
                        "status": "REJECTED",
                        "variety": broker_variety,
                        "session_hint": session_hint,
                    },
                    "broker": _public_broker(conn, int(user["id"])),
                },
                status_code=400,
            )
    finally:
        conn.close()


@router.get("/api/user/signals")
def user_signals(limit: int = 40, unread_only: bool = False, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    conn = get_conn()
    try:
        return {"ok": True, "items": _signals_list(conn, int(user["id"]), limit=max(1, min(limit, 200)), unread_only=bool(unread_only))}
    finally:
        conn.close()


@router.post("/api/user/signals/{signal_id}/read")
def user_signal_read(signal_id: int, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    conn = get_conn()
    try:
        now = _utc_iso()
        conn.execute("UPDATE saas_signal_inbox SET read_at=COALESCE(read_at,?), status='READ' WHERE id=? AND user_id=?", (now, signal_id, int(user["id"])))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.get("/api/user/strategies")
def user_strategies(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    conn = get_conn()
    try:
        return {"ok": True, "items": [dict(r) for r in _strategy_rows(conn, int(user["id"]))]}
    finally:
        conn.close()


@router.patch("/api/user/strategies/{strategy_code}")
async def user_strategy_update(strategy_code: str, request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    body = await request.json()
    conn = get_conn()
    try:
        _ensure_user_strategy_rows(conn, int(user["id"]))
        row = conn.execute("SELECT * FROM saas_user_strategies WHERE user_id=? AND strategy_code=?", (int(user["id"]), strategy_code.upper())).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Strategy not found")
        now = _utc_iso()
        conn.execute(
            "UPDATE saas_user_strategies SET enabled=?, min_confidence=?, max_trades_per_day=?, risk_level=?, updated_at=? WHERE id=?",
            (
                1 if body.get("enabled", row["enabled"]) else 0,
                float(body.get("min_confidence", row["min_confidence"])),
                int(body.get("max_trades_per_day", row["max_trades_per_day"])),
                str(body.get("risk_level", row["risk_level"]) or "MEDIUM").upper(),
                now,
                int(row["id"]),
            ),
        )
        conn.commit()
        return {"ok": True, "items": [dict(r) for r in _strategy_rows(conn, int(user["id"]))]}
    finally:
        conn.close()


@router.get("/api/user/wallet")
def user_wallet(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    conn = get_conn()
    try:
        return {"ok": True, "wallet": _wallet_snapshot(conn, int(user["id"]))}
    finally:
        conn.close()


@router.post("/api/user/wallet/redeem-coupon")
async def user_redeem_coupon(request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    body = await request.json()
    code = str(body.get("code") or "").strip().upper()
    if not code:
        raise HTTPException(status_code=400, detail="Coupon code required")
    conn = get_conn()
    try:
        coupon = conn.execute("SELECT * FROM saas_coupons WHERE code=?", (code,)).fetchone()
        if coupon is None or not int(coupon["active"] or 0):
            raise HTTPException(status_code=404, detail="Coupon not active")
        if coupon["expires_at"] and str(coupon["expires_at"]) < _today_ist().isoformat():
            raise HTTPException(status_code=400, detail="Coupon expired")
        if int(coupon["usage_limit"] or 0) > 0 and int(coupon["used_count"] or 0) >= int(coupon["usage_limit"] or 0):
            raise HTTPException(status_code=400, detail="Coupon usage limit reached")
        now = _utc_iso()
        try:
            conn.execute("INSERT INTO saas_coupon_redemptions(coupon_id,user_id,redeemed_at,credit) VALUES(?,?,?,?)", (int(coupon["id"]), int(user["id"]), now, float(coupon["credit"] or 0)))
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail="Coupon already redeemed") from exc
        conn.execute("UPDATE saas_coupons SET used_count=used_count+1, updated_at=? WHERE id=?", (now, int(coupon["id"])))
        conn.execute("UPDATE saas_wallets SET wallet_type='COUPON', coupon_code=?, updated_at=? WHERE user_id=?", (coupon["code"], now, int(user["id"])))
        conn.execute("UPDATE saas_users SET wallet_type='COUPON', coupon_profit_cap=?, updated_at=? WHERE id=?", (float(coupon["max_profit"] or COUPON_PROFIT_CAP), now, int(user["id"])))
        _credit_wallet(conn, int(user["id"]), float(coupon["credit"] or 0), "COUPON", f"Coupon {coupon['code']} redeemed", reference_type="COUPON", reference_id=str(coupon["id"]))
        conn.commit()
        return {"ok": True, "wallet": _wallet_snapshot(conn, int(user["id"]))}
    finally:
        conn.close()


@router.post("/api/user/payments/create")
async def user_payment_create(request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    body = await request.json()
    raw_plan = str(body.get("plan_code") or "STARTER").upper().strip()
    custom_mode = raw_plan == "CUSTOM"
    plan = _plan_spec(raw_plan if not custom_mode else "STARTER")
    amount = float(body.get("amount") or (0 if custom_mode else plan["price"]) or 0)
    if amount < 10:
        raise HTTPException(status_code=400, detail="Minimum payment is Rs 10")
    store_plan_code = "CUSTOM" if custom_mode else plan["code"]
    conn = get_conn()
    try:
        order = _create_payment_order(conn, int(user["id"]), amount, store_plan_code)
        return {"ok": True, "order": order, "plan": ({"code": "CUSTOM", "name": "Custom Pay", "price": amount, "duration_days": 0, "wallet_credit": 0, "features": ["Wallet top-up after admin validation"]} if custom_mode else plan), "payment_profile": _public_payment_profile(conn)}
    finally:
        conn.close()


@router.get("/api/user/payments")
def user_payments(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    conn = get_conn()
    try:
        rows = conn.execute("SELECT * FROM saas_payment_orders WHERE user_id=? ORDER BY id DESC LIMIT 50", (int(user["id"]),)).fetchall()
        return {"ok": True, "items": [{"id": int(r["id"]), "provider": r["provider"], "provider_order_id": r["provider_order_id"], "amount": float(r["amount"] or 0), "currency": r["currency"], "status": r["status"], "plan_code": r["plan_code"], "qr_payload": r["qr_payload"], "checkout_url": r["checkout_url"], "created_at": r["created_at"], "meta": _loads(r["meta_json"], {})} for r in rows], "payment_profile": _public_payment_profile(conn)}
    finally:
        conn.close()


@router.post("/api/user/payments/{order_id}/mark-paid")
async def user_mark_payment_paid(order_id: int, request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    body = await request.json()
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM saas_payment_orders WHERE id=? AND user_id=?", (order_id, int(user["id"]))).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Payment order not found")
        st = str(row["status"] or "").upper()
        if st == "PAID":
            return {"ok": True, "already_paid": True, "result": {"plan": row["plan_code"], "status": "PAID"}}
        if st == "PENDING_VALIDATION":
            return {"ok": True, "already_pending": True, "result": {"plan": row["plan_code"], "status": "PENDING_VALIDATION"}}
        meta = _loads(row["meta_json"], {})
        meta["user_marked_paid"] = True
        meta["user_marked_paid_at"] = _utc_iso()
        meta["user_marked_source"] = str(body.get("source") or "user-panel")[:40]
        conn.execute("UPDATE saas_payment_orders SET status='PENDING_VALIDATION', meta_json=?, updated_at=? WHERE id=?", (_dumps(meta), _utc_iso(), order_id))
        conn.commit()
        return {"ok": True, "result": {"plan": row["plan_code"], "status": "PENDING_VALIDATION"}, "wallet": _wallet_snapshot(conn, int(user["id"])), "payments": user_payments(authorization)["items"]}
    finally:
        conn.close()


@router.delete("/api/user/payments/{order_id}")
def user_delete_payment(order_id: int, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM saas_payment_orders WHERE id=? AND user_id=?", (order_id, int(user["id"]))).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Payment order not found")
        if str(row["status"] or "").upper() == "PAID":
            raise HTTPException(status_code=400, detail="Paid orders cannot be removed")
        conn.execute("DELETE FROM saas_payment_orders WHERE id=? AND user_id=?", (order_id, int(user["id"])))
        conn.commit()
        rows = conn.execute("SELECT * FROM saas_payment_orders WHERE user_id=? ORDER BY id DESC LIMIT 50", (int(user["id"]),)).fetchall()
        items = [{"id": int(r["id"]), "provider": r["provider"], "provider_order_id": r["provider_order_id"], "amount": float(r["amount"] or 0), "currency": r["currency"], "status": r["status"], "plan_code": r["plan_code"], "qr_payload": r["qr_payload"], "checkout_url": r["checkout_url"], "created_at": r["created_at"], "meta": _loads(r["meta_json"], {})} for r in rows]
        return {"ok": True, "removed": True, "payments": items}
    finally:
        conn.close()


@router.get("/api/user/performance")
def user_performance(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    conn = get_conn()
    try:
        return {"ok": True, **_performance_snapshot(conn, int(user["id"]))}
    finally:
        conn.close()

@router.get("/api/user/trades")
def user_trades(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    conn = get_conn()
    try:
        rows = conn.execute("SELECT * FROM saas_trade_journal WHERE user_id=? ORDER BY id DESC LIMIT 100", (int(user["id"]),)).fetchall()
        return {"ok": True, "items": [dict(r) for r in rows]}
    finally:
        conn.close()


@router.post("/api/user/trades")
async def user_trade_create(request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    body = await request.json()
    strategy_code = str(body.get("strategy_code") or "SPIKE").upper()
    symbol = str(body.get("symbol") or "").upper().strip()
    direction = str(body.get("direction") or "LONG").upper().strip()
    if not symbol:
        raise HTTPException(status_code=400, detail="Symbol required")
    conn = get_conn()
    try:
        now = _utc_iso()
        cur = conn.execute(
            "INSERT INTO saas_trade_journal(user_id,strategy_code,symbol,direction,entry_price,exit_price,quantity,pnl,status,source_signal_key,opened_at,closed_at,fee_amount,notes,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                int(user["id"]), strategy_code, symbol, direction,
                float(body.get("entry_price") or 0),
                body.get("exit_price"),
                float(body.get("quantity") or 1),
                float(body.get("pnl") or 0),
                str(body.get("status") or "OPEN").upper(),
                body.get("source_signal_key"),
                str(body.get("opened_at") or now),
                body.get("closed_at"),
                float(body.get("fee_amount") or 0),
                str(body.get("notes") or "")[:500],
                now,
                now,
            ),
        )
        conn.commit()
        return {"ok": True, "trade_id": int(cur.lastrowid)}
    finally:
        conn.close()


@router.patch("/api/user/trades/{trade_id}")
async def user_trade_update(trade_id: int, request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    user = _require_user(authorization)
    body = await request.json()
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM saas_trade_journal WHERE id=? AND user_id=?", (trade_id, int(user["id"]))).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Trade not found")
        now = _utc_iso()
        new_status = str(body.get("status", row["status"]) or row["status"]).upper()
        pnl = float(body.get("pnl", row["pnl"]) or 0)
        exit_price = body.get("exit_price", row["exit_price"])
        fee_amount = float(row["fee_amount"] or 0)
        if str(row["status"] or "").upper() != "CLOSED" and new_status == "CLOSED":
            profile = _public_user(conn, int(user["id"]))
            pct = float(profile["controls"]["profit_share_pct"] or 0)
            fee_amount = round(max(0.0, pnl) * (pct / 100.0), 2)
            conn.execute("UPDATE saas_wallets SET realized_profit=COALESCE(realized_profit,0)+?, total_fees=COALESCE(total_fees,0)+?, updated_at=? WHERE user_id=?", (pnl, fee_amount, now, int(user["id"])))
            if fee_amount > 0:
                _debit_wallet(conn, int(user["id"]), fee_amount, "FEE", f"Profit share fee for trade {trade_id}", reference_type="TRADE", reference_id=str(trade_id))
            _maybe_block_coupon_wallet(conn, int(user["id"]))
        conn.execute(
            "UPDATE saas_trade_journal SET entry_price=?, exit_price=?, quantity=?, pnl=?, status=?, closed_at=?, fee_amount=?, notes=?, updated_at=? WHERE id=? AND user_id=?",
            (
                float(body.get("entry_price", row["entry_price"]) or 0),
                exit_price,
                float(body.get("quantity", row["quantity"]) or 1),
                pnl,
                new_status,
                body.get("closed_at") or (now if new_status == "CLOSED" else row["closed_at"]),
                fee_amount,
                str(body.get("notes", row["notes"]) or "")[:500],
                now,
                trade_id,
                int(user["id"]),
            ),
        )
        conn.commit()
        return {"ok": True, "performance": _performance_snapshot(conn, int(user["id"])), "wallet": _wallet_snapshot(conn, int(user["id"]))}
    finally:
        conn.close()


@router.get("/api/admin/dashboard")
def admin_dashboard(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    conn = get_conn()
    try:
        total_users = int(conn.execute("SELECT COUNT(*) FROM saas_users WHERE role='USER'").fetchone()[0] or 0)
        active_users = int(conn.execute("SELECT COUNT(*) FROM saas_users WHERE role='USER' AND status IN ('ACTIVE','LIMITED')").fetchone()[0] or 0)
        revenue = float(conn.execute("SELECT COALESCE(SUM(amount),0) FROM saas_payment_orders WHERE status='PAID'").fetchone()[0] or 0)
        signals_total = int(conn.execute("SELECT COUNT(*) FROM saas_signal_inbox").fetchone()[0] or 0)
        return {
            "ok": True,
            "metrics": {"total_users": total_users, "active_users": active_users, "revenue": round(revenue, 2), "signals_total": signals_total},
            "users": [_public_user(conn, int(r["id"])) for r in conn.execute("SELECT id FROM saas_users ORDER BY id DESC LIMIT 25").fetchall()],
            "strategies": [dict(r) for r in conn.execute("SELECT * FROM saas_strategies ORDER BY id ASC").fetchall()],
            "payments": [{"id": int(r["id"]), "user_id": int(r["user_id"]), "amount": float(r["amount"] or 0), "status": r["status"], "provider": r["provider"], "plan_code": r["plan_code"], "created_at": r["created_at"]} for r in conn.execute("SELECT * FROM saas_payment_orders ORDER BY id DESC LIMIT 20").fetchall()],
            "signals": [{"strategy_code": r["strategy_code"], "headline": r["headline"], "confidence": float(r["confidence"] or 0), "created_at": r["created_at"]} for r in conn.execute("SELECT * FROM saas_signal_inbox ORDER BY id DESC LIMIT 20").fetchall()],
            "payment_profile": _public_payment_profile(conn),
        }
    finally:
        conn.close()


@router.get("/api/admin/users")
def admin_users(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    conn = get_conn()
    try:
        return {"ok": True, "items": [_public_user(conn, int(r["id"])) for r in conn.execute("SELECT id FROM saas_users ORDER BY id DESC").fetchall()]}
    finally:
        conn.close()

@router.post("/api/admin/users")
async def admin_create_user(request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    body = await request.json()
    email = str(body.get("email") or "").strip().lower()
    password = str(body.get("password") or "Welcome@123").strip()
    full_name = str(body.get("full_name") or "Managed User").strip()
    role = str(body.get("role") or "USER").upper()
    notify_email = 1 if body.get("notify_email", True) else 0
    send_email = bool(body.get("send_email", True))
    conn = get_conn()
    try:
        if conn.execute("SELECT 1 FROM saas_users WHERE email=?", (email,)).fetchone():
            raise HTTPException(status_code=409, detail="Email already exists")
        now = _utc_iso()
        pw_hash, salt = _hash_password(password)
        cur = conn.execute(
            "INSERT INTO saas_users(email,full_name,password_hash,password_salt,role,status,wallet_type,notify_email,coupon_profit_cap,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (email, full_name, pw_hash, salt, role, "ACTIVE", "PAID" if role == "ADMIN" else "COUPON", notify_email, COUPON_PROFIT_CAP, now, now),
        )
        user_id = int(cur.lastrowid)
        conn.execute("INSERT INTO saas_wallets(user_id,balance,reserved_balance,status,wallet_type,realized_profit,total_fees,updated_at) VALUES(?,?,?,?,?,?,?,?)", (user_id, 0, 0, "ACTIVE", "PAID" if role == "ADMIN" else "COUPON", 0, 0, now))
        _ensure_user_strategy_rows(conn, user_id)
        _ensure_broker_row(conn, user_id)
        conn.commit()
        user_public = _public_user(conn, user_id)
        emailed = _send_managed_password_email(email, full_name, password, role=role) if send_email else False
        return {"ok": True, "user": user_public, "temporary_password": password, "emailed": bool(emailed)}
    finally:
        conn.close()


@router.patch("/api/admin/users/{user_id}")
async def admin_update_user(user_id: int, request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    body = await request.json()
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM saas_users WHERE id=?", (user_id,)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="User not found")
        now = _utc_iso()
        fields = {
            "full_name": str(body.get("full_name", row["full_name"]) or row["full_name"]),
            "status": str(body.get("status", row["status"]) or row["status"]).upper(),
            "wallet_type": str(body.get("wallet_type", row["wallet_type"]) or row["wallet_type"]).upper(),
            "daily_loss_limit": float(body.get("daily_loss_limit", row["daily_loss_limit"]) or 0),
            "max_trades_per_day": int(body.get("max_trades_per_day", row["max_trades_per_day"]) or 0),
            "max_open_signals": int(body.get("max_open_signals", row["max_open_signals"]) or 0),
            "profit_share_pct": float(body.get("profit_share_pct", row["profit_share_pct"]) or 0),
            "coupon_profit_cap": float(body.get("coupon_profit_cap", row["coupon_profit_cap"]) or 0),
            "auto_execute": 1 if body.get("auto_execute", bool(row["auto_execute"])) else 0,
            "notify_email": 1 if body.get("notify_email", bool(row["notify_email"])) else 0,
            "notify_telegram": 1 if body.get("notify_telegram", bool(row["notify_telegram"])) else 0,
            "notify_whatsapp": 1 if body.get("notify_whatsapp", bool(row["notify_whatsapp"])) else 0,
            "notify_token_reminder": 1 if body.get("notify_token_reminder", bool(row["notify_token_reminder"])) else 0,
            "whatsapp_phone": _normalize_whatsapp_phone(body.get("whatsapp_phone", row["whatsapp_phone"])),
            "telegram_chat_id": _normalize_telegram_chat_id(body.get("telegram_chat_id", row["telegram_chat_id"])),
            "notes": str(body.get("notes", row["notes"]) or "")[:1000],
        }
        conn.execute(
            "UPDATE saas_users SET full_name=?, status=?, wallet_type=?, daily_loss_limit=?, max_trades_per_day=?, max_open_signals=?, profit_share_pct=?, coupon_profit_cap=?, auto_execute=?, notify_email=?, notify_telegram=?, notify_whatsapp=?, notify_token_reminder=?, whatsapp_phone=?, telegram_chat_id=?, notes=?, updated_at=? WHERE id=?",
            (fields["full_name"], fields["status"], fields["wallet_type"], fields["daily_loss_limit"], fields["max_trades_per_day"], fields["max_open_signals"], fields["profit_share_pct"], fields["coupon_profit_cap"], fields["auto_execute"], fields["notify_email"], fields["notify_telegram"], fields["notify_whatsapp"], fields["notify_token_reminder"], fields["whatsapp_phone"], fields["telegram_chat_id"], fields["notes"], now, user_id),
        )
        conn.execute("UPDATE saas_wallets SET wallet_type=?, status=?, updated_at=? WHERE user_id=?", (fields["wallet_type"], "BLOCKED" if fields["status"] == "DISABLED" else "ACTIVE", now, user_id))
        conn.commit()
        user_public = _public_user(conn, user_id)
        emailed = False
        if body.get("send_email", False):
            emailed = _send_user_account_summary_email(
                user_public,
                headline="Your account details were updated",
                intro="your admin updated your desk settings. Here is the latest snapshot of your account.",
            )
        return {"ok": True, "user": user_public, "emailed": bool(emailed)}
    finally:
        conn.close()


@router.post("/api/admin/users/{user_id}/password-reset")
async def admin_reset_user_password(user_id: int, request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    body = await request.json()
    temporary_password = str(body.get("password") or "").strip() or f"Desk@{secrets.randbelow(999999):06d}"
    if len(temporary_password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM saas_users WHERE id=?", (user_id,)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="User not found")
        now = _utc_iso()
        pw_hash, salt = _hash_password(temporary_password)
        conn.execute("UPDATE saas_users SET password_hash=?, password_salt=?, updated_at=? WHERE id=?", (pw_hash, salt, now, user_id))
        conn.commit()
        user_public = _public_user(conn, user_id)
        emailed = _send_managed_password_email(str(row["email"] or "").strip(), str(row["full_name"] or "").strip(), temporary_password, role=str(row["role"] or "USER")) if body.get("send_email", True) else False
        return {"ok": True, "user": user_public, "temporary_password": temporary_password, "emailed": bool(emailed)}
    finally:
        conn.close()


@router.post("/api/admin/users/{user_id}/send-summary")
async def admin_send_user_summary(user_id: int, request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    body = await request.json()
    conn = get_conn()
    try:
        user_public = _public_user(conn, user_id)
        wallet_note = str(body.get("wallet_note") or "").strip()
        sent = _send_user_account_summary_email(
            user_public,
            headline=str(body.get("headline") or "Your latest account summary"),
            intro=str(body.get("intro") or "here is the latest summary of your desk, wallet, and operating limits."),
            wallet_note=wallet_note,
        )
        if not sent:
            raise HTTPException(status_code=502, detail="Failed to send summary email")
        return {"ok": True, "sent": True, "user": user_public}
    finally:
        conn.close()


@router.post("/api/admin/users/{user_id}/wallet/credit")
async def admin_wallet_credit(user_id: int, request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    body = await request.json()
    amount = float(body.get("amount") or 0)
    note = str(body.get("note") or "Admin wallet credit")[:200]
    if amount == 0:
        raise HTTPException(status_code=400, detail="Amount required")
    conn = get_conn()
    try:
        if amount > 0:
            _credit_wallet(conn, user_id, amount, "ADMIN", note, reference_type="ADMIN", reference_id=str(user_id))
        else:
            _debit_wallet(conn, user_id, abs(amount), "ADMIN", note, reference_type="ADMIN", reference_id=str(user_id))
        conn.commit()
        user_public = _public_user(conn, user_id)
        wallet = _wallet_snapshot(conn, user_id)
        emailed = False
        if body.get("send_email", False):
            direction = "credited" if amount > 0 else "debited"
            emailed = _send_user_account_summary_email(
                user_public,
                headline="Your wallet was updated",
                intro=f"your admin just {direction} your wallet. The latest wallet status is shown below.",
                wallet_note=f"Wallet adjustment: Rs {amount:,.2f}. Note: {note}",
            )
        return {"ok": True, "wallet": wallet, "emailed": bool(emailed)}
    finally:
        conn.close()


@router.get("/api/admin/strategies")
def admin_strategies(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    conn = get_conn()
    try:
        return {"ok": True, "items": [dict(r) for r in conn.execute("SELECT * FROM saas_strategies ORDER BY id ASC").fetchall()]}
    finally:
        conn.close()


@router.patch("/api/admin/strategies/{strategy_code}")
async def admin_strategy_update(strategy_code: str, request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    body = await request.json()
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM saas_strategies WHERE code=?", (strategy_code.upper(),)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Strategy not found")
        now = _utc_iso()
        conn.execute(
            "UPDATE saas_strategies SET name=?, strategy_type=?, description=?, active=?, theme=?, accent=?, default_confidence=?, default_max_trades=?, updated_at=? WHERE code=?",
            (
                str(body.get("name", row["name"]) or row["name"]),
                str(body.get("strategy_type", row["strategy_type"]) or row["strategy_type"]).upper(),
                str(body.get("description", row["description"]) or row["description"]),
                1 if body.get("active", bool(row["active"])) else 0,
                str(body.get("theme", row["theme"]) or row["theme"]),
                str(body.get("accent", row["accent"]) or row["accent"]),
                float(body.get("default_confidence", row["default_confidence"]) or 0),
                int(body.get("default_max_trades", row["default_max_trades"]) or 0),
                now,
                strategy_code.upper(),
            ),
        )
        conn.commit()
        return {"ok": True, "items": [dict(r) for r in conn.execute("SELECT * FROM saas_strategies ORDER BY id ASC").fetchall()]}
    finally:
        conn.close()


@router.get("/api/admin/coupons")
def admin_coupons(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    conn = get_conn()
    try:
        return {"ok": True, "items": [dict(r) for r in conn.execute("SELECT * FROM saas_coupons ORDER BY id DESC").fetchall()]}
    finally:
        conn.close()

@router.post("/api/admin/coupons")
async def admin_create_coupon(request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    body = await request.json()
    code = str(body.get("code") or "").strip().upper()
    if not code:
        raise HTTPException(status_code=400, detail="Code required")
    conn = get_conn()
    try:
        now = _utc_iso()
        conn.execute(
            "INSERT INTO saas_coupons(code,credit,max_profit,active,usage_limit,used_count,expires_at,strategy_bundle,created_at,updated_at,notes) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                code,
                float(body.get("credit") or 0),
                float(body.get("max_profit") or COUPON_PROFIT_CAP),
                1 if body.get("active", True) else 0,
                int(body.get("usage_limit") or 1),
                0,
                body.get("expires_at"),
                str(body.get("strategy_bundle") or "SPIKE,INDEX,SWING"),
                now,
                now,
                str(body.get("notes") or "")[:300],
            ),
        )
        conn.commit()
        return {"ok": True, "items": [dict(r) for r in conn.execute("SELECT * FROM saas_coupons ORDER BY id DESC").fetchall()]}
    finally:
        conn.close()


@router.get("/api/admin/payments")
def admin_payments(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    conn = get_conn()
    try:
        rows = conn.execute("SELECT * FROM saas_payment_orders ORDER BY id DESC LIMIT 100").fetchall()
        return {"ok": True, "items": [{"id": int(r["id"]), "user_id": int(r["user_id"]), "provider": r["provider"], "provider_order_id": r["provider_order_id"], "amount": float(r["amount"] or 0), "status": r["status"], "plan_code": r["plan_code"], "created_at": r["created_at"], "meta": _loads(r["meta_json"], {})} for r in rows], "payment_profile": _public_payment_profile(conn)}
    finally:
        conn.close()


@router.post("/api/admin/payments/{order_id}/mark-paid")
async def admin_mark_payment_paid(order_id: int, request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    body = await request.json()
    conn = get_conn()
    try:
        result = _activate_payment(conn, order_id, payload={"manual": True, "by": "admin", **body})
        return {"ok": True, "result": result}
    finally:
        conn.close()


@router.get("/api/admin/signals/recent")
def admin_recent_signals(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    conn = get_conn()
    try:
        rows = conn.execute("SELECT * FROM saas_signal_inbox ORDER BY id DESC LIMIT 120").fetchall()
        return {"ok": True, "items": [{"id": int(r["id"]), "user_id": int(r["user_id"]), "strategy_code": r["strategy_code"], "headline": r["headline"], "confidence": float(r["confidence"] or 0), "status": r["status"], "created_at": r["created_at"]} for r in rows]}
    finally:
        conn.close()


@router.get("/api/admin/settings")
def admin_settings(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    conn = get_conn()
    try:
        rows = conn.execute("SELECT * FROM saas_app_settings ORDER BY key ASC").fetchall()
        items = []
        for r in rows:
            key = str(r["key"] or "")
            value = _loads(r["value_json"], {})
            if key == "payment_profile":
                value = _public_payment_profile(conn)
            items.append({"key": key, "value": value, "updated_at": r["updated_at"]})
        return {"ok": True, "items": items}
    finally:
        conn.close()


@router.post("/api/admin/settings")
async def admin_settings_upsert(request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    body = await request.json()
    key = str(body.get("key") or "").strip()
    value = body.get("value")
    if not key:
        raise HTTPException(status_code=400, detail="Setting key required")
    conn = get_conn()
    try:
        now = _utc_iso()
        if key == "payment_profile":
            value = _normalize_payment_profile(value)
        conn.execute("INSERT OR REPLACE INTO saas_app_settings(key,value_json,updated_at) VALUES(?,?,?)", (key, _dumps(value), now))
        conn.commit()
        return {"ok": True, "key": key, "value": value}
    finally:
        conn.close()


@router.get("/api/admin/gmail/oauth/status")
def admin_gmail_oauth_status(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    init_saas_db()
    conn = get_conn()
    try:
        return {"ok": True, "gmail": _gmail_runtime_status(conn)}
    finally:
        conn.close()


@router.post("/api/admin/gmail/oauth/start")
async def admin_gmail_oauth_start(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    admin = _require_admin(authorization)
    cfg = _gmail_oauth_cfg()
    if not _gmail_oauth_ready():
        raise HTTPException(status_code=400, detail="Gmail OAuth is not configured on server")
    state = _gmail_oauth_state_sign(
        {
            "kind": "gmail_oauth",
            "admin_id": int(admin.get("uid") or 0),
            "admin_email": str(admin.get("email") or "").strip().lower(),
            "exp": int(time.time()) + 900,
        }
    )
    params = {
        "client_id": cfg["client_id"],
        "redirect_uri": cfg["redirect_uri"],
        "response_type": "code",
        "scope": " ".join(cfg["scopes"]),
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
        "state": state,
    }
    auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)
    return {"ok": True, "url": auth_url, "state": state}


@router.get("/api/admin/gmail/oauth/callback")
def admin_gmail_oauth_callback(code: str = "", state: str = "", error: str = ""):
    if error:
        msg = html.escape(str(error))
        return HTMLResponse(
            "<html><body style='font-family:Segoe UI;background:#07111d;color:#e5eefc;padding:24px'>"
            "<div style='max-width:620px;margin:auto;background:#0d1b31;border:1px solid #1f3657;border-radius:18px;padding:24px'>"
            f"<h2 style='margin:0 0 10px'>Gmail OAuth failed</h2><p style='line-height:1.7;margin:0'>Google returned: {msg}. You can close this window and retry.</p>"
            "<script>try{if(window.opener){window.opener.postMessage({type:'nx-gmail-oauth',ok:false}, '*');}}catch(e){}</script>"
            "</div></body></html>",
            status_code=400,
        )
    payload = _gmail_oauth_state_verify(state)
    if str(payload.get("kind") or "") != "gmail_oauth":
        raise HTTPException(status_code=400, detail="Invalid OAuth state kind")
    if not str(code or "").strip():
        raise HTTPException(status_code=400, detail="OAuth code missing")
    cfg = _gmail_oauth_cfg()
    if not _gmail_oauth_ready():
        raise HTTPException(status_code=400, detail="Gmail OAuth not configured on server")
    try:
        token_resp = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": str(code).strip(),
                "client_id": cfg["client_id"],
                "client_secret": cfg["client_secret"],
                "redirect_uri": cfg["redirect_uri"],
                "grant_type": "authorization_code",
            },
            timeout=25,
        )
        token_data = token_resp.json() if token_resp.content else {}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"OAuth exchange failed: {exc}") from exc
    if not token_resp.ok or not token_data.get("access_token"):
        detail = str(token_data.get("error_description") or token_data.get("error") or "OAuth token exchange failed")
        raise HTTPException(status_code=502, detail=detail)
    access_token = str(token_data.get("access_token") or "").strip()
    refresh_token = str(token_data.get("refresh_token") or "").strip()
    expires_in = max(300, _safe_int(token_data.get("expires_in"), default=3600))
    token_type = str(token_data.get("token_type") or "Bearer").strip() or "Bearer"
    scope = str(token_data.get("scope") or "")
    oauth_email = _gmail_oauth_user_email(access_token) or str(payload.get("admin_email") or "")
    conn = get_conn()
    try:
        old = _gmail_oauth_row(conn)
        _gmail_oauth_save(
            conn,
            {
                "connected": True,
                "email": oauth_email,
                "access_token": access_token,
                "refresh_token": refresh_token or old.get("refresh_token") or "",
                "token_type": token_type,
                "scope": scope,
                "expires_at": int(time.time()) + expires_in,
                "updated_at": _utc_iso(),
                "error": "",
            },
        )
        conn.commit()
    finally:
        conn.close()
    close_html = (
        "<html><body style='font-family:Segoe UI;background:#07111d;color:#e5eefc;padding:24px'>"
        "<div style='max-width:620px;margin:auto;background:#0d1b31;border:1px solid #1f3657;border-radius:18px;padding:24px'>"
        "<h2 style='margin:0 0 10px'>Gmail connected successfully</h2>"
        "<p style='line-height:1.7;margin:0'>You can close this window and return to Nexus admin.</p>"
        "<script>try{if(window.opener){window.opener.postMessage({type:'nx-gmail-oauth',ok:true}, '*');}}catch(e){}setTimeout(function(){window.close();}, 700);</script>"
        "</div></body></html>"
    )
    return HTMLResponse(close_html)


@router.post("/api/admin/gmail/oauth/disconnect")
def admin_gmail_oauth_disconnect(authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    _require_admin(authorization)
    init_saas_db()
    conn = get_conn()
    try:
        _gmail_oauth_save(
            conn,
            {
                "connected": False,
                "email": "",
                "access_token": "",
                "refresh_token": "",
                "token_type": "Bearer",
                "scope": "",
                "expires_at": 0,
                "updated_at": _utc_iso(),
                "error": "",
            },
        )
        conn.commit()
        return {"ok": True, "gmail": _gmail_runtime_status(conn)}
    finally:
        conn.close()


@router.post("/api/admin/gmail/test")
async def admin_gmail_test(request: Request, authorization: Optional[str] = Header(None)) -> dict[str, Any]:
    admin = _require_admin(authorization)
    body = await request.json()
    to_email = str(body.get("email") or admin.get("email") or DEFAULT_ADMIN_EMAIL).strip().lower()
    if not to_email or "@" not in to_email:
        raise HTTPException(status_code=400, detail="Valid email required")
    status = _gmail_runtime_status()
    if not bool(status.get("ready")):
        raise HTTPException(status_code=400, detail="Gmail is not configured yet")
    sent = _send_admin_test_email(to_email)
    if not sent:
        raise HTTPException(status_code=502, detail="Failed to send Gmail test")
    return {"ok": True, "sent": True, "email": to_email, "brand": BRAND_NAME}


@router.post("/api/payments/razorpay/webhook")
async def razorpay_webhook(request: Request, x_razorpay_signature: Optional[str] = Header(None)) -> JSONResponse:
    init_saas_db()
    payload = await request.body()
    secret = (os.getenv("RAZORPAY_WEBHOOK_SECRET", "") or "").strip()
    if secret:
        digest = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
        if not x_razorpay_signature or not hmac.compare_digest(digest, x_razorpay_signature):
            raise HTTPException(status_code=401, detail="Invalid webhook signature")
    try:
        data = json.loads(payload.decode() or "{}")
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid webhook body") from exc
    event = str(data.get("event") or "")
    entity = (((data.get("payload") or {}).get("payment") or {}).get("entity") or {})
    order_ref = str(entity.get("order_id") or "")
    conn = get_conn()
    try:
        order = None
        if order_ref:
            order = conn.execute("SELECT * FROM saas_payment_orders WHERE provider_order_id=? ORDER BY id DESC LIMIT 1", (order_ref,)).fetchone()
        if order is None:
            return JSONResponse({"ok": True, "ignored": True, "reason": "order-not-found"})
        if event in {"payment.captured", "order.paid"}:
            result = _activate_payment(conn, int(order["id"]), payload=data)
            return JSONResponse({"ok": True, "result": result})
        return JSONResponse({"ok": True, "ignored": True, "event": event})
    finally:
        conn.close()
