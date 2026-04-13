"""
Playbook design API — mounted early from main.py (include_router).
Keeps /api/playbook-design/* out of the bottom of main.py so a truncated or stale
merge cannot silently drop these routes while the rest of the app still runs.
"""

from __future__ import annotations

import datetime
import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

import signals
from feed import get_all_prices

logger = logging.getLogger("playbook_routes")

router = APIRouter(tags=["playbook-design"])


@router.get("/api/playbook-design/ping")
async def playbook_ping():
    """Minimal endpoint — open in browser to verify this module is loaded."""
    return {"ok": True, "playbook_routes": True}


@router.get("/api/playbook-design/snapshot")
async def playbook_design_snapshot():
    try:
        import playbook_design as pbd

        snap = pbd.snapshot_from_live_state(signals.state, get_all_prices() or {})
        snap["checklist"] = pbd.get_checklist()
        snap["checklist_completion"] = pbd.checklist_completion_ratio()
        try:
            td = datetime.datetime.now(pbd.IST).date().isoformat()
            pbd.record_playbook_day(td, snap)
        except Exception as _e:
            logger.debug("playbook auto-record on snapshot: %s", _e)
        return JSONResponse(snap)
    except Exception as e:
        logger.error("playbook_design_snapshot: %s", e, exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/api/playbook-design/checklist")
async def playbook_design_checklist_get():
    try:
        import playbook_design as pbd

        return JSONResponse(
            {
                "items": pbd.get_checklist(),
                "completion": pbd.checklist_completion_ratio(),
            }
        )
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/playbook-design/checklist")
async def playbook_design_checklist_post(request: Request):
    try:
        import playbook_design as pbd

        body = await request.json()
        key = str(body.get("item_key") or "").strip()
        if not key:
            return JSONResponse({"error": "item_key required"}, status_code=400)
        ok = pbd.set_checklist_item(key, bool(body.get("checked")))
        return JSONResponse({"ok": ok, "completion": pbd.checklist_completion_ratio()})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/api/playbook-design/backtest")
async def playbook_design_backtest(days: int = 400):
    try:
        import playbook_design as pbd

        days = max(60, min(int(days), 1200))
        result = pbd.run_playbook_backtest(days)
        return JSONResponse(result)
    except Exception as e:
        logger.error("playbook_design_backtest: %s", e, exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/api/playbook-design/history")
async def playbook_design_history(limit: int = 60):
    try:
        import playbook_design as pbd

        return JSONResponse({"rows": pbd.fetch_playbook_history(limit)})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/playbook-design/record")
async def playbook_design_record(request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    try:
        import playbook_design as pbd

        td = str((body or {}).get("trade_date") or "").strip()[:10]
        if not td:
            td = datetime.datetime.now(pbd.IST).date().isoformat()
        if (body or {}).get("snapshot"):
            payload = body["snapshot"]
        else:
            payload = pbd.snapshot_from_live_state(signals.state, get_all_prices() or {})
        pbd.record_playbook_day(td, payload)
        return JSONResponse({"ok": True, "trade_date": td, "primary": (payload.get("selection") or {}).get("primary")})
    except Exception as e:
        logger.error("playbook_design_record: %s", e, exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)
