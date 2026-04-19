/**
 * BNIFTY OPTIONS / NIFTY OPTIONS — synthetic 6m shadow book (separate from futures STATE.bt).
 * Option leg follows the same signal clock as the index future stack (research / UI demo).
 */
(function(){
  "use strict";
  var O = { underlying: "bnf", tab: "day_log", filter: "ALL", date: "ALL", bt: null };

  function el(id){ return document.getElementById(id); }
  function esc(v){
    return String(v == null ? "" : v).replace(/[&<>"']/g, function(m){
      return ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m]);
    });
  }
  function inr(n){
    n = Number(n) || 0;
    return (n >= 0 ? "₹+" : "₹−") + Math.abs(Math.round(n)).toLocaleString("en-IN");
  }
  function base(){
    var b = String(window._backendBase || "").replace(/\/$/, "");
    if (/\/api$/i.test(b)) b = b.slice(0, -4).replace(/\/$/, "");
    return b;
  }
  function req(path, noCache){
    var init = { headers: { "Content-Type": "application/json" } };
    if(noCache) init.cache = "no-store";
    return fetch(base() + path, init)
      .then(function(r){
        return r.json().then(function(d){
          if(!r.ok) throw new Error(d.detail || d.error || ("HTTP " + r.status));
          return d;
        });
      });
  }
  /** True when the options-shadow modal is visible (do not rely on inline style === "block" only). */
  function _optionsShadowPopupOpen(){
    var pop = el("opt-pop"), mask = el("opt-mask");
    if(!pop || !mask) return false;
    if(String(mask.style.display || "").toLowerCase() === "none") return false;
    try{
      var cs = window.getComputedStyle(pop);
      if(cs.display === "none" || cs.visibility === "hidden") return false;
    }catch(_e){
      if(String(pop.style.display || "").toLowerCase() === "none") return false;
    }
    return true;
  }
  function _todayIso(){
    var d = new Date();
    return d.toISOString().slice(0, 10);
  }
  function _daysAgoIso(days){
    var d = new Date();
    d.setDate(d.getDate() - Number(days || 180));
    return d.toISOString().slice(0, 10);
  }
  function _normYmd(x){ return String(x == null ? "" : x).trim().slice(0, 10); }

  function _clip(rows, a, b){
    if(!a || !b || a > b) return (rows || []).slice();
    return (rows || []).filter(function(r){
      var d = _normYmd(r.date || r.day || r.trade_date);
      return d && d >= a && d <= b;
    });
  }
  function _pill(txt, cls){
    var r = String(txt || "").toUpperCase();
    var c = r === "CHOP" ? "chop" : r === "BREAKOUT" ? "breakout" : r === "MIXED" ? "mixed" : "";
    return '<span class="opt-pill ' + c + '">' + esc(r || "—") + "</span>";
  }
  function _isLiveMode(){
    try{
      if(typeof window.getNseEdgeMarketMode === "function") return window.getNseEdgeMarketMode() === "live";
    }catch(_e){}
    return false;
  }
  function _syncOptModeStrip(){
    var s = el("opt-mode-strip");
    if(!s) return;
    if(_isLiveMode()){
      s.innerHTML = "<strong>LIVE</strong> — Same as <strong>MODE → LIVE</strong> on the main top bar. This book is still synthetic-only; use <strong>BACKTEST</strong> (buttons above) to load <code>/api/options-shadow/report</code>.";
    }else{
      s.innerHTML = "<strong>BACKTEST</strong> — Same as <strong>MODE → BACKTEST</strong> on the main top bar (left of BNIFTY FUTURE). Data from <code>/api/options-shadow/report</code> below.";
    }
  }
  function _syncOptModeButtons(){
    var mm = _isLiveMode();
    var L = el("opt-mode-live"), B = el("opt-mode-bt");
    if(L){
      L.style.background = mm ? "linear-gradient(135deg,#22c55e,#15803d)" : "transparent";
      L.style.color = mm ? "#ecfdf5" : "#a78bfa";
    }
    if(B){
      B.style.background = !mm ? "linear-gradient(135deg,#7c3aed,#5b21b6)" : "transparent";
      B.style.color = !mm ? "#f5f3ff" : "#a78bfa";
    }
  }
  function _maxDdFromDays(dayRows){
    var sorted = (dayRows || []).slice().sort(function(x, y){
      return String(x.date || "").localeCompare(String(y.date || ""));
    });
    var cum = 0, peak = 0, mdd = 0;
    sorted.forEach(function(r){
      cum += Number(r.day_pnl || 0);
      if(cum > peak) peak = cum;
      var dr = cum - peak;
      if(dr < mdd) mdd = dr;
    });
    return mdd;
  }
  function _summFromTrades(trades, dayLog){
    trades = trades || [];
    var tp = trades.reduce(function(a, t){ return a + Number(t.pnl_inr || 0); }, 0);
    var tw = trades.filter(function(t){ return Number(t.pnl_inr || 0) > 0; }).length;
    var tt = trades.length;
    var uniq = {};
    trades.forEach(function(t){
      var d = _normYmd(t.date);
      if(d) uniq[d] = 1;
    });
    var dn = Object.keys(uniq).length;
    var dl = dayLog && dayLog.length ? dayLog : Object.keys(uniq).sort().map(function(k){
      var p = trades.filter(function(t){ return _normYmd(t.date) === k; }).reduce(function(a, t){ return a + Number(t.pnl_inr || 0); }, 0);
      return { date: k, day_pnl: p };
    });
    if(!dn) dn = dl.length;
    return {
      total_pnl: tp,
      total_trades: tt,
      win_rate: tt ? Math.round(tw * 1000 / tt) / 10 : 0,
      days: dn,
      max_drawdown: _maxDdFromDays(dl),
    };
  }
  function _optByStrat(trades){
    var sm = {};
    (trades || []).forEach(function(t){
      var sid = String(t.strategy_id || "NA");
      if(!sm[sid]) sm[sid] = { strategy_id: sid, strategy_name: String(t.strategy_name || ""), trades: 0, wins: 0, pnl: 0 };
      var p = Number(t.pnl_inr || 0);
      sm[sid].trades += 1;
      sm[sid].pnl += p;
      if(p > 0) sm[sid].wins += 1;
    });
    return Object.keys(sm).sort(function(a, b){ return sm[b].pnl - sm[a].pnl; }).map(function(k){
      var x = sm[k];
      var n = Math.max(1, x.trades);
      return {
        strategy_id: x.strategy_id,
        strategy_name: x.strategy_name,
        trades: x.trades,
        win_rate: Math.round(x.wins / n * 1000) / 10,
        target_win_rate: 65,
        total_pnl: Math.round(x.pnl * 100) / 100,
        avg_pnl: Math.round(x.pnl / n * 100) / 100,
        best: 0,
        worst: 0,
      };
    });
  }
  function _optByReg(trades){
    var rm = {};
    (trades || []).forEach(function(t){
      var r = String(t.regime || "MIXED").toUpperCase();
      if(!rm[r]) rm[r] = { regime: r, days: {}, trades: 0, wins: 0, pnl: 0, top: {} };
      var p = Number(t.pnl_inr || 0);
      rm[r].days[_normYmd(t.date)] = 1;
      rm[r].trades += 1;
      rm[r].pnl += p;
      if(p > 0) rm[r].wins += 1;
      var sid = String(t.strategy_id || "NA");
      rm[r].top[sid] = (rm[r].top[sid] || 0) + 1;
    });
    return Object.keys(rm).sort().map(function(rk){
      var x = rm[rk];
      var n = Math.max(1, x.trades);
      var top = Object.keys(x.top).sort(function(a, b){ return x.top[b] - x.top[a]; }).slice(0, 5).map(function(k){ return { strategy_id: k, count: x.top[k] }; });
      return {
        regime: rk,
        days: Object.keys(x.days).length,
        trades: x.trades,
        win_rate: Math.round(x.wins / n * 1000) / 10,
        total_pnl: Math.round(x.pnl * 100) / 100,
        top_strategies: top,
      };
    });
  }

  function _optToolbarSetMode(m){
    if(typeof window.setNseEdgeMarketMode === "function") window.setNseEdgeMarketMode(m);
    else{
      try{ sessionStorage.setItem("nse_edge_market_mode", m === "live" ? "live" : "backtest"); }catch(_e){}
      loadOptReport(true);
    }
  }
  function _optCloseUi(){
    var pop = el("opt-pop"), mask = el("opt-mask");
    if(pop) pop.style.display = "none";
    if(mask) mask.style.display = "none";
  }
  /** Capture-phase: runs before bubbling handlers that might swallow clicks. */
  function _optChromePointerCapture(ev){
    var t = ev.target;
    if(!t || !t.closest) return;
    if(!t.closest("#opt-pop")) return;
    if(t.closest("#opt-close")){ ev.preventDefault(); ev.stopPropagation(); _optCloseUi(); return; }
    if(t.closest("#opt-refresh")){ ev.preventDefault(); ev.stopPropagation(); loadOptReport(true); return; }
    if(t.closest("#opt-load")){ ev.preventDefault(); ev.stopPropagation(); loadOptReport(true); return; }
    if(t.closest("#opt-mode-live")){ ev.preventDefault(); ev.stopPropagation(); _optToolbarSetMode("live"); return; }
    if(t.closest("#opt-mode-bt")){ ev.preventDefault(); ev.stopPropagation(); _optToolbarSetMode("backtest"); }
  }
  function _optDocPointerCapture(ev){
    if(!_optionsShadowPopupOpen()) return;
    var t = ev.target;
    if(t && t.closest && t.closest("#opt-pop")) return;
    _optCloseUi();
  }
  function _wireOptChrome(){
    var pop = el("opt-pop"), mask = el("opt-mask");
    if(!pop || !mask) return;
    el("opt-date-sel").onchange = function(){ O.date = String(this.value || "ALL"); renderOpt(); };
    if(!pop.dataset.optChromePtr){
      pop.dataset.optChromePtr = "1";
      pop.addEventListener("pointerdown", _optChromePointerCapture, true);
    }
    if(!window._optShadowDocPtrBound){
      window._optShadowDocPtrBound = 1;
      document.addEventListener("pointerdown", _optDocPointerCapture, true);
    }
  }
  function _ensureOptPopup(){
    var OPT_REV = "4";
    var stale = el("opt-root");
    if(stale && stale.getAttribute("data-opt-rev") !== OPT_REV){
      try{ stale.remove(); }catch(_e){}
    }
    if(el("opt-root")) return;
    var w = document.createElement("div");
    w.id = "opt-root";
    w.setAttribute("data-opt-rev", OPT_REV);
    w.innerHTML = ""
      + "<style>"
      + "#opt-root{position:relative;z-index:200050}"
      + "#opt-mask{position:fixed;inset:0;background:rgba(2,6,23,.62);backdrop-filter:blur(2px);z-index:0;display:none;pointer-events:none}"
      + "#opt-pop{position:fixed;left:10px;top:42px;width:calc(100vw - 20px);height:calc(100vh - 54px);overflow:auto;"
      + "background:radial-gradient(1200px 500px at 15% -15%,rgba(167,139,250,.14),transparent),linear-gradient(180deg,#0c0618,#070510 52%,#0a0616);"
      + "color:#e9e4ff;border-radius:14px;box-shadow:0 28px 95px rgba(2,6,23,.65);z-index:2;display:none;padding:16px 16px 20px;"
      + "border:1px solid rgba(167,139,250,.35);font-family:var(--fn,inherit);pointer-events:auto;isolation:isolate}"
      + ".opt-head{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:12px 16px;align-items:start}"
      + ".opt-head > div:first-child{min-width:0;overflow:hidden}"
      + ".opt-head-actions{position:relative;z-index:2;justify-self:end;pointer-events:auto;padding-right:4px;white-space:nowrap}"
      + "#opt-mode-live,#opt-mode-bt,.opt-btn,.opt-go{pointer-events:auto;touch-action:manipulation}"
      + ".opt-title{font-size:40px;font-weight:900;color:#f5f3ff;letter-spacing:.2px}"
      + ".opt-sub{font-size:12px;color:#c4b5fd;margin-top:4px;line-height:1.55;opacity:.9}"
      + ".opt-btn{border:1px solid rgba(148,163,184,.4);background:rgba(30,20,50,.75);border-radius:10px;padding:8px 12px;font-weight:800;font-size:11px;color:#ddd6fe;cursor:pointer;font-family:inherit}"
      + ".opt-toolbar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-top:12px;padding:10px;border:1px solid rgba(167,139,250,.25);border-radius:10px;background:rgba(20,10,40,.55)}"
      + ".opt-ctl b{font-size:10px;color:#a78bfa;letter-spacing:.5px}"
      + ".opt-date{height:28px;padding:0 8px;border-radius:7px;border:1px solid rgba(167,139,250,.4);background:rgba(8,4,20,.75);color:#ede9fe;font-size:11px;font-family:inherit}"
      + ".opt-go{border-radius:999px;padding:7px 14px;font-weight:900;font-size:10px;cursor:pointer;border:1px solid rgba(34,211,238,.55);"
      + "background:linear-gradient(135deg,rgba(139,92,246,.95),rgba(34,211,238,.85));color:#0b0220;font-family:inherit}"
      + ".opt-metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin-top:12px}"
      + ".opt-metric{padding:12px;border-radius:12px;border:1px solid rgba(167,139,250,.2);background:rgba(0,0,0,.28)}"
      + ".opt-metric.hero{grid-column:1/-1;border-color:rgba(167,139,250,.45);background:linear-gradient(125deg,rgba(76,29,149,.45),rgba(15,23,42,.55))}"
      + ".opt-k{font-size:10px;color:#c4b5fd;text-transform:uppercase;font-weight:700}"
      + ".opt-v{font-size:clamp(20px,3vw,34px);font-weight:900;color:#f5f3ff;margin-top:4px}"
      + ".opt-v.good{color:#4ade80}.opt-v.bad{color:#fb7185}"
      + ".opt-tabs{display:flex;gap:18px;border-bottom:1px solid rgba(167,139,250,.22);margin-top:14px}"
      + ".opt-tab{border:none;background:none;padding:8px 0;font-size:15px;color:#a78bfa;cursor:pointer;font-weight:600;font-family:inherit}"
      + ".opt-tab.on{color:#f5f3ff;border-bottom:2px solid #a78bfa;font-weight:800}"
      + ".opt-chip{border:1px solid rgba(167,139,250,.35);border-radius:999px;padding:5px 10px;font-size:11px;color:#ddd6fe;cursor:pointer;font-weight:700;background:rgba(20,10,40,.6)}"
      + ".opt-chip.on{background:#7c3aed;color:#fff;border-color:#a78bfa}"
      + ".opt-table{width:100%;border-collapse:collapse;margin-top:10px}"
      + ".opt-table th,.opt-table td{padding:8px 6px;border-bottom:1px solid rgba(167,139,250,.12);text-align:left;font-size:12px;vertical-align:top}"
      + ".opt-table th{color:#c4b5fd;font-size:10px;text-transform:uppercase;font-weight:800}"
      + "#opt-pop .opt-table th{color:#e9d5ff!important}"
      + "#opt-pop .opt-table td{color:#f5f3ff!important}"
      + "#opt-pop .opt-table td.opt-td{color:#f5f3ff!important}"
      + "#opt-pop .opt-table td.opt-pos{color:#4ade80!important;font-weight:800!important}"
      + "#opt-pop .opt-table td.opt-neg{color:#fb7185!important;font-weight:800!important}"
      + "#opt-pop .opt-stratnm{color:#f5f3ff!important;font-weight:600}"
      + ".opt-pill{display:inline-flex;padding:2px 8px;border-radius:999px;font-size:10px;font-weight:800;background:rgba(148,163,184,.18);color:#f8fafc}"
      + ".opt-pill.chop{background:rgba(234,179,8,.22);color:#fde68a}.opt-pill.breakout{background:rgba(34,197,94,.2);color:#bbf7d0}"
      + ".opt-pill.mixed{background:rgba(168,85,247,.22);color:#e9d5ff}"
      + ".opt-rowcard{display:flex;justify-content:space-between;gap:10px;border-bottom:1px solid rgba(167,139,250,.12);padding:10px 0}"
      + ".opt-tag{font-size:10px;background:rgba(99,102,241,.25);color:#e0e7ff;padding:2px 7px;border-radius:999px;font-weight:800;margin-right:4px}"
      + "html.light #opt-pop .opt-table td,html.light #opt-pop .opt-table td.opt-td{color:#1e1b4b!important}"
      + "html.light #opt-pop .opt-table th{color:#5b21b6!important}"
      + "html.light #opt-pop .opt-stratnm{color:#312e81!important}"
      + "html.light #opt-pop .opt-pill:not(.chop):not(.breakout):not(.mixed){background:rgba(99,102,241,.12);color:#3730a3}"
      + "</style>"
      + "<div id=\"opt-mask\"></div>"
      + "<div id=\"opt-pop\">"
      + "  <div class=\"opt-head\"><div>"
      + "    <div id=\"opt-title\" class=\"opt-title\">BNIFTY OPTIONS</div>"
      + "    <div id=\"opt-sub\" class=\"opt-sub\"></div>"
      + "  </div><div class=\"opt-head-actions\" style=\"display:flex;flex-direction:column;align-items:flex-end;gap:8px\">"
      + "    <div style=\"display:flex;align-items:center;gap:10px;flex-wrap:wrap;justify-content:flex-end\">"
      + "      <span style=\"font-size:8px;color:#c4b5fd;font-weight:800;letter-spacing:.75px\">MODE</span>"
      + "      <div style=\"display:inline-flex;border-radius:999px;border:1px solid rgba(167,139,250,.5);overflow:hidden;background:rgba(12,6,30,.9)\">"
      + "        <button type=\"button\" id=\"opt-mode-live\" style=\"border:none;padding:6px 12px;font-size:10px;font-weight:800;cursor:pointer;font-family:inherit;background:transparent;color:#a78bfa\">LIVE</button>"
      + "        <button type=\"button\" id=\"opt-mode-bt\" style=\"border:none;padding:6px 12px;font-size:10px;font-weight:800;cursor:pointer;font-family:inherit;background:transparent;color:#a78bfa\">BACKTEST</button>"
      + "      </div>"
      + "      <button type=\"button\" class=\"opt-btn\" id=\"opt-refresh\">Refresh</button>"
      + "      <button type=\"button\" class=\"opt-btn\" id=\"opt-close\">Close</button>"
      + "    </div>"
      + "  </div></div>"
      + "  <div id=\"opt-mode-strip\" style=\"margin-top:2px;padding:8px 0 10px;font-size:11px;line-height:1.45;color:#c4b5fd;border-bottom:1px solid rgba(167,139,250,.18)\"></div>"
      + "  <div class=\"opt-toolbar\">"
      + "    <div class=\"opt-ctl\"><b>FROM</b><input id=\"opt-from\" class=\"opt-date\" type=\"date\"></div>"
      + "    <div class=\"opt-ctl\"><b>TO</b><input id=\"opt-to\" class=\"opt-date\" type=\"date\"></div>"
      + "    <div class=\"opt-ctl\"><b>DATE</b><select id=\"opt-date-sel\" class=\"opt-date\"><option value=\"ALL\">ALL</option></select></div>"
      + "    <button type=\"button\" class=\"opt-go\" id=\"opt-load\">Load / refresh range</button>"
      + "  </div>"
      + "  <div id=\"opt-warn\" style=\"display:none;margin-top:10px;padding:10px;border-radius:10px;border:1px solid rgba(251,191,36,.35);background:rgba(60,40,10,.4);color:#fde68a;font-size:12px\"></div>"
      + "  <div class=\"opt-metrics\" id=\"opt-metrics\"></div>"
      + "  <div class=\"opt-tabs\" id=\"opt-tabs\">"
      + "    <button type=\"button\" class=\"opt-tab on\" data-v=\"day_log\">Day log</button>"
      + "    <button type=\"button\" class=\"opt-tab\" data-v=\"trade_detail\">Trade detail</button>"
      + "    <button type=\"button\" class=\"opt-tab\" data-v=\"by_strategy\">By strategy</button>"
      + "    <button type=\"button\" class=\"opt-tab\" data-v=\"by_regime\">By regime</button>"
      + "  </div>"
      + "  <div id=\"opt-filters\" style=\"display:flex;gap:8px;flex-wrap:wrap;margin-top:10px\"></div>"
      + "  <div id=\"opt-view\" style=\"margin-top:8px\"></div>"
      + "  <div id=\"opt-foot\" style=\"margin-top:12px;font-size:10px;color:#a78bfa;opacity:.75\"></div>"
      + "</div>";
    document.body.appendChild(w);
    el("opt-from").addEventListener("change", function(){ renderOpt(); });
    el("opt-to").addEventListener("change", function(){ renderOpt(); });
    el("opt-tabs").querySelectorAll(".opt-tab").forEach(function(b){
      b.onclick = function(){
        O.tab = String(this.getAttribute("data-v") || "day_log");
        O.filter = "ALL";
        el("opt-tabs").querySelectorAll(".opt-tab").forEach(function(x){ x.classList.remove("on"); });
        this.classList.add("on");
        renderOpt();
      };
    });
    _wireOptChrome();
    _syncOptModeButtons();
  }

  function _rowsByTab(bObj){
    var b = bObj || O.bt || {};
    if(O.tab === "day_log") return (b.day_log || []).slice();
    if(O.tab === "trade_detail") return (b.trade_detail || []).slice();
    if(O.tab === "by_strategy") return (b.by_strategy || []).slice();
    return (b.by_regime || []).slice();
  }
  function _filterKey(){
    if(O.tab === "day_log") return "regime";
    if(O.tab === "trade_detail") return "strategy_id";
    if(O.tab === "by_strategy") return "strategy_id";
    return "regime";
  }

  function renderOpt(){
    _ensureOptPopup();
    _syncOptModeStrip();
    _syncOptModeButtons();
    var b = O.bt || {};
    var fromV = _normYmd((el("opt-from") || {}).value);
    var toV = _normYmd((el("opt-to") || {}).value);
    var useClip = !!(fromV && toV && fromV <= toV);
    var trades0 = (b.trade_detail || []).slice();
    var days0 = (b.day_log || []).slice();
    var trades = useClip ? _clip(trades0, fromV, toV) : trades0;
    var days = useClip ? _clip(days0, fromV, toV) : days0;
    var selD = String(O.date || "ALL");
    var dateTrades = selD === "ALL" ? trades.slice() : trades.filter(function(t){ return _normYmd(t.date) === selD; });
    var s0 = b.summary || {};
    var sDisp = useClip ? _summFromTrades(trades, days) : {
      total_pnl: Number(s0.total_pnl || 0),
      total_trades: Number(s0.total_trades || 0),
      win_rate: Number(s0.win_rate || 0),
      days: Number(s0.days || 0),
      max_drawdown: Number(s0.max_drawdown || 0),
    };
    var bRender = useClip ? {
      ok: b.ok,
      source: b.source,
      run_key: b.run_key,
      anchor: b.anchor,
      underlying: b.underlying,
      data_warning: b.data_warning,
      from_date: fromV,
      to_date: toV,
      summary: {
        days: sDisp.days,
        total_trades: sDisp.total_trades,
        win_rate: sDisp.win_rate,
        total_pnl: sDisp.total_pnl,
        max_drawdown: sDisp.max_drawdown,
      },
      day_log: days,
      trade_detail: trades,
      by_strategy: _optByStrat(trades),
      by_regime: _optByReg(trades),
    } : b;

    var warn = el("opt-warn");
    if(warn){
      var wtxt = String(b.data_warning || "").trim();
      warn.style.display = wtxt ? "block" : "none";
      warn.textContent = wtxt;
    }

    var m = el("opt-metrics");
    if(m){
      var pnl = Number(sDisp.total_pnl || 0);
      var dd = Number(sDisp.max_drawdown || 0);
      var ddShow = dd <= 0 ? dd : -Math.abs(dd);
      var tt = Number(sDisp.total_trades || 0);
      var avg = tt ? pnl / tt : 0;
      var dayPnl = dateTrades.reduce(function(a, t){ return a + Number(t.pnl_inr || 0); }, 0);
      var fifthLab = selD === "ALL" ? "Avg P&amp;L / trade" : ("Day P&amp;L · " + esc(selD));
      var fifthVal = selD === "ALL" ? avg : dayPnl;
      var fifthCls = fifthVal >= 0 ? "good" : "bad";
      m.innerHTML = ""
        + "<div class=\"opt-metric hero\"><div class=\"opt-k\">Total P&amp;L" + (useClip ? " (FROM/TO)" : "") + "</div>"
        + "<div class=\"opt-v " + (pnl >= 0 ? "good" : "bad") + "\">" + esc(inr(pnl)) + "</div></div>"
        + "<div class=\"opt-metric\"><div class=\"opt-k\">Days</div><div class=\"opt-v\">" + esc(sDisp.days || 0) + "</div></div>"
        + "<div class=\"opt-metric\"><div class=\"opt-k\">Trades</div><div class=\"opt-v\">" + esc(sDisp.total_trades || 0) + "</div></div>"
        + "<div class=\"opt-metric\"><div class=\"opt-k\">Win rate</div><div class=\"opt-v good\">" + esc((sDisp.win_rate || 0) + "%") + "</div></div>"
        + "<div class=\"opt-metric\"><div class=\"opt-k\">Max drawdown</div><div class=\"opt-v bad\">" + esc(inr(ddShow)) + "</div></div>"
        + "<div class=\"opt-metric\"><div class=\"opt-k\">" + fifthLab + "</div><div class=\"opt-v " + fifthCls + "\">" + esc(inr(fifthVal)) + "</div></div>"
        + "<div class=\"opt-metric\"><div class=\"opt-k\">Window</div><div class=\"opt-v\" style=\"font-size:13px;font-weight:700\">"
        + esc(useClip ? fromV + " → " + toV : ((b.from_date || "—") + " → " + (b.to_date || "—")))
        + "</div><div class=\"opt-k\" style=\"margin-top:6px;opacity:.85\">" + (selD === "ALL" ? "DATE = single day drill-down" : esc(selD)) + "</div></div>";
    }

    var ds = el("opt-date-sel");
    if(ds){
      var dsrc = bRender.day_log || [];
      var opts = ["ALL"].concat(Array.from(new Set(dsrc.map(function(r){ return _normYmd(r.date || r.day); }).filter(Boolean))).sort().reverse());
      var cur = String(O.date || "ALL");
      if(opts.indexOf(cur) < 0) cur = "ALL";
      O.date = cur;
      ds.innerHTML = opts.map(function(d){
        return "<option value=\"" + esc(d) + "\"" + (d === cur ? " selected" : "") + ">" + esc(d) + "</option>";
      }).join("");
    }

    var rows = _rowsByTab(bRender);
    if(selD !== "ALL"){
      if(O.tab === "day_log") rows = rows.filter(function(r){ return _normYmd(r.date || r.day) === selD; });
      else if(O.tab === "trade_detail") rows = dateTrades.slice();
    }
    var key = _filterKey();
    var vals = [];
    if(key === "regime") vals = ["ALL", "TREND", "CHOP", "BREAKOUT", "MIXED", "UNCLASSIFIED"];
    else if(key === "strategy_id") vals = ["ALL", "S1", "S2", "S3"];
    else vals = ["ALL"].concat(Array.from(new Set(rows.map(function(r){ return String(r[key] || "").toUpperCase(); }).filter(Boolean))).sort());
    var fh = el("opt-filters");
    if(fh){
      fh.innerHTML = vals.map(function(v){
        return "<button type=\"button\" class=\"opt-chip" + (String(O.filter).toUpperCase() === v ? " on" : "") + "\" data-v=\"" + esc(v) + "\">" + esc(v) + "</button>";
      }).join("");
      fh.querySelectorAll(".opt-chip").forEach(function(c){
        c.onclick = function(){ O.filter = String(this.getAttribute("data-v") || "ALL"); renderOpt(); };
      });
    }
    var fl = String(O.filter || "ALL").toUpperCase();
    if(fl !== "ALL") rows = rows.filter(function(r){ return String(r[key] || "").toUpperCase() === fl; });

    var host = el("opt-view");
    if(!host) return;
    if(O.tab === "day_log"){
      host.innerHTML = "<table class=\"opt-table\"><thead><tr><th>Date</th><th>Regime</th><th>Strategies</th><th>Trades</th><th>W/L</th><th>Day P&amp;L</th><th>Best</th><th>Notes</th></tr></thead><tbody>"
        + (rows.length ? rows.map(function(r){
          var p = Number(r.day_pnl || 0);
          var tags = (r.strategies_used || []).map(function(x){ return "<span class=\"opt-tag\">" + esc(x) + "</span>"; }).join("");
          return "<tr><td class=\"opt-td\">" + esc(r.date || "") + "</td><td class=\"opt-td\">" + _pill(r.regime) + "</td><td class=\"opt-td\">" + tags + "</td><td class=\"opt-td\">" + esc(r.trades || 0) + "</td><td class=\"opt-td\">" + esc(r.wl || "—") + "</td>"
            + "<td class=\"" + (p >= 0 ? "opt-pos" : "opt-neg") + "\">" + esc(inr(p)) + "</td><td class=\"opt-td\">" + esc(inr(r.best_trade || 0)) + "</td><td class=\"opt-td\">" + esc(r.notes || "") + "</td></tr>";
        }).join("") : "<tr><td colspan=\"8\" style=\"color:#9ca3af\">No rows</td></tr>") + "</tbody></table>";
      return;
    }
    if(O.tab === "trade_detail"){
      host.innerHTML = "<table class=\"opt-table\"><thead><tr><th>Date</th><th>Instrument</th><th>Strategy</th><th>Regime</th><th>Fut dir</th><th>In</th><th>Entry ₹</th><th>SL ₹</th><th>Tgt ₹</th><th>Out</th><th>Exit ₹</th><th>Score</th><th>Outcome</th><th>P&amp;L</th></tr></thead><tbody>"
        + (rows.length ? rows.slice(0, 500).map(function(r){
          var p = Number(r.pnl_inr || 0);
          var ep = Number(r.entry_price || 0), xp = Number(r.exit_price || 0);
          var sl = Number(r.stop_loss != null ? r.stop_loss : r.sl_price || 0);
          var tg = Number(r.target_price != null ? r.target_price : r.target || 0);
          function px(n){ return n ? n.toFixed(2) : "—"; }
          return "<tr><td class=\"opt-td\">" + esc(r.date || "") + "</td><td class=\"opt-td\">" + esc(r.instrument || "") + "</td><td class=\"opt-td\"><span class=\"opt-tag\">" + esc(r.strategy_id || "") + "</span> <span class=\"opt-stratnm\">" + esc(r.strategy_name || "") + "</span></td><td class=\"opt-td\">" + _pill(r.regime) + "</td>"
            + "<td class=\"opt-td\">" + esc(r.dir || "") + "</td><td class=\"opt-td\">" + esc(r.entry_time || "") + "</td><td class=\"opt-td\">" + esc(px(ep)) + "</td><td class=\"opt-td\">" + esc(px(sl)) + "</td><td class=\"opt-td\">" + esc(px(tg)) + "</td><td class=\"opt-td\">" + esc(r.exit_time || "") + "</td><td class=\"opt-td\">" + esc(px(xp)) + "</td>"
            + "<td class=\"opt-td\">" + esc(Number(r.score || 0).toFixed(2)) + "</td><td class=\"opt-td\">" + esc(r.outcome || "") + "</td><td class=\"" + (p >= 0 ? "opt-pos" : "opt-neg") + "\">" + esc(inr(p)) + "</td></tr>";
        }).join("") : "<tr><td colspan=\"14\" style=\"color:#9ca3af\">No rows</td></tr>") + "</tbody></table>";
      return;
    }
    if(O.tab === "by_strategy"){
      host.innerHTML = rows.length ? rows.map(function(r){
        var p = Number(r.total_pnl || 0);
        return "<div class=\"opt-rowcard\"><div><span class=\"opt-tag\">" + esc(r.strategy_id || "") + "</span> <b>" + esc(r.strategy_name || "") + "</b>"
          + "<div style=\"font-size:11px;color:#c4b5fd;margin-top:4px\">" + esc(r.trades || 0) + " trades · WR " + esc(r.win_rate || 0) + "%</div></div>"
          + "<div style=\"font-weight:900;font-size:26px;color:" + (p >= 0 ? "#4ade80" : "#fb7185") + "\">" + esc(inr(p)) + "</div></div>";
      }).join("") : "<div style=\"color:#9ca3af\">No rows</div>";
      return;
    }
    host.innerHTML = rows.length ? rows.map(function(r){
      var p = Number(r.total_pnl || 0);
      var tags = (r.top_strategies || []).map(function(x){ return "<span class=\"opt-tag\">" + esc(x.strategy_id) + " " + esc(x.count) + "×</span>"; }).join("");
      return "<div class=\"opt-rowcard\"><div>" + _pill(r.regime) + "<div style=\"font-size:11px;color:#c4b5fd;margin-top:6px\">" + esc(r.days || 0) + " d · " + esc(r.trades || 0) + " tr · " + esc(r.win_rate || 0) + "% WR</div><div style=\"margin-top:6px\">" + tags + "</div></div>"
        + "<div style=\"font-weight:900;font-size:32px;color:" + (p >= 0 ? "#4ade80" : "#fb7185") + "\">" + esc(inr(p)) + "</div></div>";
    }).join("") : "<div style=\"color:#9ca3af\">No rows</div>";
  }

  function loadOptReport(showErr){
    _ensureOptPopup();
    _syncOptModeStrip();
    _syncOptModeButtons();
    if(_isLiveMode()){
      O.bt = {
        ok: true,
        source: "options_shadow_live_placeholder",
        run_key: "live-mode",
        anchor: O.underlying,
        underlying: O.underlying,
        from_date: "—",
        to_date: "—",
        lot_size_units: null,
        data_warning: "Toolbar is set to LIVE. A live option-chain shadow book is not wired to this endpoint yet — choose BACKTEST on the Mode bar to load the synthetic 6M report.",
        excluded_strategies: [],
        summary: { days: 0, total_trades: 0, win_rate: 0, total_pnl: 0, max_drawdown: 0 },
        day_log: [],
        trade_detail: [],
        by_strategy: [],
        by_regime: [],
      };
      if(el("opt-foot")) el("opt-foot").textContent = "Mode: LIVE — use BACKTEST for synthetic shadow data.";
      if(showErr && el("opt-warn")){
        el("opt-warn").style.display = "none";
        el("opt-warn").textContent = "";
      }
      renderOpt();
      return Promise.resolve();
    }
    var u = O.underlying;
    var s = _normYmd((el("opt-from") || {}).value);
    var e = _normYmd((el("opt-to") || {}).value);
    var q = "/api/options-shadow/report?underlying=" + encodeURIComponent(u);
    if(s && e) q += "&start_date=" + encodeURIComponent(s) + "&end_date=" + encodeURIComponent(e);
    return req(q, true)
      .then(function(d){
        O.bt = d;
        if(el("opt-foot")) el("opt-foot").textContent = "Run key: " + (d.run_key || "—") + " · Synthetic shadow · Not live Greeks";
        _syncOptModeStrip();
        renderOpt();
      })
      .catch(function(err){
        O.bt = { summary: {}, day_log: [], trade_detail: [], by_strategy: [], by_regime: [], data_warning: String(err.message || err) };
        if(showErr && el("opt-warn")){
          el("opt-warn").style.display = "block";
          el("opt-warn").textContent = "Load failed: " + (err.message || err);
        }
        renderOpt();
      });
  }

  function openOpt(u){
    O.underlying = u === "nifty" ? "nifty" : "bnf";
    O.tab = "day_log";
    O.filter = "ALL";
    O.date = "ALL";
    _ensureOptPopup();
    var rootLift = el("opt-root");
    if(rootLift && rootLift.parentNode) document.body.appendChild(rootLift);
    var title = el("opt-title");
    var sub = el("opt-sub");
    if(title) title.textContent = O.underlying === "nifty" ? "NIFTY OPTIONS" : "BNIFTY OPTIONS";
    if(sub){
      sub.innerHTML = O.underlying === "nifty"
        ? "Shadow option book on <strong>NIFTY</strong> entries (long CE on BUY signal, long PE on SELL). 6-month synthetic backtest — same strategy ids as the future view for comparison."
        : "Shadow option book on <strong>BANKNIFTY</strong> entries (long CE on BUY, long PE on SELL). 6-month synthetic backtest — aligned signal clock to BNIFTY future stack.";
    }
    if(el("opt-from")) el("opt-from").value = _daysAgoIso(182);
    if(el("opt-to")) el("opt-to").value = _todayIso();
    _wireOptChrome();
    el("opt-pop").style.display = "block";
    el("opt-mask").style.display = "block";
    return loadOptReport(false);
  }

  function mountOptButtons(){
    var legB = document.getElementById("is-bnf-opts-topbtn");
    var legN = document.getElementById("is-nifty-opts-topbtn");
    if(legB && legB.parentNode) legB.parentNode.removeChild(legB);
    if(legN && legN.parentNode) legN.parentNode.removeChild(legN);
    if(el("opt-btn-bnf")) return;
    var adv = document.getElementById("is-advcash-topbtn");
    var niftyFut = document.getElementById("is-pop-topbtn-nifty");
    var dayBtn = document.querySelector("button.tbbtn.primary[onclick*='openDayPerf']");
    var ref = adv && adv.parentNode ? adv : niftyFut && niftyFut.parentNode ? niftyFut : dayBtn && dayBtn.parentNode ? dayBtn : null;
    var parent = ref && ref.parentNode ? ref.parentNode : null;
    if(!parent) return;
    var b1 = document.createElement("button");
    b1.id = "opt-btn-bnf";
    b1.type = "button";
    b1.className = "is-tb-keep";
    b1.textContent = "BNIFTY OPTIONS";
    b1.style.cssText = "height:26px;padding:0 11px;margin-left:6px;border-radius:999px;border:1px solid rgba(167,139,250,.55);"
      + "background:linear-gradient(135deg,#4c1d95,#7c3aed);color:#f5f3ff;font-size:10px;font-weight:900;letter-spacing:.5px;font-family:var(--fn,inherit);cursor:pointer;"
      + "box-shadow:0 4px 12px rgba(124,58,237,.22)";
    b1.title = "Synthetic 6m option shadow on Bank Nifty signal clock";
    b1.onclick = function(){ openOpt("bnf"); };
    var b2 = document.createElement("button");
    b2.id = "opt-btn-nifty";
    b2.type = "button";
    b2.className = "is-tb-keep";
    b2.textContent = "NIFTY OPTIONS";
    b2.style.cssText = b1.style.cssText;
    b2.title = "Synthetic 6m option shadow on Nifty signal clock";
    b2.onclick = function(){ openOpt("nifty"); };
    parent.insertBefore(b1, ref.nextSibling);
    parent.insertBefore(b2, b1.nextSibling);
  }

  function mountExpiryToolbarButtons(){
    if(el("eo-btn-bnf-exp")) return;
    var b1 = el("opt-btn-bnf");
    var b2 = el("opt-btn-nifty");
    var parent = b1 && b1.parentNode && b2 && b2.parentNode && b1.parentNode === b2.parentNode ? b1.parentNode : null;
    if(!parent) return;
    var styBnf = "height:26px;padding:0 11px;margin-left:6px;border-radius:999px;border:1px solid rgba(245,158,11,.65);"
      + "background:linear-gradient(135deg,#92400e,#f59e0b);color:#1a0f00;font-size:10px;font-weight:900;letter-spacing:.5px;font-family:var(--fn,inherit);cursor:pointer;"
      + "box-shadow:0 4px 12px rgba(245,158,11,.28)";
    var styNfy = "height:26px;padding:0 11px;margin-left:6px;border-radius:999px;border:1px solid rgba(34,211,238,.55);"
      + "background:linear-gradient(135deg,#0f5c52,#14b8a6);color:#ecfdf5;font-size:10px;font-weight:900;letter-spacing:.5px;font-family:var(--fn,inherit);cursor:pointer;"
      + "box-shadow:0 4px 12px rgba(20,184,166,.25)";
    var eb = document.createElement("button");
    eb.id = "eo-btn-bnf-exp";
    eb.type = "button";
    eb.className = "is-tb-keep";
    eb.textContent = "BNIFTY EXPIRY";
    eb.style.cssText = styBnf;
    eb.title = "EXPIRY ONLY dashboard + Thursday synthetic backtest (NIFTY BANK)";
    eb.onclick = function(){
      window.open("/frontend-static/expiry_only.html?underlying=bnf&v=4&edge_mode=backtest&tab=backtest", "_blank");
    };
    var en = document.createElement("button");
    en.id = "eo-btn-nifty-exp";
    en.type = "button";
    en.className = "is-tb-keep";
    en.textContent = "NIFTY EXPIRY";
    en.style.cssText = styNfy;
    en.title = "EXPIRY ONLY dashboard + Thursday synthetic backtest (NIFTY 50)";
    en.onclick = function(){
      window.open("/frontend-static/expiry_only.html?underlying=nifty&v=4&edge_mode=backtest&tab=backtest", "_blank");
    };
    parent.insertBefore(eb, b2);
    parent.insertBefore(en, b2.nextSibling);
  }

  function tryMount(){
    try{
      if(typeof window.mountIntradayFutTopbar === "function") window.mountIntradayFutTopbar();
    }catch(_e){}
    mountOptButtons();
    mountExpiryToolbarButtons();
    if(!el("opt-btn-bnf")) setTimeout(tryMount, 400);
  }
  if(document.readyState === "loading") document.addEventListener("DOMContentLoaded", tryMount);
  else tryMount();
  window.addEventListener("load", tryMount);
  window.openBniftyOptionsShadow = function(){ return openOpt("bnf"); };
  window.openNiftyOptionsShadow = function(){ return openOpt("nifty"); };
  window.openBniftyExpiryDashboard = function(){
    window.open("/frontend-static/expiry_only.html?underlying=bnf&v=4&edge_mode=backtest&tab=backtest", "_blank");
  };
  window.openNiftyExpiryDashboard = function(){
    window.open("/frontend-static/expiry_only.html?underlying=nifty&v=4&edge_mode=backtest&tab=backtest", "_blank");
  };
  window._reloadOptionsShadowForMode = function(){
    if(_optionsShadowPopupOpen()) loadOptReport(false);
  };
})();
