(function(){
  var MODE_KEY = "intraday_system_report_mode";
  /** Toolbar + popups: `live` | `backtest` (persisted). */
  var MARKET_MODE_KEY = "nse_edge_market_mode";
  var STATE = { snap: null, bt: null, btSource: "legacy", febt: null, feRuns: [], reportMode: "legacy", popTab: "day_log", popFilter: "ALL", popDate: "ALL", timer: null, cashBt: null, cashPopTab: "day_log", cashPopFilter: "ALL", cashPopDate: "ALL", futAnchor: "bnf" };

  function el(id){ return document.getElementById(id); }
  function esc(v){ return String(v == null ? "" : v).replace(/[&<>"']/g, function(m){ return ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]); }); }
  function _parseExcludedFromRunKey(rk){
    var parts = String(rk || "").split("-");
    if(parts.length < 2) return [];
    var tail = parts[parts.length - 1];
    if(!/^\d{10,}$/.test(tail)) return [];
    var tag = parts[parts.length - 2];
    if(typeof tag !== "string" || tag.indexOf("no") !== 0 || tag.indexOf("S") < 0) return [];
    var m = tag.match(/S\d+/g);
    return m ? m.slice().sort() : [];
  }
  function _anchorFromRunKey(rk){
    var p = String(rk || "").split("-");
    if(p.length >= 3 && (p[p.length - 3] === "bnf" || p[p.length - 3] === "nifty")) return p[p.length - 3];
    return "bnf";
  }
  function _getMarketMode(){
    try{
      var v = String(sessionStorage.getItem(MARKET_MODE_KEY) || "backtest").toLowerCase();
      return v === "live" ? "live" : "backtest";
    }catch(_e){
      return "backtest";
    }
  }
  function _updateFutPopupModeBanner(){
    var s = el("is-pop-mode-strip");
    if(!s) return;
    s.style.display = "block";
    if(_getMarketMode() === "live"){
      s.innerHTML = "<strong>LIVE</strong> — Same as <strong>MODE → LIVE</strong> on the main top bar. Shows the latest <strong>saved</strong> full-engine snapshot for this anchor (<code>/api/intraday-system/full-engine/report</code>). Use <strong>Load / refresh range</strong> or <strong>RUN BACKTEST</strong> for a dated refresh.";
    }else{
      s.innerHTML = "<strong>BACKTEST</strong> — Same as <strong>MODE → BACKTEST</strong> on the main top bar (left of BNIFTY FUTURE). Pick FROM/TO, then <strong>Load / refresh range</strong> or <strong>RUN BACKTEST</strong> (Shift+click forces a new Kite replay — slow).";
    }
  }
  function _updateAcdModeStrip(){
    var s = _el("acd-mode-strip");
    if(!s) return;
    if(_getMarketMode() === "live"){
      s.innerHTML = "<strong>LIVE</strong> — Summary cards refresh from the server; BT tabs still use DB / demo rows for the selected dates.";
    }else{
      s.innerHTML = "<strong>BACKTEST</strong> — Focus on date filters + BT Trades / By Stock / By Window for stored or demo backtest rows.";
    }
  }
  function _syncFutPopModeTog(){
    var mm = _getMarketMode();
    var L = el("is-fut-pop-mode-live"), B = el("is-fut-pop-mode-bt");
    if(L){
      var onL = mm === "live";
      L.style.background = onL ? "linear-gradient(135deg,#22c55e,#15803d)" : "transparent";
      L.style.color = onL ? "#ecfdf5" : "#7dd3fc";
      L.style.fontWeight = onL ? "900" : "600";
    }
    if(B){
      var onB = mm === "backtest";
      B.style.background = onB ? "linear-gradient(135deg,#7c3aed,#5b21b6)" : "transparent";
      B.style.color = onB ? "#f5f3ff" : "#7dd3fc";
      B.style.fontWeight = onB ? "900" : "600";
    }
  }
  function _syncFutPopupInfoBox(){
    var box = el("is-pop-fut-info");
    if(!box) return;
    var anc = String(STATE.futAnchor || "bnf") === "nifty" ? "NIFTY 50" : "NIFTY BANK";
    box.innerHTML =
      "<strong>Index futures full-engine</strong> — Minute-path replay and ₹ P&amp;L from saved Kite runs for <strong>" + esc(anc) + "</strong>. "
      + "Same layout vibe as <strong>NIFTY / BNIFTY OPTIONS</strong> (synthetic shadow on <code>/api/options-shadow/report</code>), but this panel is the real futures stack. "
      + "Win rate follows the loaded run — not a fixed target.";
  }
  function _setMarketMode(m){
    try{
      sessionStorage.setItem(MARKET_MODE_KEY, m === "live" ? "live" : "backtest");
    }catch(_e){}
    _syncMarketModeToggleUI();
    _syncFutPopModeTog();
    _updateFutPopupModeBanner();
    _updateAcdModeStrip();
    if(el("is-pop") && el("is-pop").style.display === "block"){
      try{ renderPopupTracker(); }catch(_e2){}
    }
    if(el("is-cash-pop") && el("is-cash-pop").style.display === "block"){
      try{ renderCashTracker(); }catch(_e3){}
    }
    try{
      if(typeof window._reloadOptionsShadowForMode === "function") window._reloadOptionsShadowForMode();
    }catch(_e4){}
  }
  function _syncMarketModeToggleUI(){
    var mm = _getMarketMode();
    var L = el("is-mode-live"), B = el("is-mode-bt");
    if(L){
      var onL = mm === "live";
      L.style.background = onL ? "linear-gradient(135deg,#22c55e,#15803d)" : "transparent";
      L.style.color = onL ? "#ecfdf5" : "#94a3b8";
      L.style.fontWeight = onL ? "900" : "600";
    }
    if(B){
      var onB = mm === "backtest";
      B.style.background = onB ? "linear-gradient(135deg,#6366f1,#5b21b6)" : "transparent";
      B.style.color = onB ? "#f5f3ff" : "#94a3b8";
      B.style.fontWeight = onB ? "900" : "600";
    }
  }
  window.getNseEdgeMarketMode = _getMarketMode;
  window.setNseEdgeMarketMode = _setMarketMode;
  function _feRunsUrl(limit, anchorOpt){
    var lim = Number(limit || 40);
    var q = "/api/intraday-system/full-engine/runs?limit=" + encodeURIComponent(String(lim));
    var a = anchorOpt != null ? String(anchorOpt) : String(STATE.futAnchor || "bnf");
    if(a === "nifty" || a === "bnf") q += "&anchor=" + encodeURIComponent(a);
    return q;
  }
  function _emptyFullEngineState(anchor){
    var a = anchor === "nifty" ? "nifty" : "bnf";
    return {
      source: "full_engine_3m",
      run_key: "",
      anchor: a,
      excluded_strategies: [],
      from_date: "—",
      to_date: "—",
      data_warning: "No saved full-engine run for this anchor in the database. Set FROM/TO above and press RUN BACKTEST to store one.",
      summary: { days: 0, total_trades: 0, win_rate: 0, total_pnl: 0, max_drawdown: 0 },
      day_log: [],
      trade_detail: [],
      by_strategy: [],
      by_regime: []
    };
  }
  function _trackerAnchorFromState(){
    var b = STATE.bt || {};
    if(String(b.source || "") !== "full_engine_3m") return "";
    var rk = String(b.run_key || "");
    if(rk) return _anchorFromRunKey(rk);
    return String(b.anchor || "");
  }
  function syncTrackerToCurrentAnchor(){
    var want = String(STATE.futAnchor || "bnf");
    var have = _trackerAnchorFromState();
    if(have === want) return Promise.resolve();
    STATE.popDate = "ALL";
    STATE.popFilter = "ALL";
    return loadFullEngineRuns(false, want).then(function(){
      if(STATE.feRuns && STATE.feRuns.length){
        return loadFullEngineReport(STATE.feRuns[0].run_key);
      }
      renderBacktest(_emptyFullEngineState(want));
      return null;
    });
  }
  function base(){
    var b = String(window._backendBase || "").replace(/\/$/, "");
    // Paths here are `/api/...`. If `nse_edge_api_base` (or meta) ends with `/api`, requests become `/api/api/...` (404).
    if (/\/api$/i.test(b)) b = b.slice(0, -4).replace(/\/$/, "");
    return b;
  }
  function req(path, opts){
    opts = opts || {};
    var h = Object.assign({"Content-Type":"application/json"}, opts.headers || {});
    return fetch(base() + path, Object.assign({}, opts, { headers: h }))
      .then(function(r){
        return r.json().then(function(d){
          if(!r.ok) throw new Error(d.detail || d.error || ("HTTP " + r.status));
          return d;
        });
      });
  }
  /** fetch + JSON; does not throw on HTTP error (use for endpoints that return 409 with JSON body). */
  function reqLoose(path, opts){
    opts = opts || {};
    var h = Object.assign({"Content-Type":"application/json"}, opts.headers || {});
    return fetch(base() + path, Object.assign({}, opts, { headers: h }))
      .then(function(r){
        return r.json().then(function(d){ return { httpOk: r.ok, status: r.status, d: d }; });
      });
  }
  function regimeCls(r){
    if(r === "TREND") return "good";
    if(r === "CHOP") return "bad";
    if(r === "BREAKOUT") return "warn";
    return "warn";
  }
  function pill(txt, cls){ return '<span class="is-pill ' + (cls || "") + '">' + esc(txt) + '</span>'; }

  function renderShell(){
    var host = el("intraday-system-root");
    if(!host) return;
    host.innerHTML = ''
      + '<style>'
      + '.is-wrap{display:grid;gap:10px}'
      + '.is-card{border:1px solid rgba(255,255,255,.12);background:rgba(7,13,28,.75);border-radius:12px;padding:12px}'
      + '.is-title{font-weight:900;letter-spacing:.6px;color:var(--t0);font-size:13px}'
      + '.is-sub{color:var(--t2);font-size:11px;line-height:1.45;margin-top:4px}'
      + '.is-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin-top:10px}'
      + '.is-lbl{font-size:10px;color:var(--t2);margin-bottom:4px;letter-spacing:.5px}'
      + '.is-input,.is-select,.is-text{width:100%;border:1px solid rgba(255,255,255,.14);background:rgba(2,6,23,.66);color:var(--t0);padding:7px 8px;border-radius:8px;font-size:12px;outline:none}'
      + '.is-text{min-height:72px;resize:vertical}'
      + '.is-btn{border:1px solid rgba(34,211,238,.45);background:rgba(34,211,238,.12);color:#67e8f9;border-radius:8px;padding:7px 10px;font-weight:800;cursor:pointer}'
      + '.is-btn.ghost{border-color:rgba(255,255,255,.25);color:var(--t1);background:transparent}'
      + '.is-kpi{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}'
      + '.is-pill{border:1px solid rgba(255,255,255,.16);border-radius:999px;padding:4px 10px;font-size:11px;color:var(--t1)}'
      + '.is-pill.good{border-color:rgba(16,185,129,.55);color:#34d399}'
      + '.is-pill.warn{border-color:rgba(245,158,11,.55);color:#fbbf24}'
      + '.is-pill.bad{border-color:rgba(248,113,113,.55);color:#f87171}'
      + '.is-row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}'
      + '.is-matrix{width:100%;border-collapse:collapse;margin-top:8px}'
      + '.is-matrix th,.is-matrix td{border-bottom:1px solid rgba(255,255,255,.08);padding:6px 4px;font-size:11px;text-align:left;vertical-align:top}'
      + '.is-matrix th{color:var(--t2);font-size:10px;letter-spacing:.5px}'
      + '.is-sect{margin-top:6px;font-size:11px;color:#a5b4fc;font-weight:800;letter-spacing:.4px}'
      + '.is-note{font-size:11px;color:var(--t2);margin-top:8px;line-height:1.45}'
      + '.is-check{display:grid;gap:6px;margin-top:8px}'
      + '.is-check-row{display:flex;justify-content:space-between;gap:8px;border:1px solid rgba(255,255,255,.08);border-radius:8px;padding:7px 8px;font-size:11px}'
      + '.is-check-status{font-weight:800}'
      + '.is-check-status.DONE{color:#34d399}.is-check-status.PENDING{color:#cbd5e1}.is-check-status.LOCKED{color:#fbbf24}'
      + '.is-rpt-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px}'
      + '.is-rpt-card{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.10);border-radius:12px;padding:10px}'
      + '.is-rpt-k{font-size:11px;color:var(--t2)}'
      + '.is-rpt-v{font-size:22px;font-weight:900;color:var(--t0);line-height:1.1;margin-top:4px}'
      + '.is-rpt-v.good{color:#15803d}.is-rpt-v.bad{color:#dc2626}'
      + '.is-rpt-tabs{display:flex;gap:18px;border-bottom:1px solid rgba(255,255,255,.14);margin-top:12px}'
      + '.is-rpt-tab{background:none;border:none;color:var(--t1);padding:8px 0;font-size:14px;font-weight:700;cursor:pointer;position:relative}'
      + '.is-rpt-tab.on{color:var(--t0)}'
      + '.is-rpt-tab.on:after{content:"";position:absolute;left:0;right:0;bottom:-1px;height:3px;background:#111827;border-radius:3px}'
      + '.is-rpt-filters{display:flex;gap:10px;flex-wrap:wrap;margin-top:10px}'
      + '.is-rpt-chip{border:1px solid rgba(255,255,255,.20);background:transparent;color:var(--t0);border-radius:12px;padding:8px 12px;font-size:13px;font-weight:700;cursor:pointer}'
      + '.is-rpt-chip.on{border-color:#111827;box-shadow:inset 0 0 0 1px #111827}'
      + '.is-badge{display:inline-flex;align-items:center;padding:2px 10px;border-radius:999px;font-size:12px;font-weight:800;background:#dbeafe;color:#1e3a8a}'
      + '.is-reg-box{border:1px solid rgba(255,255,255,.08);border-radius:12px;background:rgba(255,255,255,.04);padding:10px;margin-top:8px}'
      + '.is-strategy-tags{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}'
      + '.is-tag{font-size:12px;background:#e2e8f0;color:#475569;padding:2px 8px;border-radius:999px;font-weight:700}'
      + '.is-split{display:grid;grid-template-columns:1fr 1fr;gap:10px}'
      + '@media(max-width:980px){.is-split{grid-template-columns:1fr}}'
      + '.is-mode-tabs{display:flex;gap:10px;flex-wrap:wrap;margin-top:10px}'
      + '.is-mode-tab{border:1px solid rgba(255,255,255,.20);background:transparent;color:var(--t0);border-radius:10px;padding:7px 12px;font-size:12px;font-weight:800;cursor:pointer}'
      + '.is-mode-tab.on{border-color:#111827;box-shadow:inset 0 0 0 1px #111827}'
      + '</style>'
      + '<div class="is-wrap">'
      + '  <div class="is-card">'
      + '    <div class="is-title">NSE INTRADAY SYSTEM (END-TO-END · SEPARATE)</div>'
      + '    <div class="is-sub">Live regime engine from app market data: ORB (9:15-9:30), India VIX, and bank breadth. Uses PDF mapping: TREND / CHOP / BREAKOUT -> active strategy basket.</div>'
      + '    <div class="is-grid">'
      + '      <div><div class="is-lbl">AUTO ORB STATUS</div><div id="is-orb-auto" class="is-input" style="display:flex;align-items:center;min-height:34px">—</div></div>'
      + '      <div><div class="is-lbl">AUTO VIX</div><div id="is-vix-auto" class="is-input" style="display:flex;align-items:center;min-height:34px">—</div></div>'
      + '      <div><div class="is-lbl">AUTO BREADTH</div><div id="is-br-auto" class="is-input" style="display:flex;align-items:center;min-height:34px">—</div></div>'
      + '      <div><div class="is-lbl">REGIME</div><div id="is-regime-auto" class="is-input" style="display:flex;align-items:center;min-height:34px">—</div></div>'
      + '    </div>'
      + '    <div class="is-kpi" id="is-kpi"></div>'
      + '    <div class="is-grid" style="margin-top:12px">'
      + '      <label><div class="is-lbl">MANUAL ORB BREAK</div><select id="is-orb" class="is-select"><option value="">AUTO</option><option value="yes">YES</option><option value="no">NO</option></select></label>'
      + '      <label><div class="is-lbl">MANUAL VIX</div><input id="is-vix" class="is-input" type="number" min="0" step="0.1" placeholder="AUTO"></label>'
      + '      <label><div class="is-lbl">MANUAL BREADTH %</div><input id="is-br" class="is-input" type="number" min="0" max="100" step="0.1" placeholder="AUTO"></label>'
      + '      <div class="is-row" style="align-items:flex-end"><button id="is-run" class="is-btn" type="button">CLASSIFY + SAVE</button><button id="is-refresh" class="is-btn ghost" type="button">REFRESH</button></div>'
      + '    </div>'
      + '    <div class="is-note">Manual inputs are optional overrides. Leave blank to classify using live feed values only.</div>'
      + '  </div>'
      + '  <div class="is-card"><div class="is-title">Daily Operating Checklist</div><div id="is-checklist" class="is-check"></div></div>'
      + '  <div class="is-card">'
      + '    <div class="is-title">Session Notes (saved in backtest.db)</div>'
      + '    <textarea id="is-notes" class="is-text" placeholder="Why regime chosen, what was suppressed, execution notes..."></textarea>'
      + '    <div class="is-row" style="margin-top:8px"><button id="is-save-note" class="is-btn" type="button">SAVE NOTE</button><span id="is-note-status" class="is-note"></span></div>'
      + '  </div>'
      + '  <div class="is-card">'
      + '    <div class="is-mode-tabs">'
      + '      <button id="is-mode-legacy" type="button" class="is-mode-tab on">Legacy Report</button>'
      + '      <button id="is-mode-full" type="button" class="is-mode-tab">Full Engine</button>'
      + '    </div>'
      + '  </div>'
      + '  <div id="is-legacy-wrap" class="is-card">'
      + '    <div class="is-title">Backtest Report</div>'
      + '    <div class="is-grid">'
      + '      <label><div class="is-lbl">LOOKBACK DAYS</div><input id="is-bt-days" class="is-input" type="number" min="20" max="1200" step="10" value="90"></label>'
      + '      <div class="is-row" style="align-items:flex-end"><button id="is-run-bt" class="is-btn" type="button">RUN BACKTEST</button></div>'
      + '    </div>'
      + '    <div id="is-bt-summary" class="is-rpt-cards" style="margin-top:12px"></div>'
      + '    <div class="is-rpt-tabs">'
      + '      <button type="button" class="is-rpt-tab on" data-v="day_log">Day log</button>'
      + '      <button type="button" class="is-rpt-tab" data-v="trade_detail">Trade detail</button>'
      + '      <button type="button" class="is-rpt-tab" data-v="by_strategy">By strategy</button>'
      + '      <button type="button" class="is-rpt-tab" data-v="by_regime">By regime</button>'
      + '    </div>'
      + '    <div id="is-rpt-filters" class="is-rpt-filters"></div>'
      + '    <div id="is-rpt-view" style="margin-top:8px"></div>'
      + '    <div id="is-bt-note" class="is-note"></div>'
      + '  </div>'
      + '  <div id="is-full-wrap" class="is-card" style="display:none">'
      + '    <div class="is-title">Full Engine Backtest (Separate Replay Engine)</div>'
      + '    <div class="is-note">This runs the newly isolated engine and stores results in dedicated tables only.</div>'
      + '    <div class="is-grid">'
      + '      <label><div class="is-lbl">START DATE</div><input id="is-fe-start" class="is-input" type="date"></label>'
      + '      <label><div class="is-lbl">END DATE</div><input id="is-fe-end" class="is-input" type="date"></label>'
      + '      <div class="is-row" style="align-items:flex-end"><button id="is-run-fe" class="is-btn" type="button">RUN FULL ENGINE</button><button id="is-refresh-fe" class="is-btn ghost" type="button">REFRESH RUNS</button></div>'
      + '    </div>'
      + '    <div class="is-grid" style="margin-top:8px">'
      + '      <label><div class="is-lbl">PAST RUNS</div><select id="is-fe-runs" class="is-select"></select></label>'
      + '      <div class="is-row" style="align-items:flex-end"><button id="is-load-fe" class="is-btn ghost" type="button">LOAD SELECTED RUN</button></div>'
      + '    </div>'
      + '    <div id="is-fe-summary" class="is-rpt-cards" style="margin-top:12px"></div>'
      + '    <div id="is-fe-excluded" class="is-note" style="display:none;margin-top:8px"></div>'
      + '    <div class="is-split" style="margin-top:8px">'
      + '      <div><div class="is-lbl">DAY LOG</div><div id="is-fe-day"></div></div>'
      + '      <div><div class="is-lbl">BY STRATEGY</div><div id="is-fe-strategy"></div></div>'
      + '    </div>'
      + '    <div class="is-lbl" style="margin-top:12px">LATEST TRADES (up to 200)</div>'
      + '    <div id="is-fe-trades"></div>'
      + '    <div id="is-fe-note" class="is-note"></div>'
      + '    <div style="margin-top:14px;padding-top:12px;border-top:1px solid rgba(255,255,255,.12)">'
      + '      <div class="is-lbl">LEAD_LAG IMBALANCE ENGINE (LIE)</div>'
      + '      <div class="is-note" style="margin-bottom:8px">Imbalance (banks + lag) -> dwell confirm -> pullback -> expansion entry. Results in lie_engine_* tables.</div>'
      + '      <div class="is-row" style="align-items:flex-end;gap:8px;flex-wrap:wrap">'
      + '        <button id="is-run-lie" class="is-btn" type="button">RUN LIE BACKTEST</button>'
      + '        <button id="is-refresh-lie" class="is-btn ghost" type="button">REFRESH LIE RUNS</button>'
      + '      </div>'
      + '      <label style="display:block;margin-top:8px"><div class="is-lbl">LIE PAST RUNS</div><select id="is-lie-runs" class="is-select"></select></label>'
      + '      <div class="is-row" style="margin-top:8px"><button id="is-load-lie" class="is-btn ghost" type="button">LOAD LIE REPORT</button></div>'
      + '      <div id="is-lie-summary" class="is-rpt-cards" style="margin-top:10px"></div>'
      + '      <div class="is-lbl" style="margin-top:10px">LIE TRADES</div>'
      + '      <div id="is-lie-trades"></div>'
      + '      <div id="is-lie-note" class="is-note"></div>'
      + '    </div>'
      + '  </div>'
      + '  <div class="is-card"><div class="is-title">Regime Strategy Matrix (S1-S15)</div><div id="is-matrix"></div>'
      + '    <div class="is-note"><b>Rule:</b> flat by 15:15, no fresh entries after 14:30. Classify first, activate second, enter third.</div>'
      + '  </div>'
      + '</div>';
  }

  function renderMatrix(m){
    var host = el("is-matrix");
    if(!host) return;
    var sections = ["TREND","CHOP","BREAKOUT","ADVANCED"];
    host.innerHTML = sections.map(function(k){
      var rows = m && m[k] ? m[k] : [];
      return '<div class="is-sect">' + esc(k + " DAY") + '</div>'
        + '<table class="is-matrix"><thead><tr><th>ID</th><th>Strategy</th><th>Window</th><th>Expected Accuracy</th></tr></thead><tbody>'
        + rows.map(function(r){
          return '<tr><td>' + esc(r.id) + '</td><td>' + esc(r.name) + '</td><td>' + esc(r.window) + '</td><td>' + esc(r.acc) + '</td></tr>';
        }).join("")
        + '</tbody></table>';
    }).join("");
  }

  function renderChecklist(list){
    var host = el("is-checklist");
    if(!host) return;
    host.innerHTML = (list || []).map(function(x){
      var st = String(x.status || "PENDING").toUpperCase();
      return '<div class="is-check-row"><span><b>' + esc(x.time || "") + '</b> · ' + esc(x.label || "") + '</span><span class="is-check-status ' + st + '">' + st + '</span></div>';
    }).join("");
  }

  function renderSnapshot(s){
    STATE.snap = s || {};
    var inp = (s || {}).inputs || {};
    var orb = inp.orb || {};
    var br = inp.breadth || {};
    var reg = String((s || {}).regime || "UNCLASSIFIED");
    var active = (s || {}).active_strategies || [];
    if(el("is-orb-auto")) el("is-orb-auto").innerHTML = "High " + esc(orb.high != null ? orb.high : "—") + " · Low " + esc(orb.low != null ? orb.low : "—") + " · " + (orb.orb_break ? ("BREAK " + esc(orb.direction || "")) : "NO BREAK");
    if(el("is-vix-auto")) el("is-vix-auto").textContent = (inp.vix != null ? inp.vix : "—") + " (" + (inp.vix_band || "UNKNOWN") + ")";
    if(el("is-br-auto")) el("is-br-auto").textContent = (br.pct != null ? br.pct : "—") + "%  [" + (br.count || 0) + "/" + (br.total || 0) + "]";
    if(el("is-regime-auto")) el("is-regime-auto").innerHTML = '<span class="is-pill ' + regimeCls(reg) + '">' + esc(reg) + '</span>';
    if(el("is-kpi")){
      el("is-kpi").innerHTML = ''
        + pill("Trade Date: " + ((s || {}).trade_date || "—"))
        + pill("Now IST: " + ((s || {}).now_ist || "—"))
        + pill("Active: " + (active.length ? active.join(", ") : "NONE"), active.length ? "good" : "warn")
        + pill("ORB Locked: " + (orb.locked ? "YES" : "NO"), orb.locked ? "good" : "warn");
    }
    renderChecklist((s || {}).checklist || []);
    renderMatrix((s || {}).strategy_matrix || {});
    var saved = (s || {}).saved_session || {};
    if(el("is-notes")){
      var note = String(saved.notes || "");
      if(note && !el("is-notes").dataset.touched) el("is-notes").value = note;
    }
  }

  function inr(n){
    var x = Number(n || 0);
    var abs = Math.abs(x).toLocaleString("en-IN", { maximumFractionDigits: 2, minimumFractionDigits: 2 });
    return (x >= 0 ? "+₹" : "-₹") + abs;
  }

  function _rptTab(){ return String(STATE.rptTab || "day_log"); }
  function _popupTab(){ return String(STATE.popTab || "day_log"); }

  function _renderSummaryCards(summary){
    var s = summary || {};
    var pnl = Number(s.total_pnl || 0);
    var dd = Number(s.max_drawdown || 0);
    if(!el("is-bt-summary")) return;
    el("is-bt-summary").innerHTML = ''
      + '<div class="is-rpt-card"><div class="is-rpt-k">Days tracked</div><div class="is-rpt-v">' + esc(s.days || 0) + '</div></div>'
      + '<div class="is-rpt-card"><div class="is-rpt-k">Total trades</div><div class="is-rpt-v">' + esc(s.total_trades || 0) + '</div></div>'
      + '<div class="is-rpt-card"><div class="is-rpt-k">Win rate</div><div class="is-rpt-v ' + ((s.win_rate || 0) >= 60 ? "good" : "") + '">' + esc((s.win_rate || 0) + "%") + '</div></div>'
      + '<div class="is-rpt-card"><div class="is-rpt-k">Total P&L</div><div class="is-rpt-v ' + (pnl >= 0 ? "good" : "bad") + '">' + esc(inr(pnl)) + '</div></div>'
      + '<div class="is-rpt-card"><div class="is-rpt-k">Max drawdown</div><div class="is-rpt-v bad">' + esc(inr(-Math.abs(dd))) + '</div></div>';
  }

  function _renderFilters(rows, key){
    var host = el("is-rpt-filters");
    if(!host) return;
    var values = [];
    if(key === "regime"){
      values = ["ALL","TREND","CHOP","BREAKOUT","MIXED","UNCLASSIFIED"];
    }else{
      values = ["ALL"].concat(Array.from(new Set((rows || []).map(function(r){ return String(r[key] || "").toUpperCase(); }).filter(Boolean))).sort());
    }
    host.innerHTML = values.map(function(v){
      var on = String(STATE.rptFilter || "ALL").toUpperCase() === v ? " on" : "";
      return '<button type="button" class="is-rpt-chip' + on + '" data-v="' + esc(v) + '">' + esc(v) + '</button>';
    }).join("");
    host.querySelectorAll(".is-rpt-chip").forEach(function(b){
      b.onclick = function(){ STATE.rptFilter = String(this.getAttribute("data-v") || "ALL"); _renderReportView(); };
    });
  }

  function _renderReportView(){
    var b = STATE.bt || {};
    var tab = _rptTab();
    var host = el("is-rpt-view");
    if(!host) return;
    var filter = String(STATE.rptFilter || "ALL").toUpperCase();
    if(tab === "day_log"){
      var rows = (b.day_log || []).slice();
      _renderFilters(rows, "regime");
      rows = filter === "ALL" ? rows : rows.filter(function(r){ return String(r.regime || "").toUpperCase() === filter; });
      host.innerHTML = '<table class="is-matrix"><thead><tr><th>Date</th><th>Regime</th><th>Strategies used</th><th>Trades</th><th>W/L</th><th>Day P&L</th></tr></thead><tbody>'
        + (rows.length ? rows.map(function(r){
          var p = Number(r.day_pnl || 0);
          var sTags = (r.strategies_used || []).map(function(x){ return '<span class="is-tag">' + esc(x) + '</span>'; }).join("");
          return '<tr><td><b>' + esc(r.date) + '</b></td><td><span class="is-badge">' + esc(r.regime) + '</span></td><td><div class="is-strategy-tags">' + sTags + '</div></td><td>' + esc(r.trades) + '</td><td>' + esc(r.wl) + '</td><td style="font-weight:800;color:' + (p >= 0 ? "var(--go)" : "var(--st)") + '">' + esc(inr(p)) + '</td></tr>';
        }).join("") : '<tr><td colspan="6" style="color:var(--t2)">No rows.</td></tr>')
        + '</tbody></table>';
      return;
    }
    if(tab === "trade_detail"){
      var tRows = (b.trade_detail || []).slice();
      _renderFilters(tRows, "strategy_id");
      tRows = filter === "ALL" ? tRows : tRows.filter(function(r){ return String(r.strategy_id || "").toUpperCase() === filter; });
      host.innerHTML = '<table class="is-matrix"><thead><tr><th>Date</th><th>Strategy</th><th>Regime</th><th>Dir</th><th>Entry</th><th>Exit</th><th>Score</th><th>P&L</th></tr></thead><tbody>'
        + (tRows.length ? tRows.slice(0, 300).map(function(r){
          var p = Number(r.pnl_inr || 0);
          return '<tr><td>' + esc(r.date) + '</td><td><span class="is-tag">' + esc(r.strategy_id) + '</span> ' + esc(r.strategy_name || "") + '</td><td><span class="is-badge">' + esc(r.regime) + '</span></td><td>' + esc(r.dir) + '</td><td>' + esc(r.entry_time || "—") + '</td><td>' + esc(r.exit_time || "—") + '</td><td>' + esc(Number(r.score || 0).toFixed(2)) + '</td><td style="font-weight:800;color:' + (p >= 0 ? "var(--go)" : "var(--st)") + '">' + esc(inr(p)) + '</td></tr>';
        }).join("") : '<tr><td colspan="8" style="color:var(--t2)">No rows.</td></tr>')
        + '</tbody></table>';
      return;
    }
    if(tab === "by_strategy"){
      var sRows = (b.by_strategy || []).slice();
      _renderFilters(sRows, "strategy_id");
      sRows = filter === "ALL" ? sRows : sRows.filter(function(r){ return String(r.strategy_id || "").toUpperCase() === filter; });
      host.innerHTML = sRows.length ? sRows.map(function(r){
        var wr = Number(r.win_rate || 0), tw = Number(r.target_win_rate || 0), p = Number(r.total_pnl || 0);
        return '<div class="is-reg-box"><div style="display:flex;justify-content:space-between;gap:8px"><div><div style="font-size:32px"><span class="is-tag">' + esc(r.strategy_id) + '</span> <b>' + esc(r.strategy_name || "") + '</b></div><div class="is-note">' + esc(r.trades) + ' trades · Win rate ' + esc(wr + "%") + ' · Target ' + esc(tw + "%") + '</div></div><div style="text-align:right"><div class="is-rpt-v ' + (p >= 0 ? "good" : "bad") + '">' + esc(inr(p)) + '</div><div class="is-note">avg ' + esc(inr(r.avg_pnl || 0)) + '/trade</div></div></div><div class="is-note">Best: <span style="color:var(--go)">' + esc(inr(r.best || 0)) + '</span> · Worst: <span style="color:var(--st)">' + esc(inr(r.worst || 0)) + '</span></div></div>';
      }).join("") : '<div class="is-note">No strategy rows.</div>';
      return;
    }
    var rRows = (b.by_regime || []).slice();
    _renderFilters(rRows, "regime");
    rRows = filter === "ALL" ? rRows : rRows.filter(function(r){ return String(r.regime || "").toUpperCase() === filter; });
    host.innerHTML = rRows.length ? rRows.map(function(r){
      var p = Number(r.total_pnl || 0);
      var tags = (r.top_strategies || []).map(function(x){ return '<span class="is-tag">' + esc(x.strategy_id) + " " + esc(x.count) + "×</span>"; }).join("");
      return '<div class="is-reg-box"><div style="display:flex;justify-content:space-between;gap:8px"><div><span class="is-badge">' + esc(r.regime) + '</span><div class="is-note" style="margin-top:6px">' + esc(r.days) + ' days · ' + esc(r.trades) + ' trades · ' + esc(r.win_rate) + '% win rate</div><div class="is-note"><b>Top strategies on ' + esc(r.regime) + ' days:</b></div><div class="is-strategy-tags">' + tags + '</div></div><div class="is-rpt-v ' + (p >= 0 ? "good" : "bad") + '">' + esc(inr(p)) + '</div></div></div>';
    }).join("") : '<div class="is-note">No regime rows.</div>';
  }

  function renderBacktest(b){
    STATE.bt = b || {};
    STATE.btSource = String((b || {}).source || STATE.btSource || "legacy");
    if(!STATE.rptTab) STATE.rptTab = "day_log";
    if(!STATE.rptFilter) STATE.rptFilter = "ALL";
    _renderSummaryCards((b || {}).summary || {});
    var exBt = (b || {}).excluded_strategies || [];
    var exPartBt = (Array.isArray(exBt) && exBt.length && String((b || {}).source || "") === "full_engine_3m")
      ? (" · Engine excludes: " + exBt.join(", "))
      : "";
    if(el("is-bt-note")) el("is-bt-note").textContent = "Range: " + esc((b || {}).from_date || "—") + " to " + esc((b || {}).to_date || "—") + " · Source: " + esc(STATE.btSource || "legacy") + exPartBt;
    _renderReportView();
    renderPopupTracker();
  }

  function _adaptFullEngineReportToTracker(rep){
    rep = rep || {};
    var s = rep.summary || {};
    var tl = (rep.trade_detail || []).slice();
    var exList = Array.isArray(rep.excluded_strategies) ? rep.excluded_strategies.slice() : [];
    if(!exList.length){
      exList = _parseExcludedFromRunKey(String((rep.run || {}).run_key || rep.run_key || ""));
    }
    var exNote = exList.length ? ("Sim excludes: " + exList.join(", ")) : "";
    var dayMap = {};
    var regMap = {};
    tl.forEach(function(t){
      var d = String(t.trade_date || t.date || "");
      if(!d) return;
      if(!dayMap[d]){
        dayMap[d] = { date: d, regime: String(t.regime || "MIXED"), strategies: {}, trades: 0, wins: 0, losses: 0, day_pnl: 0, best_trade: -1e18, notes: "" };
      }
      var p = Number(t.pnl || t.pnl_inr || 0);
      var rg = String(t.regime || "MIXED").toUpperCase();
      dayMap[d].trades += 1;
      dayMap[d].day_pnl += p;
      dayMap[d].best_trade = Math.max(dayMap[d].best_trade, p);
      if(p > 0) dayMap[d].wins += 1; else if(p < 0) dayMap[d].losses += 1;
      dayMap[d].strategies[String(t.strategy_id || "NA")] = 1;
      if(!regMap[rg]) regMap[rg] = { regime: rg, days: {}, trades: 0, wins: 0, total_pnl: 0, top: {} };
      regMap[rg].days[d] = 1;
      regMap[rg].trades += 1;
      regMap[rg].total_pnl += p;
      if(p > 0) regMap[rg].wins += 1;
      var sid = String(t.strategy_id || "NA");
      regMap[rg].top[sid] = (regMap[rg].top[sid] || 0) + 1;
    });
    var dayLog = Object.keys(dayMap).sort().map(function(d){
      var x = dayMap[d];
      return {
        date: d,
        regime: x.regime,
        strategies_used: Object.keys(x.strategies).sort(),
        trades: x.trades,
        wl: x.wins + "W/" + x.losses + "L",
        day_pnl: Number(x.day_pnl || 0).toFixed ? Number(x.day_pnl.toFixed(2)) : Number(x.day_pnl || 0),
        best_trade: Number((x.best_trade <= -1e17 ? 0 : x.best_trade).toFixed(2)),
        notes: exNote
      };
    });
    var byRegime = Object.keys(regMap).sort().map(function(rg){
      var x = regMap[rg];
      return {
        regime: rg,
        days: Object.keys(x.days).length,
        trades: x.trades,
        win_rate: Number((100 * x.wins / Math.max(1, x.trades)).toFixed(1)),
        total_pnl: Number(x.total_pnl.toFixed(2)),
        top_strategies: Object.keys(x.top).sort(function(a,b){ return x.top[b]-x.top[a]; }).slice(0,5).map(function(k){ return { strategy_id: k, count: x.top[k] }; })
      };
    });
    
    var rkAll = String((rep.run || {}).run_key || rep.run_key || "");
    var anchorResolved = rkAll ? _anchorFromRunKey(rkAll) : String((rep.run || {}).anchor || rep.anchor || "bnf");
    return {
      source: "full_engine_3m",
      run_key: rkAll,
      anchor: anchorResolved,
      excluded_strategies: exList.slice(),
      from_date: ((rep.run || {}).start_date || "—"),
      to_date: ((rep.run || {}).end_date || "—"),
      data_warning: String(rep.data_warning || ""),
      summary: {
        days: Number(s.days || 0),
        total_trades: Number(s.trades || 0),
        win_rate: Number(s.win_rate || 0),
        total_pnl: Number(s.total_pnl || 0),
        max_drawdown: Number(s.max_drawdown || 0)
      },
      day_log: dayLog,
      trade_detail: tl.map(function(t){
        return {
          date: String(t.trade_date || t.date || ""),
          strategy_id: String(t.strategy_id || ""),
          strategy_name: String(t.strategy_name || ""),
          regime: String(t.regime || "MIXED"),
          instrument: String(t.instrument || ""),
          dir: String(t.direction || ""),
          entry_time: String(t.trade_time || t.entry_time || ""),
          entry_price: Number(t.entry || 0),
          exit_time: String(t.exit_time || ""),
          exit_price: Number(t.exit || 0),
          score: Number(t.score || 0),
          pnl_inr: Number(t.pnl || t.pnl_inr || 0),
          outcome: String(t.reason_code || "TIME_EXIT")
        };
      }),
      by_strategy: (rep.by_strategy || []).map(function(r){
        return {
          strategy_id: r.strategy_id, strategy_name: r.strategy_name, trades: Number(r.trades || 0),
          win_rate: Number(r.win_rate || 0), target_win_rate: 65, total_pnl: Number(r.total_pnl || 0),
          avg_pnl: Number((Number(r.total_pnl || 0) / Math.max(1, Number(r.trades || 0))).toFixed(2)),
          best: 0, worst: 0
        };
      }),
      by_regime: byRegime
    };
  }

  function _ensureThreeMonthRealisticBacktest(){
    return req(_feRunsUrl(5, STATE.futAnchor), { method: "GET" })
      .then(function(d){
        var runs = (d || {}).runs || [];
        if(runs.length){
          return req("/api/intraday-system/full-engine/report?run_key=" + encodeURIComponent(runs[0].run_key), { method: "GET" });
        }
        var end = _todayIso();
        var start = _daysAgoIso(95);
        return req("/api/intraday-system/full-engine/backtest", { method: "POST", body: JSON.stringify({ start_date: start, end_date: end, anchor: String(STATE.futAnchor || "bnf") }) });
      })
      .then(function(rep){
        if(rep && rep.summary && Number((rep.summary.days || 0)) >= 30){
          renderBacktest(_adaptFullEngineReportToTracker(rep));
          if(el("is-fe-note")) el("is-fe-note").textContent = "Loaded realistic 3-month full-engine report.";
        }
      })
      .catch(function(e){
        if(el("is-fe-note")) el("is-fe-note").textContent = "3-month full-engine load skipped: " + e.message;
      });
  }

  function _popupRowsByTab(tab, b){
    b = b || {};
    if(tab === "day_log") return (b.day_log || []).slice();
    if(tab === "trade_detail") return (b.trade_detail || []).slice();
    if(tab === "by_strategy") return (b.by_strategy || []).slice();
    return (b.by_regime || []).slice();
  }

  function _popupFilterKey(tab){
    if(tab === "day_log") return "regime";
    if(tab === "trade_detail") return "strategy_id";
    if(tab === "by_strategy") return "strategy_id";
    return "regime";
  }

  function _ensurePopup(){
    var IS_REV = "3";
    var oldRoot = el("is-popup-root");
    if(oldRoot && String(oldRoot.getAttribute("data-is-rev") || "") !== IS_REV){
      try{ oldRoot.remove(); }catch(_eRm){}
    }
    if(el("is-popup-root")) return;
    var wrap = document.createElement("div");
    wrap.id = "is-popup-root";
    wrap.setAttribute("data-is-rev", IS_REV);
    wrap.innerHTML = ''
      + '<style id="is-popup-style">'
      + '.is-pop-futbtn{height:26px;padding:0 10px;margin-right:6px;border-radius:999px;border:1px solid rgba(56,189,248,.5);background:rgba(12,32,58,.9);color:#e0f7fa;font-size:10px;font-weight:900;letter-spacing:.6px;font-family:var(--fn,inherit);cursor:pointer;box-shadow:0 2px 10px rgba(34,211,238,.14)}'
      + '.is-pop-futbtn:hover{filter:brightness(1.08);border-color:rgba(125,211,252,.75);color:#f0fdff}'
      + '.is-pop-futbtn.on{background:linear-gradient(135deg,#16a34a,#22d3ee);color:#04210f;border-color:rgba(34,211,238,.75);box-shadow:0 4px 12px rgba(34,211,238,.25)}'
      + '#is-cash-topbtn{height:26px;padding:0 11px;margin-left:6px;border-radius:999px;border:1px solid rgba(251,191,36,.55);background:linear-gradient(135deg,#b45309,#f59e0b);color:#1a0f00;font-size:10px;font-weight:900;letter-spacing:.8px;font-family:var(--fn,inherit);cursor:pointer;box-shadow:0 4px 12px rgba(245,158,11,.22)}'
      + '#is-cash-topbtn:hover{filter:brightness(1.06)}'
      + '#is-advcash-topbtn{height:26px;padding:0 11px;margin-left:6px;border-radius:999px;border:1px solid rgba(45,212,191,.55);background:linear-gradient(135deg,#0d5e58,#0d9488);color:#ccfbf1;font-size:10px;font-weight:900;letter-spacing:.8px;font-family:var(--fn,inherit);cursor:pointer;box-shadow:0 4px 12px rgba(13,148,136,.25)}'
      + '#is-advcash-topbtn:hover{filter:brightness(1.08);border-color:rgba(45,212,191,.8)}'
      + '#is-pop-mask{position:fixed;inset:0;background:rgba(2,6,23,.62);backdrop-filter:blur(2px);z-index:100000;display:none}'
      + '#is-pop{position:fixed;left:10px;top:42px;width:calc(100vw - 20px);height:calc(100vh - 54px);overflow:auto;background:radial-gradient(1400px 560px at 20% -20%,rgba(34,211,238,.12),transparent),linear-gradient(180deg,#071126,#060d1b 52%,#070f20);color:#dbe7ff;border-radius:14px;box-shadow:0 28px 95px rgba(2,6,23,.62), inset 0 0 0 1px rgba(125,211,252,.18), inset 0 0 40px rgba(8,145,178,.12);z-index:100001;display:none;padding:16px 16px 20px;border:1px solid rgba(56,189,248,.28);font-family:var(--fn,inherit)}'
      + '.is-pop-head{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:12px 16px;align-items:start}'
      + '.is-pop-head>div:first-child{min-width:0;overflow:hidden}'
      + '.is-pop-head-actions{justify-self:end;display:flex;flex-direction:column;align-items:flex-end;gap:8px}'
      + '#is-fut-pop-mode-live,#is-fut-pop-mode-bt{pointer-events:auto;touch-action:manipulation}'
      + '#is-pop-fut-info{margin-top:10px;padding:10px 12px;border-radius:10px;border:1px solid rgba(251,191,36,.38);background:rgba(60,40,10,.35);color:#fde68a;font-size:11px;line-height:1.55}'
      + '.is-pop-title{font-size:46px;line-height:1.02;font-weight:900;color:#eaf2ff;letter-spacing:.2px;text-shadow:0 2px 14px rgba(56,189,248,.14)}'
      + '.is-pop-sub{font-size:12px;color:#8fa7c7;margin-top:4px;line-height:1.55}'
      + '.is-pop-sub-link{border:none;background:none;padding:0;margin:0;font:inherit;font-weight:800;color:#7dd3fc;text-decoration:underline;cursor:pointer;text-underline-offset:2px}'
      + '.is-pop-sub-link:hover{color:#bae6fd}'
      + '.is-pop-close{border:1px solid rgba(148,163,184,.35);background:rgba(7,18,40,.72);border-radius:10px;padding:8px 12px;font-weight:900;font-size:11px;color:#d6e6ff;cursor:pointer;font-family:var(--fn,inherit)}'
      + '.is-pop-close:hover{border-color:rgba(56,189,248,.55);color:#7dd3fc}'
      + '.is-pop-summary-wrap{margin-top:14px;padding:18px 16px 20px;border:1px solid rgba(56,189,248,.22);border-radius:16px;background:linear-gradient(165deg,rgba(12,24,52,.92),rgba(6,12,28,.88))}'
      + '.is-pop-metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;align-items:stretch}'
      + '@media(min-width:1100px){.is-pop-metrics{grid-template-columns:repeat(6,minmax(0,1fr))}}'
      + '@media(max-width:520px){.is-pop-metrics{grid-template-columns:repeat(2,minmax(0,1fr))}}'
      + '.is-pop-metric{display:flex;flex-direction:column;align-items:stretch;justify-content:flex-start;gap:6px;padding:14px 12px;border-radius:12px;border:1px solid rgba(148,163,184,.14);background:rgba(0,0,0,.22);min-height:88px;min-width:0;overflow:hidden}'
      + '.is-pop-metric.is-pop-metric--hero{grid-column:1/-1;min-height:0;padding:18px 18px 20px;border-color:rgba(56,189,248,.32);background:linear-gradient(125deg,rgba(8,47,73,.5),rgba(15,23,42,.55))}'
      + '.is-pop-metric.is-pop-metric--hero .is-pop-v{font-size:clamp(28px,4.2vw,42px)}'
      + '.is-pop-metric .is-pop-k{font-size:10px;color:#94a3b8;text-transform:uppercase;letter-spacing:.55px;font-weight:700;line-height:1.2}'
      + '.is-pop-metric .is-pop-v{font-size:clamp(22px,2.6vw,34px);font-weight:900;color:#f1f5f9;line-height:1.08;letter-spacing:-.02em;overflow-wrap:anywhere;word-break:break-word}'
      + '.is-pop-metric .is-pop-v.is-num-sm{font-size:clamp(18px,2vw,26px)}'
      + '.is-pop-metric .is-pop-v.good{color:#4ade80}.is-pop-metric .is-pop-v.bad{color:#fb7185}'
      + '.is-pop-metric .is-pop-v.muted{color:#94a3b8;font-size:13px;font-weight:700}'
      + '.is-pop-metric-sub{margin-top:2px;font-size:10px;color:#64748b;font-weight:600;line-height:1.3}'
      + '.is-pop-engine-bar{margin-top:12px;padding:11px 14px;border-radius:11px;border:1px solid rgba(56,189,248,.18);background:rgba(8,20,44,.55);font-size:12px;line-height:1.5;color:#cbd5e1}'
      + '.is-pop-engine-bar strong{color:#e2e8f0;font-weight:800}'
      + '.is-pop-engine-bar code{font-size:11px;padding:1px 6px;border-radius:4px;background:rgba(0,0,0,.35);color:#93c5fd}'
      + '.is-pop-tabs{display:flex;gap:22px;border-bottom:1px solid rgba(148,163,184,.18);margin-top:16px}'
      + '.is-pop-tab{border:none;background:none;padding:8px 0 9px;font-size:16px;color:#8ea3c0;cursor:pointer;font-weight:600;border-bottom:2px solid transparent;font-family:var(--fn,inherit)}'
      + '.is-pop-tab.on{border-bottom-color:#38bdf8;color:#e6f0ff;font-weight:800}'
      + '.is-pop-filters{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}'
      + '.is-pop-chip{border:1px solid rgba(148,163,184,.30);background:rgba(8,20,44,.68);color:#c6d6ee;border-radius:999px;padding:5px 11px;font-size:11px;cursor:pointer;font-weight:700;font-family:var(--fn,inherit)}'
      + '.is-pop-chip.on{background:#0ea5e9;color:#06243a;border-color:#22d3ee}'
      + '.is-pop-table{width:100%;border-collapse:collapse;margin-top:10px}'
      + '.is-pop-table th,.is-pop-table td{padding:9px 8px;border-bottom:1px solid rgba(100,116,139,.16);text-align:left;font-size:13px;vertical-align:top}'
      + '/* Beat .light table td { color: var(--t0) !important } — scope light text to classes so PnL colors still work */'
      + '#is-pop .is-pop-table th{color:#86a0c4!important}'
      + '#is-pop .is-pop-table td.is-pop-td-base{color:#dbe7ff!important}'
      + '#is-pop .is-pop-table td.is-pop-pnl-pos{color:#4ade80!important;font-weight:800!important}'
      + '#is-pop .is-pop-table td.is-pop-pnl-neg{color:#fb7185!important;font-weight:800!important}'
      + '#is-pop .is-pop-table td.is-pop-pnl-mid{color:#67e8f9!important}'
      + '.is-pop-table th{font-size:11px;color:#86a0c4;font-weight:800;letter-spacing:.3px;text-transform:uppercase}'
      + '.is-pop-pill{display:inline-flex;padding:3px 10px;border-radius:999px;background:rgba(59,130,246,.22);color:#bfdbfe;font-size:10px;font-weight:900;letter-spacing:.4px}'
      + '.is-pop-pill.chop{background:rgba(234,179,8,.24);color:#fde68a}.is-pop-pill.breakout{background:rgba(34,197,94,.22);color:#bbf7d0}.is-pop-pill.mixed{background:rgba(168,85,247,.24);color:#e9d5ff}'
      + '.is-pop-tag{display:inline-flex;padding:3px 9px;border-radius:999px;background:rgba(99,102,241,.22);color:#dbeafe;font-size:11px;font-weight:800;margin:0 5px 5px 0}'
      + '.is-pop-rowcard{display:flex;justify-content:space-between;gap:10px;border-bottom:1px solid rgba(100,116,139,.16);padding:10px 0}'
      + '.is-pop-toolbar{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-top:10px;padding:10px;border:1px solid rgba(56,189,248,.20);border-radius:10px;background:rgba(8,20,44,.62)}'
      + '.is-pop-ctl{display:inline-flex;align-items:center;gap:6px}'
      + '.is-pop-ctl b{font-size:10px;color:#86a0c4;letter-spacing:.5px}'
      + '.is-pop-date{height:28px;padding:0 8px;border-radius:7px;border:1px solid rgba(148,163,184,.35);background:rgba(2,10,24,.65);color:#dbeafe;font-family:var(--fn,inherit);font-size:11px}'
      + '.is-pop-q{height:25px;padding:0 9px;border-radius:999px;border:1px solid rgba(148,163,184,.30);background:rgba(8,20,44,.72);color:#c7d2fe;font-size:10px;font-weight:800;cursor:pointer}'
      + '.is-pop-q:hover{border-color:rgba(34,211,238,.55);color:#67e8f9}'
      + '.is-pop-run{height:28px;padding:0 12px;border-radius:999px;border:1px solid rgba(34,211,238,.55);background:linear-gradient(135deg,rgba(34,197,94,.95),rgba(6,182,212,.95));color:#03221a;font-size:10px;font-weight:900;letter-spacing:.5px;cursor:pointer}'
      + '@media(max-width:980px){#is-pop{left:4px;top:40px;width:calc(100vw - 8px);height:calc(100vh - 48px)}.is-pop-title{font-size:26px;line-height:1.08}}'
      + '</style>'
      + '<div id="is-pop-mask"></div>'
      + '<div id="is-pop">'
      + '  <div class="is-pop-head">'
      + '    <div><div id="is-pop-title" class="is-pop-title">BNIFTY FUTURE</div><div id="is-pop-sub" class="is-pop-sub"></div></div>'
      + '    <div class="is-pop-head-actions">'
      + '      <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;justify-content:flex-end">'
      + '        <span style="font-size:8px;color:#94a3b8;font-weight:800;letter-spacing:.75px">MODE</span>'
      + '        <div style="display:inline-flex;border-radius:999px;border:1px solid rgba(56,189,248,.45);overflow:hidden;background:rgba(15,23,42,.88)">'
      + '          <button type="button" id="is-fut-pop-mode-live" style="border:none;padding:6px 12px;font-size:10px;font-weight:800;cursor:pointer;font-family:var(--fn,inherit);background:transparent;color:#7dd3fc">LIVE</button>'
      + '          <button type="button" id="is-fut-pop-mode-bt" style="border:none;padding:6px 12px;font-size:10px;font-weight:800;cursor:pointer;font-family:var(--fn,inherit);background:transparent;color:#7dd3fc">BACKTEST</button>'
      + '        </div>'
      + '        <button id="is-pop-refresh" class="is-pop-close" type="button">Refresh</button>'
      + '        <button id="is-pop-close" class="is-pop-close" type="button">Close</button>'
      + '      </div>'
      + '    </div>'
      + '  </div>'
      + '  <div id="is-pop-mode-strip" class="is-pop-engine-bar" style="margin-top:2px;padding:8px 0 10px;font-size:11px;line-height:1.45;color:#bae6fd;border-bottom:1px solid rgba(56,189,248,.22)"></div>'
      + '  <div class="is-pop-toolbar">'
      + '    <div class="is-pop-ctl"><b>FROM</b><input id="is-pop-from" class="is-pop-date" type="date"></div>'
      + '    <div class="is-pop-ctl"><b>TO</b><input id="is-pop-to" class="is-pop-date" type="date"></div>'
      + '    <div class="is-pop-ctl"><b>DATE</b><select id="is-pop-date" class="is-pop-date"><option value="ALL">ALL</option></select></div>'
      + '    <div class="is-pop-ctl"><button id="is-pop-load-range" class="is-pop-run" type="button" title="Reload tables for the current FROM/TO (uses saved run when available).">Load / refresh range</button></div>'
      + '    <div class="is-pop-ctl"><button class="is-pop-q" data-d="30" type="button">30D</button><button class="is-pop-q" data-d="90" type="button">3M</button><button class="is-pop-q" data-d="180" type="button">6M</button></div>'
      + '    <div class="is-pop-ctl"><button id="is-pop-run" class="is-pop-run" type="button" title="Loads from a saved DB run when one covers your FROM/TO. Shift+click to force a fresh Kite replay (slow).">RUN BACKTEST</button></div>'
      + '  </div>'
      + '  <div id="is-pop-fut-info"></div>'
      + '  <div id="is-pop-warn" class="is-pop-engine-bar" style="display:none;margin-top:10px;border-color:rgba(248,113,113,.4);background:rgba(50,10,10,.45);color:#fecaca;font-size:12px;line-height:1.5"></div>'
      + '  <div id="is-pop-engine" class="is-pop-engine-bar" style="display:none"></div>'
      + '  <div class="is-pop-summary-wrap"><div id="is-pop-summary" class="is-pop-metrics"></div></div>'
      + '  <div class="is-pop-tabs">'
      + '    <button class="is-pop-tab on" data-v="day_log" type="button">Day log</button>'
      + '    <button class="is-pop-tab" data-v="trade_detail" type="button">Trade detail</button>'
      + '    <button class="is-pop-tab" data-v="by_strategy" type="button">By strategy</button>'
      + '    <button class="is-pop-tab" data-v="by_regime" type="button">By regime</button>'
      + '  </div>'
      + '  <div id="is-pop-filters" class="is-pop-filters"></div>'
      + '  <div id="is-pop-view"></div>'
      + '</div>';
    document.body.appendChild(wrap);
    var show = function(on){
      if(el("is-pop")) el("is-pop").style.display = on ? "block" : "none";
      if(el("is-pop-mask")) el("is-pop-mask").style.display = on ? "block" : "none";
    };
    if(!window.intradaySystemOpenPopup){
      window.intradaySystemOpenPopup = function(){
        try{
          var a = sessionStorage.getItem("is_fut_anchor");
          if(a === "nifty" || a === "bnf") STATE.futAnchor = a;
        }catch(_e){}
        if(el("is-cash-pop")) el("is-cash-pop").style.display = "none";
        if(el("is-cash-mask")) el("is-cash-mask").style.display = "none";
        _ensurePopup();
        _syncFutPopupHeadings();
        _syncFutTopBtnStyles();
        show(true);
        _updateFutPopupModeBanner();
        _syncFutPopModeTog();
        _syncFutPopupInfoBox();
        syncTrackerToCurrentAnchor().then(function(){ renderPopupTracker(); }).catch(function(){ renderPopupTracker(); });
      };
    }
    if(el("is-pop-close")) el("is-pop-close").onclick = function(){ show(false); };
    if(el("is-pop-mask")) el("is-pop-mask").onclick = function(){ show(false); };
    function _isPopLoadOrRefresh(){
      var pf = el("is-pop-from"), pt = el("is-pop-to");
      if(pf && pt && String(pf.value || "").trim() && String(pt.value || "").trim()){
        runPopupRange(null);
      }else{
        renderPopupTracker();
      }
    }
    if(el("is-pop-refresh")) el("is-pop-refresh").onclick = function(){ _isPopLoadOrRefresh(); };
    if(el("is-pop-load-range")) el("is-pop-load-range").onclick = function(){ _isPopLoadOrRefresh(); };
    if(el("is-fut-pop-mode-live")) el("is-fut-pop-mode-live").onclick = function(){ _setMarketMode("live"); };
    if(el("is-fut-pop-mode-bt")) el("is-fut-pop-mode-bt").onclick = function(){ _setMarketMode("backtest"); };
    if(el("is-pop-run")) el("is-pop-run").onclick = function(ev){ runPopupRange(ev); };
    if(el("is-pop-date")) el("is-pop-date").onchange = function(){
      STATE.popDate = String(this.value || "ALL");
      renderPopupTracker();
    };
    if(el("is-pop-from") && !el("is-pop-from").dataset.dtbind){
      el("is-pop-from").dataset.dtbind = "1";
      el("is-pop-from").addEventListener("change", function(){ renderPopupTracker(); });
    }
    if(el("is-pop-to") && !el("is-pop-to").dataset.dtbind){
      el("is-pop-to").dataset.dtbind = "1";
      el("is-pop-to").addEventListener("change", function(){ renderPopupTracker(); });
    }
    document.querySelectorAll(".is-pop-q").forEach(function(q){
      q.onclick = function(){
        var d = Number(this.getAttribute("data-d") || 90);
        if(el("is-pop-from")) el("is-pop-from").value = _daysAgoIso(d);
        if(el("is-pop-to")) el("is-pop-to").value = _todayIso();
        STATE.popDate = "ALL";
        renderPopupTracker();
      };
    });
    document.querySelectorAll(".is-pop-tab").forEach(function(btn){
      btn.onclick = function(){
        STATE.popTab = String(this.getAttribute("data-v") || "day_log");
        STATE.popFilter = "ALL";
        document.querySelectorAll(".is-pop-tab").forEach(function(x){ x.classList.remove("on"); });
        this.classList.add("on");
        renderPopupTracker();
      };
    });
    if(el("is-pop-from") && !el("is-pop-from").value) el("is-pop-from").value = _daysAgoIso(90);
    if(el("is-pop-to") && !el("is-pop-to").value) el("is-pop-to").value = _todayIso();
    _syncFutPopupHeadings();
  }

  function _cashPopupTab(){ return String(STATE.cashPopTab || "day_log"); }
  function _cashRowsByTab(tab, b){
    b = b || {};
    if(tab === "day_log") return (b.day_log || []).slice();
    if(tab === "trade_detail") return (b.trade_detail || []).slice();
    if(tab === "by_strategy") return (b.by_strategy || []).slice();
    return (b.by_symbol || []).slice();
  }
  function _cashFilterKey(tab){
    if(tab === "day_log") return "regime";
    if(tab === "trade_detail") return "instrument";
    if(tab === "by_strategy") return "strategy_id";
    return "symbol";
  }
  function _cashRenderFilters(rows, key){
    var host = el("is-cash-trk-filters");
    if(!host) return [];
    var vals = [];
    if(key === "regime"){
      vals = ["ALL","CASH"];
    }else if(key === "strategy_id"){
      vals = ["ALL","SPIKE"];
    }else{
      vals = ["ALL"].concat(Array.from(new Set((rows || []).map(function(r){ return String(r[key] || "").toUpperCase(); }).filter(Boolean))).sort());
    }
    host.innerHTML = vals.map(function(v){
      return '<button type="button" class="is-cash-trk-chip' + (String(STATE.cashPopFilter || "ALL").toUpperCase() === v ? " on" : "") + '" data-v="' + esc(v) + '">' + esc(v) + '</button>';
    }).join("");
    host.querySelectorAll(".is-cash-trk-chip").forEach(function(c){
      c.onclick = function(){ STATE.cashPopFilter = String(this.getAttribute("data-v") || "ALL"); renderCashTracker(); };
    });
    return vals;
  }
  function renderCashTracker(){
    _ensureCashPopup();
    var b = STATE.cashBt || {};
    var s = b.summary || {};
    var tab = _cashPopupTab();
    var allSig = (b.trade_detail || []).slice();
    var selectedDate = String(STATE.cashPopDate || "ALL");
    var dateSig = selectedDate === "ALL" ? allSig : allSig.filter(function(t){ return String(t.date || "") === selectedDate; });
    var warnLine = el("is-cash-trk-warn");
    if(warnLine){
      var dw = String((b || {}).data_warning || "").trim();
      if(dw){
        var low = dw.toLowerCase();
        var isErr = (/^not found$|(^|[\s])404([\s]|$)|not found.*api|failed to fetch|networkerror|load failed/i.test(low))
          && !/no jsonl|sessions in range|performance cap|produced zero signals/i.test(low);
        if(/^not found$/i.test(dw.trim())){
          dw = "The server returned 404 for POST /api/nse-cash/replay-report. Restart the backend from this project (python main.py in backend/) so the route is registered, hard-refresh the page, and confirm the UI uses the same host/port as that server.";
        }
        warnLine.style.display = "block";
        warnLine.className = isErr ? "is-cash-trk-warn" : "is-cash-trk-hint";
        warnLine.innerHTML = (isErr ? "<strong>Error.</strong> " : "<strong>Note.</strong> ") + esc(dw);
      }else{
        warnLine.style.display = "none";
        warnLine.innerHTML = "";
        warnLine.className = "is-cash-trk-warn";
      }
    }
    var eng = el("is-cash-trk-engine");
    if(eng){
      eng.style.display = "block";
      var mm = typeof window.getNseEdgeMarketMode === "function" ? window.getNseEdgeMarketMode() : "backtest";
      var modeLine = mm === "live"
        ? '<div style="margin-bottom:8px;padding-bottom:8px;border-bottom:1px solid rgba(251,191,36,.28)"><strong>LIVE</strong> — Scanner + recording below. <strong>RUN SIGNAL REPORT</strong> still fills this grid from JSONL replay.</div>'
        : '<div style="margin-bottom:8px;padding-bottom:8px;border-bottom:1px solid rgba(251,191,36,.28)"><strong>BACKTEST</strong> — <strong>RUN SIGNAL REPORT</strong> for SQLite-cached or forced JSONL replay.</div>';
      var svc = !!(b && b.served_from_cache);
      eng.innerHTML = modeLine + "<strong>JSONL replay</strong> — Same <code>SpikeScanner</code> path as live. "
        + "Totals are <strong>signal counts</strong>; exits and ₹ P&amp;L are <em>not</em> simulated (unlike NSE Future full-engine)."
        + (svc ? " <strong style=\"color:#86efac\">Instant:</strong> loaded from SQLite cache (same date range + session files unchanged). <strong>Shift+click</strong> RUN SIGNAL REPORT to force a fresh JSONL replay." : "");
    }
    var sum = el("is-cash-trk-summary");
    if(sum){
      var sigN = Number(s.total_trades || 0);
      var daysN = Number(s.days || 0);
      var sessR = Number(s.sessions_replayed != null ? s.sessions_replayed : 0);
      var sessI = Number(s.sessions_in_range != null ? s.sessions_in_range : sessR);
      sum.innerHTML = ''
        + '<div class="is-cash-trk-metric is-cash-trk-metric--hero">'
        +   '<div class="k">Total signals (replay)</div>'
        +   '<div class="v good">' + esc(sigN) + '</div>'
        +   '<div class="mut" style="margin-top:4px">Sessions replayed: ' + esc(sessR) + (sessI > sessR ? " of " + esc(sessI) + " in range" : "") + "</div>"
        + '</div>'
        + '<div class="is-cash-trk-metric"><div class="k">Days with data</div><div class="v">' + esc(daysN) + '</div></div>'
        + '<div class="is-cash-trk-metric"><div class="k">Win rate</div><div class="v mut">—</div><div class="mut" style="font-size:10px">N/A (no exit sim)</div></div>'
        + '<div class="is-cash-trk-metric"><div class="k">Total ₹ P&amp;L</div><div class="v mut">₹0</div><div class="mut" style="font-size:10px">not simulated</div></div>'
        + '<div class="is-cash-trk-metric"><div class="k">Max drawdown</div><div class="v mut">—</div></div>'
        + '<div class="is-cash-trk-metric"><div class="k">Signals / day (avg)</div><div class="v">' + esc(daysN ? (sigN / Math.max(1, daysN)).toFixed(1) : "—") + '</div></div>';
    }
    var dateSel = el("is-cash-trk-date");
    if(dateSel){
      var dates = ["ALL"].concat(Array.from(new Set((b.day_log || []).map(function(r){ return String(r.date || ""); }).filter(Boolean))).sort().reverse());
      var existing = String(STATE.cashPopDate || "ALL");
      dateSel.innerHTML = dates.map(function(d){
        return '<option value="' + esc(d) + '"' + (d === existing ? " selected" : "") + '>' + esc(d) + '</option>';
      }).join("");
    }
    var key = _cashFilterKey(tab);
    var rows = _cashRowsByTab(tab, b);
    if(selectedDate !== "ALL"){
      if(tab === "day_log"){
        rows = rows.filter(function(r){ return String(r.date || "") === selectedDate; });
      }else if(tab === "trade_detail"){
        rows = dateSig.slice();
      }else if(tab === "by_strategy"){
        var sm = {};
        dateSig.forEach(function(t){
          var sid = String(t.strategy_id || "SPIKE");
          if(!sm[sid]) sm[sid] = { strategy_id: sid, strategy_name: String(t.strategy_name || ""), trades: 0 };
          sm[sid].trades += 1;
        });
        rows = Object.keys(sm).map(function(k){ return sm[k]; });
      }else if(tab === "by_symbol"){
        var bm = {};
        dateSig.forEach(function(t){
          var sy = String(t.instrument || "");
          if(!sy) return;
          bm[sy] = (bm[sy] || 0) + 1;
        });
        rows = Object.keys(bm).sort(function(a,b){ return bm[b]-bm[a]; }).map(function(sym){
          return { symbol: sym, signals: bm[sym] };
        });
      }
    }
    _cashRenderFilters(rows, key);
    var filter = String(STATE.cashPopFilter || "ALL").toUpperCase();
    rows = filter === "ALL" ? rows : rows.filter(function(r){ return String(r[key] || "").toUpperCase() === filter; });
    var host = el("is-cash-trk-view");
    if(!host) return;
    if(tab === "day_log"){
      host.innerHTML = '<table class="is-cash-trk-table"><thead><tr><th>Date</th><th>Regime</th><th>Strategies</th><th>Signals</th><th>W/L</th><th>Day ₹ P&amp;L</th><th>Notes</th></tr></thead><tbody>'
        + (rows.length ? rows.map(function(r){
          var tags = (r.strategies_used || []).map(function(x){ return '<span class="is-cash-trk-tag">' + esc(x) + '</span>'; }).join("");
          return '<tr><td>' + esc(r.date || "") + '</td><td><span class="is-cash-trk-tag">' + esc(r.regime || "") + '</span></td><td>' + tags + '</td><td>' + esc(r.trades || 0) + '</td><td>' + esc(r.wl || "—") + '</td><td>—</td><td style="font-size:11px;color:#d6d3d1">' + esc(r.notes || "") + '</td></tr>';
        }).join("") : '<tr><td colspan="7" style="color:#78716c">Choose a date range that includes your recording sessions (see table below), then press <strong>RUN SIGNAL REPORT</strong>. Empty ranges show zeros — ₹ P&amp;L is not simulated.</td></tr>')
        + '</tbody></table>';
      return;
    }
    if(tab === "trade_detail"){
      host.innerHTML = '<table class="is-cash-trk-table"><thead><tr><th>Date</th><th>Session</th><th>Instrument</th><th>Dir</th><th>Score</th><th>Entry</th><th>SL</th><th>Target</th><th>₹ P&amp;L</th></tr></thead><tbody>'
        + (rows.length ? rows.slice(0, 800).map(function(r){
          return '<tr><td>' + esc(r.date || "") + '</td><td style="font-size:10px">' + esc(r.session_id || "") + '</td><td>' + esc(r.instrument || "") + '</td><td>' + esc(r.dir || "") + '</td><td>' + esc(Number(r.score || 0).toFixed(2)) + '</td><td>' + esc(Number(r.entry_price || 0).toFixed(2)) + '</td><td>' + esc(Number(r.sl || 0).toFixed(2)) + '</td><td>' + esc(Number(r.target || 0).toFixed(2)) + '</td><td>—</td></tr>';
        }).join("") : '<tr><td colspan="9" style="color:#78716c">No signals in range.</td></tr>')
        + '</tbody></table>';
      return;
    }
    if(tab === "by_strategy"){
      host.innerHTML = rows.length ? rows.map(function(r){
        return '<div class="is-cash-trk-row"><div><span class="is-cash-trk-tag">' + esc(r.strategy_id || "") + '</span> <b>' + esc(r.strategy_name || "") + '</b><div style="font-size:11px;color:#a8a29e;margin-top:4px">' + esc(r.trades || 0) + ' signals (replay)</div></div><div style="font-weight:900;font-size:22px;color:#fde68a">' + esc(r.trades || 0) + '</div></div>';
      }).join("") : '<div style="color:#78716c;margin-top:10px">No rows.</div>';
      return;
    }
    host.innerHTML = rows.length ? rows.map(function(r){
      return '<div class="is-cash-trk-row"><div><b>' + esc(r.symbol || "") + '</b><div style="font-size:11px;color:#a8a29e;margin-top:4px">signals on replay</div></div><div style="font-weight:900;font-size:26px;color:#fde68a">' + esc(r.signals || 0) + '</div></div>';
    }).join("") : '<div style="color:#78716c;margin-top:10px">No rows.</div>';
  }

  function runCashRange(ev){
    var force = !!(ev && ev.shiftKey);
    var pf = el("is-cash-trk-from"), pt = el("is-cash-trk-to"), btn = el("is-cash-trk-run");
    var s = pf && String(pf.value || "").trim();
    var e = pt && String(pt.value || "").trim();
    if(!s || !e){ return; }
    if(btn){ btn.disabled = true; btn.textContent = force ? "WAIT… replay" : "WAIT…"; }
    reqLoose("/api/nse-cash/replay-report", { method: "POST", body: JSON.stringify({ start_date: s, end_date: e, max_sessions: 40, force_refresh: force }) })
      .then(function(x){
        var rep = x.d || {};
        if(!x.httpOk){
          var detail = rep.detail;
          if(Array.isArray(detail)){
            detail = detail.map(function(z){ return (z && (z.msg || z.message)) ? (z.msg || z.message) : String(z); }).join("; ");
          }
          var msg = String(detail != null ? detail : "").trim() || String(rep.error || "").trim() || ("HTTP " + x.status);
          if(x.status === 404 || /^not found$/i.test(msg)){
            msg = "404 Not Found — POST /api/nse-cash/replay-report is missing on this server. Restart backend (python main.py in backend/), hard-refresh the app, and ensure the browser targets that same origin.";
          }
          STATE.cashBt = { source: "nse_cash_replay", data_warning: msg, summary: {}, day_log: [], trade_detail: [], by_strategy: [], by_symbol: [] };
          renderCashTracker();
          return;
        }
        if(!rep.ok){
          STATE.cashBt = { source: "nse_cash_replay", data_warning: String(rep.error || rep.detail || "request failed"), summary: {}, day_log: [], trade_detail: [], by_strategy: [], by_symbol: [] };
          renderCashTracker();
          return;
        }
        STATE.cashBt = {
          source: "nse_cash_replay",
          served_from_cache: !!rep.served_from_cache,
          data_warning: String(rep.data_warning || ""),
          summary: rep.summary || {},
          day_log: rep.day_log || [],
          trade_detail: rep.trade_detail || [],
          by_strategy: rep.by_strategy || [],
          by_symbol: rep.by_symbol || [],
          by_regime: rep.by_regime || []
        };
        renderCashTracker();
      })
      .catch(function(err){
        STATE.cashBt = { source: "nse_cash_replay", data_warning: String(err.message || err), summary: {}, day_log: [], trade_detail: [], by_strategy: [], by_symbol: [] };
        renderCashTracker();
      })
      .finally(function(){
        if(btn){ btn.disabled = false; btn.textContent = "RUN SIGNAL REPORT"; }
      });
  }

  function _ensureCashPopup(){
    if(el("is-cash-trk-wrap")) return;
    var oldRoot = el("is-cash-popup-root");
    if(oldRoot) oldRoot.remove();
    var w = document.createElement("div");
    w.id = "is-cash-popup-root";
    w.innerHTML = ''
      + '<style>'
      + '#is-cash-mask{position:fixed;inset:0;background:rgba(2,6,23,.62);backdrop-filter:blur(2px);z-index:99998;display:none}'
      + '#is-cash-pop{position:fixed;left:10px;top:42px;width:calc(100vw - 20px);height:calc(100vh - 54px);overflow:auto;background:linear-gradient(180deg,#1a0f0a,#0c0a12);color:#fef3c7;border-radius:14px;box-shadow:0 28px 95px rgba(0,0,0,.55),inset 0 0 0 1px rgba(251,191,36,.22);z-index:99999;display:none;padding:16px 18px 24px;border:1px solid rgba(245,158,11,.35);font-family:var(--fn,inherit)}'
      + '#is-cash-pop .is-pop-close{border:1px solid rgba(251,191,36,.4);background:rgba(30,20,8,.85);border-radius:10px;padding:8px 12px;font-weight:900;font-size:11px;color:#fff7ed;cursor:pointer;font-family:var(--fn,inherit)}'
      + '.is-cash-h{font-size:clamp(22px,3.2vw,38px);font-weight:900;color:#fff7ed;line-height:1.1}'
      + '.is-cash-sub{font-size:12px;color:#fcd34d;margin-top:6px;opacity:.95;line-height:1.55}'
      + '.is-cash-sub-link{border:none;background:none;padding:0;margin:0;font:inherit;font-weight:800;color:#fbbf24;text-decoration:underline;cursor:pointer;text-underline-offset:2px}'
      + '.is-cash-sub-link:hover{color:#fde68a}'
      + '.is-cash-table{width:100%;border-collapse:collapse;margin-top:12px;font-size:12px}'
      + '.is-cash-table th,.is-cash-table td{border-bottom:1px solid rgba(251,191,36,.15);padding:8px 6px;text-align:left}'
      + '.is-cash-table th{color:#fde68a;font-size:10px;text-transform:uppercase}'
      + '#is-cash-pop .is-cash-table td,#is-cash-pop .is-cash-trk-table td{color:#fde68a!important}'
      + '#is-cash-pop .is-cash-trk-table th{color:#fcd34d!important}'
      + '.is-cash-pill{display:inline-block;padding:4px 10px;border-radius:8px;background:rgba(180,83,9,.35);color:#fff;font-size:11px;font-weight:800;margin:4px 6px 0 0}'
      + '.is-cash-note{margin-top:14px;font-size:12px;color:#d6d3d1;line-height:1.55}'
      + '.is-cash-rec{margin-top:18px;padding-top:14px;border-top:1px solid rgba(251,191,36,.2)}'
      + '.is-cash-rec h3{margin:0 0 8px;font-size:13px;color:#fde68a;letter-spacing:.4px}'
      + '.is-cash-rec-row{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-top:8px}'
      + '.is-cash-btn{border:1px solid rgba(251,191,36,.45);background:rgba(180,83,9,.25);color:#fff7ed;border-radius:8px;padding:7px 12px;font-weight:800;font-size:11px;cursor:pointer;font-family:var(--fn,inherit)}'
      + '.is-cash-btn:disabled{opacity:.45;cursor:not-allowed}'
      + '.is-cash-inp{flex:1;min-width:160px;border:1px solid rgba(251,191,36,.25);background:rgba(30,20,8,.6);color:#fff7ed;border-radius:8px;padding:7px 10px;font-size:12px}'
      + '.is-cash-pre{margin-top:10px;padding:10px;border-radius:8px;background:rgba(0,0,0,.35);border:1px solid rgba(251,191,36,.15);font-size:11px;white-space:pre-wrap;word-break:break-word;max-height:220px;overflow:auto;color:#e7e5e4}'
      + '.is-cash-sess{cursor:pointer}'
      + '.is-cash-sess:hover{background:rgba(251,191,36,.08)}'
      + '.is-cash-trk-toolbar{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-top:14px;padding:10px;border:1px solid rgba(251,191,36,.22);border-radius:10px;background:rgba(30,20,8,.5)}'
      + '.is-cash-trk-ctl{display:inline-flex;align-items:center;gap:6px}'
      + '.is-cash-trk-ctl b{font-size:10px;color:#fcd34d;letter-spacing:.5px}'
      + '.is-cash-trk-date{height:28px;padding:0 8px;border-radius:7px;border:1px solid rgba(251,191,36,.35);background:rgba(20,12,4,.65);color:#fef3c7;font-size:11px;font-family:var(--fn,inherit)}'
      + '.is-cash-trk-q{height:25px;padding:0 9px;border-radius:999px;border:1px solid rgba(251,191,36,.3);background:rgba(30,20,8,.72);color:#fcd34d;font-size:10px;font-weight:800;cursor:pointer}'
      + '.is-cash-trk-run{height:28px;padding:0 14px;border-radius:999px;border:1px solid rgba(34,197,94,.55);background:linear-gradient(135deg,rgba(22,163,74,.95),rgba(245,158,11,.9));color:#0c1a0d;font-size:10px;font-weight:900;letter-spacing:.5px;cursor:pointer}'
      + '.is-cash-trk-summary-wrap{margin-top:12px;padding:16px;border:1px solid rgba(251,191,36,.2);border-radius:14px;background:rgba(0,0,0,.2)}'
      + '.is-cash-trk-metrics{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}'
      + '@media(min-width:1100px){.is-cash-trk-metrics{grid-template-columns:repeat(6,minmax(0,1fr))}}'
      + '@media(max-width:520px){.is-cash-trk-metrics{grid-template-columns:repeat(2,minmax(0,1fr))}}'
      + '.is-cash-trk-metric{display:flex;flex-direction:column;gap:4px;padding:12px;border-radius:10px;border:1px solid rgba(251,191,36,.12);background:rgba(40,25,8,.35);min-height:76px}'
      + '.is-cash-trk-metric--hero{grid-column:1/-1;border-color:rgba(251,191,36,.35)}'
      + '.is-cash-trk-metric .k{font-size:10px;color:#d6d3d1;text-transform:uppercase;font-weight:700}'
      + '.is-cash-trk-metric .v{font-size:clamp(22px,3vw,36px);font-weight:900;color:#fffbeb}'
      + '.is-cash-trk-metric .v.good{color:#86efac}.is-cash-trk-metric .v.mut{color:#94a3b8;font-size:12px;font-weight:700}'
      + '.is-cash-trk-engine{margin-top:10px;padding:10px 12px;border-radius:10px;border:1px solid rgba(251,191,36,.18);background:rgba(0,0,0,.25);font-size:11px;line-height:1.45;color:#e7e5e4}'
      + '.is-cash-trk-warn{margin-top:10px;display:none;padding:10px;border-radius:10px;border:1px solid rgba(248,113,113,.4);background:rgba(60,10,10,.4);color:#fecaca;font-size:12px}'
      + '.is-cash-trk-hint{margin-top:10px;display:none;padding:10px;border-radius:10px;border:1px solid rgba(251,191,36,.35);background:rgba(60,40,8,.45);color:#fef3c7;font-size:12px;line-height:1.45}'
      + '.is-cash-trk-tabs{display:flex;gap:18px;border-bottom:1px solid rgba(251,191,36,.2);margin-top:14px;flex-wrap:wrap}'
      + '.is-cash-trk-tab{border:none;background:none;padding:8px 0 9px;font-size:15px;color:#a8a29e;cursor:pointer;font-weight:600;border-bottom:2px solid transparent;font-family:var(--fn,inherit)}'
      + '.is-cash-trk-tab.on{border-bottom-color:#fbbf24;color:#fffbeb;font-weight:800}'
      + '.is-cash-trk-filters{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}'
      + '.is-cash-trk-chip{border:1px solid rgba(251,191,36,.3);background:rgba(30,20,8,.68);color:#fef3c7;border-radius:999px;padding:5px 11px;font-size:11px;cursor:pointer;font-weight:700}'
      + '.is-cash-trk-chip.on{background:#b45309;color:#fffbeb;border-color:#fbbf24}'
      + '.is-cash-trk-table{width:100%;border-collapse:collapse;margin-top:10px;font-size:12px}'
      + '.is-cash-trk-table th,.is-cash-trk-table td{padding:8px;border-bottom:1px solid rgba(251,191,36,.12);text-align:left;vertical-align:top}'
      + '.is-cash-trk-table th{font-size:10px;color:#fcd34d;text-transform:uppercase}'
      + '.is-cash-trk-tag{display:inline-flex;padding:3px 8px;border-radius:999px;background:rgba(245,158,11,.25);color:#fff;font-size:10px;font-weight:800;margin:2px 4px 0 0}'
      + '.is-cash-trk-row{display:flex;justify-content:space-between;gap:10px;border-bottom:1px solid rgba(251,191,36,.12);padding:10px 0}'
      + '</style>'
      + '<div id="is-cash-mask"></div>'
      + '<div id="is-cash-pop">'
      + '  <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:10px">'
      + '    <div><div class="is-cash-h">NSE Cash — day-by-day signal log (JSONL replay)</div>'
      + '    <div class="is-cash-sub">Same layout as <button type="button" class="is-cash-sub-link" id="is-cash-open-fut">NSE Future</button> tracker · microstructure ticks · <strong>signal counts</strong> only (no full-engine ₹ P&amp;L).</div></div>'
      + '    <div><button type="button" id="is-cash-refresh" class="is-pop-close">Refresh</button> <button type="button" id="is-cash-close" class="is-pop-close">Close</button></div>'
      + '  </div>'
      + '  <div id="is-cash-trk-wrap">'
      + '    <div class="is-cash-trk-toolbar">'
      + '      <div class="is-cash-trk-ctl"><b>FROM</b><input id="is-cash-trk-from" class="is-cash-trk-date" type="date"></div>'
      + '      <div class="is-cash-trk-ctl"><b>TO</b><input id="is-cash-trk-to" class="is-cash-trk-date" type="date"></div>'
      + '      <div class="is-cash-trk-ctl"><b>DATE</b><select id="is-cash-trk-date" class="is-cash-trk-date"><option value="ALL">ALL</option></select></div>'
      + '      <div class="is-cash-trk-ctl"><button type="button" class="is-cash-trk-q" data-d="10">10D</button><button type="button" class="is-cash-trk-q" data-d="30">30D</button><button type="button" class="is-cash-trk-q" data-d="90">3M</button></div>'
      + '      <div class="is-cash-trk-ctl"><button id="is-cash-trk-run" class="is-cash-trk-run" type="button" title="Uses SQLite cache when range + JSONL files unchanged (fast). Shift+click to force full replay.">RUN SIGNAL REPORT</button></div>'
      + '    </div>'
      + '    <div id="is-cash-trk-warn" class="is-cash-trk-warn"></div>'
      + '    <div id="is-cash-trk-engine" class="is-cash-trk-engine"></div>'
      + '    <div class="is-cash-trk-summary-wrap"><div id="is-cash-trk-summary" class="is-cash-trk-metrics"></div></div>'
      + '    <div class="is-cash-trk-tabs">'
      + '      <button type="button" class="is-cash-trk-tab on" data-v="day_log">Day log</button>'
      + '      <button type="button" class="is-cash-trk-tab" data-v="trade_detail">Signal detail</button>'
      + '      <button type="button" class="is-cash-trk-tab" data-v="by_strategy">By strategy</button>'
      + '      <button type="button" class="is-cash-trk-tab" data-v="by_symbol">By symbol</button>'
      + '    </div>'
      + '    <div id="is-cash-trk-filters" class="is-cash-trk-filters"></div>'
      + '    <div id="is-cash-trk-view"></div>'
      + '  </div>'
      + '  <div class="is-cash-note" style="margin-top:18px;padding-top:14px;border-top:1px solid rgba(251,191,36,.2)"><strong>Scanner context</strong> — order book · LTQ · classifier · sector · basis · velocity · composite gate.</div>'
      + '  <div id="is-cash-status" class="is-cash-note" style="margin-top:12px">Loading…</div>'
      + '  <div class="is-cash-note"><strong>Why price is late</strong> — microstructure timeline (you enter T-1…T-2, not T+0):</div>'
      + '  <table class="is-cash-table"><thead><tr><th>Offset</th><th>Signal</th><th>Note</th></tr></thead><tbody id="is-cash-lead-body"><tr><td colspan="3" style="color:#a8a29e">Loading…</td></tr></tbody></table>'
      + '  <div class="is-cash-note" id="is-cash-weights"></div>'
      + '  <div class="is-cash-note"><strong>Run live scanner:</strong> Python process uses Kite <code>MODE_FULL</code> on EQ and <code>MODE_QUOTE</code> on stock futures — see <code>backend/nse_cash/scanner/main.py</code>. SQLite log: <code>backend/data/nse_cash_spike.db</code>.</div>'
      + '  <div class="is-cash-rec" id="is-cash-rec-box">'
      + '    <h3>Tick recording &amp; replay</h3>'
      + '    <div class="is-cash-note" style="margin-top:0">Records microstructure ticks to JSONL (second Kite connection — avoid if your token is near limits). Replay runs the same <code>SpikeScanner</code> path as live. <strong>Lines stay 0</strong> until the cash session is open (≈9:15–15:30 IST) and ticks flow; pre-market you will see 0.</div>'
      + '    <div class="is-cash-rec-row">'
      + '      <button type="button" class="is-cash-btn" id="is-cash-rec-start">Start recording</button>'
      + '      <button type="button" class="is-cash-btn" id="is-cash-rec-stop">Stop recording</button>'
      + '      <span id="is-cash-rec-pill" class="is-cash-pill">Record status: …</span>'
      + '    </div>'
      + '    <table class="is-cash-table"><thead><tr><th>Session</th><th>Started</th><th>Ticks</th><th>Status</th></tr></thead><tbody id="is-cash-rec-body"><tr><td colspan="4" style="color:#a8a29e">Loading…</td></tr></tbody></table>'
      + '    <div class="is-cash-note" style="margin-top:10px">Replay: use the <strong>Session</strong> id from the table (click a row to fill). Not your email or Kite login.</div>'
      + '    <div class="is-cash-rec-row" style="margin-top:12px">'
      + '      <input class="is-cash-inp" id="is-cash-replay-sid" placeholder="cash-YYYYMMDD-HHMMSS from table" autocomplete="off" />'
      + '      <input class="is-cash-inp" id="is-cash-replay-max" style="max-width:120px" placeholder="max_ticks (optional)" />'
      + '      <button type="button" class="is-cash-btn" id="is-cash-replay-run">Run replay</button>'
      + '    </div>'
      + '    <pre class="is-cash-pre" id="is-cash-replay-out"></pre>'
      + '  </div>'
      + '</div>';
    document.body.appendChild(w);
    var showCash = function(on){
      if(el("is-cash-pop")) el("is-cash-pop").style.display = on ? "block" : "none";
      if(el("is-cash-mask")) el("is-cash-mask").style.display = on ? "block" : "none";
    };
    if(!window.intradaySystemOpenCashPopup){
      window.intradaySystemOpenCashPopup = function(){
        if(el("is-pop")) el("is-pop").style.display = "none";
        if(el("is-pop-mask")) el("is-pop-mask").style.display = "none";
        showCash(true);
        loadCashPopupData();
        renderCashTracker();
      };
    }
    if(el("is-cash-trk-from") && !el("is-cash-trk-from").value) el("is-cash-trk-from").value = _daysAgoIso(90);
    if(el("is-cash-trk-to") && !el("is-cash-trk-to").value) el("is-cash-trk-to").value = _todayIso();
    if(el("is-cash-close")) el("is-cash-close").onclick = function(){ showCash(false); };
    if(el("is-cash-mask")) el("is-cash-mask").onclick = function(){ showCash(false); };
    if(el("is-cash-refresh")) el("is-cash-refresh").onclick = function(){ loadCashPopupData(); renderCashTracker(); };
    if(el("is-cash-trk-run")) el("is-cash-trk-run").onclick = function(ev){ runCashRange(ev); };
    if(el("is-cash-trk-date")) el("is-cash-trk-date").onchange = function(){
      STATE.cashPopDate = String(this.value || "ALL");
      renderCashTracker();
    };
    document.querySelectorAll(".is-cash-trk-q").forEach(function(q){
      q.onclick = function(){
        var d = Number(this.getAttribute("data-d") || 30);
        if(el("is-cash-trk-from")) el("is-cash-trk-from").value = _daysAgoIso(d);
        if(el("is-cash-trk-to")) el("is-cash-trk-to").value = _todayIso();
      };
    });
    document.querySelectorAll(".is-cash-trk-tab").forEach(function(btn){
      btn.onclick = function(){
        STATE.cashPopTab = String(this.getAttribute("data-v") || "day_log");
        STATE.cashPopFilter = "ALL";
        document.querySelectorAll(".is-cash-trk-tab").forEach(function(x){ x.classList.remove("on"); });
        btn.classList.add("on");
        renderCashTracker();
      };
    });
    if(el("is-cash-rec-start")) el("is-cash-rec-start").onclick = function(){
      var out = el("is-cash-replay-out");
      reqLoose("/api/nse-cash/record/start", { method: "POST", body: "{}" })
        .then(function(x){
          var d = x.d || {};
          if(out) out.textContent = JSON.stringify(d, null, 2);
          loadCashPopupData();
        });
    };
    if(el("is-cash-rec-stop")) el("is-cash-rec-stop").onclick = function(){
      var out = el("is-cash-replay-out");
      reqLoose("/api/nse-cash/record/stop", { method: "POST", body: "{}" })
        .then(function(x){
          var d = x.d || {};
          if(out) out.textContent = JSON.stringify(d, null, 2);
          loadCashPopupData();
        });
    };
    if(el("is-cash-replay-run")) el("is-cash-replay-run").onclick = function(){
      var sid = el("is-cash-replay-sid"), mx = el("is-cash-replay-max"), out = el("is-cash-replay-out");
      var s = sid && String(sid.value || "").trim();
      if(!s){ if(out) out.textContent = "Enter session_id."; return; }
      if(s.indexOf("@") >= 0){ if(out) out.textContent = "Use the Session id from the table (e.g. cash-...), not an email. Click a row to fill."; return; }
      var payload = { session_id: s };
      var mt = mx && String(mx.value || "").trim();
      if(mt && !isNaN(Number(mt))) payload.max_ticks = Number(mt);
      if(out) out.textContent = "Running…";
      req("/api/nse-cash/replay", { method: "POST", body: JSON.stringify(payload) })
        .then(function(d){ if(out) out.textContent = JSON.stringify(d, null, 2); })
        .catch(function(e){ if(out) out.textContent = esc(e.message || e); });
    };
    if(el("is-cash-open-fut")) el("is-cash-open-fut").onclick = function(){
      showCash(false);
      if(typeof window.intradaySystemOpenPopup === "function") window.intradaySystemOpenPopup();
    };
  }

  function loadCashPopupData(){
    var st = el("is-cash-status");
    var tb = el("is-cash-lead-body");
    var wg = el("is-cash-weights");
    var recPill = el("is-cash-rec-pill");
    var recBody = el("is-cash-rec-body");
    if(st) st.textContent = "Loading…";
    req("/api/nse-cash/record/status", { method: "GET" })
      .then(function(d){
        if(recPill){
          var active = d.active ? "ON" : "off";
          var sid = d.session_id ? esc(d.session_id) : "—";
          var lw = typeof d.lines_written === "number" ? d.lines_written : "—";
          recPill.innerHTML = "Record: <strong>" + active + "</strong> · session " + sid + " · lines " + lw;
        }
      })
      .catch(function(){
        if(recPill) recPill.textContent = "Record status unavailable";
      });
    req("/api/nse-cash/recordings", { method: "GET" })
      .then(function(d){
        var rows = (d || {}).recordings || [];
        if(recBody){
          recBody.innerHTML = rows.length ? rows.map(function(r){
            var raw = String(r.session_id || "");
            var idAttr = encodeURIComponent(raw);
            return "<tr class=\"is-cash-sess\" data-sid=\"" + idAttr + "\"><td><code style=\"font-size:11px\">" + esc(raw) + "</code></td><td>" + esc(r.started_at || "") + "</td><td>" + esc(String(r.tick_count != null ? r.tick_count : "—")) + "</td><td>" + esc(r.status || "") + "</td></tr>";
          }).join("") : "<tr><td colspan=\"4\">No sessions yet.</td></tr>";
          recBody.querySelectorAll("tr.is-cash-sess").forEach(function(tr){
            tr.onclick = function(){
              var enc = this.getAttribute("data-sid");
              var id = enc ? decodeURIComponent(enc) : "";
              var inp = el("is-cash-replay-sid");
              if(inp && id) inp.value = id;
            };
          });
        }
      })
      .catch(function(){
        if(recBody) recBody.innerHTML = "<tr><td colspan=\"4\" style=\"color:#f87171\">Could not load recordings.</td></tr>";
      });
    req("/api/nse-cash/status", { method: "GET" })
      .then(function(d){
        if(st){
          var parts = ["Segment: " + esc(d.segment || "—")];
          if(d.module_loaded) parts.push("Module OK");
          if(d.db_path) parts.push("DB: " + esc(d.db_path));
          if(d.error) parts.push("Error: " + esc(d.error));
          st.innerHTML = parts.map(function(p){ return "<div class=\"is-cash-pill\">" + p + "</div>"; }).join(" ");
        }
        if(wg && d.weights){
          wg.innerHTML = "<strong>Composite weights</strong> — " + esc(JSON.stringify(d.weights));
        }
      })
      .catch(function(e){
        if(st) st.textContent = "Status failed: " + esc(e.message || e);
      });
    req("/api/nse-cash/lead-lag", { method: "GET" })
      .then(function(d){
        var rows = (d || {}).rows || [];
        if(tb){
          tb.innerHTML = rows.length ? rows.map(function(r){
            return "<tr><td>" + esc(r.t || "") + "</td><td>" + esc(r.signal || "") + "</td><td>" + esc(r.note || "") + "</td></tr>";
          }).join("") : "<tr><td colspan=\"3\">No rows.</td></tr>";
        }
      })
      .catch(function(){
        if(tb) tb.innerHTML = "<tr><td colspan=\"3\" style=\"color:#f87171\">Could not load lead–lag table.</td></tr>";
      });
  }

  function _openFutureTracker(anchor){
    STATE.futAnchor = anchor === "nifty" ? "nifty" : "bnf";
    try{ sessionStorage.setItem("is_fut_anchor", STATE.futAnchor); }catch(_e){}
    _syncFutTopBtnStyles();
    if(typeof window.intradaySystemOpenPopup === "function") window.intradaySystemOpenPopup();
  }
  function _wireFutSubLinks(){
    var sub = el("is-pop-sub");
    if(!sub) return;
    sub.querySelectorAll("[data-is-fut-switch]").forEach(function(btn){
      btn.onclick = function(){
        var a = String(this.getAttribute("data-is-fut-switch") || "bnf");
        _openFutureTracker(a === "nifty" ? "nifty" : "bnf");
      };
    });
  }
  function _syncFutPopupHeadings(){
    var h = el("is-pop-title");
    var sub = el("is-pop-sub");
    if(!h) return;
    var sw = '<button type="button" class="is-pop-sub-link" data-is-fut-switch="bnf">BNIFTY FUTURE</button>'
      + " · "
      + '<button type="button" class="is-pop-sub-link" data-is-fut-switch="nifty">NIFTY FUTURE</button>';
    if(String(STATE.futAnchor || "bnf") === "nifty"){
      h.textContent = "NIFTY FUTURE";
      if(sub) sub.innerHTML = "Full-engine index futures (Kite minute history) on <strong>NIFTY 50</strong>. Day log + trade detail + strategy/regime tabs — mirror of the options shadow UI. Switch anchor: " + sw + ".";
    }else{
      h.textContent = "BNIFTY FUTURE";
      if(sub) sub.innerHTML = "Full-engine index futures (Kite minute history) on <strong>NIFTY BANK</strong>. Same chrome as <strong>NIFTY OPTIONS</strong> for easy comparison. Switch anchor: " + sw + ".";
    }
    _wireFutSubLinks();
  }
  function _syncFutTopBtnStyles(){
    var b1 = el("is-pop-topbtn-bnf");
    var b2 = el("is-pop-topbtn-nifty");
    var a = String(STATE.futAnchor || "bnf");
    if(b1) b1.classList.toggle("on", a === "bnf");
    if(b2) b2.classList.toggle("on", a === "nifty");
  }
  function _mountTopButton(){
    var dayBtn = document.getElementById("tb-day-perf-btn")
      || document.querySelector("button.tbbtn.primary[onclick*='openDayPerf']")
      || document.querySelector(".tbr button.tbbtn.primary");
    if(!dayBtn) return;
    if(!el("is-market-mode-wrap") && dayBtn.parentNode){
      var mw = document.createElement("div");
      mw.id = "is-market-mode-wrap";
      mw.style.cssText = "display:inline-flex;align-items:center;gap:6px;margin-right:10px;vertical-align:middle;flex-wrap:wrap";
      mw.innerHTML = ""
        + "<span style=\"font-size:8px;color:rgba(148,163,184,.95);letter-spacing:.85px;text-transform:uppercase;font-weight:800\">Mode</span>"
        + "<div style=\"display:inline-flex;border-radius:999px;border:1px solid rgba(100,116,139,.5);overflow:hidden;background:rgba(15,23,42,.85)\">"
        + "<button type=\"button\" id=\"is-mode-live\" style=\"border:none;padding:5px 12px;font-size:9px;font-weight:700;font-family:var(--fn,inherit);cursor:pointer\">LIVE</button>"
        + "<button type=\"button\" id=\"is-mode-bt\" style=\"border:none;padding:5px 12px;font-size:9px;font-weight:700;font-family:var(--fn,inherit);cursor:pointer\">BACKTEST</button>"
        + "</div>";
      dayBtn.parentNode.insertBefore(mw, dayBtn);
      if(el("is-mode-live")) el("is-mode-live").onclick = function(){ _setMarketMode("live"); };
      if(el("is-mode-bt")) el("is-mode-bt").onclick = function(){ _setMarketMode("backtest"); };
      _syncMarketModeToggleUI();
    }
    dayBtn.classList.add("is-tb-keep");
    var leg = el("is-pop-topbtn");
    if(leg) leg.remove();
    if(!el("is-pop-topbtn-bnf")){
      var b1 = document.createElement("button");
      b1.id = "is-pop-topbtn-bnf";
      b1.className = "is-pop-futbtn is-tb-keep";
      b1.type = "button";
      b1.textContent = "BNIFTY FUTURE";
      b1.title = "BANKNIFTY — full-engine backtest (NIFTY BANK anchor)";
      b1.onclick = function(){ _openFutureTracker("bnf"); };
      dayBtn.parentNode.insertBefore(b1, dayBtn);
    }
    if(!el("is-pop-topbtn-nifty")){
      var b2 = document.createElement("button");
      b2.id = "is-pop-topbtn-nifty";
      b2.className = "is-pop-futbtn is-tb-keep";
      b2.type = "button";
      b2.textContent = "NIFTY FUTURE";
      b2.title = "NIFTY — full-engine backtest (NIFTY 50 anchor)";
      b2.onclick = function(){ _openFutureTracker("nifty"); };
      var ref = el("is-pop-topbtn-bnf");
      (ref && ref.parentNode ? ref.parentNode : dayBtn.parentNode).insertBefore(b2, ref ? ref.nextSibling : dayBtn);
    }
    try{
      var sa = sessionStorage.getItem("is_fut_anchor");
      if(sa === "nifty" || sa === "bnf") STATE.futAnchor = sa;
    }catch(_e){}
    _syncFutTopBtnStyles();
    if(!el("is-cash-topbtn")){
      var c = document.createElement("button");
      c.id = "is-cash-topbtn";
      c.className = "is-tb-keep";
      c.type = "button";
      c.textContent = "NSE CASH";
      c.title = "NSE Cash — JSONL signal report + recording (EQ)";
      c.onclick = function(){
        if(typeof window.intradaySystemOpenCashPopup === "function") window.intradaySystemOpenCashPopup();
      };
      var niftyBtn = el("is-pop-topbtn-nifty");
      if(niftyBtn && niftyBtn.parentNode) niftyBtn.parentNode.insertBefore(c, niftyBtn.nextSibling);
      else dayBtn.parentNode.insertBefore(c, dayBtn);
    }
    if(!el("is-advcash-topbtn")){
      var av = document.createElement("button");
      av.id = "is-advcash-topbtn";
      av.className = "is-tb-keep";
      av.type = "button";
      av.textContent = "NSE ADV CASH";
      av.title = "NSE ADV CASH — 10-step accuracy tracker · OBI slope · KNN · drawdown protection";
      av.onclick = function(){ if(typeof window.openAdvCashPopup==="function") window.openAdvCashPopup(); };
      var cashBtn = el("is-cash-topbtn");
      if(cashBtn && cashBtn.parentNode) cashBtn.parentNode.insertBefore(av, cashBtn.nextSibling);
      else dayBtn.parentNode.insertBefore(av, dayBtn);
    }
    /* BNIFTY / NIFTY OPTIONS toolbar: options_shadow_tracker.js mounts #opt-btn-bnf / #opt-btn-nifty (styled, API popup). */
  }

  function _popupRegimePill(v){
    var r = String(v || "").toUpperCase();
    var c = r === "CHOP" ? "chop" : (r === "BREAKOUT" ? "breakout" : (r === "MIXED" ? "mixed" : ""));
    return '<span class="is-pop-pill ' + c + '">' + esc(r || "—") + '</span>';
  }

  function _popupRenderFilters(rows, key){
    var host = el("is-pop-filters");
    if(!host) return [];
    var vals = [];
    if(key === "regime"){
      vals = ["ALL","TREND","CHOP","BREAKOUT","MIXED","UNCLASSIFIED"];
    }else if(key === "strategy_id"){
      vals = ["ALL","S1","S2","S3","S4","S5","S6","S7","S8","S9","S10","S11","S12","S13","S14","S15"];
    }else{
      vals = ["ALL"].concat(Array.from(new Set((rows || []).map(function(r){ return String(r[key] || "").toUpperCase(); }).filter(Boolean))).sort());
    }
    host.innerHTML = vals.map(function(v){
      return '<button class="is-pop-chip' + (String(STATE.popFilter || "ALL").toUpperCase() === v ? " on" : "") + '" data-v="' + esc(v) + '" type="button">' + esc(v) + '</button>';
    }).join("");
    host.querySelectorAll(".is-pop-chip").forEach(function(c){
      c.onclick = function(){ STATE.popFilter = String(this.getAttribute("data-v") || "ALL"); renderPopupTracker(); };
    });
    return vals;
  }

  function _popupClipRangeRows(rows, fromY, toY){
    if(!fromY || !toY || fromY > toY) return (rows || []).slice();
    return (rows || []).filter(function(r){
      var d = String(r.date || r.day || "").split("T")[0].trim();
      return d && d >= fromY && d <= toY;
    });
  }
  function _popupMaxDdFromDayLog(dayRows){
    var sorted = (dayRows || []).slice().sort(function(a, b){
      return String(a.date || a.day || "").localeCompare(String(b.date || b.day || ""));
    });
    var cum = 0, peak = 0, maxDd = 0;
    sorted.forEach(function(r){
      cum += Number(r.day_pnl || 0);
      if(cum > peak) peak = cum;
      var draw = cum - peak;
      if(draw < maxDd) maxDd = draw;
    });
    return maxDd;
  }
  function _popupDayRowsFromTrades(trades){
    var dm = {};
    (trades || []).forEach(function(t){
      var d = String(t.date || "").split("T")[0].trim();
      if(!d) return;
      if(!dm[d]) dm[d] = { date: d, day_pnl: 0 };
      dm[d].day_pnl += Number(t.pnl_inr || 0);
    });
    return Object.keys(dm).sort().map(function(k){ return dm[k]; });
  }
  function _popupSummFromWindow(trades, dayRows){
    trades = trades || [];
    dayRows = dayRows || [];
    var dr = dayRows.length ? dayRows : _popupDayRowsFromTrades(trades);
    var tp = trades.reduce(function(a, t){ return a + Number(t.pnl_inr || 0); }, 0);
    var tw = trades.filter(function(t){ return Number(t.pnl_inr || 0) > 0; }).length;
    var tt = trades.length;
    var uniq = {};
    trades.forEach(function(t){
      var d = String(t.date || "").split("T")[0].trim();
      if(d) uniq[d] = 1;
    });
    var daysN = Object.keys(uniq).length;
    if(!daysN && dr.length) daysN = dr.length;
    return {
      total_pnl: tp,
      total_trades: tt,
      win_rate: tt ? Math.round(tw * 1000 / tt) / 10 : 0,
      days: daysN,
      max_drawdown: _popupMaxDdFromDayLog(dr),
    };
  }
  function _popupByStrategyAgg(trades){
    var sm = {};
    (trades || []).forEach(function(t){
      var sid = String(t.strategy_id || "NA");
      if(!sm[sid]) sm[sid] = { strategy_id: sid, strategy_name: String(t.strategy_name || ""), trades: 0, wins: 0, pnl: 0, best: -1e18, worst: 1e18 };
      var p = Number(t.pnl_inr || 0);
      sm[sid].trades += 1;
      sm[sid].pnl += p;
      if(p > 0) sm[sid].wins += 1;
      sm[sid].best = Math.max(sm[sid].best, p);
      sm[sid].worst = Math.min(sm[sid].worst, p);
    });
    return Object.keys(sm).sort(function(a, b){ return sm[b].pnl - sm[a].pnl; }).map(function(k){
      var x = sm[k];
      return {
        strategy_id: x.strategy_id,
        strategy_name: x.strategy_name,
        trades: x.trades,
        win_rate: Number((x.wins * 100.0 / Math.max(1, x.trades)).toFixed(1)),
        target_win_rate: 65,
        total_pnl: Number(x.pnl.toFixed(2)),
        avg_pnl: Number((x.pnl / Math.max(1, x.trades)).toFixed(2)),
        best: Number((x.best <= -1e17 ? 0 : x.best).toFixed(2)),
        worst: Number((x.worst >= 1e17 ? 0 : x.worst).toFixed(2)),
      };
    });
  }
  function _popupByRegimeAgg(trades){
    var rm = {};
    (trades || []).forEach(function(t){
      var r = String(t.regime || "UNCLASSIFIED").toUpperCase();
      if(!rm[r]) rm[r] = { regime: r, days: {}, trades: 0, wins: 0, pnl: 0, top: {} };
      var p = Number(t.pnl_inr || 0);
      rm[r].days[String(t.date || "")] = 1;
      rm[r].trades += 1;
      rm[r].pnl += p;
      if(p > 0) rm[r].wins += 1;
      var sid2 = String(t.strategy_id || "NA");
      rm[r].top[sid2] = (rm[r].top[sid2] || 0) + 1;
    });
    return Object.keys(rm).sort().map(function(rk){
      var x = rm[rk];
      return {
        regime: rk,
        days: Object.keys(x.days).length,
        trades: x.trades,
        win_rate: Number((x.wins * 100.0 / Math.max(1, x.trades)).toFixed(1)),
        total_pnl: Number(x.pnl.toFixed(2)),
        top_strategies: Object.keys(x.top).sort(function(a, b){ return x.top[b] - x.top[a]; }).slice(0, 5).map(function(k){ return { strategy_id: k, count: x.top[k] }; }),
      };
    });
  }

  function renderPopupTracker(){
    _ensurePopup();
    _updateFutPopupModeBanner();
    _syncFutPopModeTog();
    var b = STATE.bt || {};
    var s = b.summary || {};
    var fromIn = _normYmd((el("is-pop-from") || {}).value);
    var toIn = _normYmd((el("is-pop-to") || {}).value);
    var useClip = !!(fromIn && toIn && fromIn <= toIn);
    var tradesWin = useClip ? _popupClipRangeRows((b.trade_detail || []).slice(), fromIn, toIn) : (b.trade_detail || []).slice();
    var daysWin = useClip ? _popupClipRangeRows((b.day_log || []).slice(), fromIn, toIn) : (b.day_log || []).slice();
    var sDisp = useClip ? _popupSummFromWindow(tradesWin, daysWin) : s;
    var selectedDate = String(STATE.popDate || "ALL");
    var dateTrades = selectedDate === "ALL" ? tradesWin.slice() : tradesWin.filter(function(t){ return String(t.date || "").split("T")[0].trim() === selectedDate; });
    var datePnl = dateTrades.reduce(function(a, t){ return a + Number(t.pnl_inr || 0); }, 0);
    var dateWins = dateTrades.filter(function(t){ return Number(t.pnl_inr || 0) > 0; }).length;
    var dateWr = dateTrades.length ? (dateWins * 100.0 / dateTrades.length) : 0;
    var warnLine = el("is-pop-warn");
    if(warnLine){
      var dw = String((b || {}).data_warning || "").trim();
      if(dw){
        warnLine.style.display = "block";
        var wlab = /no minute|zero trades|empty/i.test(dw) ? "No data / range issue." : "Note.";
        warnLine.innerHTML = "<strong>" + esc(wlab) + "</strong> " + esc(dw);
      }else{
        warnLine.style.display = "none";
        warnLine.innerHTML = "";
      }
    }
    var engLine = el("is-pop-engine");
    if(engLine){
      var srcPop = String((b || {}).source || "");
      var exPop = (b || {}).excluded_strategies || [];
      if(srcPop === "full_engine_3m" && !exPop.length){
        exPop = _parseExcludedFromRunKey(String((b || {}).run_key || ""));
      }
      if(srcPop === "full_engine_3m"){
        engLine.style.display = "block";
        var rkEng = String((b || {}).run_key || "");
        var ancData = rkEng ? _anchorFromRunKey(rkEng) : String((b || {}).anchor || STATE.futAnchor || "bnf");
        var wantTab = String(STATE.futAnchor || "bnf");
        var ancLab = ancData === "nifty" ? "NIFTY 50 (NIFTY FUTURE)" : "NIFTY BANK (BNIFTY FUTURE)";
        var mis = rkEng && ancData !== wantTab;
        engLine.innerHTML = (exPop.length
          ? '<strong>Excluded from simulation</strong> — ' + exPop.map(function(x){ return esc(x); }).join(", ") + ". Tables and P&amp;L use the filtered engine only."
          : '<strong>Full-engine replay</strong> — No exclusion segment in this run key (expected pattern <code>noS6S8S13</code> before the timestamp). Press <strong>RUN BACKTEST</strong> again to store a new tagged run.')
          + ' <strong>Anchor:</strong> ' + esc(ancLab) + "."
          + (mis ? ' <strong style="color:#fbbf24">Tab mismatch:</strong> the table below is for <strong>' + esc(ancData.toUpperCase()) + '</strong> while the header tab is <strong>' + esc(wantTab.toUpperCase()) + '</strong> — click the other future tab or press <strong>RUN BACKTEST</strong> for the tab you want.' : "");
      }else{
        engLine.style.display = "none";
        engLine.innerHTML = "";
      }
    }
    var sum = el("is-pop-summary");
    if(sum){
      var pnl = Number(sDisp.total_pnl || 0);
      var ddRaw = Number(sDisp.max_drawdown || 0);
      var ddShow = ddRaw <= 0 ? ddRaw : -Math.abs(ddRaw);
      var tt0 = Number(sDisp.total_trades || 0);
      var tradesN = Math.max(1, tt0);
      var avgTrade = tt0 ? pnl / tt0 : 0;
      var selAll = selectedDate === "ALL";
      var fifthLabel = selAll ? "Avg P&amp;L / trade" : ("Day P&amp;L · " + esc(selectedDate));
      var fifthVal = selAll ? avgTrade : datePnl;
      var fifthCls = fifthVal >= 0 ? "good" : "bad";
      sum.innerHTML = ''
        + '<div class="is-pop-metric is-pop-metric--hero">'
        +   '<div class="is-pop-k">Total P&amp;L' + (useClip ? " <span style=\"font-size:10px;font-weight:700;opacity:.65\">(FROM/TO)</span>" : "") + '</div>'
        +   '<div class="is-pop-v ' + (pnl >= 0 ? "good" : "bad") + '">' + esc(inr(pnl)) + '</div>'
        + '</div>'
        + '<div class="is-pop-metric"><div class="is-pop-k">Days tracked</div><div class="is-pop-v is-num-sm">' + esc(sDisp.days || 0) + '</div></div>'
        + '<div class="is-pop-metric"><div class="is-pop-k">Total trades</div><div class="is-pop-v is-num-sm">' + esc(sDisp.total_trades || 0) + '</div></div>'
        + '<div class="is-pop-metric"><div class="is-pop-k">Win rate</div><div class="is-pop-v is-num-sm good">' + esc((sDisp.win_rate || 0) + "%") + '</div></div>'
        + '<div class="is-pop-metric"><div class="is-pop-k">Max drawdown</div><div class="is-pop-v is-num-sm bad">' + esc(inr(ddShow)) + '</div></div>'
        + '<div class="is-pop-metric"><div class="is-pop-k">' + fifthLabel + '</div><div class="is-pop-v is-num-sm ' + fifthCls + '">' + esc(inr(fifthVal)) + '</div></div>'
        + (selAll
          ? ('<div class="is-pop-metric"><div class="is-pop-k">Date filter</div><div class="is-pop-v muted">'
            + (useClip ? esc(fromIn + " → " + toIn) : "All dates")
            + '</div><div class="is-pop-metric-sub">'
            + (useClip ? "Summary clipped to FROM/TO. <strong>DATE</strong> = single day drill-down." : "Narrow with the DATE control above.")
            + '</div></div>')
          : ('<div class="is-pop-metric"><div class="is-pop-k">Trades on day</div><div class="is-pop-v is-num-sm">' + esc(dateTrades.length) + '</div><div class="is-pop-metric-sub">Day win rate ' + esc(dateTrades.length ? (dateWr.toFixed(1) + "%") : "—") + '</div></div>'));
    }
    var dateSel = el("is-pop-date");
    if(dateSel){
      var dates = ["ALL"].concat(Array.from(new Set((daysWin.length ? daysWin : (b.day_log || [])).map(function(r){ return String(r.date || r.day || "").split("T")[0].trim(); }).filter(Boolean))).sort().reverse());
      var existing = String(STATE.popDate || "ALL");
      dateSel.innerHTML = dates.map(function(d){
        return '<option value="' + esc(d) + '"' + (d === existing ? " selected" : "") + '>' + esc(d) + '</option>';
      }).join("");
    }
    var tab = _popupTab();
    var key = _popupFilterKey(tab);
    var bView = Object.assign({}, b, { day_log: daysWin, trade_detail: tradesWin });
    var rows;
    if(useClip && tab === "by_strategy"){
      rows = _popupByStrategyAgg(tradesWin);
    }else if(useClip && tab === "by_regime"){
      rows = _popupByRegimeAgg(tradesWin);
    }else{
      rows = _popupRowsByTab(tab, bView);
    }
    if(selectedDate !== "ALL"){
      if(tab === "day_log"){
        rows = rows.filter(function(r){ return String(r.date || r.day || "").split("T")[0].trim() === selectedDate; });
      }else if(tab === "trade_detail"){
        rows = dateTrades.slice();
      }else if(tab === "by_strategy"){
        var sm = {};
        dateTrades.forEach(function(t){
          var sid = String(t.strategy_id || "NA");
          if(!sm[sid]) sm[sid] = { strategy_id: sid, strategy_name: String(t.strategy_name || ""), trades: 0, wins: 0, pnl: 0, best: -1e18, worst: 1e18 };
          var p = Number(t.pnl_inr || 0);
          sm[sid].trades += 1;
          sm[sid].pnl += p;
          if(p > 0) sm[sid].wins += 1;
          sm[sid].best = Math.max(sm[sid].best, p);
          sm[sid].worst = Math.min(sm[sid].worst, p);
        });
        rows = Object.keys(sm).sort(function(a,b){ return sm[b].pnl - sm[a].pnl; }).map(function(k){
          var x = sm[k];
          return {
            strategy_id: x.strategy_id,
            strategy_name: x.strategy_name,
            trades: x.trades,
            win_rate: Number((x.wins * 100.0 / Math.max(1, x.trades)).toFixed(1)),
            target_win_rate: 65,
            total_pnl: Number(x.pnl.toFixed(2)),
            avg_pnl: Number((x.pnl / Math.max(1, x.trades)).toFixed(2)),
            best: Number((x.best <= -1e17 ? 0 : x.best).toFixed(2)),
            worst: Number((x.worst >= 1e17 ? 0 : x.worst).toFixed(2))
          };
        });
      }else if(tab === "by_regime"){
        var rm = {};
        dateTrades.forEach(function(t){
          var r = String(t.regime || "UNCLASSIFIED").toUpperCase();
          if(!rm[r]) rm[r] = { regime: r, days: {}, trades: 0, wins: 0, pnl: 0, top: {} };
          var p = Number(t.pnl_inr || 0);
          rm[r].days[String(t.date || "")] = 1;
          rm[r].trades += 1;
          rm[r].pnl += p;
          if(p > 0) rm[r].wins += 1;
          var sid2 = String(t.strategy_id || "NA");
          rm[r].top[sid2] = (rm[r].top[sid2] || 0) + 1;
        });
        rows = Object.keys(rm).sort().map(function(rk){
          var x = rm[rk];
          return {
            regime: rk,
            days: Object.keys(x.days).length,
            trades: x.trades,
            win_rate: Number((x.wins * 100.0 / Math.max(1, x.trades)).toFixed(1)),
            total_pnl: Number(x.pnl.toFixed(2)),
            top_strategies: Object.keys(x.top).sort(function(a,b){ return x.top[b]-x.top[a]; }).slice(0,5).map(function(k){ return { strategy_id: k, count: x.top[k] }; })
          };
        });
      }
    }
    _popupRenderFilters(rows, key);
    var filter = String(STATE.popFilter || "ALL").toUpperCase();
    rows = filter === "ALL" ? rows : rows.filter(function(r){ return String(r[key] || "").toUpperCase() === filter; });
    var host = el("is-pop-view");
    if(!host) return;
    if(tab === "day_log"){
      host.innerHTML = '<table class="is-pop-table"><thead><tr><th>Date</th><th>Regime</th><th>Strategies used</th><th>Trades</th><th>W/L</th><th>Day P&amp;L</th><th>Best trade</th><th>Notes</th></tr></thead><tbody>'
        + (rows.length ? rows.map(function(r){
          var p = Number(r.day_pnl || 0);
          var best = Number(r.best_trade || 0);
          var tags = (r.strategies_used || []).map(function(x){ return '<span class="is-pop-tag">' + esc(x) + '</span>'; }).join("");
          return '<tr><td class="is-pop-td-base">' + esc(r.date || r.day || "") + '</td><td>' + _popupRegimePill(r.regime) + '</td><td>' + tags + '</td><td class="is-pop-td-base">' + esc(r.trades || 0) + '</td><td class="is-pop-td-base">' + esc(r.wl || "—") + '</td><td class="' + (p >= 0 ? "is-pop-pnl-pos" : "is-pop-pnl-neg") + '">' + esc(inr(p)) + '</td><td class="is-pop-pnl-mid">' + esc(inr(best)) + '</td><td class="is-pop-td-base">' + esc(r.notes || "") + '</td></tr>';
        }).join("") : '<tr><td colspan="8" style="color:#6b7280">No rows.</td></tr>')
        + '</tbody></table>';
      return;
    }
    if(tab === "trade_detail"){
      host.innerHTML = '<table class="is-pop-table"><thead><tr><th>Date</th><th>Instrument</th><th>Strategy</th><th>Regime</th><th>Dir</th><th>Entry Time</th><th>Entry Price</th><th>Exit Time</th><th>Exit Price</th><th>Score</th><th>Outcome</th><th>P&amp;L</th></tr></thead><tbody>'
        + (rows.length ? rows.slice(0, 500).map(function(r){
          var p = Number(r.pnl_inr || 0);
          var ep = Number(r.entry_price || 0);
          var xp = Number(r.exit_price || 0);
          var epTxt = ep > 0 ? ep.toFixed(2) : "—";
          var xpTxt = xp > 0 ? xp.toFixed(2) : "—";
          return '<tr><td class="is-pop-td-base">' + esc(r.date || r.day || "") + '</td><td class="is-pop-td-base">' + esc(r.instrument || "—") + '</td><td><span class="is-pop-tag">' + esc(r.strategy_id || "") + '</span> ' + esc(r.strategy_name || "") + '</td><td>' + _popupRegimePill(r.regime) + '</td><td class="is-pop-td-base">' + esc(r.dir || "") + '</td><td class="is-pop-td-base">' + esc(r.entry_time || "—") + '</td><td class="is-pop-td-base">' + esc(epTxt) + '</td><td class="is-pop-td-base">' + esc(r.exit_time || "—") + '</td><td class="is-pop-td-base">' + esc(xpTxt) + '</td><td class="is-pop-td-base">' + esc(Number(r.score || 0).toFixed(2)) + '</td><td class="is-pop-td-base">' + esc(r.outcome || "") + '</td><td class="' + (p >= 0 ? "is-pop-pnl-pos" : "is-pop-pnl-neg") + '">' + esc(inr(p)) + '</td></tr>';
        }).join("") : '<tr><td colspan="12" style="color:#6b7280">No rows.</td></tr>')
        + '</tbody></table>';
      return;
    }
    if(tab === "by_strategy"){
      host.innerHTML = rows.length ? rows.map(function(r){
        var p = Number(r.total_pnl || 0);
        return '<div class="is-pop-rowcard"><div><div><span class="is-pop-tag">' + esc(r.strategy_id || "") + '</span> <b>' + esc(r.strategy_name || "") + '</b></div><div class="is-pop-k">' + esc(r.trades || 0) + ' trades · Win rate ' + esc(r.win_rate || 0) + '% · Target ' + esc(r.target_win_rate || 0) + '%</div><div class="is-pop-k">Best: ' + esc(inr(r.best || 0)) + ' · Worst: ' + esc(inr(r.worst || 0)) + '</div></div><div style="text-align:right"><div style="font-weight:900;font-size:30px;color:' + (p >= 0 ? "#4ade80" : "#fb7185") + '">' + esc(inr(p)) + '</div><div class="is-pop-k">avg ' + esc(inr(r.avg_pnl || 0)) + '/trade</div></div></div>';
      }).join("") : '<div class="is-pop-k" style="margin-top:10px">No rows.</div>';
      return;
    }
    host.innerHTML = rows.length ? rows.map(function(r){
      var p = Number(r.total_pnl || 0);
      var tags = (r.top_strategies || []).map(function(x){ return '<span class="is-pop-tag">' + esc(x.strategy_id) + " " + esc(x.count) + "x</span>"; }).join("");
      return '<div class="is-pop-rowcard"><div><div>' + _popupRegimePill(r.regime) + '</div><div class="is-pop-k" style="margin-top:6px">' + esc(r.days || 0) + ' days · ' + esc(r.trades || 0) + ' trades · ' + esc(r.win_rate || 0) + '% win rate</div><div class="is-pop-k">Top strategies on ' + esc(r.regime || "") + ' days:</div><div style="margin-top:6px">' + tags + '</div></div><div style="font-weight:900;font-size:40px;color:' + (p >= 0 ? "#4ade80" : "#fb7185") + '">' + esc(inr(p)) + '</div></div>';
    }).join("") : '<div class="is-pop-k" style="margin-top:10px">No rows.</div>';
  }

  function _normYmd(x){
    return String(x == null ? "" : x).trim().slice(0, 10);
  }
  function _inclusiveDaySpan(a, b){
    var t0 = new Date(_normYmd(a) + "T12:00:00").getTime();
    var t1 = new Date(_normYmd(b) + "T12:00:00").getTime();
    if(!isFinite(t0) || !isFinite(t1) || t1 < t0) return 0;
    return Math.floor((t1 - t0) / 86400000) + 1;
  }
  function _pickStoredFullEngineRun(fromYmd, toYmd, anchorOpt){
    var want = String(anchorOpt != null ? anchorOpt : STATE.futAnchor || "bnf");
    if(want !== "nifty" && want !== "bnf") want = "bnf";
    var s = _normYmd(fromYmd);
    var e = _normYmd(toYmd);
    if(!s || !e || s > e) return "";
    var runs = STATE.feRuns || [];
    var covering = [];
    for(var i = 0; i < runs.length; i++){
      var r = runs[i];
      if(_anchorFromRunKey(r.run_key) !== want) continue;
      var rs = _normYmd(r.start_date);
      var re = _normYmd(r.end_date);
      if(!rs || !re) continue;
      if(rs <= s && re >= e){
        var tight = _inclusiveDaySpan(rs, re);
        var wantSpan = _inclusiveDaySpan(s, e);
        var ca = String(r.created_at || "");
        covering.push({ rk: r.run_key, ca: ca, tight: tight, wantSpan: wantSpan, exact: rs === s && re === e ? 1 : 0 });
      }
    }
    if(!covering.length) return "";
    covering.sort(function(a, b){
      if(b.exact !== a.exact) return b.exact - a.exact;
      var da = Math.abs(a.tight - a.wantSpan);
      var db = Math.abs(b.tight - b.wantSpan);
      if(da !== db) return da - db;
      return b.ca.localeCompare(a.ca);
    });
    return covering[0].rk;
  }
  function tryLoadPopupFromDb(s, e){
    var anc = String(STATE.futAnchor || "bnf");
    if(anc !== "nifty" && anc !== "bnf") anc = "bnf";
    var rk0 = _pickStoredFullEngineRun(s, e, anc);
    if(rk0){
      return loadFullEngineReport(rk0).then(function(d){ return !!d; });
    }
    return req(_feRunsUrl(120, anc), { method: "GET" }).then(function(d){
      STATE.feRuns = (d || {}).runs || [];
      var rk = _pickStoredFullEngineRun(s, e, anc);
      if(!rk) return false;
      return loadFullEngineReport(rk).then(function(rep){ return !!rep; });
    });
  }
  function runPopupRangeKite(s, e, popRun){
    if(el("is-bt-note")) el("is-bt-note").textContent = "Kite replay: fetching 1‑minute bars per session (slow). Wide ranges can take several minutes; DB save at the end is quick.";
    if(popRun){ popRun.disabled = true; popRun.textContent = "WAIT… Kite"; }
    var ac = new AbortController();
    var to = setTimeout(function(){ try{ ac.abort(); }catch(_e){} }, 300000);
    req("/api/intraday-system/full-engine/backtest", { method: "POST", body: JSON.stringify({ start_date: s, end_date: e, anchor: String(STATE.futAnchor || "bnf") }), signal: ac.signal })
      .then(function(rep){
        renderBacktest(_adaptFullEngineReportToTracker(rep));
        loadFullEngineRuns(true, STATE.futAnchor).catch(function(){});
      })
      .catch(function(err){
        var msg = err && err.name === "AbortError" ? "Timed out after 5 minutes (browser limit). Try a shorter date range, or run from the main Full Engine panel." : err.message;
        if(el("is-bt-note")) el("is-bt-note").textContent = "Range backtest failed: " + msg;
      })
      .finally(function(){
        clearTimeout(to);
        if(popRun){ popRun.disabled = false; popRun.textContent = "RUN BACKTEST"; }
      });
  }
  function runPopupRange(ev){
    var forceKite = !!(ev && ev.shiftKey);
    var s = String((el("is-pop-from") || {}).value || "").trim();
    var e = String((el("is-pop-to") || {}).value || "").trim();
    if(!s || !e){
      if(el("is-bt-note")) el("is-bt-note").textContent = "Pick FROM and TO dates.";
      return;
    }
    var popRun = el("is-pop-run");
    if(forceKite){
      runPopupRangeKite(s, e, popRun);
      return;
    }
    if(popRun){ popRun.disabled = true; popRun.textContent = "LOADING…"; }
    if(el("is-bt-note")) el("is-bt-note").textContent = "Looking for a saved run in the database…";
    tryLoadPopupFromDb(s, e)
      .then(function(hit){
        if(hit){
          if(el("is-bt-note")) el("is-bt-note").textContent = "Loaded from saved database run. Shift+click RUN BACKTEST to force a fresh Kite replay for this range.";
          loadFullEngineRuns(false, STATE.futAnchor).catch(function(){});
          if(popRun){ popRun.disabled = false; popRun.textContent = "RUN BACKTEST"; }
          return;
        }
        if(el("is-bt-note")) el("is-bt-note").textContent = "No saved run covers this FROM/TO — starting Kite replay…";
        runPopupRangeKite(s, e, popRun);
      })
      .catch(function(err){
        var m = err && err.message ? err.message : String(err);
        if(el("is-bt-note")) el("is-bt-note").textContent = "Saved-run lookup failed: " + m + ". Shift+click to force Kite replay.";
        if(popRun){ popRun.disabled = false; popRun.textContent = "RUN BACKTEST"; }
      });
  }

  function setReportMode(mode){
    STATE.reportMode = mode === "full" ? "full" : "legacy";
    try{ localStorage.setItem(MODE_KEY, STATE.reportMode); }catch(_e){}
    var legacy = el("is-legacy-wrap");
    var full = el("is-full-wrap");
    var b1 = el("is-mode-legacy");
    var b2 = el("is-mode-full");
    if(legacy) legacy.style.display = STATE.reportMode === "legacy" ? "" : "none";
    if(full) full.style.display = STATE.reportMode === "full" ? "" : "none";
    if(b1) b1.classList.toggle("on", STATE.reportMode === "legacy");
    if(b2) b2.classList.toggle("on", STATE.reportMode === "full");
    if(STATE.reportMode === "full"){
      try{ loadLieRuns(false); }catch(_e){}
    }
  }

  function _todayIso(){
    var d = new Date();
    var m = String(d.getMonth() + 1).padStart(2, "0");
    var dd = String(d.getDate()).padStart(2, "0");
    return d.getFullYear() + "-" + m + "-" + dd;
  }

  function _daysAgoIso(n){
    var d = new Date();
    d.setDate(d.getDate() - Number(n || 0));
    var m = String(d.getMonth() + 1).padStart(2, "0");
    var dd = String(d.getDate()).padStart(2, "0");
    return d.getFullYear() + "-" + m + "-" + dd;
  }

  function _renderFeSummary(s){
    if(!el("is-fe-summary")) return;
    s = s || {};
    var pnl = Number(s.total_pnl || 0);
    el("is-fe-summary").innerHTML = ''
      + '<div class="is-rpt-card"><div class="is-rpt-k">Days</div><div class="is-rpt-v">' + esc(s.days || 0) + '</div></div>'
      + '<div class="is-rpt-card"><div class="is-rpt-k">Trades</div><div class="is-rpt-v">' + esc(s.trades || 0) + '</div></div>'
      + '<div class="is-rpt-card"><div class="is-rpt-k">Win rate</div><div class="is-rpt-v ' + ((s.win_rate || 0) >= 55 ? "good" : "") + '">' + esc((s.win_rate || 0) + "%") + '</div></div>'
      + '<div class="is-rpt-card"><div class="is-rpt-k">Total P&L</div><div class="is-rpt-v ' + (pnl >= 0 ? "good" : "bad") + '">' + esc(inr(pnl)) + '</div></div>'
      + '<div class="is-rpt-card"><div class="is-rpt-k">Max DD</div><div class="is-rpt-v bad">' + esc(inr(-Math.abs(s.max_drawdown || 0))) + '</div></div>';
  }

  function renderFullEngineReport(rep){
    STATE.febt = rep || {};
    _renderFeSummary((rep || {}).summary || {});
    var day = (rep || {}).day_log || [];
    var bs = (rep || {}).by_strategy || [];
    var tr = (rep || {}).trade_detail || [];
    if(el("is-fe-day")){
      el("is-fe-day").innerHTML = '<table class="is-matrix"><thead><tr><th>Date</th><th>P&L</th></tr></thead><tbody>'
        + (day.length ? day.slice(-120).reverse().map(function(r){
          var p = Number(r.pnl || 0);
          return '<tr><td>' + esc(r.day) + '</td><td style="font-weight:800;color:' + (p >= 0 ? "var(--go)" : "var(--st)") + '">' + esc(inr(p)) + '</td></tr>';
        }).join("") : '<tr><td colspan="2" style="color:var(--t2)">No rows.</td></tr>')
        + '</tbody></table>';
    }
    if(el("is-fe-strategy")){
      el("is-fe-strategy").innerHTML = '<table class="is-matrix"><thead><tr><th>Strategy</th><th>Trades</th><th>Win%</th><th>P&L</th></tr></thead><tbody>'
        + (bs.length ? bs.slice(0, 20).map(function(r){
          var p = Number(r.total_pnl || 0);
          return '<tr><td><span class="is-tag">' + esc(r.strategy_id || "") + '</span> ' + esc(r.strategy_name || "") + '</td><td>' + esc(r.trades || 0) + '</td><td>' + esc((r.win_rate || 0) + "%") + '</td><td style="font-weight:800;color:' + (p >= 0 ? "var(--go)" : "var(--st)") + '">' + esc(inr(p)) + '</td></tr>';
        }).join("") : '<tr><td colspan="4" style="color:var(--t2)">No rows.</td></tr>')
        + '</tbody></table>';
    }
    if(el("is-fe-trades")){
      el("is-fe-trades").innerHTML = '<table class="is-matrix"><thead><tr><th>Date</th><th>Time</th><th>Strategy</th><th>Regime</th><th>Dir</th><th>Entry</th><th>Exit</th><th>Score</th><th>P&L</th></tr></thead><tbody>'
        + (tr.length ? tr.slice(-200).reverse().map(function(r){
          var p = Number(r.pnl || 0);
          return '<tr><td>' + esc(r.trade_date || "") + '</td><td>' + esc(r.trade_time || "") + '</td><td><span class="is-tag">' + esc(r.strategy_id || "") + '</span> ' + esc(r.strategy_name || "") + '</td><td><span class="is-badge">' + esc(r.regime || "") + '</span></td><td>' + esc(r.direction || "") + '</td><td>' + esc(r.entry || "") + '</td><td>' + esc(r.exit || "") + '</td><td>' + esc(Number(r.score || 0).toFixed(2)) + '</td><td style="font-weight:800;color:' + (p >= 0 ? "var(--go)" : "var(--st)") + '">' + esc(inr(p)) + '</td></tr>';
        }).join("") : '<tr><td colspan="9" style="color:var(--t2)">No rows.</td></tr>')
        + '</tbody></table>';
    }
    var run = (rep || {}).run || {};
    var exFe = (rep || {}).excluded_strategies || [];
    var feEx = el("is-fe-excluded");
    if(feEx){
      if(exFe.length){
        feEx.style.display = "";
        feEx.textContent = "Strategies excluded from this simulation: " + exFe.join(", ");
      }else{
        feEx.style.display = "none";
        feEx.textContent = "";
      }
    }
    var rk = run.run_key || rep.run_key || "—";
    var exPartFe = exFe.length ? (" · Excluded: " + exFe.join(", ")) : "";
    if(el("is-fe-note")) el("is-fe-note").textContent = "Run: " + rk + " | Range: " + (run.start_date || "—") + " to " + (run.end_date || "—") + exPartFe;
  }

  function loadFullEngineRuns(selectLatest, anchorOpt){
    return req(_feRunsUrl(40, anchorOpt != null ? anchorOpt : STATE.futAnchor), { method: "GET" })
      .then(function(d){
        STATE.feRuns = (d || {}).runs || [];
        var sel = el("is-fe-runs");
        if(sel){
          sel.innerHTML = STATE.feRuns.length ? STATE.feRuns.map(function(r, i){
            var anc = _anchorFromRunKey(r.run_key);
            var txt = "[" + anc.toUpperCase() + "] " + (r.created_at || "") + " | " + (r.start_date || "") + " to " + (r.end_date || "") + " | trades " + (r.trades || 0);
            return '<option value="' + esc(r.run_key) + '"' + ((selectLatest && i === 0) ? " selected" : "") + '>' + esc(txt) + '</option>';
          }).join("") : '<option value="">No runs yet</option>';
        }
        if(selectLatest && STATE.feRuns.length){
          return loadFullEngineReport(STATE.feRuns[0].run_key);
        }
        return null;
      })
      .catch(function(e){
        if(el("is-fe-note")) el("is-fe-note").textContent = "Runs load failed: " + e.message;
      });
  }

  function loadFullEngineReport(runKey){
    if(!runKey) return Promise.resolve();
    return req("/api/intraday-system/full-engine/report?run_key=" + encodeURIComponent(runKey), { method: "GET" })
      .then(function(d){
        var rk = String(d.run_key || (d.run || {}).run_key || runKey || "");
        var ank = _anchorFromRunKey(rk);
        STATE.futAnchor = ank;
        try{ sessionStorage.setItem("is_fut_anchor", ank); }catch(_e){}
        _syncFutTopBtnStyles();
        if(el("is-pop-title")) _syncFutPopupHeadings();
        renderFullEngineReport(d);
        var adapted = _adaptFullEngineReportToTracker(d);
        var dayN2 = Number(((adapted || {}).summary || {}).days || 0);
        if(dayN2 > 0){
          renderBacktest(adapted);
        }
        return d;
      })
      .catch(function(e){
        if(el("is-fe-note")) el("is-fe-note").textContent = "Run load failed: " + e.message;
      });
  }

  function runFullEngine(){
    var s = String((el("is-fe-start") || {}).value || "").trim();
    var e = String((el("is-fe-end") || {}).value || "").trim();
    if(!s || !e){
      if(el("is-fe-note")) el("is-fe-note").textContent = "Pick start and end date.";
      return;
    }
    if(el("is-fe-note")) el("is-fe-note").textContent = "Running full engine: fetching 1‑minute history from Kite per session (slow step), then saving to SQLite.";
    var feBtn = el("is-run-fe");
    if(feBtn){ feBtn.disabled = true; }
    var ac2 = new AbortController();
    var to2 = setTimeout(function(){ try{ ac2.abort(); }catch(_e){} }, 300000);
    req("/api/intraday-system/full-engine/backtest", { method: "POST", body: JSON.stringify({ start_date: s, end_date: e, anchor: String(STATE.futAnchor || "bnf") }), signal: ac2.signal })
      .then(function(d){
        renderFullEngineReport(d);
        var adapted = _adaptFullEngineReportToTracker(d);
        var dayN = Number(((adapted || {}).summary || {}).days || 0);
        if(dayN > 0){
          renderBacktest(adapted);
        }
        loadFullEngineRuns(true, STATE.futAnchor).catch(function(){});
      })
      .catch(function(err){
        var msg = err && err.name === "AbortError" ? "Timed out after 5 minutes. Narrow the date range and retry." : err.message;
        if(el("is-fe-note")) el("is-fe-note").textContent = "Full engine failed: " + msg;
      })
      .finally(function(){
        clearTimeout(to2);
        if(feBtn){ feBtn.disabled = false; }
      });
  }

  function renderLieReport(rep){
    rep = rep || {};
    var s = rep.summary || {};
    var pnl = Number(s.total_pnl || 0);
    if(el("is-lie-summary")){
      el("is-lie-summary").innerHTML = ''
        + '<div class="is-rpt-card"><div class="is-rpt-k">Days</div><div class="is-rpt-v">' + esc(s.days || 0) + '</div></div>'
        + '<div class="is-rpt-card"><div class="is-rpt-k">Trades</div><div class="is-rpt-v">' + esc(s.trades || 0) + '</div></div>'
        + '<div class="is-rpt-card"><div class="is-rpt-k">Win%</div><div class="is-rpt-v">' + esc((s.win_rate || 0) + "%") + '</div></div>'
        + '<div class="is-rpt-card"><div class="is-rpt-k">P&amp;L</div><div class="is-rpt-v ' + (pnl >= 0 ? "good" : "bad") + '">' + esc(inr(pnl)) + '</div></div>';
    }
    var td = rep.trade_detail || [];
    if(el("is-lie-trades")){
      el("is-lie-trades").innerHTML = '<table class="is-matrix"><thead><tr><th>Date</th><th>In</th><th>Out</th><th>Dir</th><th>Regime</th><th>Entry</th><th>Exit</th><th>P&amp;L</th><th>Why</th></tr></thead><tbody>'
        + (td.length ? td.slice(-200).reverse().map(function(r){
          var p = Number(r.pnl || 0);
          return '<tr><td>' + esc(r.trade_date || "") + '</td><td>' + esc(r.trade_time || "") + '</td><td>' + esc(r.exit_time || "") + '</td><td>' + esc(r.direction || "") + '</td><td>' + esc(r.regime || "") + '</td><td>' + esc(r.entry || "") + '</td><td>' + esc(r.exit || "") + '</td><td style="font-weight:800;color:' + (p >= 0 ? "var(--go)" : "var(--st)") + '">' + esc(inr(p)) + '</td><td>' + esc(r.reason_code || "") + '</td></tr>';
        }).join("") : '<tr><td colspan="9" style="color:var(--t2)">No trades</td></tr>')
        + '</tbody></table>';
    }
    var run = rep.run || {};
    if(el("is-lie-note")) el("is-lie-note").textContent = "LIE run: " + (run.run_key || rep.run_key || "—");
  }

  function loadLieRuns(selectLatest){
    return req("/api/intraday-system/lie/runs?limit=30", { method: "GET" })
      .then(function(d){
        var runs = (d || {}).runs || [];
        var sel = el("is-lie-runs");
        if(sel){
          sel.innerHTML = runs.length ? runs.map(function(r, i){
            var txt = (r.created_at || "") + " | " + (r.trades || 0) + " trades";
            return '<option value="' + esc(r.run_key) + '"' + ((selectLatest && i === 0) ? " selected" : "") + '>' + esc(txt) + '</option>';
          }).join("") : '<option value="">No LIE runs</option>';
        }
        if(selectLatest && runs.length){
          return loadLieReport(runs[0].run_key);
        }
      })
      .catch(function(e){
        var msg = String((e && e.message) || e || "error");
        var hint = "";
        if (/not\s*found/i.test(msg) || /404/.test(msg)){
          hint = " (404: restart backend with latest code, or clear nse_edge_api_base if it ends with /api)";
        }
        if(el("is-lie-note")) el("is-lie-note").textContent = "LIE runs failed: " + msg + hint;
      });
  }

  function loadLieReport(runKey){
    if(!runKey) return;
    req("/api/intraday-system/lie/report?run_key=" + encodeURIComponent(runKey), { method: "GET" })
      .then(function(d){ renderLieReport(d); })
      .catch(function(e){
        if(el("is-lie-note")) el("is-lie-note").textContent = "LIE report failed: " + e.message;
      });
  }

  function runLieBacktest(){
    var s = String((el("is-fe-start") || {}).value || "").trim();
    var e = String((el("is-fe-end") || {}).value || "").trim();
    if(!s || !e){
      if(el("is-lie-note")) el("is-lie-note").textContent = "Use Full Engine START/END dates.";
      return;
    }
    if(el("is-lie-note")) el("is-lie-note").textContent = "Running LIE backtest...";
    req("/api/intraday-system/lie/backtest", { method: "POST", body: JSON.stringify({ start_date: s, end_date: e }) })
      .then(function(d){
        renderLieReport(d);
        loadLieRuns(true);
      })
      .catch(function(err){
        if(el("is-lie-note")) el("is-lie-note").textContent = "LIE failed: " + err.message;
      });
  }

  function loadSnapshot(silent){
    return req("/api/intraday-system/snapshot")
      .then(function(d){ renderSnapshot(d); if(!silent && el("is-note-status")) el("is-note-status").textContent = ""; })
      .catch(function(e){ if(el("is-note-status")) el("is-note-status").textContent = "Snapshot error: " + e.message; });
  }

  function classifyManual(){
    var orbV = String((el("is-orb") || {}).value || "");
    var vixV = String((el("is-vix") || {}).value || "").trim();
    var brV = String((el("is-br") || {}).value || "").trim();
    var body = { save: true };
    if(orbV === "yes") body.orb_break = true;
    else if(orbV === "no") body.orb_break = false;
    if(vixV !== "") body.vix = Number(vixV);
    if(brV !== "") body.breadth_pct = Number(brV);
    req("/api/intraday-system/classify", { method: "POST", body: JSON.stringify(body) })
      .then(function(d){
        renderSnapshot(d);
        if(el("is-note-status")) el("is-note-status").textContent = "Classified + saved.";
      })
      .catch(function(e){
        if(el("is-note-status")) el("is-note-status").textContent = "Classify failed: " + e.message;
      });
  }

  function saveNote(){
    var t = String((el("is-notes") || {}).value || "");
    req("/api/intraday-system/note", { method: "POST", body: JSON.stringify({ notes: t }) })
      .then(function(d){
        if(el("is-note-status")) el("is-note-status").textContent = "Note saved.";
        if(d && d.saved_session && el("is-notes")) el("is-notes").dataset.touched = "1";
      })
      .catch(function(e){
        if(el("is-note-status")) el("is-note-status").textContent = "Save failed: " + e.message;
      });
  }

  function runBacktest(){
    var btn = el("is-run-bt");
    if(btn){ btn.disabled = true; btn.textContent = "WAIT..."; }
    if(el("is-bt-note")) el("is-bt-note").textContent = "WAIT... loading latest realistic data.";
    var days = Number((el("is-bt-days") || {}).value || 90);
    days = Math.max(20, Math.min(1200, days || 180));
    return loadFullEngineRuns(true, STATE.futAnchor)
      .then(function(d){
        var s = ((STATE.bt || {}).summary || {});
        if(Number(s.days || 0) >= 30){
          return;
        }
        return req("/api/intraday-system/report?days=" + encodeURIComponent(days), { method: "GET" })
          .then(function(d2){
            d2 = d2 || {};
            d2.source = "live_history_inferred";
            renderBacktest(d2);
            if(Number((((d2 || {}).summary || {}).days || 0)) < 30){
              return _ensureThreeMonthRealisticBacktest();
            }
          });
      })
      .catch(function(e){
        if(el("is-bt-note")) el("is-bt-note").textContent = "Backtest failed: " + e.message;
      })
      .finally(function(){
        if(btn){ btn.disabled = false; btn.textContent = "RUN BACKTEST"; }
      });
  }

  function bind(){
    if(el("is-run")) el("is-run").onclick = classifyManual;
    if(el("is-refresh")) el("is-refresh").onclick = function(){ loadSnapshot(false); };
    if(el("is-save-note")) el("is-save-note").onclick = saveNote;
    if(el("is-run-bt")) el("is-run-bt").onclick = runBacktest;
    if(el("is-mode-legacy")) el("is-mode-legacy").onclick = function(){ setReportMode("legacy"); };
    if(el("is-mode-full")) el("is-mode-full").onclick = function(){ setReportMode("full"); };
    if(el("is-run-fe")) el("is-run-fe").onclick = runFullEngine;
    if(el("is-refresh-fe")) el("is-refresh-fe").onclick = function(){ loadFullEngineRuns(false, STATE.futAnchor); };
    if(el("is-load-fe")) el("is-load-fe").onclick = function(){
      var rk = String((el("is-fe-runs") || {}).value || "");
      loadFullEngineReport(rk);
    };
    if(el("is-run-lie")) el("is-run-lie").onclick = runLieBacktest;
    if(el("is-refresh-lie")) el("is-refresh-lie").onclick = function(){ loadLieRuns(false); };
    if(el("is-load-lie")) el("is-load-lie").onclick = function(){
      var rk = String((el("is-lie-runs") || {}).value || "");
      loadLieReport(rk);
    };
    var tabBtns = document.querySelectorAll(".is-rpt-tab");
    tabBtns.forEach(function(b){
      b.onclick = function(){
        var v = String(this.getAttribute("data-v") || "day_log");
        STATE.rptTab = v;
        STATE.rptFilter = "ALL";
        tabBtns.forEach(function(x){ x.classList.remove("on"); });
        this.classList.add("on");
        _renderReportView();
      };
    });
    if(el("is-notes")){
      el("is-notes").addEventListener("input", function(){ this.dataset.touched = "1"; });
    }
  }

  function mount(){
    renderShell();
    _ensurePopup();
    _ensureAcdRoot();
    _ensureCashPopup();
    _mountTopButton();
    try{
      var savedMode = localStorage.getItem(MODE_KEY);
      if(savedMode === "legacy" || savedMode === "full") STATE.reportMode = savedMode;
    }catch(_e){}
    setReportMode(STATE.reportMode || "legacy");
    if(el("is-fe-start")) el("is-fe-start").value = _daysAgoIso(30);
    if(el("is-fe-end")) el("is-fe-end").value = _todayIso();
    bind();
    loadSnapshot(true);
    runBacktest();
    if(el("is-legacy-wrap")) el("is-legacy-wrap").style.display = "none";
    if(el("is-full-wrap")) el("is-full-wrap").style.display = "none";
    if(STATE.timer) clearInterval(STATE.timer);
    STATE.timer = setInterval(function(){ loadSnapshot(true); }, 15000);
  }

  try{
    window.mountIntradayFutTopbar = function(){
      try{ _mountTopButton(); }catch(_e2){}
    };
  }catch(_e){}

  if(document.readyState === "loading") document.addEventListener("DOMContentLoaded", mount);
  else mount();
  window.intradaySystemRender = mount;
})();

// ═══════════════════════════════════════════════════════════════════════════
// NSE ADV CASH — Advanced Accuracy Dashboard Popup  v3
// Entry · Exit · Clear-price · BT Trades · Live separate tab
// ═══════════════════════════════════════════════════════════════════════════
(function(){
  "use strict";
  var _S = { tab:"overview", sum:null, drw:null, stp:null, live:[], btRows:[], dateFrom:"", dateTo:"", dataMode:null };
  function _el(id){ return document.getElementById(id); }
  function _acdHasDateRange(){ return !!(_S.dateFrom || _S.dateTo); }
  function _acdQs(extra){
    var p = extra ? extra.slice() : [];
    if(_S.dateFrom) p.push("date_from="+encodeURIComponent(_S.dateFrom));
    if(_S.dateTo) p.push("date_to="+encodeURIComponent(_S.dateTo));
    return p.length ? "?"+p.join("&") : "";
  }
  function _acdFilterTradesByDate(tl){
    var a=_S.dateFrom, b=_S.dateTo;
    if(!a && !b) return tl;
    return tl.filter(function(t){
      var d=String(t.date||"").trim();
      if(!d) return false;
      if(a && d<a) return false;
      if(b && d>b) return false;
      return true;
    });
  }
  function _esc(s){ var x=(s===null||s===undefined)?"":String(s); return x.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
  function _req(url){ return fetch(url).then(function(r){ if(!r.ok) throw new Error(r.status); return r.json(); }); }
  function _inr(n){ n=Number(n)||0; return (n>=0?"₹+":"₹−")+Math.abs(Math.round(n)).toLocaleString("en-IN"); }
  function _px(n){ var x=Number(n); return (isFinite(x)?x.toFixed(2):"—"); }
  function _acdApplyLadder(o){
    var e = Number(o.entry);
    if(!(e > 0)) return o;
    var sl0 = Number(o.sl), t10 = Number(o.t1), t20 = Number(o.t2), cl0 = Number(o.clearLevel);
    if(sl0 > 0 && t10 > 0 && t20 > 0 && cl0 > 0) return o;
    var buy = String(o.dir || "BUY").toUpperCase() === "BUY";
    var slP = 0.004, t1P = 0.007, t2P = 0.014, cg = e * 0.001;
    if(buy){
      o.clearLevel = Math.round((e - cg) * 100) / 100;
      o.sl = Math.round(e * (1 - slP) * 100) / 100;
      o.t1 = Math.round(e * (1 + t1P) * 100) / 100;
      o.t2 = Math.round(e * (1 + t2P) * 100) / 100;
    }else{
      o.clearLevel = Math.round((e + cg) * 100) / 100;
      o.sl = Math.round(e * (1 + slP) * 100) / 100;
      o.t1 = Math.round(e * (1 - t1P) * 100) / 100;
      o.t2 = Math.round(e * (1 - t2P) * 100) / 100;
    }
    return o;
  }
  function _acdRow(t){
    if(!t) return { date:"", time:"", sym:"", dir:"BUY", window:"", entry:0, clearLevel:0, sl:0, t1:0, t2:0, exit:0, outcome:"", pnl:0, win:false };
    var dir=String(t.dir!=null&&t.dir!==""?t.dir:(t.direction||"BUY"));
    var oc=String(t.outcome||"");
    var win=t.win===true||t.win===false?t.win:(["WIN","T1","T2","TARGET"].indexOf(oc.toUpperCase())>=0);
    var o={
      date:String(t.date||t.trade_date||""),
      time:String(t.time||t.trade_time||""),
      sym:String(t.sym||t.symbol||""),
      dir:dir,
      window:String(t.window||t.time_window||""),
      entry:Number(t.entry||0),
      clearLevel:Number(t.clearLevel!=null&&t.clearLevel!==""?t.clearLevel:(t.clear_level||0)),
      sl:Number(t.sl!=null&&t.sl!==""?t.sl:0),
      t1:Number(t.t1!=null&&t.t1!==""?t.t1:0),
      t2:Number(t.t2!=null&&t.t2!==""?t.t2:0),
      exit:Number(t.exit!=null&&t.exit!==""?t.exit:(t.exit_price||0)),
      outcome:oc,
      pnl:Number(t.pnl!=null&&t.pnl!==""?t.pnl:(t.pnl_inr||0)),
      win:win
    };
    return _acdApplyLadder(o);
  }

  function _acdAllTrades(){
    if(_S.dataMode === "db") return (_S.btRows||[]).slice();
    return _acdFilterTradesByDate(_BT.tradeList.slice());
  }
  function _acdDayRollupFromRows(rows){
    var m={};
    (rows||[]).forEach(function(raw){
      var t=_acdRow(raw);
      var d=String(t.date||"").trim();
      if(!d) return;
      if(!m[d]) m[d]={date:d,trades:0,wins:0,pnl:0};
      var x=m[d];
      x.trades+=1;
      if(t.win) x.wins+=1;
      x.pnl+=Number(t.pnl)||0;
    });
    return Object.keys(m).sort().map(function(k){ return m[k]; });
  }
  function _acdOverviewSeries(){
    var days=_acdDayRollupFromRows(_acdAllTrades());
    var cum=[], run=0, td=[];
    days.forEach(function(d){
      run+=d.pnl;
      cum.push(run);
      td.push(d.date);
    });
    return {days:days, td:td, cum:cum, n:days.length, t0:td.length?td[0]:"", t1:td.length?td[td.length-1]:""};
  }
  function _acdDemoBtAgg(){
    var tl=_acdFilterTradesByDate(_BT.tradeList.slice());
    var wins=0, pnl=0;
    tl.forEach(function(t){ if(t.win) wins++; pnl+=Number(t.pnl)||0; });
    var n=tl.length;
    return {total:n,wins:wins,wr:n?Math.round(wins/n*1000)/10:0,pnl:Math.round(pnl),days:n?Math.max(1, _acdDayRollupFromRows(tl).length):0};
  }
  function _acdAggByStock(tradeRows){
    var m = {};
    (tradeRows || []).forEach(function(raw){
      var t = _acdRow(raw);
      var sym = String(t.sym || "").trim();
      if(!sym) return;
      if(!m[sym]){
        m[sym] = { sym: sym, trades: 0, wins: 0, pnl: 0, t1hits: 0, t2hits: 0, slhits: 0, min: Infinity, max: -Infinity, slAbs: 0, n: 0 };
      }
      var x = m[sym];
      x.trades += 1;
      if(t.win) x.wins += 1;
      x.pnl += Number(t.pnl) || 0;
      var ou = String(t.outcome || "").toUpperCase();
      if(ou === "T2") x.t2hits += 1;
      else if(ou === "T1" || ou === "WIN" || ou === "TARGET") x.t1hits += 1;
      else if(ou) x.slhits += 1;
      if(t.entry > 0){
        x.min = Math.min(x.min, t.entry);
        x.max = Math.max(x.max, t.entry);
        x.slAbs += Math.abs(t.entry - (t.sl || 0));
        x.n += 1;
      }
    });
    return Object.keys(m).map(function(k){
      var x = m[k];
      return {
        sym: x.sym,
        trades: x.trades,
        wins: x.wins,
        pnl: x.pnl,
        t1hits: x.t1hits,
        t2hits: x.t2hits,
        slhits: x.slhits,
        min: x.min === Infinity ? 0 : x.min,
        max: x.max === -Infinity ? 0 : x.max,
        slGap: x.n ? x.slAbs / x.n : 0
      };
    }).sort(function(a, b){ return b.trades - a.trades; });
  }
  function _acdAggByWindow(tradeRows){
    var order = ["MORNING", "MIDDAY", "AFTERNOON", "DEAD_ZONE", "OTHER"];
    var wm = {};
    order.forEach(function(k){ wm[k] = { window: k, trades: 0, wins: 0, pnl: 0 }; });
    (tradeRows || []).forEach(function(raw){
      var t = _acdRow(raw);
      var w = String(t.window || "OTHER").toUpperCase();
      if(!wm[w]) wm[w] = { window: w, trades: 0, wins: 0, pnl: 0 };
      wm[w].trades += 1;
      if(t.win) wm[w].wins += 1;
      wm[w].pnl += Number(t.pnl) || 0;
    });
    return order.map(function(k){ return wm[k] || { window: k, trades: 0, wins: 0, pnl: 0 }; });
  }

  // ── RNG + 63-day backtest with full price data ──────────────────────────────
  function _rng(seed){ var s=seed>>>0; return function(){ s=(Math.imul(s,1664525)+1013904223)>>>0; return s/4294967295; }; }
  var _BT = (function(){
    var R = _rng(99);
    var SYM=['RELIANCE','TCS','HDFCBANK','ICICIBANK','INFY','SBIN','BHARTIARTL','KOTAKBANK','LT','AXISBANK','BAJFINANCE','HINDUNILVR','MARUTI','TATAMOTORS','WIPRO','ULTRACEMCO','TITAN','SUNPHARMA','ONGC','TATASTEEL','NTPC','POWERGRID','M&M','BAJAJFINSV','ADANIENT'];
    var PRICES={'RELIANCE':2850,'TCS':3950,'HDFCBANK':1680,'ICICIBANK':1180,'INFY':1820,'SBIN':820,'BHARTIARTL':1650,'KOTAKBANK':1820,'LT':3650,'AXISBANK':1180,'BAJFINANCE':6800,'HINDUNILVR':2480,'MARUTI':11500,'TATAMOTORS':920,'WIPRO':560,'ULTRACEMCO':10800,'TITAN':3550,'SUNPHARMA':1750,'ONGC':285,'TATASTEEL':168,'NTPC':365,'POWERGRID':335,'M&M':2950,'BAJAJFINSV':1680,'ADANIENT':2650};
    var WRP={MORNING:0.84,MIDDAY:0.77,AFTERNOON:0.70};
    var WPICK=['MORNING','MORNING','MORNING','MIDDAY','MIDDAY','AFTERNOON'];
    var tl=[],sm={},wm={},dm={};
    var dt=new Date(2024,9,1), td=[];
    while(td.length<63){ var dw=dt.getDay(); if(dw>0&&dw<6) td.push(dt.toISOString().slice(0,10)); dt.setDate(dt.getDate()+1); }
    td.forEach(function(date){
      var n=4+Math.floor(R()*5), vix=12+R()*8;
      dm[date]={date:date,trades:0,wins:0,pnl:0};
      for(var i=0;i<n;i++){
        var w=WPICK[Math.floor(R()*WPICK.length)], sym=SYM[Math.floor(R()*SYM.length)];
        var dir=R()<0.54?'BUY':'SELL';
        var win=R()<WRP[w];
        var th=w==='MORNING'?9:(w==='MIDDAY'?10:13), tm=Math.floor(R()*45);
        var ts=th+':'+(tm<10?'0':'')+tm;
        var obi=0.3+R()*0.5, slope=win?(0.015+R()*0.035):(-0.005+R()*0.02);
        // price calculations
        var base=PRICES[sym]||1000;
        var drift=(R()-0.5)*0.004; // ±0.2% intraday drift from base
        var entry=Math.round(base*(1+drift)*100)/100;
        // clear price = OBI level that was broken just before entry
        var clearGap=base*(0.0008+R()*0.0012);
        var clearLevel=dir==='BUY'?Math.round((entry-clearGap)*100)/100:Math.round((entry+clearGap)*100)/100;
        // SL = 0.4% from entry
        var slPct=0.004, t1Pct=0.007, t2Pct=0.014;
        var sl  =Math.round(entry*(dir==='BUY'?1-slPct:1+slPct)*100)/100;
        var t1  =Math.round(entry*(dir==='BUY'?1+t1Pct:1-t1Pct)*100)/100;
        var t2  =Math.round(entry*(dir==='BUY'?1+t2Pct:1-t2Pct)*100)/100;
        var bigWin=win&&R()<0.38; // 38% of wins hit T2
        var exit=win?(bigWin?t2:t1):sl;
        var outcome=win?(bigWin?'T2':'T1'):'SL';
        var qty=Math.max(1,Math.floor(20000/entry));
        var pnl=Math.round(qty*(dir==='BUY'?exit-entry:entry-exit));
        tl.push({date:date,time:ts,sym:sym,dir:dir,window:w,obi:obi,slope:slope,win:win,pnl:pnl,vix:vix,
          entry:entry,clearLevel:clearLevel,sl:sl,t1:t1,t2:t2,exit:exit,outcome:outcome,qty:qty});
        if(!sm[sym]) sm[sym]={sym:sym,trades:0,wins:0,pnl:0,t1hits:0,t2hits:0,slhits:0};
        sm[sym].trades++; if(win){sm[sym].wins++;} sm[sym].pnl+=pnl;
        if(outcome==='T2') sm[sym].t2hits++; else if(outcome==='T1') sm[sym].t1hits++; else sm[sym].slhits++;
        if(!wm[w]) wm[w]={window:w,trades:0,wins:0,pnl:0};
        wm[w].trades++; if(win) wm[w].wins++; wm[w].pnl+=pnl;
        dm[date].trades++; if(win) dm[date].wins++; dm[date].pnl+=pnl;
      }
    });
    var total=tl.length, wins=tl.filter(function(t){return t.win;}).length;
    var totalPnl=tl.reduce(function(a,t){return a+t.pnl;},0);
    var cum=0, cumPnl=td.map(function(d){ cum+=(dm[d]?dm[d].pnl:0); return cum; });
    var stocks=Object.values(sm).sort(function(a,b){return b.trades-a.trades;});
    var windows=['MORNING','MIDDAY','AFTERNOON','DEAD_ZONE'].map(function(w){ return wm[w]||{window:w,trades:0,wins:0,pnl:0}; });
    return { total:total, wins:wins, pnl:Math.round(totalPnl), wr:Math.round(wins/total*1000)/10,
      days:td.map(function(d){return dm[d];}), stocks:stocks, windows:windows,
      tradeList:tl, cumPnl:cumPnl, tradingDays:td, prices:PRICES };
  })();

  // ── ADV CASH popup DOM (must not shadow futures `_ensurePopup` above) ───────
  function _ensureAcdRoot(){
    if(_el("acd-root")) return;
    var root=document.createElement("div"); root.id="acd-root";
    root.innerHTML='<style id="acd-style">'
      +'#acd-mask{position:fixed;inset:0;background:rgba(2,6,23,.65);backdrop-filter:blur(2px);z-index:100002;display:none}'
      +'#acd-pop{position:fixed;left:10px;top:42px;width:calc(100vw - 20px);height:calc(100vh - 54px);overflow:auto;'
      +'background:radial-gradient(1400px 560px at 20% -20%,rgba(13,148,136,.13),transparent),'
      +'linear-gradient(180deg,#061512,#050e0d 52%,#061512);'
      +'color:#d1faf5;border-radius:14px;'
      +'box-shadow:0 28px 95px rgba(2,6,23,.62),inset 0 0 0 1px rgba(45,212,191,.18),inset 0 0 40px rgba(13,148,136,.12);'
      +'z-index:100003;display:none;padding:16px 16px 24px;border:1px solid rgba(45,212,191,.28);font-family:inherit}'
      +'@media(max-width:980px){#acd-pop{left:4px;top:40px;width:calc(100vw - 8px);height:calc(100vh - 48px)}}'
      +'.acd-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px}'
      +'.acd-title{font-size:46px;line-height:1.02;font-weight:900;color:#eafaf8;letter-spacing:.2px;text-shadow:0 2px 14px rgba(45,212,191,.14)}'
      +'@media(max-width:980px){.acd-title{font-size:26px}}'
      +'.acd-sub{font-size:12px;color:#5eead4;margin-top:4px;line-height:1.55;opacity:.7}'
      +'.acd-cbtn{border:1px solid rgba(148,163,184,.35);background:rgba(5,22,20,.72);border-radius:10px;padding:8px 14px;font-weight:900;font-size:11px;color:#a7f3d0;cursor:pointer;font-family:inherit}'
      +'.acd-cbtn:hover{border-color:rgba(45,212,191,.55);color:#2dd4bf}'
      +'.acd-mwrap{margin-top:14px;padding:18px 16px 20px;border:1px solid rgba(45,212,191,.22);border-radius:16px;background:linear-gradient(165deg,rgba(5,20,18,.92),rgba(3,12,10,.88))}'
      +'.acd-mgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px}'
      +'@media(min-width:1100px){.acd-mgrid{grid-template-columns:repeat(6,minmax(0,1fr))}}'
      +'@media(max-width:520px){.acd-mgrid{grid-template-columns:repeat(2,minmax(0,1fr))}}'
      +'.acd-mc{display:flex;flex-direction:column;gap:6px;padding:14px 12px;border-radius:12px;border:1px solid rgba(45,212,191,.18);background:rgba(0,0,0,.28);min-height:88px}'
      +'.acd-mk{font-size:10px;color:#5eead4;text-transform:uppercase;letter-spacing:.55px;font-weight:700;line-height:1.2}'
      +'.acd-mv{font-size:clamp(22px,2.6vw,34px);font-weight:900;color:#f0fdfb;line-height:1.08;letter-spacing:-.02em}'
      +'.acd-msub{font-size:10px;color:#99f6e4;font-weight:500;line-height:1.3;opacity:.75}'
      +'.acd-mv.gd{color:#4ade80}.acd-mv.bd{color:#fb7185}.acd-mv.tl{color:#2dd4bf}.acd-mv.mt{color:#5eead4;font-size:14px;font-weight:700}'
      +'.acd-tabs{display:flex;gap:8px 14px;flex-wrap:wrap;border-bottom:1px solid rgba(45,212,191,.18);margin-top:16px;padding-bottom:10px;overflow-x:auto;-webkit-overflow-scrolling:touch;scrollbar-width:thin}'
      +'.acd-tab{border:none;background:none;padding:11px 20px 12px;font-size:13px;color:#5eead4;cursor:pointer;font-weight:600;border-bottom:2px solid transparent;font-family:inherit;opacity:.75;border-radius:8px 8px 0 0;flex:0 0 auto;white-space:nowrap}'
      +'.acd-tab.on{border-bottom-color:#2dd4bf;color:#e6fdfb;font-weight:800;opacity:1}'
      +'.acd-tab:hover:not(.on){opacity:1;color:#a7f3d0}'
      +'.acd-tbl{width:100%;min-width:1080px;border-collapse:collapse;margin-top:8px}'
      +'.acd-tbl th{font-size:11px;color:#5eead4;font-weight:800;letter-spacing:.3px;text-transform:uppercase;padding:9px 8px;border-bottom:1px solid rgba(45,212,191,.2);text-align:left;white-space:nowrap}'
      +'.acd-tbl td{padding:8px 8px;border-bottom:1px solid rgba(45,212,191,.07);text-align:left;font-size:12.5px!important;color:#c4ede8!important}'
      +'.acd-tbl tr:hover td{background:rgba(13,148,136,.08)}'
      +'.acd-pill{display:inline-flex;padding:3px 9px;border-radius:999px;font-size:10px;font-weight:900;letter-spacing:.3px}'
      +'.acd-bar{height:5px;background:rgba(45,212,191,.12);border-radius:3px;overflow:hidden;min-width:60px;flex:1}'
      +'.acd-bari{height:100%;border-radius:3px}'
      +'.acd-card{background:rgba(5,20,18,.85);border:1px solid rgba(45,212,191,.15);border-radius:10px;padding:12px 16px;margin-bottom:8px}'
      +'.acd-prow{display:flex;justify-content:space-between;align-items:center;padding:4px 0}'
      +'.acd-pl{color:#99f6e4;font-size:12px;opacity:.8}.acd-pv{font-size:12px;font-weight:600;color:#a7f3d0}'
      +'.acd-bdg{display:inline-flex;padding:2px 8px;border-radius:999px;font-size:9px;font-weight:800;letter-spacing:.5px}'
      +'.acd-srow{display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid rgba(45,212,191,.07)}'
      +'.acd-snum{width:24px;height:24px;border-radius:50%;font-size:9px;font-weight:800;display:flex;align-items:center;justify-content:center;flex-shrink:0}'
      +'.acd-note{font-size:11px;color:#99f6e4;opacity:.7;margin-bottom:10px}'
      +'#acd-pop table.acd-tbl td,#acd-pop table.acd-tbl th{color:#ecfdf5!important;-webkit-text-fill-color:#ecfdf5!important;opacity:1!important;font-size:12px!important}'
      +'#acd-pop table.acd-tbl thead th{color:#5eead4!important;-webkit-text-fill-color:#5eead4!important}'
      +'html.light #acd-pop table.acd-tbl td,html.light #acd-pop table.acd-tbl th{color:#ecfdf5!important;-webkit-text-fill-color:#ecfdf5!important}'
      +'html.light #acd-pop table.acd-tbl thead th{color:#5eead4!important;-webkit-text-fill-color:#5eead4!important}'
      +'.acd-datebar{display:flex;flex-wrap:wrap;align-items:center;gap:10px 14px;margin-top:12px;padding:11px 14px;background:rgba(0,0,0,.28);border-radius:11px;border:1px solid rgba(45,212,191,.18)}'
      +'.acd-datebar label{font-size:11px;color:#5eead4;font-weight:700;white-space:nowrap}'
      +'.acd-datebar input[type=date]{background:rgba(5,22,20,.9);border:1px solid rgba(45,212,191,.35);color:#ecfdf5;border-radius:8px;padding:6px 8px;font-family:inherit;font-size:12px;min-width:132px}'
      +'.acd-datebar .acd-dbgo{border:1px solid rgba(45,212,191,.45);background:rgba(13,148,136,.35);color:#ccfbf1;border-radius:8px;padding:7px 14px;font-weight:800;font-size:11px;cursor:pointer;font-family:inherit}'
      +'.acd-datebar .acd-dbclear{border:1px solid rgba(148,163,184,.35);background:rgba(5,22,20,.72);color:#a7f3d0;border-radius:8px;padding:7px 12px;font-weight:700;font-size:11px;cursor:pointer;font-family:inherit}'
      +'</style>'
      +'<div id="acd-mask"></div>'
      +'<div id="acd-pop">'
      +'<div class="acd-head"><div>'
      +'<div class="acd-title">NSE ADV CASH</div>'
      +'<div class="acd-sub">10-step accuracy roadmap · OBI slope (≥0.015) · KNN memory (300 trades) · Top 25 stocks · ₹20k/stock · drawdown protection</div>'
      +'<div id="acd-mode-strip" style="margin-top:10px;font-size:11px;line-height:1.45;color:#a7f3d0;border:1px solid rgba(45,212,191,.28);border-radius:8px;padding:8px 10px;background:rgba(5,40,35,.5)"></div>'
      +'</div><button class="acd-cbtn" id="acd-close" type="button">✕ Close</button></div>'
      +'<div class="acd-mwrap"><div class="acd-mgrid" id="acd-metrics"></div></div>'
      +'<div class="acd-tabs" id="acd-tabs">'
      +'<button class="acd-tab on" data-t="overview" type="button">Overview</button>'
      +'<button class="acd-tab" data-t="bt-trades" type="button">BT Trades</button>'
      +'<button class="acd-tab" data-t="by-stock" type="button">By Stock</button>'
      +'<button class="acd-tab" data-t="by-window" type="button">By Window</button>'
      +'<button class="acd-tab" data-t="live" type="button">Live Signals</button>'
      +'<button class="acd-tab" data-t="protection" type="button">Protection</button>'
      +'<button class="acd-tab" data-t="steps" type="button">10 Steps</button>'
      +'</div>'
      +'<div id="acd-date-bar" class="acd-datebar">'
      +'<label for="acd-df">From</label><input type="date" id="acd-df" autocomplete="off">'
      +'<label for="acd-dt">To</label><input type="date" id="acd-dt" autocomplete="off">'
      +'<button type="button" class="acd-dbgo" id="acd-date-apply">Apply</button>'
      +'<button type="button" class="acd-dbclear" id="acd-date-clear">Clear</button>'
      +'<span style="font-size:10px;color:#5eead4;opacity:.65;flex:1;min-width:200px">Filters BT Trades, By Stock, By Window, Live list, Overview day log, and live summary cards.</span>'
      +'</div>'
      +'<div id="acd-panel" style="padding-top:12px"></div>'
      +'</div>';
    document.body.appendChild(root);
    _el("acd-close").onclick=_closeAcd;
    _el("acd-mask").onclick=_closeAcd;
    _el("acd-tabs").querySelectorAll(".acd-tab").forEach(function(b){
      b.onclick=function(){
        _S.tab=this.getAttribute("data-t");
        _el("acd-tabs").querySelectorAll(".acd-tab").forEach(function(x){x.classList.remove("on");});
        this.classList.add("on");
        _renderPanel();
      };
    });
    (function _bindAcdDateBar(){
      var ap=_el("acd-date-apply"), cl=_el("acd-date-clear");
      if(ap && !ap.dataset.bound){
        ap.dataset.bound="1";
        ap.onclick=function(){
          var df=_el("acd-df"), dt=_el("acd-dt");
          _S.dateFrom=(df&&df.value)?df.value:"";
          _S.dateTo=(dt&&dt.value)?dt.value:"";
          if(_S.dateFrom && _S.dateTo && _S.dateFrom>_S.dateTo){
            var x=_S.dateFrom; _S.dateFrom=_S.dateTo; _S.dateTo=x;
            if(df) df.value=_S.dateFrom;
            if(dt) dt.value=_S.dateTo;
          }
          _loadLive().then(function(){ _renderMetrics(); _renderPanel(); });
        };
      }
      if(cl && !cl.dataset.bound){
        cl.dataset.bound="1";
        cl.onclick=function(){
          _S.dateFrom=""; _S.dateTo="";
          var df=_el("acd-df"), dt=_el("acd-dt");
          if(df) df.value="";
          if(dt) dt.value="";
          _loadLive().then(function(){ _renderMetrics(); _renderPanel(); });
        };
      }
    })();
  }

  function _closeAcd(){
    if(_el("acd-pop")) _el("acd-pop").style.display="none";
    if(_el("acd-mask")) _el("acd-mask").style.display="none";
  }

  function _renderMetrics(){
    var s=_S.sum||{};
    var est=Number(s.est_accuracy_pct)||81;
    var knnN=Number(s.knn_trades)||0;
    var stA=Number(s.steps_active)||4, stT=Number(s.steps_total)||10;
    var halted=s.today_halted||false;
    var liveN=Number(s.total_trades)||0, liveWr=Number(s.win_rate)||0;
    var demoFilt=(_S.dataMode!=="db"&&_acdHasDateRange());
    var dx=demoFilt?_acdDemoBtAgg():null;
    var wrShow=demoFilt&&!dx.total?0:((dx&&dx.total)?dx.wr:_BT.wr);
    var wrCls=wrShow>=78?"gd":wrShow>=68?"tl":"bd";
    var wrSub=demoFilt&&!dx.total?"0 trades in range":((dx&&dx.total)?(dx.wins+" W · "+(dx.total-dx.wins)+" L · "+dx.days+" day"+(dx.days===1?"":"s")+" (filtered)")
      :(_BT.wins+" W · "+(_BT.total-_BT.wins)+" L · 63 days"));
    var pnlShow=demoFilt&&!dx.total?0:((dx&&dx.total)?dx.pnl:_BT.pnl);
    var pnlSub=demoFilt&&!dx.total?"No demo rows in range":((dx&&dx.total)?"Demo · date range":"Oct–Dec 2024 · Top 25");
    _el("acd-metrics").innerHTML=
      '<div class="acd-mc">'
      +'<div class="acd-mk">EST. ACCURACY</div>'
      +'<div class="acd-mv tl">'+est+'%</div>'
      +'<div class="acd-msub">Base 72% + '+(est-72)+'% · '+stA+' steps</div>'
      +'</div>'
      +'<div class="acd-mc">'
      +'<div class="acd-mk">BACKTEST WR</div>'
      +'<div class="acd-mv '+wrCls+'">'+wrShow+'%</div>'
      +'<div class="acd-msub">'+wrSub+'</div>'
      +'</div>'
      +'<div class="acd-mc">'
      +'<div class="acd-mk">BACKTEST P&amp;L</div>'
      +'<div class="acd-mv '+(pnlShow>=0?"gd":"bd")+'">'+_inr(pnlShow)+'</div>'
      +'<div class="acd-msub">'+pnlSub+'</div>'
      +'</div>'
      +'<div class="acd-mc">'
      +'<div class="acd-mk">STEPS ACTIVE</div>'
      +'<div class="acd-mv tl">'+stA+'<span style="font-size:16px;font-weight:600;color:#5eead4;opacity:.6"> / '+stT+'</span></div>'
      +'<div class="acd-msub">1–4 on · 5–10 build</div>'
      +'</div>'
      +'<div class="acd-mc">'
      +'<div class="acd-mk">KNN MEMORY</div>'
      +'<div class="acd-mv '+(knnN>=300?"gd":"mt")+'">'+knnN+'<span style="font-size:16px;font-weight:600;color:#5eead4;opacity:.6">/300</span></div>'
      +'<div class="acd-msub">'+(knnN>=300?"READY · +3%":"Need "+(300-knnN)+" live trades")+'</div>'
      +'</div>'
      +'<div class="acd-mc">'
      +'<div class="acd-mk">SYSTEM</div>'
      +'<div class="acd-mv '+(halted?"bd":"gd")+'">'+(halted?"HALTED":"ACTIVE")+'</div>'
      +'<div class="acd-msub">Live: '+liveN+' trades'+(liveN>0?' · WR '+liveWr.toFixed(1)+'%':'')+'</div>'
      +'</div>';
  }

  // ── Overview tab: cumulative chart + day log ──────────────────────────────
  function _renderOverview(){
    var ser=_acdOverviewSeries();
    var days=ser.days, cum=ser.cum, td=ser.td;
    if(!days.length){
      var hint=(_acdHasDateRange()&&_S.dataMode!=="db")
        ? " Built-in demo data is only <strong>Oct–Dec 2024</strong>; pick dates in that span or clear filters."
        :(_acdHasDateRange()?" Or your database has no completed trades in this range.":"");
      return '<div class="acd-note">No trades in the current view — widen the date range or clear filters.'+hint+'</div>';
    }
    var totT=0, totW=0, totP=0;
    days.forEach(function(d){ totT+=d.trades; totW+=d.wins; totP+=d.pnl; });
    var wrAll=totT?Math.round(totW/totT*10)/10:0;
    var maxCum=Math.max.apply(null,cum.map(Math.abs))||1;
    var maxDay=Math.max.apply(null,days.map(function(d){return Math.abs(d.pnl);}))||1;
    var lastCum=cum.length?cum[cum.length-1]:0;
    var hdr=ser.n===63&&!_acdHasDateRange()&&_S.dataMode!=="db"?"CUMULATIVE P&L — 63 TRADING DAYS":"CUMULATIVE P&L — "+ser.n+" day"+(ser.n===1?"":"s");
    var sub=_acdHasDateRange()?"Filtered range":"63 DAYS · OCT–DEC 2024";
    if(_S.dataMode==="db") sub="Logged trades · "+ser.n+" day"+(ser.n===1?"":"s");
    var html='<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:18px">'
      +'<div class="acd-card" style="flex:2;min-width:220px;border-color:rgba(45,212,191,.3)">'
      +'<div style="font-size:10px;color:#5eead4;font-weight:700;letter-spacing:.5px;margin-bottom:8px">'+hdr+'</div>'
      +'<div style="font-size:clamp(24px,3.5vw,40px);font-weight:900;color:'+(lastCum>=0?'#4ade80':'#fb7185')+'">'+_inr(lastCum)+'</div>'
      +'<div style="font-size:11px;color:#5eead4;margin-top:3px;opacity:.7">'+_esc(ser.t0)+' → '+_esc(ser.t1)+'</div>'
      +'<div style="margin-top:10px;display:flex;gap:2px;align-items:flex-end;height:52px">'
      +cum.map(function(v,i){ var h=Math.max(Math.round(Math.abs(v)/maxCum*50),2);
        return '<div style="flex:1;height:'+h+'px;background:'+(v>=0?'rgba(74,222,128,.6)':'rgba(251,113,133,.5)')+';border-radius:1px 1px 0 0" title="'+_esc(td[i])+': '+_inr(v)+'"></div>';
      }).join("")+'</div>'
      +'</div>'
      +'<div style="display:flex;flex-direction:column;gap:8px;flex:1;min-width:140px">'
      +'<div class="acd-card" style="flex:1">'
      +'<div class="acd-mk" style="margin-bottom:4px">WIN RATE</div>'
      +'<div style="font-size:clamp(22px,3vw,36px);font-weight:900;color:#2dd4bf">'+wrAll+'%</div>'
      +'<div style="font-size:11px;color:#5eead4;opacity:.7">'+totW+' W · '+(totT-totW)+' L</div>'
      +'</div>'
      +'<div class="acd-card" style="flex:1">'
      +'<div class="acd-mk" style="margin-bottom:4px">AVG / DAY</div>'
      +'<div style="font-size:clamp(18px,2.4vw,30px);font-weight:900;color:#4ade80">'+_inr(days.length?Math.round(totP/days.length):0)+'</div>'
      +'<div style="font-size:11px;color:#5eead4;opacity:.7">~₹'+(days.length?Math.round(totP/days.length*22/1000):0)+'k/mo est.</div>'
      +'</div>'
      +'</div></div>'
      +'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">'
      +'<span style="font-size:10px;color:#5eead4;font-weight:700;letter-spacing:.4px">DAY-BY-DAY LOG</span>'
      +'<span style="font-size:9px;color:#5eead4;opacity:.5">'+_esc(sub)+'</span>'
      +'</div>'
      +'<table class="acd-tbl"><thead><tr>'
      +'<th>Date</th><th>Trades</th><th>Wins</th><th>WR</th><th>P&L</th><th style="min-width:80px">Bar</th>'
      +'</tr></thead><tbody>'
      +days.slice().reverse().map(function(d){
        var wr=d.trades?Math.round(d.wins/d.trades*100):0;
        var bw=maxDay?Math.round(Math.abs(d.pnl)/maxDay*88):0;
        var clr=d.pnl>=0?'#4ade80':'#fb7185';
        return '<tr><td style="color:#7ae8d8">'+_esc(d.date)+'</td>'
          +'<td>'+d.trades+'</td><td style="color:#4ade80">'+d.wins+'</td>'
          +'<td style="color:'+(wr>=75?'#4ade80':wr>=65?'#2dd4bf':'#fb7185')+';font-weight:800">'+wr+'%</td>'
          +'<td style="color:'+clr+';font-weight:800">'+_inr(d.pnl)+'</td>'
          +'<td><div style="width:'+bw+'%;height:5px;background:'+clr+';border-radius:3px;min-width:2px"></div></td></tr>';
      }).join("")+'</tbody></table>';
    return html;
  }

  // ── BT Trades tab: per-trade entry / clear / SL / exit ───────────────────
  function _renderBtTrades(){
    var fromDb=(_S.dataMode==="db");
    var raw=_acdAllTrades();
    var tl=fromDb?raw.slice():raw.slice().reverse();
    var rng=_acdHasDateRange()?" <strong>Date filter on.</strong>":"";
    var note=fromDb
      ? 'Logged trades from <strong>database</strong> (newest first).'+rng+' SL / T1 / T2 / clear level use the same % ladder as the built-in demo backtest from your stored entry and direction.'
      : 'Demo backtest (Oct–Dec 2024) — entry · clear level (OBI trigger) · SL · T1 · T2 · exit · outcome.'+rng+' Use the date bar to narrow days. When you log signals + outcomes via the API, this tab uses <strong>live DB rows</strong>.';
    var html='<div class="acd-note">'+note+'</div>'
      +'<div style="overflow-x:auto;-webkit-overflow-scrolling:touch;margin-top:4px">'
      +'<table class="acd-tbl"><thead><tr>'
      +'<th>Date</th><th>Time</th><th>Symbol</th><th>Dir</th><th>Window</th>'
      +'<th>Entry</th><th>Clear Lvl</th><th>SL</th><th>T1</th><th>T2</th>'
      +'<th>Exit</th><th>Out</th><th>P&L</th>'
      +'</tr></thead><tbody>'
      +tl.slice(0,150).map(function(raw){
        var t=_acdRow(raw);
        var ou=String(t.outcome||"").toUpperCase();
        var oc=ou==='T2'?'<span style="color:#4ade80;font-weight:800">T2 ★</span>'
          :(ou==='T1'||ou==='WIN'||ou==='TARGET')?'<span style="color:#2dd4bf;font-weight:800">'+_esc(ou||"T1")+'</span>'
          :'<span style="color:#fb7185;font-weight:800">'+_esc(ou||"SL")+'</span>';
        var isBuy=String(t.dir).toUpperCase()==='BUY';
        return '<tr>'
          +'<td style="color:#7ae8d8;font-size:12px">'+_esc(t.date||"—")+'</td>'
          +'<td style="color:#ccfbf1">'+_esc(t.time||"—")+'</td>'
          +'<td style="font-weight:800;color:#2dd4bf">'+_esc(t.sym||"—")+'</td>'
          +'<td><span class="acd-pill" style="background:'+(isBuy?'rgba(6,78,59,.8)':'rgba(127,29,29,.8)')+';color:'+(isBuy?'#6ee7b7':'#fca5a5')+'">'+_esc(t.dir)+'</span></td>'
          +'<td style="color:#5eead4;font-size:12px">'+_esc(t.window||"—")+'</td>'
          +'<td style="color:#f0fdf4;font-weight:700">'+_px(t.entry)+'</td>'
          +'<td style="color:#fbbf24;font-size:12px">'+_px(t.clearLevel)+'</td>'
          +'<td style="color:#fecaca">'+_px(t.sl)+'</td>'
          +'<td style="color:#bbf7d0">'+_px(t.t1)+'</td>'
          +'<td style="color:#4ade80;font-weight:700">'+_px(t.t2)+'</td>'
          +'<td style="color:'+(t.win?'#4ade80':'#fb7185')+';font-weight:700">'+_px(t.exit)+'</td>'
          +'<td>'+oc+'</td>'
          +'<td style="color:'+(t.pnl>=0?'#4ade80':'#fb7185')+';font-weight:800">'+_inr(t.pnl)+'</td>'
          +'</tr>';
      }).join("")+'</tbody></table></div>'
      +(tl.length>150?'<div style="color:#5eead4;font-size:11px;opacity:.6;text-align:center;padding:8px">Showing 150 of '+tl.length+' trades</div>':"");
    return html;
  }

  // ── By Stock tab ─────────────────────────────────────────────────────────
  function _renderByStock(){
    var rows = _acdAggByStock(_acdAllTrades());
    var lm = {};
    (_S.live || []).forEach(function(t){ var s = String(t.sym || "").trim(); if(s) lm[s] = (lm[s] || 0) + 1; });
    if(!rows.length){
      return '<div class="acd-note">No completed trades yet — log signals and outcomes via the API, or use the demo backtest (BT Trades tab).</div>';
    }
    var absPnls = rows.map(function(r){ return Math.abs(Number(r.pnl) || 0); });
    var maxPnl = absPnls.length ? Math.max.apply(null, absPnls) : 1;
    if(!(maxPnl > 0)) maxPnl = 1;
    var srcNote = (_S.dataMode==="db") ? "Source: <strong>database</strong> (aggregated from logged trades)." : "Source: <strong>demo</strong> Oct–Dec 2024 backtest.";
    if(_acdHasDateRange()) srcNote += " <strong>Date filter on.</strong>";
    return '<div class="acd-note">' + srcNote + ' Entry range and SL gap are computed from the same trade rows as <strong>BT Trades</strong>.</div>'
      +'<div style="overflow-x:auto;-webkit-overflow-scrolling:touch;margin-top:4px">'
      +'<table class="acd-tbl"><thead><tr>'
      +'<th>#</th><th>Symbol</th><th>Entry Range</th><th>Avg SL Gap</th>'
      +'<th>Trades</th><th>WR</th><th>T1</th><th>T2 ★</th><th>SL</th><th>P&L</th><th>Live</th><th style="min-width:60px">Bar</th>'
      +'</tr></thead><tbody>'
      +rows.map(function(r, i){
        var wr = r.trades ? Math.round(r.wins / r.trades * 100) : 0;
        var slGap = Number(r.slGap || 0).toFixed(2);
        var rng = (r.min > 0 || r.max > 0) ? (_px(r.min) + "–" + _px(r.max)) : "—";
        var bw = Math.round(Math.abs(Number(r.pnl) || 0) / maxPnl * 80);
        return '<tr>'
          +'<td style="color:#5eead4!important;opacity:.9!important">'+(i + 1)+'</td>'
          +'<td style="font-weight:800!important;color:#2dd4bf!important">'+_esc(r.sym)+'</td>'
          +'<td style="color:#ccfbf1!important">'+rng+'</td>'
          +'<td style="color:#fcd34d!important">±'+slGap+'</td>'
          +'<td style="color:#ecfdf5!important">'+r.trades+'</td>'
          +'<td style="color:'+(wr >= 78 ? "#4ade80" : wr >= 68 ? "#2dd4bf" : "#fb7185")+'!important;font-weight:800">'+wr+'%</td>'
          +'<td style="color:#86efac!important">'+r.t1hits+'</td>'
          +'<td style="color:#4ade80!important;font-weight:700">'+r.t2hits+'</td>'
          +'<td style="color:#fb7185!important">'+r.slhits+'</td>'
          +'<td style="color:'+(r.pnl >= 0 ? "#4ade80" : "#fb7185")+'!important;font-weight:800">'+_inr(r.pnl)+'</td>'
          +'<td style="color:'+(lm[r.sym] ? "#2dd4bf" : "#94a3b8")+'!important">'+(lm[r.sym] || "—")+'</td>'
          +'<td><div style="width:'+bw+'%;height:5px;background:'+(r.pnl >= 0 ? "rgba(74,222,128,.55)" : "rgba(251,113,133,.45)")+';border-radius:3px;min-width:2px"></div></td>'
          +'</tr>';
      }).join("")+'</tbody></table></div>';
  }

  // ── By Window tab ─────────────────────────────────────────────────────────
  function _renderByWindow(){
    var NOTES={MORNING:'09:15–10:00 · First move · institutional loading',MIDDAY:'10:00–11:30 · Secondary · OBI slope confirmation',DEAD_ZONE:'11:30–13:15 · BLOCKED (Step 2) · no entries',AFTERNOON:'13:30–14:30 · Continuation · reduce size 25%'};
    var html='<div class="acd-note">DEAD_ZONE (11:30–13:15) fully blocked by Step 2. Signals ignored regardless of strength. '
      +((_S.dataMode==="db") ? "Aggregated from <strong>database</strong> trades." : "Aggregated from <strong>demo</strong> backtest.")
      +(_acdHasDateRange()?" <strong>Date filter on.</strong>":"")+'</div>';
    _acdAggByWindow(_acdAllTrades()).forEach(function(w){
      var dead=w.window==='DEAD_ZONE', wr=w.trades?Math.round(w.wins/w.trades*100):0;
      var bc=dead?'rgba(239,68,68,.6)':(wr>=78?'rgba(74,222,128,.45)':'rgba(45,212,191,.3)');
      html+='<div class="acd-card" style="border-left:3px solid '+bc+';margin-bottom:10px">'
        +'<div style="display:flex;justify-content:space-between;align-items:flex-start">'
        +'<div><div style="font-size:16px;font-weight:800;color:'+(dead?'#fb7185':wr>=78?'#4ade80':'#2dd4bf')+'">'
        +_esc(w.window)+(dead?'<span class="acd-bdg" style="background:rgba(127,29,29,.7);color:#fca5a5;margin-left:8px">BLOCKED · STEP 2</span>':"")
        +'</div><div style="font-size:12px;color:#5eead4;margin-top:4px;opacity:.8">'+_esc(NOTES[w.window]||"")+'</div></div>'
        +'<div style="text-align:right;flex-shrink:0;margin-left:16px">'
        +'<div style="font-size:clamp(18px,2.2vw,26px);font-weight:900;color:'+(w.pnl>=0?'#4ade80':'#fb7185')+'">'+_inr(w.pnl)+'</div>'
        +'<div style="font-size:11px;color:#5eead4;opacity:.7">'+w.trades+' trades</div>'
        +'</div></div>'
        +(dead?'':'<div style="display:flex;align-items:center;gap:16px;margin-top:10px">'
          +'<span style="color:#c4ede8;font-size:13px">WR <strong style="color:'+(wr>=78?'#4ade80':wr>=68?'#2dd4bf':'#fb7185')+'">'+wr+'%</strong></span>'
          +'<span style="color:#c4ede8;font-size:13px">Wins <strong style="color:#86efac">'+w.wins+'</strong></span>'
          +'<span style="color:#c4ede8;font-size:13px">Losses <strong style="color:#fb7185">'+(w.trades-w.wins)+'</strong></span>'
          +'<div class="acd-bar"><div class="acd-bari" style="width:'+Math.min(wr,100)+'%;background:'+(wr>=78?'#4ade80':wr>=68?'#2dd4bf':'#f87171')+'"></div></div>'
          +'</div>')
        +'</div>';
    });
    return html;
  }

  // ── Live Signals tab (from API) ───────────────────────────────────────────
  function _renderLive(){
    var live=_S.live||[];
    if(!live.length){
      return '<div style="text-align:center;padding:48px 24px;color:#5eead4;opacity:.7">'
        +'<div style="font-size:18px;font-weight:700;margin-bottom:8px">No live signals yet</div>'
        +'<div style="font-size:12px;line-height:1.7">Use <span style="color:#2dd4bf;font-family:monospace">POST /api/adv-cash/log-signal</span> to start recording.<br>'
        +'Once signals are logged with outcomes, live WR and P&L will appear here.</div>'
        +'</div>';
    }
    var total=live.filter(function(t){return t.outcome;}).length;
    var wins=live.filter(function(t){return ['WIN','T1','T2','TARGET'].indexOf(String(t.outcome||""))>=0;}).length;
    var lPnl=live.reduce(function(a,t){return a+(Number(t.pnl_inr)||0);},0);
    var html='<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px">'
      +'<div class="acd-card" style="flex:1;padding:10px 14px"><div class="acd-mk">LIVE TRADES</div><div style="font-size:26px;font-weight:900;color:#2dd4bf">'+live.length+'</div></div>'
      +'<div class="acd-card" style="flex:1;padding:10px 14px"><div class="acd-mk">LIVE WR</div><div style="font-size:26px;font-weight:900;color:'+(total>0&&wins/total>=0.7?'#4ade80':'#fbbf24')+'">'+(total?Math.round(wins/total*100)+'%':'—')+'</div></div>'
      +'<div class="acd-card" style="flex:1;padding:10px 14px"><div class="acd-mk">LIVE P&L</div><div style="font-size:26px;font-weight:900;color:'+(lPnl>=0?'#4ade80':'#fb7185')+'">'+_inr(lPnl)+'</div></div>'
      +'</div>'
      +'<table class="acd-tbl"><thead><tr>'
      +'<th>Date</th><th>Time</th><th>Symbol</th><th>Dir</th><th>Window</th>'
      +'<th>Entry</th><th>OBI</th><th>Slope</th><th>Outcome</th><th>Exit</th><th>P&L</th>'
      +'</tr></thead><tbody>'
      +live.slice(0,100).map(function(t){
        var win=['WIN','T1','T2','TARGET'].indexOf(String(t.outcome||""))>=0;
        var oc=t.outcome?(win?'<span style="color:#4ade80;font-weight:800">'+_esc(t.outcome)+'</span>':'<span style="color:#fb7185;font-weight:800">'+_esc(t.outcome)+'</span>'):'<span style="color:#5eead4;opacity:.5">pending</span>';
        var isBuy=t.direction==="BUY";
        var slopeOk=Number(t.obi_slope||0)>=0.015;
        return '<tr>'
          +'<td style="color:#7ae8d8;font-size:11px">'+_esc(t.date)+'</td>'
          +'<td>'+_esc(t.time)+'</td>'
          +'<td style="font-weight:800;color:#2dd4bf">'+_esc(t.sym)+'</td>'
          +'<td><span class="acd-pill" style="background:'+(isBuy?'rgba(6,78,59,.8)':'rgba(127,29,29,.8)')+';color:'+(isBuy?'#6ee7b7':'#fca5a5')+'">'+_esc(t.direction)+'</span></td>'
          +'<td style="font-size:11px;color:#5eead4">'+_esc(t.time_window||"—")+'</td>'
          +'<td style="font-weight:700;color:#e2e8f0">'+Number(t.entry||0).toFixed(2)+'</td>'
          +'<td>'+Number(t.obi_score||0).toFixed(2)+'</td>'
          +'<td style="color:'+(slopeOk?'#2dd4bf':'#fb7185')+'">'+(slopeOk?"✓ ":"")+Number(t.obi_slope||0).toFixed(3)+'</td>'
          +'<td>'+oc+'</td>'
          +'<td style="color:#c4ede8">'+Number(t.exit_price||0).toFixed(2)+'</td>'
          +'<td style="color:'+(Number(t.pnl_inr||0)>=0?'#4ade80':'#fb7185')+';font-weight:800">'+_inr(t.pnl_inr||0)+'</td>'
          +'</tr>';
      }).join("")+'</tbody></table>';
    return html;
  }

  // ── Protection tab ────────────────────────────────────────────────────────
  function _renderProtection(){
    var d=_S.drw||{};
    var st=d.system_status||'ACTIVE', sc=st==='HALTED'?'#fb7185':st==='PAUSED'?'#fbbf24':'#4ade80';
    var dPnl=Number(d.daily_pnl_inr||0), dPct=Math.min(Math.abs(dPnl)/1500*100,100);
    var cL=Number(d.consec_losses||0), cPct=Math.min(cL/4*100,100);
    return '<div class="acd-card" style="border-left:4px solid '+sc+';margin-bottom:16px">'
      +'<div style="display:flex;justify-content:space-between;align-items:center">'
      +'<div style="font-size:clamp(18px,2.5vw,26px);font-weight:900;color:'+sc+'">SYSTEM '+_esc(st)+'</div>'
      +'<div style="font-size:11px;color:#5eead4;opacity:.7">'+_esc(d.today||new Date().toISOString().slice(0,10))+'</div>'
      +'</div>'
      +(d.halt_active?'<div style="background:rgba(127,29,29,.5);border:1px solid rgba(239,68,68,.4);border-radius:6px;padding:10px;margin-top:10px;font-size:12px;color:#fca5a5;line-height:1.5">Daily loss limit ₹1,500 reached — all entries blocked until 09:15 tomorrow.</div>':"")
      +(d.pause_active&&!d.halt_active?'<div style="background:rgba(113,63,18,.5);border:1px solid rgba(245,158,11,.4);border-radius:6px;padding:10px;margin-top:10px;font-size:12px;color:#fde68a;line-height:1.5">4 consecutive losses — 30-min pause active. Let market settle.</div>':"")
      +'</div>'
      +'<div style="font-size:10px;color:#5eead4;font-weight:700;letter-spacing:.5px;margin:12px 0 6px">RULE 1 — DAILY HALT · ₹1,500 (1.5% of ₹1L)</div>'
      +'<div class="acd-card" style="margin-bottom:12px">'
      +'<div class="acd-prow"><span class="acd-pl">Today P&L</span><span class="acd-pv" style="color:'+(dPnl>=0?'#4ade80':'#fb7185')+'">'+_inr(dPnl)+'</span></div>'
      +'<div class="acd-prow"><span class="acd-pl">Halt at</span><span class="acd-pv">₹−1,500 loss</span></div>'
      +'<div style="margin-top:8px"><div style="display:flex;justify-content:space-between;font-size:11px;color:#5eead4;margin-bottom:4px;opacity:.8"><span>Daily loss used</span><span>'+Math.round(dPct)+'%</span></div>'
      +'<div class="acd-bar"><div class="acd-bari" style="width:'+dPct+'%;background:'+(dPct>80?'#ef4444':dPct>50?'#f59e0b':'#2dd4bf')+'"></div></div></div>'
      +'</div>'
      +'<div style="font-size:10px;color:#5eead4;font-weight:700;letter-spacing:.5px;margin-bottom:6px">RULE 2 — CONSECUTIVE LOSS PAUSE (4 → 30 MIN)</div>'
      +'<div class="acd-card" style="margin-bottom:12px">'
      +'<div class="acd-prow"><span class="acd-pl">Current streak</span><span class="acd-pv" style="color:'+(cL>=4?'#fb7185':cL>=2?'#fbbf24':'#4ade80')+'">'+cL+' consecutive losses</span></div>'
      +'<div class="acd-prow"><span class="acd-pl">Pause at</span><span class="acd-pv">4 in a row</span></div>'
      +'<div class="acd-bar" style="margin-top:6px"><div class="acd-bari" style="width:'+cPct+'%;background:'+(cPct>=100?'#ef4444':cPct>=50?'#f59e0b':'#2dd4bf')+'"></div></div>'
      +'</div>'
      +'<div style="font-size:10px;color:#5eead4;font-weight:700;letter-spacing:.5px;margin-bottom:6px">RULE 3 — VIX POSITION SIZING</div>'
      +'<div class="acd-card" style="margin-bottom:14px">'
      +'<div class="acd-prow"><span class="acd-pl">VIX &lt; 18</span><span class="acd-pv" style="color:#4ade80">₹20,000/stock · 100% size · 3 concurrent = ₹60k deployed</span></div>'
      +'<div class="acd-prow"><span class="acd-pl">VIX 18–22</span><span class="acd-pv" style="color:#fbbf24">₹15,000/stock · 75%</span></div>'
      +'<div class="acd-prow"><span class="acd-pl">VIX &gt; 22</span><span class="acd-pv" style="color:#fb7185">₹10,000/stock · 50%</span></div>'
      +'</div>'
      +'<div style="font-size:11px;color:#5eead4;line-height:1.9;padding:14px;background:rgba(5,20,18,.8);border-radius:8px;border:1px solid rgba(45,212,191,.1);opacity:.85">'
      +'<strong style="opacity:1;color:#a7f3d0">P&L math at 80% WR · 15 trades/day · ₹20k/stock:</strong><br>'
      +'Wins 12 × <span style="color:#4ade80">₹160</span> = ₹1,920 &nbsp;·&nbsp; Losses 3 × <span style="color:#fb7185">₹80</span> = ₹240 &nbsp;·&nbsp; Brokerage = ₹300<br>'
      +'<strong style="color:#2dd4bf">Net ₹1,380/day</strong> &nbsp;·&nbsp; Monthly ~<strong style="color:#2dd4bf">₹15,000</strong> &nbsp;·&nbsp; Annual ~<strong style="color:#4ade80">₹1.8L on ₹1L = 180% p.a.</strong>'
      +'</div>';
  }

  // ── 10 Steps tab ─────────────────────────────────────────────────────────
  function _renderSteps(){
    var d=_S.stp||{};
    var base=Number(d.base_accuracy)||72, gain=Number(d.active_gain)||9, est=Number(d.est_accuracy)||81;
    var steps=d.steps||[
      {step:1,name:'Top 25 liquid stocks only',gain_pct:3,active:true},
      {step:2,name:'Hard time windows — no dead zones',gain_pct:2,active:true},
      {step:3,name:'Dynamic VIX threshold',gain_pct:2,active:true},
      {step:4,name:'OBI slope filter (rising not peaked)',gain_pct:2,active:true},
      {step:5,name:'Weighted 5-level OBI',gain_pct:2,active:false},
      {step:6,name:'Min 4-signal consensus (up from 3)',gain_pct:2,active:false},
      {step:7,name:'Pre-market delivery + OI watchlist',gain_pct:2,active:false},
      {step:8,name:'First candle gate 9:15–9:17',gain_pct:2,active:false},
      {step:9,name:'Pattern memory KNN (300+ trades)',gain_pct:3,active:false},
      {step:10,name:'OBI reversal exit mid-trade',gain_pct:1,active:false}
    ];
    var html='<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:16px">'
      +'<div class="acd-card" style="flex:1;min-width:100px;padding:12px">'
      +'<div class="acd-mk" style="margin-bottom:4px">BASE</div>'
      +'<div style="font-size:clamp(20px,2.4vw,30px);font-weight:900;color:#5eead4;opacity:.7">'+base+'%</div>'
      +'</div>'
      +'<div class="acd-card" style="flex:1;min-width:100px;padding:12px">'
      +'<div class="acd-mk" style="margin-bottom:4px">STEP GAIN</div>'
      +'<div style="font-size:clamp(20px,2.4vw,30px);font-weight:900;color:#2dd4bf">+'+gain+'%</div>'
      +'</div>'
      +'<div class="acd-card" style="flex:1;min-width:100px;padding:12px;border-color:rgba(74,222,128,.25)">'
      +'<div class="acd-mk" style="margin-bottom:4px">EST. ACCURACY</div>'
      +'<div style="font-size:clamp(20px,2.4vw,30px);font-weight:900;color:#4ade80">'+est+'%</div>'
      +'</div>'
      +'</div>'
      +'<div class="acd-note" style="margin-bottom:12px">Steps 1–4 live now. Enable 5–10 one at a time — validate 50+ trades each before next step.</div>'
      +steps.map(function(s){
        return '<div class="acd-srow">'
          +'<div class="acd-snum" style="background:'+(s.active?'rgba(13,148,136,.75)':'rgba(20,40,38,.8)')+';color:'+(s.active?'#ccfbf1':'#5eead4;opacity:.5')+'">'+s.step+'</div>'
          +'<div style="flex:1;color:'+(s.active?'#d1faf5':'#5eead4')+';font-size:12px;'+(s.active?'':'opacity:.55')+'">'+_esc(s.name)+'</div>'
          +'<div style="font-size:13px;font-weight:800;width:36px;text-align:right;color:'+(s.active?'#2dd4bf':'#5eead4')+';'+(s.active?'':'opacity:.4')+'">+'+s.gain_pct+'%</div>'
          +'<div style="width:72px;text-align:right">'
          +(s.active
            ?'<span class="acd-bdg" style="background:rgba(13,148,136,.45);color:#a7f3d0">ACTIVE</span>'
            :'<span class="acd-bdg" style="background:rgba(20,40,38,.8);color:#5eead4;opacity:.6">BUILD</span>')
          +'</div></div>';
      }).join("")
      +'<div style="margin-top:18px;font-size:11px;color:#5eead4;line-height:1.9;padding:14px;background:rgba(5,20,18,.8);border-radius:8px;border:1px solid rgba(45,212,191,.1);opacity:.85">'
      +'<strong style="opacity:1;color:#a7f3d0">OBI Slope gate (Step 4) — single highest-impact filter:</strong><br>'
      +'Stationary OBI 0.45 = imbalance peaked. Rising 0.20→0.42 over 8 ticks = institutions loading NOW.<br>'
      +'Gate: <span style="color:#2dd4bf;font-family:monospace">obi_slope(history[-8:]) ≥ 0.015</span> — skip if below, regardless of OBI score.<br>'
      +'Steps 1–3 alone: 72% → 79%. Step 4 → 81%. KNN (Step 9): needs 300 live trades first.'
      +'</div>';
    return html;
  }

  function _renderPanel(){
    var p=_el("acd-panel"); if(!p) return;
    var t=_S.tab;
    if(t==="overview") p.innerHTML=_renderOverview();
    else if(t==="bt-trades") p.innerHTML=_renderBtTrades();
    else if(t==="by-stock") p.innerHTML=_renderByStock();
    else if(t==="by-window") p.innerHTML=_renderByWindow();
    else if(t==="live") p.innerHTML=_renderLive();
    else if(t==="protection") p.innerHTML=_renderProtection();
    else if(t==="steps") p.innerHTML=_renderSteps();
  }

  function _loadLive(){
    return Promise.all([
      _req("/api/adv-cash/summary"+_acdQs()).then(function(d){_S.sum=d;}).catch(function(){}),
      _req("/api/adv-cash/recent"+_acdQs(["limit=50"])).then(function(d){_S.live=Array.isArray(d)?d:[];}).catch(function(){_S.live=[];}),
      _req("/api/adv-cash/bt-display-trades"+_acdQs(["limit=200"])).then(function(d){
        _S.btRows=Array.isArray(d)?d:[];
        if(_S.btRows.length) _S.dataMode="db";
        else if(_acdHasDateRange()) _S.dataMode="db";
        else _S.dataMode="demo";
      }).catch(function(){ _S.btRows=[]; _S.dataMode="demo"; }),
      _req("/api/adv-cash/drawdown").then(function(d){_S.drw=d;}).catch(function(){}),
      _req("/api/adv-cash/steps").then(function(d){_S.stp=d;}).catch(function(){}),
    ]);
  }

  window.openAdvCashPopup = function(){
    _ensureAcdRoot();
    var df=_el("acd-df"), dt=_el("acd-dt");
    if(df) df.value=_S.dateFrom||"";
    if(dt) dt.value=_S.dateTo||"";
    ["is-pop","is-pop-mask","is-cash-pop","is-cash-mask"].forEach(function(id){
      var e=_el(id); if(e) e.style.display="none";
    });
    if(_el("acd-pop")) _el("acd-pop").style.display="block";
    if(_el("acd-mask")) _el("acd-mask").style.display="block";
    _updateAcdModeStrip();
    // render backtest data immediately (no API needed)
    _renderMetrics();
    _renderPanel();
    // then refresh with live data from server
    _loadLive().then(function(){ _renderMetrics(); _renderPanel(); });
  };
})();

