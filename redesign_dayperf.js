var fs = require('fs');
var fp = 'c:/Users/visra/OneDrive/Desktop/trading_adv/nse_edge_live/frontend/index.html';
var html = fs.readFileSync(fp, 'utf8');

// ── 1. Replace modal CSS block ──────────────────────────────────────────────
var oldCss = `#perf-modal{display:none;position:fixed;inset:0;z-index:99998}
#perf-modal.open{display:flex}
.perf-ov{position:absolute;inset:0;background:rgba(0,0,0,.62);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px)}
.perf-panel{position:relative;margin:auto;width:min(1100px,96vw);height:min(760px,92vh);border-radius:12px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(180deg,rgba(18,29,44,.96),rgba(13,21,32,.98));box-shadow:0 18px 70px rgba(0,0,0,.55);display:flex;flex-direction:column;overflow:hidden}
.light .perf-panel{background:linear-gradient(180deg,rgba(255,255,255,.98),rgba(242,245,251,.98));border-color:rgba(0,0,0,.08)}
.perf-hd{display:flex;align-items:center;gap:10px;padding:14px 16px;border-bottom:1px solid rgba(255,255,255,.08)}
.light .perf-hd{border-bottom-color:rgba(0,0,0,.08)}
.perf-title{font-family:var(--fh);font-size:14px;font-weight:900;letter-spacing:1.6px;color:var(--t0)}
.perf-sub{margin-left:auto;font-size:9px;color:var(--t2);font-weight:700;letter-spacing:.3px}
.perf-x{margin-left:8px;width:30px;height:30px;border-radius:8px;border:1px solid var(--b1);background:transparent;color:var(--t2);cursor:pointer;font-size:16px;line-height:1}
.perf-x:hover{background:var(--st-d);border-color:var(--st-b);color:var(--st)}
.perf-bd{padding:14px 16px;overflow:auto;min-height:0;display:flex;flex-direction:column;gap:12px}
.perf-sum{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}
@media (max-width:860px){.perf-sum{grid-template-columns:repeat(2,minmax(0,1fr))}}
.perf-card{border:1px solid rgba(255,255,255,.08);background:rgba(0,0,0,.18);border-radius:10px;padding:12px 12px;box-shadow:inset 0 1px 0 rgba(255,255,255,.04)}
.light .perf-card{background:rgba(255,255,255,.7);border-color:rgba(0,0,0,.08)}
.perf-k{font-size:8px;color:var(--t2);letter-spacing:1.2px;font-weight:800;text-transform:uppercase}
.perf-v{margin-top:6px;font-size:18px;font-weight:900;font-variant-numeric:tabular-nums;color:var(--t0)}
.perf-v.pos{color:var(--go)}.perf-v.neg{color:var(--st)}.perf-v.mid{color:var(--am)}
.perf-grid{display:grid;grid-template-columns:320px 1fr;gap:12px;min-height:0}
@media (max-width:920px){.perf-grid{grid-template-columns:1fr}}
.perf-sec{border:1px solid rgba(255,255,255,.08);background:rgba(0,0,0,.14);border-radius:10px;padding:12px}
.light .perf-sec{background:rgba(255,255,255,.62);border-color:rgba(0,0,0,.08)}
.perf-sec-h{display:flex;align-items:baseline;justify-content:space-between;margin-bottom:8px}
.perf-sec-h b{font-size:9px;letter-spacing:1.2px;text-transform:uppercase;color:var(--t1)}
.perf-sec-h span{font-size:8px;color:var(--t2)}
.perf-sec-row{display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid rgba(255,255,255,.06);font-size:9px}
.light .perf-sec-row{border-bottom-color:rgba(0,0,0,.06)}
.perf-sec-row:last-child{border:none}
.perf-sec-row .l{color:var(--t1);font-weight:700}
.perf-sec-row .r{font-variant-numeric:tabular-nums;color:var(--t0);font-weight:800}
.perf-sec-item{padding:8px 10px;border-radius:10px;border:1px solid transparent;cursor:pointer;transition:background .14s,border-color .14s,transform .14s}
.perf-sec-item:hover{background:rgba(255,255,255,.04);border-color:rgba(255,255,255,.08);transform:translateY(-1px)}
.light .perf-sec-item:hover{background:rgba(0,0,0,.03);border-color:rgba(0,0,0,.08)}
.perf-sec-item.on{background:rgba(56,190,255,.08);border-color:rgba(56,190,255,.22)}
.light .perf-sec-item.on{background:rgba(0,97,168,.06);border-color:rgba(0,97,168,.20)}
.perf-filters{display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin:8px 0 10px}`;

var newCss = `#perf-modal{display:none;position:fixed;inset:0;z-index:99998;align-items:center;justify-content:center}
#perf-modal.open{display:flex}
.perf-ov{position:absolute;inset:0;background:rgba(0,0,0,.72);backdrop-filter:blur(18px);-webkit-backdrop-filter:blur(18px)}
.perf-panel{position:relative;margin:auto;width:min(1380px,98vw);height:min(900px,96vh);border-radius:18px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(160deg,rgba(10,18,30,.98) 0%,rgba(7,13,22,.99) 100%);box-shadow:0 32px 100px rgba(0,0,0,.8),0 0 0 1px rgba(255,255,255,.04);display:flex;flex-direction:column;overflow:hidden}
.light .perf-panel{background:linear-gradient(160deg,#f8fafc 0%,#edf2f7 100%);border-color:rgba(0,0,0,.10)}
/* header */
.perf-hd{display:flex;align-items:center;gap:14px;padding:0 20px;height:56px;flex-shrink:0;border-bottom:1px solid rgba(255,255,255,.07);background:rgba(255,255,255,.02)}
.light .perf-hd{border-bottom-color:rgba(0,0,0,.08);background:rgba(0,0,0,.02)}
.perf-title{font-family:var(--fh);font-size:12px;font-weight:900;letter-spacing:2.5px;color:var(--t0);text-transform:uppercase}
.perf-sub{font-size:10px;color:var(--t2);font-weight:600;padding:3px 10px;border-radius:20px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08)}
.light .perf-sub{background:rgba(0,0,0,.04);border-color:rgba(0,0,0,.08)}
.perf-x{margin-left:auto;width:32px;height:32px;border-radius:10px;border:1px solid rgba(255,255,255,.10);background:transparent;color:var(--t2);cursor:pointer;font-size:18px;line-height:1;display:flex;align-items:center;justify-content:center;transition:all .15s}
.perf-x:hover{background:rgba(239,68,68,.15);border-color:rgba(239,68,68,.3);color:#ef4444}
/* body */
.perf-bd{padding:16px 20px 20px;overflow:auto;min-height:0;display:flex;flex-direction:column;gap:14px;flex:1}
/* KPI cards */
.perf-sum{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;flex-shrink:0}
.perf-card{border:1px solid rgba(255,255,255,.07);background:rgba(255,255,255,.03);border-radius:12px;padding:14px 16px;position:relative;overflow:hidden;transition:border-color .15s}
.perf-card:hover{border-color:rgba(255,255,255,.14)}
.perf-card::after{content:'';position:absolute;inset:0;border-radius:12px;background:linear-gradient(135deg,rgba(255,255,255,.03) 0%,transparent 60%);pointer-events:none}
.light .perf-card{background:rgba(255,255,255,.85);border-color:rgba(0,0,0,.08)}
.perf-k{font-size:8px;color:var(--t2);letter-spacing:1.5px;font-weight:800;text-transform:uppercase;margin-bottom:8px}
.perf-v{font-size:26px;font-weight:900;font-variant-numeric:tabular-nums;color:var(--t0);line-height:1}
.perf-v.pos{color:var(--go)}.perf-v.neg{color:var(--st)}.perf-v.mid{color:var(--am)}
/* main grid */
.perf-grid{display:grid;grid-template-columns:280px 1fr;gap:14px;min-height:0;flex:1}
/* section sidebar */
.perf-sec{border:1px solid rgba(255,255,255,.07);background:rgba(255,255,255,.02);border-radius:12px;padding:14px;display:flex;flex-direction:column;gap:2px;overflow-y:auto}
.light .perf-sec{background:rgba(255,255,255,.6);border-color:rgba(0,0,0,.07)}
.perf-sec-h{display:flex;align-items:baseline;justify-content:space-between;margin-bottom:10px;padding-bottom:8px;border-bottom:1px solid rgba(255,255,255,.07)}
.light .perf-sec-h{border-bottom-color:rgba(0,0,0,.07)}
.perf-sec-h b{font-size:9px;letter-spacing:1.5px;text-transform:uppercase;color:var(--t1);font-weight:800}
.perf-sec-h span{font-size:8px;color:var(--t2)}
.perf-sec-row{display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid rgba(255,255,255,.05);font-size:9px}
.light .perf-sec-row{border-bottom-color:rgba(0,0,0,.05)}
.perf-sec-row:last-child{border:none}
.perf-sec-row .l{color:var(--t1);font-weight:700}
.perf-sec-row .r{font-variant-numeric:tabular-nums;color:var(--t0);font-weight:800}
.perf-sec-item{padding:10px 12px;border-radius:10px;border:1px solid transparent;cursor:pointer;transition:all .14s}
.perf-sec-item:hover{background:rgba(255,255,255,.04);border-color:rgba(255,255,255,.08)}
.light .perf-sec-item:hover{background:rgba(0,0,0,.03);border-color:rgba(0,0,0,.08)}
.perf-sec-item.on{background:rgba(56,190,255,.07);border-color:rgba(56,190,255,.2)}
.light .perf-sec-item.on{background:rgba(0,97,168,.05);border-color:rgba(0,97,168,.18)}
.perf-filters{display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin:10px 0 12px}`;

if(html.indexOf(oldCss) !== -1){
  html = html.replace(oldCss, newCss);
  console.log('CSS replaced OK');
} else {
  console.log('CSS marker not found exactly, trying partial...');
  // Replace just the perf-panel and perf-hd lines
  html = html.replace(
    '.perf-panel{position:relative;margin:auto;width:min(1100px,96vw);height:min(760px,92vh);border-radius:12px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(180deg,rgba(18,29,44,.96),rgba(13,21,32,.98));box-shadow:0 18px 70px rgba(0,0,0,.55);display:flex;flex-direction:column;overflow:hidden}',
    '.perf-panel{position:relative;margin:auto;width:min(1380px,98vw);height:min(900px,96vh);border-radius:18px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(160deg,rgba(10,18,30,.98) 0%,rgba(7,13,22,.99) 100%);box-shadow:0 32px 100px rgba(0,0,0,.8);display:flex;flex-direction:column;overflow:hidden}'
  );
  console.log('Did partial CSS patch');
}

// ── 2. Also update perf-table styles ──────────────────────────────────────
html = html.replace(
  '.perf-table{width:100%;border-collapse:collapse;font-size:9px}',
  '.perf-table{width:100%;border-collapse:collapse;font-size:10px}'
);
html = html.replace(
  '.perf-table th{position:sticky;top:0;background:rgba(0,0,0,.35);backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);padding:8px 10px;text-align:left;color:var(--t2);letter-spacing:1px;text-transform:uppercase;border-bottom:1px solid rgba(255,255,255,.10)}',
  '.perf-table th{position:sticky;top:0;background:rgba(7,15,26,.95);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);padding:10px 12px;text-align:left;color:var(--t2);letter-spacing:1.2px;text-transform:uppercase;border-bottom:1px solid rgba(255,255,255,.08);font-size:9px;font-weight:800}'
);
html = html.replace(
  '.perf-table td{padding:8px 10px;border-bottom:1px solid rgba(255,255,255,.06);vertical-align:top}',
  '.perf-table td{padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.05);vertical-align:middle}'
);
console.log('Table CSS updated');

// ── 3. Replace modal HTML (lines 6251-6375) ───────────────────────────────
var oldModal = `<!-- DAY PERFORMANCE modal (full screen) -->
<div id="perf-modal" role="dialog" aria-modal="true" aria-label="Day performance" onclick="if(event.target && event.target.id==='perf-modal') closeDayPerf()">
  <div class="perf-ov"></div>
  <div class="perf-panel" style="width:min(1200px,97vw);height:min(820px,94vh)">
    <div class="perf-hd">
      <div class="perf-title">DAY PERFORMANCE</div>
      <div class="perf-sub" id="perf-sub">—</div>
      <button class="perf-x" onclick="closeDayPerf()" title="Close">×</button>
    </div>
    <div class="perf-bd">

      <!-- Sub-tab switcher -->
      <div class="dp-tabs">
        <button class="dp-tab on go" id="dp-tab-signals" onclick="dpSwitchTab('signals')">📊 All Signals</button>
        <button class="dp-tab" id="dp-tab-adv" onclick="dpSwitchTab('adv')">⚡ SPIKE HUNT · NIFTY 200</button>
      </div>

      <!-- ── PANE 1: ALL SIGNALS (existing) ── -->
      <div class="dp-pane on" id="dp-pane-signals">
        <div class="perf-sum">
          <div class="perf-card"><div class="perf-k">Total P&amp;L (₹ · 1L model)</div><div class="perf-v" id="perf-total">—</div></div>
          <div class="perf-card"><div class="perf-k">Signals</div><div class="perf-v" id="perf-signals">—</div></div>
          <div class="perf-card"><div class="perf-k">Win rate</div><div class="perf-v" id="perf-wr">—</div></div>
          <div class="perf-card"><div class="perf-k">Open (MTM)</div><div class="perf-v" id="perf-open">—</div></div>
        </div>
        <div class="perf-grid" style="flex:1;min-height:0">
          <div class="perf-sec">
            <div class="perf-sec-h"><b>By section</b><span>Where alerts came from</span></div>
            <div id="perf-sections"></div>
          </div>
          <div class="perf-sec" style="min-height:260px;overflow:auto">
            <div class="perf-sec-h"><b>All alerts</b><span><button class="perf-dlbtn" onclick="perfDownloadCSV()">DOWNLOAD CSV</button> <span id="perf-table-sub">—</span></span></div>
            <div id="perf-outcome-filters" class="perf-filters"></div>
            <div style="display:flex;justify-content:flex-end;margin:-2px 0 8px" id="perf-validation"></div>
            <table class="perf-table">
              <thead>
                <tr>
                  <th style="width:70px">Time</th>
                  <th style="width:110px">Section</th>
                  <th style="width:90px">Symbol</th>
                  <th style="width:70px">Dir</th>
                  <th>Trigger</th>
                  <th style="width:80px;text-align:right">Entry</th>
                  <th style="width:70px;text-align:right">SL</th>
                  <th style="width:80px;text-align:right">Target</th>
                  <th style="width:80px;text-align:right">LTP/Exit</th>
                  <th style="width:76px;text-align:right">Hit Time</th>
                  <th style="width:88px;text-align:right">P&amp;L</th>
                  <th style="width:90px">Outcome</th>
                </tr>
              </thead>
              <tbody id="perf-tbody"></tbody>
            </table>
          </div>
        </div>
      </div>`;

var newModal = `<!-- DAY PERFORMANCE modal (full screen) -->
<div id="perf-modal" role="dialog" aria-modal="true" aria-label="Day performance" onclick="if(event.target&&event.target.id==='perf-modal')closeDayPerf()">
  <div class="perf-ov"></div>
  <div class="perf-panel">

    <!-- ══ HEADER ══ -->
    <div class="perf-hd">
      <!-- Logo mark -->
      <div style="width:32px;height:32px;border-radius:9px;background:linear-gradient(135deg,rgba(16,185,129,.25),rgba(99,102,241,.15));border:1px solid rgba(16,185,129,.25);display:flex;align-items:center;justify-content:center;font-size:14px;flex-shrink:0">📊</div>
      <div>
        <div class="perf-title">DAY PERFORMANCE</div>
      </div>
      <!-- Date nav -->
      <div id="perf-date-nav" style="display:flex;align-items:center;gap:6px;margin-left:12px">
        <button onclick="dpNavDate(-1)" title="Previous day" style="width:26px;height:26px;border-radius:7px;border:1px solid rgba(255,255,255,.1);background:transparent;color:var(--t2);cursor:pointer;font-size:13px;display:flex;align-items:center;justify-content:center;transition:all .14s" onmouseover="this.style.background='rgba(255,255,255,.06)'" onmouseout="this.style.background='transparent'">‹</button>
        <input type="date" id="perf-date-picker" onchange="dpLoadDate(this.value)" style="font-size:10px;font-weight:700;padding:4px 10px;border-radius:8px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.05);color:var(--t0);font-family:var(--fn);cursor:pointer;outline:none">
        <button onclick="dpNavDate(1)" title="Next day" style="width:26px;height:26px;border-radius:7px;border:1px solid rgba(255,255,255,.1);background:transparent;color:var(--t2);cursor:pointer;font-size:13px;display:flex;align-items:center;justify-content:center;transition:all .14s" onmouseover="this.style.background='rgba(255,255,255,.06)'" onmouseout="this.style.background='transparent'">›</button>
        <button onclick="dpLoadDate('today')" style="font-size:9px;font-weight:800;padding:4px 9px;border-radius:7px;border:1px solid rgba(16,185,129,.3);background:rgba(16,185,129,.1);color:var(--go);cursor:pointer;letter-spacing:.5px">TODAY</button>
      </div>
      <div class="perf-sub" id="perf-sub" style="margin-left:8px">—</div>
      <!-- Sub-tabs in header -->
      <div style="margin-left:auto;display:flex;gap:3px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:10px;padding:3px">
        <button class="dp-tab on go" id="dp-tab-signals" onclick="dpSwitchTab('signals')" style="border-radius:7px;padding:5px 12px;font-size:9px;border:none;cursor:pointer;font-family:var(--fn);font-weight:800;letter-spacing:.5px;transition:all .14s">📊 Signals</button>
        <button class="dp-tab" id="dp-tab-adv" onclick="dpSwitchTab('adv')" style="border-radius:7px;padding:5px 12px;font-size:9px;border:none;cursor:pointer;font-family:var(--fn);font-weight:800;letter-spacing:.5px;transition:all .14s">⚡ SPIKE HUNT</button>
      </div>
      <button class="perf-x" onclick="closeDayPerf()" title="Close">✕</button>
    </div>

    <!-- ══ BODY ══ -->
    <div class="perf-bd">

      <!-- ── PANE 1: ALL SIGNALS ── -->
      <div class="dp-pane on" id="dp-pane-signals">

        <!-- KPI strip -->
        <div class="perf-sum">
          <div class="perf-card" style="border-color:rgba(16,185,129,.2)">
            <div class="perf-k">Total P&L · 1L Model</div>
            <div class="perf-v pos" id="perf-total">—</div>
            <div style="margin-top:6px;height:3px;border-radius:2px;background:rgba(16,185,129,.15)"><div id="perf-pnl-bar" style="height:100%;width:0%;background:var(--go);border-radius:2px;transition:width .6s"></div></div>
          </div>
          <div class="perf-card" style="border-color:rgba(99,102,241,.2)">
            <div class="perf-k">Signals Fired</div>
            <div class="perf-v" id="perf-signals" style="color:var(--bl)">—</div>
            <div style="margin-top:6px;font-size:9px;color:var(--t2)" id="perf-sig-sub">—</div>
          </div>
          <div class="perf-card" style="border-color:rgba(245,158,11,.2)">
            <div class="perf-k">Win Rate</div>
            <div class="perf-v mid" id="perf-wr">—</div>
            <div style="margin-top:6px;height:3px;border-radius:2px;background:rgba(245,158,11,.12)"><div id="perf-wr-bar" style="height:100%;width:0%;background:var(--am);border-radius:2px;transition:width .6s"></div></div>
          </div>
          <div class="perf-card" style="border-color:rgba(56,190,255,.2)">
            <div class="perf-k">Open / MTM Live</div>
            <div class="perf-v" id="perf-open" style="color:var(--bl)">—</div>
            <div style="margin-top:6px;font-size:9px;color:var(--t2)" id="perf-open-sub">positions running</div>
          </div>
        </div>

        <!-- Main 2-col layout -->
        <div class="perf-grid" style="flex:1;min-height:0">

          <!-- LEFT: section sidebar -->
          <div class="perf-sec" style="overflow-y:auto">
            <div class="perf-sec-h">
              <b>By Section</b>
              <span style="font-size:8px;color:var(--t2)">click to filter</span>
            </div>
            <div id="perf-sections"></div>
          </div>

          <!-- RIGHT: alerts table -->
          <div class="perf-sec" style="overflow:hidden;display:flex;flex-direction:column;gap:10px">
            <!-- Filter row -->
            <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;flex-shrink:0">
              <div style="display:flex;align-items:center;gap:5px">
                <span style="font-size:9px;font-weight:800;letter-spacing:1px;color:var(--t1);text-transform:uppercase">Alerts</span>
                <span style="font-size:9px;color:var(--t2)" id="perf-table-sub">—</span>
              </div>
              <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">
                <div id="perf-outcome-filters" class="perf-filters" style="margin:0"></div>
                <div id="perf-validation"></div>
                <button onclick="perfDownloadCSV()" style="display:inline-flex;align-items:center;gap:5px;padding:5px 10px;border-radius:8px;border:1px solid rgba(99,102,241,.3);background:rgba(99,102,241,.1);color:#818cf8;font-size:9px;font-weight:800;cursor:pointer;letter-spacing:.5px;transition:all .14s" onmouseover="this.style.background='rgba(99,102,241,.2)'" onmouseout="this.style.background='rgba(99,102,241,.1)'">⬇ CSV</button>
              </div>
            </div>
            <!-- Table -->
            <div style="flex:1;min-height:0;overflow:auto;border-radius:10px;border:1px solid rgba(255,255,255,.07)">
              <table class="perf-table">
                <thead>
                  <tr>
                    <th style="width:64px">Time</th>
                    <th style="width:96px">Section</th>
                    <th style="width:88px">Symbol</th>
                    <th style="width:62px">Dir</th>
                    <th style="min-width:120px">Trigger</th>
                    <th style="width:82px;text-align:right">Entry</th>
                    <th style="width:72px;text-align:right">SL</th>
                    <th style="width:82px;text-align:right">Target</th>
                    <th style="width:82px;text-align:right">LTP/Exit</th>
                    <th style="width:72px;text-align:right">Hit Time</th>
                    <th style="width:90px;text-align:right">P&amp;L</th>
                    <th style="width:96px">Outcome</th>
                  </tr>
                </thead>
                <tbody id="perf-tbody"></tbody>
              </table>
            </div>
          </div>

        </div>
      </div>`;

if(html.indexOf(oldModal) !== -1){
  html = html.replace(oldModal, newModal);
  console.log('Modal HTML replaced OK');
} else {
  console.log('WARN: oldModal marker not found exactly');
}

// ── 4. Inject dpNavDate + dpLoadDate JS ────────────────────────────────────
if(html.indexOf('function dpNavDate(') === -1){
  var jsMarker = 'function closeDayPerf(){';
  var dpJs = `function dpNavDate(dir){
  var picker = document.getElementById('perf-date-picker');
  if(!picker || !picker.value) return;
  var d = new Date(picker.value + 'T00:00:00');
  d.setDate(d.getDate() + dir);
  // Skip weekends
  while(d.getDay()===0||d.getDay()===6) d.setDate(d.getDate()+dir);
  var iso = d.toISOString().slice(0,10);
  picker.value = iso;
  dpLoadDate(iso);
}
function dpLoadDate(dateStr){
  var today = new Date().toISOString().slice(0,10);
  if(dateStr==='today') dateStr = today;
  var picker = document.getElementById('perf-date-picker');
  if(picker) picker.value = dateStr;
  loadDayPerf(dateStr);
}
`;
  html = html.replace(jsMarker, dpJs + jsMarker);
  console.log('dpNavDate JS injected');
}

// ── 5. Update openDayPerf to set picker date ──────────────────────────────
html = html.replace(
  'function openDayPerf(){\n  const m = document.getElementById(\'perf-modal\');\n  if(!m) return;\n  m.classList.add(\'open\');\n  loadDayPerf();\n  advDpRender(); // render SPIKE HUNT (NIFTY 200) from live log\n}',
  `function openDayPerf(dateStr){\n  const m = document.getElementById('perf-modal');\n  if(!m) return;\n  m.classList.add('open');\n  var today = new Date().toISOString().slice(0,10);\n  var d = dateStr || today;\n  var picker = document.getElementById('perf-date-picker');\n  if(picker) picker.value = d;\n  loadDayPerf(d===today ? undefined : d);\n  advDpRender();\n}`
);
console.log('openDayPerf updated');

// ── 6. Update dp-tab CSS to work inside the header pill ───────────────────
html = html.replace(
  '.dp-tab{flex:1;padding:8px 12px;font-family:var(--fn);font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;border:none;cursor:pointer;background:transparent;color:var(--t2);border-bottom:2px solid transparent;margin-bottom:-2px;transition:color .15s,border-color .15s}',
  '.dp-tab{padding:5px 12px;font-family:var(--fn);font-size:9px;font-weight:800;letter-spacing:.5px;text-transform:uppercase;border:none;cursor:pointer;background:transparent;color:var(--t2);transition:all .15s}'
);
html = html.replace(
  '.dp-tab.on,.dp-tab.go.on{color:var(--go);border-bottom-color:var(--go)}',
  '.dp-tab.on{color:var(--t0);background:rgba(255,255,255,.08)} .dp-tab.go.on{color:var(--go)}'
);
console.log('dp-tab CSS updated');

fs.writeFileSync(fp, html, 'utf8');
console.log('Done. Lines:', html.split('\n').length);
