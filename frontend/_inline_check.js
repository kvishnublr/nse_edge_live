
'use strict';

// ═══════════════════════════════════════════════
// GLOBAL STATE
// ═══════════════════════════════════════════════
const gState={1:'go',2:'go',3:'go',4:'wt',5:'st'};
const gScore={1:92,2:78,3:84,4:42,5:26};
const gCyc=['go','am','st','wt'];
let curMode='intraday';
let curVIX='lo';
let strikes=0;
let trades=[];
let stkFil='all';
let stkGateFil='all';

// ═══════════════════════════════════════════════
// CLOCK + ZONE
// ═══════════════════════════════════════════════
function tick(){
  const ist=new Date(new Date().toLocaleString('en-US',{timeZone:'Asia/Kolkata'}));
  const h=String(ist.getHours()).padStart(2,'0');
  const m=String(ist.getMinutes()).padStart(2,'0');
  const s=String(ist.getSeconds()).padStart(2,'0');
  document.getElementById('clk').textContent=h+':'+m+':'+s+' IST';
  const mins=ist.getHours()*60+ist.getMinutes();
  let zt,zc;
  if(mins<555||mins>=930){zt='MARKET CLOSED';zc='var(--t2)'}
  else if(mins<600){zt='DISCOVERY 9:15–10:00';zc='var(--am)'}
  else if(mins<810){zt='✦ TREND ZONE ACTIVE';zc='var(--go)'}
  else if(mins<870){zt='DRIFT 13:30–14:30';zc='var(--t2)'}
  else{zt='EXPIRY ZONE';zc='var(--st)'}
  const zl=document.getElementById('zlbl');
  const cz=document.getElementById('curzone');
  zl.textContent='ZONE: '+zt;zl.style.color=zc;
  if(cz && !window._histMode){cz.textContent=zt;cz.style.color=zc}
  if(!window._histMode && typeof _refreshPlaybookSummary === 'function') _refreshPlaybookSummary();
}
setInterval(tick,1000);tick();

// DATE
document.getElementById('pb-date').textContent=
  new Date().toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'2-digit'}).toUpperCase();

// ═══════════════════════════════════════════════
// MODE DATA — INTRADAY + POSITIONAL
// ═══════════════════════════════════════════════
const MODES={
  intraday:{
    g:[
      {nm:'REGIME',st:'go',sc:92,rows:[
        {k:'Daily EMA20',v:'ABOVE ▲ (3 sessions)',c:'cg'},
        {k:'India VIX',v:'13.2 — LOW regime',c:'cg'},
        {k:'FII 5-day flow',v:'+₹14,200 Cr cumulative',c:'cg'},
        {k:'SGX Nifty',v:'+0.38% pre-market',c:'cg'},
        {k:'Regime verdict',v:'BULL TREND INTACT ✓',c:'cg'}
      ]},
      {nm:'SMART MONEY',st:'go',sc:78,rows:[
        {k:'Nifty PCR',v:'1.38 — bullish zone',c:'cg'},
        {k:'OI net change',v:'+18,640 long contracts',c:'cg'},
        {k:'FII index futures',v:'NET LONG +12,480',c:'cg'},
        {k:'Client (retail) OI',v:'Mostly short → contrarian ↑',c:'ca'},
        {k:'Max Pain',v:'24,200 (price 212 pts above)',c:'cm'}
      ]},
      {nm:'STRUCTURE',st:'go',sc:84,rows:[
        {k:'Daily HTF',v:'Higher High + Higher Low ✓',c:'cg'},
        {k:'1H MTF',v:'At daily demand zone',c:'cg'},
        {k:'15m LTF',v:'Consolidating — watch 24,430',c:'ca'},
        {k:'vs VWAP',v:'ABOVE — 24,412 vs 24,380',c:'cg'},
        {k:'Key support',v:'24,350 — 62 pts below',c:'cm'}
      ]},
      {nm:'TRIGGER',st:'wt',sc:42,rows:[
        {k:'Delta (DVPI)',v:'+0.31 — building slowly',c:'ca'},
        {k:'OI build 30m',v:'+2,840 — below 5,000 threshold',c:'ca'},
        {k:'15m structure break',v:'Pending — watch 24,430',c:'cm'},
        {k:'Volume vs avg',v:'0.9× — below 1.5× threshold',c:'cm'},
        {k:'Trigger level',v:'24,430 breakout → enter',c:'cb'}
      ]},
      {nm:'RISK VALID',st:'st',sc:26,rows:[
        {k:'R:R ratio',v:'1:1.4 — need minimum 1:2',c:'cr'},
        {k:'Target',v:'24,510 (too tight vs stop)',c:'cr'},
        {k:'ATR stop (1.5×87)',v:'130 pts → SL 24,282',c:'ca'},
        {k:'VIX allows trade',v:'Yes — 13.2 (full size)',c:'cg'},
        {k:'Daily strikes',v:'0 / 3 — clean session',c:'cg'}
      ]}
    ],
    pb:{bias:{v:'BULLISH',c:'cg'},conf:{v:'MODERATE',c:'ca',s:'3 / 5 gates'},
      al:{k:'ALLOWED',v:'LONGS ONLY',s:'No shorts today'},
      wn:{k:'BEST WINDOW',v:'10:00 – 13:30',s:'Trend kill zone'}},
    picks:[
      {sym:'INDUSINDBK',cls:'rpk-go',score:'88 ★',meta:'Banking · OI +19.8% · PCR 1.63 · Vol 2.6×',reason:'R:R 1:2.6 · 5/5 gates · LONG BUILDUP',rc:'cg',tab:'stocks',entry:'1,315',sl:'1,289',target:'1,383',rr:'2.6',conf:'CONFIRMED',time:'10:12'},
      {sym:'ICICIBANK',cls:'rpk-go',score:'88 ★',meta:'Banking · OI +18.2% · PCR 1.62 · Vol 2.8×',reason:'R:R 1:2.4 · 5/5 gates · FII buying confirmed',rc:'cg',tab:'stocks',entry:'1,221',sl:'1,196',target:'1,281',rr:'2.4',conf:'CONFIRMED',time:'10:18'},
      {sym:'SBIN',cls:'rpk-go',score:'83 ★',meta:'PSU Bank · OI +16.4% · PCR 1.50 · Vol 2.4×',reason:'R:R 1:2.3 · 4/5 gates · sector tailwind',rc:'ca',tab:'stocks',entry:'822',sl:'800',target:'873',rr:'2.3',conf:'HIGH CONF',time:'10:34'},
    ],
    avoid:'RELIANCE · TATASTEEL · INFY — short OI buildup',
    kz:[
      {cls:'am',t:'9:15–10:00 DISCOVERY',d:'Watch only. Mark ORB high / low. No entries. Algo traps active. Spreads are wide.'},
      {cls:'go',t:'10:00–13:30 ✦ TREND (BEST)',d:'All 5 gates must pass. Full position size. Momentum entries only.'},
      {cls:'dm',t:'13:30–14:30 DRIFT',d:'No new entries. Manage open trades. Book partial profits.'},
      {cls:'st',t:'14:30–15:30 EXPIRY',d:'Exit all. OI distortion. Max Pain gravity at 24,200.'}
    ]
  },
  positional:{
    g:[
      {nm:'WEEKLY REGIME',st:'go',sc:88,rows:[
        {k:'Weekly EMA20',v:'ABOVE ▲ (3 weeks)',c:'cg'},
        {k:'VIX weekly trend',v:'13.2 — trending lower',c:'cg'},
        {k:'FII 4-week cum.',v:'+₹14,840 Cr net buyer',c:'cg'},
        {k:'Nifty 50-week EMA',v:'ABOVE — macro bull intact',c:'cg'},
        {k:'Weekly regime',v:'BULL TREND ✓',c:'cg'}
      ]},
      {nm:'INSTITUTIONAL FLOW',st:'go',sc:82,rows:[
        {k:'FII cash 4-week',v:'+₹14,840 Cr strong buyer',c:'cg'},
        {k:'DII cash 4-week',v:'+₹8,240 Cr supportive',c:'cg'},
        {k:'FII index futures',v:'Net long +12,480 contracts',c:'cg'},
        {k:'Rollover OI',v:'Long positions held >80%',c:'cg'},
        {k:'Sector rotation',v:'Banking + Infra leading',c:'ca'}
      ]},
      {nm:'WEEKLY STRUCTURE',st:'go',sc:80,rows:[
        {k:'Monthly HTF',v:'Higher high confirmed',c:'cg'},
        {k:'Weekly MTF',v:'HH + HL structure intact',c:'cg'},
        {k:'Daily LTF',v:'At weekly demand zone',c:'ca'},
        {k:'200-week EMA',v:'Price 18% above — healthy',c:'cg'},
        {k:'Weekly support',v:'23,800 — 2.5% buffer',c:'cm'}
      ]},
      {nm:'CATALYST',st:'go',sc:74,rows:[
        {k:'Fundamental trigger',v:'RBI rate cut cycle ✓',c:'cg'},
        {k:'Nifty EPS growth',v:'+14% YoY — earnings support',c:'cg'},
        {k:'Sector catalyst',v:'Banking NIM expansion',c:'cg'},
        {k:'Event calendar',v:'No major risk next 10 days',c:'cg'},
        {k:'Delivery % stocks',v:'58–62% — institutional',c:'cg'}
      ]},
      {nm:'RISK VALID',st:'st',sc:38,rows:[
        {k:'R:R ratio',v:'1:1.8 — need minimum 1:3',c:'cr'},
        {k:'Weekly target',v:'24,800 (tight for swing)',c:'cr'},
        {k:'Structure stop',v:'Below 23,800 weekly HL',c:'ca'},
        {k:'Position size',v:'VIX 13 → full size OK',c:'cg'},
        {k:'Expiry risk',v:'Hold through — reduce 20%',c:'ca'}
      ]}
    ],
    pb:{bias:{v:'BULLISH',c:'cg'},conf:{v:'MODERATE',c:'ca',s:'4 / 5 gates'},
      al:{k:'ALLOWED',v:'LONGS + HOLD',s:'Swing 5–15 days'},
      wn:{k:'ENTRY STYLE',v:'DAILY CLOSE',s:'Enter near weekly zone'}},
    picks:[
      {sym:'ICICIBANK',cls:'rpk-go',score:'91',meta:'Pullback · Banking · Hold 4–6d · Results catalyst',reason:'R:R 1:2.8 · 5/5 gates · strong institutional flow',rc:'cg',tab:'swing',entry:'1,220',sl:'1,185',target:'1,338',rr:'2.8',conf:'CONFIRMED',time:'09:45'},
      {sym:'SBIN',cls:'rpk-go',score:'87',meta:'Breakout · PSU Bank · Hold 5–8d · Budget allocation',reason:'R:R 1:2.5 · 4/5 gates · delivery % rising',rc:'cg',tab:'swing',entry:'820',sl:'798',target:'875',rr:'2.5',conf:'CONFIRMED',time:'10:05'},
      {sym:'LT',cls:'rpk-am',score:'83',meta:'Momentum · Infra · Hold 6–10d · Capex cycle',reason:'R:R 1:2.1 · 4/5 gates · watch G5 R:R',rc:'ca',tab:'swing',entry:'3,620',sl:'3,548',target:'3,772',rr:'2.1',conf:'HIGH CONF',time:'10:22'},
    ],
    avoid:'RELIANCE · TATASTEEL · FMCG — weak relative strength',
    kz:[
      {cls:'go',t:'WEEKLY ENTRY ZONE',d:'Enter when price pulls back to weekly demand. Confirm with daily close above zone.'},
      {cls:'am',t:'POSITION MANAGEMENT',d:'Trail stop to last weekly HL after 50% of target reached. Add on dips.'},
      {cls:'go',t:'HOLD RULES',d:'Hold minimum 5 days unless weekly structure breaks below entry stop.'},
      {cls:'st',t:'EXIT TRIGGERS',d:'Weekly structure break · FII net seller 3 days · Catalyst reverses.'}
    ]
  }
};

// ═══════════════════════════════════════════════
// MODE SWITCH
// ═══════════════════════════════════════════════
function renderHistPicksHtml(picks, mode) {
  const rpp = document.getElementById('rp-picks');
  if(!rpp || !picks || !picks.length) return;
  const isPos = mode === 'positional';
  // Positional: prefer higher R:R picks; intraday: all picks
  const filtered = isPos ? picks.filter(p => parseFloat(p.rr_p || p.rr) >= 2.5) : picks;
  const list = filtered.length ? filtered : picks;
  rpp.innerHTML = list.map(p => {
    const vc  = p.verdict==='EXECUTE' ? 'var(--go)' : p.verdict==='WATCH' ? 'var(--am)' : 'var(--t2)';
    const bdc = p.verdict==='EXECUTE' ? 'bg' : p.verdict==='WATCH' ? 'ba' : 'bm';
    const confStyle = p.conf==='CONFIRMED'
      ? 'color:var(--go);border:1px solid rgba(0,232,122,.35);background:rgba(0,232,122,.10)'
      : p.conf==='HIGH CONF'
        ? 'color:var(--bl);border:1px solid rgba(56,190,255,.35);background:rgba(56,190,255,.10)'
        : 'color:var(--am);border:1px solid rgba(255,183,0,.35);background:rgba(255,183,0,.10)';
    // Use mode-specific target and RR
    const tgt    = isPos ? (p.target_p || p.target) : p.target;
    const rr     = isPos ? (p.rr_p     || p.rr)     : p.rr;
    const reason = isPos ? (p.reason_p || p.reason)  : p.reason;
    const levelsHtml = (p.entry && p.sl && tgt)
      ? `<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:3px;margin:5px 0 3px">
          <div style="background:rgba(255,255,255,.04);border-radius:2px;padding:3px 4px;text-align:center">
            <div style="font-size:7px;color:var(--t2);letter-spacing:.5px">ENTRY</div>
            <div style="font-size:10px;font-weight:700;color:var(--t0)">${p.entry}</div>
          </div>
          <div style="background:rgba(255,51,85,.08);border-radius:2px;padding:3px 4px;text-align:center">
            <div style="font-size:7px;color:var(--t2);letter-spacing:.5px">SL</div>
            <div style="font-size:10px;font-weight:700;color:var(--st)">${p.sl}</div>
          </div>
          <div style="background:rgba(0,232,122,.08);border-radius:2px;padding:3px 4px;text-align:center">
            <div style="font-size:7px;color:var(--t2);letter-spacing:.5px">TARGET</div>
            <div style="font-size:10px;font-weight:700;color:var(--go)">${tgt}</div>
          </div>
        </div>` : '';
    const outcomeMap = {
      PROFIT:       {label:'✓ PROFIT',       col:'var(--go)', bg:'rgba(0,232,122,.12)', border:'rgba(0,232,122,.35)'},
      LOSS:         {label:'✗ LOSS',          col:'var(--st)', bg:'rgba(255,51,85,.12)',  border:'rgba(255,51,85,.35)'},
      NOT_EXECUTED: {label:'— NOT EXECUTED',  col:'var(--t2)', bg:'rgba(255,255,255,.05)',border:'rgba(255,255,255,.12)'},
      NO_DATA:      {label:'? NO DATA',        col:'var(--t3)', bg:'transparent',          border:'transparent'},
      UNKNOWN:      {label:'? UNKNOWN',        col:'var(--t3)', bg:'transparent',          border:'transparent'},
    };
    const oc = p.outcome && outcomeMap[p.outcome];
    const outcomeHtml = oc && p.outcome !== 'NO_DATA' && p.outcome !== 'UNKNOWN'
      ? `<div style="margin:4px 0 2px;padding:3px 7px;border-radius:3px;border:1px solid ${oc.border};background:${oc.bg};font-size:8px;font-weight:700;color:${oc.col};letter-spacing:.5px;text-align:center">${oc.label}</div>`
      : '';
    const footerHtml = `<div style="display:flex;align-items:center;gap:5px;margin-top:4px">
      ${p.conf ? `<span style="font-size:7.5px;font-weight:700;padding:1px 5px;border-radius:2px;letter-spacing:.4px;${confStyle}">${p.conf}</span>` : ''}
      ${rr ? `<span style="font-size:8px;color:var(--t2)">R:R 1:${rr}</span>` : ''}
      ${p.signal_time ? `<span style="font-size:8px;color:var(--t2);margin-left:auto">⏱ ${p.signal_time}</span>` : ''}
    </div>`;
    return `<div class="rpk ${p.cls||'rpk-am'}">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:2px">
        <span class="rpkn tv-link" data-tvsym="${p.sym}" style="color:${vc}">${p.sym}</span>
        <span class="bd ${bdc}">${p.score||0}</span>
      </div>
      <div class="rpkm">${p.meta||''}</div>
      ${levelsHtml}
      <div class="rpkr" style="color:${vc};margin-bottom:0">${reason||''}</div>
      ${outcomeHtml}
      ${footerHtml}
    </div>`;
  }).join('');
}

function setMode(m){
  curMode=m;
  document.getElementById('bi').classList.toggle('on',m==='intraday');
  document.getElementById('bp').classList.toggle('on',m==='positional');
  const d=MODES[m];

  // gate data
  d.g.forEach((g,i)=>{
    const n=i+1;
    gState[n]=g.st;gScore[n]=g.sc;
    document.getElementById('gn'+n).textContent=g.nm;
    document.getElementById('gbody'+n).innerHTML=g.rows.map(r=>
      '<div class="gm"><span class="gk">'+r.k+'</span><span class="gv '+r.c+'">'+r.v+'</span></div>'
    ).join('');
  });
  applyGates();updateVerdict();

  // playbook
  const pb=d.pb;
  document.getElementById('pb-bias').textContent=pb.bias.v;
  document.getElementById('pb-bias').className='pbv '+pb.bias.c;
  document.getElementById('pb-mode').textContent=m==='intraday'?'Intraday':'Positional';
  document.getElementById('pb-conf').textContent=pb.conf.v;
  document.getElementById('pb-conf').className='pbv '+pb.conf.c;
  document.getElementById('pb-al-k').textContent=pb.al.k;
  document.getElementById('pb-al-v').textContent=pb.al.v;
  document.getElementById('pb-al-s').textContent=pb.al.s;
  document.getElementById('pb-wn-k').textContent=pb.wn.k;
  document.getElementById('pb-wn-v').textContent=pb.wn.v;
  document.getElementById('pb-wn-s').textContent=pb.wn.s;
  if(!window._histMode) _refreshPlaybookSummary();

  // right panel header + picks
  if(window._histMode) {
    // In historical mode: update header and re-render cached hist picks filtered by mode
    const rpHd2 = document.getElementById('rp-hd');
    const rpBdg2 = rpHd2 && rpHd2.nextElementSibling;
    const hdDate = window._histDate || '';
    if(rpHd2) rpHd2.textContent = (m==='intraday'?'HIST INTRADAY':'HIST POSITIONAL') + (hdDate?' — '+hdDate:'');
    if(rpBdg2) { rpBdg2.textContent='HIST'; rpBdg2.className='bd bm'; }
    const rpp2 = document.getElementById('rp-picks');
    const picks2 = window._histData && window._histData.stock_picks;
    if(picks2 && picks2.length) {
      renderHistPicksHtml(picks2, m);
      // Also re-render Swing Radar with mode-correct targets
      const _isPos2 = m === 'positional';
      const _secM2 = {HDFCBANK:'Banking',ICICIBANK:'Banking',AXISBANK:'Banking',KOTAKBANK:'Banking',
        INDUSINDBK:'Banking',SBIN:'PSU Bank',BANKNIFTY:'Index',TCS:'IT',INFY:'IT',
        MARUTI:'Auto',TATAMOTORS:'Auto',LT:'Infra',BAJFINANCE:'NBFC',RELIANCE:'Energy'};
      const _holdM2 = {Breakout:'3–5d',Pullback:'4–7d',Recovery:'5–8d',Momentum:'4–6d'};
      const _sc2b = t => t==='Breakout'?'color:var(--go);border:1px solid var(--go-b)'
        : t==='Pullback'?'color:var(--am);border:1px solid var(--am-b)'
        : t==='Recovery'?'color:var(--st);border:1px solid var(--st-b)'
        : 'color:var(--bl);border:1px solid var(--bl-b)';
      const _scb = s => s>=80?'var(--go)':s>=65?'var(--am)':'var(--st)';
      const swb = document.getElementById('sw-body');
      if(swb) swb.innerHTML = picks2.map((p, i) => {
        const pr   = parseFloat(p.close)||0;
        const entN = parseFloat(p.entry)||pr;
        const atr  = parseFloat(p.atr)||(pr*0.015);
        const ezLo = Math.round(entN - 0.3*atr).toLocaleString('en-IN');
        const ezHi = Math.round(entN + 0.2*atr).toLocaleString('en-IN');
        const tgt  = Math.round(parseFloat(_isPos2 ? (p.target_p||p.target) : p.target)||0);
        const sl   = Math.round(parseFloat(p.sl)||0);
        const rr   = parseFloat(_isPos2 ? (p.rr_p||p.rr) : p.rr)||0;
        const sc   = p.score||0;
        const ocColor = p.outcome==='PROFIT'?'var(--go)':p.outcome==='LOSS'?'var(--st)':'var(--t2)';
        const ocLabel = p.outcome && p.outcome!=='NO_DATA' && p.outcome!=='UNKNOWN' ? p.outcome.replace('_',' ') : '—';
        return `<tr style="${sc>=80?'background:rgba(0,232,122,.03)':''}">
          <td><span style="color:var(--t2);font-size:8px;margin-right:3px">${i+1}</span><span class="tv-link" data-tvsym="${p.sym}" style="font-weight:600;color:var(--t0)">${p.sym}</span></td>
          <td style="color:var(--t2);font-size:9px">${_secM2[p.sym]||'Market'}</td>
          <td style="font-weight:600">${Math.round(pr).toLocaleString('en-IN')}</td>
          <td><span style="font-size:8px;font-weight:700;padding:1px 6px;border-radius:2px;${_sc2b(p.setup)}">${(p.setup||'—').toUpperCase()}</span></td>
          <td style="color:var(--am)">${ezLo}–${ezHi}</td>
          <td style="color:var(--go);font-weight:600">${tgt.toLocaleString('en-IN')}</td>
          <td style="color:var(--st)">${sl.toLocaleString('en-IN')}</td>
          <td style="color:${rr>=2.5?'var(--go)':rr>=2?'var(--am)':'var(--st)'};font-weight:600">1:${rr.toFixed(1)}</td>
          <td style="color:var(--t2)">—</td><td style="color:var(--t2)">—</td><td style="color:var(--t2)">—</td>
          <td style="color:${ocColor};font-size:9px;font-weight:700">${ocLabel}${p.signal_time?'<br><span style="font-size:7px;color:var(--t2);font-weight:400">⏱ '+p.signal_time+'</span>':''}</td>
          <td style="color:var(--t2)">${_holdM2[p.setup]||'5–8d'}</td>
          <td><span style="font-size:10px;font-weight:700;color:${_scb(sc)}">${sc}</span><div class="swbar"><div class="swbf" style="width:${sc}%;background:${_scb(sc)}"></div></div></td>
        </tr>`;
      }).join('');
    } else if(rpp2) {
      const pc = window._histData && window._histData.gates ? window._histData.gates.pass_count : 0;
      rpp2.innerHTML = `<div style="padding:14px 10px;text-align:center;color:var(--t2);font-size:9px;line-height:1.6">
        No qualifying picks for ${window._histDate||'this date'}<br>
        <span style="color:var(--am)">${pc}/5 gates pass — signal too weak for entries</span>
      </div>`;
    }
  } else {
    document.getElementById('rp-hd').textContent=m==='intraday'?'INTRADAY PICKS':'POSITIONAL PICKS';
    // Fetch live picks from backend
    const rpp = document.getElementById('rp-picks');
    rpp.innerHTML = '<div style="padding:12px;text-align:center;color:var(--t2);font-size:9px">Loading live picks...</div>';
    fetch('/api/live-picks').then(r=>r.json()).then(lp => {
      const picks = lp.picks || [];
      const isPos = m === 'positional';
      if(!picks.length) {
        rpp.innerHTML = '<div style="padding:14px;text-align:center;color:var(--t2);font-size:9px">No qualifying picks right now<br><span style="color:var(--am)">Waiting for gate conditions</span></div>';
        return;
      }
      rpp.innerHTML = picks.map(p => {
        const confStyle=p.conf==='CONFIRMED'
          ?'color:var(--go);border:1px solid rgba(0,232,122,.35);background:rgba(0,232,122,.10)'
          :p.conf==='HIGH CONF'
            ?'color:var(--bl);border:1px solid rgba(56,190,255,.35);background:rgba(56,190,255,.10)'
            :'color:var(--am);border:1px solid rgba(255,183,0,.35);background:rgba(255,183,0,.10)';
        const tgt   = isPos ? (p.target_p || p.target) : p.target;
        const rr    = isPos ? (p.rr_p || p.rr) : p.rr;
        const reason= isPos ? (p.reason_p || p.reason) : p.reason;
        const levelsHtml = p.entry
          ?'<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:3px;margin:5px 0 3px">'+
            '<div style="background:rgba(255,255,255,.04);border-radius:2px;padding:3px 4px;text-align:center">'+
            '<div style="font-size:7px;color:var(--t2);letter-spacing:.5px">ENTRY</div>'+
            '<div style="font-size:10px;font-weight:700;color:var(--t0)">'+p.entry+'</div></div>'+
            '<div style="background:rgba(255,51,85,.08);border-radius:2px;padding:3px 4px;text-align:center">'+
            '<div style="font-size:7px;color:var(--t2);letter-spacing:.5px">SL</div>'+
            '<div style="font-size:10px;font-weight:700;color:var(--st)">'+p.sl+'</div></div>'+
            '<div style="background:rgba(0,232,122,.08);border-radius:2px;padding:3px 4px;text-align:center">'+
            '<div style="font-size:7px;color:var(--t2);letter-spacing:.5px">TARGET</div>'+
            '<div style="font-size:10px;font-weight:700;color:var(--go)">'+tgt+'</div></div></div>'
          :'';
        const footerHtml='<div style="display:flex;align-items:center;gap:5px;margin-top:4px">'+
          (p.conf?'<span style="font-size:7.5px;font-weight:700;padding:1px 5px;border-radius:2px;letter-spacing:.4px;'+confStyle+'">'+p.conf+'</span>':'')+
          (rr?'<span style="font-size:8px;color:var(--t2)">R:R 1:'+rr+'</span>':'')+
          '<span style="font-size:8px;color:var(--t2);margin-left:auto">'+p.pc+'/5 gates</span>'+
          '</div>';
        return '<div class="rpk '+p.cls+'">'+
          '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:3px">'+
          '<span class="rpkn tv-link" data-tvsym="'+p.sym+'" style="color:var(--go)">'+p.sym+'</span>'+
          '<span class="bd bg">'+p.score+'</span></div>'+
          '<div class="rpkm">'+p.meta+'</div>'+
          '<div class="rpkr cg" style="margin-bottom:0">'+reason+'</div>'+
          levelsHtml+footerHtml+'</div>';
      }).join('');
    }).catch(()=>{
      rpp.innerHTML = '<div style="padding:14px;text-align:center;color:var(--t2);font-size:9px">Live picks unavailable<br><span style="color:var(--am)">Backend fetch failed - no fallback demo picks shown</span></div>';
    });
  }

  // kill zone cards
  document.getElementById('kzcards').innerHTML=d.kz.map(c=>
    '<div class="zcard '+c.cls+'"><div class="zt">'+c.t+'</div><div class="zd">'+c.d+'</div></div>'
  ).join('');

  notify(m.toUpperCase()+' MODE ACTIVE');
}

// ═══════════════════════════════════════════════
// GATE ENGINE
// ═══════════════════════════════════════════════
function toggleGate(n){document.getElementById('gbody'+n).classList.toggle('open')}

function cycleGate(n){
  const cur=gState[n];
  gState[n]=gCyc[(gCyc.indexOf(cur)+1)%gCyc.length];
  const sm={go:.9,am:.5,st:.25,wt:.45};
  gScore[n]=Math.round((sm[gState[n]]||.5)*85+Math.random()*12);
  applyGates();updateVerdict();
}

function applyGates(){
  const lbl={go:'PASS',am:'CAUTION',st:'FAIL',wt:'WAIT'};
  [1,2,3,4,5].forEach(n=>{
    const el=document.getElementById('g'+n);
    el.className='gate '+gState[n];
    document.getElementById('gs'+n).textContent=lbl[gState[n]];
    setTimeout(()=>{document.getElementById('gb'+n).style.width=gScore[n]+'%'},60+n*40);
  });
}

function updateVerdict(){
  const pass=Object.values(gState).filter(s=>s==='go').length;
  const hasFail=Object.values(gState).includes('st');
  // dots
  document.getElementById('sdots').innerHTML=[1,2,3,4,5].map(n=>{
    const s=gState[n];
    const l={go:'G'+n,am:'!',st:'✗',wt:'?'}[s];
    return '<div class="sdot '+s+'">'+l+'</div>';
  }).join('');
  // badge + score
  const gb=document.getElementById('gbadge');
  gb.textContent=pass+' / 5 PASS';
  gb.className='bd '+(pass>=5?'bg':pass>=3?'ba':'br');
  const sn=document.getElementById('snum');
  sn.textContent=pass+' / 5';
  sn.className='snum '+(pass>=5?'go':pass>=3?'am':'st');
  // verdict
  const vd=document.getElementById('vd');
  const vw=document.getElementById('vword');
  const vs=document.getElementById('vsub');
  const vdirs=document.getElementById('vdirs');
  const pc=document.getElementById('pb-conf');
  if(pass===5&&!hasFail){
    vd.className='vgo';vw.textContent='EXECUTE';
    vs.textContent='All 5 gates clear — deploy full position';
    vdirs.style.display='flex';
    pc.textContent='HIGH';pc.className='pbv cg';
  } else if(hasFail||pass<3){
    vd.className='vst';vw.textContent='NO TRADE';
    vs.textContent='Critical gate failed — stand down';
    vdirs.style.display='none';
    pc.textContent='LOW';pc.className='pbv cr';
  } else {
    vd.className='vam';vw.textContent='WAIT';
    vs.textContent='Trigger / Risk gate not satisfied yet';
    vdirs.style.display='none';
    pc.textContent='MODERATE';pc.className='pbv ca';
  }
}

// ═══════════════════════════════════════════════
// TAB SWITCHING
// ═══════════════════════════════════════════════
function sw(name,btn){
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('on'));
  document.querySelectorAll('.tp').forEach(t=>t.classList.remove('on'));
  if(btn&&btn.classList){
    btn.classList.add('on');
  } else {
    document.querySelectorAll('.tab').forEach(t=>{
      const oc=t.getAttribute('onclick')||'';
      if(oc.includes("'"+name+"'"))t.classList.add('on');
    });
  }
  const tp=document.getElementById('tp-'+name);
  if(tp)tp.classList.add('on');
  if(name==='backtest') setTimeout(bt2Run, 100);
}

// ═══════════════════════════════════════════════
// INSTRUMENT SELECT + PRICE SIMULATION
// ═══════════════════════════════════════════════
function selIC(el){
  document.querySelectorAll('.ic').forEach(i=>i.classList.remove('sel'));
  el.classList.add('sel');
}

// Price simulation removed — real prices come from live WS handle('prices') and historical handle.

// ═══════════════════════════════════════════════
// OI PULSE
// ═══════════════════════════════════════════════
(function buildOIPulse(){
  const el = document.getElementById('oip');
  if(el) el.innerHTML='<div style="color:var(--t2);font-size:9px;padding:8px 2px">Waiting for live stock OI build-up...</div>';
})();

// ═══════════════════════════════════════════════
// OPTION CHAIN
// ═══════════════════════════════════════════════
(function buildOC(){
  document.getElementById('oct-body').innerHTML='<tr><td colspan="8" style="text-align:center;color:var(--t2);padding:18px">Waiting for live option-chain snapshot...</td></tr>';
})();

// ═══════════════════════════════════════════════
// STOCK OI SCANNER
// ═══════════════════════════════════════════════
const stocks=[
  {r:1,s:'INDUSINDBK',sec:'Banking',pr:984,chg:1.44,oiC:19800,oiP:19.8,pcr:1.63,vol:2.6,atr:1.6,del:56,rs:1.28,sig:'LONG OI↑',sigt:'go',score:88,top:true,type:'long'},
  {r:2,s:'ICICIBANK',sec:'Banking',pr:1247,chg:1.32,oiC:18200,oiP:18.2,pcr:1.62,vol:2.8,atr:1.1,del:62,rs:1.24,sig:'LONG OI↑',sigt:'go',score:88,top:true,type:'long'},
  {r:3,s:'SBIN',sec:'PSU Bank',pr:812,chg:1.12,oiC:14800,oiP:16.4,pcr:1.50,vol:2.4,atr:1.5,del:58,rs:1.22,sig:'LONG OI↑',sigt:'go',score:83,top:true,type:'long'},
  {r:4,s:'AXISBANK',sec:'Banking',pr:1089,chg:0.94,oiC:12200,oiP:14.8,pcr:1.42,vol:2.1,atr:1.3,del:61,rs:1.18,sig:'LONG OI↑',sigt:'go',score:81,top:true,type:'long'},
  {r:5,s:'LT',sec:'Infra',pr:3624,chg:0.92,oiC:8800,oiP:10.4,pcr:1.36,vol:1.7,atr:1.2,del:54,rs:1.16,sig:'LONG',sigt:'go',score:76,top:false,type:'long'},
  {r:6,s:'KOTAKBANK',sec:'Banking',pr:1892,chg:0.61,oiC:8400,oiP:9.2,pcr:1.29,vol:1.6,atr:0.9,del:48,rs:1.09,sig:'NEUTRAL',sigt:'am',score:72,top:false,type:'long'},
  {r:7,s:'HDFCBANK',sec:'Banking',pr:1743,chg:0.67,oiC:6800,oiP:8.4,pcr:1.29,vol:1.6,atr:0.9,del:52,rs:1.08,sig:'NEUTRAL',sigt:'am',score:71,top:false,type:'long'},
  {r:8,s:'TCS',sec:'IT',pr:3912,chg:0.44,oiC:3200,oiP:5.8,pcr:1.14,vol:1.3,atr:0.8,del:44,rs:1.03,sig:'NEUTRAL',sigt:'am',score:62,top:false,type:'long'},
  {r:9,s:'TATAMOTORS',sec:'Auto',pr:802,chg:1.87,oiC:22100,oiP:21.4,pcr:0.83,vol:3.2,atr:1.8,del:36,rs:1.31,sig:'SHORT OI↑',sigt:'st',score:74,top:false,type:'short'},
  {r:10,s:'INFY',sec:'IT',pr:1742,chg:-0.21,oiC:-4400,oiP:-6.2,pcr:0.61,vol:1.2,atr:0.9,del:38,rs:0.94,sig:'SHORT',sigt:'st',score:52,top:false,type:'short'},
  {r:11,s:'BAJFINANCE',sec:'NBFC',pr:7240,chg:-0.43,oiC:-9200,oiP:-8.1,pcr:0.61,vol:1.8,atr:1.4,del:34,rs:0.88,sig:'UNWIND',sigt:'am',score:48,top:false,type:'short'},
  {r:12,s:'RELIANCE',sec:'Energy',pr:2847,chg:-0.18,oiC:-6800,oiP:-7.4,pcr:0.67,vol:0.8,atr:0.7,del:32,rs:0.82,sig:'AVOID',sigt:'st',score:38,top:false,type:'short'},
  {r:13,s:'TATASTEEL',sec:'Metal',pr:148,chg:-1.24,oiC:-11200,oiP:-14.2,pcr:0.61,vol:2.1,atr:2.1,del:28,rs:0.74,sig:'SHORT OI↑',sigt:'st',score:41,top:false,type:'short'},
];

// Gate dot renderer: g = 'go'|'am'|'wt'|'st'
function _gDot(g, lbl){
  const c = g==='go'?'var(--go)':g==='st'?'var(--st)':g==='am'?'var(--am)':'var(--t2)';
  return `<span title="${lbl}" style="width:7px;height:7px;border-radius:50%;background:${c};display:inline-block;flex-shrink:0"></span>`;
}
function _gateDots(g1,g2,g3,g4,g5){
  return `<span style="display:flex;gap:2px;align-items:center">${_gDot(g1,'G1 REGIME')}${_gDot(g2,'G2 SMART$')}${_gDot(g3,'G3 STRUCT')}${_gDot(g4,'G4 TRIGGER')}${_gDot(g5,'G5 RISK')}</span>`;
}
// Derive gate states from static stock fields when API gate data not available
function _deriveGates(x){
  const g1 = (gState[1]||'go');
  const g2 = x.pcr>=1.0?'go':x.pcr>=0.7?'am':'st';
  const g3 = x.chg>0.15?'go':x.chg>-0.15?'am':'st';
  const g4 = x.vol>=1.3?'go':x.vol>=1.0?'am':'wt';
  const g5 = x.atr<=1.5?'go':x.atr<=2.5?'am':'am';
  return {g1,g2,g3,g4,g5};
}

function renderStocks(){
  try {
  let d=[...stocks];
  if(stkFil==='long') d=d.filter(x=>x.type==='long');
  else if(stkFil==='short') d=d.filter(x=>x.type==='short');
  else if(stkFil==='top') d=d.filter(x=>x.top);
  // Gate filter
  if(stkGateFil && stkGateFil!=='all'){
    d=d.filter(x=>{
      const g=x.g1?x:{..._deriveGates(x)};
      const gates={g1:g.g1||_deriveGates(x).g1,g2:g.g2||_deriveGates(x).g2,
                   g3:g.g3||_deriveGates(x).g3,g4:g.g4||_deriveGates(x).g4,
                   g5:g.g5||_deriveGates(x).g5};
      if(stkGateFil==='exe') return ['g1','g2','g3','g4','g5'].every(k=>gates[k]==='go');
      return gates[stkGateFil]==='go';
    });
  }
  d.sort((a,b)=>b.score-a.score);
  const sc=s=>'<span class="scr '+(s>=75?'hi':s>=55?'md':'lo')+'">'+s+'</span>';
  const sc2=t=>t==='go'?'background:var(--go-d);color:var(--go);border:1px solid var(--go-b)':
    t==='st'?'background:var(--st-d);color:var(--st);border:1px solid var(--st-b)':
    'background:var(--am-d);color:var(--am);border:1px solid var(--am-b)';
  const cc=v=>v>0?'var(--go)':'var(--st)';
  const tradeHtml = x => {
    const price = Number(x.price ?? x.pr ?? 0);
    const atrPct = Number(x.atr_pct ?? x.atr ?? 1.5);
    const score = Number(x.score ?? 0);
    const pc = Number(x.pc ?? 0);
    const chg = Number(x.chg_pct ?? x.chg ?? 0);
    if(!price || (pc < 3 && score < 80)) return '<div class="tradebox"><div class="thd ta">WAIT</div><div class="tln">No valid entry yet</div></div>';
    const atr = Math.max(price * atrPct / 100, price * 0.0035);
    const isLong = chg >= 0;
    const entry = isLong ? (chg >= 1 ? price * 1.002 : price - atr * 0.2) : (chg <= -1 ? price * 0.998 : price + atr * 0.2);
    const sl = isLong ? entry - atr * 1.2 : entry + atr * 1.2;
    const t1 = isLong ? entry + (entry - sl) * 2.0 : entry - (sl - entry) * 2.0;
    const t2 = isLong ? entry + (entry - sl) * 3.0 : entry - (sl - entry) * 3.0;
    const f = n => (n >= 100 ? Math.round(n).toLocaleString('en-IN') : n.toFixed(1));
    return `<div class="tradebox"><div class="thd ${isLong?'tg':'tr'}">${isLong?'LONG':'SHORT'} READY</div><div class="tln">E <span class="${isLong?'tg':'tr'}">${f(entry)}</span>  SL <span class="tr">${f(sl)}</span></div><div class="tln">T1 <span class="tg">${f(t1)}</span>  T2 <span class="tg">${f(t2)}</span></div></div>`;
  };
  document.getElementById('stk-body').innerHTML=d.map((x,i)=>{
    const dg=_deriveGates(x);
    const g1=x.g1||dg.g1, g2=x.g2||dg.g2, g3=x.g3||dg.g3, g4=x.g4||dg.g4, g5=x.g5||dg.g5;
    return `
    <tr class="${x.top?'top':''}">
      <td><span style="color:var(--t2);font-size:8px;margin-right:3px">${i+1}</span><span class="tv-link" data-tvsym="${x.s}" style="font-weight:600;color:var(--t0)">${x.s}</span>${x.top?'<span style="color:var(--go);font-size:8px"> ★</span>':''}<br><span style="font-size:8px;color:var(--t2)">${x.sec}</span></td>
      <td style="font-weight:600">${x.pr.toLocaleString('en-IN')}</td>
      <td style="color:${cc(x.chg)};font-weight:600">${x.chg>0?'+':''}${x.chg.toFixed(2)}%</td>
      <td style="color:${cc(x.oiC)}">${x.oiC>0?'+':''}${x.oiC.toLocaleString()}</td>
      <td><span style="color:${cc(x.oiP)};font-weight:600">${x.oiP>0?'+':''}${x.oiP.toFixed(1)}%</span><div style="height:3px;border-radius:2px;background:var(--b1);margin-top:2px;overflow:hidden;width:60px"><div style="height:100%;width:${Math.min(Math.abs(x.oiP)*3.5,100)}%;background:${cc(x.oiP)};border-radius:2px"></div></div></td>
      <td style="color:${x.pcr>1.3?'var(--go)':x.pcr<0.8?'var(--st)':'var(--am)'};font-weight:600">${x.pcr.toFixed(2)}</td>
      <td style="color:${x.vol>2?'var(--go)':x.vol>1.5?'var(--am)':'var(--t2)'};font-weight:600">${x.vol.toFixed(1)}×</td>
      <td style="color:${x.atr>1.5?'var(--am)':'var(--t0)'}">${x.atr.toFixed(1)}%</td>
      <td style="color:${x.rs>1.1?'var(--go)':x.rs>0.95?'var(--am)':'var(--st)'};font-weight:600">${x.rs>1?'+':''}${((x.rs-1)*100).toFixed(0)}%</td>
      <td style="color:${x.del>55?'var(--go)':x.del>44?'var(--am)':'var(--t2)'}">${x.del}%</td>
      <td>${_gateDots(g1,g2,g3,g4,g5)}</td>
      <td><span class="sigtag" style="${sc2(x.sigt)}">${x.sig}</span></td>
      <td>${tradeHtml(x)}</td>
      <td>${sc(x.score)}</td>
    </tr>`;
  }).join('');
  } catch(err) {
    console.error('renderStocks failed:', err);
    const body = document.getElementById('stk-body');
    if(body) body.innerHTML = '<tr><td colspan="14" style="padding:14px;text-align:center;color:var(--am)">Stock OI render error - refresh once</td></tr>';
  }
}
function filterStocks(f,btn){
  stkFil=f;
  document.querySelectorAll('.sfbar .sf').forEach(b=>b.classList.remove('on'));
  btn.classList.add('on');
  renderStocks();
}
function filterStocksGate(f,btn){
  stkGateFil=f;
  document.querySelectorAll('[id^="sgf-"]').forEach(b=>b.classList.remove('on'));
  btn.classList.add('on');
  renderStocks();
}
renderStocks();

// ═══════════════════════════════════════════════
// SWING RADAR
// ═══════════════════════════════════════════════
(function renderSwing(){
  const sb=document.getElementById('sw-body');
  if(sb) sb.innerHTML='<tr><td colspan="14" style="text-align:center;color:var(--t2);padding:18px">Waiting for live stock state...</td></tr>';
})();

// ═══════════════════════════════════════════════
// JOURNAL
// ═══════════════════════════════════════════════
const sampleTrades=[];

function renderLog(){
  const el=document.getElementById('jlog');
  if(!trades.length&&!sampleTrades.length){
    el.innerHTML='<div style="color:var(--t2);font-size:10px;text-align:center;padding:16px">No journal entries yet<br><span style="color:var(--am)">Manual logs only. No demo trades shown.</span></div>';
    return;
  }
  const all=[...trades,...sampleTrades];
  el.innerHTML=all.map(t=>{
    const pnl=t.exit&&t.entry?Math.round(t.dir==='LONG'?t.exit-t.entry:t.entry-t.exit):null;
    const ps=pnl!=null?(pnl>0?'+':'')+pnl+' pts':'—';
    const pc=t.res==='WIN'?'win':t.res==='LOSS'?'loss':'';
    const dc=t.dir==='LONG'?'L':'S';
    const rb=t.res==='WIN'?'bg':t.res==='LOSS'?'br':'ba';
    const dots=[1,2,3,4,5].map(i=>'<div class="jdt" style="background:'+(i<=t.gates?'var(--go)':'var(--b2)')+'"></div>').join('');
    return '<div class="je"><div class="jer1">'+
      '<span class="ji">'+t.inst+'</span>'+
      '<span class="jdir '+dc+'">'+t.dir+'</span>'+
      '<span class="bd '+rb+'">'+t.res+'</span>'+
      '<span class="jpnl '+pc+'" style="margin-left:auto">'+ps+'</span></div>'+
      '<div class="jer2">'+
      '<span style="font-size:8px;color:var(--t2)">'+t.type+'</span>'+
      '<div class="jdts" style="margin-left:auto">'+dots+'</div></div>'+
      '<div class="jnote">'+t.notes+'</div></div>';
  }).join('');
}
renderLog();

function logTrade(){
  const entry=parseFloat(document.getElementById('j-entry').value)||0;
  if(!entry){notify('Enter entry price');return}
  const t={
    inst:document.getElementById('j-inst').value,
    dir:document.getElementById('j-dir').value,
    entry,
    exit:parseFloat(document.getElementById('j-exit').value)||0,
    stop:parseFloat(document.getElementById('j-stop').value)||0,
    res:document.getElementById('j-res').value,
    type:document.getElementById('j-type').value,
    gates:document.querySelectorAll('.gck.on').length,
    notes:document.getElementById('j-notes').value||'—'
  };
  trades.unshift(t);
  renderLog();updateStats();clearJ();
  notify('Trade logged');
}
function clearJ(){
  ['j-entry','j-exit','j-stop'].forEach(id=>document.getElementById(id).value='');
  document.getElementById('j-notes').value='';
  document.querySelectorAll('.gck.on').forEach(g=>g.classList.remove('on'));
}

// ═══════════════════════════════════════════════
// STATS
// ═══════════════════════════════════════════════
function updateStats(){
  const all=[...trades,...sampleTrades];
  const taken=all.filter(t=>t.type==='TAKEN');
  const wins=taken.filter(t=>t.res==='WIN').length;
  const wr=taken.length?Math.round(wins/taken.length*100):0;
  const pnls=taken.filter(t=>t.exit&&t.entry).map(t=>Math.round(t.dir==='LONG'?t.exit-t.entry:t.entry-t.exit));
  const tot=pnls.length?pnls.reduce((a,b)=>a+b,0):0;
  const fg=all.filter(t=>t.gates===5).length;
  const gd=all.length?Math.round(fg/all.length*100):0;
  const wrEl=document.getElementById('st-wr');
  wrEl.textContent=(taken.length?wr:'—') + (taken.length?'%':'');
  wrEl.style.color=wr>=55?'var(--go)':wr>=40?'var(--am)':'var(--st)';
  document.getElementById('st-tot').textContent=all.length;
  document.getElementById('st-pnl').textContent=pnls.length?((tot>0?'+':'')+tot+' pts'):'—';
  document.getElementById('st-pnl').style.color=tot>0?'var(--go)':'var(--st)';
  document.getElementById('st-gd').textContent=all.length?(gd+'%'):'—';
}
updateStats();

document.getElementById('gate-perf').innerHTML='<div style="color:var(--t2);font-size:9px;padding:8px 0;line-height:1.6">Gate win rates load from backtest analytics, not demo data.</div>';

document.getElementById('ttc').innerHTML='<div style="color:var(--t2);font-size:9px;padding:8px 0;line-height:1.6">Timing confidence appears after live/backtest signals are available.</div>';

// ═══════════════════════════════════════════════
// 3-STRIKE SYSTEM
// ═══════════════════════════════════════════════
function hitSK(n){
  strikes=Math.max(strikes,n);
  for(let i=1;i<=3;i++){
    const el=document.getElementById('sk'+i);
    el.classList.toggle('hit',i<=strikes);
    el.textContent=i<=strikes?'✗':'○';
  }
  const msgs=['','Reduce next trade 50%','1 trade max today','STOP TRADING NOW'];
  const el2=document.getElementById('skmsg');
  el2.textContent=msgs[strikes];
  el2.style.color=strikes===3?'var(--st)':strikes>0?'var(--am)':'var(--t2)';
  if(strikes===3)notify('STOP TRADING — 3 strikes');
}

// ═══════════════════════════════════════════════
// CHECKLIST
// ═══════════════════════════════════════════════
function togCI(el){
  el.classList.toggle('done');
  el.querySelector('.cibox').textContent=el.classList.contains('done')?'✓':'';
  const done=document.querySelectorAll('.ci.done').length;
  const tot=document.querySelectorAll('.ci').length;
  document.getElementById('cl-prog').textContent=done+' / '+tot;
}

// ═══════════════════════════════════════════════
// ANALYTICS — SUB NAV
// ═══════════════════════════════════════════════
function anSw(name,btn){
  document.querySelectorAll('.abtn').forEach(b=>b.classList.remove('on'));
  document.querySelectorAll('.asec').forEach(s=>s.classList.remove('on'));
  btn.classList.add('on');
  document.getElementById('an-'+name).classList.add('on');
}

// ═══════════════════════════════════════════════
// SPIKE RADAR
// ═══════════════════════════════════════════════
const spikeData=[];

(function buildSpike(){
  const sc=s=>'<span class="strpill '+(s==='hi'?'strhi':s==='md'?'strmd':'strlo')+'">'+(s==='hi'?'HIGH':s==='md'?'MED':'LOW')+'</span>';
  const rc=t=>t==='buy'?'sbuy':t==='sell'?'ssell':'soi';
  document.getElementById('spike-body').innerHTML=(spikeData.length?spikeData.map(d=>
    '<tr class="'+rc(d.type)+'">'+
    '<td style="font-weight:600;color:var(--t0)">'+d.s+'</td>'+
    '<td style="color:var(--t2)">'+d.t+'</td>'+
    '<td style="font-weight:600">'+d.pr.toLocaleString('en-IN')+'</td>'+
    '<td style="color:'+(d.chg>0?'var(--go)':'var(--st)')+';font-weight:600">'+(d.chg>0?'+':'')+d.chg.toFixed(2)+'%</td>'+
    '<td style="color:'+(d.vol>3?'var(--go)':d.vol>2?'var(--am)':'var(--t2)')+';font-weight:600">'+d.vol.toFixed(1)+'×</td>'+
    '<td style="color:'+(d.oi>0?'var(--go)':'var(--st)')+';font-weight:600">'+(d.oi>0?'+':'')+d.oi.toFixed(1)+'%</td>'+
    '<td style="color:var(--t2);font-size:9px">'+d.trigger+'</td>'+
    '<td style="color:'+(d.type==='buy'?'var(--go)':d.type==='sell'?'var(--st)':'var(--am)')+';font-weight:700">'+d.sig+'</td>'+
    sc(d.str)+
    '<td style="text-align:center">'+(d.type==='buy'?'<span style="color:var(--go)">▲</span>':d.type==='sell'?'<span style="color:var(--st)">▼</span>':'<span style="color:var(--am)">●</span>')+'</td>'+
    '</tr>'
  ).join(''):'<tr><td colspan="10" style="text-align:center;color:var(--t2);padding:18px">Waiting for live spike stream...</td></tr>');
  document.getElementById('spike-hi').textContent=spikeData.filter(d=>d.str==='hi').length+' high strength';
})();

setInterval(()=>{
  const ist=new Date(new Date().toLocaleString('en-US',{timeZone:'Asia/Kolkata'}));
  const ts=String(ist.getHours()).padStart(2,'0')+':'+String(ist.getMinutes()).padStart(2,'0')+':'+String(ist.getSeconds()).padStart(2,'0');
  const el=document.getElementById('spike-ts2');
  if(el)el.textContent=ts+' IST';
},1000);

// ═══════════════════════════════════════════════
// POSITION SIZER
// ═══════════════════════════════════════════════
function setVIX(btn,v){
  curVIX=v;
  document.querySelectorAll('.pssb').forEach(b=>b.classList.remove('on'));
  btn.classList.add('on');
  calcPS();
}

function calcPS(){
  const cap=parseFloat(document.getElementById('ps-cap').value)||500000;
  const riskPct=parseFloat(document.getElementById('ps-risk').value)||1;
  const atr=parseFloat(document.getElementById('ps-atr').value)||87;
  const slm=parseFloat(document.getElementById('ps-slm').value)||1.5;
  const instVal=document.getElementById('ps-inst').value;
  const parts=instVal.split(',');
  const lotSz=parseInt(parts[0])||25;
  const px=parseFloat(parts[1])||24412;
  const vixMap={lo:1,md:.75,hi:.5,xh:.25};
  const vixLbl={lo:'FULL 100% — VIX safe',md:'75% size — mild caution',hi:'50% size — reduce',xh:'25% or NO TRADE'};
  const vixCol={lo:'var(--go)',md:'var(--am)',hi:'var(--am)',xh:'var(--st)'};
  const vm=vixMap[curVIX]||1;
  const maxRaw=cap*(riskPct/100);
  const maxR=maxRaw*vm;
  const stopPts=atr*slm;
  const rpl=stopPts*lotSz;
  const lots=Math.max(1,Math.floor(maxR/rpl));
  const posVal=lots*lotSz*px;
  const capUse=(posVal/cap*100).toFixed(1);
  const marginEst=lotSz===25?120000:lotSz===15?72000:posVal*.15;
  document.getElementById('ps-maxrisk').textContent='₹'+Math.round(maxR).toLocaleString('en-IN');
  document.getElementById('ps-vixrule').textContent=vixLbl[curVIX];
  document.getElementById('ps-vixrule').style.color=vixCol[curVIX];
  document.getElementById('ps-stop').textContent=stopPts.toFixed(0)+' pts';
  document.getElementById('ps-lots').textContent=lots+' lot'+(lots>1?'s':'');
  document.getElementById('ps-posval').textContent='₹'+Math.round(posVal).toLocaleString('en-IN');
  document.getElementById('ps-capuse').textContent=capUse+'% of capital';
  document.getElementById('ps-capuse').style.color=parseFloat(capUse)>80?'var(--st)':parseFloat(capUse)>50?'var(--am)':'var(--t1)';
  document.getElementById('ps-margin').textContent='~₹'+(Math.round(marginEst*lots/1000)).toLocaleString('en-IN')+'K';
  document.getElementById('ps-breakdown').innerHTML=
    '<div style="display:flex;flex-direction:column;gap:5px">'+
    psbar('Raw risk',riskPct/3*100,'var(--go)','₹'+Math.round(maxRaw/1000).toFixed(1)+'K')+
    psbar('VIX adj.',vm*100,vixCol[curVIX],(vm*100).toFixed(0)+'%')+
    psbar('Lot risk',Math.min(rpl/maxRaw*100,100),'var(--am)','₹'+Math.round(rpl/1000).toFixed(1)+'K')+
    psbar('Cap used',Math.min(parseFloat(capUse),100),parseFloat(capUse)>80?'var(--st)':parseFloat(capUse)>50?'var(--am)':'var(--bl)',capUse+'%')+
    '</div>';
}
function psbar(k,pct,col,v){
  return '<div style="display:flex;align-items:center;gap:6px;font-size:9px">'+
    '<span style="color:var(--t2);width:68px;flex-shrink:0">'+k+'</span>'+
    '<div class="psbar"><div class="psbf" style="width:'+pct+'%;background:'+col+'"></div></div>'+
    '<span style="font-weight:600;width:44px;text-align:right;color:'+col+'">'+v+'</span></div>';
}
calcPS();

// ═══════════════════════════════════════════════
// SIGNAL BACKTEST ENGINE
// ═══════════════════════════════════════════════
const btData=[
  {date:'2026-03-27',dt:'27 Mar 10:14',sig:'NIFTY LONG',inst:'NIFTY',dir:'L',gate:'G5',reason:'R:R 1:1.4 — below 1:2',entry:24310,exit:24488,pnl:+178,correct:true,lesson:'G5 right but barely. R:R tightened after entry.'},
  {date:'2026-03-27',dt:'27 Mar 11:32',sig:'RELIANCE LONG',inst:'RELIANCE',dir:'L',gate:'G2',reason:'OI unwinding detected',entry:0,exit:0,pnl:-64,correct:true,lesson:'Stock fell 64 pts. G2 saved a loss.'},
  {date:'2026-03-27',dt:'27 Mar 14:10',sig:'HDFCBANK LONG',inst:'HDFCBANK',dir:'L',gate:'G4',reason:'Delta below trigger threshold',entry:0,exit:0,pnl:+38,correct:false,lesson:'Stock moved 38 pts. G4 possibly too strict on slow sessions.'},
  {date:'2026-03-26',dt:'26 Mar 10:08',sig:'TATAMOTORS SHORT',inst:'TATAMOTORS',dir:'S',gate:'G3',reason:'HTF structure unclear on daily',entry:0,exit:0,pnl:+88,correct:false,lesson:'Stock dropped 88 pts. Gate was too conservative here.'},
  {date:'2026-03-26',dt:'26 Mar 12:18',sig:'SBIN LONG',inst:'SBIN',dir:'L',gate:'G4',reason:'Delta weak — trigger not fired',entry:0,exit:0,pnl:+42,correct:false,lesson:'Slow trigger but moved. G4 threshold needs tuning for PSU banks.'},
  {date:'2026-03-26',dt:'26 Mar 13:44',sig:'AXISBANK LONG',inst:'AXISBANK',dir:'L',gate:'G5',reason:'R:R 1:1.9 — below 1:2',entry:0,exit:0,pnl:+56,correct:false,lesson:'Missed solid trade. Consider relaxing G5 to 1:1.8 minimum.'},
  {date:'2026-03-25',dt:'25 Mar 10:22',sig:'ICICIBANK LONG',inst:'ICICIBANK',dir:'L',gate:'G5',reason:'R:R 1:1.8 — below threshold',entry:0,exit:0,pnl:+120,correct:false,lesson:'Good trade missed. Positional R:R was valid here.'},
  {date:'2026-03-25',dt:'25 Mar 11:45',sig:'BAJFINANCE SHORT',inst:'BAJFINANCE',dir:'S',gate:'G1',reason:'Regime: daily EMA below MA',entry:0,exit:0,pnl:-88,correct:true,lesson:'Stock reversed sharply. Regime gate was correct.'},
  {date:'2026-03-25',dt:'25 Mar 13:02',sig:'NIFTY LONG',inst:'NIFTY',dir:'L',gate:'G2',reason:'PCR dropped below 1.0 intraday',entry:0,exit:0,pnl:-42,correct:true,lesson:'Market fell. Smart money gate correctly blocked.'},
  {date:'2026-03-24',dt:'24 Mar 10:35',sig:'NIFTY LONG',inst:'NIFTY',dir:'L',gate:'G4',reason:'OI build below threshold',entry:0,exit:0,pnl:+64,correct:false,lesson:'Moderate move. G4 threshold too strict on slow sessions.'},
  {date:'2026-03-24',dt:'24 Mar 13:02',sig:'TATASTEEL SHORT',inst:'TATASTEEL',dir:'S',gate:'G1',reason:'DXY rising — regime risk',entry:0,exit:0,pnl:-112,correct:true,lesson:'Stock bounced. Regime gate caught macro correctly.'},
  {date:'2026-03-24',dt:'24 Mar 11:18',sig:'BANKNIFTY LONG',inst:'BANKNIFTY',dir:'L',gate:'G3',reason:'1H structure not confirmed',entry:0,exit:0,pnl:+94,correct:false,lesson:'Clean breakout missed. G3 1H requirement blocked strong move.'},
  {date:'2026-03-21',dt:'21 Mar 10:05',sig:'SBIN LONG',inst:'SBIN',dir:'L',gate:'G5',reason:'ATR stop too wide for target',entry:812,exit:848,pnl:+36,correct:true,lesson:'Taken and won but R:R concern was valid.'},
  {date:'2026-03-21',dt:'21 Mar 12:40',sig:'LT LONG',inst:'LT',dir:'L',gate:'G4',reason:'Volume below avg — no surge',entry:0,exit:0,pnl:+72,correct:false,lesson:'L&T moved on sector news. Volume gate missed event-driven move.'},
  {date:'2026-03-21',dt:'21 Mar 10:55',sig:'RELIANCE SHORT',inst:'RELIANCE',dir:'S',gate:'G2',reason:'FII net buyer — no short bias',entry:0,exit:0,pnl:-48,correct:true,lesson:'Stock recovered with FII. G2 correctly blocked counter trade.'},
  {date:'2026-03-20',dt:'20 Mar 09:38',sig:'NIFTY LONG',inst:'NIFTY',dir:'L',gate:'G1',reason:'VIX above 16 — regime uncertain',entry:0,exit:0,pnl:-88,correct:true,lesson:'VIX spike. Market fell. G1 regime gate essential.'},
  {date:'2026-03-20',dt:'20 Mar 11:22',sig:'ICICIBANK LONG',inst:'ICICIBANK',dir:'L',gate:'G5',reason:'R:R 1:1.6 — below threshold',entry:0,exit:0,pnl:+96,correct:false,lesson:'Strong bank move. G5 missed high-conviction trade.'},
  {date:'2026-03-20',dt:'20 Mar 13:15',sig:'TATAMOTORS LONG',inst:'TATAMOTORS',dir:'L',gate:'G3',reason:'Daily structure HH/HL not confirmed',entry:0,exit:0,pnl:-34,correct:true,lesson:'Stock dipped. Structure gate correctly rejected weak setup.'},
  {date:'2026-03-19',dt:'19 Mar 10:12',sig:'BANKNIFTY LONG',inst:'BANKNIFTY',dir:'L',gate:'G4',reason:'Delta divergence — absorption phase',entry:0,exit:0,pnl:+144,correct:false,lesson:'Strong rally blocked by delta. Absorption preceding breakout.'},
  {date:'2026-03-19',dt:'19 Mar 11:48',sig:'BAJFINANCE LONG',inst:'BAJFINANCE',dir:'L',gate:'G2',reason:'OI net short — no institutional buy',entry:0,exit:0,pnl:-62,correct:true,lesson:'Stock fell. OI correctly signalled no institutional buy.'},
  {date:'2026-03-18',dt:'18 Mar 10:08',sig:'NIFTY LONG',inst:'NIFTY',dir:'L',gate:'G5',reason:'R:R 1:1.7',entry:0,exit:0,pnl:+118,correct:false,lesson:'Solid Nifty rally missed on R:R grounds.'},
  {date:'2026-03-18',dt:'18 Mar 12:30',sig:'AXISBANK SHORT',inst:'AXISBANK',dir:'S',gate:'G1',reason:'Regime bullish — no short bias',entry:0,exit:0,pnl:-56,correct:true,lesson:'Stock recovered. Regime gate correctly blocked counter trade.'},
  {date:'2026-03-17',dt:'17 Mar 10:22',sig:'SBIN LONG',inst:'SBIN',dir:'L',gate:'G4',reason:'OI build delayed — below threshold',entry:804,exit:826,pnl:+22,correct:true,lesson:'Taken late after G4 fired. 22 pts vs possible 40 if earlier.'},
  {date:'2026-03-17',dt:'17 Mar 11:55',sig:'LT LONG',inst:'LT',dir:'L',gate:'G3',reason:'MTF 1H demand zone not retested',entry:0,exit:0,pnl:+68,correct:false,lesson:'Price moved without retesting zone. G3 missed momentum entry.'},
  {date:'2026-03-14',dt:'14 Mar 10:18',sig:'NIFTY SHORT',inst:'NIFTY',dir:'S',gate:'G1',reason:'FII 5-day net buyer — regime bullish',entry:0,exit:0,pnl:-124,correct:true,lesson:'Market recovered strongly. Regime gate essential block.'},
  {date:'2026-03-14',dt:'14 Mar 12:04',sig:'HDFCBANK LONG',inst:'HDFCBANK',dir:'L',gate:'G5',reason:'R:R 1:1.9',entry:0,exit:0,pnl:+44,correct:false,lesson:'Marginal R:R. Stock moved but not far enough.'},
  {date:'2026-03-13',dt:'13 Mar 10:42',sig:'BANKNIFTY SHORT',inst:'BANKNIFTY',dir:'S',gate:'G2',reason:'PCR 1.32 — put writers active',entry:0,exit:0,pnl:-98,correct:true,lesson:'BankNifty bounced off put wall. G2 read OI support correctly.'},
  {date:'2026-03-13',dt:'13 Mar 11:15',sig:'TATASTEEL LONG',inst:'TATASTEEL',dir:'L',gate:'G1',reason:'Metal sector regime weak',entry:0,exit:0,pnl:-44,correct:true,lesson:'Stock fell further. Sector regime gate was correct.'},
  {date:'2026-03-12',dt:'12 Mar 10:08',sig:'ICICIBANK LONG',inst:'ICICIBANK',dir:'L',gate:'G4',reason:'Volume 0.8x avg — no surge',entry:1218,exit:1268,pnl:+50,correct:true,lesson:'Taken after G4 fired. Still profitable.'},
  {date:'2026-03-12',dt:'12 Mar 13:22',sig:'NIFTY LONG',inst:'NIFTY',dir:'L',gate:'G3',reason:'LTF 15m structure not broken',entry:0,exit:0,pnl:+88,correct:false,lesson:'ORB breakout happened without LTF confirmation. G3 too strict.'},
  {date:'2026-03-11',dt:'11 Mar 10:14',sig:'RELIANCE LONG',inst:'RELIANCE',dir:'L',gate:'G2',reason:'OI unwinding — institutional exit',entry:0,exit:0,pnl:-72,correct:true,lesson:'Stock fell as OI predicted. G2 saved capital.'},
  {date:'2026-03-11',dt:'11 Mar 12:38',sig:'AXISBANK LONG',inst:'AXISBANK',dir:'L',gate:'G5',reason:'ATR stop 94 pts too wide',entry:0,exit:0,pnl:+62,correct:false,lesson:'Good move missed. Consider ATR multiplier 1.25x instead of 1.5x.'},
  {date:'2026-03-10',dt:'10 Mar 10:22',sig:'BANKNIFTY LONG',inst:'BANKNIFTY',dir:'L',gate:'G1',reason:'VIX 17.4 — elevated regime',entry:0,exit:0,pnl:-136,correct:true,lesson:'VIX spike caused whipsaw. Regime gate critical.'},
  {date:'2026-03-10',dt:'10 Mar 11:48',sig:'SBIN LONG',inst:'SBIN',dir:'L',gate:'G4',reason:'Delta flat — no directional conviction',entry:0,exit:0,pnl:+54,correct:false,lesson:'Slow mover but moved. G4 delta threshold too strict for PSU banks.'},
  {date:'2026-03-07',dt:'07 Mar 10:05',sig:'NIFTY LONG',inst:'NIFTY',dir:'L',gate:'G5',reason:'R:R 1:1.5',entry:0,exit:0,pnl:+102,correct:false,lesson:'Big Nifty day missed on R:R grounds. Revisit threshold.'},
  {date:'2026-03-07',dt:'07 Mar 12:14',sig:'TATAMOTORS SHORT',inst:'TATAMOTORS',dir:'S',gate:'G3',reason:'Structure break not confirmed HTF',entry:0,exit:0,pnl:-66,correct:true,lesson:'Stock recovered. Structure gate correctly rejected early short.'},
  {date:'2026-03-06',dt:'06 Mar 10:18',sig:'LT LONG',inst:'LT',dir:'L',gate:'G4',reason:'OI build 8% below 12% threshold',entry:3580,exit:3624,pnl:+44,correct:true,lesson:'Taken after OI crossed threshold. G4 fire slightly delayed.'},
  {date:'2026-03-06',dt:'06 Mar 11:40',sig:'HDFCBANK SHORT',inst:'HDFCBANK',dir:'S',gate:'G2',reason:'FII net buyer — no bear signal',entry:0,exit:0,pnl:-48,correct:true,lesson:'Stock rose with FII buying. G2 correctly blocked short.'},
  {date:'2026-03-05',dt:'05 Mar 10:08',sig:'ICICIBANK LONG',inst:'ICICIBANK',dir:'L',gate:'G5',reason:'R:R 1:1.8 — below threshold',entry:0,exit:0,pnl:+84,correct:false,lesson:'R:R issue again. Most common false block in this system.'},
  {date:'2026-03-05',dt:'05 Mar 13:05',sig:'BAJFINANCE LONG',inst:'BAJFINANCE',dir:'L',gate:'G1',reason:'Weekly regime not confirmed',entry:0,exit:0,pnl:-94,correct:true,lesson:'Positional trade would have lost. G1 weekly check essential.'},
];

function btPre(btn,p){
  document.querySelectorAll('.btpre').forEach(b=>b.classList.remove('on'));
  btn.classList.add('on');
  const ref=new Date('2026-03-27');
  let from=new Date(ref);
  if(p==='1W')from.setDate(ref.getDate()-7);
  else if(p==='2W')from.setDate(ref.getDate()-14);
  else if(p==='1M')from.setMonth(ref.getMonth()-1);
  else if(p==='3M')from.setMonth(ref.getMonth()-3);
  else from=new Date('2026-01-01');
  const fmt=d=>d.toISOString().split('T')[0];
  document.getElementById('bt-from').value=fmt(from);
  document.getElementById('bt-to').value=fmt(ref);
  runBT();
}

function btReset(){
  document.querySelectorAll('.btsel').forEach(s=>s.value='ALL');
  btPre(document.querySelector('.btpre'),'1W');
}

let btFiltered=[];
let btPage=0;
const BT_PER_PAGE=10;

function runBT(){
  const inst=document.getElementById('bt-inst').value;
  const gate=document.getElementById('bt-gate').value;
  const dir=document.getElementById('bt-dir').value;
  const res=document.getElementById('bt-res').value;
  const from=document.getElementById('bt-from').value||'2000-01-01';
  const to=document.getElementById('bt-to').value||'2099-12-31';
  btFiltered=btData.filter(d=>{
    if(d.date<from||d.date>to)return false;
    if(inst!=='ALL'&&!d.inst.includes(inst))return false;
    if(gate!=='ALL'&&d.gate!==gate)return false;
    if(dir!=='ALL'&&d.dir!==dir)return false;
    if(res!=='ALL'&&String(d.correct)!==res)return false;
    return true;
  });
  const tot=btFiltered.length;
  const cor=btFiltered.filter(d=>d.correct).length;
  const pct=tot?Math.round(cor/tot*100):0;
  const saved=btFiltered.filter(d=>d.correct&&d.pnl<0).reduce((a,d)=>a+Math.abs(d.pnl),0);
  const missed=btFiltered.filter(d=>!d.correct&&d.pnl>0).reduce((a,d)=>a+d.pnl,0);
  const edge=saved-missed;
  document.getElementById('bts-total').textContent=tot;
  document.getElementById('bts-correct').textContent=pct+'%';
  document.getElementById('bts-correct').style.color=pct>=70?'var(--go)':pct>=55?'var(--am)':'var(--st)';
  document.getElementById('bts-saved').textContent='+'+saved+' pts';
  document.getElementById('bts-missed').textContent='-'+missed+' pts';
  document.getElementById('bts-edge').textContent=(edge>=0?'+':'')+edge+' pts';
  document.getElementById('bts-edge').style.color=edge>=0?'var(--go)':'var(--st)';
  btPage=0;
  btRenderPage();
}

function btRenderPage(){
  const tot=btFiltered.length;
  const pages=Math.max(1,Math.ceil(tot/BT_PER_PAGE));
  btPage=Math.max(0,Math.min(btPage,pages-1));
  const slice=btFiltered.slice(btPage*BT_PER_PAGE,(btPage+1)*BT_PER_PAGE);
  const start=btPage*BT_PER_PAGE+1;
  const end=Math.min((btPage+1)*BT_PER_PAGE,tot);
  document.getElementById('bt-showing').textContent=tot?start+'–'+end+' of '+tot+' signals':'0 signals';
  document.getElementById('bt-page-info').textContent=(btPage+1)+'/'+pages;
  const prev=document.getElementById('bt-prev');
  const next=document.getElementById('bt-next');
  if(prev){prev.disabled=btPage===0;prev.style.opacity=btPage===0?'0.35':'1';}
  if(next){next.disabled=btPage>=pages-1;next.style.opacity=btPage>=pages-1?'0.35':'1';}
  document.getElementById('bt-body').innerHTML=!tot
    ?'<tr><td colspan="10" style="text-align:center;padding:20px;color:var(--t2)">No signals match the selected filters</td></tr>'
    :slice.map(d=>{
      const hp=d.entry>0;
      const ps=d.pnl>0?'<span style="color:var(--go);font-weight:600">+'+d.pnl+' pts</span>':'<span style="color:var(--st);font-weight:600">'+d.pnl+' pts</span>';
      const pw=!hp?(d.pnl>0?'<span style="color:var(--go)">+'+d.pnl+' pts</span>':'<span style="color:var(--st)">'+d.pnl+' pts</span>'):'<span style="color:var(--t2)">N/A</span>';
      const sym = d.sig ? d.sig.split(' ')[0] : 'NIFTY';
      const sigLink = `<span style="color:#38beff;cursor:pointer;font-weight:700" onclick="event.stopPropagation();openChart('${sym}','${d.date}')" title="Click to view chart">${d.sig}</span>`;
      return '<tr style="cursor:pointer" onclick="openChart(\''+sym+'\',\''+d.date+'\')">'+
        '<td style="color:var(--t2);font-size:8px;white-space:nowrap">'+d.dt+'</td>'+
        '<td style="font-weight:600;color:var(--t0)">'+sigLink+'</td>'+
        '<td style="color:'+(d.dir==='L'?'var(--go)':'var(--st)')+';font-weight:700">'+(d.dir==='L'?'LONG':'SHORT')+'</td>'+
        '<td style="color:var(--am);font-weight:600">'+d.gate+'</td>'+
        '<td style="color:var(--t2);font-size:9px">'+d.reason+'</td>'+
        '<td style="color:var(--t2)">'+(hp?d.entry.toLocaleString('en-IN'):'—')+'</td>'+
        '<td style="color:var(--t2)">'+(hp?d.exit.toLocaleString('en-IN'):'—')+'</td>'+
        '<td>'+(hp?ps:pw)+'</td>'+
        '<td>'+(d.correct?'<span class="btcor bty">YES ✓</span>':'<span class="btcor btn2">NO ✗</span>')+'</td>'+
        '<td style="font-size:8px;color:var(--t2)">'+d.lesson+'</td>'+
        '</tr>';
    }).join('');
}

function btPageNav(dir){
  btPage+=dir;
  btRenderPage();
}
btPre(document.querySelector('.btpre'),'1W');

// ═══════════════════════════════════════════════
// VOLUME RADAR
// ═══════════════════════════════════════════════
(function buildVolRadar(){
  document.getElementById('del-body').innerHTML='<tr><td colspan="5" style="text-align:center;color:var(--t2);padding:16px">Waiting for live stock flow...</td></tr>';
  document.getElementById('pvd-body').innerHTML='<div style="color:var(--t2);font-size:9px;padding:8px 0">Waiting for live divergence scan...</div>';
  document.getElementById('oic-body').innerHTML='<div style="color:var(--t2);font-size:9px;padding:8px 0">Waiting for live chain concentration...</div>';
  document.getElementById('vs-body').innerHTML='<div style="color:var(--t2);font-size:9px;padding:8px 0">Waiting for live volume surges...</div>';
  document.getElementById('fii-body').innerHTML='<div style="color:var(--t2);font-size:9px;padding:8px 0">Waiting for live FII / OI context...</div>';
})();

// ═══════════════════════════════════════════════
// TICKER
// ═══════════════════════════════════════════════
const tItems=[
  'Connecting live market summary...',
  'Waiting for gate engine + stock OI stream...',
  'Historical mode will replace all visible panels for selected date',
];
document.getElementById('tktrack').innerHTML=
  [...tItems,...tItems].map(i=>'<span class="tki '+(i.w?'w':'')+'">'+(typeof i==='string'?i:i.t)+'</span><span style="color:var(--t2);padding:0 6px">·</span>').join('');

// ═══════════════════════════════════════════════
// NOTIFICATION
// ═══════════════════════════════════════════════
function notify(msg){
  const el=document.getElementById('notif');
  el.textContent=msg;el.style.display='block';
  clearTimeout(notify._t);
  notify._t=setTimeout(()=>el.style.display='none',2400);
}

// ═══════════════════════════════════════════════
// INIT
// ═══════════════════════════════════════════════
setMode('intraday');
setTimeout(()=>notify('NSE EDGE v5 ready · Connecting to live data...'),400);

// ═══════════════════════════════════════════════
// LIVE DATA — WEBSOCKET CLIENT
// ═══════════════════════════════════════════════
(function startLiveData(){
  const _backendBase = window.location.hostname === 'kvishnublr.github.io'
    ? 'https://nse-edge-backend.fly.dev'
    : 'http://localhost:8765';
  const WS_URL = window.location.hostname === 'kvishnublr.github.io'
    ? 'wss://nse-edge-backend.fly.dev/ws'
    : 'ws://localhost:8765/ws';

  let ws = null;
  let reconnectDelay = 2000;
  let reconnectTimer = null;
  let alive = true;
  const lpill = document.querySelector('.lpill');
  const ldot  = document.querySelector('.ldot');

  function setStatus(live){
    if(ldot){ ldot.style.background = live ? 'var(--go)' : 'var(--st)'; }
    if(lpill){ lpill.style.color = live ? 'var(--go)' : 'var(--st)'; }
    const l = document.querySelector('.tklbl');
    if(l){ l.textContent = live ? 'LIVE' : 'OFFLINE'; }
  }

  function connect(){
    if(!alive) return;
    try { ws = new WebSocket(WS_URL); } catch(e){ scheduleReconnect(); return; }

    ws.onopen = () => {
      reconnectDelay = 2000;
      setStatus(true);
      notify('Live data connected');
      startHeartbeat();
    };

    ws.onmessage = (evt) => {
      if(window._histMode) return;  // frozen in historical mode
      try {
        handle(JSON.parse(evt.data));
      } catch(e) {
        console.error("WebSocket message parse error:", e, evt.data);
        notify("Error: Invalid data from server");
      }
    };

    ws.onerror = (ev) => {
      console.error("WebSocket error:", ev);
      notify("Connection error - will reconnect");
      setStatus(false);
    };

    ws.onclose = () => {
      setStatus(false);
      scheduleReconnect();
    };
  }

  function scheduleReconnect(){
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(()=>{ connect(); }, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 1.5, 30000);
  }

  // ── Heartbeat ──────────────────────────────────
  let hbTimer = null;
  function startHeartbeat(){
    clearInterval(hbTimer);
    hbTimer = setInterval(()=>{
      if(ws && ws.readyState === WebSocket.OPEN){
        ws.send(JSON.stringify({type:'ping'}));
      }
    }, 30000);
  }

  // ── Mode change → tell server ──────────────────
  const origSetMode = window.setMode;
  window.setMode = function(m){
    origSetMode(m);
    if(ws && ws.readyState === WebSocket.OPEN){
      ws.send(JSON.stringify({type:'set_mode', mode:m}));
    }
  };

  // ═══════════════════════════════════════════════
  // MESSAGE HANDLER
  // ═══════════════════════════════════════════════
  function _cardMeta(sym){
    const m = {
      NIFTY: 'NIFTY 50', BANKNIFTY: 'BANK NIFTY', ICICIBANK: 'ICICI BANK', SBIN: 'SBIN',
      HDFCBANK: 'HDFC BANK', AXISBANK: 'AXIS BANK', KOTAKBANK: 'KOTAK BANK',
      INDUSINDBK: 'INDUSIND BK', LT: 'LT', TCS: 'TCS', RELIANCE: 'RELIANCE',
      BAJFINANCE: 'BAJAJ FIN', TATASTEEL: 'TATA STEEL', INFY: 'INFY',
      MARUTI: 'MARUTI', SUNPHARMA: 'SUN PHARMA', TATAMOTORS: 'TATA MOTORS'
    };
    return m[sym] || sym;
  }

  function _activeScore(s){
    if(s && typeof s.score === 'number') return s.score;
    const pc = Number(s && s.pc || 0);
    const chg = Math.abs(Number(s && s.chg_pct || 0));
    const oi = Math.max(0, Number(s && s.oi_chg_pct || 0));
    const vol = Number(s && s.vol_ratio || 0);
    const rs = Math.max(0, Number(s && s.rs_pct || 0));
    return Math.round(pc * 20 + chg * 8 + oi * 1.5 + vol * 4 + rs * 3);
  }

  function _firstWeakGate(s){
    const order = [['g1','G1 Regime'],['g2','G2 Smart$'],['g3','G3 Struct'],['g4','G4 Trigger'],['g5','G5 Risk']];
    for(const [k, lbl] of order){
      if((s && s[k]) !== 'go') return lbl;
    }
    return 'All Gates';
  }

  function _topActiveStocks(stocks){
    if(!stocks || !stocks.length) return [];
    return [...stocks]
      .filter(s => s && !['NIFTY','BANKNIFTY','INDIAVIX'].includes(s.symbol))
      .sort((a,b) => (_activeScore(b) - _activeScore(a)) || (Math.abs((b.chg_pct||0)) - Math.abs((a.chg_pct||0))))
      .slice(0, 2);
  }

  function _setCard(cardIdx, sym, priceObj, stockObj, chainObj){
    const card = document.querySelectorAll('#inst .ic')[cardIdx];
    if(!card) return;
    const nameEl = card.querySelector('.icn');
    const priceEl = card.querySelector('.icp');
    const chgEl = card.querySelector('.icc');
    const biasEl = card.querySelector('.ics');
    const auxEl = card.querySelector('.ico');
    if(nameEl){
      nameEl.textContent = _cardMeta(sym);
      nameEl.dataset.tvsym = sym;
    }
    if(priceEl && priceObj && priceObj.price != null){
      const up = (priceObj.chg_pts || 0) >= 0;
      priceEl.textContent = priceObj.price < 100 ? priceObj.price.toFixed(2) : Math.round(priceObj.price).toLocaleString('en-IN');
      priceEl.style.color = up ? 'var(--go)' : 'var(--st)';
      if(chgEl){
        const sign = up ? '+' : '';
        chgEl.textContent = sign + (priceObj.chg_pts || 0).toFixed(0) + ' · ' + sign + (priceObj.chg_pct || 0).toFixed(2) + '%';
        chgEl.style.color = up ? 'var(--go)' : 'var(--st)';
      }
      if(biasEl){
        const upBias = (priceObj.chg_pct || 0) >= 0;
        biasEl.textContent = upBias ? 'LONG BIAS' : 'SHORT BIAS';
        biasEl.className = 'ics ' + (upBias ? 'bg' : 'br');
      }
    }
    if(auxEl){
      if(sym === 'NIFTY' && chainObj) auxEl.textContent = 'PCR ' + Number(chainObj.pcr || 0).toFixed(2);
      else if(stockObj && stockObj.oi_chg_pct != null) auxEl.textContent = 'OI ' + (stockObj.oi_chg_pct >= 0 ? '+' : '') + Number(stockObj.oi_chg_pct).toFixed(1) + '%';
      else auxEl.textContent = 'OI —';
    }
    card.dataset.symbol = sym;
  }

  function _refreshTopCards(){
    const prices = window._livePrices || {};
    const stocks = window._lastStocks || [];
    const chain = window._lastChain || null;
    _setCard(0, 'NIFTY', prices.NIFTY, null, chain);
    _setCard(1, 'BANKNIFTY', prices.BANKNIFTY, (stocks||[]).find(s => s.symbol === 'BANKNIFTY'), chain);
    const top = _topActiveStocks(stocks);
    _setCard(2, top[0] ? top[0].symbol : 'ICICIBANK', top[0] ? prices[top[0].symbol] : prices.ICICIBANK, top[0] || null, chain);
    _setCard(3, top[1] ? top[1].symbol : 'RELIANCE', top[1] ? prices[top[1].symbol] : prices.RELIANCE, top[1] || null, chain);
  }

  function _renderLiveLevels(){
    const fmt = n => Math.round(Number(n || 0)).toLocaleString('en-IN');
    const prices = window._livePrices || {};
    const chain = window._lastChain || {};
    const stocks = window._lastStocks || [];
    const fii = window._lastFii || {};
    const nifty = prices.NIFTY;
    const bank = prices.BANKNIFTY;
    const lv2r = document.querySelector('#tp-levels .lv2');
    if(!lv2r || lv2r.children.length < 2 || !nifty) return;
    const np = Math.round(nifty.price || 0), nh = Math.round(nifty.high || np), nl = Math.round(nifty.low || np);
    const atm = chain.atm ? Math.round(chain.atm) : Math.round(np / 50) * 50;
    const mp = chain.max_pain ? Math.round(chain.max_pain) : atm;
    const top = _topActiveStocks(stocks);
    const topSym = top[0];
    lv2r.children[0].innerHTML = `
      <div class="lvhd">NIFTY 50 — LIVE MAP (DERIVED)</div>
      <div class="lvl"><span class="lk">Weekly supply</span><span class="lp">${fmt(atm+500)}</span><span class="lt br">RESIST</span></div>
      <div class="lvl"><span class="lk">Day High</span><span class="lp cg">${fmt(nh)}</span><span class="lt br">RESIST</span></div>
      <div class="lvl"><span class="lk">CE wall (OI)</span><span class="lp cr">${fmt(atm+100)}</span><span class="lt br">CE WALL</span></div>
      <div class="lvl" style="background:var(--am-d);border-color:var(--am-b)"><span class="lk">Max Pain</span><span class="lp ca">${fmt(mp)}</span><span class="lt ba">GRAVITY</span></div>
      <div class="lvl"><span class="lk">VWAP live</span><span class="lp cb">${fmt(np)}</span><span class="lt bb">LIVE</span></div>
      <div class="lvl" style="background:var(--go-d);border-color:var(--go-b)"><span class="lk">PE wall (OI)</span><span class="lp cg">${fmt(atm-100)}</span><span class="lt bg">PE WALL</span></div>
      <div class="lvl"><span class="lk">Day Low</span><span class="lp ca">${fmt(nl)}</span><span class="lt ba">LOW</span></div>
      <div class="lvl"><span class="lk">Weekly demand</span><span class="lp">${fmt(atm-500)}</span><span class="lt bg">SUPPORT</span></div>`;
    let rhs = '';
    if(bank){
      const bp = Math.round(bank.price || 0);
      const batm = Math.round(bp / 100) * 100;
      rhs += `
        <div class="lvhd">BANK NIFTY — LIVE MAP (DERIVED)</div>
        <div class="lvl"><span class="lk">Weekly supply</span><span class="lp">${fmt(batm+2000)}</span><span class="lt br">RESIST</span></div>
        <div class="lvl"><span class="lk">CE wall (OI)</span><span class="lp cr">${fmt(batm+1000)}</span><span class="lt br">CE WALL</span></div>
        <div class="lvl"><span class="lk">Live price</span><span class="lp cb">${fmt(bp)}</span><span class="lt bb">SPOT</span></div>
        <div class="lvl" style="background:var(--go-d);border-color:var(--go-b)"><span class="lk">PE wall (OI)</span><span class="lp cg">${fmt(batm-1000)}</span><span class="lt bg">PE WALL</span></div>
        <div class="lvl"><span class="lk">Weekly demand</span><span class="lp">${fmt(batm-2000)}</span><span class="lt bg">SUPPORT</span></div>`;
    }
    if(topSym){
      rhs += `
        <div class="lvhd" style="margin-top:8px">${_cardMeta(topSym.symbol)} — ACTIVE SETUP (DERIVED)</div>
        <div class="lvl" style="background:var(--go-d);border-color:var(--go-b)"><span class="lk">Price</span><span class="lp cg">${fmt(topSym.price || 0)}</span><span class="lt bg">LIVE</span></div>
        <div class="lvl"><span class="lk">Change</span><span class="lp ${topSym.chg_pct >= 0 ? 'cg' : 'cr'}">${topSym.chg_pct >= 0 ? '+' : ''}${Number(topSym.chg_pct || 0).toFixed(2)}%</span><span class="lt ${topSym.chg_pct >= 0 ? 'bg' : 'br'}">MOVE</span></div>
        <div class="lvl"><span class="lk">OI Change</span><span class="lp cg">${topSym.oi_chg_pct >= 0 ? '+' : ''}${Number(topSym.oi_chg_pct || 0).toFixed(1)}%</span><span class="lt bg">OI</span></div>
        <div class="lvl"><span class="lk">Score</span><span class="lp cb">${_activeScore(topSym)}</span><span class="lt bb">TOP</span></div>`;
    }
    lv2r.children[1].innerHTML = rhs;
    const wi = document.querySelector('#tp-levels .wbox .wi');
    if(wi){
      const parts = [`Nifty ${Math.abs(np-mp)} pts ${np>=mp?'above':'below'} Max Pain (${fmt(mp)})`];
      if(chain.pcr != null) parts.push(`PCR ${Number(chain.pcr).toFixed(2)} ${chain.pcr >= 1.1 ? 'bullish' : chain.pcr <= 0.8 ? 'bearish' : 'neutral'}`);
      if(fii.fii_net != null) parts.push(`FII ${fii.fii_net >= 0 ? 'net buy' : 'net sell'} ₹${Math.abs(Number(fii.fii_net || 0)).toFixed(0)} Cr`);
      if(topSym) parts.push(`Top active stock: ${_cardMeta(topSym.symbol)} (${topSym.chg_pct >= 0 ? '+' : ''}${Number(topSym.chg_pct || 0).toFixed(2)}%, OI ${topSym.oi_chg_pct >= 0 ? '+' : ''}${Number(topSym.oi_chg_pct || 0).toFixed(1)}%)`);
      wi.textContent = parts.join(' · ');
    }
    const kzCards = document.getElementById('kzcards');
    if(kzCards){
      const cards = kzCards.querySelectorAll('.zcard');
      if(cards.length >= 4) cards[3].querySelector('.zd').textContent = `Exit all positions. OI distortion. Max Pain gravity at ${fmt(mp)}.`;
    }
  }

  function _refreshPlaybookSummary(){
    const macro = window._lastMacro || {};
    const chain = window._lastChain || {};
    const gateS = window._lastGateSummary || {};
    const pbBias = document.getElementById('pb-bias');
    const pbAlV = document.getElementById('pb-al-v');
    const pbAlS = document.getElementById('pb-al-s');
    const pbWnV = document.getElementById('pb-wn-v');
    const pbWnS = document.getElementById('pb-wn-s');
    if(!pbBias || !pbAlV || !pbAlS) return;

    const niftyChg = Number(macro.nifty_chg || 0);
    const pcr = Number(chain.pcr || 0);
    const verdict = gateS.verdict || '';
    const g1 = gState[1], g2 = gState[2], g3 = gState[3], g5 = gState[5];

    let biasText = 'NEUTRAL', biasCls = 'ca';
    if((niftyChg <= -0.5 && pcr <= 0.9) || g1 === 'st' || g3 === 'st') {
      biasText = 'BEARISH'; biasCls = 'cr';
    } else if((niftyChg >= 0.5 && pcr >= 1.1) || (g2 === 'go' && g1 !== 'st')) {
      biasText = 'BULLISH'; biasCls = 'cg';
    }

    let allowedText = 'WAIT', allowedSub = 'Need confirmation first', allowedCls = 'ca';
    if(verdict === 'NO TRADE' || g1 === 'st' || g5 === 'st') {
      allowedText = 'NO TRADE';
      allowedSub = 'Stand down - regime/risk invalid';
      allowedCls = 'cr';
    } else if((niftyChg <= -0.5 && pcr <= 0.9) || g3 === 'st') {
      allowedText = 'SHORTS ONLY';
      allowedSub = 'Downtrend pressure - avoid longs';
      allowedCls = 'cr';
    } else if((niftyChg >= 0.5 && pcr >= 1.1) || g2 === 'go') {
      allowedText = 'LONGS ONLY';
      allowedSub = 'Bullish flow still supportive';
      allowedCls = 'cg';
    }

    pbBias.textContent = biasText;
    pbBias.className = 'pbv ' + biasCls;
    pbAlV.textContent = allowedText;
    pbAlV.className = 'pbv ' + allowedCls;
    pbAlS.textContent = allowedSub;
    const pbConfS = document.getElementById('pb-conf-s');
    if(pbConfS) pbConfS.textContent = `${Number(gateS.pass_count || 0)} / 5 gates`;
    const pbMkt = document.getElementById('pb-mkt');
    if(pbMkt){
      const ist = new Date(new Date().toLocaleString('en-US',{timeZone:'Asia/Kolkata'}));
      const mins = ist.getHours()*60 + ist.getMinutes();
      const open = mins >= 555 && mins < 930;
      pbMkt.textContent = open ? 'Market open' : 'Market closed';
      pbMkt.className = 'pbs ' + (open ? 'cg' : 'ca');
      if(pbWnV && pbWnS){
        if(!open){ pbWnV.textContent = 'Closed'; pbWnV.className = 'pbv ca'; pbWnS.textContent = 'Review only'; }
        else if(mins < 600){ pbWnV.textContent = '09:15 – 10:00'; pbWnV.className = 'pbv ca'; pbWnS.textContent = 'Discovery / no chase'; }
        else if(mins < 810){ pbWnV.textContent = '10:00 – 13:30'; pbWnV.className = 'pbv cg'; pbWnS.textContent = 'Trend zone active'; }
        else if(mins < 870){ pbWnV.textContent = '13:30 – 14:30'; pbWnV.className = 'pbv cb'; pbWnS.textContent = 'Drift / partial profit'; }
        else { pbWnV.textContent = '14:30 – 15:30'; pbWnV.className = 'pbv cr'; pbWnS.textContent = 'Expiry risk / exit bias'; }
      }
    }
  }

  function _renderOIPulseLive(){
    const el = document.getElementById('oip');
    if(!el) return;
    const stocks = (window._lastStocks || []).filter(s => s && s.symbol && s.symbol !== 'BANKNIFTY');
    if(!stocks.length) return;
    const rows = [...stocks]
      .sort((a,b) => Math.abs(Number(b.oi_chg_pct||0)) - Math.abs(Number(a.oi_chg_pct||0)))
      .slice(0,5)
      .map(s => {
        const oi = Number(s.oi_chg_pct || 0);
        const chg = Number(s.chg_pct || 0);
        const c = oi > 8 && chg >= 0 ? 'go' : oi > 8 && chg < 0 ? 'cr' : 'am';
        const lbl = oi > 8 && chg >= 0 ? 'Fresh longs' : oi > 8 && chg < 0 ? 'Short buildup' : 'Watch';
        return {t:s.symbol, oi:(oi>=0?'+':'')+oi.toFixed(1)+'%', pt:(chg>=0?'+':'')+chg.toFixed(2), lbl, c};
      });
    el.innerHTML = rows.map(d=>'<div class="oip '+d.c+'"><span class="oip-t">'+d.t+'</span><span class="oip-oi">'+d.oi+'</span><span class="oip-pt">'+d.pt+'%</span><span class="oip-lbl">'+d.lbl+'</span></div>').join('');
  }

  function _renderMacroContext(){
    const macro = window._lastMacro || {};
    const chain = window._lastChain || {};
    const fii = window._lastFii || {};
    const setTxt = (id, txt, cls='') => { const el = document.getElementById(id); if(el){ el.textContent = txt; if(cls) el.className = cls; } };
    if(macro.nifty != null){
      setTxt('rp-nifty', Math.round(Number(macro.nifty)).toLocaleString('en-IN'), 'miv ' + (Number(macro.nifty_chg||0) >= 0 ? 'cg' : 'cr'));
      setTxt('rp-nifty-chg', (Number(macro.nifty_chg||0) >= 0 ? '+' : '') + Number(macro.nifty_chg||0).toFixed(2) + '%', 'mic ' + (Number(macro.nifty_chg||0) >= 0 ? 'cg' : 'cr'));
      setTxt('rp-nifty-lbl', 'Live index move');
    }
    if(chain.pcr != null){
      const pcr = Number(chain.pcr||0);
      const cls = pcr >= 1.1 ? 'cg' : pcr <= 0.8 ? 'cr' : 'ca';
      const bias = pcr >= 1.1 ? 'PUT WRITERS' : pcr <= 0.8 ? 'CALL WRITERS' : 'BALANCED';
      setTxt('rp-pcrv', bias, 'miv ' + cls);
      setTxt('rp-pcrchg', 'PCR ' + pcr.toFixed(2), 'mic ' + cls);
      setTxt('rp-pcrlbl', pcr >= 1.1 ? 'Bullish OI' : pcr <= 0.8 ? 'Bearish OI' : 'Neutral OI', 'mit ' + cls);
    }
    const badge = document.getElementById('macro-badge');
    if(badge) badge.textContent = 'LIVE CORE';
    if(fii.fii_net != null){
      const cEl = document.getElementById('rp-fii-lbl');
      const dEl = document.getElementById('rp-dii');
      if(cEl) cEl.className = 'mic ' + (Number(fii.fii_net||0) >= 0 ? 'cg' : 'cr');
      if(dEl){
        const dii = Number(fii.dii_net || 0);
        dEl.textContent = 'DII ' + (dii >= 0 ? '+' : '') + '₹' + Math.abs(dii).toFixed(0) + ' Cr';
      }
    }
    const setMini = (valId, chgId, val, chg, prefix='', suffix='') => {
      const vEl = document.getElementById(valId);
      const cEl = document.getElementById(chgId);
      if(vEl && val != null){
        vEl.textContent = prefix + Number(val).toFixed(2) + suffix;
        vEl.className = (vEl.className || '').replace(/\b(cg|cr|ca)\b/g,'').trim() + ' ' + (Number(chg||0) >= 0 ? 'cg' : 'cr');
      }
      if(cEl && chg != null){
        cEl.textContent = (Number(chg) >= 0 ? '+' : '') + Number(chg).toFixed(2) + '%';
        cEl.className = 'mit ' + (Number(chg) >= 0 ? 'cg' : 'cr');
      }
    };
    setMini('rp-us10y','rp-us10y-chg', macro.us10y, macro.us10y_chg, '', '%');
    setMini('rp-dxy','rp-dxy-chg', macro.dxy, macro.dxy_chg);
    setMini('rp-crude','rp-crude-chg', macro.crude, macro.crude_chg, '$');
  }

  function _renderSwingLive(){
    const body = document.getElementById('sw-body');
    if(!body) return;
    const stocks = (window._lastStocks || []).filter(s => s && s.symbol && s.symbol !== 'BANKNIFTY');
    if(!stocks.length) return;
    const secM = {HDFCBANK:'Banking',ICICIBANK:'Banking',AXISBANK:'Banking',KOTAKBANK:'Banking',INDUSINDBK:'Banking',SBIN:'PSU Bank',TCS:'IT',INFY:'IT',MARUTI:'Auto',TATAMOTORS:'Auto',LT:'Infra',BAJFINANCE:'NBFC',RELIANCE:'Energy'};
    const picks = [...stocks]
      .map(s => {
        const score = _activeScore(s);
        const atrPct = Number(s.atr_pct || Math.max(Math.abs(Number(s.chg_pct||0))*0.8, 1.2));
        const atr = Math.max(Number(s.price||0) * atrPct / 100, Number(s.price||0) * 0.004);
        const entry = Number(s.price||0);
        const stop = entry - atr * 1.2;
        const target = entry + atr * 2.5;
        const rr = atr > 0 ? ((target-entry)/(entry-stop)).toFixed(1) : '0.0';
        const pc = Number(s.pc || 0);
        const sigLbl = pc >= 3 ? 'EXECUTE' : pc >= 2 ? 'WATCH' : 'NO TRADE';
        const sigCss = pc >= 3 ? 'color:var(--go);border:1px solid var(--go-b)' : pc >= 2 ? 'color:var(--am);border:1px solid var(--am-b)' : 'color:var(--t2);border:1px solid var(--b1)';
        const outcome = (window._histData && Array.isArray(window._histData.stock_picks) ? (window._histData.stock_picks.find(p => p.sym === s.symbol) || {}).outcome : '') || 'LIVE';
        return {sym:s.symbol, sector:secM[s.symbol]||'Market', price:Math.round(Number(s.price||0)), setup:Number(s.chg_pct||0)>=0?'MOMENTUM':'REVERSAL', entry:`${Math.round(entry-atr*0.3).toLocaleString('en-IN')}–${Math.round(entry+atr*0.2).toLocaleString('en-IN')}`, target:Math.round(target), stop:Math.round(stop), rr, pc, sigLbl, sigCss, outcome, hold:pc>=3?'1–3d':'Watch', score};
      })
      .sort((a,b)=>b.score-a.score)
      .slice(0,8);
    body.innerHTML = picks.map((d,i)=>`<tr style="${d.score>=80?'background:rgba(0,232,122,.03)':''}"><td><span style="color:var(--t2);font-size:8px;margin-right:3px">${i+1}</span><span class="tv-link" data-tvsym="${d.sym}" style="font-weight:600;color:var(--t0)">${d.sym}</span></td><td style="color:var(--t2);font-size:9px">${d.sector}</td><td style="font-weight:600">${d.price.toLocaleString('en-IN')}</td><td><span style="font-size:8px;font-weight:700;padding:1px 6px;border-radius:2px;${d.setup==='MOMENTUM'?'color:var(--bl);border:1px solid var(--bl-b)':'color:var(--am);border:1px solid var(--am-b)'}">${d.setup}</span></td><td style="color:var(--am)">${d.entry}</td><td style="color:var(--go);font-weight:600">${d.target.toLocaleString('en-IN')}</td><td style="color:var(--st)">${d.stop.toLocaleString('en-IN')}</td><td style="color:${Number(d.rr)>=2.5?'var(--go)':Number(d.rr)>=2?'var(--am)':'var(--st)'};font-weight:600">1:${d.rr}</td><td>${_gateDots('go', d.pc>=2?'go':'am', d.pc>=2?'go':'am', d.pc>=3?'go':'wt', d.pc>=3?'go':'st')}</td><td style="color:${d.pc>=3?'var(--go)':d.pc>=2?'var(--am)':'var(--t2)'};font-weight:700">${d.pc}/5</td><td><span style="font-size:8px;font-weight:700;padding:1px 6px;border-radius:2px;${d.sigCss}">${d.sigLbl}</span></td><td style="color:${d.outcome==='PROFIT'?'var(--go)':d.outcome==='LOSS'?'var(--st)':'var(--t2)'};font-size:9px">${d.outcome.replace('_',' ')}</td><td style="color:var(--t2)">${d.hold}</td><td><span style="font-size:10px;font-weight:700;color:${d.score>=80?'var(--go)':d.score>=65?'var(--am)':'var(--st)'}">${d.score}</span><div class="swbar"><div class="swbf" style="width:${Math.min(99,d.score)}%;background:${d.score>=80?'var(--go)':d.score>=65?'var(--am)':'var(--st)'}"></div></div></td></tr>`).join('');
  }

  function _renderVolumeRadarLive(){
    const stocks = (window._lastStocks || []).filter(s => s && s.symbol && s.symbol !== 'BANKNIFTY');
    if(!stocks.length) return;
    const byScore = [...stocks].sort((a,b)=>_activeScore(b)-_activeScore(a));
    const delBody = document.getElementById('del-body');
    if(delBody){
      delBody.innerHTML = byScore.slice(0,8).map(s=>{
        const del = Math.max(28, Math.min(72, Math.round(40 + Number(s.oi_chg_pct||0)*0.8 + Math.abs(Number(s.chg_pct||0))*2)));
        const avg = Math.max(30, del - Math.round(Math.abs(Number(s.chg_pct||0))*4) - 6);
        const dlt = del - avg;
        return '<tr><td style="font-weight:600;color:var(--t0)">'+s.symbol+'</td><td style="color:'+(del>55?'var(--go)':del>44?'var(--am)':'var(--t2)')+';font-weight:600">'+del+'%</td><td style="color:var(--t2)">'+avg+'%</td><td style="color:'+(dlt>0?'var(--go)':'var(--st)')+';font-weight:600">'+(dlt>0?'+':'')+dlt+'%</td><td style="font-size:8px;font-weight:700;color:'+(dlt>10?'var(--go)':dlt>0?'var(--am)':'var(--st)')+'">'+(dlt>10?'INST BUY':dlt>0?'MILD BUY':'SELLING')+'</td></tr>';
      }).join('');
    }
    const pvdBody = document.getElementById('pvd-body');
    if(pvdBody){
      pvdBody.innerHTML = byScore.slice(0,5).map(s => {
        const vol = Number(s.volume || 0);
        const volSignal = vol ? ((Math.abs(Number(s.oi_chg_pct||0))*4).toFixed(0)) : '0';
        const cls = Number(s.chg_pct||0) > 0 && Number(s.oi_chg_pct||0) > 8 ? 'cg' : Number(s.chg_pct||0) < 0 && Number(s.oi_chg_pct||0) > 8 ? 'cr' : 'ca';
        const lbl = cls==='cg' ? 'STRONG MOVE' : cls==='cr' ? 'STRONG SELL' : 'MIXED';
        return '<div class="vrrow"><span class="vrn">'+s.symbol+'</span><span style="font-size:9px;color:var(--t1)">'+(Number(s.chg_pct||0)>=0?'+':'')+Number(s.chg_pct||0).toFixed(2)+'% / '+(Number(s.oi_chg_pct||0)>=0?'+':'')+Number(s.oi_chg_pct||0).toFixed(0)+'%</span><span class="vrv '+cls+'">'+lbl+'</span></div>';
      }).join('');
    }
    const oicBody = document.getElementById('oic-body');
    if(oicBody && window._lastChain && Array.isArray(window._lastChain.strikes)){
      const totalCall = Number(window._lastChain.total_call_oi || 1), totalPut = Number(window._lastChain.total_put_oi || 1);
      const rows = [...window._lastChain.strikes].map(r => ([
        {strike:r.strike+' CE', pct:100*Number(r.call_oi||0)/totalCall, inst:'NIFTY', sig:'CE WALL', c:'cr'},
        {strike:r.strike+' PE', pct:100*Number(r.put_oi||0)/totalPut, inst:'NIFTY', sig:'PE FLOOR', c:'cg'}
      ])).flat().filter(r=>r.pct>=8).sort((a,b)=>b.pct-a.pct).slice(0,4);
      oicBody.innerHTML = rows.map(r=>'<div class="vrrow"><span class="vrn">'+r.strike+'</span><span style="font-size:8px;color:var(--t2)">'+r.inst+'</span><span class="vrv '+r.c+'">'+r.pct.toFixed(1)+'%</span><span style="font-size:8px;font-weight:700;color:'+(r.sig.includes('WALL')?'var(--st)':'var(--go)')+'">'+r.sig+'</span></div>').join('');
    }
    const vsBody = document.getElementById('vs-body');
    if(vsBody){
      vsBody.innerHTML = byScore.slice(0,5).map(s=>{
        const mult = Math.max(1.2, Math.abs(Number(s.oi_chg_pct||0))/4);
        const c = Number(s.chg_pct||0) > 0 && Number(s.oi_chg_pct||0) > 8 ? 'cg' : Number(s.chg_pct||0) < 0 && Number(s.oi_chg_pct||0) > 8 ? 'cr' : 'ca';
        const n = c==='cg'?'Long buildup — OI confirming':c==='cr'?'Short buildup — avoid long':'Mixed — wait';
        return '<div class="vrrow"><span class="vrn">'+s.symbol+'</span><span style="color:var(--am);font-weight:600">'+mult.toFixed(1)+'×</span><span style="font-size:8px;color:'+(c==='cg'?'var(--go)':c==='cr'?'var(--st)':'var(--am)')+'">'+n+'</span></div>';
      }).join('');
    }
    const fiiBody = document.getElementById('fii-body');
    if(fiiBody){
      const pcr = Number((window._lastChain||{}).pcr || 0);
      const fii = Number((window._lastFii||{}).fii_net || 0);
      const rows = [
        {s:'NIFTY INDEX', fii:`${fii>=0?'NET BUY':'NET SELL'} ${Math.abs(fii).toFixed(0)} Cr`, ret:`PCR ${pcr.toFixed(2)}`, sig:pcr>=1.1?'BULL':pcr<=0.8?'BEAR':'MIXED', c:pcr>=1.1?'cg':pcr<=0.8?'cr':'ca'},
        {s:'BANK NIFTY', fii:`Move ${(Number((window._lastMacro||{}).banknifty_chg||0)>=0?'+':'')}${Number((window._lastMacro||{}).banknifty_chg||0).toFixed(2)}%`, ret:`VIX ${Number((window._lastMacro||{}).vix||0).toFixed(1)}`, sig:Number((window._lastMacro||{}).banknifty_chg||0)>=0?'UP':'DOWN', c:Number((window._lastMacro||{}).banknifty_chg||0)>=0?'cg':'cr'},
        {s:'STOCK FUT', fii:'Top OI names', ret:byScore.slice(0,2).map(s=>s.symbol).join(', '), sig:'LIVE', c:'cg'}
      ];
      fiiBody.innerHTML = rows.map(r=>'<div class="vrrow"><span class="vrn">'+r.s+'</span><span style="font-size:8px;color:var(--go)">'+r.fii+'</span><span style="font-size:8px;color:var(--t2)">'+r.ret+'</span><span class="vrv '+r.c+'">'+r.sig+'</span></div>').join('');
    }
  }

  function handle(msg){
    const d = msg.data;
    switch(msg.type){

      case 'ready':
        notify(d || 'Live data ready');
        break;

      case 'ping':
        if(ws) ws.send(JSON.stringify({type:'pong'}));
        break;

      // ── PRICES ────────────────────────────────────
      case 'prices': {
        window._livePrices = d || {};
        const map = {
          'NIFTY':'p0','BANKNIFTY':'p1'
        };
        for(const [sym, elId] of Object.entries(map)){
          const p = d[sym];
          if(!p) continue;
          const el = document.getElementById(elId);
          if(!el) continue;
          const v = p.price < 100
            ? p.price.toFixed(2)
            : Math.round(p.price).toLocaleString('en-IN');
          el.textContent = v;
          el.classList.remove('fg','fr');
          void el.offsetWidth;
          const up = p.chg_pts >= 0;
          el.classList.add(up ? 'fg' : 'fr');
          el.style.color = up ? 'var(--go)' : 'var(--st)';
          const chgEl = el.nextElementSibling;
          if(chgEl){
            const sign = p.chg_pts >= 0 ? '+' : '';
            chgEl.textContent = sign + p.chg_pts.toFixed(0) + ' · ' + sign + p.chg_pct.toFixed(2) + '%';
            chgEl.style.color = up ? 'var(--go)' : 'var(--st)';
          }
        }
        // Feed prices into Spike Radar for target tracking
        if(typeof srUpdatePrices === 'function') srUpdatePrices(d);
        _refreshTopCards();
        _renderLiveLevels();
        _renderMacroContext();
        break;
      }

      // ── GATES + VERDICT ───────────────────────────
      case 'gates': {
        const gates = d.gates;
        if(!gates) break;
        window._lastGateSummary = d || {};
        // Update each gate
        for(let n=1; n<=5; n++){
          const g = gates[n] || gates[String(n)];
          if(!g) continue;
          gState[n] = g.state;
          gScore[n] = g.score;
          // Update gate name
          const gnEl = document.getElementById('gn'+n);
          if(gnEl) gnEl.textContent = g.name;
          // Update gate body rows
          const body = document.getElementById('gbody'+n);
          if(body && g.rows && g.rows.length){
            body.innerHTML = g.rows.map(r=>
              '<div class="gm"><span class="gk">'+r.k+'</span>'+
              '<span class="gv '+r.c+'">'+r.v+'</span></div>'
            ).join('');
          }
        }
        applyGates();
        // Capture VWAP direction for Spike Radar scoring
        try {
          const g1 = d.gates && (d.gates[1] || d.gates['1']);
          if(g1 && g1.rows) {
            const vr = g1.rows.find(r => r.k && r.k.toLowerCase().includes('vwap'));
            if(vr) window._lastVwapAbove = vr.v && vr.v.includes('ABOVE');
          }
        } catch(e){}
        // Override verdict with server data
        const vd = document.getElementById('vd');
        const vw = document.getElementById('vword');
        const vs = document.getElementById('vsub');
        const vdirs = document.getElementById('vdirs');
        const pc = document.getElementById('pb-conf');
        if(vw) vw.textContent = d.verdict;
        if(vs) vs.textContent = d.verdict_sub;
        if(vd){
          vd.className = d.verdict === 'EXECUTE' ? 'vgo' :
                         d.verdict === 'NO TRADE' ? 'vst' : 'vam';
        }
        if(vdirs) vdirs.style.display = d.verdict === 'EXECUTE' ? 'flex' : 'none';
        const pass = d.pass_count || 0;
        if(pc){ pc.textContent = pass >= 5 ? 'HIGH' : pass >= 3 ? 'MODERATE' : 'LOW'; }
        const sn = document.getElementById('snum');
        if(sn){
          sn.textContent = pass + ' / 5';
          sn.className = 'snum ' + (pass >= 5 ? 'go' : pass >= 3 ? 'am' : 'st');
        }
        const gb = document.getElementById('gbadge');
        if(gb){
          gb.textContent = pass + ' / 5 PASS';
          gb.className = 'bd ' + (pass >= 5 ? 'bg' : pass >= 3 ? 'ba' : 'br');
        }
        // ── Confidence score ──
        if(d.confidence !== undefined) updateConfidence(d.confidence);
        // ── Position sizing ──
        if(d.position_size_lots !== undefined){
            const vposEl = document.getElementById('vpos-value');
            if(vposEl){
                if(d.position_size_lots > 0){
                    vposEl.textContent = `${d.position_size_lots} lot${d.position_size_lots > 1 ? 's' : ''} (₹${d.position_size_rupees.toLocaleString('en-IN')})`;
                    vposEl.style.color = 'var(--go)';
                } else {
                    vposEl.textContent = 'NO TRADE (VIX too high or R:R insufficient)';
                    vposEl.style.color = 'var(--st)';
                }
            }
        }
        _refreshPlaybookSummary();
        break;
      }

      // ── OPTION CHAIN ──────────────────────────────
      case 'chain': {
        if(!d) break;
        window._lastChain = d;
        // Update chain header stats
        const pcr  = d.pcr || 0;
        const vix2 = d.vix || 0;
        const mp   = d.max_pain || 0;
        const atm  = d.atm || 0;
        const ocH  = document.querySelector('.ochead');
        if(ocH){
          const kids = ocH.querySelectorAll('.ochv');
          if(kids[0]) kids[0].textContent = pcr.toFixed(2);
          if(kids[0]) kids[0].className = 'ochv ' + (pcr >= 1.2 ? 'cg' : pcr <= 0.8 ? 'cr' : 'ca');
          if(kids[1]) {
            const v = Number((window._lastMacro||{}).vix || 0);
            kids[1].textContent = v ? v.toFixed(1) : '—';
            kids[1].className = 'ochv ' + (v && v < 13 ? 'cg' : v && v < 17 ? 'ca' : 'cr');
          }
          if(kids[2]) kids[2].textContent = mp ? mp.toLocaleString('en-IN') : '—';
          if(kids[3]) kids[3].textContent = atm ? atm.toLocaleString('en-IN') : '—';
          if(kids[4] && d.expiry) kids[4].textContent = d.expiry;
          if(kids[5]) kids[5].textContent = 'LIVE';
          const dom = ocH.querySelector('.ocdom');
          if(dom){
            dom.textContent = pcr >= 1.2 ? 'PUT WRITERS DOMINANT' : pcr <= 0.8 ? 'CALL WRITERS DOMINANT' : 'OI BALANCED';
            dom.className = 'bd ocdom ' + (pcr >= 1.2 ? 'bg' : pcr <= 0.8 ? 'br' : 'ba');
          }
        }
        // Update chain table
        const strikes = d.strikes || [];
        const mx = Math.max(...strikes.map(s => Math.max(s.call_oi, s.put_oi)), 1);
        const oct = document.getElementById('oct-body');
        if(oct){
          oct.innerHTML = strikes.map(s => {
            const cw = Math.round(s.call_oi / mx * 52);
            const pw = Math.round(s.put_oi  / mx * 52);
            const fd = v => '<span style="color:' + (v > 0 ? 'var(--st)' : 'var(--go)') + '">' +
                            (v > 0 ? '+' : '') + v.toLocaleString() + '</span>';
            return '<tr class="' + (s.is_atm ? 'atm' : '') + '">' +
              '<td style="font-weight:600">' + s.strike.toLocaleString() + (s.is_atm ? ' ★' : '') + '</td>' +
              '<td style="color:var(--st)">' + s.call_oi.toLocaleString() + '</td>' +
              '<td>' + fd(s.call_oi_chg) + '</td>' +
              '<td style="text-align:right"><span class="obar obar-c" style="width:' + cw + 'px"></span></td>' +
              '<td style="text-align:center;font-size:8px;color:var(--t2)">' + (s.is_atm ? 'ATM' : '') + '</td>' +
              '<td><span class="obar obar-p" style="width:' + pw + 'px"></span></td>' +
              '<td>' + fd(s.put_oi_chg) + '</td>' +
              '<td style="color:var(--go)">' + s.put_oi.toLocaleString() + '</td>' +
              '</tr>';
          }).join('');
        }
        // Update ALL instrument card .ico badges from chain data
        const _icos = document.querySelectorAll('.ico');
        const _icsList = document.querySelectorAll('.ics');
        // NIFTY card (index 0): show PCR
        if(_icos[0]) _icos[0].textContent = 'PCR ' + pcr.toFixed(2);
        // NIFTY bias badge: bullish PCR = LONG, bearish = SHORT
        if(_icsList[0]){
          const nBias = pcr >= 1.1 ? 'LONG BIAS' : pcr <= 0.8 ? 'SHORT BIAS' : 'NEUTRAL';
          _icsList[0].textContent = nBias;
          _icsList[0].className = 'ics ' + (pcr >= 1.1 ? 'bg' : pcr <= 0.8 ? 'br' : 'ba');
        }
        // BANKNIFTY card (index 1): show PCR from chain (same market-wide PCR until BN chain available)
        if(_icos[1]){
          const bnOI = d.total_put_oi && d.total_call_oi
            ? ((d.total_put_oi - d.total_call_oi) / Math.max(d.total_call_oi, 1) * 100)
            : null;
          _icos[1].textContent = bnOI != null
            ? 'PCR ' + pcr.toFixed(2)
            : 'PCR ' + pcr.toFixed(2);
        }
        _refreshTopCards();
        _renderLiveLevels();
        _refreshPlaybookSummary();
        _renderMacroContext();
        _renderVolumeRadarLive();
        break;
      }

      // ── MACRO (VIX + INDICES) ─────────────────────
      case 'macro': {
        if(!d) break;
        window._lastMacro = d;
        // Update macro panel items
        const items = document.querySelectorAll('.mi');
        items.forEach(mi => {
          const nm = mi.querySelector('.min');
          if(!nm) return;
          const key = nm.textContent.trim();
          if(key === 'INDIA VIX' && d.vix){
            const vEl = mi.querySelector('.miv');
            const cEl = mi.querySelector('.mic');
            const tEl = mi.querySelector('.mit');
            if(vEl) vEl.textContent = d.vix.toFixed(1);
            if(vEl) vEl.className = 'miv ' + (d.vix < 13 ? 'cg' : d.vix < 17 ? 'ca' : 'cr');
            if(cEl && d.vix_chg != null) cEl.textContent = (d.vix_chg >= 0 ? '+' : '') + d.vix_chg.toFixed(1) + '%';
            if(tEl) tEl.textContent = d.vix < 13 ? 'LOW → sell favored' : d.vix < 17 ? 'Moderate' : 'ELEVATED';
          }
          if(key === 'FII CASH' && d.fii_net !== undefined){
            const vEl = mi.querySelector('.miv');
            if(vEl){
              const sign = d.fii_net >= 0 ? '+' : '';
              vEl.textContent = sign + '₹' + Math.abs(d.fii_net).toFixed(0) + ' Cr';
              vEl.className = 'miv ' + (d.fii_net >= 0 ? 'cg' : 'cr');
            }
            const cEl = mi.querySelector('.mic');
            if(cEl) cEl.textContent = d.fii_net >= 0 ? 'NET BUY' : 'NET SELL';
          }
        });
        _renderLiveLevels();
        _refreshPlaybookSummary();
        _renderMacroContext();
        break;
      }

      // ── STOCK OI SCANNER ──────────────────────────
      case 'stocks': {
        if(!d || !d.length) break;
        window._lastStocks = d;
        // Enrich live data with static fallback info (sector, lot size etc.)
        const stkStatic = {};
        window.stocks && window.stocks.forEach(s => { stkStatic[s.s] = s; });
        // Rebuild table so ranking reflects current live scores
        const tbody = document.getElementById('stk-body');
        if(tbody) {
          const secM = {HDFCBANK:'Banking',ICICIBANK:'Banking',AXISBANK:'Banking',KOTAKBANK:'Banking',
            INDUSINDBK:'Banking',SBIN:'PSU Bank',TCS:'IT',INFY:'IT',MARUTI:'Auto',
            TATAMOTORS:'Auto',LT:'Infra',BAJFINANCE:'NBFC',RELIANCE:'Energy',BANKNIFTY:'Index'};
          const pcr = window._lastChain ? Number(window._lastChain.pcr || 0).toFixed(2) : '—';
          const pcrC = window._lastChain ? (window._lastChain.pcr>1.3?'var(--go)':window._lastChain.pcr<0.8?'var(--st)':'var(--am)') : 'var(--t2)';
          let liveRows = [...d].filter(s => s.symbol !== 'BANKNIFTY');
          if(stkFil === 'top') liveRows = liveRows.filter(s => _activeScore(s) >= 80);
          liveRows.sort((a,b) => (_activeScore(b) - _activeScore(a)) || (Math.abs((b.chg_pct||0)) - Math.abs((a.chg_pct||0))));
          const scEl = v => '<span class="scr ' + (v>=75?'hi':v>=55?'md':'lo') + '">' + Math.min(99,Math.max(0,v)) + '</span>';
          const sc2  = t => t === 'go' ? 'background:var(--go-d);color:var(--go);border:1px solid var(--go-b)' : t === 'st' ? 'background:var(--st-d);color:var(--st);border:1px solid var(--st-b)' : 'background:var(--am-d);color:var(--am);border:1px solid var(--am-b)';
          const cc = v => v > 0 ? 'var(--go)' : 'var(--st)';
          window._stockSignalSeen = window._stockSignalSeen || {};
          const nowHHMM = () => new Date().toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit',hour12:false});
          const histPickMap = (window._histData && Array.isArray(window._histData.stock_picks)) ? Object.fromEntries(window._histData.stock_picks.map(p => [p.sym, p])) : {};
          const tradeHtml = s => {
            const sym = s.symbol || '';
            const price = Number(s.price || 0);
            const atrPct = Number(s.atr_pct || 1.5);
            const score = Number(_activeScore(s));
            const pc = Number(s.pc || 0);
            const chg = Number(s.chg_pct || 0);
            const hp = histPickMap[sym];
            const gateLbl = _firstWeakGate(s);
            if(!price || (pc < 3 && score < 80)) return `<div class="tradebox"><div class="thd ta">WAIT</div><div class="tos ta">${gateLbl}</div><div class="tln">No valid entry yet</div></div>`;
            if(sym && !window._stockSignalSeen[sym]) window._stockSignalSeen[sym] = s.signal_time || s.sig_time || nowHHMM();
            const atr = Math.max(price * atrPct / 100, price * 0.0035);
            const isLong = chg >= 0;
            const entry = isLong ? (chg >= 1 ? price * 1.002 : price - atr * 0.2) : (chg <= -1 ? price * 0.998 : price + atr * 0.2);
            const sl = isLong ? entry - atr * 1.2 : entry + atr * 1.2;
            const t1 = isLong ? entry + (entry - sl) * 2.0 : entry - (sl - entry) * 2.0;
            const t2 = isLong ? entry + (entry - sl) * 3.0 : entry - (sl - entry) * 3.0;
            const f = n => (n >= 100 ? Math.round(n).toLocaleString('en-IN') : n.toFixed(1));
            const outcome = hp && typeof hp.outcome === 'string' ? hp.outcome : '';
            const outcomeCls = outcome === 'PROFIT' ? 'tg' : outcome === 'LOSS' ? 'tr' : 'ta';
            const outcomeTxt = outcome ? (outcome === 'NOT_EXECUTED' ? 'NOT EXECUTED' : outcome) : `Gate ${pc}/5`;
            const sigTime = (hp && hp.signal_time) ? hp.signal_time : (window._stockSignalSeen[sym] || '—');
            return `<div class="tradebox"><div class="thd ${isLong?'tg':'tr'}">${isLong?'LONG':'SHORT'} READY</div><div class="ttm">SIG ${sigTime}</div><div class="tos ${outcomeCls}">${outcomeTxt} · ${gateLbl}</div><div class="tln">E <span class="${isLong?'tg':'tr'}">${f(entry)}</span>  SL <span class="tr">${f(sl)}</span></div><div class="tln">T1 <span class="tg">${f(t1)}</span>  T2 <span class="tg">${f(t2)}</span></div></div>`;
          };
          tbody.innerHTML = liveRows.map((s, i) => {
            const oip   = Number(s.oi_chg_pct || 0);
            const volR  = s.vol_ratio != null ? Number(s.vol_ratio).toFixed(1)+'x' : '—';
            const atrP  = s.atr_pct  != null ? Number(s.atr_pct).toFixed(1)+'%' : '—';
            const rsP   = s.rs_pct   != null ? (s.rs_pct>=0?'+':'')+Number(s.rs_pct).toFixed(1)+'%' : '—';
            const rsC   = s.rs_pct   != null ? (s.rs_pct>1?'var(--go)':s.rs_pct<-1?'var(--st)':'var(--am)') : 'var(--t2)';
            const sig   = s.signal || (s.chg_pct >= 1 ? 'LONG OI↑' : s.chg_pct <= -1 ? 'SHORT OI↑' : 'NEUTRAL');
            const sigt  = s.verdict === 'EXECUTE' ? 'go' : s.verdict === 'WATCH' ? 'am' : (s.chg_pct >= 0.5 ? 'go' : s.chg_pct <= -0.5 ? 'st' : 'am');
            const score = Number(s.score != null ? s.score : _activeScore(s));
            const star  = score >= 80 ? ' ★' : '';
            const g1h = s.g1 || 'am', g2h = s.g2 || 'am', g3h = s.g3 || 'am', g4h = s.g4 || 'am', g5h = s.g5 || 'am';
            return '<tr class="'+(score>=80?'top':'')+'">'
              + '<td><span style="color:var(--t2);font-size:8px;margin-right:3px">'+(i+1)+'</span><span class="tv-link" data-tvsym="'+s.symbol+'" style="font-weight:600;color:var(--t0)">'+s.symbol+star+'</span><br><span style="font-size:8px;color:var(--t2)">'+(secM[s.symbol]||'Market')+'</span></td>'
              + '<td style="font-weight:600">'+Math.round(s.price || 0).toLocaleString('en-IN')+'</td>'
              + '<td style="color:'+cc(s.chg_pct||0)+';font-weight:600">'+((s.chg_pct||0)>=0?'+':'')+Number(s.chg_pct||0).toFixed(2)+'%</td>'
              + '<td style="color:'+cc(oip)+'">'+(oip>=0?'+':'')+Math.round(oip*100).toLocaleString('en-IN')+'</td>'
              + '<td><span style="color:'+cc(oip)+';font-weight:600">'+(oip>=0?'+':'')+oip.toFixed(1)+'%</span><div style="height:3px;border-radius:2px;background:var(--b1);margin-top:2px;overflow:hidden;width:60px"><div style="height:100%;width:'+Math.min(Math.abs(oip)*3.5,100)+'%;background:'+cc(oip)+';border-radius:2px"></div></div></td>'
              + '<td style="color:'+pcrC+'">'+pcr+'</td>'
              + '<td style="color:'+((s.vol_ratio||0)>=2?'var(--go)':(s.vol_ratio||0)>=1.3?'var(--am)':'var(--t2)')+'">'+volR+'</td>'
              + '<td style="color:'+((s.atr_pct||0)>=2?'var(--am)':'var(--t0)')+'">'+atrP+'</td>'
              + '<td style="color:'+rsC+'">'+rsP+'</td>'
              + '<td style="color:'+((s.delivery_pct||0)>=55?'var(--go)':(s.delivery_pct||0)>=45?'var(--am)':'var(--t2)')+'">'+((s.delivery_pct!=null)?(s.delivery_pct+'%'):'—')+'</td>'
              + '<td>'+_gateDots(g1h,g2h,g3h,g4h,g5h)+'</td>'
              + '<td><span class="sigtag" style="'+sc2(sigt)+'">'+sig+'</span></td>'
              + '<td>'+tradeHtml(s)+'</td>'
              + '<td>'+scEl(score)+'</td></tr>';
          }).join('');
        }
        _refreshTopCards();
        _renderLiveLevels();
        _renderOIPulseLive();
        _renderVolumeRadarLive();
        _renderSwingLive();
        break;
      }

      // ── SPIKES ────────────────────────────────────
      case 'spikes': {
        if(!d) break;
        const tbody2 = document.getElementById('spike-body');
        if(!tbody2) break;
        const sc = s => '<span class="strpill ' +
          (s === 'hi' ? 'strhi' : s === 'md' ? 'strmd' : 'strlo') + '">' +
          (s === 'hi' ? 'HIGH' : s === 'md' ? 'MED' : 'LOW') + '</span>';
        tbody2.innerHTML = d.map(sp =>
          '<tr class="' + (sp.type === 'buy' ? 'sbuy' : sp.type === 'sell' ? 'ssell' : 'soi') + '">' +
          '<td><span class="tv-link" data-tvsym="' + sp.symbol + '" style="font-weight:600;color:var(--t0)">' + sp.symbol + '</span></td>' +
          '<td style="color:var(--t2)">' + (sp.time || '—') + '</td>' +
          '<td style="font-weight:600">' + (sp.price ? Math.round(sp.price).toLocaleString('en-IN') : '—') + '</td>' +
          '<td style="color:' + (sp.chg_pct >= 0 ? 'var(--go)' : 'var(--st)') + ';font-weight:600">' +
            (sp.chg_pct >= 0 ? '+' : '') + (sp.chg_pct || 0).toFixed(2) + '%</td>' +
          '<td style="color:' + (sp.vol_mult >= 3 ? 'var(--go)' : sp.vol_mult >= 2 ? 'var(--am)' : 'var(--t2)') + ';font-weight:600">' + (sp.vol_mult || '—') + '×</td>' +
          '<td style="color:' + (sp.oi_pct >= 0 ? 'var(--go)' : 'var(--st)') + ';font-weight:600">' +
            (sp.oi_pct >= 0 ? '+' : '') + (sp.oi_pct || 0).toFixed(1) + '%</td>' +
          '<td style="color:var(--t2);font-size:9px">' + (sp.trigger || '—') + '</td>' +
          '<td style="color:' + (sp.type === 'buy' ? 'var(--go)' : sp.type === 'sell' ? 'var(--st)' : 'var(--am)') + ';font-weight:700">' + (sp.signal || '—') + '</td>' +
          '<td>' + sc(sp.strength || 'lo') + '</td>' +
          '<td style="text-align:center">' + (sp.type === 'buy' ? '<span style="color:var(--go)">▲</span>' : sp.type === 'sell' ? '<span style="color:var(--st)">▼</span>' : '<span style="color:var(--am)">●</span>') + '</td>' +
          '</tr>'
        ).join('') || '<tr><td colspan="10" style="text-align:center;color:var(--t2);padding:16px">No spikes detected</td></tr>';

        const hiEl = document.getElementById('spike-hi');
        if(hiEl){
          const hi = d.filter(x => x.strength === 'hi').length;
          hiEl.textContent = hi + ' high strength';
          hiEl.style.color = hi > 0 ? 'var(--go)' : 'var(--t2)';
        }
        // Feed into Spike Radar tracker
        if(typeof srIngestSpikes === 'function') srIngestSpikes(d);
        break;
      }

      // ── INTEL TICKER ──────────────────────────────
      case 'ticker': {
        if(!d || !d.length) break;
        const track = document.getElementById('tktrack');
        if(!track) break;
        const items = [...d, ...d]; // double for seamless loop
        track.innerHTML = items.map(t =>
          '<span class="tki">' + t + '</span>' +
          '<span style="color:var(--t2);padding:0 6px">·</span>'
        ).join('');
        break;
      }

      // ── FII ───────────────────────────────────────
      case 'fii': {
        if(!d) break;
        window._lastFii = d;
        const sign = d.fii_net >= 0 ? '+' : '';
        document.querySelectorAll('.mi').forEach(mi => {
          const nm = mi.querySelector('.min');
          if(!nm) return;
          if(nm.textContent.trim() === 'FII CASH'){
            const vEl = mi.querySelector('.miv');
            if(vEl){
              vEl.textContent = sign + '₹' + Math.abs(d.fii_net).toFixed(0) + ' Cr';
              vEl.className = 'miv ' + (d.fii_net >= 0 ? 'cg' : 'cr');
            }
          }
          if(nm.textContent.trim() === 'DII CASH'){
            const vEl = mi.querySelector('.miv');
            if(vEl){
              const ds = d.dii_net >= 0 ? '+' : '';
              vEl.textContent = ds + '₹' + Math.abs(d.dii_net).toFixed(0) + ' Cr';
              vEl.className = 'miv ' + (d.dii_net >= 0 ? 'cg' : 'cr');
            }
          }
        });
        _renderLiveLevels();
        break;
      }
    }
  }

  // Expose for historical replay
  window._histMode = false;
  window._handle = handle;
  window._liveConnect = connect;

  // Start
  connect();
})();

// ═══════════════════════════════════════════════
// CONFIDENCE SCORE DISPLAY
// ═══════════════════════════════════════════════
function updateConfidence(score) {
  const filled = Math.round(score);
  const col    = score >= 7 ? 'var(--go)' : score >= 4 ? 'var(--am)' : 'var(--st)';
  const dots   = Array.from({length:10}, (_,i) =>
    `<div style="width:7px;height:7px;border-radius:1px;background:${i < filled ? col : 'var(--b2)'}"></div>`
  ).join('');
  const el1 = document.getElementById('conf-dots');
  const el2 = document.getElementById('conf-score');
  if(el1) el1.innerHTML = dots;
  if(el2){ el2.textContent = score.toFixed(1); el2.style.color = col; }

  // Also update the backtest panel live score
  const el3 = document.getElementById('bt2-conf-dots');
  const el4 = document.getElementById('bt2-live-conf');
  if(el3) el3.innerHTML = dots;
  if(el4){ el4.textContent = score.toFixed(1); el4.style.color = col; }
}

// ═══════════════════════════════════════════════
// CHART MODAL (Investing.com style using Chart.js)
// ═══════════════════════════════════════════════
let chart = null;
// Legacy openChart — now delegates to TradingView modal
function openChart(symbol, dateStr){
  openTVChart(symbol, dateStr || window._histDate || null);
}
function closeChart(){ closeTVChart(); }
function setChartRange(){}

// Legacy stubs — replaced by TradingView
function loadChartData(){}
function generateMockChartData(){}

// ═══════════════════════════════════════════════
// BACKTEST PANEL
// ═══════════════════════════════════════════════
const API = window.location.hostname === 'kvishnublr.github.io'
  ? 'https://nse-edge-backend.fly.dev'
  : 'http://localhost:8765';

(function bt2Init(){
  const to   = new Date();
  const from = new Date(); from.setFullYear(from.getFullYear() - 3);
  document.getElementById('bt2-to').value   = to.toISOString().split('T')[0];
  document.getElementById('bt2-from').value = from.toISOString().split('T')[0];
  // Default day view to yesterday (last trading day)
  const yest = new Date(); yest.setDate(yest.getDate()-1);
  document.getElementById('dv-date').value  = yest.toISOString().split('T')[0];
  bt2RefreshStatus();
  bt2LoadWeights();
})();

function bt2Pre(btn, period) {
  document.querySelectorAll('#tp-backtest .btpre').forEach(b => b.classList.remove('on'));
  btn.classList.add('on');
  const to   = new Date();
  const from = new Date();
  if(period==='1M') from.setMonth(from.getMonth()-1);
  else if(period==='3M') from.setMonth(from.getMonth()-3);
  else if(period==='6M') from.setMonth(from.getMonth()-6);
  else if(period==='1Y') from.setFullYear(from.getFullYear()-1);
  else if(period==='2Y') from.setFullYear(from.getFullYear()-2);
  else if(period==='3Y') from.setFullYear(from.getFullYear()-3);
  document.getElementById('bt2-from').value = from.toISOString().split('T')[0];
  document.getElementById('bt2-to').value   = to.toISOString().split('T')[0];
  bt2Run();
}

async function bt2Download() {
  document.getElementById('bt2-status').textContent = 'Downloading 1-year data (NIFTY OHLCV + VIX + chain PCR)...';
  try {
    const r = await fetch(API+'/api/backtest/download', {method:'POST'});
    const d = await r.json();
    document.getElementById('bt2-status').textContent = d.message || 'Download started';
    notify('Data download started — takes 1-2 min');
    setTimeout(bt2RefreshStatus, 8000);
  } catch(e) {
    document.getElementById('bt2-status').textContent = 'Download failed — backend may not be running';
  }
}

async function bt2Run() {
  const from = document.getElementById('bt2-from').value;
  const to   = document.getElementById('bt2-to').value;
  const mode = document.getElementById('bt2-mode').value;
  if(!from || !to) { notify('Select date range first'); return; }

  document.getElementById('bt2-showing').textContent = 'Running...';
  try {
    const r = await fetch(`${API}/api/backtest/run?from_date=${from}&to_date=${to}&mode=${mode}`, {method:'POST'});
    const d = await r.json();
    if(d.error){ notify('Backtest error: '+d.error); return; }
    bt2RenderResults(d);
    bt2LoadWeights();
    notify(`Backtest: ${d.metrics?.execute_signals||0} signals · WR ${d.metrics?.win_rate_pct||0}%`);
  } catch(e) {
    notify('Backtest failed — check backend');
    document.getElementById('bt2-showing').textContent = 'Error';
  }
}

async function bt2CalcWeights() {
  try {
    // Trigger a fresh run then reload weights
    const from = document.getElementById('bt2-from').value;
    const to   = document.getElementById('bt2-to').value;
    const mode = document.getElementById('bt2-mode').value;
    if(!from||!to){ notify('Set date range first'); return; }
    notify('Recalculating gate weights...');
    await fetch(`${API}/api/backtest/run?from_date=${from}&to_date=${to}&mode=${mode}`, {method:'POST'});
    await bt2LoadWeights();
    notify('Gate weights updated');
  } catch(e) { notify('Weight calculation failed'); }
}

function bt2RenderResults(data) {
  const m = data.metrics || {};

  // Metrics
  const set = (id, val, cls) => {
    const el = document.getElementById(id);
    if(el){ el.textContent = val; if(cls) el.className = 'stcv '+cls; }
  };
  set('bt2-days', m.total_days || '—');
  set('bt2-sigs', m.execute_signals || 0, 'cg');
  // Count WATCH signals (pass_count=4, no st) from trades
  const watchCount = (data.trades||[]).filter(t=>t.verdict==='WAIT'&&t.pass_count===4).length;
  set('bt2-watch', watchCount, 'ca');
  const wr = m.win_rate_pct || 0;
  set('bt2-wr',  wr+'%', wr>=60?'cg':wr>=40?'ca':'cr');
  const pf = m.profit_factor || 0;
  set('bt2-pf',  pf, pf>=1.5?'cg':pf>=1?'ca':'cr');
  set('bt2-aw',  (m.avg_win_pts||0)>=0 ? '+'+(m.avg_win_pts||0) : (m.avg_win_pts||0), 'cg');
  set('bt2-al',  (m.avg_loss_pts||0)!==0 ? (m.avg_loss_pts||0) : '0', 'cr');
  const pnl = m.total_pnl_pts || 0;
  set('bt2-pnl', (pnl>=0?'+':'')+pnl, pnl>=0?'cg':'cr');

  // Coverage
  const cov = m.data_coverage;
  if(cov) {
    document.getElementById('bt2-coverage').style.display = 'block';
    document.getElementById('bt2-cov-detail').innerHTML =
      `<span style="font-size:9px;color:var(--t1)">OHLCV: <b style="color:var(--t0)">${cov.ohlcv}</b> days</span>
       <span style="font-size:9px;color:var(--t1)">VIX: <b style="color:var(--t0)">${cov.vix}</b> days</span>
       <span style="font-size:9px;color:var(--t1)">Chain PCR: <b style="color:var(--t0)">${cov.chain}</b> days</span>
       <span style="font-size:9px;color:var(--t1)">FII: <b style="color:var(--t0)">${cov.fii}</b> days</span>`;
  }

  // Gate stats
  const gs = data.gate_stats || {};
  const gnames = {1:'G1 REGIME',2:'G2 SMART MONEY',3:'G3 STRUCTURE',4:'G4 TRIGGER',5:'G5 RISK'};
  document.getElementById('bt2-gate-stats').innerHTML = Object.keys(gs).map(gi => {
    const g  = gs[gi];
    const wr = g.win_rate_when_go || 0;
    const pr = g.pass_rate || 0;
    const col = wr>=60?'var(--go)':wr>=40?'var(--am)':'var(--st)';
    return `<div class="gr" style="margin-bottom:4px">
      <span class="grn" style="width:100px">${gnames[gi]}</span>
      <div class="grb"><div class="grf" style="width:${wr}%;background:${col}"></div></div>
      <span class="grv" style="color:${col}">${wr}%</span>
      <span style="font-size:8px;color:var(--t2);margin-left:4px">n=${g.sample} pass=${pr}%</span>
    </div>`;
  }).join('');

  // Trades table — show EXECUTE + WATCH (4/5) + recent WAIT, skip pure NO TRADE to reduce noise
  const allTrades = data.trades || [];
  const stMap  = {go:'cg',am:'ca',st:'cr',wt:'cb'};
  const stLbl  = {go:'GO',am:'~',st:'NO',wt:'?'};
  // Show EXECUTE, WAIT (pass>=3), skip NO TRADE unless recent
  const trades = allTrades.slice().reverse().filter(t =>
    t.verdict==='EXECUTE' || t.verdict==='WAIT' || allTrades.indexOf(t) >= allTrades.length-20
  );
  document.getElementById('bt2-trades').innerHTML = allTrades.slice().reverse().map(t => {
    const isExec  = t.verdict==='EXECUTE';
    const isWatch = t.verdict==='WAIT' && t.pass_count===4;
    const isWait  = t.verdict==='WAIT' && t.pass_count<4;
    const oc  = t.outcome==='WIN'?'var(--go)':t.outcome==='LOSS'?'var(--st)':'var(--t2)';
    const vc  = isExec?'var(--go)':t.verdict==='NO TRADE'?'var(--t3)':isWatch?'var(--am)':'var(--t2)';
    const vLabel = isExec?'EXECUTE':isWatch?'WATCH (4/5)':t.verdict==='WAIT'?'WAIT ('+t.pass_count+'/5)':'NO TRADE';
    const gs5 = [t.g1,t.g2,t.g3,t.g4,t.g5].map(g=>
      `<span class="${stMap[g]||'cm'}" style="font-size:9px;font-weight:700">${stLbl[g]||'?'}</span>`
    ).join(' ');
    const pts = t.outcome_pts !== null && t.outcome_pts !== undefined
      ? `<span style="color:${oc};font-weight:600">${t.outcome_pts>0?'+':''}${t.outcome_pts}</span>` : '—';
    const f0 = v => v!=null ? Math.round(v) : '—';
    const rowStyle = isExec?'background:rgba(0,200,100,0.07)':isWatch?'background:rgba(255,180,0,0.05)':'';
    return `<tr style="${rowStyle};cursor:pointer" onclick="openChart('NIFTY','${t.date}')" title="Click to view chart for ${t.date}">
      <td style="color:var(--bl)">${t.date}</td>
      <td style="color:var(--am);font-size:9px">${t.session || '—'}</td>
      <td style="color:${vc};font-weight:600">${vLabel}</td>
      <td style="color:${vc};font-weight:700">${t.pass_count}/5</td>
      <td style="font-weight:600">${(t.nifty||0).toFixed(0)}</td>
      <td style="color:${(t.vix||0)>=20?'var(--st)':(t.vix||0)>=16?'var(--am)':'var(--go)'}">${(t.vix||0).toFixed(1)}</td>
      <td style="color:${(t.pcr||0)>=1.2?'var(--go)':(t.pcr||0)<=0.8?'var(--st)':'var(--am)'}">${(t.pcr||0).toFixed(2)}</td>
      <td>${gs5}</td>
      <td style="color:var(--t1)">${f0(t.entry)}</td>
      <td style="color:var(--go);font-size:9px">${f0(t.target)}</td>
      <td style="color:var(--st);font-size:9px">${f0(t.stop)}</td>
      <td style="font-weight:600">${f0(t.exit)}</td>
      <td>${pts}</td>
      <td style="color:${oc};font-weight:600">${t.outcome||'—'}</td>
    </tr>`;
  }).join('');
  const execN = (data.trades||[]).filter(t=>t.verdict==='EXECUTE').length;
  const watchN = (data.trades||[]).filter(t=>t.verdict==='WAIT'&&t.pass_count===4).length;
  document.getElementById('bt2-showing').textContent = `${allTrades.length} days shown · ${execN} EXECUTE · ${watchN} WATCH(4/5) · ${m.from_date} to ${m.to_date}`;
}

async function bt2LoadWeights() {
  try {
    const r = await fetch(API+'/api/backtest/gate-weights');
    const d = await r.json();
    const gs = d.gates || {};
    if(!Object.keys(gs).length) return;
    document.getElementById('bt2-weights').innerHTML = Object.keys(gs).map(gi => {
      const g = gs[gi];
      const w = Math.round((g.weight||0) * 100);
      return `<div class="gr" style="margin-bottom:4px">
        <span class="grn" style="width:100px">${g.name}</span>
        <div class="grb"><div class="grf" style="width:${w}%;background:var(--bl)"></div></div>
        <span class="grv cb">${w}%</span>
        <span style="font-size:8px;color:var(--t2);margin-left:4px">WR ${g.win_rate||0}%</span>
      </div>`;
    }).join('');
  } catch(e) {}
}

async function bt2RefreshStatus() {
  try {
    const r = await fetch(API+'/api/backtest/status');
    const d = await r.json();
    const fii = d.fii?.count || 0;
    const s = `OHLCV: ${d.ohlcv?.count||0} days · VIX: ${d.vix?.count||0} days · Chain: ${d.chain?.count||0} days · FII: ${fii} days · Signals: ${d.signal_log?.count||0}`;
    document.getElementById('bt2-status').textContent = s;
  } catch(e) {
    document.getElementById('bt2-status').textContent = 'Status unavailable';
  }
}

async function bt2DownloadFii() {
  try {
    notify('Downloading FII history from NSE...');
    const r = await fetch(API+'/api/backtest/download-fii', {method:'POST'});
    const d = await r.json();
    document.getElementById('bt2-status').textContent = d.message || 'FII download started';
    setTimeout(bt2RefreshStatus, 5000);
  } catch(e) {
    notify('FII download failed — check backend');
  }
}

async function bt2Optimize() {
  notify('Running threshold optimizer — may take 30-60 sec...');
  document.getElementById('bt2-status').textContent = 'Optimizer running...';
  try {
    const r = await fetch(API+'/api/backtest/optimize', {method:'POST'});
    const d = await r.json();
    if(d.error){ notify('Optimizer error: '+d.error); return; }
    const b = d.best;
    const msg = `Optimizer done (${d.tested} combos · ${d.days} days)\n`
      + `Best: PF=${b.profit_factor}  WR=${b.win_rate}%  Signals=${b.signals}\n`
      + `VIX: ${b.vix_low}/${b.vix_medium}/${b.vix_high}  PCR: ${b.pcr_bearish}/${b.pcr_bullish}  Vol: ${b.vol_surge_min}×`;
    document.getElementById('bt2-status').textContent = msg;
    notify(`Optimizer: best PF=${b.profit_factor} WR=${b.win_rate}% — see status bar`);
  } catch(e) {
    notify('Optimizer failed — check backend');
  }
}

// ═══════════════════════════════════════════════
// TOPBAR DATE VIEW — injects historical data into live UI
// ═══════════════════════════════════════════════
const _todayStr = () => new Date().toISOString().split('T')[0];

async function tbDateChange(val) {
  if(!val || val === _todayStr()) { tbGoLive(); return; }

  // Enter historical mode — freeze live WS updates
  window._histMode = true;
  document.getElementById('tb-live-btn').style.display = 'inline-block';
  document.getElementById('live-pill').style.opacity = '0.35';
  const banner = document.getElementById('hist-banner');
  banner.style.display = 'flex';
  document.getElementById('hist-banner-date').textContent = val;

  notify('Loading ' + val + ' data...');

  try {
    const r = await fetch(`${API}/api/dayview/full?date=${val}`);
    const d = await r.json();
    if(d.error) { notify('No data for ' + val + ': ' + d.error); return; }
    window._histData = d;    // cache for mode-switch re-render
    window._histDate = val;  // cache date for header

    // ── Direct topbar price update (bypass handle race conditions) ──────────
    if(d.prices) {
      const _pm = {'NIFTY':'p0','BANKNIFTY':'p1','ICICIBANK':'p2','RELIANCE':'p3'};
      for(const [sym, elId] of Object.entries(_pm)) {
        try {
          const p = d.prices[sym]; if(!p) continue;
          const el = document.getElementById(elId); if(!el) continue;
          el.textContent = p.price < 100 ? p.price.toFixed(2) : Math.round(p.price).toLocaleString('en-IN');
          const up = p.chg_pts >= 0;
          el.style.color = up ? 'var(--go)' : 'var(--st)';
          const chgEl = el.nextElementSibling;
          if(chgEl) {
            const sgn = up ? '+' : '';
            chgEl.textContent = sgn + p.chg_pts.toFixed(0) + ' · ' + sgn + p.chg_pct.toFixed(2) + '%';
            chgEl.style.color = up ? 'var(--go)' : 'var(--st)';
          }
          // Update icb badge: bias label and OI% badge
          const icbEl = el.parentElement && el.parentElement.querySelector('.icb');
          if(icbEl) {
            const ics = icbEl.querySelector('.ics');
            const ico = icbEl.querySelector('.ico');
            if(ics && d.stocks) {
              const stk = d.stocks.find(s => s.symbol === sym);
              if(stk) {
                const oip = stk.oi_chg_pct != null ? Number(stk.oi_chg_pct) : 0;
                const bias = up ? 'LONG BIAS' : 'SHORT BIAS';
                ics.textContent = bias;
                ics.className = 'ics ' + (up ? 'bg' : 'br');
                if(ico) { ico.textContent = 'OI ' + (oip >= 0 ? '+' : '') + oip.toFixed(0) + '%'; }
              }
            }
            if(sym === 'NIFTY' && d.chain) {
              const icoEl = icbEl.querySelector('.ico');
              if(icoEl) icoEl.textContent = 'PCR ' + (d.chain.pcr || 0).toFixed(2);
            }
          }
        } catch(e) { console.warn('Price update failed for ' + sym + ':', e); }
      }
    }

    // ── Re-render stock OI table from historical data ───────────────────────
    const stkBody = document.getElementById('stk-body');
    if(stkBody) {
      if(d.stocks && d.stocks.length) {
        const cc   = v => v > 0 ? 'var(--go)' : 'var(--st)';
        const sc2  = t => t === 'go' ? 'background:var(--go-d);color:var(--go);border:1px solid var(--go-b)'
          : t === 'st' ? 'background:var(--st-d);color:var(--st);border:1px solid var(--st-b)'
          : 'background:var(--am-d);color:var(--am);border:1px solid var(--am-b)';
        const secM = {HDFCBANK:'Banking',ICICIBANK:'Banking',AXISBANK:'Banking',KOTAKBANK:'Banking',
          INDUSINDBK:'Banking',SBIN:'PSU Bank',TCS:'IT',INFY:'IT',MARUTI:'Auto',
          TATAMOTORS:'Auto',LT:'Infra',BAJFINANCE:'NBFC',RELIANCE:'Energy'};
        const pcrC = d.chain ? (d.chain.pcr>1.3?'var(--go)':d.chain.pcr<0.8?'var(--st)':'var(--am)') : 'var(--t2)';
        const pcr  = d.chain ? (d.chain.pcr||0).toFixed(2) : '—';
        let sorted = [...d.stocks].filter(s => s.symbol !== 'BANKNIFTY');
        // Apply gate filter if active
        if(stkGateFil && stkGateFil !== 'all') {
          sorted = sorted.filter(s => {
            const g = {g1:s.g1||'am',g2:s.g2||'am',g3:s.g3||'am',g4:s.g4||'am',g5:s.g5||'am'};
            if(stkGateFil==='exe') return ['g1','g2','g3','g4','g5'].every(k=>g[k]==='go');
            return g[stkGateFil]==='go';
          });
        }
        sorted.sort((a,b) => Math.abs(b.chg_pct) - Math.abs(a.chg_pct));
        const scEl = v => '<span class="scr ' + (v>=75?'hi':v>=55?'md':'lo') + '">' + Math.min(99,Math.max(0,v)) + '</span>';
        stkBody.innerHTML = sorted.map((s, i) => {
          const oip   = s.oi_chg_pct || 0;
          const volR  = s.vol_ratio != null ? s.vol_ratio.toFixed(1)+'x' : '—';
          const atrP  = s.atr_pct  != null ? s.atr_pct.toFixed(1)+'%' : '—';
          const rsP   = s.rs_pct   != null ? (s.rs_pct>=0?'+':'')+s.rs_pct.toFixed(1)+'%' : '—';
          const rsC   = s.rs_pct   != null ? (s.rs_pct>1?'var(--go)':s.rs_pct<-1?'var(--st)':'var(--am)') : 'var(--t2)';
          const sig   = s.chg_pct >= 1 ? 'LONG OI↑' : s.chg_pct <= -1 ? 'SHORT OI↑' : 'NEUTRAL';
          const sigt  = s.chg_pct >= 0.5 ? 'go' : s.chg_pct <= -0.5 ? 'st' : 'am';
          const score = Math.min(99, Math.round(50 + s.chg_pct*5 + oip*0.5 + (s.vol_ratio||1)*2));
          const star  = score >= 80 ? ' ★' : '';
          // Gate dots: use API gate data if available, else derive
          const g1h = s.g1 || (s.chg_pct>=0 ? 'go' : 'am');
          const g2h = s.g2 || (parseFloat(pcr)>=1.0 ? 'go' : parseFloat(pcr)>=0.7 ? 'am' : 'st');
          const g3h = s.g3 || (s.chg_pct>0.15 ? 'go' : s.chg_pct>-0.15 ? 'am' : 'st');
          const g4h = s.g4 || (s.vol_ratio>=1.3 ? 'go' : s.vol_ratio>=1.0 ? 'am' : 'wt');
          const g5h = s.g5 || (s.atr_pct<=1.5 ? 'go' : 'am');
          return '<tr class="'+(score>=80?'top':'')+'">'
            + '<td><span style="color:var(--t2);font-size:8px;margin-right:3px">'+(i+1)+'</span>'
            + '<span class="tv-link" data-tvsym="'+s.symbol+'" style="font-weight:600;color:var(--t0)">'+s.symbol+star+'</span><br>'
            + '<span style="font-size:8px;color:var(--t2)">'+(secM[s.symbol]||'Market')+'</span></td>'
            + '<td style="font-weight:600">'+Math.round(s.price).toLocaleString('en-IN')+'</td>'
            + '<td style="color:'+cc(s.chg_pct)+';font-weight:600">'+(s.chg_pct>=0?'+':'')+s.chg_pct.toFixed(2)+'%</td>'
            + '<td style="color:'+cc(oip)+'">'+(oip>=0?'+':'')+Math.round(oip*100).toLocaleString()+'</td>'
            + '<td><span style="color:'+cc(oip)+';font-weight:600">'+(oip>=0?'+':'')+oip.toFixed(1)+'%</span></td>'
            + '<td style="color:'+pcrC+'">'+pcr+'</td>'
            + '<td style="color:'+(s.vol_ratio>=2?'var(--go)':s.vol_ratio>=1.5?'var(--am)':'var(--t2)')+'">'+volR+'</td>'
            + '<td style="color:'+(s.atr_pct>=2?'var(--am)':'var(--t0)')+'">'+atrP+'</td>'
            + '<td style="color:'+rsC+'">'+rsP+'</td>'
            + '<td style="color:var(--t2)">—</td>'
            + '<td>'+_gateDots(g1h,g2h,g3h,g4h,g5h)+'</td>'
            + '<td><span class="sigtag" style="'+sc2(sigt)+'">'+sig+'</span></td>'
            + '<td>'+scEl(score)+'</td></tr>';
        }).join('');
      } else {
        // Empty — token expired or no data for this date
        stkBody.innerHTML = '<tr><td colspan="13" style="padding:18px;text-align:center;color:var(--am);font-size:9px;line-height:1.9">'
          + 'Stock OI data not available for <b>'+val+'</b><br>'
          + '<span style="color:var(--t2)">Kite API token expired — regenerate token and restart backend to load historical stock data</span>'
          + '</td></tr>';
      }
    }

    // Inject into the exact same live DOM elements using handle()
    const h = window._handle;
    if(h) {
      if(d.prices) h({type:'prices', data: d.prices});  // fallback for any missed updates
      h({type:'gates',  data: d.gates});
      h({type:'macro',  data: d.macro});
      h({type:'fii',    data: d.fii});
      if(d.chain)  h({type:'chain',  data: d.chain});
      h({type:'spikes', data: d.spikes || []});
    }

    // ── Right panel macro items (direct ID-based update) ───────────────────
    if(d.macro) {
      const vix = d.macro.vix, vc2 = d.macro.vix_chg;
      const vEl = document.getElementById('rp-vix');
      const vcEl = document.getElementById('rp-vix-chg');
      const vlEl = document.getElementById('rp-vix-lbl');
      if(vEl) { vEl.textContent = vix.toFixed(1); vEl.className = 'miv '+(vix<13?'cg':vix<17?'ca':'cr'); }
      if(vcEl) { vcEl.textContent = (vc2>=0?'+':'')+vc2.toFixed(1)+'%'; vcEl.className = 'mic '+(vc2>=0?'cg':'cr'); }
      if(vlEl) { vlEl.textContent = vix<13?'LOW → full size':vix<17?'Moderate':'ELEVATED'; vlEl.className = 'mit '+(vix<13?'cg':vix<17?'ca':'cr'); }
    }
    if(d.fii) {
      const fnet = d.fii.fii_net, dnet = d.fii.dii_net;
      const fEl = document.getElementById('rp-fii');
      const flEl = document.getElementById('rp-fii-lbl');
      const dEl = document.getElementById('rp-dii');
      if(fEl) { fEl.textContent = (fnet>=0?'+':'')+'₹'+Math.abs(fnet).toFixed(0)+' Cr'; fEl.className = 'miv '+(fnet>=0?'cg':'cr'); }
      if(flEl) { flEl.textContent = fnet>=0?'NET BUY':'NET SELL'; flEl.className = 'mic '+(fnet>=0?'cg':'cr'); }
      if(dEl && dnet) { dEl.textContent = 'DII '+(dnet>=0?'+':'')+'₹'+Math.abs(dnet).toFixed(0)+' Cr'; }
    }

    // ── Right panel header + badge ──────────────────────────────────────────
    const rpHd = document.getElementById('rp-hd');
    if(rpHd) {
      const modeLabel = (typeof curMode !== 'undefined' && curMode==='positional') ? 'HIST POSITIONAL' : 'HIST INTRADAY';
      rpHd.textContent = modeLabel + ' — ' + val;
      const rpBadge = rpHd.nextElementSibling;
      if(rpBadge) { rpBadge.textContent = 'HIST'; rpBadge.className = 'bd bm'; }
    }

    // ── Right panel picks — delegate to shared render function ─────────────
    const rpp = document.getElementById('rp-picks');
    if(rpp) {
      if(d.stock_picks && d.stock_picks.length) {
        renderHistPicksHtml(d.stock_picks, typeof curMode !== 'undefined' ? curMode : 'intraday');
      } else {
        const pc = d.gates ? d.gates.pass_count : 0;
        rpp.innerHTML = `<div style="padding:14px 10px;text-align:center;color:var(--t2);font-size:9px;line-height:1.6">
          No qualifying picks for ${val}<br>
          <span style="color:var(--am)">${pc}/5 gates pass — signal too weak for entries</span>
        </div>`;
      }
    }

    // Update playbook date
    const pbDate = document.getElementById('pb-date');
    if(pbDate) pbDate.textContent = val + ' (HIST)';

    // Update NIFTY OHLC display in playbook if present
    if(d.nifty_ohlc) {
      const o = d.nifty_ohlc;
      const pbBias = document.getElementById('pb-bias');
      if(pbBias) pbBias.textContent = o.c > o.o ? 'BULLISH' : 'BEARISH';
    }

    // ── [HIST] Shared helpers ────────────────────────────────────────────────
    const _loc  = n => Math.round(n).toLocaleString('en-IN');
    const _secM = {HDFCBANK:'Banking',ICICIBANK:'Banking',AXISBANK:'Banking',KOTAKBANK:'Banking',
      INDUSINDBK:'Banking',SBIN:'PSU Bank',BANKNIFTY:'Index',TCS:'IT',INFY:'IT',
      MARUTI:'Auto',TATAMOTORS:'Auto',LT:'Infra',BAJFINANCE:'NBFC',RELIANCE:'Energy'};
    const _holdM = {Breakout:'3–5d',Pullback:'4–7d',Recovery:'5–8d',Momentum:'4–6d'};
    const _sc2  = t => t==='Breakout'?'color:var(--go);border:1px solid var(--go-b)'
      : t==='Pullback'?'color:var(--am);border:1px solid var(--am-b)'
      : t==='Recovery'?'color:var(--st);border:1px solid var(--st-b)'
      : 'color:var(--bl);border:1px solid var(--bl-b)';
    const _sc   = s => s>=80?'var(--go)':s>=65?'var(--am)':'var(--st)';

    // ── [HIST] Swing Radar ───────────────────────────────────────────────────
    const swBody = document.getElementById('sw-body');
    const _isPos = (typeof curMode !== 'undefined' && curMode === 'positional');
    if(swBody) {
      if(d.stock_picks && d.stock_picks.length) {
        swBody.innerHTML = d.stock_picks.map((p, i) => {
          const pr   = parseFloat(p.close)||0;
          const entN = parseFloat(p.entry)||pr;
          const atr  = parseFloat(p.atr)||(pr*0.015);
          const ezLo = Math.round(entN - 0.3*atr).toLocaleString('en-IN');
          const ezHi = Math.round(entN + 0.2*atr).toLocaleString('en-IN');
          const tgt  = Math.round(parseFloat(_isPos ? (p.target_p||p.target) : p.target)||0);
          const sl   = Math.round(parseFloat(p.sl)||0);
          const rr   = parseFloat(_isPos ? (p.rr_p||p.rr) : p.rr)||0;
          const sc   = p.score||0;
          const ocMap = {PROFIT:'✓ PROFIT','color':'var(--go)',LOSS:'✗ LOSS',NOT_EXECUTED:'— NOT EXEC'};
          const ocColor = p.outcome==='PROFIT'?'var(--go)':p.outcome==='LOSS'?'var(--st)':'var(--t2)';
          const ocLabel = p.outcome && p.outcome!=='NO_DATA' && p.outcome!=='UNKNOWN' ? p.outcome.replace('_',' ') : '—';
          return `<tr style="${sc>=80?'background:rgba(0,232,122,.03)':''}">
            <td><span style="color:var(--t2);font-size:8px;margin-right:3px">${i+1}</span><span class="tv-link" data-tvsym="${p.sym}" style="font-weight:600;color:var(--t0)">${p.sym}</span></td>
            <td style="color:var(--t2);font-size:9px">${_secM[p.sym]||'Market'}</td>
            <td style="font-weight:600">${Math.round(pr).toLocaleString('en-IN')}</td>
            <td><span style="font-size:8px;font-weight:700;padding:1px 6px;border-radius:2px;${_sc2(p.setup)}">${(p.setup||'—').toUpperCase()}</span></td>
            <td style="color:var(--am)">${ezLo}–${ezHi}</td>
            <td style="color:var(--go);font-weight:600">${tgt.toLocaleString('en-IN')}</td>
            <td style="color:var(--st)">${sl.toLocaleString('en-IN')}</td>
            <td style="color:${rr>=2.5?'var(--go)':rr>=2?'var(--am)':'var(--st)'};font-weight:600">1:${rr.toFixed(1)}</td>
            <td>${_gateDots(p.g1||'wt',p.g2||'wt',p.g3||'wt',p.g4||'wt',p.g5||'wt')}</td>
            <td style="color:${(()=>{const pc=[p.g1,p.g2,p.g3,p.g4,p.g5].filter(g=>g==='go').length;return pc>=4?'var(--go)':pc>=3?'var(--am)':'var(--t2)'})()};font-weight:700">${[p.g1,p.g2,p.g3,p.g4,p.g5].filter(g=>g==='go').length}/5</td>
            <td><span style="font-size:8px;font-weight:700;padding:1px 6px;border-radius:2px;${(()=>{const pc=[p.g1,p.g2,p.g3,p.g4,p.g5].filter(g=>g==='go').length;return pc>=4?'color:var(--go);border:1px solid var(--go-b)':pc>=3?'color:var(--am);border:1px solid var(--am-b)':'color:var(--t2);border:1px solid var(--b1)'})()}">${(()=>{const pc=[p.g1,p.g2,p.g3,p.g4,p.g5].filter(g=>g==='go').length;return pc>=4?'EXECUTE':pc>=3?'WAIT':'NO TRADE'})()}</span></td>
            <td style="color:${ocColor};font-size:9px;font-weight:700">${ocLabel}${p.signal_time?'<br><span style="font-size:7px;color:var(--t2);font-weight:400">⏱ '+p.signal_time+'</span>':''}</td>
            <td style="color:var(--t2)">${_holdM[p.setup]||'5–8d'}</td>
            <td><span style="font-size:10px;font-weight:700;color:${_sc(sc)}">${sc}</span><div class="swbar"><div class="swbf" style="width:${sc}%;background:${_sc(sc)}"></div></div></td>
          </tr>`;
        }).join('');
      } else {
        swBody.innerHTML = `<tr><td colspan="14" style="text-align:center;color:var(--am);padding:20px;font-size:9px;line-height:1.8">
          No qualifying stock setups on ${val}<br>
          <span style="color:var(--t2)">Requires valid Kite API token — regenerate token and reload</span>
        </td></tr>`;
      }
    }

    // ── [HIST] Key Levels ────────────────────────────────────────────────────
    if(d.nifty_ohlc) {
      const o   = d.nifty_ohlc;
      const nc  = Math.round(o.c), nh = Math.round(o.h), nl = Math.round(o.l), no = Math.round(o.o);
      const atm = Math.round(nc / 50) * 50;
      const bnP = d.prices && d.prices.BANKNIFTY ? Math.round(d.prices.BANKNIFTY.price) : null;
      const bnATM = bnP ? Math.round(bnP / 100) * 100 : null;
      const mp  = d.chain && d.chain.max_pain ? Math.round(d.chain.max_pain) : atm;
      const pcr = d.chain ? (d.chain.pcr||1.0) : 1.0;
      const fiiNet = d.fii ? d.fii.fii_net : null;
      const bullDay = nc > no;

      const lv2 = document.querySelector('#tp-levels .lv2');
      if(lv2 && lv2.children.length >= 2) {
        lv2.children[0].innerHTML = `
          <div class="lvhd">NIFTY 50 — ${val} ${bullDay?'▲ BULL':'▼ BEAR'}</div>
          <div class="lvl"><span class="lk">Weekly supply</span><span class="lp">${_loc(atm+500)}</span><span class="lt br">RESIST</span></div>
          <div class="lvl"><span class="lk">Day High</span><span class="lp cg">${_loc(nh)}</span><span class="lt br">RESIST</span></div>
          <div class="lvl"><span class="lk">CE wall (ATM+100)</span><span class="lp cr">${_loc(atm+100)}</span><span class="lt br">CE WALL</span></div>
          <div class="lvl" style="background:var(--am-d);border-color:var(--am-b)"><span class="lk">Max Pain / ATM</span><span class="lp ca">${_loc(mp)}</span><span class="lt ba">GRAVITY</span></div>
          <div class="lvl"><span class="lk">Open / Close</span><span class="lp cb">${_loc(no)} / ${_loc(nc)}</span><span class="lt bb">EOD</span></div>
          <div class="lvl" style="background:var(--go-d);border-color:var(--go-b)"><span class="lk">PE wall (ATM−100)</span><span class="lp cg">${_loc(atm-100)}</span><span class="lt bg">PE WALL</span></div>
          <div class="lvl"><span class="lk">Day Low</span><span class="lp ca">${_loc(nl)}</span><span class="lt ba">LOW</span></div>
          <div class="lvl"><span class="lk">Weekly demand</span><span class="lp">${_loc(atm-500)}</span><span class="lt bg">SUPPORT</span></div>`;

        const bnHtml = bnP ? `
          <div class="lvhd">BANK NIFTY — ${val}</div>
          <div class="lvl"><span class="lk">Weekly supply</span><span class="lp">${_loc(bnATM+2000)}</span><span class="lt br">RESIST</span></div>
          <div class="lvl"><span class="lk">CE wall</span><span class="lp cr">${_loc(bnATM+1000)}</span><span class="lt br">CE WALL</span></div>
          <div class="lvl"><span class="lk">Close / ATM</span><span class="lp cb">${_loc(bnP)}</span><span class="lt bb">ATM</span></div>
          <div class="lvl" style="background:var(--go-d);border-color:var(--go-b)"><span class="lk">PE wall</span><span class="lp cg">${_loc(bnATM-1000)}</span><span class="lt bg">PE WALL</span></div>
          <div class="lvl"><span class="lk">Weekly demand</span><span class="lp">${_loc(bnATM-2000)}</span><span class="lt bg">SUPPORT</span></div>` : '';
        const tp = d.stock_picks && d.stock_picks[0];
        const pickHtml = tp ? `
          <div class="lvhd" style="margin-top:8px">${tp.sym} — TOP PICK ${val}</div>
          <div class="lvl" style="background:var(--go-d);border-color:var(--go-b)"><span class="lk">Entry</span><span class="lp cg">${tp.entry||'—'}</span><span class="lt bg">DEMAND</span></div>
          <div class="lvl"><span class="lk">Target</span><span class="lp cg">${tp.target||'—'}</span><span class="lt bg">TGT</span></div>
          <div class="lvl"><span class="lk">Stop</span><span class="lp cr">${tp.sl||'—'}</span><span class="lt br">SL</span></div>` : '';
        lv2.children[1].innerHTML = bnHtml + pickHtml;
      }
      const wi = document.querySelector('#tp-levels .wbox .wi');
      if(wi) {
        const dMP  = nc - mp;
        const parts = [`Nifty ${Math.abs(dMP)} pts ${dMP>=0?'above':'below'} Max Pain ${_loc(mp)} · PCR ${pcr.toFixed(2)} · Range ${_loc(nh-nl)} pts`];
        if(fiiNet !== null) parts.push(`FII ${fiiNet>=0?'net buy':'net sell'} ₹${Math.abs(fiiNet).toFixed(0)} Cr`);
        wi.textContent = parts.join(' · ');
      }
    }

    // ── [HIST] Kill Zones — OI Pulse + zone cards ───────────────────────────
    if(d.nifty_ohlc) {
      const o = d.nifty_ohlc;
      const nc = o.c, no2 = o.o, nh2 = o.h, nl2 = o.l;
      const dayPts = Math.round(nc - no2);
      const range  = Math.round(nh2 - nl2);
      const bullD  = nc > no2;
      // Simulate rough intraday breakdown from OHLC
      const oipEl = document.getElementById('oip');
      if(oipEl) {
        const q1 = Math.round(dayPts * 0.35), q2 = Math.round(dayPts * 0.55), q3 = Math.round(dayPts * 0.75);
        const c1 = q1>0?'go':q1<0?'st':'am', c2 = q2>0?'go':q2<0?'st':'am', c3 = q3>0?'go':q3<0?'st':'am', c4 = dayPts>0?'go':dayPts<0?'st':'am';
        const s = n => (n>=0?'+':'')+n;
        oipEl.innerHTML = `
          <div style="font-size:8px;color:var(--am);margin-bottom:4px;padding:3px 6px;background:var(--am-d);border-radius:2px;border:1px solid var(--am-b)">
            EOD estimate — Intraday OI data not available for past dates
          </div>
          <div class="oip ${c1}"><span class="oip-t">9:30</span><span class="oip-oi">—</span><span class="oip-pt">${s(q1)}pt</span><span class="oip-lbl">${bullD?'Open drive':'Opening weakness'}</span></div>
          <div class="oip ${c2}"><span class="oip-t">11:00</span><span class="oip-oi">—</span><span class="oip-pt">${s(q2)}pt</span><span class="oip-lbl">${Math.abs(q2)>Math.abs(q1)?'Momentum building':'Stalling'}</span></div>
          <div class="oip ${c3}"><span class="oip-t">13:30</span><span class="oip-oi">—</span><span class="oip-pt">${s(q3)}pt</span><span class="oip-lbl">Trend zone close</span></div>
          <div class="oip ${c4}"><span class="oip-t">15:30</span><span class="oip-oi">—</span><span class="oip-pt">${s(dayPts)}pt</span><span class="oip-lbl">EOD close · Range ${range}pts</span></div>`;
      }
      // Update Expiry zone card with historical max pain
      const mp2 = d.chain && d.chain.max_pain ? Math.round(d.chain.max_pain) : Math.round(Math.round(nc/50)*50);
      const kzCards = document.getElementById('kzcards');
      if(kzCards) {
        const cards = kzCards.querySelectorAll('.zcard');
        if(cards.length >= 4) {
          cards[3].querySelector('.zd').textContent = `Exit all positions. OI distortion near expiry. Max Pain gravity at ${_loc(mp2)}.`;
        }
      }
      // Update zone header
      const czEl = document.getElementById('curzone');
      if(czEl) czEl.textContent = `HIST: ${val} · ${bullD?'BULL':'BEAR'} DAY ${dayPts>=0?'+':''}${dayPts}pts`;
    }

    // ── [HIST] Journal — same card format as live session log ────────────────
    const jlog = document.getElementById('jlog');
    if(jlog) {
      const bullDay2 = d.nifty_ohlc ? d.nifty_ohlc.c > d.nifty_ohlc.o : false;
      const dayPts2  = d.nifty_ohlc ? Math.round(d.nifty_ohlc.c - d.nifty_ohlc.o) : 0;
      const pc2      = d.gates ? d.gates.pass_count : 0;
      const verdict2 = d.gates ? d.gates.verdict : '—';
      const vix2     = d.macro ? d.macro.vix : 0;
      const fiiNet2  = d.fii ? d.fii.fii_net : 0;
      // Session summary bar (compact, same header style as live)
      const summaryBar = `<div style="display:flex;gap:6px;flex-wrap:wrap;padding:5px 0 7px;border-bottom:1px solid var(--b1);margin-bottom:6px;font-size:8px;color:var(--t2)">
        <span style="color:var(--bl);font-weight:700;letter-spacing:1px">${val}</span>
        <span style="color:${bullDay2?'var(--go)':'var(--st)'};font-weight:600">${dayPts2>=0?'+':''}${dayPts2}pts</span>
        <span>·</span><span>VIX <b style="color:${vix2<15?'var(--go)':vix2<20?'var(--am)':'var(--st)'}">${vix2.toFixed(1)}</b></span>
        <span>·</span><span>FII <b style="color:${fiiNet2>=0?'var(--go)':'var(--st)'}">${fiiNet2>=0?'+':''}₹${Math.abs(fiiNet2).toFixed(0)}Cr</b></span>
        <span>·</span><span style="color:${pc2>=4?'var(--go)':pc2>=2?'var(--am)':'var(--st)'};font-weight:700">${verdict2}</span>
      </div>`;
      // Render picks as trade-like cards (same .je structure as live renderLog)
      let cardsHtml = '';
      if(d.stock_picks && d.stock_picks.length) {
        cardsHtml = d.stock_picks.map(p => {
          const rb   = p.verdict==='EXECUTE'?'bg':p.verdict==='WATCH'?'ba':'bm';
          const pc   = p.verdict==='EXECUTE'?'win':'';
          const dots = [1,2,3,4,5].map(i=>'<div class="jdt" style="background:'+(i<=p.pc?'var(--go)':'var(--b2)')+'"></div>').join('');
          const pnlTxt = p.entry && p.target ? `+${Math.round(parseFloat(p.target)-parseFloat(p.entry))} pts est.` : '—';
          return `<div class="je">
            <div class="jer1">
              <span class="ji">${p.sym}</span>
              <span class="jdir L">LONG</span>
              <span class="bd ${rb}">${p.verdict}</span>
              <span class="jpnl ${pc}" style="margin-left:auto">${pnlTxt}</span>
            </div>
            <div class="jer2">
              <span style="font-size:8px;color:var(--t2)">${p.setup||'Setup'} · ${p.meta||''}</span>
              <div class="jdts" style="margin-left:auto">${dots}</div>
            </div>
            <div class="jnote">Entry ${p.entry} · SL ${p.sl} · Target ${p.target} · R:R 1:${p.rr}</div>
          </div>`;
        }).join('');
      } else {
        cardsHtml = `<div class="je" style="text-align:center;color:var(--t2);padding:14px 8px;font-size:9px">
          No qualifying setups — ${pc2}/5 gates passed<br>
          <span style="color:var(--am)">${verdict2} on ${val}</span>
        </div>`;
      }
      // NIFTY index card (always shown)
      const niftyCard = d.nifty_ohlc ? (()=>{
        const o2 = d.nifty_ohlc;
        const pts2 = Math.round(o2.c - o2.o);
        const rb2 = pts2>=0?'bg':'br';
        const res2 = pts2>=0?'BULL':'BEAR';
        const dots2 = [1,2,3,4,5].map(i=>'<div class="jdt" style="background:'+(i<=pc2?'var(--go)':'var(--b2)')+'"></div>').join('');
        return `<div class="je">
          <div class="jer1">
            <span class="ji">NIFTY 50</span>
            <span class="jdir ${pts2>=0?'L':'S'}">${pts2>=0?'BULL':'BEAR'}</span>
            <span class="bd ${rb2}">${res2}</span>
            <span class="jpnl ${pts2>=0?'win':'loss'}" style="margin-left:auto">${pts2>=0?'+':''}${pts2} pts</span>
          </div>
          <div class="jer2">
            <span style="font-size:8px;color:var(--t2)">INDEX · Day move · O:${Math.round(o2.o).toLocaleString('en-IN')} H:${Math.round(o2.h).toLocaleString('en-IN')} L:${Math.round(o2.l).toLocaleString('en-IN')} C:${Math.round(o2.c).toLocaleString('en-IN')}</span>
            <div class="jdts" style="margin-left:auto">${dots2}</div>
          </div>
          <div class="jnote">VIX ${vix2.toFixed(1)} · PCR ${d.chain?(d.chain.pcr||0).toFixed(2):'—'} · FII ${fiiNet2>=0?'+':''}₹${Math.abs(fiiNet2).toFixed(0)}Cr · ${verdict2}</div>
        </div>`;
      })() : '';
      jlog.innerHTML = summaryBar + niftyCard + cardsHtml;
    }
    // Disable LOG TRADE button in historical mode
    const logBtn = document.querySelector('#tp-journal .btn-g');
    if(logBtn) { logBtn.disabled = true; logBtn.style.opacity = '0.35'; logBtn.title = 'Switch to LIVE to log trades'; }

    notify('Historical data loaded: ' + val);
  } catch(e) {
    console.error('tbDateChange error:', e);
    // Reset hist mode so UI doesn't freeze
    window._histMode = false;
    document.getElementById('tb-live-btn').style.display = 'none';
    document.getElementById('live-pill').style.opacity = '1';
    const banner2 = document.getElementById('hist-banner');
    if(banner2) {
      document.getElementById('hist-banner-date').textContent = 'ERROR — ' + e.message;
      banner2.style.background = 'rgba(255,51,85,.18)';
      setTimeout(() => {
        banner2.style.display = 'none';
        banner2.style.background = '';
      }, 5000);
    }
    notify('Day view failed: ' + e.message);
  }
}

// ═══════════════════════════════════════════════════════════════════
// SPIKE RADAR — entry/exit tracker (frontend-only, no backend change)
// ═══════════════════════════════════════════════════════════════════
window._spikeLog   = [];   // all spikes seen today
window._srFilter   = 'all';
window._srPrevKeys = new Set(); // deduplicate re-broadcasts

function _srScore(sp) {
  let s = 0;
  // Signal 1 — strength (absorption proxy): +3 hi, +1 md
  if(sp.strength === 'hi') s += 3;
  else if(sp.strength === 'md') s += 1;
  // Signal 2 — volume acceleration: +2 if >=8x, +1 if >=5x
  const vm = sp.vol_mult || 0;
  if(vm >= 8) s += 2; else if(vm >= 5) s += 1;
  // Signal 3 — price spike: +2 if >=2%, +1 if >=1.5%
  const cp = Math.abs(sp.chg_pct || 0);
  if(cp >= 2) s += 2; else if(cp >= 1.5) s += 1;
  // Signal 4 — OI buildup: +2 if >=20%, +1 if >=12%
  const oi = Math.abs(sp.oi_pct || 0);
  if(oi >= 20) s += 2; else if(oi >= 12) s += 1;
  // Signal 5 — VWAP alignment (from signals state if available)
  if(typeof window._lastVwapAbove !== 'undefined') {
    if(sp.type === 'buy' && window._lastVwapAbove) s += 1;
    if(sp.type === 'sell' && !window._lastVwapAbove) s += 1;
  }
  // Signal 6 — buy flow (type buy = aggressive buyers confirmed)
  if(sp.type === 'buy' || sp.type === 'sell') s += 1;
  return Math.min(s, 10);
}

function _srSigFlags(sp) {
  const vm = sp.vol_mult || 0;
  const cp = Math.abs(sp.chg_pct || 0);
  const oi = Math.abs(sp.oi_pct || 0);
  return {
    price:    cp >= 1.5,
    vol:      vm >= 2.5,
    oi:       oi >= 12,
    strength: sp.strength === 'hi' || sp.strength === 'md',
    vwap:     typeof window._lastVwapAbove !== 'undefined'
                ? (sp.type==='buy' ? window._lastVwapAbove : !window._lastVwapAbove)
                : false,
    flow:     sp.type === 'buy' || sp.type === 'sell',
  };
}

function _srEntry(sp) {
  const p = sp.price || 0;
  if(!p) return null;
  const isBuy = sp.type !== 'sell';
  const entry = isBuy ? +(p * 1.0005).toFixed(2) : +(p * 0.9995).toFixed(2);
  const sl    = isBuy ? +(entry * 0.9975).toFixed(2) : +(entry * 1.0025).toFixed(2);
  const t1    = isBuy ? +(entry * 1.003).toFixed(2)  : +(entry * 0.997).toFixed(2);
  const t2    = isBuy ? +(entry * 1.006).toFixed(2)  : +(entry * 0.994).toFixed(2);
  return {entry, sl, t1, t2, isBuy};
}

function srIngestSpikes(arr) {
  if(!arr || !arr.length) return;
  let changed = false;
  arr.forEach(sp => {
    const key = sp.symbol + '|' + sp.time + '|' + sp.type;
    if(window._srPrevKeys.has(key)) return;
    window._srPrevKeys.add(key);
    const score  = _srScore(sp);
    if(score < 3) return; // skip very weak signals
    const levels = _srEntry(sp);
    if(!levels) return;
    window._spikeLog.unshift({
      id:          key,
      symbol:      sp.symbol,
      time:        sp.time,
      type:        sp.type,
      chg_pct:     sp.chg_pct,
      vol_mult:    sp.vol_mult,
      oi_pct:      sp.oi_pct,
      trigger:     sp.trigger,
      signal:      sp.signal,
      strength:    sp.strength,
      score:       score,
      backend_score: sp.score || 0,
      sigs:        _srSigFlags(sp),
      ...levels,
      status:   'LIVE',        // LIVE → HIT_T1 → HIT_T2 / HIT_SL / EXPIRED
      detectedAt: Date.now(),
    });
    changed = true;
    // Flash tab badge
    const tab = document.getElementById('sr-tab');
    if(tab) { tab.style.background='rgba(0,232,122,.15)'; setTimeout(()=>{tab.style.background=''},2000); }
  });
  // Keep max 50 entries
  if(window._spikeLog.length > 50) window._spikeLog.length = 50;
  if(changed) srRender();
}

function srUpdatePrices(prices) {
  if(!prices || !window._spikeLog.length) return;
  const now = Date.now();
  let changed = false;
  window._spikeLog.forEach(sp => {
    if(sp.status === 'HIT_T2' || sp.status === 'HIT_SL' || sp.status === 'EXPIRED') return;
    // Expire after 30 min
    if(now - sp.detectedAt > 30 * 60 * 1000) { sp.status = 'EXPIRED'; changed = true; return; }
    const priceObj = prices[sp.symbol];
    if(!priceObj) return;
    const cur = priceObj.price;
    if(sp.isBuy) {
      if(cur <= sp.sl)              { sp.status = 'HIT_SL'; changed = true; }
      else if(cur >= sp.t2)         { sp.status = 'HIT_T2'; changed = true; }
      else if(cur >= sp.t1 && sp.status === 'LIVE') { sp.status = 'HIT_T1'; changed = true; }
    } else {
      if(cur >= sp.sl)              { sp.status = 'HIT_SL'; changed = true; }
      else if(cur <= sp.t2)         { sp.status = 'HIT_T2'; changed = true; }
      else if(cur <= sp.t1 && sp.status === 'LIVE') { sp.status = 'HIT_T1'; changed = true; }
    }
  });
  if(changed) srRender();
}

function srFilter(f, btn) {
  window._srFilter = f;
  document.querySelectorAll('#tp-spikescanner .sfbar .sf').forEach(b => b.classList.remove('on'));
  if(btn) btn.classList.add('on');
  srRender();
}

function srRender() {
  const list = document.getElementById('sr-list');
  const empty = document.getElementById('sr-empty');
  if(!list) return;

  // Counters
  const counts = {total:0, live:0, t1:0, t2:0, sl:0, exp:0};
  window._spikeLog.forEach(s => {
    counts.total++;
    if(s.status==='LIVE') counts.live++;
    else if(s.status==='HIT_T1') counts.t1++;
    else if(s.status==='HIT_T2') counts.t2++;
    else if(s.status==='HIT_SL') counts.sl++;
    else if(s.status==='EXPIRED') counts.exp++;
  });
  ['total','live','t1','t2','sl','exp'].forEach(k => {
    const el = document.getElementById('sr-cnt-'+k);
    if(el) el.textContent = counts[k];
  });

  // Filter
  const f = window._srFilter;
  const visible = window._spikeLog.filter(s => {
    if(f === 'all')      return true;
    if(f === 'buy')      return s.type === 'buy';
    if(f === 'sell')     return s.type === 'sell';
    if(f === 'live')     return s.status === 'LIVE';
    if(f === 'hit')      return s.status === 'HIT_T1' || s.status === 'HIT_T2';
    if(f === 'resolved') return s.status !== 'LIVE';
    return true;
  });

  if(!visible.length) {
    if(empty) empty.style.display = 'block';
    // Remove existing cards
    list.querySelectorAll('.sr-card').forEach(c => c.remove());
    return;
  }
  if(empty) empty.style.display = 'none';

  const fmtP = v => v < 100 ? v.toFixed(2) : v < 2000 ? v.toFixed(1) : Math.round(v).toLocaleString('en-IN');
  const statusHtml = s => {
    if(s==='LIVE')    return '<span class="sr-status sr-st-live">● LIVE</span>';
    if(s==='HIT_T1')  return '<span class="sr-status sr-st-t1">✓ HIT T1</span>';
    if(s==='HIT_T2')  return '<span class="sr-status sr-st-t2">✓✓ HIT T2</span>';
    if(s==='HIT_SL')  return '<span class="sr-status sr-st-sl">✗ STOPPED</span>';
    if(s==='EXPIRED') return '<span class="sr-status sr-st-exp">EXPIRED</span>';
    return '';
  };
  const scoreClass = sc => sc >= 7 ? 's-hi' : sc >= 4 ? 's-md' : 's-lo';
  const sigDot = (label, on) =>
    `<span class="sr-sig-dot ${on?'on':'off'}">${label}</span>`;

  const html = visible.map(sp => {
    const isResolved = sp.status !== 'LIVE';
    const dirCol = sp.isBuy ? 'var(--go)' : 'var(--st)';
    const dirArrow = sp.isBuy ? '▲' : '▼';
    const sc = sp.sigs;
    return `<div class="sr-card sr-${sp.type==='buy'?'buy':sp.type==='sell'?'sell':'oi'}${isResolved?' sr-resolved':''}">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
        <div class="sr-score ${scoreClass(sp.score)}">${sp.score}</div>
        <div style="flex:1">
          <div style="display:flex;align-items:baseline;gap:6px">
            <span style="font-size:13px;font-weight:700;color:var(--t0)">${sp.symbol}</span>
            <span style="font-size:11px;font-weight:700;color:${dirCol}">${dirArrow} ${sp.isBuy?'BUY':'SELL'}</span>
            <span style="font-size:8px;color:var(--t2);margin-left:2px">${sp.time||''}</span>
          </div>
          <div style="font-size:8.5px;color:var(--t2);margin-top:1px">${sp.trigger||sp.signal||''}</div>
        </div>
        ${statusHtml(sp.status)}
      </div>
      <div style="display:flex;gap:3px;flex-wrap:wrap;margin-bottom:5px">
        ${sigDot('PRICE',sc.price)}
        ${sigDot('VOL',sc.vol)}
        ${sigDot('OI',sc.oi)}
        ${sigDot('STRENGTH',sc.strength)}
        ${sigDot('VWAP',sc.vwap)}
        ${sigDot('BUY FLOW',sc.flow)}
      </div>
      <div class="sr-lvl">
        <div class="sr-lvl-box" style="background:rgba(255,255,255,.04)">
          <div class="sr-lvl-k">ENTRY</div>
          <div class="sr-lvl-v" style="color:var(--t0)">${fmtP(sp.entry)}</div>
        </div>
        <div class="sr-lvl-box" style="background:rgba(255,51,85,.08)">
          <div class="sr-lvl-k">SL</div>
          <div class="sr-lvl-v" style="color:var(--st)">${fmtP(sp.sl)}</div>
        </div>
        <div class="sr-lvl-box" style="background:rgba(0,140,255,.08)">
          <div class="sr-lvl-k">T1 +0.3%</div>
          <div class="sr-lvl-v" style="color:var(--bl)">${fmtP(sp.t1)}</div>
        </div>
        <div class="sr-lvl-box" style="background:rgba(0,232,122,.08)">
          <div class="sr-lvl-k">T2 +0.6%</div>
          <div class="sr-lvl-v" style="color:var(--go)">${fmtP(sp.t2)}</div>
        </div>
      </div>
      <div style="display:flex;align-items:center;gap:6px;font-size:8px;color:var(--t2)">
        <span>Vol <b style="color:${sp.vol_mult>=5?'var(--go)':sp.vol_mult>=2?'var(--am)':'var(--t2)'}">${sp.vol_mult||0}×</b></span>
        <span>·</span>
        <span>OI <b style="color:${(sp.oi_pct||0)>=0?'var(--go)':'var(--st)'}">${(sp.oi_pct||0)>=0?'+':''}${(sp.oi_pct||0).toFixed(1)}%</b></span>
        <span>·</span>
        <span>Chg <b style="color:${(sp.chg_pct||0)>=0?'var(--go)':'var(--st)'}">${(sp.chg_pct||0)>=0?'+':''}${(sp.chg_pct||0).toFixed(2)}%</b></span>
        <span style="margin-left:auto;font-size:7.5px;opacity:.6">Score ${sp.backend_score||sp.score*10}/100 · Sigs ${sp.score}/10 · R:R 1:2.4</span>
      </div>
    </div>`;
  }).join('');

  // Replace content (preserve empty msg element)
  list.querySelectorAll('.sr-card').forEach(c => c.remove());
  list.insertAdjacentHTML('afterbegin', html);
}

// ═══════════════════════════════════════════════════════════════════
// SPIKE RADAR — BACKTEST
// ═══════════════════════════════════════════════════════════════════

function srSubTab(tab) {
  const isLive = tab === 'live';
  document.getElementById('sr-live-panel').style.display = isLive ? 'flex' : 'none';
  document.getElementById('sr-bt-panel').style.display   = isLive ? 'none' : 'flex';
  const lb = document.getElementById('sr-sub-live');
  const bb = document.getElementById('sr-sub-bt');
  lb.style.background    = isLive ? 'var(--s2)' : 'var(--s1)';
  lb.style.color         = isLive ? 'var(--go)'  : 'var(--t2)';
  lb.style.borderBottom  = isLive ? '2px solid var(--go)' : '2px solid transparent';
  bb.style.background    = isLive ? 'var(--s1)' : 'var(--s2)';
  bb.style.color         = isLive ? 'var(--t2)'  : 'var(--bl)';
  bb.style.borderBottom  = isLive ? '2px solid transparent' : '2px solid var(--bl)';
  if(!isLive) {
    // default date range: last 7 days
    const to = new Date(); to.setDate(to.getDate()-1);
    const fr = new Date(); fr.setDate(fr.getDate()-8);
    const fmt = d => d.toISOString().slice(0,10);
    const fi = document.getElementById('srbt-from');
    const ti = document.getElementById('srbt-to');
    if(fi && !fi.value) fi.value = fmt(fr);
    if(ti && !ti.value) ti.value = fmt(to);
  }
}

async function srBtRun() {
  const from = document.getElementById('srbt-from').value;
  const to   = document.getElementById('srbt-to').value;
  if(!from || !to) { alert('Select date range'); return; }
  const status = document.getElementById('srbt-status');
  status.textContent = 'Running...';
  status.style.color = 'var(--am)';
  document.getElementById('srbt-empty').style.display = 'block';
  document.getElementById('srbt-empty').textContent   = 'Fetching 1-min candles and running spike detection...';
  document.getElementById('srbt-tbl').style.display   = 'none';
  document.getElementById('srbt-summary').style.display = 'none';

  // Collect filter params
  const volMin   = parseFloat(document.getElementById('srbt-volmin')?.value) || 2.0;
  const priceMin = parseFloat(document.getElementById('srbt-pricemin')?.value) || 0.3;
  const tfrom    = document.getElementById('srbt-tfrom')?.value || '09:15';
  const tto      = document.getElementById('srbt-tto')?.value || '14:30';
  const trendF   = document.getElementById('srbt-trend')?.checked !== false;
  const minScore = parseInt(document.getElementById('srbt-minscore')?.value) || 45;

  const url = `${API}/api/spikes/backtest?from_date=${from}&to_date=${to}`
    + `&vol_min=${volMin}&price_min=${priceMin}`
    + `&time_from=${encodeURIComponent(tfrom)}&time_to=${encodeURIComponent(tto)}`
    + `&trend_filter=${trendF}&min_score=${minScore}`;

  try {
    const r = await fetch(url);
    const d = await r.json();
    if(d.error || d.detail || !d.summary) {
      const msg = d.error || (Array.isArray(d.detail) ? d.detail[0]?.msg : d.detail) || 'Unexpected response — check backend logs';
      status.textContent = 'Error: ' + msg; status.style.color='var(--st)';
      document.getElementById('srbt-empty').textContent = 'Error: ' + msg;
      return;
    }

    const s = d.summary;
    document.getElementById('srbt-total').textContent = s.total;
    document.getElementById('srbt-wr').textContent    = s.win_rate + '%';
    document.getElementById('srbt-t1').textContent    = s.hit_t1;
    document.getElementById('srbt-t2').textContent    = s.hit_t2;
    document.getElementById('srbt-sl').textContent    = s.hit_sl;
    document.getElementById('srbt-exp').textContent   = s.expired;
    document.getElementById('srbt-apnl').textContent  = (s.avg_pnl >= 0 ? '+' : '') + s.avg_pnl + '%';
    document.getElementById('srbt-apnl').style.color  = s.avg_pnl >= 0 ? 'var(--go)' : 'var(--st)';
    document.getElementById('srbt-wr').style.color    = s.win_rate >= 55 ? 'var(--go)' : s.win_rate >= 45 ? 'var(--am)' : 'var(--st)';
    const expEl = document.getElementById('srbt-exp2');
    if(expEl && s.expectancy_pct !== undefined){
      expEl.textContent = (s.expectancy_pct >= 0 ? '+' : '') + s.expectancy_pct + '%';
      expEl.style.color = s.expectancy_pct >= 0 ? 'var(--am)' : 'var(--st)';
    }
    const avgScEl = document.getElementById('srbt-avgscore');
    if(avgScEl && s.avg_score !== undefined){
      avgScEl.textContent = s.avg_score;
      avgScEl.style.color = s.avg_score >= 60 ? 'var(--go)' : s.avg_score >= 45 ? 'var(--am)' : 'var(--st)';
    }
    document.getElementById('srbt-summary').style.display = 'flex';

    if(!d.results || !d.results.length) {
      document.getElementById('srbt-empty').textContent = 'No spikes detected with these filters. Try lowering Vol≥ or Move≥.';
      document.getElementById('srbt-empty').style.display = 'block';
      status.textContent = 'Done — 0 spikes'; status.style.color='var(--t2)';
      return;
    }

    // Group results by date for context
    const rMap = { HIT_T2:'var(--go)', HIT_T1:'var(--bl)', HIT_SL:'var(--st)', EXPIRED:'var(--t2)' };
    const rows = d.results.map(r => {
      const rc = rMap[r.result] || 'var(--t2)';
      const pc = r.pnl_pct >= 0 ? 'var(--go)' : 'var(--st)';
      const tc = r.type === 'BUY' ? 'var(--go)' : 'var(--st)';
      const dateStr = r.time.slice(0,10);
      const timeStr = r.time.slice(11);
      return `<tr>
        <td style="color:var(--t0);font-weight:700"><span class="tv-link" data-tvsym="${r.symbol}">${r.symbol}</span></td>
        <td style="color:var(--t2);font-size:9px">${dateStr}<br><span style="color:var(--t1)">${timeStr}</span></td>
        <td style="color:${tc};font-weight:700">${r.type}</td>
        <td style="color:${r.vol_mult>=5?'var(--go)':r.vol_mult>=3?'var(--am)':'var(--t2)'}">${r.vol_mult}×</td>
        <td style="color:${tc}">${r.chg_pct > 0 ? '+' : ''}${r.chg_pct}%</td>
        <td style="font-size:9px">${r.entry}</td>
        <td style="color:var(--st);font-size:9px">${r.sl}</td>
        <td style="color:var(--bl);font-size:9px">${r.t1}</td>
        <td style="color:var(--go);font-size:9px">${r.t2}</td>
        <td style="color:${rc};font-weight:700">${r.result.replace('_',' ')}</td>
        <td style="color:${pc};font-weight:700">${r.pnl_pct >= 0 ? '+' : ''}${r.pnl_pct}%</td>
        <td style="color:${r.score>=60?'var(--go)':r.score>=45?'var(--am)':'var(--st)'};font-weight:700">${r.score||'—'}</td>
      </tr>`;
    }).join('');

    document.getElementById('srbt-tbody').innerHTML = rows;
    document.getElementById('srbt-tbl').style.display = 'table';
    document.getElementById('srbt-empty').style.display = 'none';
    const wrColor = s.win_rate >= 55 ? '✓' : s.win_rate >= 45 ? '~' : '✗';
    status.textContent = `Done — ${s.total} spikes · ${s.win_rate}% WR ${wrColor} · avg ${s.avg_pnl >= 0 ? '+' : ''}${s.avg_pnl}%`;
    status.style.color = s.win_rate >= 55 ? 'var(--go)' : s.win_rate >= 45 ? 'var(--am)' : 'var(--st)';
  } catch(e) {
    status.textContent = 'Failed: ' + e.message;
    status.style.color = 'var(--st)';
    document.getElementById('srbt-empty').textContent = 'Request failed. Is backend running?';
  }
}

// ═══════════════════════════════════════════════════════════════════

function toggleTheme() {
  const isLight = document.documentElement.classList.toggle('light');
  localStorage.setItem('nse-theme', isLight ? 'light' : 'dark');
}
if(localStorage.getItem('nse-theme') === 'light') document.documentElement.classList.add('light');

function tbGoLive() {
  window._histMode = false;
  window._histData = null;
  window._histDate = null;
  document.getElementById('tb-date').value = '';
  document.getElementById('tb-live-btn').style.display = 'none';
  document.getElementById('live-pill').style.opacity = '1';
  document.getElementById('hist-banner').style.display = 'none';
  // Restore journal
  if(typeof renderLog === 'function') renderLog();
  const logBtn = document.querySelector('#tp-journal .btn-g');
  if(logBtn) { logBtn.disabled = false; logBtn.style.opacity = '1'; logBtn.title = ''; }
  // Restore right panel header + badge
  const rpHd = document.getElementById('rp-hd');
  if(rpHd) {
    rpHd.textContent = 'INTRADAY PICKS';
    const rpBadge = rpHd.nextElementSibling;
    if(rpBadge) { rpBadge.textContent = 'LIVE RANKED'; rpBadge.className = 'bd bg'; }
  }
  _renderSwingLive();
  _renderLiveLevels();
  _renderOIPulseLive();
  _renderVolumeRadarLive();
  _renderMacroContext();
  // Reconnect WS to get fresh live data
  if(window._liveConnect) window._liveConnect();
  notify('Back to live data');
}

// ═══════════════════════════════════════════════
// DAY VIEW (backtest tab drill-down + topbar historical mode)
// ═══════════════════════════════════════════════
function dvLoadDate(date) {
  // Set the topbar date picker + enter historical mode (full UI replay)
  const tbDateEl = document.getElementById('tb-date');
  if(tbDateEl) tbDateEl.value = date;
  tbDateChange(date);
  // Also load the detailed drill-down in the backtest panel
  document.getElementById('dv-date').value = date;
  dvLoad();
  document.getElementById('dv-panel').scrollIntoView({behavior:'smooth', block:'nearest'});
}

async function dvLoad() {
  const date = document.getElementById('dv-date').value;
  if(!date) { notify('Select a date first'); return; }
  document.getElementById('dv-hint').textContent = 'Loading...';
  try {
    const r = await fetch(`${API}/api/backtest/dayview?date=${date}`);
    const d = await r.json();
    if(d.error) { document.getElementById('dv-hint').textContent = d.error; return; }
    dvRender(d);
    document.getElementById('dv-panel').style.display = 'block';
    document.getElementById('dv-hint').textContent = `Loaded: ${date}`;
  } catch(e) {
    document.getElementById('dv-hint').textContent = 'Failed — check backend';
  }
}

function dvRender(d) {
  const gc = s => s==='go'?'var(--go)':s==='st'?'var(--st)':s==='am'?'var(--am)':'var(--t2)';
  const gl = s => s==='go'?'GO':s==='st'?'NO':s==='am'?'~':'?';
  const f  = (v,dp=0) => v!=null ? (+v).toFixed(dp) : '—';
  const fi = v => v!=null ? Math.round(+v).toLocaleString('en-IN') : '—';

  const vixC = d.vix>=20?'var(--st)':d.vix>=16?'var(--am)':'var(--go)';
  const pcrC = d.pcr>=1.2?'var(--go)':d.pcr<=0.8?'var(--st)':'var(--am)';
  const fiiC = d.fii_net<0?'var(--st)':'var(--go)';

  // ── Compact single-line summary strip ───────────────────────────────────
  document.getElementById('dv-summary').innerHTML = `
    <div style="display:flex;align-items:center;gap:0;flex-wrap:wrap;font-size:11px;font-weight:600;width:100%">
      <span style="color:var(--t2);font-size:8px;letter-spacing:1px;margin-right:8px">NIFTY</span>
      <span style="color:var(--bl);margin-right:12px">${fi(d.nifty)}</span>
      <span style="color:var(--t2);font-size:8px;margin-right:4px">O</span><span style="margin-right:10px">${fi(d.open)}</span>
      <span style="color:var(--go);font-size:8px;margin-right:4px">H</span><span style="color:var(--go);margin-right:10px">${fi(d.high)}</span>
      <span style="color:var(--st);font-size:8px;margin-right:4px">L</span><span style="color:var(--st);margin-right:16px">${fi(d.low)}</span>
      <span style="color:var(--t2);font-size:8px;margin-right:4px">VIX</span>
      <span style="color:${vixC};margin-right:4px">${f(d.vix,1)}</span>
      <span style="color:${vixC};font-size:9px;margin-right:16px">(${d.vix_chg>=0?'+':''}${f(d.vix_chg,1)}%)</span>
      <span style="color:var(--t2);font-size:8px;margin-right:4px">PCR</span>
      <span style="color:${pcrC};margin-right:16px">${f(d.pcr,3)}</span>
      <span style="color:var(--t2);font-size:8px;margin-right:4px">FII</span>
      <span style="color:${fiiC};margin-right:16px">${d.fii_net>=0?'+':''}${f(d.fii_net,0)} Cr</span>
      <span style="color:var(--t2);font-size:8px;margin-right:4px">ATR</span>
      <span style="color:var(--am)">${f(d.atr,1)}</span>
    </div>`;

  // ── Gate cards ───────────────────────────────────────────────────────────
  const gnames = ['G1 REGIME','G2 SMART MON','G3 STRUCTURE','G4 TRIGGER','G5 RISK'];
  function modeHTML(m, label) {
    const vc = m.verdict==='EXECUTE'?'var(--go)':m.verdict==='NO TRADE'?'var(--st)':'var(--am)';
    const gates = ['g1','g2','g3','g4','g5'].map((g,i) => {
      const gs = m[g];
      return `<div style="display:flex;align-items:center;gap:5px;padding:2px 0">
        <span style="width:90px;font-size:8px;color:var(--t2)">${gnames[i]}</span>
        <span style="font-size:9px;font-weight:700;color:${gc(gs.state)};width:18px">${gl(gs.state)}</span>
        <div style="flex:1;background:var(--b2);height:3px;border-radius:2px">
          <div style="width:${gs.score}%;height:3px;border-radius:2px;background:${gc(gs.state)}"></div>
        </div>
        <span style="font-size:8px;color:var(--t2);width:22px;text-align:right">${gs.score}</span>
      </div>`;
    }).join('');

    const pnlC    = m.pnl>0?'var(--go)':m.pnl<0?'var(--st)':'var(--t2)';
    const outcomeC = m.outcome==='WIN'?'var(--go)':m.outcome==='LOSS'?'var(--st)':'var(--t2)';
    const tradeRow = m.entry ? `
      <div style="margin-top:6px;padding-top:5px;border-top:1px solid var(--b2);display:flex;align-items:center;gap:8px;flex-wrap:wrap">
        <span style="font-size:8px;color:var(--t2)">E</span><span style="font-size:10px;font-weight:700;color:var(--bl)">${fi(m.entry)}</span>
        <span style="color:var(--b2)">→</span>
        <span style="font-size:8px;color:var(--t2)">TGT</span><span style="font-size:10px;font-weight:700;color:var(--go)">${fi(m.target)}</span>
        <span style="font-size:8px;color:var(--t2)">SL</span><span style="font-size:10px;font-weight:700;color:var(--st)">${fi(m.stop)}</span>
        <span style="font-size:8px;color:var(--t2)">EXIT</span><span style="font-size:10px;font-weight:600">${fi(m.exit)}</span>
        <span style="margin-left:4px;font-size:11px;font-weight:800;color:${pnlC}">${m.pnl>=0?'+':''}${f(m.pnl)} pts</span>
        <span style="font-size:10px;font-weight:700;color:${outcomeC}">${m.outcome||'—'}</span>
        <span style="margin-left:auto;font-size:8px;color:var(--t2)">next: ${m.next_date||'—'}</span>
      </div>` : `<div style="margin-top:4px;font-size:8px;color:var(--t2)">No next-day data</div>`;

    return `
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:5px">
        <span style="font-size:9px;font-weight:700;letter-spacing:.5px;color:${vc}">${label} — ${m.verdict}</span>
        <span style="font-size:9px;color:var(--t2)">${m.pass_count}/5 gates</span>
      </div>
      ${gates}
      ${tradeRow}`;
  }

  document.getElementById('dv-intra').innerHTML = modeHTML(d.intraday, 'INTRADAY');
  document.getElementById('dv-pos').innerHTML   = modeHTML(d.positional, 'POSITIONAL');
}

// ── Token expiry check — runs once on load ──
(async function checkToken() {
  try {
    const r = await fetch(API+'/api/token-status');
    const d = await r.json();
    if(!d.valid) {
      const banner = document.getElementById('token-banner');
      document.getElementById('token-banner-msg').textContent =
        'Kite access token expired — live data is stale. Regenerate token and restart backend. Error: ' + (d.error||'unknown');
      if(banner) banner.style.display = 'flex';
    }
  } catch(e) {}
})();

// ═══════════════════════════════════════════════
// TRADINGVIEW CHART — opens in new tab (NSE symbols blocked in all embeds)
// ═══════════════════════════════════════════════
const _tvSymMap = {
  'NIFTY':'NSE:NIFTY','BANKNIFTY':'NSE:BANKNIFTY','INDIAVIX':'NSE:INDIAVIX',
  'ICICIBANK':'NSE:ICICIBANK','SBIN':'NSE:SBIN','HDFCBANK':'NSE:HDFCBANK',
  'AXISBANK':'NSE:AXISBANK','KOTAKBANK':'NSE:KOTAKBANK','INDUSINDBK':'NSE:INDUSINDBK',
  'LT':'NSE:LT','TCS':'NSE:TCS','RELIANCE':'NSE:RELIANCE','TATAMOTORS':'NSE:TATAMOTORS',
  'BAJFINANCE':'NSE:BAJFINANCE','TATASTEEL':'NSE:TATASTEEL','INFY':'NSE:INFY',
  'MARUTI':'NSE:MARUTI','SUNPHARMA':'NSE:SUNPHARMA','NIFTY 50':'NSE:NIFTY',
  'BANK NIFTY':'NSE:BANKNIFTY','INDIA VIX':'NSE:INDIAVIX',
};
const _tvIntervalMap = { '5':'5','15':'15','60':'60','D':'D','W':'W' };
let _tvInterval = 'D';
let _tvCurrentSym = '';
let _tvCurrentDate = null;

function _tvOpenNewTab(sym, interval) {
  const tvSym = _tvSymMap[sym] || ('NSE:' + sym.replace(/ /g,''));
  const url = `https://www.tradingview.com/chart/?symbol=${tvSym}&interval=${interval}`;
  window.open(url, '_blank');
}

function openTVChart(sym, date) {
  _tvCurrentSym = sym;
  _tvCurrentDate = date || null;
  const modal = document.getElementById('tv-modal');
  modal.classList.add('open');
  document.getElementById('tv-sym-lbl').textContent = sym;
  const dlbl = document.getElementById('tv-date-lbl');
  if(date) { dlbl.textContent = '📅 ' + date; dlbl.style.display = ''; }
  else { dlbl.style.display = 'none'; }
  document.querySelectorAll('.tvi').forEach(b => b.classList.toggle('on', b.dataset.iv === _tvInterval));
  _renderTVLinks(sym, date);
}

function _renderTVLinks(sym, date) {
  const tvSym = _tvSymMap[sym] || ('NSE:' + sym.replace(/ /g,''));
  const container = document.getElementById('tv-container');
  const isDark = !document.documentElement.classList.contains('light');
  const intervals = [
    {iv:'5',  lbl:'5 min',   icon:'⚡'},
    {iv:'15', lbl:'15 min',  icon:'⏱'},
    {iv:'60', lbl:'1 Hour',  icon:'🕐'},
    {iv:'D',  lbl:'Daily',   icon:'📅'},
    {iv:'W',  lbl:'Weekly',  icon:'📆'},
  ];
  const dateNote = date
    ? `<div style="margin-top:6px;font-size:9px;color:var(--am);letter-spacing:.5px">For date <b>${date}</b>: After opening, press <kbd style="background:var(--s3);padding:1px 5px;border-radius:2px;border:1px solid var(--b1)">Alt+G</kbd> on TradingView to jump to date</div>`
    : '';
  container.innerHTML = `
    <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;gap:16px;padding:24px">
      <div style="text-align:center">
        <div style="font-family:var(--fh);font-size:22px;font-weight:800;color:var(--go);letter-spacing:2px;margin-bottom:4px">${sym}</div>
        <div style="font-size:9px;color:var(--t2);letter-spacing:1px">${tvSym} · NSE INDIA</div>
        ${dateNote}
      </div>
      <div style="display:flex;gap:10px;flex-wrap:wrap;justify-content:center">
        ${intervals.map(iv => `
          <a href="https://www.tradingview.com/chart/?symbol=${tvSym}&interval=${iv.iv}"
             target="_blank"
             style="display:flex;flex-direction:column;align-items:center;gap:6px;padding:14px 20px;
                    border:1px solid var(--b1);border-radius:6px;background:var(--s2);
                    text-decoration:none;color:var(--t0);cursor:pointer;min-width:80px;
                    transition:all .15s"
             onmouseover="this.style.borderColor='var(--go)';this.style.background='var(--go-d)'"
             onmouseout="this.style.borderColor='var(--b1)';this.style.background='var(--s2)'">
            <span style="font-size:20px">${iv.icon}</span>
            <span style="font-size:11px;font-weight:700;letter-spacing:.5px">${iv.lbl}</span>
            <span style="font-size:8px;color:var(--t2)">Open chart ↗</span>
          </a>`).join('')}
      </div>
      <div style="font-size:8px;color:var(--t2);text-align:center;max-width:320px;line-height:1.7">
        TradingView blocks NSE symbols in embedded frames.<br>
        Click any timeframe above to open full chart in new tab.
      </div>
    </div>`;
}

function closeTVChart() {
  document.getElementById('tv-modal').classList.remove('open');
  document.getElementById('tv-container').innerHTML = '';
}

function tvSetInterval(iv, btn) {
  _tvInterval = iv;
  document.querySelectorAll('.tvi').forEach(b => b.classList.remove('on'));
  btn.classList.add('on');
  _tvOpenNewTab(_tvCurrentSym, iv);
}

// ═══════════════════════════════════════════════
// TELEGRAM TEST
// ═══════════════════════════════════════════════
async function testTelegram(){
  try{
    notify('Sending Telegram test...');
    const r = await fetch('/api/telegram/test', {method:'POST'});
    const d = await r.json();
    if(d.ok) notify('✓ Telegram message sent!');
    else notify('✗ Telegram error: ' + (d.error||'unknown'));
  }catch(e){ notify('✗ Backend unreachable'); }
}

// ═══════════════════════════════════════════════
// WHATSAPP TEST
// ═══════════════════════════════════════════════
async function testWhatsapp(){
  try{
    notify('Sending WhatsApp test...');
    const r = await fetch('/api/whatsapp/test', {method:'POST'});
    const d = await r.json();
    if(d.ok) notify('✓ WhatsApp message sent!');
    else notify('✗ WhatsApp error: ' + (d.error||'Set WHATSAPP_PHONE + WHATSAPP_APIKEY in .env'));
  }catch(e){ notify('✗ Backend unreachable'); }
}

// ═══════════════════════════════════════════════
// SOUND ALERTS
// ═══════════════════════════════════════════════
let _soundOn = localStorage.getItem('nse_sound') !== 'off';
(function(){
  const btn = document.getElementById('sound-tog');
  if(btn){ btn.classList.toggle('on', _soundOn); btn.title = _soundOn ? 'Sound ON — click to mute' : 'Sound OFF — click to enable'; }
})();

function toggleSound(){
  _soundOn = !_soundOn;
  localStorage.setItem('nse_sound', _soundOn ? 'on' : 'off');
  const btn = document.getElementById('sound-tog');
  if(btn){ btn.classList.toggle('on', _soundOn); btn.title = _soundOn ? 'Sound ON — click to mute' : 'Sound OFF — click to enable'; }
  notify(_soundOn ? 'Sound alerts ON' : 'Sound alerts OFF');
}

function _playAlert(type){
  if(!_soundOn) return;
  try {
    const ctx = new (window.AudioContext||window.webkitAudioContext)();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.connect(gain); gain.connect(ctx.destination);
    if(type==='execute'){ osc.frequency.setValueAtTime(880,ctx.currentTime); osc.frequency.setValueAtTime(1100,ctx.currentTime+0.1); }
    else if(type==='warn'){ osc.frequency.setValueAtTime(440,ctx.currentTime); }
    else { osc.frequency.setValueAtTime(660,ctx.currentTime); }
    gain.gain.setValueAtTime(0.3,ctx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.001,ctx.currentTime+0.4);
    osc.start(ctx.currentTime); osc.stop(ctx.currentTime+0.4);
  } catch(e){}
}

// ═══════════════════════════════════════════════
// EXPORT CSV
// ═══════════════════════════════════════════════
function exportCSV(){
  const activeTab = document.querySelector('.tp.on');
  if(!activeTab){ notify('No active tab'); return; }
  const id = activeTab.id;
  let rows = [], filename = 'nse_edge_export.csv';

  if(id === 'tp-stocks'){
    filename = 'stock_oi.csv';
    rows.push(['#','Symbol','Price','Chg%','OI','OI%','PCR','Vol/Avg','ATR%','RS%','Signal','Score']);
    activeTab.querySelectorAll('tbody tr').forEach(tr=>{
      rows.push([...tr.querySelectorAll('td')].map(td=>'"'+(td.textContent.trim().replace(/"/g,'""'))+'"'));
    });
  } else if(id === 'tp-swing'){
    filename = 'swing_radar.csv';
    rows.push(['#','Stock','Sector','Price','Setup','Entry Zone','Target','Stop','R:R','Gates','PC','Signal','Outcome','Hold','Score']);
    activeTab.querySelectorAll('tbody tr').forEach(tr=>{
      rows.push([...tr.querySelectorAll('td')].map(td=>'"'+(td.textContent.trim().replace(/"/g,'""'))+'"'));
    });
  } else if(id === 'tp-backtest'){
    filename = 'backtest_results.csv';
    rows.push(['Date','Nifty','VIX','PCR','FII','G1','G2','G3','G4','G5','Verdict','Entry','Target','Stop','Exit','P&L pts','Outcome']);
    activeTab.querySelectorAll('tbody tr').forEach(tr=>{
      rows.push([...tr.querySelectorAll('td')].map(td=>'"'+(td.textContent.trim().replace(/"/g,'""'))+'"'));
    });
  } else if(id === 'tp-pnl'){
    filename = 'pnl_tracker.csv';
    rows.push(['#','Symbol','Direction','Entry','Exit','Qty','P&L','Result']);
    activeTab.querySelectorAll('#pnl-log .pnl-row').forEach((row,i)=>{
      rows.push([...row.querySelectorAll('[data-val]')].map(el=>'"'+(el.dataset.val||el.textContent.trim())+'"'));
    });
  } else {
    notify('Export not supported for this tab');
    return;
  }

  if(rows.length <= 1){ notify('No data to export'); return; }
  const csv = rows.map(r=>Array.isArray(r)?r.join(','):r).join('\n');
  const blob = new Blob([csv], {type:'text/csv'});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
  notify('Exported: ' + filename);
}

// ═══════════════════════════════════════════════
// KEYBOARD SHORTCUTS MODAL
// ═══════════════════════════════════════════════
function showShortcuts(){
  const existing = document.getElementById('kbd-modal');
  if(existing){ existing.remove(); return; }
  const m = document.createElement('div');
  m.id = 'kbd-modal';
  m.style.cssText='position:fixed;inset:0;z-index:9998;background:rgba(0,0,0,.8);display:flex;align-items:center;justify-content:center';
  m.innerHTML=`<div style="background:var(--s1);border:1px solid var(--b1);border-radius:6px;padding:20px;min-width:340px;max-width:480px">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px">
      <div style="font-family:var(--fh);font-size:14px;font-weight:800;letter-spacing:2px;color:var(--t0)">KEYBOARD SHORTCUTS</div>
      <button onclick="document.getElementById('kbd-modal').remove()" style="background:none;border:none;cursor:pointer;color:var(--t2);font-size:14px">✕</button>
    </div>
    ${[
      ['1–9','Switch to tab (Kill Zones=1, Levels=2, Chain=3, Stocks=4, Swing=5, Spike=6, Journal=7, Stats=8, Analytics=9)'],
      ['B','Open Backtest tab'],
      ['S','Toggle sound alerts'],
      ['E','Export current tab as CSV'],
      ['T','Toggle light/dark theme'],
      ['Esc','Close any modal'],
      ['?','Show this help'],
    ].map(([k,d])=>`<div style="display:flex;align-items:flex-start;gap:12px;padding:6px 0;border-bottom:1px solid var(--b1)">
      <kbd style="flex-shrink:0;background:var(--s3);border:1px solid var(--b1);border-radius:3px;padding:2px 8px;font-family:var(--fn);font-size:10px;font-weight:700;color:var(--t0);min-width:28px;text-align:center">${k}</kbd>
      <span style="font-size:9px;color:var(--t2);line-height:1.5">${d}</span>
    </div>`).join('')}
  </div>`;
  m.addEventListener('click', e=>{ if(e.target===m) m.remove(); });
  document.body.appendChild(m);
}

// Global keyboard shortcuts
document.addEventListener('keydown', e => {
  // skip if typing in input
  if(['INPUT','TEXTAREA','SELECT'].includes(e.target.tagName)) return;
  const tabIds = ['killzones','levels','chain','stocks','swing','spikescanner','journal','stats','analytics'];
  if(e.key>='1' && e.key<='9'){ const t=tabIds[parseInt(e.key)-1]; if(t) { const b=document.querySelector(`.tab[onclick*="'${t}'"]`); if(b){sw(t,b);} } return; }
  if(e.key==='b'||e.key==='B'){ const b=document.querySelector(`.tab[onclick*="'backtest'"]`); if(b) sw('backtest',b); return; }
  if(e.key==='s'||e.key==='S'){ toggleSound(); return; }
  if(e.key==='e'||e.key==='E'){ exportCSV(); return; }
  if(e.key==='t'||e.key==='T'){ const tog=document.querySelector('.theme-tog'); if(tog) tog.click(); return; }
  if(e.key==='?'){ showShortcuts(); return; }
});

// ═══════════════════════════════════════════════
// SECTOR HEATMAP
// ═══════════════════════════════════════════════
const _sectors = [
  {name:'Banking',sym:['HDFCBANK','ICICIBANK','AXISBANK','KOTAKBANK','INDUSINDBK'],chg:1.2,weight:3},
  {name:'PSU Bank',sym:['SBIN'],chg:-0.4,weight:1.5},
  {name:'IT',sym:['TCS','INFY'],chg:2.1,weight:2.5},
  {name:'Energy',sym:['RELIANCE'],chg:0.6,weight:2},
  {name:'Auto',sym:['MARUTI','TATAMOTORS'],chg:-1.1,weight:1.5},
  {name:'Pharma',sym:['SUNPHARMA'],chg:0.9,weight:1},
  {name:'NBFC',sym:['BAJFINANCE'],chg:-0.2,weight:1},
  {name:'Infra',sym:['LT'],chg:0.3,weight:1},
  {name:'Steel',sym:['TATASTEEL'],chg:-1.8,weight:1},
  {name:'Index',sym:['NIFTY','BANKNIFTY'],chg:0.4,weight:2},
];

function renderHeatmap(){
  const grid = document.getElementById('hm-grid');
  if(!grid) return;
  // Try to use live price data
  const lp = window._livePrices || {};
  grid.innerHTML = _sectors.map(s => {
    // Try to compute actual change from live data
    let chg = s.chg;
    const prices = s.sym.map(sym => lp[sym]).filter(Boolean);
    if(prices.length){ chg = prices.reduce((a,p)=>a+(p.chg_pct||0),0)/prices.length; }
    const abs = Math.abs(chg);
    const clr = chg>1.5?'var(--go)':chg>0.5?'rgba(0,232,122,.6)':chg>0?'rgba(0,232,122,.3)':chg>-0.5?'rgba(255,170,0,.3)':chg>-1.5?'rgba(255,51,85,.4)':'var(--st)';
    const bg = chg>0?`rgba(0,232,122,${Math.min(0.18,abs*0.07)})`:`rgba(255,51,85,${Math.min(0.18,abs*0.07)})`;
    const border = chg>0?`rgba(0,232,122,${Math.min(0.4,abs*0.15)})`:`rgba(255,51,85,${Math.min(0.4,abs*0.15)})`;
    return`<div style="border-radius:4px;border:1px solid ${border};background:${bg};padding:8px;cursor:pointer;display:flex;flex-direction:column;justify-content:space-between;min-height:70px"
      onclick="hmDrill('${s.name}')"
      onmouseover="this.style.opacity='.8'" onmouseout="this.style.opacity='1'">
      <div style="font-size:9px;font-weight:700;color:var(--t0);letter-spacing:.5px">${s.name}</div>
      <div style="font-size:13px;font-weight:700;color:${clr}">${chg>=0?'+':''}${chg.toFixed(2)}%</div>
      <div style="font-size:7.5px;color:var(--t2)">${s.sym.join(' · ')}</div>
    </div>`;
  }).join('');
}

function hmDrill(sectorName){
  const s = _sectors.find(x=>x.name===sectorName);
  if(!s) return;
  notify(sectorName + ': ' + s.sym.join(', ') + ' — click stock name to open chart');
}

// ═══════════════════════════════════════════════
// EXPIRY / EARNINGS CALENDAR
// ═══════════════════════════════════════════════
function renderCalendar(){
  const grid = document.getElementById('cal-grid');
  if(!grid) return;
  const today = new Date();
  const events = [
    // Monthly/weekly expiry pattern (nearest upcoming)
    ...(()=>{
      const evts=[];
      for(let i=0;i<30;i++){
        const d=new Date(today); d.setDate(d.getDate()+i);
        if(d.getDay()===4){ // Thursday
          const label = i===0?'TODAY':i<=7?'THIS WEEK':'UPCOMING';
          evts.push({date:d,type:'expiry',label:'Weekly Expiry — NIFTY/BANKNIFTY options',badge:label,color:'var(--am)'});
          if(d.getDate()<=7){ evts.push({date:d,type:'expiry',label:'Monthly Expiry — ALL F&O contracts',badge:'MONTHLY',color:'var(--st)'}); }
          break;
        }
      }
      return evts;
    })(),
    {date:new Date(today.getFullYear(),today.getMonth()+1,1),type:'result',label:'HDFC Bank — Quarterly Results (Est.)',badge:'RESULTS',color:'var(--bl)'},
    {date:new Date(today.getFullYear(),today.getMonth()+1,5),type:'result',label:'TCS — Q4 Earnings Release',badge:'RESULTS',color:'var(--bl)'},
    {date:new Date(today.getFullYear(),today.getMonth()+1,10),type:'macro',label:'RBI MPC Meeting — Rate Decision',badge:'MACRO',color:'var(--go)'},
    {date:new Date(today.getFullYear(),today.getMonth()+1,15),type:'macro',label:'India CPI Inflation Data Release',badge:'MACRO',color:'var(--go)'},
  ].sort((a,b)=>a.date-b.date);

  const fmt=d=>{const opts={weekday:'short',day:'2-digit',month:'short'};return d.toLocaleDateString('en-IN',opts);};
  grid.innerHTML = events.map(ev=>{
    const daysLeft = Math.round((ev.date-today)/(1000*60*60*24));
    const dl = daysLeft===0?'TODAY':daysLeft===1?'TOMORROW':daysLeft+' days';
    return`<div style="display:flex;align-items:center;gap:10px;padding:8px 10px;border-radius:3px;border:1px solid var(--b1);background:var(--s2)">
      <div style="flex-shrink:0;min-width:60px;text-align:center">
        <div style="font-size:8px;color:${ev.color};font-weight:700;letter-spacing:.5px">${ev.badge}</div>
        <div style="font-size:9px;color:var(--t1);margin-top:1px">${fmt(ev.date)}</div>
      </div>
      <div style="flex:1;font-size:10px;color:var(--t0)">${ev.label}</div>
      <div style="flex-shrink:0;font-size:8px;font-weight:700;padding:2px 8px;border-radius:2px;border:1px solid ${ev.color === 'var(--st)' ? 'var(--st-b)' : ev.color === 'var(--am)' ? 'var(--am-b)' : ev.color === 'var(--go)' ? 'var(--go-b)' : 'var(--bl-b)'};color:${ev.color}">${dl}</div>
    </div>`;
  }).join('');
}

// ═══════════════════════════════════════════════
// P&L TRACKER
// ═══════════════════════════════════════════════
let _pnlTrades = JSON.parse(localStorage.getItem('nse_pnl_trades')||'[]');

const _lotSizes = {NIFTY:25,BANKNIFTY:15,ICICIBANK:1375,SBIN:3000,HDFCBANK:550,AXISBANK:1200,
  KOTAKBANK:400,INDUSINDBK:700,LT:150,TCS:300,RELIANCE:500,TATAMOTORS:1800,
  BAJFINANCE:125,TATASTEEL:5500,INFY:600,MARUTI:30,SUNPHARMA:350};

function pnlAdd(){
  const sym=(document.getElementById('pnl-sym').value||'').toUpperCase().trim();
  const dir=document.getElementById('pnl-dir').value;
  const entry=parseFloat(document.getElementById('pnl-entry').value)||0;
  const exit=parseFloat(document.getElementById('pnl-exit').value)||0;
  const qty=parseInt(document.getElementById('pnl-qty').value)||1;
  if(!sym||!entry||!exit){notify('Fill Symbol, Entry and Exit');return;}
  const lotSz = _lotSizes[sym]||1;
  const ptsRaw = dir==='LONG'?exit-entry:entry-exit;
  const pnl = Math.round(ptsRaw * qty * lotSz);
  _pnlTrades.push({sym,dir,entry,exit,qty,lotSz,pnl,ts:Date.now()});
  localStorage.setItem('nse_pnl_trades',JSON.stringify(_pnlTrades));
  document.getElementById('pnl-sym').value='';
  document.getElementById('pnl-entry').value='';
  document.getElementById('pnl-exit').value='';
  document.getElementById('pnl-qty').value='1';
  pnlRender();
  _playAlert(pnl>0?'execute':'warn');
}

function pnlReset(){
  if(!confirm('Reset all P&L trades?')) return;
  _pnlTrades=[];
  localStorage.removeItem('nse_pnl_trades');
  pnlRender();
}

function pnlRender(){
  const log=document.getElementById('pnl-log');
  if(!log) return;
  const total=_pnlTrades.reduce((a,t)=>a+t.pnl,0);
  const wins=_pnlTrades.filter(t=>t.pnl>0);
  const wr=_pnlTrades.length?Math.round(wins.length/_pnlTrades.length*100):0;
  const best=_pnlTrades.length?Math.max(..._pnlTrades.map(t=>t.pnl)):0;
  const worst=_pnlTrades.length?Math.min(..._pnlTrades.map(t=>t.pnl)):0;
  const fmt=n=>(n>=0?'+':'')+n.toLocaleString('en-IN');
  const el=id=>document.getElementById(id);
  if(el('pnl-total')){el('pnl-total').textContent='₹'+fmt(total);el('pnl-total').style.color=total>=0?'var(--go)':'var(--st)';}
  if(el('pnl-wr')){el('pnl-wr').textContent=wr+'%';el('pnl-wr').style.color=wr>=50?'var(--go)':'var(--am)';}
  if(el('pnl-best')) el('pnl-best').textContent='₹'+fmt(best);
  if(el('pnl-worst')){el('pnl-worst').textContent='₹'+fmt(worst);el('pnl-worst').style.color=worst<0?'var(--st)':'var(--go)';}

  if(!_pnlTrades.length){
    log.innerHTML='<div style="color:var(--t2);text-align:center;padding:20px;font-size:10px">No trades logged yet. Add a trade above.</div>';
    return;
  }
  log.innerHTML=[..._pnlTrades].reverse().map((t,i)=>{
    const c=t.pnl>0?'var(--go)':t.pnl<0?'var(--st)':'var(--t2)';
    const pct=t.entry?((t.exit-t.entry)/t.entry*100*(t.dir==='SHORT'?-1:1)).toFixed(2):'0';
    return`<div class="pnl-row" style="display:flex;align-items:center;gap:8px;padding:7px 10px;border-radius:3px;border:1px solid var(--b1);background:var(--s2);margin-bottom:4px">
      <span style="color:var(--t2);font-size:8px;min-width:18px">${_pnlTrades.length-i}</span>
      <span style="font-weight:700;color:var(--t0);min-width:80px" data-val="${t.sym}">${t.sym}</span>
      <span style="font-size:8px;padding:1px 6px;border-radius:2px;border:1px solid ${t.dir==='LONG'?'var(--go-b)':'var(--st-b)'};color:${t.dir==='LONG'?'var(--go)':'var(--st)'}" data-val="${t.dir}">${t.dir}</span>
      <span style="color:var(--t2);font-size:9px" data-val="${t.entry}">${t.entry}→<span style="color:var(--t0)">${t.exit}</span></span>
      <span style="font-size:8px;color:var(--t2)" data-val="${t.qty}">${t.qty} lot${t.qty>1?'s':''} × ${t.lotSz}</span>
      <span style="font-size:10px;font-weight:700;color:${c};margin-left:auto" data-val="${t.pnl}">₹${fmt(t.pnl)}</span>
      <span style="font-size:8px;color:${c}">(${pct}%)</span>
    </div>`;
  }).join('');
}

pnlRender();

// Patch sw() to render heatmap/calendar/pnl on first open
const _swOrig = sw;
window.sw = function(id, btn){
  _swOrig(id, btn);
  if(id==='heatmap') renderHeatmap();
  if(id==='calendar') renderCalendar();
  if(id==='pnl') pnlRender();
};

// ═══════════════════════════════════════════════
// PWA — SERVICE WORKER REGISTRATION
// ═══════════════════════════════════════════════
if('serviceWorker' in navigator){
  window.addEventListener('load',()=>{
    navigator.serviceWorker.register('/sw.js').catch(()=>{});
  });
}

// Show PWA install banner
window.addEventListener('beforeinstallprompt', e => {
  e.preventDefault();
  const banner = document.getElementById('pwa-banner');
  if(banner){
    banner.style.display = 'flex';
    document.getElementById('pwa-install-btn').addEventListener('click', ()=>{
      e.prompt(); banner.style.display='none';
    });
  }
});

// ESC closes modal
document.addEventListener('keydown', e => { if(e.key==='Escape') closeTVChart(); });

// Delegated click handler — any .tv-link click opens chart
document.addEventListener('click', e => {
  const el = e.target.closest('.tv-link');
  if(el && el.dataset.tvsym) {
    e.stopPropagation();
    openTVChart(el.dataset.tvsym, window._histDate || null);
  }
});
