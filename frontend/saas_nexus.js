(function(){
  const NX = {
    boot: null,
    token: localStorage.getItem('nx_token') || '',
    role: localStorage.getItem('nx_role') || '',
    user: null,
    authMode: 'login',
    ready: false,
    loading: false,
    loadingAction: '',
    notice: null,
    dashboard: null,
    payments: [],
    trades: [],
    admin: null,
    workspaceTab: localStorage.getItem('nx_workspace_tab') || '',
    brokerTest: null,
    brokerAssist: null,
    brokerSampleLog: null,
    kitePopupLaunched: false,
    selectedPaymentId: null,
    adminOtpPending: false,
    adminOtpEmail: '',
    adminUserQuery: localStorage.getItem('nx_admin_user_query') || '',
    selectedAdminUserId: localStorage.getItem('nx_admin_selected_user_id') || '',
  };
  const NX_ADMIN_ONLY = String(window.location.pathname || '').toLowerCase() === '/admin';
  if(NX_ADMIN_ONLY) NX.authMode = 'admin';

  function el(id){ return document.getElementById(id); }
  function fmtMoney(v){ const n=Number(v||0); return '\u20B9' + n.toLocaleString('en-IN',{maximumFractionDigits:2}); }
  function fmtNum(v){ return Number(v||0).toLocaleString('en-IN',{maximumFractionDigits:2}); }
  function fmtDate(v){ if(!v) return '—'; try{ return new Date(v).toLocaleString('en-IN',{ timeZone:'Asia/Kolkata' }); }catch(_){ return v; } }
  function escapeHtml(v){ return String(v==null?'':v).replace(/[&<>"']/g, function(ch){ return ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[ch]; }); }
  function checkedAttr(v){ return v ? ' checked' : ''; }
  function parseQueryString(raw){
    const out = {};
    String(raw || '').replace(/^\?/, '').split('&').forEach(function(part){
      if(!part) return;
      const bits = part.split('=');
      const key = decodeURIComponent(bits[0] || '');
      const value = decodeURIComponent((bits.slice(1).join('=') || '').replace(/\+/g, ' '));
      if(key) out[key] = value;
    });
    return out;
  }
  function readKiteOAuthPayloadFromPage(){
    try{
      const q = new URLSearchParams(String(window.location.search || '').replace(/^\?/, ''));
      var rt = String(q.get('request_token') || '').trim();
      var st = String(q.get('status') || '').trim();
      var act = String(q.get('action') || '').trim();
      if(!rt && window.location.hash && String(window.location.hash).indexOf('request_token=') !== -1){
        const hq = new URLSearchParams(String(window.location.hash).replace(/^#/, ''));
        rt = String(hq.get('request_token') || '').trim();
        st = String(hq.get('status') || st).trim();
        act = String(hq.get('action') || act).trim();
      }
      if(!rt) return null;
      return { type:'stockr_kite_oauth', request_token:rt, redirect_url:String(window.location.href || ''), status:st, action:act };
    }catch(_){ return null; }
  }
  function nxStorePendingKiteOAuth(payload){
    try{
      if(payload && payload.request_token){
        localStorage.setItem('nx_kite_oauth_payload', JSON.stringify(payload));
      }
    }catch(_){}
  }
  function paymentMeta(payment){
    const qp = String((payment || {}).qr_payload || '');
    const query = qp.indexOf('?') >= 0 ? qp.split('?')[1] : '';
    const params = parseQueryString(query);
    const upi = params.pa || '';
    const profile = Object.assign({}, currentPaymentProfile(), (((payment || {}).meta || {}).payment_profile || {}));
    return {
      qrPayload: qp,
      upiId: upi,
      amount: params.am || '',
      note: params.tn || ((payment || {}).plan_code || ''),
      qrUrl: qp ? ('https://api.qrserver.com/v1/create-qr-code/?size=220x220&data=' + encodeURIComponent(qp)) : '',
      payeeName: String(profile.payee_name || ''),
      merchantCode: String(profile.merchant_code || ''),
      instructions: String(profile.instructions || ''),
      supportPhone: String(profile.support_phone || ''),
      supportEmail: String(profile.support_email || ''),
      mode: String(profile.mode || ''),
      canAutoConfirm: !!profile.can_auto_confirm,
      upiLink: qp || ('upi://pay?pa=' + encodeURIComponent(String(profile.upi_id || '')))
    };
  }
  function adminSettingValue(key, fallback){
    const items = (((NX.admin || {}).settings) || []);
    for(let i=0;i<items.length;i+=1){
      if(String((items[i] || {}).key || '') === String(key || '')) return (items[i] || {}).value || fallback;
    }
    return fallback;
  }
  function currentPaymentProfile(){
    if(NX.user && NX.user.role === 'ADMIN'){
      return ((NX.admin || {}).payment_profile) || adminSettingValue('payment_profile', ((NX.boot || {}).payment_profile) || {});
    }
    return ((NX.dashboard || {}).payment_profile) || ((NX.boot || {}).payment_profile) || {};
  }
  function planRank(code){
    const order = ['STARTER','PRO','DESK'];
    const idx = order.indexOf(String(code || '').toUpperCase());
    return idx >= 0 ? idx : -1;
  }
  function backendBase(){
    return String((window._backendBase || '')).replace(/\/$/, '');
  }
  function resolveUrl(url){
    if(/^https?:\/\//i.test(String(url||''))) return url;
    const base = backendBase();
    return base ? (base + url) : url;
  }
  function nxKiteOAuthReturnAbs(){
    const ku = String((((NX.boot || {}).kite_oauth_return_url) || '')).trim().replace(/\/$/, '');
    if(ku) return ku;
    const base = backendBase();
    if(base){
      try{
        const raw = base.indexOf('://') === -1 ? ('http://' + String(base).replace(/^\/*/, '')) : String(base);
        return new URL(raw).origin + '/kite-oauth-return';
      }catch(_){ /* fall through */ }
    }
    return String(window.location.origin || '') + '/kite-oauth-return';
  }
  function nxKiteOAuthReturnAlt(){
    try{
      const u = new URL(nxKiteOAuthReturnAbs());
      if(u.hostname === 'localhost'){
        u.hostname = '127.0.0.1';
        return u.toString();
      }
      if(u.hostname === '127.0.0.1'){
        u.hostname = 'localhost';
        return u.toString();
      }
    }catch(_){ /* ignore */ }
    return '';
  }
  function nxCloseKitePopupRef(){
    nxKiteStopPopupWatcher();
    try{
      if(window._nxKitePopupRef && !window._nxKitePopupRef.closed){
        window._nxKitePopupRef.close();
      }
    }catch(_){}
    window._nxKitePopupRef = null;
    NX.kitePopupLaunched = false;
  }
  window.nxCopyKiteOAuthRedirect = function(){
    const u = nxKiteOAuthReturnAbs();
    if(navigator.clipboard && navigator.clipboard.writeText){
      navigator.clipboard.writeText(u).then(function(){ toast('Redirect URL copied'); }).catch(function(){ window.prompt('Copy this Kite redirect URL', u); });
    }else{
      window.prompt('Copy this Kite redirect URL', u);
    }
  };
  window.nxCopyKiteOAuthRedirectAlt = function(){
    const u = nxKiteOAuthReturnAlt();
    if(!u){ window.nxCopyKiteOAuthRedirect(); return; }
    if(navigator.clipboard && navigator.clipboard.writeText){
      navigator.clipboard.writeText(u).then(function(){ toast('Alternate redirect URL copied'); }).catch(function(){ window.prompt('Copy alternate Kite redirect URL', u); });
    }else{
      window.prompt('Copy alternate Kite redirect URL', u);
    }
  };
  function normalizeEmail(v){
    let email = String(v || '').trim().toLowerCase();
    if(email.endsWith('@stokr.in')) email = email.replace(/@stokr\.in$/, '@stockr.in');
    return email;
  }
  function defaultAdminEmail(){
    return normalizeEmail((((NX.boot || {}).admin || {}).email) || 'admin@stockr.in');
  }
  function gmailStatus(){
    const g = (((NX.boot || {}).gmail) || {});
    return {
      ready: !!((NX.boot || {}).gmail_ready),
      mode: String(g.mode || (g.ready ? 'smtp' : 'none')).toLowerCase(),
      oauth_connected: !!g.oauth_connected,
      oauth_email: String(g.oauth_email || ''),
      oauth_error: String(g.oauth_error || ''),
      smtp_from: String(g.smtp_from || ''),
    };
  }
  function workspaceTabs(){
    if(!NX.user) return [];
    if(String((NX.user.role || '')).toUpperCase() === 'ADMIN'){
      return [
        { id:'overview', label:'Overview', kicker:'Desk', desc:'Metrics, brand pulse, and quick status' },
        { id:'users', label:'Users', kicker:'Accounts', desc:'Provision users and manage wallets' },
        { id:'routing', label:'Routing', kicker:'Signals', desc:'Strategy entitlement and engine delivery' },
        { id:'payments', label:'Payments', kicker:'Rail', desc:'Coupons, UPI setup, and approvals' },
        { id:'brand', label:'Brand', kicker:'Mailer', desc:'Identity, Gmail, and support profile' },
        { id:'signals', label:'Feed', kicker:'Events', desc:'Recently routed user alerts' }
      ];
    }
    return [
      { id:'overview', label:'Overview', kicker:'Desk', desc:'Account, wallet, and readiness snapshot' },
      { id:'execution', label:'Execution', kicker:'Zerodha', desc:'Broker connect, token actions, and routing' },
      { id:'signals', label:'Signals', kicker:'Inbox', desc:'Signal stream and strategy switches' },
      { id:'wallet', label:'Wallet', kicker:'Credits', desc:'Ledger, coupon, and balance state' },
      { id:'payments', label:'Payments', kicker:'Plans', desc:'Plan upgrades, QR, and approvals' },
      { id:'journal', label:'Journal', kicker:'Performance', desc:'Trade curve and quick trade logging' },
      { id:'settings', label:'Settings', kicker:'Alerts', desc:'Email, Telegram, WhatsApp, and limits' }
    ];
  }
  function workspaceTabStoreKey(){
    const role = String((((NX.user || {}).role) || NX.role || 'guest')).toLowerCase();
    return 'nx_workspace_tab_' + role;
  }
  function ensureWorkspaceTab(){
    const tabs = workspaceTabs();
    if(!tabs.length) return '';
    const active = String(NX.workspaceTab || localStorage.getItem(workspaceTabStoreKey()) || '').trim().toLowerCase();
    const valid = tabs.some(function(tab){ return tab.id === active; });
    NX.workspaceTab = valid ? active : tabs[0].id;
    localStorage.setItem(workspaceTabStoreKey(), NX.workspaceTab);
    return NX.workspaceTab;
  }
  function currentWorkspaceTab(){
    return ensureWorkspaceTab();
  }
  function workspaceMeta(tabId){
    const id = String(tabId || currentWorkspaceTab() || '').toLowerCase();
    const tabs = workspaceTabs();
    for(let i=0;i<tabs.length;i+=1){
      if(tabs[i].id === id) return tabs[i];
    }
    return tabs[0] || null;
  }
  function setNotice(type, message){
    NX.notice = message ? { type: type || 'info', message: String(message) } : null;
  }
  function clearNotice(){ NX.notice = null; }
  function toast(msg){
    let box = el('nx-toast');
    if(!box){ box = document.createElement('div'); box.id='nx-toast'; box.className='nx-toast'; document.body.appendChild(box); }
    box.textContent = msg;
    box.classList.add('on');
    clearTimeout(window._nxToastTimer);
    window._nxToastTimer = setTimeout(function(){ box.classList.remove('on'); }, 2400);
  }
  async function nxApi(url, opts){
    opts = opts || {};
    const headers = Object.assign({'Content-Type':'application/json'}, opts.headers || {});
    if(NX.token) headers.Authorization = 'Bearer ' + NX.token;
    const res = await fetch(resolveUrl(url), Object.assign({}, opts, { headers }));
    const data = await res.json().catch(function(){ return {}; });
    if(!res.ok){ throw new Error(data.detail || data.message || data.msg || ('Request failed: ' + res.status)); }
    return data;
  }
  function remember(token, role){
    NX.token = token || '';
    NX.role = role || '';
    if(NX.token) localStorage.setItem('nx_token', NX.token); else localStorage.removeItem('nx_token');
    if(NX.role) localStorage.setItem('nx_role', NX.role); else localStorage.removeItem('nx_role');
  }
  function nxKiteStopPopupWatcher(){
    if(window._nxKitePopupPoll){
      clearInterval(window._nxKitePopupPoll);
      window._nxKitePopupPoll = null;
    }
  }
  function nxKitePopupFeatures(){
    var w = 520, h = 840;
    var sl = window.screenLeft !== undefined ? window.screenLeft : window.screenX;
    var st = window.screenTop !== undefined ? window.screenTop : window.screenY;
    var vw = window.innerWidth || document.documentElement.clientWidth || screen.width;
    var vh = window.innerHeight || document.documentElement.clientHeight || screen.height;
    var left = Math.max(0, sl + (vw - w) / 2);
    var top = Math.max(0, st + (vh - h) / 2);
    return 'popup=yes,width='+w+',height='+h+',left='+Math.round(left)+',top='+Math.round(top)+',scrollbars=yes,resizable=yes,toolbar=no,menubar=no,status=no';
  }
  function nxKiteStartPopupWatcher(){
    nxKiteStopPopupWatcher();
    window._nxKitePopupPoll = setInterval(function(){
      var p = window._nxKitePopupRef;
      if(!p || p.closed){
        nxKiteStopPopupWatcher();
        if(NX.kitePopupLaunched){
          NX.kitePopupLaunched = false;
          safeRender();
        }
      }
    }, 700);
  }
  function logout(){
    if(window._nxKiteInteractiveTimer){ clearTimeout(window._nxKiteInteractiveTimer); window._nxKiteInteractiveTimer = null; }
    nxCloseKitePopupRef();
    remember('', '');
    NX.user=null; NX.dashboard=null; NX.admin=null; NX.payments=[]; NX.trades=[]; NX.loadingAction=''; NX.brokerTest=null; NX.brokerAssist=null; NX.selectedPaymentId=null; NX.ready=false;
    window._nxBootHydrateDone = false;
    clearNotice(); safeRender();
  }

  async function ensureBoot(){
    if(NX.boot) return NX.boot;
    NX.boot = nxApi('/api/saas/bootstrap').then(function(data){ NX.boot = data; return data; }).catch(function(err){ NX.boot = null; throw err; });
    return NX.boot;
  }

  async function refreshSession(){
    if(!NX.token) return null;
    try{
      const data = await nxApi('/api/auth/me', { method:'GET' });
      NX.user = data.user;
      NX.role = data.user.role || NX.role;
      remember(NX.token, NX.role);
      return data.user;
    }catch(err){
      logout();
      return null;
    }
  }

  async function loadUserData(){
    const out = await Promise.all([
      nxApi('/api/user/dashboard', { method:'GET' }),
      nxApi('/api/user/payments', { method:'GET' }),
      nxApi('/api/user/trades', { method:'GET' })
    ]);
    NX.dashboard = out[0];
    NX.payments = out[1].items || [];
    NX.trades = out[2].items || [];
    if(!NX.selectedPaymentId && NX.payments.length) NX.selectedPaymentId = NX.payments[0].id;
    if(NX.selectedPaymentId && !NX.payments.some(function(p){ return p.id === NX.selectedPaymentId; })){
      NX.selectedPaymentId = NX.payments.length ? NX.payments[0].id : null;
    }
    if(!NX.brokerTest && NX.dashboard && NX.dashboard.broker){
      const b = NX.dashboard.broker;
      if(b.status === 'CONNECTED' || b.status === 'READY'){
        NX.brokerTest = {
          type: 'success',
          message: 'Broker connected',
          detail: [((b.profile||{}).name || ''), ((b.profile||{}).user_id ? ('ID ' + (b.profile||{}).user_id) : ''), (b.broker_name || '')].filter(Boolean).join(' · ')
        };
      }
    }
  }

  async function loadAdminData(){
    const out = await Promise.all([
      nxApi('/api/admin/dashboard', { method:'GET' }),
      nxApi('/api/admin/coupons', { method:'GET' }),
      nxApi('/api/admin/settings', { method:'GET' }),
      nxApi('/api/admin/payments', { method:'GET' }),
      nxApi('/api/admin/users', { method:'GET' })
    ]);
    NX.admin = Object.assign({}, out[0], { coupons: out[1].items || [], settings: out[2].items || [], paymentsFull: out[3].items || [], payment_profile: out[0].payment_profile || out[3].payment_profile || null, users: out[4].items || [] });
    if(NX.selectedAdminUserId && !(NX.admin.users || []).some(function(u){ return String(u.id) === String(NX.selectedAdminUserId); })){
      NX.selectedAdminUserId = '';
      localStorage.removeItem('nx_admin_selected_user_id');
    }
    if(!NX.selectedAdminUserId && (NX.admin.users || []).length){
      NX.selectedAdminUserId = String(NX.admin.users[0].id);
      localStorage.setItem('nx_admin_selected_user_id', NX.selectedAdminUserId);
    }
  }

  function currentAdminManagedUser(){
    const wanted = String(NX.selectedAdminUserId || '');
    const users = ((NX.admin || {}).users) || [];
    for(let i=0;i<users.length;i+=1){
      if(String(users[i].id) === wanted) return users[i];
    }
    return users[0] || null;
  }

  async function hydrate(){
    NX.loading = true;
    safeRender();
    try{
      await ensureBoot();
      if(await refreshSession()){
        if(NX_ADMIN_ONLY && String(((NX.user||{}).role)||'').toUpperCase() !== 'ADMIN'){
          logout();
          setNotice('info', 'Admin route requires admin sign-in with OTP.');
          return;
        }
        if((NX.user && NX.user.role) === 'ADMIN') await loadAdminData();
        else await loadUserData();
      }
    }catch(err){
      setNotice('error', err.message || 'Unable to load control hub');
      toast(err.message || 'Unable to load control hub');
    }finally{
      NX.loading = false;
      NX.ready = true;
      safeRender();
    }
  }

  function heroStats(){
    if(NX.user && NX.user.role === 'ADMIN' && NX.admin){
      const m = NX.admin.metrics || {};
      return [
        ['Users', fmtNum(m.total_users || 0), 'SaaS accounts'],
        ['Active', fmtNum(m.active_users || 0), 'Ready to receive'],
        ['Revenue', fmtMoney(m.revenue || 0), 'Paid orders'],
        ['Signals', fmtNum(m.signals_total || 0), 'Delivered inbox events']
      ];
    }
    const d = NX.dashboard || {};
    const u = (d.user || NX.user || {});
    const m = d.metrics || {};
    return [
      ['Wallet', fmtMoney((((u.wallet||{}).balance)||0)), (u.wallet||{}).type || 'Wallet'],
      ['Signals', fmtNum(m.signals_unread || 0), 'Unread in inbox'],
      ['Today', fmtMoney(m.today_pnl || 0), fmtNum(m.today_trades || 0) + ' trades'],
      ['Win Rate', fmtNum((((d.performance||{}).summary||{}).win_rate)||0) + '%', 'Closed trade quality']
    ];
  }

  function renderHero(){
    const stats = heroStats().map(function(item){ return '<div class="nx-stat"><div class="nx-stat-label">'+item[0]+'</div><div class="nx-stat-value">'+item[1]+'</div><div class="nx-stat-note">'+item[2]+'</div></div>'; }).join('');
    const boot = NX.boot || { brand:'STOCKR.IN', gmail_ready:false, admin:{ email:'admin@stockr.in' } };
    const brand = boot.brand || 'STOCKR.IN';
    const g = gmailStatus();
    const gmailBadge = '<span class="nx-badge ' + (boot.gmail_ready ? 'good' : 'warn') + '">' + (boot.gmail_ready ? 'Gmail Live' : 'Gmail Pending') + '</span>';
    const who = NX.user ? ('Signed in as ' + escapeHtml(NX.user.full_name || NX.user.email || 'Trader')) : 'Unified login and workspace hub for users, admins, wallet, strategy access, and mail notifications.';
    const sub = NX.user
      ? ('Role: <b>'+escapeHtml(NX.user.role)+'</b> · Status: <b>'+escapeHtml(NX.user.status || 'ACTIVE')+'</b> · Mailer: <b>'+(boot.gmail_ready ? 'Connected' : 'Pending')+'</b> · Mode: <b>'+escapeHtml((g.mode || 'none').toUpperCase())+'</b>')
      : ('Bootstrap admin: <b>'+escapeHtml((boot.admin||{}).email || 'admin@stockr.in')+'</b> · User/Admin access remains separate from your live trading tabs.');
    return '<div class="nx-hero">'
      + '<div class="nx-kicker">'+escapeHtml(brand)+' ACCESS HUB</div>'
      + '<div class="nx-hero-top"><div><div class="nx-title">'+escapeHtml(brand)+' Control Hub</div><div class="nx-sub">'+who+'<br>'+sub+'</div><div class="nx-pill-row" style="margin-top:14px"><span class="nx-pill">Portal</span><span class="nx-pill">Brand '+escapeHtml(brand)+'</span>'+gmailBadge+'</div></div>'
      + '<div class="nx-actions">'
      + (NX.user ? '<button class="nx-btn nx-btn-ghost" onclick="nxRefresh()">Refresh</button><button class="nx-btn nx-btn-gold" onclick="nxLogout()">Logout</button>' : (NX_ADMIN_ONLY ? '<button class="nx-btn nx-btn-gold" onclick="nxSwitchAuth(\'admin\')">Open Admin Access</button>' : '<button class="nx-btn nx-btn-primary" onclick="nxSwitchAuth(\'signup\')">Open User Access</button><button class="nx-btn nx-btn-ghost" onclick="window.location.href=\'/admin\'">Open Admin Access</button>'))
      + '</div></div>'
      + '<div class="nx-hero-stats">'+stats+'</div></div>';
  }

  function renderAuthCard(){
    const boot = NX.boot || { plans: [], coupon_code:'WELCOME500' };
    if(NX.user){
      const u = NX.user;
      return '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Session</div><div class="nx-card-sub">JWT auth with separated user/admin access</div></div><span class="nx-badge cool">'+escapeHtml(u.role||'USER')+'</span></div><div class="nx-card-body"><div class="nx-list"><div class="nx-item"><div class="nx-item-title">'+escapeHtml(u.full_name || u.email)+'</div><div class="nx-item-sub">'+escapeHtml(u.email)+'<br>Created '+escapeHtml(fmtDate(u.created_at))+'</div></div><div class="nx-item"><div class="nx-item-title">Default onboarding coupon</div><div class="nx-item-sub">Use <b>'+escapeHtml(boot.coupon_code || 'WELCOME500')+'</b> for instant test credits if you create fresh users.</div></div></div></div></div>';
    }
    if(NX_ADMIN_ONLY) NX.authMode = 'admin';
    function modeBtn(id,label){ return '<button class="nx-tab-btn'+(NX.authMode===id?' nx-btn-primary':'')+'" onclick="nxSwitchAuth(\''+id+'\')">'+label+'</button>'; }
    const gmailReady = !!boot.gmail_ready;
    const statusHtml = NX.notice && NX.notice.message ? '<div class="nx-status '+escapeHtml(NX.notice.type || 'info')+'">'+escapeHtml(NX.notice.message)+'</div>' : '';
    const loginBusy = NX.loadingAction === 'login';
    const signupBusy = NX.loadingAction === 'signup';
    const adminBusy = NX.loadingAction === 'admin-login';
    const tabsHtml = NX_ADMIN_ONLY ? modeBtn('admin','Admin Login') : (modeBtn('login','User Login')+modeBtn('signup','Create User')+modeBtn('admin','Admin Login'));
    const adminSub = 'Admin login is password based.';
    return '<div class="nx-card nx-auth-card nx-auth-card-wide"><div class="nx-card-head"><div><div class="nx-card-title">Login Portal</div><div class="nx-card-sub">Fast access for users and admins.</div></div><span class="nx-badge '+(gmailReady?'good':'warn')+'">'+(gmailReady?'Gmail Ready':'Gmail Pending')+'</span></div><div class="nx-card-body">'
      + '<div class="nx-auth-wrap nx-auth-wrap-single"><div class="nx-auth-left"><div class="nx-tabs">'+tabsHtml+'</div>'+statusHtml
      + '<div class="nx-auth-mode '+(NX.authMode==='login'?'on':'')+'">'
      + '<div class="nx-form-grid"><label class="nx-form-label">Email<input id="nx-login-email" class="nx-input" placeholder="trader@email.com"></label><label class="nx-form-label">Password<input id="nx-login-password" type="password" class="nx-input" placeholder="password"></label></div>'
      + '<div class="nx-actions" style="margin-top:12px"><button class="nx-btn nx-btn-primary nx-btn-wide" onclick="nxLogin()" '+(loginBusy?'disabled':'')+'>'+(loginBusy?'Signing in...':'Enter User Desk')+'</button></div></div>'
      + '<div class="nx-auth-mode '+(NX.authMode==='signup'?'on':'')+'">'
      + '<div class="nx-form-grid"><label class="nx-form-label">Full Name<input id="nx-signup-name" class="nx-input" placeholder="Trader name"></label><label class="nx-form-label">Email<input id="nx-signup-email" class="nx-input" placeholder="new@desk.com"></label></div>'
      + '<div class="nx-form-grid"><label class="nx-form-label">Password<input id="nx-signup-password" type="password" class="nx-input" placeholder="min 6 chars"></label><label class="nx-form-label">Onboarding<input class="nx-input" value="Coupon '+escapeHtml(boot.coupon_code || 'WELCOME500')+' auto-applies" disabled></label></div>'
      + '<div class="nx-form-grid"><label class="nx-form-label">WhatsApp<input id="nx-signup-whatsapp" class="nx-input" placeholder="+91 9876543210"></label><label class="nx-form-label">Telegram ID<input id="nx-signup-telegram" class="nx-input" placeholder="123456789"></label></div>'
      + '<div class="nx-toggle-grid" style="margin-top:12px"><label class="nx-check"><input id="nx-signup-notify-email" type="checkbox" checked><span>Email alerts</span></label><label class="nx-check"><input id="nx-signup-notify-telegram" type="checkbox" checked><span>Telegram alerts</span></label><label class="nx-check"><input id="nx-signup-notify-whatsapp" type="checkbox"><span>WhatsApp alerts</span></label><label class="nx-check"><input id="nx-signup-token-reminder" type="checkbox" checked><span>Token reminder</span></label></div>'
      + '<div class="nx-actions" style="margin-top:12px"><button class="nx-btn nx-btn-primary nx-btn-wide" onclick="nxSignup()" '+(signupBusy?'disabled':'')+'>'+(signupBusy?'Creating...':'Create User Account')+'</button></div></div>'
      + '<div class="nx-auth-mode '+(NX.authMode==='admin'?'on':'')+'">'
      + '<div class="nx-form-grid"><label class="nx-form-label">Admin Email<input id="nx-admin-email" class="nx-input" placeholder="'+escapeHtml((boot.admin||{}).email || 'admin@stockr.in')+'"></label><label class="nx-form-label">Password<input id="nx-admin-password" type="password" class="nx-input" placeholder="admin password"></label></div>'
      + '<div class="nx-inline-note" style="margin-top:10px">'+adminSub+' · Admin: <b>'+escapeHtml(defaultAdminEmail())+'</b>.</div>'
      + '<div class="nx-actions" style="margin-top:12px"><button class="nx-btn nx-btn-gold nx-btn-wide" onclick="nxAdminLogin()" '+(adminBusy?'disabled':'')+'>'+(adminBusy?'Signing in...':'Enter Admin Desk')+'</button></div></div></div></div></div></div>';
  }

  function renderPublicSideCards(){
    const boot = NX.boot || {};
    const gmailReady = !!boot.gmail_ready;
    return '<div class="nx-public-grid">'
      + '<div class="nx-card nx-launch-card"><div class="nx-card-head"><div><div class="nx-card-title">Portal Highlights</div><div class="nx-card-sub">Quick overview of what users and admins can do right after login.</div></div></div><div class="nx-card-body"><div class="nx-list"><div class="nx-item"><div class="nx-item-title">User workspace</div><div class="nx-item-sub">Signup/login, strategy toggles, wallet credits, coupon redemption, payment orders, and trade journal.</div></div><div class="nx-item"><div class="nx-item-title">Admin workspace</div><div class="nx-item-sub">Dedicated OTP-secured login on <b>/admin</b>, user creation, strategy activation, coupon creation, payment confirmation, and platform metrics.</div></div><div class="nx-item"><div class="nx-item-title">Signal routing seam</div><div class="nx-item-sub">SPIKE and SWING route from the signal engine snapshot. INDEX routes from the index radar notifier. Existing tabs stay untouched.</div></div></div><div class="nx-cta-strip"><button class="nx-btn nx-btn-primary" onclick="nxSwitchAuth(\'signup\')">Create First User</button><button class="nx-btn nx-btn-ghost" onclick="window.location.href=\'/admin\'">Open Admin Access</button></div></div></div>'
      + '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Gmail Integration</div><div class="nx-card-sub">Email channel health for onboarding and account lifecycle notifications.</div></div><span class="nx-badge '+(gmailReady?'good':'warn')+'">'+(gmailReady?'Connected':'Pending')+'</span></div><div class="nx-card-body"><div class="nx-glow-card"><div class="nx-glow-kicker">Mailer status</div><div class="nx-glow-title">'+(gmailReady?'Delivery active':'Awaiting credentials')+'</div><div class="nx-item-sub">Set <b>GMAIL_USERNAME</b> and <b>GMAIL_APP_PASSWORD</b> in <b>backend/.env</b>. Once connected, login/signup/payment emails can be sent from your brand mailbox.</div><div class="nx-mail-health '+(gmailReady?'good':'warn')+'">'+(gmailReady?'Mailbox is ready to send transactional emails.':'Add Gmail sender credentials to enable email delivery.')+'</div></div></div></div>'
      + '</div>';
  }

  function renderWorkspaceShell(opts){
    const tabs = opts.tabs || [];
    const active = currentWorkspaceTab();
    const meta = workspaceMeta(active) || {};
    const railCards = (opts.railCards || []).join('');
    const tabHtml = tabs.map(function(tab){
      const on = tab.id === active;
      return '<button class="nx-workspace-tab'+(on?' on':'')+'" onclick="nxSetWorkspaceTab(\''+tab.id+'\')"><span class="nx-workspace-tab-kicker">'+escapeHtml(tab.kicker || '')+'</span><span class="nx-workspace-tab-label">'+escapeHtml(tab.label || tab.id)+'</span><span class="nx-workspace-tab-desc">'+escapeHtml(tab.desc || '')+'</span></button>';
    }).join('');
    return ''
      + '<div class="nx-workspace-shell">'
      + '<aside class="nx-workspace-rail">'
      + '<div class="nx-workspace-rail-card"><div class="nx-workspace-rail-kicker">'+escapeHtml(opts.railKicker || 'Workspace')+'</div><div class="nx-workspace-rail-title">'+escapeHtml(opts.railTitle || 'Nexus Desk')+'</div><div class="nx-workspace-rail-sub">'+escapeHtml(opts.railSub || 'Choose a tab to focus one area at a time.')+'</div></div>'
      + '<div class="nx-workspace-tab-stack">'+tabHtml+'</div>'
      + railCards
      + '</aside>'
      + '<section class="nx-workspace-main">'
      + '<div class="nx-workspace-pane-head"><div><div class="nx-workspace-pane-kicker">'+escapeHtml(meta.kicker || opts.railKicker || 'Workspace')+'</div><div class="nx-workspace-pane-title">'+escapeHtml(meta.label || opts.railTitle || 'Desk')+'</div><div class="nx-workspace-pane-sub">'+escapeHtml(meta.desc || opts.railSub || '')+'</div></div>'+(opts.headRight || '')+'</div>'
      + '<div class="nx-workspace-pane-body">'+(opts.body || '')+'</div>'
      + '</section>'
      + '</div>';
  }

  /** Broker dropdown (unsaved) vs last-saved dashboard broker — drives Zerodha tools visibility. */
  function nxEffectiveBrokerCode(broker){
    var b = broker || {};
    try{
      var bcEl = document.getElementById('nx-broker-code');
      if(bcEl && bcEl.value){
        return String(bcEl.value || 'PAPER').toUpperCase();
      }
    }catch(_){ }
    return String(b.broker_code || 'PAPER').toUpperCase();
  }

  function renderSignalItems(items){
    if(!items || !items.length) return '<div class="nx-empty">No routed signals yet. Once live engines emit SPIKE, INDEX, or SWING ideas, they will appear here automatically.</div>';
    return '<div class="nx-list">' + items.map(function(item){
      return '<div class="nx-item"><div class="nx-item-top"><div><div class="nx-item-title">'+escapeHtml(item.headline || item.strategy_code)+'</div><div class="nx-item-sub">'+escapeHtml(item.excerpt || 'Signal event')+'</div></div><div style="display:flex;flex-direction:column;align-items:flex-end;gap:8px"><span class="nx-badge '+(item.read?'cool':'warn')+'">'+escapeHtml(item.strategy_code)+' · '+fmtNum(item.confidence)+'%</span>'+(item.read?'':'<button class="nx-mini-btn" onclick="nxMarkSignalRead('+item.id+')">Mark Read</button>')+'</div></div><div class="nx-inline-note" style="margin-top:10px">'+escapeHtml(fmtDate(item.created_at))+'</div></div>';
    }).join('') + '</div>';
  }

  function renderCurve(curve){
    if(!curve || !curve.length) return '<div class="nx-empty">Trade curve will start plotting as soon as journal entries are closed.</div>';
    const max = Math.max.apply(null, curve.map(function(x){ return Math.abs(Number(x.pnl||0)); }).concat([1]));
    return '<div class="nx-curve">' + curve.map(function(x){
      const v = Number(x.pnl||0), h = Math.max(18, Math.round(Math.abs(v)/max*110));
      return '<div class="nx-bar'+(v<0?' neg':'')+'" style="height:'+h+'px"><span>'+escapeHtml(String(x.date||'').slice(5))+'</span></div>';
    }).join('') + '</div>';
  }

  function renderUserNotifyCard(contacts, notifications){
    return '<div class="nx-card nx-notify-card"><div class="nx-card-head"><div><div class="nx-card-title">Alerts &amp; channels</div><div class="nx-card-sub">Signal delivery, risk nudges, and weekday token reminders.</div></div></div><div class="nx-card-body">'
      + '<div class="nx-form-grid"><label class="nx-form-label">WhatsApp<input id="nx-user-whatsapp" class="nx-input" placeholder="+91 9876543210" value="'+escapeHtml(contacts.whatsapp_phone || '')+'"></label><label class="nx-form-label">Telegram ID<input id="nx-user-telegram" class="nx-input" placeholder="123456789" value="'+escapeHtml(contacts.telegram_chat_id || '')+'"></label></div>'
      + '<div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">WhatsApp API Key<input id="nx-user-whatsapp-apikey" class="nx-input" placeholder="'+escapeHtml(contacts.whatsapp_apikey_masked || 'CallMeBot API key')+'" value=""></label><label class="nx-form-label">Channel state<input class="nx-input" disabled value="'+escapeHtml((notifications.email ? 'Email ' : '') + (notifications.telegram ? 'Telegram ' : '') + (notifications.whatsapp ? 'WhatsApp ' : '') || 'All disabled')+'"></label></div>'
      + '<div class="nx-toggle-grid" style="margin-top:14px"><label class="nx-check"><input id="nx-user-notify-email" type="checkbox"'+checkedAttr(!!notifications.email)+'><span>Email</span></label><label class="nx-check"><input id="nx-user-notify-telegram" type="checkbox"'+checkedAttr(!!notifications.telegram)+'><span>Telegram</span></label><label class="nx-check"><input id="nx-user-notify-whatsapp" type="checkbox"'+checkedAttr(!!notifications.whatsapp)+'><span>WhatsApp</span></label><label class="nx-check"><input id="nx-user-token-reminder" type="checkbox"'+checkedAttr(!!notifications.token_reminder)+'><span>Token reminder</span></label></div>'
      + '<div class="nx-actions" style="margin-top:12px"><button class="nx-btn nx-btn-primary" onclick="nxSaveNotifications()">Save notifications</button></div>'
      + '<div class="nx-inline-note" style="margin-top:10px">Last reminder: '+escapeHtml(fmtDate(notifications.last_token_reminder_at))+'</div></div></div>';
  }

  function nxBrokerOrderStatusLabel(raw){
    var s = String(raw || '');
    if(s === 'SIMULATED_CANCELLED') return 'Paper test OK (legacy)';
    if(s === 'SIMULATED_OK') return 'Paper test OK';
    if(s === 'SIMULATED') return 'Simulated';
    return s || 'PENDING';
  }
  function nxBrokerOrderStatusBadgeClass(raw){
    var s = String(raw || '');
    if(/FAILED|ERROR/i.test(s)) return 'bad';
    if(/SKIPPED/i.test(s)) return 'warn';
    return 'good';
  }

  function renderKiteExecutionHub(user, broker, brokerOptions, brokerProfileBits, brokerTestHtml, executionOrders, effectiveBrokerCode){
    try{
    user = user || {};
    broker = broker || {};
    const profileBits = brokerProfileBits || [];
    const code = String(effectiveBrokerCode || broker.broker_code || 'PAPER').toUpperCase();
    const paper = code === 'PAPER';
    const liveLocked = code === 'PAPER';
    const paperRouteOn = !!broker.paper_mode;
    const effectiveLive = !!broker.effective_live;
    const sessOk = broker.status === 'CONNECTED' || broker.status === 'READY';
    const engineReady = !!broker.enabled && sessOk && !paper;
    const assist = NX.brokerAssist || null;
    const statusClass = sessOk ? 'good' : (broker.status === 'ERROR' ? 'bad' : 'warn');
    const modeBadge = effectiveLive ? 'bad' : 'cool';
    const modeLabel = effectiveLive ? 'Live execution' : 'Paper / safe';
    const profileLine = profileBits.length ? profileBits.join(' · ') : 'Not verified yet';
    const tokenClass = assist ? (assist.type === 'success' ? 'success' : (assist.type === 'error' ? 'error' : (assist.type === 'info' ? 'info' : 'cool'))) : 'warn';
    const tokenTitle = assist ? escapeHtml(assist.title || 'Shared session checked') : 'Shared session not checked yet';
    const tokenMessage = assist ? escapeHtml(assist.message || '') : 'Fastest path: use the server session if your Zerodha keys and token already live in backend/.env.';
    const tokenDetail = assist && assist.detail ? '<div class="nx-inline-note" style="margin-top:8px">'+escapeHtml(assist.detail)+'</div>' : '';
    const waitBanner = assist && assist.waitHint ? '<div class="nx-z-wait-banner">'+escapeHtml(assist.waitHint)+'</div>' : '';
    const importing = NX.loadingAction === 'broker-import-env';
    const refreshing = NX.loadingAction === 'broker-refresh-env';
    const checking = NX.loadingAction === 'broker-token-status';
    const interactiveStarting = NX.loadingAction === 'broker-interactive-start';
    const fireModeBusy = NX.loadingAction === 'broker-fire-mode';
    const saving = NX.loadingAction === 'broker-connect' || fireModeBusy;
    const testing = NX.loadingAction === 'broker-test';
    const sampling = NX.loadingAction === 'broker-sample';
    const sampleLog = NX.brokerSampleLog || null;
    const liveFireSelected = !paper && effectiveLive;
    const paperFireSelected = !paper && !effectiveLive;
    const fireModeBar = paper
      ? ''
      : (
        '<div class="nx-fire-mode-bar">'
        + '<div class="nx-fire-mode-title">Order firing</div>'
        + '<div class="nx-fire-mode-opts" role="radiogroup" aria-label="Paper or live exchange orders">'
        + '<label class="nx-fire-mode-opt'+(paperFireSelected ? ' nx-fire-mode-opt-on' : '')+'">'
        + '<input type="radio" name="nx-desk-fire-mode" value="paper" '+(paperFireSelected ? 'checked ' : '')+(saving ? 'disabled ' : '')+'onchange="nxSetTradingFireMode(\'paper\')">'
        + '<span class="nx-fire-mode-copy"><span class="nx-fire-mode-name">Paper</span><span class="nx-fire-mode-hint">Simulated only — nothing sent to NSE</span></span>'
        + '</label>'
        + '<label class="nx-fire-mode-opt'+(liveFireSelected ? ' nx-fire-mode-opt-on' : '')+'">'
        + '<input type="radio" name="nx-desk-fire-mode" value="live" '+(liveFireSelected ? 'checked ' : '')+(saving ? 'disabled ' : '')+'onchange="nxSetTradingFireMode(\'live\')">'
        + '<span class="nx-fire-mode-copy"><span class="nx-fire-mode-name">Live</span><span class="nx-fire-mode-hint">Real Kite orders to the exchange when you run sample or auto-route</span></span>'
        + '</label>'
        + '</div>'
        + (!sessOk
          ? '<div class="nx-inline-note" style="margin-top:10px">Finish Kite login first; live orders only work with a connected session.</div>'
          : (liveFireSelected
            ? '<div class="nx-inline-note nx-fire-mode-live-warn" style="margin-top:10px">Live is selected — use Sample order and auto-routing carefully during market hours.</div>'
            : ''))
        + '</div>'
      );
    const kiteBrand = '<div class="nx-kite-brand"><span class="nx-kite-z">Z</span><span>Zerodha Kite</span></div>';
    const allowServerImport = !!((NX.boot || {}).allow_user_server_session_import);
    const altRedirect = nxKiteOAuthReturnAlt();
    const primaryRedirect = escapeHtml(nxKiteOAuthReturnAbs());
    const altRedirectHtml = altRedirect ? ('<div class="nx-inline-note" style="margin-top:6px">Alternate: <code style="word-break:break-all">' + escapeHtml(altRedirect) + '</code></div>') : '';
    const loginDone = !paper && sessOk;
    const simpleLoginCard = paper ? (
      '<div class="nx-z-paper-callout"><div class="nx-z-paper-title">Select Zerodha first</div><div class="nx-z-paper-sub">Change the Broker dropdown from <b>Paper</b> to <b>Zerodha Kite</b>. Then one button is enough: <b>Login with Zerodha</b>.</div></div>'
    ) : (
      '<div class="nx-z-simple-login">'
      + '<div class="nx-z-simple-kicker">Use any of below options for smooth login</div>'
      + '<div class="nx-z-simple-title">Generate your token in 2 steps</div>'
      + '<div class="nx-z-simple-steps">'
      + '<div class="nx-z-simple-step"><span>1</span><div>Click <b>Login with Zerodha</b>.</div></div>'
      + '<div class="nx-z-simple-step"><span>2</span><div>Finish login on Kite. Nexus will capture the token and update this desk automatically.</div></div>'
      + '</div>'
      + '<div class="nx-z-simple-actions">'
      + '<button type="button" class="nx-btn nx-btn-gold nx-kite-login-btn" onclick="nxKiteInteractiveLogin()" '+((interactiveStarting || loginDone)?'disabled':'')+'>'+(loginDone ? 'Login completed' : (interactiveStarting?'Opening...':'Login with Zerodha'))+'</button>'
      + '<button type="button" class="nx-btn nx-btn-ghost" onclick="nxKiteOpenLogin()" '+(loginDone?'disabled':'')+'>Popup login</button>'
      + (loginDone
        ? '<button type="button" class="nx-btn nx-btn-ghost" onclick="nxBrokerDisconnect()" '+(NX.loadingAction === 'broker-disconnect'?'disabled':'')+'>'+(NX.loadingAction === 'broker-disconnect'?'Closing...':'Close session')+'</button>'
        : '<button type="button" class="nx-btn nx-btn-ghost" onclick="nxKiteClosePopup()" '+(NX.kitePopupLaunched?'':'style="opacity:.45"')+'>Close popup</button>')
      + '</div>'
      + '<div class="nx-form-grid" style="margin-top:14px"><label class="nx-form-label">Sample symbol<input id="nx-sample-symbol" class="nx-input" placeholder="SBIN"></label><label class="nx-form-label">Sample quantity<input id="nx-sample-qty" class="nx-input" placeholder="1" value="'+escapeHtml(String(broker.default_quantity || 1))+'"></label></div>'
      + '<div class="nx-z-simple-actions" style="margin-top:12px">'
      + '<button type="button" class="nx-btn nx-btn-gold" onclick="nxBrokerSampleOrder()" '+(sampling?'disabled':'')+'>'+(sampling?'Running...':'Sample order')+'</button>'
      + '<button type="button" class="nx-btn nx-btn-ghost" onclick="nxSendKiteMobileLoginLink()" '+((NX.loadingAction === 'broker-mobile-link' || loginDone)?'disabled':'')+'>'+(NX.loadingAction === 'broker-mobile-link'?'Sending...':(loginDone?'Mobile link not needed':'Send Telegram mobile login'))+'</button>'
      + '</div>'
      + '<div class="nx-z-simple-help">If a browser popup is blocked, allow popups and retry. After success, this desk stores your token in the server DB and uses it for trading.</div>'
      + '</div>'
    );
    const deskFeedSync = !!(NX.boot || {}).desk_feed_sync_enabled;
    const feedSyncHint = paper ? '' : (
      '<div class="nx-inline-note" style="margin-top:12px;line-height:1.55">'
      + '<b>Main Trading OS</b> (factors, indices, PCR) reads live Kite ticks from <code>backend/.env</code> — separate from Nexus until you sync.'
      + (deskFeedSync
        ? ' <button type="button" class="nx-mini-btn" onclick="nxSyncMainFeedFromDesk()" '+(NX.loadingAction === 'broker-feed-sync' ? 'disabled' : '')+'>'+(NX.loadingAction === 'broker-feed-sync' ? 'Syncing…' : 'Push desk token → Trading OS')+'</button>'
        : ' After desk login: paste <code>KITE_ACCESS_TOKEN</code> into <code>backend/.env</code> and click dashboard <b>LOAD .ENV</b>, or set <code>ALLOW_DESK_TOKEN_TO_MAIN_FEED=1</code> + restart to enable push here.')
      + '</div>'
    );
    const quickLane = paper ? '' : ''
      + simpleLoginCard
      + '<div class="nx-status '+tokenClass+'" style="margin-top:12px"><b>'+tokenTitle+'</b>'+(tokenMessage ? '<div style="margin-top:6px">'+tokenMessage+'</div>' : '')+tokenDetail+waitBanner+'</div>'
      + '<details class="nx-z-simple-more"><summary>Need help or another login method?</summary><div class="nx-z-simple-more-body">'
      + '<div class="nx-inline-note"><b>Redirect URL for auto-close:</b> <code style="word-break:break-all">'+primaryRedirect+'</code> <button type="button" class="nx-mini-btn" onclick="nxCopyKiteOAuthRedirect()">Copy</button></div>'
      + altRedirectHtml
      + '<div class="nx-inline-note" style="margin-top:8px">If Zerodha opens <code>127.0.0.1</code> and shows connection refused, update the redirect URL in <b>Kite Connect → My apps</b> to the value above.</div>'
      + (allowServerImport
        ? '<div class="nx-actions" style="margin-top:12px"><button type="button" class="nx-btn nx-btn-primary" onclick="nxImportEnvBrokerToken()" '+(importing?'disabled':'')+'>'+(importing?'Importing...':'Use server session')+'</button><button type="button" class="nx-btn nx-btn-ghost" onclick="nxCheckServerTokenStatus()" '+(checking?'disabled':'')+'>'+(checking?'Checking...':'Check status')+'</button><button type="button" class="nx-btn nx-btn-ghost" onclick="nxRefreshEnvBrokerToken()" '+(refreshing?'disabled':'')+'>'+(refreshing?'Refreshing...':'Refresh token')+'</button></div>'
        : '<div class="nx-inline-note" style="margin-top:10px">Server session is disabled for user desks. This page uses only the user token generated from your own Kite login.</div>')
      + feedSyncHint
      + '</div></details>';
    const manualCard = paper ? '' : ''
      + '<div class="nx-z-manual-card">'
      + '<div class="nx-z-manual-head"><div><div class="nx-z-fast-kicker">Fallback</div><div class="nx-z-fast-title">Manual redirect paste</div></div><div class="nx-inline-note">Only use this if the one-time login flow is unavailable for your Kite redirect setup.</div></div>'
      + '<div class="nx-form-grid" style="margin-top:14px"><label class="nx-form-label">API key<input id="nx-broker-api-key" class="nx-input" autocomplete="off" placeholder="'+escapeHtml(broker.api_key_masked || 'From Kite Connect')+'" value=""></label><label class="nx-form-label">API secret<input id="nx-broker-api-secret" type="password" class="nx-input" autocomplete="new-password" placeholder="'+escapeHtml(broker.api_secret_masked || 'App secret')+'" value=""></label></div>'
      + '<label class="nx-form-label" style="margin-top:14px">Redirect URL or request_token<input id="nx-broker-request-paste" class="nx-input" placeholder="https://…?request_token=…&amp;action=login&amp;status=success"></label>'
      + '<div class="nx-inline-note" style="margin-top:10px"><b>Kite Connect redirect URL</b> must be exactly <code style="word-break:break-all">'+escapeHtml(nxKiteOAuthReturnAbs())+'</code> · <button type="button" class="nx-mini-btn" onclick="nxCopyKiteOAuthRedirect()">Copy</button></div>'
      + '<div class="nx-inline-note" style="margin-top:8px">Step 1: Save API key/secret. Step 2: Open Kite in popup. Step 3: If redirect is not configured, paste the final URL here and Capture Token.</div>'
      + '</div>';
    const simpleForm = ''
      + '<div class="nx-z-simple">'
      + '<div class="nx-form-grid nx-z-topline"><label class="nx-form-label">Broker<select id="nx-broker-code" class="nx-select" onchange="nxBrokerCodeSync()">'+brokerOptions+'</select></label><label class="nx-form-label">Desk label<input id="nx-broker-label" class="nx-input" placeholder="Primary desk" value="'+escapeHtml(broker.account_label || '')+'"></label></div>'
      + '<div id="nx-paper-fields" class="nx-z-toggle" style="display:'+(paper?'block':'none')+'"><p class="nx-z-lead">You have <b>Paper</b> selected as the broker <em>type</em>. Pick <b>Zerodha Kite</b> in the Broker menu to show login tools. After Kite is connected, use <b>Paper route</b> in Advanced to keep orders simulated.</p></div>'
      + '<div id="nx-zerodha-fields" class="nx-z-toggle" style="display:'+(paper?'none':'block')+'">'
      + quickLane
      + '</div></div>';
    let readiness = '';
    if(paper){
      readiness = '<div class="nx-z-ready nx-z-ready-paper"><div class="nx-z-ready-title">Paper desk type</div><div class="nx-z-ready-sub">Choose <b>Zerodha Kite</b> in the Broker menu to connect. <b>Paper route</b> (Advanced) is optional and only affects whether orders are sent live.</div></div>';
    }else if(sessOk && broker.enabled && effectiveLive){
      readiness = '<div class="nx-z-ready nx-z-ready-on nx-z-ready-fire"><div class="nx-z-ready-title">Ready to fire live trades today</div><div class="nx-z-ready-sub">Kite session is live, routing is on, and <b>Live</b> order firing is selected. Use <b>Sample order</b> or auto-route during market hours with care.</div></div>';
    }else if(sessOk && broker.enabled){
      readiness = '<div class="nx-z-ready nx-z-ready-on"><div class="nx-z-ready-title">Session live — paper-safe today</div><div class="nx-z-ready-sub">Kite is connected. Choose <b>Live</b> under <b>Order firing</b> when you want real exchange orders today, or stay on <b>Paper</b> for simulated drills.</div></div>';
    }else if(sessOk && !broker.enabled){
      readiness = '<div class="nx-z-ready nx-z-ready-warn"><div class="nx-z-ready-title">Connected — routing off</div><div class="nx-z-ready-sub">Turn on <b>Broker enabled</b> in Advanced when you want auto-routing.</div></div>';
    }else{
      readiness = '<div class="nx-z-ready"><div class="nx-z-ready-title">Not connected yet</div><div class="nx-z-ready-sub">Use <b>Login with Zerodha</b>. After successful Kite login, this desk updates automatically.</div></div>';
    }
    return ''
      + '<div class="nx-card nx-kite-dock">'
      + fireModeBar
      + '<div class="nx-kite-dock-head">'
      + '<div class="nx-kite-dock-title-row">'
      + '<div><div class="nx-card-title nx-kite-dock-title">Trading execution</div><div class="nx-card-sub">Pick your broker type first. Zerodha tools appear only when <b>Zerodha Kite</b> is selected — then connect, validate, and tune routing below.</div></div>'
      + '<div class="nx-kite-dock-badges"><span class="nx-badge '+statusClass+'">'+escapeHtml(broker.status || 'Idle')+'</span><span class="nx-badge '+modeBadge+'">'+modeLabel+'</span><span class="nx-badge '+(engineReady ? 'good' : 'warn')+'">'+(engineReady ? 'Engine ready' : (paper ? 'Paper' : 'Awaiting validation'))+'</span></div>'
      + '</div>'
      + '<div class="nx-kite-dock-hero">'+(paper ? '' : kiteBrand)+'</div>'
      + '</div>'
      + '<div class="nx-card-body nx-kite-dock-body"><div class="nx-kite-dock-grid">'
      + '<div class="nx-kite-dock-main">'+simpleForm
      + '<details class="nx-z-advanced"><summary>Advanced options</summary>'
      + '<div class="nx-z-advanced-body">'
      + '<div class="nx-form-grid"><label class="nx-form-label">Broker user ID<input id="nx-broker-user-id" class="nx-input" placeholder="AB1234" value="'+escapeHtml(broker.broker_user_id || '')+'"></label><label class="nx-form-label">Default qty<input id="nx-broker-qty" class="nx-input" placeholder="1" value="'+escapeHtml(String(broker.default_quantity || 1))+'"></label></div>'
      + '<div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Manual access token<input id="nx-broker-access-token" class="nx-input" placeholder="'+escapeHtml(broker.access_token_masked || 'Only if not using login URL')+'" value=""></label><label class="nx-form-label">Intraday product<select id="nx-broker-intraday-product" class="nx-select"><option '+((broker.intraday_product||'MIS')==='MIS'?'selected':'')+'>MIS</option><option '+((broker.intraday_product||'MIS')==='CNC'?'selected':'')+'>CNC</option><option '+((broker.intraday_product||'MIS')==='NRML'?'selected':'')+'>NRML</option></select></label></div>'
      + '<div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Positional product<select id="nx-broker-positional-product" class="nx-select"><option '+((broker.positional_product||'CNC')==='CNC'?'selected':'')+'>CNC</option><option '+((broker.positional_product||'CNC')==='NRML'?'selected':'')+'>NRML</option><option '+((broker.positional_product||'CNC')==='MIS'?'selected':'')+'>MIS</option></select></label><label class="nx-form-label">&nbsp;</label></div>'
      + '<div class="nx-toggle-grid" style="margin-top:14px"><label class="nx-check"><input id="nx-broker-enabled" type="checkbox" onchange="nxBrokerCodeSync()"'+checkedAttr(!!broker.enabled)+'><span>Broker enabled</span><span class="nx-check-hint">Required for Sample order and auto-routing — must be saved with Capture Token or Save Credentials</span></label><label class="nx-check"><input id="nx-broker-paper-mode" type="checkbox" onchange="nxBrokerCodeSync()"'+checkedAttr(!!(broker.paper_mode || paper))+'><span>Paper route</span><span class="nx-check-hint">Simulate orders (Kite session can still be real)</span></label><label class="nx-check"><input id="nx-broker-live-mode" type="checkbox" onchange="nxBrokerCodeSync()"'+checkedAttr(!!broker.live_mode && !paper)+' '+(liveLocked ? 'disabled' : '')+'><span>Live mode</span><span class="nx-check-hint">Real broker orders when off paper route</span></label><label class="nx-check"><input id="nx-user-auto-execute" type="checkbox"'+checkedAttr(!!((user.controls||{}).auto_execute))+'><span>Auto execute</span></label></div>'
      + '<div class="nx-form-grid-3" style="margin-top:12px"><label class="nx-form-label">Daily loss<input id="nx-user-daily-loss" class="nx-input" placeholder="2500" value="'+escapeHtml(String((user.controls||{}).daily_loss_limit || 0))+'"></label><label class="nx-form-label">Max trades / day<input id="nx-user-max-trades" class="nx-input" placeholder="6" value="'+escapeHtml(String((user.controls||{}).max_trades_per_day || 0))+'"></label><label class="nx-form-label">Max open signals<input id="nx-user-max-open" class="nx-input" placeholder="3" value="'+escapeHtml(String((user.controls||{}).max_open_signals || 0))+'"></label></div>'
      + '<div class="nx-z-cta">'
      + '<button type="button" class="nx-btn nx-btn-primary nx-btn-validate" onclick="nxBrokerSaveCredentials()" '+(saving?'disabled':'')+'>'+(saving?'Saving...':'Save advanced changes')+'</button>'
      + '<button type="button" class="nx-btn nx-btn-gold" onclick="nxCaptureKiteToken()" '+(saving?'disabled':'')+'>'+(saving?'Saving...':'Use pasted token')+'</button>'
      + '<button type="button" class="nx-btn nx-btn-ghost" onclick="nxBrokerTest()" '+(testing?'disabled':'')+'>'+(testing?'Testing...':'Test session')+'</button>'
      + '</div>'
      + manualCard
      + '</div></details>'
      + brokerTestHtml
      + '</div>'
      + '<div class="nx-kite-dock-side">'
      + readiness
      + '<div class="nx-item nx-kite-health"><div class="nx-item-title">Session</div><div class="nx-item-sub">'+profileLine+'</div>'
      + '<div class="nx-inline-note" style="margin-top:8px">Key / Secret / Token show what is <b>saved on this desk</b> after Validate. Dashes mean not stored yet — use Quick connect, One-time login, or paste in Advanced.</div>'
      + '<div class="nx-broker-chips" style="margin-top:12px"><span class="nx-badge cool">Key '+escapeHtml(broker.api_key_masked || '—')+'</span><span class="nx-badge warn">Secret '+escapeHtml(broker.api_secret_masked || '—')+'</span><span class="nx-badge cool">Token '+escapeHtml(broker.access_token_masked || '—')+'</span></div>'
      + (broker.last_error ? '<div class="nx-status error" style="margin-top:12px">'+escapeHtml(broker.last_error)+'</div>' : '<div class="nx-status success" style="margin-top:12px">No blocking errors. Use <b>Test session</b> after market opens if needed.</div>')
      + '</div>'
      + '<div class="nx-item" style="margin-top:12px"><div class="nx-item-title">Recent auto orders</div><div class="nx-item-sub">Live orders and paper drills. On paper route, <b>Paper test OK</b> means the desk check succeeded — nothing is sent to the exchange.</div><div style="margin-top:12px">'+executionOrders+'</div></div>'
      + '<div class="nx-item" style="margin-top:12px"><div class="nx-item-title">Test trade log</div><div class="nx-item-sub">Latest sample order. Requires <b>Broker enabled</b> (Advanced routing) saved to the server. Use top <b>Order firing → Live</b> for real Kite orders (market hours). <b>Paper route</b> in Advanced must be off for live.</div>'
      + (sampleLog ? '<div class="nx-inline-note" style="margin-top:10px">Symbol <b>'+escapeHtml(sampleLog.symbol || '—')+'</b> · Qty <b>'+escapeHtml(String(sampleLog.quantity || '—'))+'</b> · Status <b>'+escapeHtml(sampleLog.status || 'PENDING')+'</b></div><div class="nx-status '+(sampleLog.ok ? 'success' : 'error')+'" style="margin-top:10px">'+escapeHtml(sampleLog.message || '')+'</div>' : '<div class="nx-inline-note" style="margin-top:10px">Run <b>Sample Order</b> to populate this panel.</div>')
      + '</div>'
      + '</div></div></div></div>';
    }catch(err){
      console.error('Nexus renderKiteExecutionHub failed', err);
      return '<div class="nx-card nx-status error" style="margin:12px"><div class="nx-item-title">Trading execution</div><div class="nx-item-sub" style="margin-top:8px;word-break:break-word">This section hit a render error (the rest of Nexus may still work).</div><div class="nx-inline-note" style="margin-top:10px;word-break:break-word;font-family:ui-monospace,Consolas,monospace;font-size:12px">'+escapeHtml(String((err && err.message) || err || 'unknown'))+'</div><div class="nx-actions" style="margin-top:12px"><button type="button" class="nx-btn nx-btn-primary" onclick="window.nxRefresh&&window.nxRefresh()">Reload desk data</button></div></div>';
    }
  }

  function renderUserCards(){
    const d = NX.dashboard || {};
    const user = d.user || NX.user || {};
    const perf = d.performance || { summary:{}, curve:[] };
    const wallet = d.wallet || { ledger:[] };
    const metrics = d.metrics || {};
    const strategies = d.strategies || [];
    const plans = d.plans || [];
    const contacts = user.contacts || {};
    const notifications = user.notifications || {};
    const broker = d.broker || { catalog:[], recent_orders:[], profile:{}, capabilities:{} };
    const brokerDeskLive = !!broker.effective_live;
    const brokerCatalog = broker.catalog || [];
    const strategyHtml = strategies.length ? '<div class="nx-strategy-grid">' + strategies.map(function(s){
      return '<div class="nx-strategy" style="--acc:'+(s.accent||'#58d6ff')+'"><div class="nx-acc-line"></div><h4>'+escapeHtml(s.name)+'</h4><p>'+escapeHtml(s.description || '')+'</p><div class="nx-row"><span>'+escapeHtml(s.strategy_type)+'</span><span class="nx-badge '+(s.enabled?'good':'bad')+'">'+(s.enabled?'Enabled':'Paused')+'</span></div><div class="nx-row"><span>Min confidence</span><span>'+fmtNum(s.min_confidence || s.default_confidence)+'%</span></div><div class="nx-row"><span>Daily cap</span><span>'+fmtNum(s.max_trades_per_day || s.default_max_trades)+'</span></div><div class="nx-actions" style="margin-top:12px"><button class="nx-mini-btn" onclick="nxToggleStrategy(\''+s.code+'\','+(s.enabled?0:1)+')">'+(s.enabled?'Pause':'Enable')+'</button></div></div>';
    }).join('') + '</div>' : '<div class="nx-empty">No strategies assigned.</div>';
    const activePlanCode = String((((user.subscription||{}).plan_code) || '')).toUpperCase();
    const activeRank = planRank(activePlanCode);
    const planHtml = plans.length ? '<div class="nx-plan-grid">' + plans.map(function(p){
      const code = String(p.code || '').toUpperCase();
      const rank = planRank(code);
      const isCurrent = activePlanCode && code === activePlanCode && String((user.subscription||{}).status || '').toUpperCase() === 'ACTIVE';
      const isUpgrade = activeRank >= 0 && rank > activeRank;
      const cta = isCurrent ? '<button class="nx-mini-btn" disabled>Current Plan</button>' : '<button class="nx-mini-btn" onclick="nxCreatePayment(\''+p.code+'\')">'+(isUpgrade ? 'Upgrade Plan' : 'Create Order')+'</button>';
      return '<div class="nx-plan"><h4>'+escapeHtml(p.name)+'</h4><p>'+escapeHtml((p.features||[]).join(' · '))+'</p><div class="nx-plan-price">'+fmtMoney(p.price)+'</div><div class="nx-inline-note">'+fmtNum(p.duration_days)+' day access · bonus '+fmtMoney(p.wallet_credit||0)+'</div><div class="nx-actions" style="margin-top:12px">'+cta+'</div></div>';
    }).join('') + '</div><div class="nx-actions" style="margin-top:12px"><button class="nx-btn nx-btn-ghost" onclick="nxCreateCustomPayment()">Custom Pay (min ₹10)</button></div>' : '<div class="nx-empty">Plan catalog unavailable.</div>';
    const ledgerHtml = (wallet.ledger||[]).length ? '<div class="nx-list">'+wallet.ledger.slice(0,8).map(function(x){ return '<div class="nx-item"><div class="nx-item-top"><div><div class="nx-item-title">'+escapeHtml(x.kind)+'</div><div class="nx-item-sub">'+escapeHtml(x.note || x.reference_type || 'Ledger event')+'</div></div><div class="nx-badge '+((Number(x.amount||0)>=0)?'good':'bad')+'">'+fmtMoney(x.amount||0)+'</div></div><div class="nx-inline-note" style="margin-top:10px">'+escapeHtml(fmtDate(x.created_at))+'</div></div>'; }).join('')+'</div>' : '<div class="nx-empty">Wallet ledger is empty.</div>';
    const paymentHtml = NX.payments.length ? '<table class="nx-mini-table"><thead><tr><th>Plan</th><th>Status</th><th>Amount</th><th>Created</th></tr></thead><tbody>'+NX.payments.slice(0,8).map(function(p){ return '<tr><td>'+escapeHtml(p.plan_code || 'CUSTOM')+'</td><td>'+escapeHtml(p.status)+'</td><td>'+fmtMoney(p.amount)+'</td><td>'+escapeHtml(fmtDate(p.created_at))+'</td></tr>'; }).join('')+'</tbody></table>' : '<div class="nx-empty">No payment orders created yet.</div>';
    const selectedPayment = NX.payments.filter(function(p){ return p.id === NX.selectedPaymentId; })[0] || NX.payments[0] || null;
    const payMeta = paymentMeta(selectedPayment || {});
    const paymentProfile = currentPaymentProfile();
    const paymentRows = NX.payments.length ? '<table class="nx-mini-table"><thead><tr><th>Plan</th><th>Status</th><th>Amount</th><th>Created</th></tr></thead><tbody>'+NX.payments.slice(0,8).map(function(p){ const st=String(p.status||'').toUpperCase(); const cls=(st==='PAID'?'good':(st==='PENDING_VALIDATION'?'cool':'warn')); return '<tr class="'+(selectedPayment && p.id === selectedPayment.id ? 'nx-row-active' : '')+'" onclick="nxSelectPayment('+p.id+')"><td>'+escapeHtml(p.plan_code || 'CUSTOM')+'</td><td><span class="nx-badge '+cls+'">'+escapeHtml(p.status)+'</span></td><td>'+fmtMoney(p.amount)+'</td><td>'+escapeHtml(fmtDate(p.created_at))+'</td></tr>'; }).join('')+'</tbody></table>' : '<div class="nx-empty">No payment orders created yet.</div>';
    const paymentViewer = selectedPayment ? (
      '<div class="nx-pay-panel">'
      + '<div class="nx-pay-head"><div><div class="nx-item-title">'+escapeHtml((selectedPayment.plan_code || 'CUSTOM') + ' Payment')+'</div><div class="nx-item-sub">Order #'+fmtNum(selectedPayment.id)+' · '+escapeHtml(selectedPayment.provider || 'LOCAL')+'</div></div><span class="nx-badge '+(String(selectedPayment.status||'').toUpperCase()==='PAID' ? 'good' : (String(selectedPayment.status||'').toUpperCase()==='PENDING_VALIDATION' ? 'cool' : 'warn'))+'">'+escapeHtml(selectedPayment.status || 'CREATED')+'</span></div>'
      + '<div class="nx-pay-grid">'
      + '<div class="nx-pay-qr nx-pay-qr-wrap">'+(payMeta.qrUrl ? '<div class="nx-qr-ring"><img src="'+escapeHtml(payMeta.qrUrl)+'" alt="UPI QR"></div><div class="nx-inline-note" style="margin-top:12px">This QR routes the payment to <b>'+escapeHtml(payMeta.payeeName || paymentProfile.payee_name || 'Configured UPI payee')+'</b>.</div>' : '<div class="nx-empty">Ask admin to save the UPI destination first.</div>')+'</div>'
      + '<div><div class="nx-item nx-pay-destination"><div class="nx-item-top"><div><div class="nx-item-title">Pay with Scanner or UPI</div><div class="nx-item-sub">The QR and UPI below are generated from the admin-saved payment rail.</div></div><span class="nx-badge '+(payMeta.canAutoConfirm ? 'good' : 'warn')+'">'+(payMeta.canAutoConfirm ? 'Auto confirm ready' : 'Manual confirm flow')+'</span></div><div class="nx-pay-bits" style="margin-top:12px"><div class="nx-pay-chip"><span>Payee</span><strong>'+escapeHtml(payMeta.payeeName || paymentProfile.payee_name || 'Not configured')+'</strong></div><div class="nx-pay-chip"><span>UPI</span><strong>'+escapeHtml(payMeta.upiId || paymentProfile.upi_id || 'Not available')+'</strong></div><div class="nx-pay-chip"><span>Amount</span><strong>'+escapeHtml(payMeta.amount || String(selectedPayment.amount || ''))+'</strong></div><div class="nx-pay-chip"><span>Order Ref</span><strong>'+escapeHtml(payMeta.note || selectedPayment.plan_code || '')+'</strong></div></div><div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Merchant Code<input class="nx-input" value="'+escapeHtml(payMeta.merchantCode || 'Not set')+'" readonly></label><label class="nx-form-label">Checkout / UPI Intent<input class="nx-input" value="'+escapeHtml(selectedPayment.checkout_url || payMeta.qrPayload || 'Local UPI payment')+'" readonly></label></div><div class="nx-pay-callout" style="margin-top:12px"><div class="nx-pay-callout-title">How this works</div><div class="nx-item-sub">'+escapeHtml(payMeta.instructions || 'Scan and pay the exact amount shown here.')+'</div>'+(payMeta.supportPhone || payMeta.supportEmail ? '<div class="nx-inline-note" style="margin-top:10px">Support: '+escapeHtml([payMeta.supportPhone, payMeta.supportEmail].filter(Boolean).join(' · '))+'</div>' : '')+'</div><div class="nx-actions" style="margin-top:12px"><button class="nx-btn nx-btn-primary" onclick="nxOpenUpiIntent(\''+escapeHtml(payMeta.upiLink)+'\')">Open UPI App</button><button class="nx-btn nx-btn-ghost" onclick="nxCopyText(\''+escapeHtml(payMeta.upiId || paymentProfile.upi_id || '')+'\', \'UPI ID copied\')">Copy UPI ID</button>'+(selectedPayment.checkout_url ? '<a class="nx-btn nx-btn-ghost" href="'+escapeHtml(selectedPayment.checkout_url)+'" target="_blank" rel="noreferrer">Open Checkout</a>' : '')+(String(selectedPayment.status||'').toUpperCase()==='PAID' ? '<button class="nx-btn nx-btn-gold" disabled>Payment Successful</button>' : (String(selectedPayment.status||'').toUpperCase()==='PENDING_VALIDATION' ? '<button class="nx-btn nx-btn-gold" disabled>Pending Admin Validation</button>' : '<button class="nx-btn nx-btn-gold" onclick="nxMarkMyPaymentPaid('+selectedPayment.id+')">Submit For Validation</button>'))+(String(selectedPayment.status||'').toUpperCase()==='PAID' ? '' : '<button class="nx-btn nx-btn-ghost" onclick="nxDeletePaymentOrder('+selectedPayment.id+')">Remove Order</button>')+'</div></div>'+(String(selectedPayment.status||'').toUpperCase()==='PAID' ? '<div class="nx-status success" style="margin-top:12px">Payment successful. Your subscription and wallet have been updated.</div>' : (String(selectedPayment.status||'').toUpperCase()==='PENDING_VALIDATION' ? '<div class="nx-status info" style="margin-top:12px">Payment submitted. Waiting for admin approval. Once approved, it reflects in your account automatically.</div>' : '<div class="nx-status info" style="margin-top:12px">'+(payMeta.canAutoConfirm ? 'Gateway/webhook confirmation can update status automatically once payment settles.' : 'For direct UPI, pay exact amount then click <b>Submit For Validation</b>. Admin approval is required before account update.')+'</div>'))+'</div>'
      + '</div></div>'
    ) : '<div class="nx-empty">Select a plan to generate a payment order and scanner.</div>';
    const tradeHtml = NX.trades.length ? '<table class="nx-mini-table"><thead><tr><th>Symbol</th><th>Strategy</th><th>Status</th><th>PnL</th></tr></thead><tbody>'+NX.trades.slice(0,8).map(function(t){ return '<tr><td>'+escapeHtml(t.symbol)+'</td><td>'+escapeHtml(t.strategy_code)+'</td><td>'+escapeHtml(t.status)+'</td><td>'+fmtMoney(t.pnl)+'</td></tr>'; }).join('')+'</tbody></table>' : '<div class="nx-empty">Manual or synced trade journal entries will appear here.</div>';
    const effectiveBrokerCode = nxEffectiveBrokerCode(broker);
    const brokerOptions = brokerCatalog.length ? brokerCatalog.map(function(b){
      var bc = String(b.code||'').toUpperCase();
      return '<option value="'+escapeHtml(b.code)+'" '+(effectiveBrokerCode===bc ? 'selected' : '')+'>'+escapeHtml(b.name)+' • '+escapeHtml(b.tagline || '')+'</option>';
    }).join('') : '<option value="PAPER"'+(effectiveBrokerCode==='PAPER'?' selected':'')+'>Paper Router</option><option value="ZERODHA"'+(effectiveBrokerCode==='ZERODHA'?' selected':'')+'>Zerodha Kite</option>';
    const executionOrders = (broker.recent_orders||[]).length ? '<table class="nx-mini-table"><thead><tr><th>When</th><th>Strategy</th><th>Symbol</th><th>Status</th><th>Mode</th></tr></thead><tbody>'+(broker.recent_orders||[]).slice(0,8).map(function(o){
      var st = String(o.status || '');
      var lbl = nxBrokerOrderStatusLabel(st);
      var cls = nxBrokerOrderStatusBadgeClass(st);
      return '<tr><td>'+escapeHtml(fmtDate(o.created_at))+'</td><td>'+escapeHtml(o.strategy_code)+'</td><td>'+escapeHtml(o.symbol || o.tradingsymbol)+'</td><td><span class="nx-badge '+cls+'">'+escapeHtml(lbl)+'</span>'+(o.error_text ? '<div class="nx-inline-note" style="margin-top:6px">'+escapeHtml(o.error_text)+'</div>' : '')+'</td><td>'+(o.live_mode ? 'Live' : 'Paper')+'</td></tr>';
    }).join('')+'</tbody></table>' : '<div class="nx-empty">No broker execution attempts yet. Once auto-routing is enabled, placed or simulated orders will show here.</div>';
    const brokerProfileBits = [];
    if((broker.profile||{}).name) brokerProfileBits.push(escapeHtml(broker.profile.name));
    if((broker.profile||{}).user_id) brokerProfileBits.push('ID ' + escapeHtml(broker.profile.user_id));
    if((broker.profile||{}).email) brokerProfileBits.push(escapeHtml(broker.profile.email));
    const brokerTest = NX.brokerTest || null;
    const brokerTestHtml = brokerTest ? '<div class="nx-status '+escapeHtml(brokerTest.type === 'error' ? 'error' : 'success')+'" style="margin-top:12px"><b>'+(brokerTest.type === 'error' ? 'Validation issue' : 'Session OK')+'</b>'+(brokerTest.message ? ' · ' + escapeHtml(brokerTest.message) : '')+(brokerTest.detail ? '<div style="margin-top:6px">'+escapeHtml(brokerTest.detail)+'</div>' : '')+'</div>' : '';
    const kiteDeskHtml = renderKiteExecutionHub(user, broker, brokerOptions, brokerProfileBits, brokerTestHtml, executionOrders, effectiveBrokerCode);
    const notifyCardHtml = renderUserNotifyCard(contacts, notifications);
    const overviewCard = '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Overview</div><div class="nx-card-sub">Wallet, signals, and performance snapshot</div></div><span class="nx-badge cool">'+escapeHtml((user.subscription||{}).plan_code || 'No Plan')+'</span></div><div class="nx-card-body"><div class="nx-metric-grid"><div class="nx-metric"><div class="nx-metric-k">Balance</div><div class="nx-metric-v">'+fmtMoney((((user.wallet||{}).balance)||0))+'</div></div><div class="nx-metric"><div class="nx-metric-k">Unread</div><div class="nx-metric-v">'+fmtNum(metrics.signals_unread||0)+'</div></div><div class="nx-metric"><div class="nx-metric-k">Closed Trades</div><div class="nx-metric-v">'+fmtNum((perf.summary||{}).closed_trades||0)+'</div></div><div class="nx-metric"><div class="nx-metric-k">Total PnL</div><div class="nx-metric-v">'+fmtMoney((perf.summary||{}).total_pnl||0)+'</div></div></div><div class="nx-inline-note" style="margin-top:14px">Controls: daily loss '+fmtMoney(((user.controls||{}).daily_loss_limit)||0)+' · max trades '+fmtNum(((user.controls||{}).max_trades_per_day)||0)+' · profit share '+fmtNum(((user.controls||{}).profit_share_pct)||0)+'%</div></div></div>';
    const statusStrip = '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Workspace status</div><div class="nx-card-sub">Live access posture for your desk, broker, and payments.</div></div></div><div class="nx-card-body"><div class="nx-workspace-mini-grid"><div class="nx-item"><div class="nx-item-title">Subscription</div><div class="nx-item-sub">'+escapeHtml((user.subscription||{}).plan_code || 'No active plan')+' · '+escapeHtml((user.subscription||{}).status || 'NONE')+'</div></div><div class="nx-item"><div class="nx-item-title">Broker</div><div class="nx-item-sub">'+escapeHtml(broker.broker_name || 'Paper Router')+' · '+escapeHtml(broker.status || 'Idle')+'</div></div><div class="nx-item"><div class="nx-item-title">Notifications</div><div class="nx-item-sub">'+escapeHtml((notifications.email ? 'Email ' : '') + (notifications.telegram ? 'Telegram ' : '') + (notifications.whatsapp ? 'WhatsApp ' : '') || 'All disabled')+'</div></div></div></div></div>';
    const signalsCard = '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Signal Inbox</div><div class="nx-card-sub">Routed SPIKE, INDEX, and SWING events from the live engines</div></div><button class="nx-mini-btn" onclick="nxRefresh()">Reload</button></div><div class="nx-card-body">'+renderSignalItems(d.signals || [])+'</div></div>';
    const strategiesCard = '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Strategies</div><div class="nx-card-sub">Per-user entitlement and threshold control</div></div></div><div class="nx-card-body">'+strategyHtml+'</div></div>';
    const walletCard = '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Wallet & Ledger</div><div class="nx-card-sub">Coupon credits, paid wallet state, and fee deductions</div></div></div><div class="nx-card-body"><div class="nx-split"><div>'+ledgerHtml+'</div><div><div class="nx-item"><div class="nx-item-title">Redeem coupon</div><div class="nx-item-sub">Apply additional credits or capped-test accounts.</div><div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Coupon Code<input id="nx-coupon-code" class="nx-input" placeholder="WELCOME500"></label><div style="display:flex;align-items:flex-end"><button class="nx-btn nx-btn-gold" onclick="nxRedeemCoupon()">Redeem</button></div></div></div><div class="nx-item" style="margin-top:12px"><div class="nx-item-title">Wallet State</div><div class="nx-item-sub">Type '+escapeHtml((user.wallet||{}).type || 'COUPON')+' · status '+escapeHtml((user.wallet||{}).status || 'ACTIVE')+' · cap '+fmtMoney(((user.wallet||{}).coupon_profit_cap)||0)+'</div></div></div></div></div></div>';
    const journalCard = '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Performance & Trades</div><div class="nx-card-sub">Trade journal, curve, and lightweight performance snapshot</div></div></div><div class="nx-card-body"><div class="nx-split"><div>'+renderCurve(perf.curve || [])+'<div class="nx-inline-note" style="margin-top:12px">Wins '+fmtNum((perf.summary||{}).wins||0)+' · Losses '+fmtNum((perf.summary||{}).losses||0)+' · Win rate '+fmtNum((perf.summary||{}).win_rate||0)+'%</div></div><div><div class="nx-item"><div class="nx-item-title">Quick trade journal</div><div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Symbol<input id="nx-trade-symbol" class="nx-input" placeholder="RELIANCE"></label><label class="nx-form-label">Strategy<select id="nx-trade-strategy" class="nx-select"><option>SPIKE</option><option>INDEX</option><option>SWING</option></select></label></div><div class="nx-form-grid-3" style="margin-top:12px"><label class="nx-form-label">Entry<input id="nx-trade-entry" class="nx-input" placeholder="100"></label><label class="nx-form-label">Exit<input id="nx-trade-exit" class="nx-input" placeholder="110"></label><label class="nx-form-label">PnL<input id="nx-trade-pnl" class="nx-input" placeholder="250"></label></div><div class="nx-actions" style="margin-top:12px"><button class="nx-btn nx-btn-primary" onclick="nxCreateTrade()">Add Trade</button></div></div><div class="nx-item" style="margin-top:12px">'+tradeHtml+'</div></div></div></div></div>';
    const paymentsCard = '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Plans & Payments</div><div class="nx-card-sub">Choose a plan, show the saved payment rail, and update payment success directly in the app</div></div><span class="nx-badge '+((paymentProfile.upi_id && paymentProfile.enabled)?'good':'warn')+'">'+escapeHtml(paymentProfile.upi_id || 'UPI pending')+'</span></div><div class="nx-card-body"><div class="nx-split"><div>'+planHtml+'</div><div>'+paymentRows+paymentViewer+'</div></div></div></div>';
    const controlsCard = '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Risk & Automation</div><div class="nx-card-sub">Save your routing posture along with alert preferences.</div></div></div><div class="nx-card-body"><div class="nx-toggle-grid"><label class="nx-check"><input id="nx-user-auto-execute" type="checkbox"'+checkedAttr(!!((user.controls||{}).auto_execute))+'><span>Auto execute</span></label><div class="nx-pay-chip"><span>Broker</span><strong>'+escapeHtml(broker.broker_name || 'Paper Router')+'</strong></div><div class="nx-pay-chip"><span>Desk mode</span><strong>'+(brokerDeskLive ? 'Live (exchange)' : 'Safe / Paper')+'</strong></div><div class="nx-pay-chip"><span>Broker status</span><strong>'+escapeHtml(broker.status || 'Idle')+'</strong></div></div><div class="nx-form-grid-3" style="margin-top:12px"><label class="nx-form-label">Daily loss<input id="nx-user-daily-loss" class="nx-input" placeholder="2500" value="'+escapeHtml(String((user.controls||{}).daily_loss_limit || 0))+'"></label><label class="nx-form-label">Max trades / day<input id="nx-user-max-trades" class="nx-input" placeholder="6" value="'+escapeHtml(String((user.controls||{}).max_trades_per_day || 0))+'"></label><label class="nx-form-label">Max open signals<input id="nx-user-max-open" class="nx-input" placeholder="3" value="'+escapeHtml(String((user.controls||{}).max_open_signals || 0))+'"></label></div><div class="nx-actions" style="margin-top:12px"><button class="nx-btn nx-btn-primary" onclick="nxSaveNotifications()">Save Preferences</button></div></div></div>';
    const tab = currentWorkspaceTab();
    let body = overviewCard + statusStrip;
    if(tab === 'execution') body = kiteDeskHtml;
    else if(tab === 'signals') body = signalsCard + strategiesCard;
    else if(tab === 'wallet') body = walletCard;
    else if(tab === 'payments') body = paymentsCard;
    else if(tab === 'journal') body = journalCard;
    else if(tab === 'settings') body = '<div class="nx-settings-grid">'+controlsCard+notifyCardHtml+'</div>';
    return renderWorkspaceShell({
      tabs: workspaceTabs(),
      railKicker: 'User workspace',
      railTitle: user.full_name || user.email || 'Trader',
      railSub: 'Vertical tabs keep each area focused while the rest of the workspace stays out of the way.',
      railCards: [
        '<div class="nx-workspace-rail-card nx-workspace-rail-metric"><div class="nx-workspace-rail-kicker">Wallet</div><div class="nx-workspace-rail-title">'+fmtMoney((((user.wallet||{}).balance)||0))+'</div><div class="nx-workspace-rail-sub">'+escapeHtml((user.wallet||{}).type || 'Wallet')+' · '+escapeHtml((user.subscription||{}).plan_code || 'No plan')+'</div></div>',
        '<div class="nx-workspace-rail-card nx-workspace-rail-metric"><div class="nx-workspace-rail-kicker">Broker</div><div class="nx-workspace-rail-title">'+escapeHtml(broker.broker_name || 'Paper Router')+'</div><div class="nx-workspace-rail-sub">'+escapeHtml(broker.status || 'Idle')+' · '+(brokerDeskLive ? 'Live (exchange)' : 'Safe mode')+'</div></div>'
      ],
      headRight: '<div class="nx-pill-row"><span class="nx-pill">Unread '+fmtNum(metrics.signals_unread||0)+'</span><span class="nx-pill">Trades '+fmtNum((perf.summary||{}).closed_trades||0)+'</span><span class="nx-pill">'+escapeHtml((user.subscription||{}).plan_code || 'No Plan')+'</span></div>',
      body: body
    });
  }

  function renderAdminCards(){
    const a = NX.admin || { metrics:{}, users:[], strategies:[], coupons:[], paymentsFull:[], signals:[], settings:[] };
    const gmailReady = !!((NX.boot || {}).gmail_ready);
    const paymentProfile = a.payment_profile || adminSettingValue('payment_profile', ((NX.boot || {}).payment_profile) || {});
    const users = (a.users || []);
    const q = String(NX.adminUserQuery || '').trim().toLowerCase();
    const filteredUsers = users.filter(function(u){
      if(!q) return true;
      const hay = [
        u.email,
        u.full_name,
        u.role,
        u.status,
        (((u.contacts||{}).whatsapp_phone)||''),
        (((u.contacts||{}).telegram_chat_id)||''),
        (u.notes || '')
      ].join(' ').toLowerCase();
      return hay.indexOf(q) >= 0;
    });
    const selectedUser = currentAdminManagedUser();
    const selectedContacts = ((selectedUser || {}).contacts) || {};
    const selectedControls = ((selectedUser || {}).controls) || {};
    const selectedWallet = ((selectedUser || {}).wallet) || {};
    const selectedNotifications = ((selectedUser || {}).notifications) || {};
    const stratCards = (a.strategies||[]).map(function(s){
      return '<div class="nx-strategy" style="--acc:'+(s.accent||'#58d6ff')+'"><div class="nx-acc-line"></div><h4>'+escapeHtml(s.name)+'</h4><p>'+escapeHtml(s.description||'')+'</p><div class="nx-row"><span>Type</span><span>'+escapeHtml(s.strategy_type)+'</span></div><div class="nx-row"><span>Active</span><span class="nx-badge '+(s.active?'good':'bad')+'">'+(s.active?'Live':'Paused')+'</span></div><div class="nx-actions" style="margin-top:12px"><button class="nx-mini-btn" onclick="nxAdminToggleStrategy(\''+s.code+'\','+(s.active?0:1)+')">'+(s.active?'Pause':'Enable')+'</button></div></div>';
    }).join('');
    const coupons = (a.coupons||[]).length ? '<table class="nx-mini-table"><thead><tr><th>Code</th><th>Credit</th><th>Used</th><th>Cap</th></tr></thead><tbody>'+(a.coupons||[]).slice(0,8).map(function(c){ return '<tr><td>'+escapeHtml(c.code)+'</td><td>'+fmtMoney(c.credit)+'</td><td>'+fmtNum(c.used_count)+' / '+fmtNum(c.usage_limit)+'</td><td>'+fmtMoney(c.max_profit)+'</td></tr>'; }).join('')+'</tbody></table>' : '<div class="nx-empty">No coupons yet.</div>';
    const payments = (a.paymentsFull||[]).length ? '<table class="nx-mini-table"><thead><tr><th>Order</th><th>Status</th><th>Plan</th><th>Action</th></tr></thead><tbody>'+(a.paymentsFull||[]).slice(0,8).map(function(p){ const st=String(p.status||'').toUpperCase(); const action=(st==='PAID'?'—':'<button class="nx-mini-btn" onclick="nxAdminMarkPaid('+p.id+')">'+(st==='PENDING_VALIDATION'?'Approve':'Mark Paid')+'</button>'); return '<tr><td>#'+fmtNum(p.id)+'</td><td>'+escapeHtml(p.status)+'</td><td>'+escapeHtml(p.plan_code || 'CUSTOM')+'</td><td>'+action+'</td></tr>'; }).join('')+'</tbody></table>' : '<div class="nx-empty">No orders yet.</div>';
    const signals = (a.signals||[]).length ? '<div class="nx-list">'+(a.signals||[]).slice(0,6).map(function(s){ return '<div class="nx-item"><div class="nx-item-top"><div><div class="nx-item-title">'+escapeHtml(s.headline)+'</div><div class="nx-item-sub">'+escapeHtml(s.strategy_code)+' · '+escapeHtml(fmtDate(s.created_at))+'</div></div><span class="nx-badge cool">'+fmtNum(s.confidence)+'%</span></div></div>'; }).join('')+'</div>' : '<div class="nx-empty">No routed events yet.</div>';
    const overviewCard = '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Admin Command Deck</div><div class="nx-card-sub">Monitor the SaaS layer without touching the existing trading panels</div></div></div><div class="nx-card-body"><div class="nx-metric-grid"><div class="nx-metric"><div class="nx-metric-k">Users</div><div class="nx-metric-v">'+fmtNum((a.metrics||{}).total_users||0)+'</div></div><div class="nx-metric"><div class="nx-metric-k">Active</div><div class="nx-metric-v">'+fmtNum((a.metrics||{}).active_users||0)+'</div></div><div class="nx-metric"><div class="nx-metric-k">Revenue</div><div class="nx-metric-v">'+fmtMoney((a.metrics||{}).revenue||0)+'</div></div><div class="nx-metric"><div class="nx-metric-k">Signals</div><div class="nx-metric-v">'+fmtNum((a.metrics||{}).signals_total||0)+'</div></div></div></div></div>';
    const gstat = gmailStatus();
    const gmailModeLabel = gstat.mode === 'oauth' ? 'OAuth Live' : (gstat.mode === 'smtp' ? 'SMTP Live' : 'Not Connected');
    const gmailStatusTone = gstat.ready ? 'good' : 'warn';
    const brandCard = ''
      + '<div class="nx-card">'
      + '<div class="nx-card-head"><div><div class="nx-card-title">Brand & Mail</div><div class="nx-card-sub">Connect Gmail once, then send onboarding and alert emails directly from admin actions.</div></div><span class="nx-badge '+gmailStatusTone+'">'+gmailModeLabel+'</span></div>'
      + '<div class="nx-card-body">'
      + '<div class="nx-item">'
      + '<div class="nx-item-title">'+escapeHtml((NX.boot||{}).brand || 'STOCKR.IN')+'</div>'
      + '<div class="nx-item-sub">Mailer state: '+(gstat.ready ? '<b>Connected</b>' : '<b>Pending</b>')+' · Mode: <b>'+escapeHtml((gmailModeLabel || '').toUpperCase())+'</b>'+(gstat.oauth_email ? ' · Account: <b>'+escapeHtml(gstat.oauth_email)+'</b>' : '')+'</div>'
      + (gstat.oauth_error ? '<div class="nx-status error" style="margin-top:10px">'+escapeHtml(gstat.oauth_error)+'</div>' : '')
      + '<div class="nx-actions" style="margin-top:12px">'
      + '<button class="nx-btn nx-btn-primary" onclick="nxAdminGmailOauthStart()">Connect Gmail OAuth</button>'
      + '<button class="nx-btn nx-btn-ghost" onclick="nxAdminGmailOauthStatus()">Refresh status</button>'
      + '<button class="nx-btn nx-btn-ghost" onclick="nxAdminGmailOauthDisconnect()" '+(gstat.oauth_connected ? '' : 'disabled')+'>Disconnect</button>'
      + '</div>'
      + '<div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Test Email<input id="nx-admin-gmail-test-email" class="nx-input" placeholder="'+escapeHtml((((NX.user||{}).email)||((NX.boot||{}).admin||{}).email || 'admin@stockr.in'))+'"></label><div style="display:flex;align-items:flex-end"><button class="nx-btn '+(gmailReady ? 'nx-btn-primary' : 'nx-btn-ghost')+'" onclick="nxAdminSendTestEmail()" '+(gmailReady ? '' : 'disabled')+'>Send Gmail Test</button></div></div>'
      + '</div></div></div>';
    const paymentRailCard = '<div class="nx-card nx-payment-admin-card"><div class="nx-card-head"><div><div class="nx-card-title">Payment Rail</div><div class="nx-card-sub">Set the exact UPI destination that user QR codes should credit.</div></div><span class="nx-badge '+((paymentProfile.upi_id && paymentProfile.enabled)?'good':'warn')+'">'+((paymentProfile.upi_id && paymentProfile.enabled)?'Live UPI rail':'Setup needed')+'</span></div><div class="nx-card-body"><div class="nx-split"><div><div class="nx-item nx-pay-destination"><div class="nx-item-title">Settlement destination</div><div class="nx-item-sub">Save this once. Every new local payment QR in the user panel will point to this UPI ID and payee.</div><div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Payee Name<input id="nx-admin-upi-payee-name" class="nx-input" placeholder="STOCKR.IN" value="'+escapeHtml(paymentProfile.payee_name || '')+'"></label><label class="nx-form-label">UPI ID<input id="nx-admin-upi-id" class="nx-input" placeholder="stockrin@upi" value="'+escapeHtml(paymentProfile.upi_id || '')+'"></label></div><div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Merchant Code<input id="nx-admin-upi-merchant-code" class="nx-input" placeholder="Optional merchant code" value="'+escapeHtml(paymentProfile.merchant_code || '')+'"></label><label class="nx-form-label">Support Phone<input id="nx-admin-upi-support-phone" class="nx-input" placeholder="+91 9876543210" value="'+escapeHtml(paymentProfile.support_phone || '')+'"></label></div><div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Support Email<input id="nx-admin-upi-support-email" class="nx-input" placeholder="payments@stockr.in" value="'+escapeHtml(paymentProfile.support_email || '')+'"></label><label class="nx-form-label">Accent Color<input id="nx-admin-upi-theme" class="nx-input" placeholder="#5ec8ff" value="'+escapeHtml(paymentProfile.theme_color || '#5ec8ff')+'"></label></div><label class="nx-form-label" style="margin-top:12px">Instructions<textarea id="nx-admin-upi-instructions" class="nx-textarea" placeholder="Explain how the payment should be confirmed.">'+escapeHtml(paymentProfile.instructions || '')+'</textarea></label><div class="nx-toggle-grid" style="margin-top:12px"><label class="nx-check"><input id="nx-admin-upi-enabled" type="checkbox"'+checkedAttr(!!paymentProfile.enabled)+'><span>Enable direct UPI rail</span></label><div class="nx-pay-chip"><span>Mode</span><strong>'+(paymentProfile.can_auto_confirm ? 'Webhook-ready' : 'Direct UPI')+'</strong></div><div class="nx-pay-chip"><span>Status</span><strong>'+(paymentProfile.upi_id ? 'Configured' : 'Pending')+'</strong></div><div class="nx-pay-chip"><span>Stored UPI</span><strong>'+escapeHtml(paymentProfile.upi_id || 'Not saved')+'</strong></div></div><div class="nx-actions" style="margin-top:12px"><button class="nx-btn nx-btn-primary" onclick="nxAdminSavePaymentProfile()">Save Payment Rail</button><button class="nx-btn nx-btn-ghost" onclick="nxCopyText(\''+escapeHtml(paymentProfile.upi_id || '')+'\', \'UPI ID copied\')">Copy UPI ID</button></div><div class="nx-status info" style="margin-top:12px">'+(paymentProfile.can_auto_confirm ? 'Razorpay + webhook can auto-confirm gateway payments. Direct UPI QR still points to this exact destination.' : 'Direct UPI QR will send money to this saved UPI ID exactly. Payment status still needs in-app confirmation unless a gateway webhook is enabled.')+'</div></div></div><div><div class="nx-pay-panel nx-pay-preview"><div class="nx-pay-head"><div><div class="nx-item-title">Live preview</div><div class="nx-item-sub">This is what the user payment rail will use for new order QR codes.</div></div><span class="nx-badge cool">'+escapeHtml(paymentProfile.payee_name || 'Payee')+'</span></div><div class="nx-pay-bits"><div class="nx-pay-chip"><span>UPI ID</span><strong>'+escapeHtml(paymentProfile.upi_id || 'Not set')+'</strong></div><div class="nx-pay-chip"><span>Support</span><strong>'+escapeHtml([paymentProfile.support_phone, paymentProfile.support_email].filter(Boolean).join(' · ') || 'Not set')+'</strong></div><div class="nx-pay-chip"><span>Merchant</span><strong>'+escapeHtml(paymentProfile.merchant_code || 'Optional')+'</strong></div><div class="nx-pay-chip"><span>Brand</span><strong>'+escapeHtml((NX.boot||{}).brand || 'STOCKR.IN')+'</strong></div></div><div class="nx-inline-note" style="margin-top:12px">'+escapeHtml(paymentProfile.instructions || 'Instructions will appear to the user here.')+'</div></div></div></div></div>';
    const registryCards = filteredUsers.length ? filteredUsers.map(function(u){
      const selected = selectedUser && Number(selectedUser.id) === Number(u.id);
      const roleBadge = String(u.role||'').toUpperCase()==='ADMIN' ? 'warn' : 'cool';
      const statusBadge = String(u.status||'').toUpperCase()==='ACTIVE' ? 'good' : (String(u.status||'').toUpperCase()==='DISABLED' ? 'bad' : 'warn');
      return '<button class="nx-admin-registry-card'+(selected?' on':'')+'" onclick="nxAdminSelectUser('+u.id+')"><div class="nx-admin-registry-main"><div class="nx-admin-registry-name">'+escapeHtml(u.full_name || u.email)+'</div><div class="nx-admin-registry-email">'+escapeHtml(u.email)+'</div><div class="nx-admin-registry-meta">Last login '+escapeHtml(fmtDate(u.last_login_at))+' · Updated '+escapeHtml(fmtDate(u.updated_at))+'</div></div><div class="nx-admin-registry-side"><span class="nx-badge '+roleBadge+'">'+escapeHtml(u.role)+'</span><span class="nx-badge '+statusBadge+'">'+escapeHtml(u.status)+'</span><strong class="nx-admin-wallet-value">'+fmtMoney((((u.wallet||{}).balance)||0))+'</strong></div></button>';
    }).join('') : '<div class="nx-empty">No users match this search yet.</div>';
    const detailPanel = selectedUser ? (
      '<div class="nx-admin-studio-panel nx-admin-detail-panel">'
      + '<div class="nx-admin-detail-top"><div><div class="nx-admin-studio-kicker">Selected User</div><div class="nx-admin-studio-title">'+escapeHtml(selectedUser.full_name || selectedUser.email)+'</div><div class="nx-admin-studio-sub">Operate this account from one control panel.</div></div><div class="nx-pill-row"><span class="nx-pill">'+escapeHtml(selectedUser.role || 'USER')+'</span><span class="nx-pill">'+escapeHtml(selectedUser.status || 'ACTIVE')+'</span><span class="nx-pill">'+fmtMoney(selectedWallet.balance || 0)+'</span></div></div>'
      + '<div class="nx-admin-spotlight-grid"><div class="nx-pay-chip"><span>Email</span><strong>'+escapeHtml(selectedUser.email || '')+'</strong></div><div class="nx-pay-chip"><span>Wallet</span><strong>'+escapeHtml(selectedWallet.type || 'COUPON')+' · '+fmtMoney(selectedWallet.balance || 0)+'</strong></div><div class="nx-pay-chip"><span>Notifications</span><strong>'+escapeHtml((selectedNotifications.email?'Email ':'')+(selectedNotifications.telegram?'Telegram ':'')+(selectedNotifications.whatsapp?'WhatsApp ':'') || 'Off')+'</strong></div><div class="nx-pay-chip"><span>Token Reminders</span><strong>'+(selectedNotifications.token_reminder?'Enabled':'Disabled')+'</strong></div></div>'
      + '<div class="nx-form-grid nx-admin-studio-grid" style="margin-top:16px"><label class="nx-form-label">Full Name<input id="nx-admin-edit-name" class="nx-input nx-admin-input" value="'+escapeHtml(selectedUser.full_name || '')+'"></label><label class="nx-form-label">Email<input id="nx-admin-edit-email" class="nx-input nx-admin-input" value="'+escapeHtml(selectedUser.email || '')+'"></label></div>'
      + '<div class="nx-form-grid nx-admin-studio-grid" style="margin-top:12px"><label class="nx-form-label">Status<select id="nx-admin-edit-status" class="nx-select nx-admin-input"><option'+(String(selectedUser.status||'').toUpperCase()==='ACTIVE'?' selected':'')+'>ACTIVE</option><option'+(String(selectedUser.status||'').toUpperCase()==='LIMITED'?' selected':'')+'>LIMITED</option><option'+(String(selectedUser.status||'').toUpperCase()==='DISABLED'?' selected':'')+'>DISABLED</option><option'+(String(selectedUser.status||'').toUpperCase()==='ARCHIVED'?' selected':'')+'>ARCHIVED</option></select></label><label class="nx-form-label">Role<select id="nx-admin-edit-role" class="nx-select nx-admin-input"><option'+(String(selectedUser.role||'').toUpperCase()==='USER'?' selected':'')+'>USER</option><option'+(String(selectedUser.role||'').toUpperCase()==='ADMIN'?' selected':'')+'>ADMIN</option></select></label></div>'
      + '<div class="nx-form-grid nx-admin-studio-grid" style="margin-top:12px"><label class="nx-form-label">Wallet Type<select id="nx-admin-edit-wallet-type" class="nx-select nx-admin-input"><option'+(String(selectedWallet.type||'').toUpperCase()==='COUPON'?' selected':'')+'>COUPON</option><option'+(String(selectedWallet.type||'').toUpperCase()==='PAID'?' selected':'')+'>PAID</option></select></label><label class="nx-form-label">WhatsApp<input id="nx-admin-edit-whatsapp" class="nx-input nx-admin-input" value="'+escapeHtml(selectedContacts.whatsapp_phone || '')+'" placeholder="+91 9876543210"></label></div>'
      + '<div class="nx-form-grid nx-admin-studio-grid" style="margin-top:12px"><label class="nx-form-label">Telegram<input id="nx-admin-edit-telegram" class="nx-input nx-admin-input" value="'+escapeHtml(selectedContacts.telegram_chat_id || '')+'" placeholder="123456789"></label><label class="nx-form-label">Internal Notes<textarea id="nx-admin-edit-notes" class="nx-textarea nx-admin-input" placeholder="Client context, support notes, onboarding notes">'+escapeHtml(selectedUser.notes || '')+'</textarea></label></div>'
      + '<div class="nx-form-grid-3 nx-admin-studio-grid" style="margin-top:12px"><label class="nx-form-label">Daily Loss<input id="nx-admin-edit-daily-loss" class="nx-input nx-admin-input" value="'+escapeHtml(String(selectedControls.daily_loss_limit || 0))+'"></label><label class="nx-form-label">Max Trades<input id="nx-admin-edit-max-trades" class="nx-input nx-admin-input" value="'+escapeHtml(String(selectedControls.max_trades_per_day || 0))+'"></label><label class="nx-form-label">Open Signals<input id="nx-admin-edit-max-open" class="nx-input nx-admin-input" value="'+escapeHtml(String(selectedControls.max_open_signals || 0))+'"></label></div>'
      + '<div class="nx-admin-mail-strip"><div class="nx-admin-mail-copy"><div class="nx-admin-mail-title">Mailer Actions</div><div class="nx-admin-mail-sub">'+(gmailReady ? 'Gmail is connected. You can send onboarding, summary, wallet, and reset-password mail from here.' : 'Gmail is not configured yet. Design is ready, but mail sends require backend Gmail credentials.')+'</div></div><label class="nx-check"><input id="nx-admin-edit-send-email" type="checkbox"'+checkedAttr(gmailReady)+' '+(gmailReady?'':'disabled')+'><span>Send email with actions</span></label></div>'
      + '<div class="nx-actions" style="margin-top:16px"><button class="nx-btn nx-btn-primary" onclick="nxAdminSaveUser('+selectedUser.id+')">Save & Send Summary</button><button class="nx-btn nx-btn-gold" onclick="nxAdminResetUserPassword('+selectedUser.id+')">Reset Password</button><button class="nx-btn nx-btn-ghost" onclick="nxAdminCredit('+selectedUser.id+')">Credit / Debit</button><button class="nx-btn nx-btn-ghost" onclick="nxAdminSendSummary('+selectedUser.id+')">Send Account Mail</button><button class="nx-btn nx-btn-ghost" onclick="nxAdminArchiveRestoreUser('+selectedUser.id+')">'+(String(selectedUser.status||'').toUpperCase()==='ARCHIVED'?'Restore User':'Archive User')+'</button></div>'
      + '</div>'
    ) : '<div class="nx-admin-studio-panel"><div class="nx-empty">Select a user to open the full account studio.</div></div>';
    const createPanel = '<div class="nx-admin-studio-panel nx-admin-create-panel-hero"><div class="nx-admin-detail-top"><div><div class="nx-admin-studio-kicker">Create User</div><div class="nx-admin-studio-title">Quick Provision</div><div class="nx-admin-studio-sub">Minimal form. Instant onboarding.</div></div><span class="nx-badge '+(gmailReady?'good':'warn')+'">'+(gmailReady?'Mailer live':'Mailer pending')+'</span></div><div class="nx-form-grid nx-admin-studio-grid" style="margin-top:16px"><label class="nx-form-label">Name<input id="nx-admin-user-name" class="nx-input nx-admin-input" placeholder="Desk User" autocomplete="off"></label><label class="nx-form-label">Email<input id="nx-admin-user-email" class="nx-input nx-admin-input" placeholder="desk@client.com" autocomplete="off"></label></div><div class="nx-form-grid nx-admin-studio-grid" style="margin-top:12px"><label class="nx-form-label">Temporary Password<input id="nx-admin-user-password" type="password" class="nx-input nx-admin-input" placeholder="Welcome@123" autocomplete="new-password"></label><label class="nx-form-label">Role<select id="nx-admin-user-role" class="nx-select nx-admin-input"><option>USER</option><option>ADMIN</option></select></label></div><div class="nx-admin-mail-strip"><div class="nx-admin-mail-copy"><div class="nx-admin-mail-title">Onboarding Mail</div><div class="nx-admin-mail-sub">Send credentials on create.</div></div><label class="nx-check"><input id="nx-admin-user-send-email" type="checkbox"'+checkedAttr(gmailReady)+' '+(gmailReady?'':'disabled')+'><span>Send now</span></label></div><div class="nx-actions" style="margin-top:16px"><button class="nx-btn nx-btn-primary nx-btn-wide" onclick="nxAdminCreateUser()">Create Desk</button></div></div>';
    const usersCard = '<div class="nx-card nx-admin-studio-shell"><div class="nx-card-head"><div><div class="nx-card-title">User Management</div><div class="nx-card-sub">Clean operations console.</div></div><div class="nx-pill-row"><span class="nx-pill">Accounts '+fmtNum(users.length)+'</span><span class="nx-pill">'+(gmailReady?'Gmail Live':'Gmail Pending')+'</span></div></div><div class="nx-card-body"><div class="nx-admin-kpi-row"><div class="nx-admin-kpi"><div class="nx-admin-kpi-label">Users</div><div class="nx-admin-kpi-value">'+fmtNum((a.metrics||{}).total_users||0)+'</div></div><div class="nx-admin-kpi"><div class="nx-admin-kpi-label">Active</div><div class="nx-admin-kpi-value">'+fmtNum((a.metrics||{}).active_users||0)+'</div></div><div class="nx-admin-kpi"><div class="nx-admin-kpi-label">Revenue</div><div class="nx-admin-kpi-value">'+fmtMoney((a.metrics||{}).revenue||0)+'</div></div><div class="nx-admin-kpi nx-admin-kpi-search"><label class="nx-form-label">Search<input id="nx-admin-user-search" class="nx-input nx-admin-input" placeholder="Search users..." value="'+escapeHtml(NX.adminUserQuery || '')+'" oninput="nxAdminSetUserSearch(this.value)"></label></div></div><div class="nx-admin-v2-layout"><div class="nx-admin-v2-left"><div class="nx-admin-studio-panel nx-admin-registry-shell"><div class="nx-admin-detail-top"><div><div class="nx-admin-studio-kicker">Registry</div><div class="nx-admin-studio-title">Live Users</div><div class="nx-admin-studio-sub">Tap a user to open controls.</div></div><div class="nx-pay-chip"><span>Visible</span><strong>'+fmtNum(filteredUsers.length)+'</strong></div></div><div class="nx-admin-registry-list">'+registryCards+'</div></div></div><div class="nx-admin-v2-right">'+createPanel+detailPanel+'</div></div></div></div>';
    const routingCard = '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Strategies & Routing</div><div class="nx-card-sub">Turn engine streams on or off at the entitlement layer</div></div></div><div class="nx-card-body"><div class="nx-strategy-grid">'+stratCards+'</div></div></div>';
    const couponsCard = '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Coupons & Payments</div><div class="nx-card-sub">Bootstrap offers, manual finance operations, and live webhook-ready orders</div></div></div><div class="nx-card-body"><div class="nx-admin-grid"><div><div class="nx-item"><div class="nx-item-title">Create coupon</div><div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Code<input id="nx-admin-coupon-code" class="nx-input" placeholder="DESK1000"></label><label class="nx-form-label">Credit<input id="nx-admin-coupon-credit" class="nx-input" placeholder="1000"></label></div><div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Max Profit<input id="nx-admin-coupon-cap" class="nx-input" placeholder="2000"></label><label class="nx-form-label">Usage Limit<input id="nx-admin-coupon-limit" class="nx-input" placeholder="25"></label></div><div class="nx-actions" style="margin-top:12px"><button class="nx-btn nx-btn-gold" onclick="nxAdminCreateCoupon()">Create Coupon</button></div></div><div class="nx-item" style="margin-top:12px">'+coupons+'</div></div><div><div class="nx-item">'+payments+'</div></div></div></div></div>';
    const signalsCard = '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Recent Routed Signals</div><div class="nx-card-sub">Last delivered inbox events across all subscribed users</div></div></div><div class="nx-card-body">'+signals+'</div></div>';
    const tab = currentWorkspaceTab();
    let body = overviewCard;
    if(tab === 'users') body = usersCard;
    else if(tab === 'routing') body = routingCard;
    else if(tab === 'payments') body = paymentRailCard + couponsCard;
    else if(tab === 'brand') body = brandCard;
    else if(tab === 'signals') body = signalsCard;
    return renderWorkspaceShell({
      tabs: workspaceTabs(),
      railKicker: 'Admin workspace',
      railTitle: (NX.user || {}).full_name || 'Admin',
      railSub: 'Each operational surface has its own lane, so user, routing, payment, and brand actions stay separated.',
      railCards: [
        '<div class="nx-workspace-rail-card nx-workspace-rail-metric"><div class="nx-workspace-rail-kicker">Users</div><div class="nx-workspace-rail-title">'+fmtNum((a.metrics||{}).total_users||0)+'</div><div class="nx-workspace-rail-sub">Active '+fmtNum((a.metrics||{}).active_users||0)+' · Gmail '+(gmailReady?'Live':'Pending')+'</div></div>',
        '<div class="nx-workspace-rail-card nx-workspace-rail-metric"><div class="nx-workspace-rail-kicker">Payments</div><div class="nx-workspace-rail-title">'+((paymentProfile.upi_id && paymentProfile.enabled)?'UPI Live':'Setup')+'</div><div class="nx-workspace-rail-sub">'+escapeHtml(paymentProfile.upi_id || 'No UPI rail saved')+'</div></div>'
      ],
      headRight: '<div class="nx-pill-row"><span class="nx-pill">Revenue '+fmtMoney((a.metrics||{}).revenue||0)+'</span><span class="nx-pill">'+(gmailReady ? 'Gmail Live' : 'Gmail Pending')+'</span></div>',
      body: body
    });
  }

  function renderBody(){
    if(NX.loading && !NX.ready) return '<div class="nx-card"><div class="nx-card-body" style="padding:28px"><div class="nx-empty">Loading control hub...</div></div></div>';
    if(!NX.user){
      if(NX_ADMIN_ONLY) return '<div class="nx-public-layout"><div>'+renderAuthCard()+'</div></div>';
      return '<div class="nx-public-layout"><div>'+renderAuthCard()+'</div>'+renderPublicSideCards()+'</div>';
    }
    ensureWorkspaceTab();
    return NX.user.role === 'ADMIN' ? renderAdminCards() : renderUserCards();
  }

  function renderAll(){
    const mount = el('nx-root');
    if(!mount) return;
    var hero = '';
    var body = '';
    try{
      hero = renderHero();
    }catch(err){
      console.error('Nexus renderHero failed', err);
      hero = '<div class="nx-hero"><div class="nx-status error" style="padding:16px">Nexus header failed to render: '+escapeHtml(String((err && err.message) || err || 'error'))+'</div></div>';
    }
    try{
      body = renderBody();
    }catch(err){
      console.error('Nexus renderBody failed', err);
      body = '<div class="nx-card"><div class="nx-card-body"><div class="nx-status error"><b>Workspace panel failed to render.</b></div><div class="nx-inline-note" style="margin-top:10px;word-break:break-word;font-size:13px">'+escapeHtml(String((err && err.message) || err || 'error'))+'</div><div class="nx-inline-note" style="margin-top:10px">Open DevTools (F12) → Console for the stack trace. APIs may still be running on the server.</div><div class="nx-actions" style="margin-top:14px"><button type="button" class="nx-btn nx-btn-primary" onclick="window.nxRefresh&&window.nxRefresh()">Reload desk data</button><button type="button" class="nx-btn nx-btn-ghost" onclick="location.reload()">Hard refresh page</button></div></div></div>';
    }
    mount.innerHTML = hero + body;
  }

  function safeRender(){
    const mount = el('nx-root');
    if(!mount) return;
    try{
      renderAll();
    }catch(err){
      mount.innerHTML = '<div style="padding:24px;color:#e5eefc;font-family:Aptos,Segoe UI,sans-serif"><div style="font-size:22px;font-weight:800;margin-bottom:8px">Nexus Control Hub</div><div style="font-size:14px;line-height:1.6;color:#b8c8e6">The panel hit a render issue while updating the page. Try <b>Reload desk data</b> below, or close Nexus and hard-refresh the browser (Ctrl+F5).</div><div style="margin-top:14px;padding:12px;border-radius:10px;background:rgba(255,80,80,.12);color:#ffd6d6;font-size:13px;word-break:break-word;font-family:ui-monospace,Consolas,monospace">'+escapeHtml(String((err && err.message) || err || 'unknown error'))+'</div><div style="margin-top:16px;display:flex;gap:10px;flex-wrap:wrap"><button type="button" class="nx-btn nx-btn-primary" style="cursor:pointer" onclick="window.nxRefresh&&window.nxRefresh()">Reload desk data</button><button type="button" class="nx-btn nx-btn-ghost" style="cursor:pointer" onclick="location.reload()">Hard refresh</button></div></div>';
      console.error('Nexus render failed', err);
    }
  }

  async function runBusy(key, work){
    NX.loadingAction = key || '';
    safeRender();
    try{
      return await work();
    }finally{
      NX.loadingAction = '';
      safeRender();
    }
  }

  async function authFlow(url, payload){
    clearNotice();
    const data = await nxApi(url, { method:'POST', body: JSON.stringify(payload) });
    remember(data.token, (data.user||{}).role || 'USER');
    NX.user = data.user;
    NX.ready = false;
    setNotice('success', 'Login successful. Loading your workspace...');
    await hydrate();
    nxConsumeDeferredKiteOAuthPayload();
    toast('Welcome to Login Nexus');
  }

  window.nxSetWorkspaceTab = function(tab){
    NX.workspaceTab = String(tab || '').trim().toLowerCase();
    localStorage.setItem(workspaceTabStoreKey(), NX.workspaceTab);
    safeRender();
    if(NX.workspaceTab === 'execution'){
      setTimeout(function(){ if(window.nxBrokerCodeSync) window.nxBrokerCodeSync(); }, 0);
    }
  };
  window.nxSwitchAuth = function(mode){ NX.authMode = mode; clearNotice(); safeRender(); };
  window.nxGooglePreview = function(){
    NX.authMode = 'login';
    safeRender();
    setTimeout(function(){
      var em = el('nx-login-email');
      if(em){ em.focus(); em.select && em.select(); }
      toast('Use your Gmail in Email field and login with password. OAuth route can be added next.');
    }, 40);
  };
  window.nxLogin = function(){
    const email = normalizeEmail((el('nx-login-email')||{}).value);
    const password = String((el('nx-login-password')||{}).value || '').trim();
    return runBusy('login', function(){
      return authFlow('/api/auth/login', { email: email, password: password }).catch(function(err){
        setNotice('error', err.message || 'Login failed');
        toast(err.message || 'Login failed');
      });
    });
  };
  window.nxSignup = function(){
    const email = normalizeEmail((el('nx-signup-email')||{}).value);
    const password = String((el('nx-signup-password')||{}).value || '').trim();
    const fullName = String((el('nx-signup-name')||{}).value || '').trim();
    const whatsappPhone = String((el('nx-signup-whatsapp')||{}).value || '').trim();
    const telegramChatId = String((el('nx-signup-telegram')||{}).value || '').trim();
    const notifyEmail = !!((el('nx-signup-notify-email')||{}).checked);
    const notifyTelegram = !!((el('nx-signup-notify-telegram')||{}).checked);
    const notifyWhatsapp = !!((el('nx-signup-notify-whatsapp')||{}).checked);
    const notifyTokenReminder = !!((el('nx-signup-token-reminder')||{}).checked);
    return runBusy('signup', function(){
      return authFlow('/api/auth/signup', { full_name: fullName, email: email, password: password, whatsapp_phone: whatsappPhone, telegram_chat_id: telegramChatId, notify_email: notifyEmail, notify_telegram: notifyTelegram, notify_whatsapp: notifyWhatsapp, notify_token_reminder: notifyTokenReminder }).catch(function(err){
        setNotice('error', err.message || 'Signup failed');
        toast(err.message || 'Signup failed');
      });
    });
  };
  window.nxAdminLogin = function(){
    const email = normalizeEmail((el('nx-admin-email')||{}).value || defaultAdminEmail());
    const password = String((el('nx-admin-password')||{}).value || '').trim();
    return runBusy('admin-login', function(){
      return authFlow('/api/admin/login', { email: email, password: password }).catch(function(err){
        const fallback = 'Admin login failed. Use ' + defaultAdminEmail() + ' or check the password.';
        setNotice('error', err.message || fallback);
        toast(err.message || fallback);
      });
    });
  };
  window.nxLogout = function(){
    return (async function(){
      if(NX.token){
        try{ await nxApi('/api/auth/logout', { method:'POST' }); }catch(_){ /* session already gone */ }
      }
      logout();
      toast('Logged out');
    })();
  };
  window.nxRefresh = function(){ hydrate(); };
  window.nxMarkSignalRead = async function(id){ try{ await nxApi('/api/user/signals/'+id+'/read', { method:'POST' }); await loadUserData(); safeRender(); }catch(err){ toast(err.message); } };
  window.nxToggleStrategy = async function(code, enabled){ try{ await nxApi('/api/user/strategies/'+code, { method:'PATCH', body: JSON.stringify({ enabled: !!enabled }) }); await loadUserData(); safeRender(); toast('Strategy updated'); }catch(err){ toast(err.message); } };
  window.nxRedeemCoupon = async function(){ try{ await nxApi('/api/user/wallet/redeem-coupon', { method:'POST', body: JSON.stringify({ code:(el('nx-coupon-code')||{}).value }) }); await loadUserData(); safeRender(); toast('Coupon applied'); }catch(err){ toast(err.message); } };
  window.nxCreatePayment = async function(plan){
    try{
      const data = await nxApi('/api/user/payments/create', { method:'POST', body: JSON.stringify({ plan_code: plan }) });
      NX.selectedPaymentId = (((data||{}).order||{}).id || NX.selectedPaymentId);
      await loadUserData();
      safeRender();
      const qp = (((data||{}).order||{}).qr_payload || '');
      toast(qp ? 'Scanner and UPI details are ready' : 'Order created');
    }catch(err){ toast(err.message); }
  };
  window.nxCreateCustomPayment = async function(){
    const amountRaw = window.prompt('Enter custom payment amount (minimum ₹10)', '10');
    if(amountRaw === null) return;
    const amount = Number(amountRaw || 0);
    if(!(amount >= 10)){ toast('Minimum custom payment is ₹10'); return; }
    try{
      const data = await nxApi('/api/user/payments/create', { method:'POST', body: JSON.stringify({ plan_code:'CUSTOM', amount: amount }) });
      NX.selectedPaymentId = (((data||{}).order||{}).id || NX.selectedPaymentId);
      await loadUserData();
      safeRender();
      toast('Custom payment order created');
    }catch(err){ toast(err.message); }
  };
  window.nxCreateTrade = async function(){ try{ await nxApi('/api/user/trades', { method:'POST', body: JSON.stringify({ symbol:(el('nx-trade-symbol')||{}).value, strategy_code:(el('nx-trade-strategy')||{}).value, entry_price:(el('nx-trade-entry')||{}).value, exit_price:(el('nx-trade-exit')||{}).value, pnl:(el('nx-trade-pnl')||{}).value, status:'CLOSED' }) }); await loadUserData(); safeRender(); toast('Trade journal updated'); }catch(err){ toast(err.message); } };
  function brokerPayload(){
    const code = String(((el('nx-broker-code')||{}).value || 'PAPER')).toUpperCase();
    const live = !!((el('nx-broker-live-mode')||{}).checked) && code !== 'PAPER';
    const pasteEl = el('nx-broker-request-paste');
    const secEl = el('nx-broker-api-secret');
    return {
      broker_code: code,
      account_label: String(((el('nx-broker-label')||{}).value || '')).trim(),
      broker_user_id: String(((el('nx-broker-user-id')||{}).value || '')).trim(),
      api_key: String(((el('nx-broker-api-key')||{}).value || '')).trim(),
      api_secret: secEl ? String(secEl.value || '').trim() : '',
      request_token: pasteEl ? String(pasteEl.value || '').trim() : '',
      access_token: String(((el('nx-broker-access-token')||{}).value || '')).trim(),
      enabled: !!((el('nx-broker-enabled')||{}).checked),
      paper_mode: !!((el('nx-broker-paper-mode')||{}).checked) || code === 'PAPER',
      live_mode: live,
      default_quantity: Number(((el('nx-broker-qty')||{}).value || 1)),
      intraday_product: String(((el('nx-broker-intraday-product')||{}).value || 'MIS')).trim(),
      positional_product: String(((el('nx-broker-positional-product')||{}).value || 'CNC')).trim(),
      auto_execute: !!((el('nx-user-auto-execute')||{}).checked)
    };
  }
  window.nxBrokerCodeSync = function(){
    safeRender();
  };
  function stopKiteInteractivePoll(){
    if(window._nxKiteInteractiveTimer){
      clearTimeout(window._nxKiteInteractiveTimer);
      window._nxKiteInteractiveTimer = null;
    }
  }
  async function pollKiteInteractiveStatus(opts){
    opts = opts || {};
    try{
      const data = await nxApi('/api/user/broker/kite-interactive-status', { method:'GET' });
      const info = (data || {}).interactive || {};
      const broker = (data || {}).broker || {};
      const status = String(info.status || 'IDLE').toUpperCase();
      if(info.message){
        const waiting = !!(info.active || status === 'WAITING' || status === 'STARTING');
        NX.brokerAssist = {
          type: status === 'CONNECTED' ? 'success' : (status === 'ERROR' ? 'error' : (status === 'CANCELLED' || status === 'TIMEOUT' ? 'info' : 'cool')),
          title: status === 'CONNECTED' ? 'One-time Kite login completed' : (status === 'WAITING' ? 'Complete login in the Kite window' : (status === 'STARTING' ? 'Opening Kite login' : (status === 'CANCELLED' ? 'Kite login was closed' : (status === 'TIMEOUT' ? 'Kite login timed out' : 'Kite login status')))),
          message: String(info.message || ''),
          detail: String(info.detail || ''),
          waitHint: waiting ? 'Please wait until Nexus confirms the connection. Keep this tab open and avoid opening extra Kite tabs or clicking Open Kite repeatedly.' : ''
        };
      }
      if(status === 'CONNECTED'){
        NX.brokerTest = {
          type:'success',
          message:'Broker connected through one-time Kite login',
          detail:[((broker.profile||{}).name || ''), ((broker.profile||{}).user_id ? ('ID ' + (broker.profile||{}).user_id) : ''), (broker.broker_name || '')].filter(Boolean).join(' · ')
        };
        stopKiteInteractivePoll();
        await loadUserData();
        safeRender();
        if(!opts.silent) toast('One-time Kite login completed');
        return info;
      }
      if(info.active || status === 'WAITING' || status === 'STARTING'){
        safeRender();
        stopKiteInteractivePoll();
        window._nxKiteInteractiveTimer = setTimeout(function(){ pollKiteInteractiveStatus({ silent:true }); }, 2000);
        return info;
      }
      stopKiteInteractivePoll();
      safeRender();
      if(!opts.silent && (status === 'CANCELLED' || status === 'TIMEOUT' || status === 'ERROR')){
        toast(String(info.message || 'Kite login did not complete'));
      }
      return info;
    }catch(err){
      stopKiteInteractivePoll();
      NX.brokerAssist = { type:'error', title:'Kite login status failed', message: err.message || 'Unable to read Kite login status' };
      safeRender();
      if(!opts.silent) toast(err.message || 'Unable to read Kite login status');
      throw err;
    }
  }
  window.nxKiteInteractiveLogin = function(){
    return runBusy('broker-interactive-start', async function(){
      try{
        stopKiteInteractivePoll();
        const payload = brokerPayload();
        payload.broker_code = 'ZERODHA';
        payload.paper_mode = false;
        const data = await nxApi('/api/user/broker/kite-interactive-start', { method:'POST', body: JSON.stringify(payload) });
        const info = (data || {}).interactive || {};
        if(info.session_reused){
          toast('One-time Kite login is already running — use that browser window. Avoid clicking again (prevents extra windows).');
        }
        NX.brokerAssist = {
          type:'cool',
          title:'Official Kite window opened',
          message:String(info.message || 'Complete login in the Kite window. Nexus will finish the connection automatically.'),
          detail:(info.session_reused ? 'A session was already running — no second browser was started. ' : '') + String(info.detail || ''),
          waitHint: 'Please wait until Nexus confirms the connection. Keep this tab open; do not start another one-time login until this finishes.'
        };
        safeRender();
        if(!info.session_reused) toast('Kite login opened. Finish it in the browser window.');
        window._nxKiteInteractiveTimer = setTimeout(function(){ pollKiteInteractiveStatus({ silent:true }); }, 1500);
      }catch(err){
        NX.brokerAssist = { type:'error', title:'Could not open Kite login', message: err.message || 'Interactive Kite login failed to start' };
        safeRender();
        toast(err.message || 'Interactive Kite login failed to start');
      }
    });
  };
  window.nxKiteClosePopup = function(){
    nxCloseKitePopupRef();
    safeRender();
    toast('Kite popup closed — paste redirect URL here if Zerodha showed it, then Capture Token.');
  };
  window.nxSendKiteMobileLoginLink = function(){
    return runBusy('broker-mobile-link', async function(){
      try{
        const data = await nxApi('/api/user/broker/mobile-login-link', { method:'POST', body: JSON.stringify({}) });
        const link = String((data || {}).mobile_login_url || '').trim();
        NX.brokerAssist = {
          type:'cool',
          title:'Telegram login link sent',
          message:'Open the Telegram message on your mobile and finish Kite login there.',
          detail: link ? ('Mobile link: ' + link) : '',
          waitHint:'After mobile login succeeds, the session is saved directly on the server.'
        };
        safeRender();
        toast('Telegram mobile login link sent');
      }catch(err){
        NX.brokerAssist = { type:'error', title:'Could not send Telegram login link', message: err.message || 'Telegram send failed', detail:'', waitHint:'' };
        safeRender();
        toast(err.message || 'Telegram send failed');
      }
    });
  };
  window.nxBrokerDisconnect = function(){
    return runBusy('broker-disconnect', async function(){
      try{
        await nxApi('/api/user/broker/disconnect', { method:'POST', body: JSON.stringify({}) });
        NX.brokerAssist = {
          type:'info',
          title:'Session closed',
          message:'The saved broker session was removed from this desk.',
          detail:'Use Login with Zerodha again when you want to reconnect.',
          waitHint:''
        };
        await loadUserData();
        safeRender();
        toast('Broker session closed');
      }catch(err){
        NX.brokerAssist = { type:'error', title:'Could not close session', message: err.message || 'Disconnect failed', detail:'', waitHint:'' };
        safeRender();
        toast(err.message || 'Disconnect failed');
      }
    });
  };
  function nxRunKiteOAuthHandshake(d){
    d = d || {};
    if(d.type !== 'stockr_kite_oauth') return;
    const rt = String(d.request_token || '').trim();
    const dedupeKey = rt || String(d.redirect_url || '').trim();
    const now = Date.now();
    if(dedupeKey && window._nxKiteOAuthDedupeKey === dedupeKey && (now - (window._nxKiteOAuthDedupeAt || 0)) < 90000){
      return;
    }
    if(dedupeKey){
      window._nxKiteOAuthDedupeKey = dedupeKey;
      window._nxKiteOAuthDedupeAt = now;
    }
    const st = String(d.status || '').trim().toLowerCase();
    if(st && st !== 'success'){
      nxCloseKitePopupRef();
      safeRender();
      toast('Kite login was not successful (' + String(d.status || '') + ').');
      return;
    }
    if(!rt){
      nxCloseKitePopupRef();
      safeRender();
      toast('No request_token in redirect — set Kite app redirect URL to ' + nxKiteOAuthReturnAbs());
      return;
    }
    if(!NX.token){
      nxStorePendingKiteOAuth({
        type:'stockr_kite_oauth',
        request_token: rt,
        redirect_url: String(d.redirect_url || '').trim(),
        status: String(d.status || '').trim(),
        action: String(d.action || '').trim()
      });
      NX.brokerAssist = {
        type:'info',
        title:'Kite login successful',
        message:'Please login to Nexus to finish token exchange automatically.',
        detail:'Your request token is saved in this browser and will be processed right after login.',
        waitHint:''
      };
      safeRender();
      toast('Kite login captured. Sign in to Nexus to complete broker connect.');
      return;
    }
    const paste = el('nx-broker-request-paste');
    const href = String(d.redirect_url || '').trim();
    if(paste){
      paste.value = href || ('https://127.0.0.1/?request_token=' + encodeURIComponent(rt) + '&action=login&status=success');
    }
    NX.brokerAssist = {
      type:'cool',
      title:'Kite login received',
      message:'Exchanging token and refreshing your desk…',
      detail:'Same-tab BroadcastChannel + popup message both work — duplicates are ignored.',
      waitHint:''
    };
    safeRender();
    return runBusy('broker-connect', async function(){
      try{
        const payload = brokerPayload();
        const data = await nxApi('/api/user/broker/connect', { method:'POST', body: JSON.stringify(payload) });
        const b = (data || {}).broker || {};
        const okConn = (b.status === 'CONNECTED' || b.status === 'READY');
        const uid = String(((b.profile || {}).user_id) || '').trim();
        const liveReady = okConn && !!(b.effective_live);
        NX.brokerAssist = okConn ? {
          type:'success',
          title: liveReady ? 'Ready to fire live trades today' : 'Kite connected — paper-safe today',
          message: (liveReady ? 'Live firing is armed for this desk. ' : 'Desk is connected; use Order firing → Live when you want real orders today. ')
            + 'API key ' + String(b.api_key_masked || '—') + ' · Kite user ' + (uid || '—') + ' · Token ' + String(b.access_token_masked || '—'),
          detail:[((b.profile || {}).name || ''), uid ? ('Session ID ' + uid) : '', (b.broker_name || '')].filter(Boolean).join(' · ') || 'Desk is ready.',
          waitHint:''
        } : {
          type:'info',
          title:'Desk saved but session not ready',
          message:String(b.last_error || 'Check API secret and Kite redirect URL.'),
          detail:'',
          waitHint:''
        };
        NX.brokerTest = {
          type: okConn ? 'success' : 'error',
          message: okConn ? 'Broker connected via Kite popup' : 'Broker save incomplete',
          detail: [((b.profile || {}).name || ''), uid ? ('ID ' + uid) : '', (b.broker_name || '')].filter(Boolean).join(' · ')
        };
        await nxApi('/api/user/controls', { method:'PATCH', body: JSON.stringify({
          auto_execute: payload.auto_execute,
          daily_loss_limit: Number(((el('nx-user-daily-loss') || {}).value || 0)),
          max_trades_per_day: Number(((el('nx-user-max-trades') || {}).value || 0)),
          max_open_signals: Number(((el('nx-user-max-open') || {}).value || 0))
        }) });
        if(okConn){
          await nxMaybeAutoSyncMainFeed(true);
        }
        await loadUserData();
        if(okConn && el('nx-broker-request-paste')) el('nx-broker-request-paste').value = '';
        nxCloseKitePopupRef();
        toast(okConn ? (liveReady ? 'Ready to fire live trades today' : 'Kite connected — session updated') : (String(b.last_error || '').trim() || 'Desk saved; check status'));
        safeRender();
        setTimeout(function(){ if(window.nxBrokerCodeSync) window.nxBrokerCodeSync(); }, 20);
      }catch(err){
        nxCloseKitePopupRef();
        window._nxKiteOAuthDedupeKey = '';
        NX.brokerAssist = { type:'error', title:'Kite exchange failed', message: err.message || 'Connection failed', detail:'', waitHint:'' };
        NX.brokerTest = { type:'error', message:'Broker not connected', detail: err.message || 'Connection failed' };
        safeRender();
        toast(err.message || 'Connection failed');
        throw err;
      }
    });
  }
  window.nxHandleKiteOAuthMessage = function(ev){
    if(!ev || ev.source !== window._nxKitePopupRef) return;
    nxRunKiteOAuthHandshake(ev.data || {});
  };
  function nxInstallKiteOAuthBroadcast(){
    if(window._nxKiteOAuthBCBound) return;
    window._nxKiteOAuthBCBound = true;
    try{
      const ch = new BroadcastChannel('stockr_kite_oauth_v1');
      window._nxKiteOAuthBC = ch;
      ch.onmessage = function(ev){
        nxRunKiteOAuthHandshake((ev && ev.data) || {});
      };
    }catch(_){ /* unsupported */ }
  }
  function nxConsumeDeferredKiteOAuthPayload(){
    var payload = null;
    try{
      var raw = localStorage.getItem('nx_kite_oauth_payload');
      if(raw) payload = JSON.parse(raw);
    }catch(_){}
    if(!payload) payload = readKiteOAuthPayloadFromPage();
    if(!payload || !payload.request_token) return;
    try{ localStorage.removeItem('nx_kite_oauth_payload'); }catch(_){}
    try{
      if(window.history && window.history.replaceState){
        var cleaned = String(window.location.pathname || '/') + String(window.location.hash || '');
        window.history.replaceState({}, document.title || '', cleaned);
      }
    }catch(_){}
    nxRunKiteOAuthHandshake(payload);
  }
  window.nxKiteOpenLogin = function(){
    const now = Date.now();
    if(window._nxKiteOpenLast && (now - window._nxKiteOpenLast) < 2800){
      toast('Please wait — Kite was just opened. Use Close Kite or wait a few seconds.');
      return;
    }
    window._nxKiteOpenLast = now;
    const key = String(((el('nx-broker-api-key')||{}).value || '')).trim();
    const dash = (NX.dashboard || {}).broker || {};
    let url = String(dash.login_url || '').trim();
    if(!url && key){
      url = 'https://kite.zerodha.com/connect/login?api_key=' + encodeURIComponent(key);
    }
    if(!url){ toast('Enter API key first (Kite Connect)'); return; }
    const wname = 'stockr_kite_login';
    let kiteWin = null;
    try{
      if(window._nxKitePopupRef && !window._nxKitePopupRef.closed){
        try{ window._nxKitePopupRef.close(); }catch(_){}
      }
      kiteWin = window.open(url, wname, nxKitePopupFeatures());
    }catch(_){ kiteWin = null; }
    if(!kiteWin){
      toast('Pop-up blocked — allow pop-ups for this site, then try again.');
      return;
    }
    try{ kiteWin.focus(); }catch(_){}
    window._nxKitePopupRef = kiteWin;
    NX.kitePopupLaunched = true;
    nxKiteStartPopupWatcher();
    NX.brokerAssist = {
      type:'cool',
      title:'Kite login (small window)',
      message:'Log in inside the popup. After a correct redirect URL (http + this app’s host/port — see orange warning), the popup closes on success and your desk shows Ready to fire… automatically.',
      detail:'Primary redirect: ' + nxKiteOAuthReturnAbs() + (nxKiteOAuthReturnAlt() ? (' · Alt: ' + nxKiteOAuthReturnAlt()) : '') + '. If you still see https://127.0.0.1 with connection refused, update Kite Connect redirect.',
      waitHint: 'Keep this Nexus tab open until you see Connected or the fire-ready banner.'
    };
    safeRender();
  };
  window.nxSyncMainFeedFromDesk = function(){
    return runBusy('broker-feed-sync', async function(){
      try{
        const data = await nxApi('/api/user/broker/sync-main-feed-token', { method:'POST', body: JSON.stringify({}) });
        NX.brokerAssist = {
          type:'success',
          title:'Trading OS feed updated',
          message:String((data || {}).msg || 'Reload the main dashboard (Ctrl+F5).'),
          detail:JSON.stringify((data || {}).kite_profile || {})
        };
        safeRender();
        toast('Trading OS now uses this desk token — refresh the main page');
      }catch(err){
        toast(err.message || 'Could not sync feed');
      }
    });
  };
  async function nxMaybeAutoSyncMainFeed(silent){
    if(!((NX.boot || {}).desk_feed_sync_enabled)) return;
    try{
      const data = await nxApi('/api/user/broker/sync-main-feed-token', { method:'POST', body: JSON.stringify({}) });
      if(!silent) toast('Main feed synced from this desk token');
      const p = (data || {}).kite_profile || {};
      NX.brokerAssist = Object.assign({}, NX.brokerAssist || {}, {
        detail: [String((NX.brokerAssist || {}).detail || '').trim(), p.user_id ? ('Main feed user ' + p.user_id) : 'Main feed synced']
          .filter(Boolean).join(' · ')
      });
    }catch(err){
      if(!silent) toast('Desk connected, but main feed sync failed: ' + (err.message || 'unknown'));
    }
  }
  window.nxCheckServerTokenStatus = function(){
    return runBusy('broker-token-status', async function(){
      if(!((NX.boot || {}).allow_user_server_session_import)){
        toast('Server session checks are disabled for user desks. Use your own Kite login.');
        return;
      }
      try{
        const data = await nxApi('/api/token-status', { method:'GET' });
        const errRaw = String(data.error || '').toLowerCase();
        const deskHint = (/api_key|access_token|incorrect/i.test(errRaw))
          ? ' This only checks backend/.env (KITE_API_KEY + KITE_ACCESS_TOKEN) for the shared engine — not your Nexus desk. A successful Kite-in-popup login can still be valid here while this shows an error. Use Test Session for this desk, or fix .env if you use “Use server session”.'
          : '';
        NX.brokerAssist = {
          type: data.valid ? 'success' : 'info',
          title: data.valid ? 'Shared engine token (.env) is live' : (data.has_token ? 'Shared engine token (.env) needs attention' : 'No shared token in .env'),
          message: (data.valid ? ('Logged in as ' + (data.user || 'Zerodha user') + '.') : (data.error || 'Token is not ready yet.')) + (data.valid ? '' : deskHint),
          detail: 'Server uptime ' + fmtNum(data.uptime_h || 0) + 'h · separate from Nexus desk session'
        };
        safeRender();
        toast(data.valid ? 'Server .env Kite token is ready' : ((data.error || 'Shared .env session not ready') + (deskHint ? ' (see Nexus note — not your desk)' : '')));
      }catch(err){
        NX.brokerAssist = { type:'error', title:'Status check failed', message: err.message || 'Unable to read shared token status' };
        safeRender();
        toast(err.message || 'Unable to read shared token status');
      }
    });
  };
  window.nxImportEnvBrokerToken = function(){
    return runBusy('broker-import-env', async function(){
      if(!((NX.boot || {}).allow_user_server_session_import)){
        toast('Use One-Time Kite Login or Kite popup. Server session import is disabled for user desks.');
        return;
      }
      try{
        const payload = brokerPayload();
        payload.broker_code = 'ZERODHA';
        payload.paper_mode = false;
        const data = await nxApi('/api/user/broker/import-env-token', { method:'POST', body: JSON.stringify(payload) });
        const b = (data || {}).broker || {};
        const status = String((b || {}).status || '').toUpperCase();
        const brokerError = String((b || {}).last_error || '').trim();
        await loadUserData();
        if(status !== 'CONNECTED' && status !== 'READY'){
          NX.brokerAssist = {
            type:'error',
            title:'Shared session not ready',
            message: brokerError || 'The server token is still invalid or expired.',
            detail: status ? ('Broker status: ' + status) : ''
          };
          NX.brokerTest = {
            type:'error',
            message:'Broker not connected',
            detail: brokerError || (status ? ('Broker status: ' + status) : 'Import failed')
          };
          safeRender();
          toast(brokerError || 'Shared Zerodha session is not ready');
          return;
        }
        NX.brokerAssist = { type:'success', title:'Server session imported', message:'Nexus attached the Zerodha session from backend/.env.', detail:(b.profile||{}).user_id ? ('User ID ' + (b.profile||{}).user_id) : '' };
        NX.brokerTest = {
          type:'success',
          message:'Broker connected using the shared server session',
          detail:[((b.profile||{}).name || ''), ((b.profile||{}).user_id ? ('ID ' + (b.profile||{}).user_id) : ''), (b.broker_name || '')].filter(Boolean).join(' · ')
        };
        safeRender();
        toast('Shared Zerodha session connected');
        setTimeout(function(){ window.nxBrokerCodeSync && window.nxBrokerCodeSync(); }, 20);
      }catch(err){
        NX.brokerAssist = { type:'error', title:'Import failed', message: err.message || 'Could not import shared session' };
        NX.brokerTest = { type:'error', message:'Broker not connected', detail: err.message || 'Import failed' };
        safeRender();
        toast(err.message || 'Could not import shared session');
      }
    });
  };
  window.nxRefreshEnvBrokerToken = function(){
    return runBusy('broker-refresh-env', async function(){
      if(!((NX.boot || {}).allow_user_server_session_import)){
        toast('Server token rebuild is disabled for user desks. Use your own Kite login.');
        return;
      }
      try{
        const status = await nxApi('/api/token-status', { method:'GET' });
        if(status && status.valid){
          NX.brokerAssist = {
            type:'success',
            title:'Shared token already ready',
            message:'The server Zerodha session is already valid. Attaching it to this Nexus desk now.',
            detail: status.user ? ('Logged in as ' + status.user) : ''
          };
          safeRender();
          await window.nxImportEnvBrokerToken();
          return;
        }
        NX.brokerAssist = {
          type:'cool',
          title:'Refreshing shared session',
          message:'Nexus is generating a fresh Zerodha token from backend credentials. This can take a little time.',
          detail:'If it cannot finish automatically, use One-Time Kite Login.'
        };
        safeRender();
        const refreshed = await nxApi('/api/token-refresh', { method:'POST', body: JSON.stringify({ source:'nexus' }) });
        NX.brokerAssist = { type:'success', title:'Server token refreshed', message: refreshed.msg || 'A new Zerodha token was generated.' };
        await window.nxImportEnvBrokerToken();
      }catch(err){
        NX.brokerAssist = {
          type:'error',
          title:'Auto refresh failed',
          message: err.message || 'Could not refresh Zerodha token automatically',
          detail:'Use One-Time Kite Login if you want the app to finish the broker connection through the official Zerodha page.'
        };
        safeRender();
        toast(err.message || 'Auto refresh failed');
      }
    });
  };
  window.nxSetTradingFireMode = function(mode){
    return runBusy('broker-fire-mode', async function(){
      try{
        const payload = brokerPayload();
        const code = String(payload.broker_code || '').toUpperCase();
        if(code === 'PAPER'){
          toast('Select Zerodha Kite in the Broker menu to use live firing');
          return;
        }
        if(mode === 'live'){
          payload.paper_mode = false;
          payload.live_mode = true;
          payload.enabled = true;
          var lbl = String(((el('nx-broker-label')||{}).value || '')).trim();
          if(!lbl || /^paper\s*router$/i.test(lbl)){
            payload.account_label = 'Zerodha desk';
          }
        }else{
          payload.paper_mode = true;
          payload.live_mode = false;
        }
        const data = await nxApi('/api/user/broker/connect', { method:'POST', body: JSON.stringify(payload) });
        const b = (data || {}).broker || {};
        await nxApi('/api/user/controls', {
          method:'PATCH',
          body: JSON.stringify({
            auto_execute: payload.auto_execute,
            daily_loss_limit: Number(((el('nx-user-daily-loss')||{}).value || 0)),
            max_trades_per_day: Number(((el('nx-user-max-trades')||{}).value || 0)),
            max_open_signals: Number(((el('nx-user-max-open')||{}).value || 0)),
          }),
        });
        await loadUserData();
        safeRender();
        toast(mode === 'live' ? 'Live firing ON — real exchange orders are allowed for this desk.' : 'Paper firing ON — orders stay simulated.');
        setTimeout(function(){ window.nxBrokerCodeSync && window.nxBrokerCodeSync(); }, 20);
      }catch(err){
        toast(err.message || 'Could not update firing mode');
        await loadUserData();
        safeRender();
      }
    });
  };
  window.nxBrokerConnect = function(){
    return runBusy('broker-connect', async function(){
      try{
        NX.brokerAssist = {
          type:'cool',
          title:'Saving broker session',
          message:'Contacting Zerodha and updating this desk…',
          detail:'',
          waitHint:'Please wait until Nexus confirms. Do not click Capture Token again or leave this tab.'
        };
        safeRender();
        const payload = brokerPayload();
        const data = await nxApi('/api/user/broker/connect', { method:'POST', body: JSON.stringify(payload) });
        const b = (data || {}).broker || {};
        const okConn = (b.status === 'CONNECTED' || b.status === 'READY');
        const liveReadyConn = okConn && !!(b.effective_live);
        NX.brokerAssist = okConn ? {
          type:'success',
          title: liveReadyConn ? 'Ready to fire live trades today' : 'Kite connected — paper-safe today',
          message: liveReadyConn ? 'Zerodha session is live and live firing is on for this desk.' : 'Zerodha session is attached. Use Order firing → Live when you want real orders today.',
          detail:'Key ' + String(b.api_key_masked || '—') + ' · Session ' + String(((b.profile || {}).user_id) || '—') + ' · Token ' + String(b.access_token_masked || '—')
        } : {
          type:'info',
          title:'Session not fully ready',
          message:String(b.last_error || 'Connection is not ready yet.'),
          detail:'Check API secret and redirect URL, then try Capture Token again.'
        };
        NX.brokerTest = {
          type: okConn ? 'success' : 'error',
          message: okConn ? (liveReadyConn ? 'Live desk ready' : 'Broker settings saved and connected') : 'Broker save completed but connection is not ready',
          detail: [((b.profile||{}).name || ''), ((b.profile||{}).user_id ? ('ID ' + (b.profile||{}).user_id) : ''), (b.broker_name || '')].filter(Boolean).join(' · ')
        };
        await nxApi('/api/user/controls', { method:'PATCH', body: JSON.stringify({ auto_execute: payload.auto_execute, daily_loss_limit: Number(((el('nx-user-daily-loss')||{}).value || 0)), max_trades_per_day: Number(((el('nx-user-max-trades')||{}).value || 0)), max_open_signals: Number(((el('nx-user-max-open')||{}).value || 0)) }) });
        if(okConn){
          await nxMaybeAutoSyncMainFeed(true);
        }
        await loadUserData();
        safeRender();
        toast(liveReadyConn ? 'Ready to fire live trades today' : (payload.live_mode ? 'Broker connected in live mode' : 'Broker settings saved'));
        if((b.status === 'CONNECTED' || b.status === 'READY') && el('nx-broker-request-paste')) el('nx-broker-request-paste').value = '';
        setTimeout(function(){ window.nxBrokerCodeSync && window.nxBrokerCodeSync(); }, 20);
      }catch(err){
        NX.brokerAssist = { type:'error', title:'Broker save failed', message: err.message || 'Connection failed', detail:'' };
        NX.brokerTest = { type:'error', message:'Broker not connected', detail: err.message || 'Connection failed' };
        safeRender();
        toast(err.message);
      }
    });
  };
  window.nxBrokerSaveCredentials = function(){
    return runBusy('broker-connect', async function(){
      try{
        const payload = brokerPayload();
        payload.request_token = '';
        payload.access_token = '';
        const data = await nxApi('/api/user/broker/connect', { method:'POST', body: JSON.stringify(payload) });
        const b = (data || {}).broker || {};
        NX.brokerTest = {
          type: (b.status === 'CONNECTED' || b.status === 'READY') ? 'success' : 'info',
          message: 'Credentials saved',
          detail: 'Now click Open Kite Login and then Capture Token.'
        };
        await loadUserData();
        safeRender();
        toast('Broker credentials saved');
      }catch(err){
        NX.brokerTest = { type:'error', message:'Could not save credentials', detail: err.message || 'Save failed' };
        safeRender();
        toast(err.message || 'Could not save credentials');
      }
    });
  };
  window.nxCaptureKiteToken = function(){
    const paste = String(((el('nx-broker-request-paste')||{}).value || '')).trim();
    const token = String(((el('nx-broker-access-token')||{}).value || '')).trim();
    if(!paste && !token){
      toast('Paste redirect URL/request_token or access token first');
      return;
    }
    return window.nxBrokerConnect();
  };
  window.nxBrokerTest = function(){
    return runBusy('broker-test', async function(){
      try{
        const payload = brokerPayload();
        const data = await nxApi('/api/user/broker/test', { method:'POST', body: JSON.stringify(payload) });
        const b = (data || {}).broker || {};
        NX.brokerTest = {
          type: 'success',
          message: 'Broker test successful',
          detail: [((b.profile||{}).name || ''), ((b.profile||{}).user_id ? ('ID ' + (b.profile||{}).user_id) : ''), (b.broker_name || '')].filter(Boolean).join(' · ') || (b.status || 'Connected')
        };
        await loadUserData();
        safeRender();
        toast('Broker connection test passed');
        setTimeout(function(){ window.nxBrokerCodeSync && window.nxBrokerCodeSync(); }, 20);
      }catch(err){
        NX.brokerTest = { type:'error', message:'Broker test failed', detail: err.message || 'Not connected' };
        safeRender();
        toast(err.message);
      }
    });
  };
  window.nxBrokerSampleOrder = function(){
    return runBusy('broker-sample', async function(){
      try{
        const symbol = String(((el('nx-sample-symbol')||{}).value || 'SBIN')).trim().toUpperCase();
        if(!symbol) return;
        const qty = Math.max(1, Number(((el('nx-sample-qty')||{}).value || ((el('nx-broker-qty')||{}).value || 1)) || 1));
        const autoCancel = true;
        const domBrokerOn = !!((el('nx-broker-enabled')||{}).checked);
        const serverBrokerOn = !!(((NX.dashboard||{}).broker||{}).enabled);
        if(!serverBrokerOn){
          const logMsg = domBrokerOn
            ? 'Broker enabled is checked in the form but not saved on the server. Click Capture Token or Save Credentials, then try Sample order again.'
            : 'Broker routing is off. Open Advanced routing, check Broker enabled, then Capture Token or Save Credentials.';
          const toastMsg = domBrokerOn
            ? 'Save the desk first: click Capture Token or Save Credentials (Broker enabled is not on the server yet).'
            : 'Turn on Broker enabled under Advanced routing, then Capture Token or Save Credentials.';
          NX.brokerSampleLog = { ok:false, symbol:'', quantity:'', status:'FAILED', message: logMsg };
          safeRender();
          toast(toastMsg);
          return;
        }
        NX.brokerSampleLog = { ok:true, symbol:symbol, quantity:qty, status:'REQUESTED', message:'Sending test trade request...' };
        safeRender();
        const data = await nxApi('/api/user/broker/sample-order', { method:'POST', body: JSON.stringify({ symbol: symbol, quantity: qty, auto_cancel: autoCancel }) });
        const sample = (data || {}).sample || {};
        var statusLine = String(sample.status || 'PROCESSED');
        if(String(sample.mode || '') === 'paper' && statusLine === 'SIMULATED_OK') statusLine = 'Paper test OK';
        var baseMsg = String(sample.message || '').trim() || ('Order ' + (sample.order_id || 'N/A') + ' · ' + statusLine + (sample.variety ? (' · ' + String(sample.variety).toUpperCase()) : ''));
        var hint = String(sample.hint || '').trim();
        NX.brokerSampleLog = {
          ok: true,
          symbol: sample.symbol || symbol,
          quantity: sample.quantity || qty,
          status: statusLine,
          message: hint ? (baseMsg + ' — ' + hint) : baseMsg,
        };
        await loadUserData();
        safeRender();
        toast(hint ? (baseMsg + ' — ' + hint) : baseMsg);
      }catch(err){
        NX.brokerSampleLog = {
          ok: false,
          symbol: '',
          quantity: '',
          status: 'FAILED',
          message: err.message || 'Sample order failed',
        };
        try{ await loadUserData(); safeRender(); }catch(_){}
        toast(err.message || 'Sample order failed');
      }
    });
  };
  window.nxSaveNotifications = async function(){
    try{
      await nxApi('/api/user/controls', {
        method:'PATCH',
        body: JSON.stringify({
          auto_execute: !!((el('nx-user-auto-execute')||{}).checked),
          daily_loss_limit: Number(((el('nx-user-daily-loss')||{}).value || 0)),
          max_trades_per_day: Number(((el('nx-user-max-trades')||{}).value || 0)),
          max_open_signals: Number(((el('nx-user-max-open')||{}).value || 0)),
          whatsapp_phone: String(((el('nx-user-whatsapp')||{}).value || '')).trim(),
          whatsapp_apikey: String(((el('nx-user-whatsapp-apikey')||{}).value || '')).trim(),
          telegram_chat_id: String(((el('nx-user-telegram')||{}).value || '')).trim(),
          notify_email: !!((el('nx-user-notify-email')||{}).checked),
          notify_telegram: !!((el('nx-user-notify-telegram')||{}).checked),
          notify_whatsapp: !!((el('nx-user-notify-whatsapp')||{}).checked),
          notify_token_reminder: !!((el('nx-user-token-reminder')||{}).checked)
        })
      });
      await loadUserData();
      safeRender();
      toast('Notification settings saved');
    }catch(err){ toast(err.message); }
  };
  window.nxSelectPayment = function(id){
    NX.selectedPaymentId = id;
    safeRender();
  };
  window.nxCopyText = async function(text, label){
    const value = String(text || '').trim();
    if(!value){ toast('Nothing to copy'); return; }
    try{
      if(navigator.clipboard && navigator.clipboard.writeText) await navigator.clipboard.writeText(value);
      else window.prompt('Copy this value', value);
      toast(label || 'Copied');
    }catch(_){
      window.prompt('Copy this value', value);
    }
  };
  window.nxOpenUpiIntent = function(url){
    const target = String(url || '').trim();
    if(!target){ toast('UPI link not available'); return; }
    window.location.href = target;
  };
  window.nxMarkMyPaymentPaid = async function(orderId){
    try{
      await nxApi('/api/user/payments/'+orderId+'/mark-paid', { method:'POST', body: JSON.stringify({ source:'user-payment-panel' }) });
      await loadUserData();
      safeRender();
      toast('Payment submitted for admin validation');
    }catch(err){ toast(err.message); }
  };
  window.nxDeletePaymentOrder = async function(orderId){
    if(!window.confirm('Remove this unpaid order from your list?')) return;
    try{
      await nxApi('/api/user/payments/'+orderId, { method:'DELETE' });
      await loadUserData();
      safeRender();
      toast('Order removed');
    }catch(err){ toast(err.message); }
  };
  window.nxAdminSetUserSearch = function(value){
    NX.adminUserQuery = String(value || '');
    localStorage.setItem('nx_admin_user_query', NX.adminUserQuery);
    safeRender();
  };
  window.nxAdminSelectUser = function(userId){
    NX.selectedAdminUserId = String(userId || '');
    if(NX.selectedAdminUserId) localStorage.setItem('nx_admin_selected_user_id', NX.selectedAdminUserId);
    else localStorage.removeItem('nx_admin_selected_user_id');
    safeRender();
  };
  window.nxAdminCreateUser = async function(){
    try{
      const data = await nxApi('/api/admin/users', {
        method:'POST',
        body: JSON.stringify({
          full_name:(el('nx-admin-user-name')||{}).value,
          email:(el('nx-admin-user-email')||{}).value,
          password:(el('nx-admin-user-password')||{}).value,
          role:(el('nx-admin-user-role')||{}).value,
          send_email: !!((el('nx-admin-user-send-email')||{}).checked)
        })
      });
      await loadAdminData();
      NX.selectedAdminUserId = String((((data||{}).user||{}).id) || NX.selectedAdminUserId || '');
      if(NX.selectedAdminUserId) localStorage.setItem('nx_admin_selected_user_id', NX.selectedAdminUserId);
      safeRender();
      toast((data.emailed ? 'Desk created and mailed' : 'User created') + ': ' + (((data.user||{}).email) || ''));
      if(data.temporary_password) window.prompt('Temporary password', data.temporary_password);
    }catch(err){ toast(err.message); }
  };
  window.nxAdminSaveUser = async function(userId){
    try{
      const data = await nxApi('/api/admin/users/'+userId, {
        method:'PATCH',
        body: JSON.stringify({
          email:(el('nx-admin-edit-email')||{}).value,
          role:(el('nx-admin-edit-role')||{}).value,
          full_name:(el('nx-admin-edit-name')||{}).value,
          status:(el('nx-admin-edit-status')||{}).value,
          wallet_type:(el('nx-admin-edit-wallet-type')||{}).value,
          whatsapp_phone:(el('nx-admin-edit-whatsapp')||{}).value,
          telegram_chat_id:(el('nx-admin-edit-telegram')||{}).value,
          daily_loss_limit:Number((el('nx-admin-edit-daily-loss')||{}).value || 0),
          max_trades_per_day:Number((el('nx-admin-edit-max-trades')||{}).value || 0),
          max_open_signals:Number((el('nx-admin-edit-max-open')||{}).value || 0),
          notes:(el('nx-admin-edit-notes')||{}).value,
          send_email: !!((el('nx-admin-edit-send-email')||{}).checked)
        })
      });
      await loadAdminData();
      safeRender();
      toast(data.emailed ? 'User saved and summary mailed' : 'User saved');
    }catch(err){ toast(err.message); }
  };
  window.nxAdminArchiveRestoreUser = async function(userId){
    const selected = adminSelectedUser();
    const isArchived = String((((selected||{}).status)||'').toUpperCase()) === 'ARCHIVED';
    const action = isArchived ? 'restore' : 'archive';
    const label = isArchived ? 'restore' : 'archive';
    if(!window.confirm('Confirm ' + label + ' for this user?')) return;
    try{
      await nxApi('/api/admin/users/'+userId+'/'+action, { method:'POST', body: JSON.stringify({}) });
      await loadAdminData();
      safeRender();
      toast(isArchived ? 'User restored' : 'User archived');
    }catch(err){ toast(err.message); }
  };
  window.nxAdminSendSummary = async function(userId){
    try{
      await nxApi('/api/admin/users/'+userId+'/send-summary', { method:'POST', body: JSON.stringify({}) });
      toast('Account summary sent');
    }catch(err){ toast(err.message); }
  };
  window.nxAdminResetUserPassword = async function(userId){
    const password = window.prompt('Temporary password (leave blank to auto-generate)', '');
    if(password === null) return;
    try{
      const data = await nxApi('/api/admin/users/'+userId+'/password-reset', {
        method:'POST',
        body: JSON.stringify({ password: password, send_email: !!((el('nx-admin-edit-send-email')||{}).checked) })
      });
      await loadAdminData();
      safeRender();
      if(data.temporary_password) window.prompt('Temporary password', data.temporary_password);
      toast(data.emailed ? 'Password reset and mailed' : 'Password reset');
    }catch(err){ toast(err.message); }
  };
  window.nxAdminCredit = async function(userId){
    const amount = window.prompt('Credit amount (use negative to debit)', '500');
    if(amount===null) return;
    const note = window.prompt('Note for wallet update', 'Admin control hub adjustment');
    if(note===null) return;
    try{
      const data = await nxApi('/api/admin/users/'+userId+'/wallet/credit', {
        method:'POST',
        body: JSON.stringify({ amount: Number(amount||0), note: note, send_email: !!((el('nx-admin-edit-send-email')||{}).checked) })
      });
      await loadAdminData();
      safeRender();
      toast(data.emailed ? 'Wallet updated and mailed' : 'Wallet updated');
    }catch(err){ toast(err.message); }
  };
  window.nxAdminToggleStrategy = async function(code, active){ try{ await nxApi('/api/admin/strategies/'+code, { method:'PATCH', body: JSON.stringify({ active: !!active }) }); await loadAdminData(); safeRender(); toast('Strategy status saved'); }catch(err){ toast(err.message); } };
  window.nxAdminCreateCoupon = async function(){ try{ await nxApi('/api/admin/coupons', { method:'POST', body: JSON.stringify({ code:(el('nx-admin-coupon-code')||{}).value, credit:Number((el('nx-admin-coupon-credit')||{}).value||0), max_profit:Number((el('nx-admin-coupon-cap')||{}).value||0), usage_limit:Number((el('nx-admin-coupon-limit')||{}).value||1) }) }); await loadAdminData(); safeRender(); toast('Coupon created'); }catch(err){ toast(err.message); } };
  window.nxAdminMarkPaid = async function(orderId){ try{ await nxApi('/api/admin/payments/'+orderId+'/mark-paid', { method:'POST', body: JSON.stringify({ source:'admin-validation' }) }); await loadAdminData(); safeRender(); toast('Payment approved and reflected to user account'); }catch(err){ toast(err.message); } };
  window.nxAdminSavePaymentProfile = async function(){
    try{
      const value = {
        enabled: !!((el('nx-admin-upi-enabled')||{}).checked),
        payee_name: String(((el('nx-admin-upi-payee-name')||{}).value || '')).trim(),
        upi_id: String(((el('nx-admin-upi-id')||{}).value || '')).trim(),
        merchant_code: String(((el('nx-admin-upi-merchant-code')||{}).value || '')).trim(),
        support_phone: String(((el('nx-admin-upi-support-phone')||{}).value || '')).trim(),
        support_email: String(((el('nx-admin-upi-support-email')||{}).value || '')).trim(),
        instructions: String(((el('nx-admin-upi-instructions')||{}).value || '')).trim(),
        theme_color: String(((el('nx-admin-upi-theme')||{}).value || '#5ec8ff')).trim()
      };
      await nxApi('/api/admin/settings', { method:'POST', body: JSON.stringify({ key:'payment_profile', value:value }) });
      await ensureBoot();
      NX.boot.payment_profile = value;
      await loadAdminData();
      safeRender();
      toast('Payment rail saved');
    }catch(err){ toast(err.message); }
  };
  window.nxAdminSendTestEmail = async function(){ try{ const email = ((el('nx-admin-gmail-test-email')||{}).value || ((NX.user||{}).email) || (((NX.boot||{}).admin||{}).email) || 'admin@stockr.in').trim(); const data = await nxApi('/api/admin/gmail/test', { method:'POST', body: JSON.stringify({ email: email }) }); toast('Gmail test sent to ' + (data.email || email)); }catch(err){ toast(err.message); } };
  window.nxAdminGmailOauthStatus = async function(){
    try{
      const data = await nxApi('/api/admin/gmail/oauth/status', { method:'GET' });
      await ensureBoot();
      NX.boot.gmail = (data || {}).gmail || {};
      NX.boot.gmail_ready = !!((NX.boot.gmail || {}).ready);
      safeRender();
      toast('Gmail status refreshed');
    }catch(err){ toast(err.message); }
  };
  window.nxAdminGmailOauthStart = async function(){
    try{
      const data = await nxApi('/api/admin/gmail/oauth/start', { method:'POST' });
      const url = String((data || {}).url || '');
      if(!url){ throw new Error('OAuth URL missing'); }
      const pop = window.open(url, 'nx-gmail-oauth', 'popup=yes,width=560,height=720');
      if(!pop){ throw new Error('Popup blocked. Allow popups and retry.'); }
      toast('Complete Google consent in popup');
    }catch(err){ toast(err.message); }
  };
  window.nxAdminGmailOauthDisconnect = async function(){
    if(!window.confirm('Disconnect Gmail OAuth for admin mailer?')) return;
    try{
      const data = await nxApi('/api/admin/gmail/oauth/disconnect', { method:'POST' });
      await ensureBoot();
      NX.boot.gmail = ((data || {}).gmail) || {};
      NX.boot.gmail_ready = !!((NX.boot.gmail || {}).ready);
      safeRender();
      toast('Gmail OAuth disconnected');
    }catch(err){ toast(err.message); }
  };


  window.openNexusModal = function(){
    var modal = document.getElementById('nx-modal');
    if(!modal) return;
    modal.classList.add('on');
    document.body.classList.add('nx-modal-open');
    safeRender();
    if(!NX.ready && !NX.loading) hydrate();
  };

  window.closeNexusModal = function(){
    var modal = document.getElementById('nx-modal');
    if(!modal) return;
    modal.classList.remove('on');
    document.body.classList.remove('nx-modal-open');
  };

  document.addEventListener('keydown', function(ev){
    if(ev.key === 'Escape') window.closeNexusModal();
  });
  nxInstallKiteOAuthBroadcast();
  window.addEventListener('message', function(ev){
    const d = (ev || {}).data || {};
    if(d && d.type === 'nx-gmail-oauth'){
      window.nxAdminGmailOauthStatus && window.nxAdminGmailOauthStatus();
      return;
    }
    if(d && d.type === 'stockr_kite_oauth' && typeof window.nxHandleKiteOAuthMessage === 'function'){
      window.nxHandleKiteOAuthMessage(ev);
    }
  });

  window.nxEnter = function(){ window.openNexusModal(); };

  function bootNexusIfMounted(){
    if(!el('nx-root')) return;
    if(window._nxBootHydrateDone) return;
    window._nxBootHydrateDone = true;
    void hydrate();
  }

  safeRender();
  nxConsumeDeferredKiteOAuthPayload();
  bootNexusIfMounted();
  document.addEventListener('DOMContentLoaded', function(){
    if(el('nx-root')){
      safeRender();
      bootNexusIfMounted();
    }
  });
  window.addEventListener('load', function(){ if(el('nx-root') && !String(el('nx-root').innerHTML||'').trim()) safeRender(); });
})();

(function(){
  var bind = function(){
    if(window._nxSwBound || typeof window.sw !== 'function') return;
    window._nxSwBound = true;
    var prev = window.sw;
    window.sw = function(name, btn){
      prev && prev(name, btn);
      if(name === 'nexus' && typeof window.nxEnter === 'function') setTimeout(window.nxEnter, 40);
    };
  };
  if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', bind);
  else bind();
})();
