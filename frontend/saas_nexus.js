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
    selectedPaymentId: null,
    adminOtpPending: false,
    adminOtpEmail: '',
    adminUserView: localStorage.getItem('nx_admin_user_view') || 'active',
    adminUserQuery: localStorage.getItem('nx_admin_user_query') || '',
    selectedAdminUserId: localStorage.getItem('nx_admin_selected_user_id') || '',
  };
  const NX_ADMIN_ONLY = String(window.location.pathname || '').toLowerCase() === '/admin';
  if(NX_ADMIN_ONLY) NX.authMode = 'admin';

  function el(id){ return document.getElementById(id); }
  function fmtMoney(v){ const n=Number(v||0); return '\u20B9' + n.toLocaleString('en-IN',{maximumFractionDigits:2}); }
  function fmtNum(v){ return Number(v||0).toLocaleString('en-IN',{maximumFractionDigits:2}); }
  function fmtDate(v){ if(!v) return '—'; try{ return new Date(v).toLocaleString('en-IN'); }catch(_){ return v; } }
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
  function normalizeEmail(v){
    let email = String(v || '').trim().toLowerCase();
    if(email.endsWith('@stokr.in')) email = email.replace(/@stokr\.in$/, '@stockr.in');
    return email;
  }
  function defaultAdminEmail(){
    return normalizeEmail((((NX.boot || {}).admin || {}).email) || 'admin@stockr.in');
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
  function logout(){ if(window._nxKiteInteractiveTimer){ clearTimeout(window._nxKiteInteractiveTimer); window._nxKiteInteractiveTimer = null; } remember('', ''); NX.user=null; NX.dashboard=null; NX.admin=null; NX.payments=[]; NX.trades=[]; NX.loadingAction=''; NX.brokerTest=null; NX.brokerAssist=null; NX.selectedPaymentId=null; clearNotice(); safeRender(); }

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
      nxApi('/api/admin/users', { method:'GET' }),
      nxApi('/api/admin/users/history', { method:'GET' })
    ]);
    NX.admin = Object.assign({}, out[0], {
      coupons: out[1].items || [],
      settings: out[2].items || [],
      paymentsFull: out[3].items || [],
      payment_profile: out[0].payment_profile || out[3].payment_profile || null,
      users: out[4].items || [],
      history: out[5].items || out[0].history || []
    });
    var users = (NX.admin.users || []);
    var selected = String(NX.selectedAdminUserId || '').trim();
    if(selected && !users.some(function(u){ return String(u.id) === selected; })) selected = '';
    if(!selected && users.length){
      var preferred = users.find(function(u){ return !(((u.lifecycle || {}).is_archived)); }) || users[0];
      selected = preferred ? String(preferred.id) : '';
    }
    NX.selectedAdminUserId = selected;
    if(selected) localStorage.setItem('nx_admin_selected_user_id', selected);
    else localStorage.removeItem('nx_admin_selected_user_id');
  }

  function adminAllUsers(){
    return ((NX.admin || {}).users) || [];
  }
  function adminActiveUsers(){
    return adminAllUsers().filter(function(u){ return !(((u.lifecycle || {}).is_archived)); });
  }
  function adminArchivedUsers(){
    return adminAllUsers().filter(function(u){ return !!(((u.lifecycle || {}).is_archived)); });
  }
  function adminHistoryItems(){
    return (((NX.admin || {}).history) || []);
  }
  function currentAdminManagedUser(){
    var wanted = String(NX.selectedAdminUserId || '').trim();
    var users = adminAllUsers();
    for(var i=0;i<users.length;i+=1){
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
    const gmailBadge = '<span class="nx-badge ' + (boot.gmail_ready ? 'good' : 'warn') + '">' + (boot.gmail_ready ? 'Gmail Live' : 'Gmail Pending') + '</span>';
    const who = NX.user ? ('Signed in as ' + escapeHtml(NX.user.full_name || NX.user.email || 'Trader')) : 'Unified login and workspace hub for users, admins, wallet, strategy access, and mail notifications.';
    const sub = NX.user
      ? ('Role: <b>'+escapeHtml(NX.user.role)+'</b> · Status: <b>'+escapeHtml(NX.user.status || 'ACTIVE')+'</b> · Mailer: <b>'+(boot.gmail_ready ? 'Connected' : 'Waiting for Gmail setup')+'</b>')
      : ('Bootstrap admin: <b>'+escapeHtml((boot.admin||{}).email || 'admin@stockr.in')+'</b> · User/Admin access remains separate from your live trading tabs.');
    return '<div class="nx-hero">'
      + '<div class="nx-kicker">'+escapeHtml(brand)+' ACCESS HUB</div>'
      + '<div class="nx-hero-top"><div><div class="nx-title">'+escapeHtml(brand)+' Login Nexus</div><div class="nx-sub">'+who+'<br>'+sub+'</div><div class="nx-pill-row" style="margin-top:14px"><span class="nx-pill">Portal + Identity</span><span class="nx-pill">Brand '+escapeHtml(brand)+'</span>'+gmailBadge+'</div></div>'
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
    const adminSub = 'Admin login uses email + password only (OTP temporarily disabled).';
    return '<div class="nx-card nx-auth-card nx-auth-card-wide"><div class="nx-card-head"><div><div class="nx-card-title">Login Portal</div><div class="nx-card-sub">Choose your lane: User Desk, New Signup, or Admin Command Access</div></div><span class="nx-badge '+(gmailReady?'good':'warn')+'">'+(gmailReady?'Gmail Ready':'Gmail Setup Pending')+'</span></div><div class="nx-card-body">'
      + '<div class="nx-auth-wrap nx-auth-wrap-single"><div class="nx-auth-left"><div class="nx-tabs">'+tabsHtml+'</div>'+statusHtml
      + '<div class="nx-auth-mode '+(NX.authMode==='login'?'on':'')+'">'
      + '<div class="nx-oauth-row"><button class="nx-oauth-btn" onclick="nxGooglePreview()"><span class="nx-oauth-g">G</span><span>Use Gmail (Email Login)</span></button></div>'
      + '<div class="nx-form-grid"><label class="nx-form-label">Email<input id="nx-login-email" class="nx-input" placeholder="trader@email.com"></label><label class="nx-form-label">Password<input id="nx-login-password" type="password" class="nx-input" placeholder="password"></label></div>'
      + '<div class="nx-actions" style="margin-top:12px"><button class="nx-btn nx-btn-primary nx-btn-wide" onclick="nxLogin()" '+(loginBusy?'disabled':'')+'>'+(loginBusy?'Signing in...':'Enter User Desk')+'</button></div></div>'
      + '<div class="nx-auth-mode '+(NX.authMode==='signup'?'on':'')+'">'
      + '<div class="nx-form-grid"><label class="nx-form-label">Full Name<input id="nx-signup-name" class="nx-input" placeholder="Trader name"></label><label class="nx-form-label">Email<input id="nx-signup-email" class="nx-input" placeholder="new@desk.com"></label></div>'
      + '<div class="nx-form-grid"><label class="nx-form-label">Password<input id="nx-signup-password" type="password" class="nx-input" placeholder="min 6 chars"></label><label class="nx-form-label">Onboarding<input class="nx-input" value="Coupon '+escapeHtml(boot.coupon_code || 'WELCOME500')+' auto-applies" disabled></label></div>'
      + '<div class="nx-form-grid"><label class="nx-form-label">WhatsApp<input id="nx-signup-whatsapp" class="nx-input" placeholder="+91 9876543210"></label><label class="nx-form-label">Telegram ID<input id="nx-signup-telegram" class="nx-input" placeholder="123456789"></label></div>'
      + '<div class="nx-toggle-grid" style="margin-top:12px"><label class="nx-check"><input id="nx-signup-notify-email" type="checkbox" checked><span>Email alerts</span></label><label class="nx-check"><input id="nx-signup-notify-telegram" type="checkbox" checked><span>Telegram alerts</span></label><label class="nx-check"><input id="nx-signup-notify-whatsapp" type="checkbox"><span>WhatsApp alerts</span></label><label class="nx-check"><input id="nx-signup-token-reminder" type="checkbox" checked><span>Token reminder</span></label></div>'
      + '<div class="nx-inline-note" style="margin-top:12px">Telegram works with your chat ID. WhatsApp alerts use your number now; add the per-user CallMeBot API key after login to fully activate WhatsApp delivery.</div>'
      + '<div class="nx-actions" style="margin-top:12px"><button class="nx-btn nx-btn-primary nx-btn-wide" onclick="nxSignup()" '+(signupBusy?'disabled':'')+'>'+(signupBusy?'Creating...':'Create User Account')+'</button></div></div>'
      + '<div class="nx-auth-mode '+(NX.authMode==='admin'?'on':'')+'">'
      + '<div class="nx-form-grid"><label class="nx-form-label">Admin Email<input id="nx-admin-email" class="nx-input" placeholder="'+escapeHtml((boot.admin||{}).email || 'admin@stockr.in')+'"></label><label class="nx-form-label">Password<input id="nx-admin-password" type="password" class="nx-input" placeholder="admin password"></label></div>'
      + '<div class="nx-inline-note" style="margin-top:10px">'+adminSub+'<br>Admin email: <b>'+escapeHtml(defaultAdminEmail())+'</b>.</div>'
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

  function renderKiteExecutionHub(user, broker, brokerOptions, brokerProfileBits, brokerTestHtml, executionOrders, effectiveBrokerCode){
    const code = String(effectiveBrokerCode || broker.broker_code || 'PAPER').toUpperCase();
    const paper = code === 'PAPER';
    const liveLocked = code === 'PAPER';
    const paperRouteOn = !!broker.paper_mode;
    const sessOk = broker.status === 'CONNECTED' || broker.status === 'READY';
    const engineReady = !!broker.enabled && sessOk && !paper;
    const assist = NX.brokerAssist || null;
    const statusClass = sessOk ? 'good' : (broker.status === 'ERROR' ? 'bad' : 'warn');
    const modeBadge = broker.live_mode ? 'bad' : 'cool';
    const modeLabel = broker.live_mode ? 'Live execution' : 'Paper / safe';
    const profileLine = brokerProfileBits.length ? brokerProfileBits.join(' · ') : 'Not verified yet';
    const tokenClass = assist ? (assist.type === 'success' ? 'good' : (assist.type === 'error' ? 'bad' : 'cool')) : 'warn';
    const tokenTitle = assist ? escapeHtml(assist.title || 'Shared session checked') : 'Shared session not checked yet';
    const tokenMessage = assist ? escapeHtml(assist.message || '') : 'Fastest path: use Server Session if backend/.env already has a live Kite token. Cleanest fresh-login path: use One-Time Kite Login.';
    const tokenDetail = assist && assist.detail ? '<div class="nx-inline-note" style="margin-top:8px">'+escapeHtml(assist.detail)+'</div>' : '';
    const importing = NX.loadingAction === 'broker-import-env';
    const refreshing = NX.loadingAction === 'broker-refresh-env';
    const checking = NX.loadingAction === 'broker-token-status';
    const interactiveStarting = NX.loadingAction === 'broker-interactive-start';
    const saving = NX.loadingAction === 'broker-connect';
    const testing = NX.loadingAction === 'broker-test';
    const sampling = NX.loadingAction === 'broker-sample';
    const kiteBrand = '<div class="nx-kite-brand"><span class="nx-kite-z">Z</span><span>Zerodha Kite</span></div>';
    const steps = paper
      ? '<div class="nx-z-paper-callout"><div class="nx-z-paper-title">Desk type: Paper</div><div class="nx-z-paper-sub">No Zerodha session on this desk. To connect Kite, open the <b>Broker</b> menu above and choose <b>Zerodha Kite</b>, then click <b>Save Desk &amp; Verify</b> after you connect. <b>Paper route</b> (below) is separate: it only simulates orders once Zerodha is connected.</div></div>'
      : '<div class="nx-z-steps-wrap"><div class="nx-z-steps-head">Connect Zerodha (4 simple ways)</div><ol class="nx-z-steps">'
        + '<li><span class="nx-z-sn">1</span><div><strong>Use Server Session</strong><span>If this machine already has a valid Kite token in <code>backend/.env</code>, connect in one click.</span></div></li>'
        + '<li><span class="nx-z-sn">2</span><div><strong>One-Time Kite Login</strong><span>Best user flow. Open official Zerodha, log in once, and let Nexus complete the desk automatically.</span></div></li>'
        + '<li><span class="nx-z-sn">3</span><div><strong>Open Login URL + paste</strong><span>Use your own browser, then paste the final redirect URL or <code>request_token</code> below.</span></div></li>'
        + '<li><span class="nx-z-sn">4</span><div><strong>Local token generator</strong><span>Run <code>python generate_token.py</code>, then paste the token below or load it from <code>backend/.env</code>.</span></div></li>'
        + '</ol></div>'
        + (paperRouteOn ? '<div class="nx-z-paper-route-note"><b>Paper route is ON</b> - orders stay simulated, but you still need a valid Kite session above for live quotes and session checks.</div>' : '');
    const quickLane = paper ? '' : ''
      + '<div class="nx-z-command-grid">'
      + '<div class="nx-z-command-card nx-z-command-card-primary"><div class="nx-z-fast-kicker">Quick connect</div><div class="nx-z-fast-title">Use server session</div><div class="nx-z-fast-sub">Fastest option. If this machine already has a valid Kite token in <b>backend/.env</b>, connect in one click.</div><div class="nx-actions" style="margin-top:14px"><button type="button" class="nx-btn nx-btn-primary" onclick="nxImportEnvBrokerToken()" '+(importing?'disabled':'')+'>'+(importing?'Importing...':'Use Server Session')+'</button><button type="button" class="nx-btn nx-btn-ghost" onclick="nxCheckServerTokenStatus()" '+(checking?'disabled':'')+'>'+(checking?'Checking...':'Check Status')+'</button></div></div>'
      + '<div class="nx-z-command-card nx-z-command-card-lite"><div class="nx-z-fast-kicker">One-time login</div><div class="nx-z-fast-title">Open official Kite</div><div class="nx-z-fast-sub">Opens Zerodha in <b>Chrome or Edge</b> on the server. Log in there; Nexus reads the redirect and saves this desk. Requires API <b>key</b> + <b>secret</b> (saved on this desk, or in <code>backend/.env</code> on the server).</div><div class="nx-actions" style="margin-top:14px"><button type="button" class="nx-btn nx-btn-gold" onclick="nxKiteInteractiveLogin()" '+(interactiveStarting?'disabled':'')+'>'+(interactiveStarting?'Opening...':'One-Time Kite Login')+'</button></div></div>'
      + '<div class="nx-z-command-card"><div class="nx-z-fast-kicker">Auto refresh</div><div class="nx-z-fast-title">Refresh and use</div><div class="nx-z-fast-sub">If user ID, password, TOTP, API key, and secret are saved in <b>backend/.env</b>, refresh the session automatically.</div><div class="nx-actions" style="margin-top:14px"><button type="button" class="nx-btn nx-btn-ghost" onclick="nxRefreshEnvBrokerToken()" '+(refreshing?'disabled':'')+'>'+(refreshing?'Refreshing...':'Auto Refresh & Use')+'</button></div></div>'
      + '</div>'
      + '<div class="nx-status '+tokenClass+'" style="margin-top:12px"><b>'+tokenTitle+'</b>'+(tokenMessage ? '<div style="margin-top:6px">'+tokenMessage+'</div>' : '')+tokenDetail+'</div>';
    const manualCard = paper ? '' : ''
      + '<div class="nx-z-manual-card">'
      + '<div class="nx-z-manual-head"><div><div class="nx-z-fast-kicker">Token playbook</div><div class="nx-z-fast-title">The easiest way to get your Kite token</div></div><div class="nx-inline-note">Best path: click <b>One-Time Kite Login</b>, finish Zerodha login, then save. Alternate paths stay ready below if you prefer manual control.</div></div>'
      + '<div class="nx-z-step-pills"><span class="nx-z-step-pill">1. Open Kite</span><span class="nx-z-step-pill">2. Login once</span><span class="nx-z-step-pill">3. Paste redirect URL or token</span><span class="nx-z-step-pill">4. Save &amp; verify</span></div>'
      + '<div class="nx-z-guide-grid">'
      + '<div class="nx-z-guide-card nx-z-guide-card-featured"><div class="nx-z-guide-top"><span class="nx-badge good">Recommended</span><span class="nx-z-guide-label">Fastest today</span></div><div class="nx-z-guide-title">One-Time Kite Login</div><div class="nx-z-guide-copy">Nexus opens official Zerodha, you complete the login once, and the desk is saved automatically.</div></div>'
      + '<div class="nx-z-guide-card"><div class="nx-z-guide-top"><span class="nx-badge cool">Alternate</span><span class="nx-z-guide-label">Own browser</span></div><div class="nx-z-guide-title">Open Login URL</div><div class="nx-z-guide-copy">Best when you want Zerodha in your own browser window. Paste the final redirect URL below.</div></div>'
      + '<div class="nx-z-guide-card"><div class="nx-z-guide-top"><span class="nx-badge warn">No re-login</span><span class="nx-z-guide-label">Reuse existing token</span></div><div class="nx-z-guide-title">Use Server Session</div><div class="nx-z-guide-copy">If <code>backend/.env</code> already has a fresh token, Nexus can attach it instantly.</div></div>'
      + '<div class="nx-z-guide-card"><div class="nx-z-guide-top"><span class="nx-badge cool">Terminal fallback</span><span class="nx-z-guide-label">Most manual</span></div><div class="nx-z-guide-title">Generate token locally</div><div class="nx-z-guide-copy">Run <code>python generate_token.py</code>, then click <b>LOAD .ENV</b> or paste the token below.</div></div>'
      + '</div>'
      + '<div class="nx-z-manual-actions"><button type="button" class="nx-btn nx-btn-gold" onclick="nxKiteInteractiveLogin()" '+(interactiveStarting?'disabled':'')+'>'+(interactiveStarting?'Opening...':'One-Time Kite Login')+'</button><button type="button" class="nx-btn nx-btn-ghost" onclick="nxKiteOpenLogin()">Open Login URL</button><button type="button" class="nx-btn nx-btn-primary" onclick="nxImportEnvBrokerToken()" '+(importing?'disabled':'')+'>'+(importing?'Importing...':'Use Server Session')+'</button><button type="button" class="nx-btn nx-btn-ghost" onclick="nxRefreshEnvBrokerToken()" '+(refreshing?'disabled':'')+'>'+(refreshing?'Refreshing...':'Auto Refresh &amp; Use')+'</button></div>'
      + '<div class="nx-z-command-strip"><span>Alternate way</span><code>python generate_token.py</code><span>Then click <b>LOAD .ENV</b> or <b>Use Server Session</b></span></div>'
      + '<div class="nx-z-manual-grid">'
      + '<div class="nx-z-manual-column"><div class="nx-z-mini-head">Manual fallback</div><div class="nx-inline-note">If Zerodha opens in your own browser, paste the final redirect URL here. Nexus extracts <code>request_token</code> automatically.</div><label class="nx-form-label" style="margin-top:14px">Redirect URL or request_token<input id="nx-broker-request-paste" class="nx-input" placeholder="https://127.0.0.1/?request_token=abc123&amp;action=login&amp;status=success"></label><div class="nx-inline-note" style="margin-top:10px">You can paste either the full URL or just the <code>request_token</code>. This is the cleanest alternate path if One-Time Kite Login is not available.</div></div>'
      + '<div class="nx-z-manual-column"><div class="nx-z-mini-head">Kite app credentials</div><div class="nx-inline-note">Needed for manual redirect exchange when the server does not already know your app. If these are already saved in <code>backend/.env</code>, you can leave them blank.</div><div class="nx-form-grid" style="margin-top:14px"><label class="nx-form-label">API key<input id="nx-broker-api-key" class="nx-input" autocomplete="off" placeholder="From Kite Connect" value=""></label><label class="nx-form-label">API secret<input id="nx-broker-api-secret" type="password" class="nx-input" autocomplete="new-password" placeholder="'+escapeHtml(broker.api_secret_masked || 'App secret')+'" value=""></label></div><div class="nx-inline-note" style="margin-top:10px">If auto refresh fails because Zerodha asks for CAPTCHA, use <b>Open Login URL</b> or <b>One-Time Kite Login</b> instead.</div></div>'
      + '</div>'
      + '</div>';
    const advancedToolkit = paper ? '' : ''
      + '<div class="nx-z-advanced-shell">'
      + '<div class="nx-z-advanced-hero"><div><div class="nx-z-fast-kicker">Advanced toolkit</div><div class="nx-z-advanced-title">Token, routing, and risk controls</div><div class="nx-z-advanced-copy">Use this section for direct access-token paste, product defaults, routing posture, and execution limits. If you only want to get connected quickly, the playbook above is enough. If Zerodha blocks automation with CAPTCHA, fall back to One-Time Kite Login, Open Login URL, or python generate_token.py.</div></div><div class="nx-z-advanced-badges"><span class="nx-badge '+tokenClass+'">'+tokenTitle+'</span><span class="nx-badge '+(sessOk?'good':'warn')+'">'+(sessOk ? 'Quotes ready' : 'Needs validation')+'</span></div></div>'
      + '<div class="nx-z-advanced-grid"><div class="nx-z-advanced-tip"><div class="nx-z-mini-head">Fastest route</div><p>Use <b>Server Session</b> if this machine already has a valid token in <code>backend/.env</code>.</p></div><div class="nx-z-advanced-tip"><div class="nx-z-mini-head">Best fallback</div><p>Use <b>One-Time Kite Login</b> when auto refresh hits CAPTCHA or when you want the cleanest user flow.</p></div><div class="nx-z-advanced-tip"><div class="nx-z-mini-head">Direct paste</div><p>If you already have an access token, paste it below and save. This is the most manual route.</p></div></div>'
      + '</div>';
    const simpleForm = ''
      + '<div class="nx-z-simple">'
      + '<div class="nx-form-grid nx-z-topline"><label class="nx-form-label">Broker<select id="nx-broker-code" class="nx-select" onchange="nxBrokerCodeSync()">'+brokerOptions+'</select></label><label class="nx-form-label">Desk label<input id="nx-broker-label" class="nx-input" placeholder="Primary desk" value="'+escapeHtml(broker.account_label || '')+'"></label></div>'
      + '<div id="nx-paper-fields" class="nx-z-toggle" style="display:'+(paper?'block':'none')+'"><p class="nx-z-lead">You have <b>Paper</b> selected as the broker <em>type</em>. Pick <b>Zerodha Kite</b> in the Broker menu to show login tools. After Kite is connected, use <b>Paper route</b> in Advanced to keep orders simulated.</p></div>'
      + '<div id="nx-zerodha-fields" class="nx-z-toggle" style="display:'+(paper?'none':'block')+'">'
      + quickLane
      + manualCard
      + '</div></div>';
    let readiness = '';
    if(paper){
      readiness = '<div class="nx-z-ready nx-z-ready-paper"><div class="nx-z-ready-title">Paper desk type</div><div class="nx-z-ready-sub">Choose <b>Zerodha Kite</b> in the Broker menu to connect. <b>Paper route</b> (Advanced) is optional and only affects whether orders are sent live.</div></div>';
    }else if(sessOk && broker.enabled){
      readiness = '<div class="nx-z-ready nx-z-ready-on"><div class="nx-z-ready-title">Ready for execution</div><div class="nx-z-ready-sub">Kite session verified. Enable <b>Live mode</b> only when you want real orders.</div></div>';
    }else if(sessOk && !broker.enabled){
      readiness = '<div class="nx-z-ready nx-z-ready-warn"><div class="nx-z-ready-title">Connected — routing off</div><div class="nx-z-ready-sub">Turn on <b>Broker enabled</b> in Advanced when you want auto-routing.</div></div>';
    }else{
      readiness = '<div class="nx-z-ready"><div class="nx-z-ready-title">Not validated</div><div class="nx-z-ready-sub">Start with <b>One-Time Kite Login</b> or <b>Use Server Session</b>. If needed, paste your redirect URL or token and save. Nexus checks the session immediately.</div></div>';
    }
    return ''
      + '<div class="nx-card nx-kite-dock">'
      + '<div class="nx-kite-dock-head">'
      + '<div class="nx-kite-dock-title-row">'
      + '<div><div class="nx-card-title nx-kite-dock-title">Trading execution</div><div class="nx-card-sub">Pick your broker type first. Zerodha tools appear only when <b>Zerodha Kite</b> is selected — then connect, validate, and tune routing below.</div></div>'
      + '<div class="nx-kite-dock-badges"><span class="nx-badge '+statusClass+'">'+escapeHtml(broker.status || 'Idle')+'</span><span class="nx-badge '+modeBadge+'">'+modeLabel+'</span><span class="nx-badge '+(engineReady ? 'good' : 'warn')+'">'+(engineReady ? 'Engine ready' : (paper ? 'Paper' : 'Awaiting validation'))+'</span></div>'
      + '</div>'
      + '<div class="nx-kite-dock-hero">'+(paper ? '' : kiteBrand)+steps+'</div>'
      + '</div>'
      + '<div class="nx-card-body nx-kite-dock-body"><div class="nx-kite-dock-grid">'
      + '<div class="nx-kite-dock-main">'+simpleForm
      + '<details class="nx-z-advanced"><summary>Token guide, routing &amp; risk controls</summary>'
      + '<div class="nx-z-advanced-body">'
      + advancedToolkit
      + '<div class="nx-form-grid"><label class="nx-form-label">Broker user ID<input id="nx-broker-user-id" class="nx-input" placeholder="AB1234" value="'+escapeHtml(broker.broker_user_id || '')+'"></label><label class="nx-form-label">Default qty<input id="nx-broker-qty" class="nx-input" placeholder="1" value="'+escapeHtml(String(broker.default_quantity || 1))+'"></label></div>'
      + '<div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Manual access token<input id="nx-broker-access-token" class="nx-input" placeholder="'+escapeHtml(broker.access_token_masked || 'Paste only if you already have a fresh access token')+'" value=""></label><label class="nx-form-label">Intraday product<select id="nx-broker-intraday-product" class="nx-select"><option '+((broker.intraday_product||'MIS')==='MIS'?'selected':'')+'>MIS</option><option '+((broker.intraday_product||'MIS')==='CNC'?'selected':'')+'>CNC</option><option '+((broker.intraday_product||'MIS')==='NRML'?'selected':'')+'>NRML</option></select></label></div>'
      + '<div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Positional product<select id="nx-broker-positional-product" class="nx-select"><option '+((broker.positional_product||'CNC')==='CNC'?'selected':'')+'>CNC</option><option '+((broker.positional_product||'CNC')==='NRML'?'selected':'')+'>NRML</option><option '+((broker.positional_product||'CNC')==='MIS'?'selected':'')+'>MIS</option></select></label><label class="nx-form-label">&nbsp;</label></div>'
      + '<div class="nx-toggle-grid" style="margin-top:14px"><label class="nx-check"><input id="nx-broker-enabled" type="checkbox"'+checkedAttr(!!broker.enabled)+'><span>Broker enabled</span><span class="nx-check-hint">Allow routing to this desk</span></label><label class="nx-check"><input id="nx-broker-paper-mode" type="checkbox"'+checkedAttr(!!broker.paper_mode || paper)+'><span>Paper route</span><span class="nx-check-hint">Simulate orders (Kite session can still be real)</span></label><label class="nx-check"><input id="nx-broker-live-mode" type="checkbox"'+checkedAttr(!!broker.live_mode && !paper)+' '+(liveLocked ? 'disabled' : '')+'><span>Live mode</span><span class="nx-check-hint">Real broker orders when off paper route</span></label><label class="nx-check"><input id="nx-user-auto-execute" type="checkbox"'+checkedAttr(!!((user.controls||{}).auto_execute))+'><span>Auto execute</span></label></div>'
      + '<div class="nx-form-grid-3" style="margin-top:12px"><label class="nx-form-label">Daily loss<input id="nx-user-daily-loss" class="nx-input" placeholder="2500" value="'+escapeHtml(String((user.controls||{}).daily_loss_limit || 0))+'"></label><label class="nx-form-label">Max trades / day<input id="nx-user-max-trades" class="nx-input" placeholder="6" value="'+escapeHtml(String((user.controls||{}).max_trades_per_day || 0))+'"></label><label class="nx-form-label">Max open signals<input id="nx-user-max-open" class="nx-input" placeholder="3" value="'+escapeHtml(String((user.controls||{}).max_open_signals || 0))+'"></label></div>'
      + '</div></details>'
      + '<div class="nx-z-cta">'
      + '<button type="button" class="nx-btn nx-btn-primary nx-btn-validate" onclick="nxBrokerConnect()" '+(saving?'disabled':'')+'>'+(saving?'Saving...':'Save Desk & Verify')+'</button>'
      + '<button type="button" class="nx-btn nx-btn-ghost" onclick="nxBrokerTest()" '+(testing?'disabled':'')+'>'+(testing?'Testing...':'Check Session')+'</button>'
      + '<button type="button" class="nx-btn nx-btn-gold" onclick="nxBrokerSampleOrder()" '+(sampling?'disabled':'')+'>'+(sampling?'Running...':'Dry-Run Order')+'</button>'
      + '</div>'
      + brokerTestHtml
      + '<div class="nx-inline-note" style="margin-top:12px">SPIKE and SWING can route as equity. INDEX stays inbox-only until contract mapping ships.</div>'
      + '</div>'
      + '<div class="nx-kite-dock-side">'
      + readiness
      + '<div class="nx-item nx-kite-health"><div class="nx-item-title">Session</div><div class="nx-item-sub">'+profileLine+'</div>'
      + '<div class="nx-inline-note" style="margin-top:8px">Key / Secret / Token show what is <b>saved on this desk</b> after Validate. Dashes mean not stored yet — use Quick connect, One-time login, or paste in Advanced.</div>'
      + '<div class="nx-broker-chips" style="margin-top:12px"><span class="nx-badge cool">Key '+escapeHtml(broker.api_key_masked || '—')+'</span><span class="nx-badge warn">Secret '+escapeHtml(broker.api_secret_masked || '—')+'</span><span class="nx-badge cool">Token '+escapeHtml(broker.access_token_masked || '—')+'</span></div>'
      + (broker.last_error ? '<div class="nx-status error" style="margin-top:12px">'+escapeHtml(broker.last_error)+'</div>' : '<div class="nx-status success" style="margin-top:12px">No blocking errors. Use <b>Test session</b> after market opens if needed.</div>')
      + '</div>'
      + '<div class="nx-item" style="margin-top:12px"><div class="nx-item-title">Recent auto orders</div><div class="nx-item-sub">Live and simulated attempts from the router.</div><div style="margin-top:12px">'+executionOrders+'</div></div>'
      + '</div></div></div></div>';
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
    const brokerOptions = brokerCatalog.length ? brokerCatalog.map(function(b){
      var bc = String(b.code||'').toUpperCase();
      return '<option value="'+escapeHtml(b.code)+'" '+(effectiveBrokerCode===bc ? 'selected' : '')+'>'+escapeHtml(b.name)+' • '+escapeHtml(b.tagline || '')+'</option>';
    }).join('') : '<option value="PAPER"'+(effectiveBrokerCode==='PAPER'?' selected':'')+'>Paper Router</option><option value="ZERODHA"'+(effectiveBrokerCode==='ZERODHA'?' selected':'')+'>Zerodha Kite</option>';
    const executionOrders = (broker.recent_orders||[]).length ? '<table class="nx-mini-table"><thead><tr><th>When</th><th>Strategy</th><th>Symbol</th><th>Status</th><th>Mode</th></tr></thead><tbody>'+(broker.recent_orders||[]).slice(0,8).map(function(o){
      return '<tr><td>'+escapeHtml(fmtDate(o.created_at))+'</td><td>'+escapeHtml(o.strategy_code)+'</td><td>'+escapeHtml(o.symbol || o.tradingsymbol)+'</td><td><span class="nx-badge '+(/FAILED|ERROR/.test(String(o.status||'')) ? 'bad' : (/SKIPPED/.test(String(o.status||'')) ? 'warn' : 'good'))+'">'+escapeHtml(o.status || 'PENDING')+'</span>'+(o.error_text ? '<div class="nx-inline-note" style="margin-top:6px">'+escapeHtml(o.error_text)+'</div>' : '')+'</td><td>'+(o.live_mode ? 'Live' : 'Paper')+'</td></tr>';
    }).join('')+'</tbody></table>' : '<div class="nx-empty">No broker execution attempts yet. Once auto-routing is enabled, placed or simulated orders will show here.</div>';
    const brokerProfileBits = [];
    if((broker.profile||{}).name) brokerProfileBits.push(escapeHtml(broker.profile.name));
    if((broker.profile||{}).user_id) brokerProfileBits.push('ID ' + escapeHtml(broker.profile.user_id));
    if((broker.profile||{}).email) brokerProfileBits.push(escapeHtml(broker.profile.email));
    const effectiveBrokerCode = nxEffectiveBrokerCode(broker);
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
    const controlsCard = '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Risk & Automation</div><div class="nx-card-sub">Save your routing posture along with alert preferences.</div></div></div><div class="nx-card-body"><div class="nx-toggle-grid"><label class="nx-check"><input id="nx-user-auto-execute" type="checkbox"'+checkedAttr(!!((user.controls||{}).auto_execute))+'><span>Auto execute</span></label><div class="nx-pay-chip"><span>Broker</span><strong>'+escapeHtml(broker.broker_name || 'Paper Router')+'</strong></div><div class="nx-pay-chip"><span>Desk mode</span><strong>'+(broker.live_mode ? 'Live' : 'Safe / Paper')+'</strong></div><div class="nx-pay-chip"><span>Broker status</span><strong>'+escapeHtml(broker.status || 'Idle')+'</strong></div></div><div class="nx-form-grid-3" style="margin-top:12px"><label class="nx-form-label">Daily loss<input id="nx-user-daily-loss" class="nx-input" placeholder="2500" value="'+escapeHtml(String((user.controls||{}).daily_loss_limit || 0))+'"></label><label class="nx-form-label">Max trades / day<input id="nx-user-max-trades" class="nx-input" placeholder="6" value="'+escapeHtml(String((user.controls||{}).max_trades_per_day || 0))+'"></label><label class="nx-form-label">Max open signals<input id="nx-user-max-open" class="nx-input" placeholder="3" value="'+escapeHtml(String((user.controls||{}).max_open_signals || 0))+'"></label></div><div class="nx-actions" style="margin-top:12px"><button class="nx-btn nx-btn-primary" onclick="nxSaveNotifications()">Save Preferences</button></div></div></div>';
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
        '<div class="nx-workspace-rail-card nx-workspace-rail-metric"><div class="nx-workspace-rail-kicker">Broker</div><div class="nx-workspace-rail-title">'+escapeHtml(broker.broker_name || 'Paper Router')+'</div><div class="nx-workspace-rail-sub">'+escapeHtml(broker.status || 'Idle')+' · '+(broker.live_mode ? 'Live mode' : 'Safe mode')+'</div></div>'
      ],
      headRight: '<div class="nx-pill-row"><span class="nx-pill">Unread '+fmtNum(metrics.signals_unread||0)+'</span><span class="nx-pill">Trades '+fmtNum((perf.summary||{}).closed_trades||0)+'</span><span class="nx-pill">'+escapeHtml((user.subscription||{}).plan_code || 'No Plan')+'</span></div>',
      body: body
    });
  }

  function renderAdminCards(){
    const a = NX.admin || { metrics:{}, users:[], strategies:[], coupons:[], paymentsFull:[], signals:[], settings:[] };
    const gmailReady = !!((NX.boot || {}).gmail_ready);
    const paymentProfile = a.payment_profile || adminSettingValue('payment_profile', ((NX.boot || {}).payment_profile) || {});
    const allUsers = adminAllUsers();
    const activeUsers = adminActiveUsers();
    const archivedUsers = adminArchivedUsers();
    const historyItems = adminHistoryItems();
    const search = String(NX.adminUserQuery || '').trim().toLowerCase();
    const view = String(NX.adminUserView || 'active').toLowerCase();
    function userMatches(u){
      if(!search) return true;
      const hay = [
        u.email,
        u.full_name,
        u.role,
        u.status,
        ((u.contacts||{}).whatsapp_phone || ''),
        ((u.contacts||{}).telegram_chat_id || ''),
        (u.notes || ''),
        ((((u.lifecycle||{}).archive_reason) || ''))
      ].join(' ').toLowerCase();
      return hay.indexOf(search) >= 0;
    }
    function historyMatches(item){
      if(!search) return true;
      const hay = [
        item.event_type,
        item.title,
        item.summary,
        (((item.user||{}).email) || ''),
        (((item.user||{}).full_name) || ''),
        (((item.actor||{}).email) || '')
      ].join(' ').toLowerCase();
      return hay.indexOf(search) >= 0;
    }
    const filteredActive = activeUsers.filter(userMatches);
    const filteredArchived = archivedUsers.filter(userMatches);
    const filteredHistory = historyItems.filter(historyMatches);
    const selected = currentAdminManagedUser();
    const selectedLifecycle = ((selected || {}).lifecycle) || {};
    const selectedContacts = ((selected || {}).contacts) || {};
    const selectedNotifications = ((selected || {}).notifications) || {};
    const selectedControls = ((selected || {}).controls) || {};
    const selectedWallet = ((selected || {}).wallet) || {};
    const selectedIsArchived = !!selectedLifecycle.is_archived;
    const visibleUsers = view === 'archived' ? filteredArchived : filteredActive;
    const stratCards = (a.strategies||[]).map(function(s){
      return '<div class="nx-strategy" style="--acc:'+(s.accent||'#58d6ff')+'"><div class="nx-acc-line"></div><h4>'+escapeHtml(s.name)+'</h4><p>'+escapeHtml(s.description||'')+'</p><div class="nx-row"><span>Type</span><span>'+escapeHtml(s.strategy_type)+'</span></div><div class="nx-row"><span>Active</span><span class="nx-badge '+(s.active?'good':'bad')+'">'+(s.active?'Live':'Paused')+'</span></div><div class="nx-actions" style="margin-top:12px"><button class="nx-mini-btn" onclick="nxAdminToggleStrategy(\''+s.code+'\','+(s.active?0:1)+')">'+(s.active?'Pause':'Enable')+'</button></div></div>';
    }).join('');
    const coupons = (a.coupons||[]).length ? '<table class="nx-mini-table"><thead><tr><th>Code</th><th>Credit</th><th>Used</th><th>Cap</th></tr></thead><tbody>'+(a.coupons||[]).slice(0,8).map(function(c){ return '<tr><td>'+escapeHtml(c.code)+'</td><td>'+fmtMoney(c.credit)+'</td><td>'+fmtNum(c.used_count)+' / '+fmtNum(c.usage_limit)+'</td><td>'+fmtMoney(c.max_profit)+'</td></tr>'; }).join('')+'</tbody></table>' : '<div class="nx-empty">No coupons yet.</div>';
    const payments = (a.paymentsFull||[]).length ? '<table class="nx-mini-table"><thead><tr><th>Order</th><th>Status</th><th>Plan</th><th>Action</th></tr></thead><tbody>'+(a.paymentsFull||[]).slice(0,8).map(function(p){ const st=String(p.status||'').toUpperCase(); const action=(st==='PAID'?'—':'<button class="nx-mini-btn" onclick="nxAdminMarkPaid('+p.id+')">'+(st==='PENDING_VALIDATION'?'Approve':'Mark Paid')+'</button>'); return '<tr><td>#'+fmtNum(p.id)+'</td><td>'+escapeHtml(p.status)+'</td><td>'+escapeHtml(p.plan_code || 'CUSTOM')+'</td><td>'+action+'</td></tr>'; }).join('')+'</tbody></table>' : '<div class="nx-empty">No orders yet.</div>';
    const signals = (a.signals||[]).length ? '<div class="nx-list">'+(a.signals||[]).slice(0,6).map(function(s){ return '<div class="nx-item"><div class="nx-item-top"><div><div class="nx-item-title">'+escapeHtml(s.headline)+'</div><div class="nx-item-sub">'+escapeHtml(s.strategy_code)+' · '+escapeHtml(fmtDate(s.created_at))+'</div></div><span class="nx-badge cool">'+fmtNum(s.confidence)+'%</span></div></div>'; }).join('')+'</div>' : '<div class="nx-empty">No routed events yet.</div>';
    const overviewCard = '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Admin Command Deck</div><div class="nx-card-sub">Monitor the SaaS layer without touching the existing trading panels</div></div></div><div class="nx-card-body"><div class="nx-metric-grid"><div class="nx-metric"><div class="nx-metric-k">Users</div><div class="nx-metric-v">'+fmtNum((a.metrics||{}).total_users||0)+'</div></div><div class="nx-metric"><div class="nx-metric-k">Active</div><div class="nx-metric-v">'+fmtNum((a.metrics||{}).active_users||0)+'</div></div><div class="nx-metric"><div class="nx-metric-k">Archived</div><div class="nx-metric-v">'+fmtNum((a.metrics||{}).archived_users||0)+'</div></div><div class="nx-metric"><div class="nx-metric-k">Revenue</div><div class="nx-metric-v">'+fmtMoney((a.metrics||{}).revenue||0)+'</div></div></div></div></div>';
    const brandCard = '<div class="nx-card"><div class="nx-card-head"><div><div class="nx-card-title">Brand & Mail</div><div class="nx-card-sub">STOCKR.IN identity and Gmail notification status</div></div><span class="nx-badge '+(gmailReady ? 'good' : 'warn')+'">'+(gmailReady ? 'Gmail Connected' : 'Gmail Pending')+'</span></div><div class="nx-card-body"><div class="nx-item"><div class="nx-item-title">'+escapeHtml((NX.boot||{}).brand || 'STOCKR.IN')+'</div><div class="nx-item-sub">Set <b>GMAIL_USERNAME</b>, <b>GMAIL_APP_PASSWORD</b>, and optional sender env vars in <b>backend/.env</b> to activate welcome, payment, and optional signal emails.</div><div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Test Email<input id="nx-admin-gmail-test-email" class="nx-input" placeholder="'+escapeHtml((((NX.user||{}).email)||((NX.boot||{}).admin||{}).email || 'admin@stockr.in'))+'"></label><div style="display:flex;align-items:flex-end"><button class="nx-btn '+(gmailReady ? 'nx-btn-primary' : 'nx-btn-ghost')+'" onclick="nxAdminSendTestEmail()" '+(gmailReady ? '' : 'disabled')+'>Send Gmail Test</button></div></div></div></div></div>';
    const paymentRailCard = '<div class="nx-card nx-payment-admin-card"><div class="nx-card-head"><div><div class="nx-card-title">Payment Rail</div><div class="nx-card-sub">Set the exact UPI destination that user QR codes should credit.</div></div><span class="nx-badge '+((paymentProfile.upi_id && paymentProfile.enabled)?'good':'warn')+'">'+((paymentProfile.upi_id && paymentProfile.enabled)?'Live UPI rail':'Setup needed')+'</span></div><div class="nx-card-body"><div class="nx-split"><div><div class="nx-item nx-pay-destination"><div class="nx-item-title">Settlement destination</div><div class="nx-item-sub">Save this once. Every new local payment QR in the user panel will point to this UPI ID and payee.</div><div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Payee Name<input id="nx-admin-upi-payee-name" class="nx-input" placeholder="STOCKR.IN" value="'+escapeHtml(paymentProfile.payee_name || '')+'"></label><label class="nx-form-label">UPI ID<input id="nx-admin-upi-id" class="nx-input" placeholder="stockrin@upi" value="'+escapeHtml(paymentProfile.upi_id || '')+'"></label></div><div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Merchant Code<input id="nx-admin-upi-merchant-code" class="nx-input" placeholder="Optional merchant code" value="'+escapeHtml(paymentProfile.merchant_code || '')+'"></label><label class="nx-form-label">Support Phone<input id="nx-admin-upi-support-phone" class="nx-input" placeholder="+91 9876543210" value="'+escapeHtml(paymentProfile.support_phone || '')+'"></label></div><div class="nx-form-grid" style="margin-top:12px"><label class="nx-form-label">Support Email<input id="nx-admin-upi-support-email" class="nx-input" placeholder="payments@stockr.in" value="'+escapeHtml(paymentProfile.support_email || '')+'"></label><label class="nx-form-label">Accent Color<input id="nx-admin-upi-theme" class="nx-input" placeholder="#5ec8ff" value="'+escapeHtml(paymentProfile.theme_color || '#5ec8ff')+'"></label></div><label class="nx-form-label" style="margin-top:12px">Instructions<textarea id="nx-admin-upi-instructions" class="nx-textarea" placeholder="Explain how the payment should be confirmed.">'+escapeHtml(paymentProfile.instructions || '')+'</textarea></label><div class="nx-toggle-grid" style="margin-top:12px"><label class="nx-check"><input id="nx-admin-upi-enabled" type="checkbox"'+checkedAttr(!!paymentProfile.enabled)+'><span>Enable direct UPI rail</span></label><div class="nx-pay-chip"><span>Mode</span><strong>'+(paymentProfile.can_auto_confirm ? 'Webhook-ready' : 'Direct UPI')+'</strong></div><div class="nx-pay-chip"><span>Status</span><strong>'+(paymentProfile.upi_id ? 'Configured' : 'Pending')+'</strong></div><div class="nx-pay-chip"><span>Stored UPI</span><strong>'+escapeHtml(paymentProfile.upi_id || 'Not saved')+'</strong></div></div><div class="nx-actions" style="margin-top:12px"><button class="nx-btn nx-btn-primary" onclick="nxAdminSavePaymentProfile()">Save Payment Rail</button><button class="nx-btn nx-btn-ghost" onclick="nxCopyText(\''+escapeHtml(paymentProfile.upi_id || '')+'\', \'UPI ID copied\')">Copy UPI ID</button></div><div class="nx-status info" style="margin-top:12px">'+(paymentProfile.can_auto_confirm ? 'Razorpay + webhook can auto-confirm gateway payments. Direct UPI QR still points to this exact destination.' : 'Direct UPI QR will send money to this saved UPI ID exactly. Payment status still needs in-app confirmation unless a gateway webhook is enabled.')+'</div></div></div><div><div class="nx-pay-panel nx-pay-preview"><div class="nx-pay-head"><div><div class="nx-item-title">Live preview</div><div class="nx-item-sub">This is what the user payment rail will use for new order QR codes.</div></div><span class="nx-badge cool">'+escapeHtml(paymentProfile.payee_name || 'Payee')+'</span></div><div class="nx-pay-bits"><div class="nx-pay-chip"><span>UPI ID</span><strong>'+escapeHtml(paymentProfile.upi_id || 'Not set')+'</strong></div><div class="nx-pay-chip"><span>Support</span><strong>'+escapeHtml([paymentProfile.support_phone, paymentProfile.support_email].filter(Boolean).join(' · ') || 'Not set')+'</strong></div><div class="nx-pay-chip"><span>Merchant</span><strong>'+escapeHtml(paymentProfile.merchant_code || 'Optional')+'</strong></div><div class="nx-pay-chip"><span>Brand</span><strong>'+escapeHtml((NX.boot||{}).brand || 'STOCKR.IN')+'</strong></div></div><div class="nx-inline-note" style="margin-top:12px">'+escapeHtml(paymentProfile.instructions || 'Instructions will appear to the user here.')+'</div></div></div></div></div>';
    const createCard = '<div class="nx-admin-lifecycle-card nx-admin-create-card"><div class="nx-admin-panel-top"><div><div class="nx-admin-panel-kicker">Provision</div><div class="nx-admin-panel-title">Create a desk and mail access in one flow</div></div><div class="nx-admin-panel-note">Best practice: create with a temporary password, send it once, and move the desk to active operations immediately.</div></div><div class="nx-form-grid nx-admin-form-grid" style="margin-top:16px"><label class="nx-form-label nx-admin-form-label">Name<input id="nx-admin-user-name" class="nx-input nx-admin-input" placeholder="Desk User"></label><label class="nx-form-label nx-admin-form-label">Email<input id="nx-admin-user-email" class="nx-input nx-admin-input" placeholder="desk@client.com"></label></div><div class="nx-form-grid nx-admin-form-grid" style="margin-top:14px"><label class="nx-form-label nx-admin-form-label">Temporary Password<input id="nx-admin-user-password" class="nx-input nx-admin-input" placeholder="Welcome@123"></label><label class="nx-form-label nx-admin-form-label">Role<select id="nx-admin-user-role" class="nx-select nx-admin-input"><option>USER</option><option>ADMIN</option></select></label></div><div class="nx-admin-inline-toggle-row" style="margin-top:14px"><label class="nx-check"><input id="nx-admin-user-send-mail" type="checkbox" checked><span>Mail access immediately</span><span class="nx-check-hint">Sends the temporary password to the user via Gmail.</span></label><label class="nx-check"><input id="nx-admin-user-notify-email" type="checkbox" checked><span>Email alerts on</span></label></div><div class="nx-admin-password-tip">Search, archive, restore, credit, and reset passwords from the lifecycle panel on the right.</div><div class="nx-actions" style="margin-top:16px"><button class="nx-btn nx-btn-primary nx-btn-wide" onclick="nxAdminCreateUser()">Create & Mail Access</button></div></div>';
    const selectedCard = selected ? (
      '<div class="nx-admin-lifecycle-card nx-admin-detail-card"><div class="nx-admin-panel-top"><div><div class="nx-admin-panel-kicker">Selected Desk</div><div class="nx-admin-panel-title">'+escapeHtml(selected.full_name || selected.email)+'</div></div><div class="nx-admin-user-badges"><span class="nx-badge '+(String(selected.role||'').toUpperCase()==='ADMIN'?'warn':'cool')+'">'+escapeHtml(selected.role || 'USER')+'</span><span class="nx-badge '+(selectedIsArchived?'warn':(String(selected.status||'').toUpperCase()==='DISABLED'?'bad':'good'))+'">'+escapeHtml(selectedIsArchived ? 'ARCHIVED' : (selected.status || 'ACTIVE'))+'</span></div></div><div class="nx-admin-user-meta"><div class="nx-pay-chip"><span>Wallet</span><strong>'+fmtMoney(selectedWallet.balance || 0)+'</strong></div><div class="nx-pay-chip"><span>Last login</span><strong>'+escapeHtml(fmtDate(selected.last_login_at))+'</strong></div><div class="nx-pay-chip"><span>Updated</span><strong>'+escapeHtml(fmtDate(selected.updated_at))+'</strong></div><div class="nx-pay-chip"><span>Archive reason</span><strong>'+escapeHtml(selectedLifecycle.archive_reason || 'Active desk')+'</strong></div></div><div class="nx-admin-maint-grid"><label class="nx-form-label nx-admin-form-label">Full Name<input id="nx-admin-edit-name" class="nx-input nx-admin-input" value="'+escapeHtml(selected.full_name || '')+'"></label><label class="nx-form-label nx-admin-form-label">Email<input id="nx-admin-edit-email" class="nx-input nx-admin-input" value="'+escapeHtml(selected.email || '')+'"></label><label class="nx-form-label nx-admin-form-label">Role<select id="nx-admin-edit-role" class="nx-select nx-admin-input"><option'+(String(selected.role||'').toUpperCase()==='USER'?' selected':'')+'>USER</option><option'+(String(selected.role||'').toUpperCase()==='ADMIN'?' selected':'')+'>ADMIN</option></select></label><label class="nx-form-label nx-admin-form-label">Status<select id="nx-admin-edit-status" class="nx-select nx-admin-input"><option'+(String(selected.status||'').toUpperCase()==='ACTIVE'?' selected':'')+'>ACTIVE</option><option'+(String(selected.status||'').toUpperCase()==='LIMITED'?' selected':'')+'>LIMITED</option><option'+(String(selected.status||'').toUpperCase()==='DISABLED'?' selected':'')+'>DISABLED</option></select></label><label class="nx-form-label nx-admin-form-label">Wallet Type<select id="nx-admin-edit-wallet-type" class="nx-select nx-admin-input"><option'+(String(selectedWallet.type||'').toUpperCase()==='COUPON'?' selected':'')+'>COUPON</option><option'+(String(selectedWallet.type||'').toUpperCase()==='PAID'?' selected':'')+'>PAID</option></select></label><label class="nx-form-label nx-admin-form-label">WhatsApp<input id="nx-admin-edit-whatsapp" class="nx-input nx-admin-input" value="'+escapeHtml(selectedContacts.whatsapp_phone || '')+'" placeholder="+91 9876543210"></label><label class="nx-form-label nx-admin-form-label">Telegram<input id="nx-admin-edit-telegram" class="nx-input nx-admin-input" value="'+escapeHtml(selectedContacts.telegram_chat_id || '')+'" placeholder="123456789"></label><label class="nx-form-label nx-admin-form-label">Daily Loss<input id="nx-admin-edit-daily-loss" class="nx-input nx-admin-input" value="'+escapeHtml(String(selectedControls.daily_loss_limit || 0))+'"></label><label class="nx-form-label nx-admin-form-label">Max Trades / Day<input id="nx-admin-edit-max-trades" class="nx-input nx-admin-input" value="'+escapeHtml(String(selectedControls.max_trades_per_day || 0))+'"></label><label class="nx-form-label nx-admin-form-label">Max Open Signals<input id="nx-admin-edit-max-signals" class="nx-input nx-admin-input" value="'+escapeHtml(String(selectedControls.max_open_signals || 0))+'"></label><label class="nx-form-label nx-admin-form-label">Internal Notes<textarea id="nx-admin-edit-notes" class="nx-textarea nx-admin-input" placeholder="Relationship notes, desk state, archive reason, follow-up reminders">'+escapeHtml(selected.notes || '')+'</textarea></label></div><div class="nx-admin-inline-toggle-row"><label class="nx-check"><input id="nx-admin-edit-notify-email" type="checkbox"'+checkedAttr(!!selectedNotifications.email)+'><span>Email alerts</span></label><label class="nx-check"><input id="nx-admin-edit-notify-telegram" type="checkbox"'+checkedAttr(!!selectedNotifications.telegram)+'><span>Telegram alerts</span></label><label class="nx-check"><input id="nx-admin-edit-notify-whatsapp" type="checkbox"'+checkedAttr(!!selectedNotifications.whatsapp)+'><span>WhatsApp alerts</span></label><label class="nx-check"><input id="nx-admin-edit-token-reminder" type="checkbox"'+checkedAttr(!!selectedNotifications.token_reminder)+'><span>Token reminder</span></label><label class="nx-check"><input id="nx-admin-edit-send-mail" type="checkbox" checked><span>Notify via Gmail</span><span class="nx-check-hint">Used for password reset, archive, and restore actions.</span></label></div><div class="nx-actions" style="margin-top:16px"><button class="nx-btn nx-btn-primary" onclick="nxAdminSaveUser('+selected.id+')">Save User Changes</button><button class="nx-btn nx-btn-ghost" onclick="nxAdminCredit('+selected.id+')">Credit / Debit</button><button class="nx-btn nx-btn-gold" onclick="nxAdminResetUserPassword('+selected.id+')">Reset Password</button>'+(selectedIsArchived ? '<button class="nx-btn nx-btn-ghost" onclick="nxAdminRestoreUser('+selected.id+')">Restore User</button>' : '<button class="nx-btn nx-btn-ghost" onclick="nxAdminArchiveUser('+selected.id+')">Archive User</button>')+'</div></div>'
    ) : '<div class="nx-admin-lifecycle-card nx-admin-detail-card"><div class="nx-empty">Select a desk on the right to edit details, reset passwords, archive users, or restore archived desks.</div></div>';
    const listRows = visibleUsers.length ? visibleUsers.map(function(u){
      const role = String(u.role || 'USER').toUpperCase();
      const archived = !!(((u.lifecycle||{}).is_archived));
      const statusLabel = archived ? 'ARCHIVED' : String(u.status || 'ACTIVE').toUpperCase();
      const selectedRow = String(u.id) === String((selected||{}).id || '');
      return '<button class="nx-admin-user-row'+(selectedRow?' on':'')+'" onclick="nxAdminSelectUser('+u.id+')"><div class="nx-admin-user-row-main"><div class="nx-admin-user-row-name">'+escapeHtml(u.full_name || u.email)+'</div><div class="nx-admin-user-row-email">'+escapeHtml(u.email)+'</div><div class="nx-admin-user-row-meta">Updated '+escapeHtml(fmtDate(u.updated_at))+' · Last login '+escapeHtml(fmtDate(u.last_login_at))+'</div></div><div class="nx-admin-user-row-side"><span class="nx-badge '+(role==='ADMIN'?'warn':'cool')+'">'+escapeHtml(role)+'</span><span class="nx-badge '+(archived?'warn':(statusLabel==='DISABLED'?'bad':'good'))+'">'+escapeHtml(statusLabel)+'</span><strong class="nx-admin-wallet-value">'+fmtMoney((((u.wallet||{}).balance)||0))+'</strong></div></button>';
    }).join('') : '<div class="nx-empty">No desks match this search in the '+escapeHtml(view === 'archived' ? 'archived' : 'active')+' list.</div>';
    const historyRows = filteredHistory.length ? filteredHistory.map(function(item){
      return '<div class="nx-admin-history-row"><div class="nx-admin-history-top"><div><div class="nx-item-title">'+escapeHtml(item.title || item.event_type || 'History event')+'</div><div class="nx-item-sub">'+escapeHtml(((item.user||{}).email) || 'Unknown user')+' · '+escapeHtml(fmtDate(item.created_at))+'</div></div><span class="nx-badge cool">'+escapeHtml(item.event_type || 'EVENT')+'</span></div><div class="nx-admin-history-summary">'+escapeHtml(item.summary || 'Lifecycle activity logged from the admin desk.')+'</div><div class="nx-inline-note" style="margin-top:8px">Actor: '+escapeHtml((((item.actor||{}).full_name) || ((item.actor||{}).email) || 'System'))+'</div></div>';
    }).join('') : '<div class="nx-empty">History will appear here as users are created, edited, credited, archived, restored, or mailed.</div>';
    const usersCard = '<div class="nx-card nx-admin-lifecycle-shell"><div class="nx-card-head"><div><div class="nx-card-title">User Management</div><div class="nx-card-sub">End-to-end lifecycle desk for provisioning, search, archive, restore, wallet actions, and mailed credentials.</div></div><span class="nx-badge cool">'+fmtNum((a.metrics||{}).total_users||0)+' users</span></div><div class="nx-card-body"><div class="nx-admin-lifecycle-hero"><div class="nx-admin-users-copy"><div class="nx-admin-users-kicker">Lifecycle Desk</div><div class="nx-admin-users-title">Search fast, maintain clean active desks, and move old desks into archive without losing history.</div><div class="nx-admin-users-sub">Active and archived users live in separate lanes. The history tab records what happened, who acted, and when Gmail-backed account actions were triggered.</div></div><div class="nx-admin-users-stats"><div class="nx-pay-chip"><span>Active</span><strong>'+fmtNum((a.metrics||{}).active_users||activeUsers.length)+'</strong></div><div class="nx-pay-chip"><span>Archived</span><strong>'+fmtNum((a.metrics||{}).archived_users||archivedUsers.length)+'</strong></div><div class="nx-pay-chip"><span>History events</span><strong>'+fmtNum((a.metrics||{}).history_events||historyItems.length)+'</strong></div></div></div><div class="nx-admin-lifecycle-grid"><div class="nx-admin-lifecycle-left">'+createCard+selectedCard+'</div><div class="nx-admin-lifecycle-right"><div class="nx-admin-lifecycle-card nx-admin-registry-card"><div class="nx-admin-panel-top"><div><div class="nx-admin-panel-kicker">Registry</div><div class="nx-admin-panel-title">Searchable active, archived, and history views</div></div><div class="nx-admin-panel-note">Search across email, name, phone, telegram, notes, and archive reason. Tabs keep current users separate from old desks.</div></div><div class="nx-admin-registry-tools"><label class="nx-form-label nx-admin-form-label">Search<input id="nx-admin-user-search" class="nx-input nx-admin-input" placeholder="Search by email, name, phone, telegram, notes" value="'+escapeHtml(NX.adminUserQuery || '')+'" oninput="nxAdminSetUserSearch(this.value)"></label><div class="nx-admin-user-tabs"><button class="nx-tab-btn'+(view==='active'?' nx-btn-primary':'')+'" onclick="nxAdminSetUserView(\'active\')">Active <span>'+fmtNum(activeUsers.length)+'</span></button><button class="nx-tab-btn'+(view==='archived'?' nx-btn-primary':'')+'" onclick="nxAdminSetUserView(\'archived\')">Archived <span>'+fmtNum(archivedUsers.length)+'</span></button><button class="nx-tab-btn'+(view==='history'?' nx-btn-primary':'')+'" onclick="nxAdminSetUserView(\'history\')">History <span>'+fmtNum(historyItems.length)+'</span></button></div></div><div class="nx-admin-registry-meta">'+(view==='history' ? ('Showing '+fmtNum(filteredHistory.length)+' lifecycle events') : ('Showing '+fmtNum(visibleUsers.length)+' desks in '+escapeHtml(view)+' view'))+'</div><div class="nx-admin-registry-results">'+(view==='history' ? historyRows : listRows)+'</div></div></div></div></div></div>';
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
        '<div class="nx-workspace-rail-card nx-workspace-rail-metric"><div class="nx-workspace-rail-kicker">Users</div><div class="nx-workspace-rail-title">'+fmtNum((a.metrics||{}).total_users||0)+'</div><div class="nx-workspace-rail-sub">Active '+fmtNum((a.metrics||{}).active_users||0)+' · Archived '+fmtNum((a.metrics||{}).archived_users||0)+'</div></div>',
        '<div class="nx-workspace-rail-card nx-workspace-rail-metric"><div class="nx-workspace-rail-kicker">Payments</div><div class="nx-workspace-rail-title">'+((paymentProfile.upi_id && paymentProfile.enabled)?'UPI Live':'Setup')+'</div><div class="nx-workspace-rail-sub">'+escapeHtml(paymentProfile.upi_id || 'No UPI rail saved')+'</div></div>'
      ],
      headRight: '<div class="nx-pill-row"><span class="nx-pill">Revenue '+fmtMoney((a.metrics||{}).revenue||0)+'</span><span class="nx-pill">Archive '+fmtNum((a.metrics||{}).archived_users||0)+'</span><span class="nx-pill">'+(gmailReady ? 'Gmail Live' : 'Gmail Pending')+'</span></div>',
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
    mount.innerHTML = renderHero() + renderBody();
  }

  function safeRender(){
    const mount = el('nx-root');
    if(!mount) return;
    try{
      renderAll();
    }catch(err){
      mount.innerHTML = '<div style="padding:24px;color:#e5eefc;font-family:Aptos,Segoe UI,sans-serif"><div style="font-size:22px;font-weight:800;margin-bottom:8px">Nexus Control Hub</div><div style="font-size:14px;line-height:1.6;color:#b8c8e6">The panel hit a render issue. Refresh once, then open NEXUS again. If this keeps happening, the fallback is active and the APIs are still live.</div></div>';
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
  window.nxLogout = function(){ logout(); toast('Logged out'); };
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
        NX.brokerAssist = {
          type: status === 'CONNECTED' ? 'success' : (status === 'ERROR' ? 'error' : (status === 'CANCELLED' || status === 'TIMEOUT' ? 'info' : 'cool')),
          title: status === 'CONNECTED' ? 'One-time Kite login completed' : (status === 'WAITING' ? 'Complete login in the Kite window' : (status === 'STARTING' ? 'Opening Kite login' : (status === 'CANCELLED' ? 'Kite login was closed' : (status === 'TIMEOUT' ? 'Kite login timed out' : 'Kite login status')))),
          message: String(info.message || ''),
          detail: String(info.detail || '')
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
        NX.brokerAssist = {
          type:'cool',
          title:'Official Kite window opened',
          message:String(info.message || 'Complete login in the Kite window. Nexus will finish the connection automatically.'),
          detail:String(info.detail || '')
        };
        safeRender();
        toast('Kite login opened. Finish it in the browser window.');
        window._nxKiteInteractiveTimer = setTimeout(function(){ pollKiteInteractiveStatus({ silent:true }); }, 1500);
      }catch(err){
        NX.brokerAssist = { type:'error', title:'Could not open Kite login', message: err.message || 'Interactive Kite login failed to start' };
        safeRender();
        toast(err.message || 'Interactive Kite login failed to start');
      }
    });
  };
  window.nxKiteOpenLogin = function(){
    return runBusy('broker-open-login-url', async function(){
      try{
        const key = String(((el('nx-broker-api-key')||{}).value || '')).trim();
        const dash = (NX.dashboard || {}).broker || {};
        let url = String(dash.login_url || '').trim();
        if(!url){
          try{
            const info = await nxApi('/api/token-login-url', { method:'GET' });
            url = String((info || {}).login_url || '').trim();
          }catch(_){ }
        }
        if(!url && key){
          url = 'https://kite.zerodha.com/connect/login?api_key=' + encodeURIComponent(key);
        }
        if(!url){ toast('Need a Kite API key or backend/.env KITE_API_KEY first'); return; }
        window.open(url, '_blank', 'noopener');
        NX.brokerAssist = {
          type:'cool',
          title:'Official Kite login opened in your browser',
          message:'Log in there, then paste the final redirect URL or request_token into Manual fallback and click Save Desk & Verify.',
          detail:'Alternate path: run python generate_token.py locally, then click LOAD .ENV or Use Server Session. One-Time Kite Login still gives the cleanest experience when it is available.'
        };
        safeRender();
      }catch(err){
        toast(err.message || 'Could not open Kite login URL');
      }
    });
  };
  window.nxCheckServerTokenStatus = function(){
    return runBusy('broker-token-status', async function(){
      try{
        const data = await nxApi('/api/token-status', { method:'GET' });
        NX.brokerAssist = {
          type: data.valid ? 'success' : 'info',
          title: data.valid ? 'Shared Zerodha session is live' : (data.has_token ? 'Shared token needs attention' : 'No shared token loaded'),
          message: data.valid ? ('Logged in as ' + (data.user || 'Zerodha user') + '.') : (data.error || 'Token is not ready yet.'),
          detail: 'Server uptime ' + fmtNum(data.uptime_h || 0) + 'h'
        };
        safeRender();
        toast(data.valid ? 'Server Zerodha session is ready' : (data.error || 'Shared session not ready'));
      }catch(err){
        NX.brokerAssist = { type:'error', title:'Status check failed', message: err.message || 'Unable to read shared token status' };
        safeRender();
        toast(err.message || 'Unable to read shared token status');
      }
    });
  };
  window.nxImportEnvBrokerToken = function(){
    return runBusy('broker-import-env', async function(){
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
  window.nxBrokerConnect = function(){
    return runBusy('broker-connect', async function(){
      try{
        const payload = brokerPayload();
        const data = await nxApi('/api/user/broker/connect', { method:'POST', body: JSON.stringify(payload) });
        const b = (data || {}).broker || {};
        NX.brokerTest = {
          type: (b.status === 'CONNECTED' || b.status === 'READY') ? 'success' : 'error',
          message: (b.status === 'CONNECTED' || b.status === 'READY') ? 'Broker settings saved and connected' : 'Broker save completed but connection is not ready',
          detail: [((b.profile||{}).name || ''), ((b.profile||{}).user_id ? ('ID ' + (b.profile||{}).user_id) : ''), (b.broker_name || '')].filter(Boolean).join(' · ')
        };
        await nxApi('/api/user/controls', { method:'PATCH', body: JSON.stringify({ auto_execute: payload.auto_execute, daily_loss_limit: Number(((el('nx-user-daily-loss')||{}).value || 0)), max_trades_per_day: Number(((el('nx-user-max-trades')||{}).value || 0)), max_open_signals: Number(((el('nx-user-max-open')||{}).value || 0)) }) });
        await loadUserData();
        safeRender();
        toast(payload.live_mode ? 'Broker connected in live mode' : 'Broker settings saved');
        if((b.status === 'CONNECTED' || b.status === 'READY') && el('nx-broker-request-paste')) el('nx-broker-request-paste').value = '';
        setTimeout(function(){ window.nxBrokerCodeSync && window.nxBrokerCodeSync(); }, 20);
      }catch(err){
        NX.brokerTest = { type:'error', message:'Broker not connected', detail: err.message || 'Connection failed' };
        safeRender();
        toast(err.message);
      }
    });
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
        const symbol = String(window.prompt('Sample symbol (NSE equity).', 'SBIN') || '').trim().toUpperCase();
        if(!symbol) return;
        const qtyRaw = window.prompt('Quantity', String(Number(((el('nx-broker-qty')||{}).value || 1) || 1)));
        if(qtyRaw === null) return;
        const qty = Math.max(1, Number(qtyRaw || 1));
        const autoCancel = window.confirm('Auto-cancel sample order after placement? (Recommended)');
        const data = await nxApi('/api/user/broker/sample-order', { method:'POST', body: JSON.stringify({ symbol: symbol, quantity: qty, auto_cancel: autoCancel }) });
        const sample = (data || {}).sample || {};
        await loadUserData();
        safeRender();
        toast('Sample order ' + (sample.order_id || '') + ' · ' + (sample.status || 'processed') + (sample.variety ? (' · ' + String(sample.variety).toUpperCase()) : ''));
      }catch(err){
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
  window.nxAdminSetUserView = function(view){
    NX.adminUserView = String(view || 'active').trim().toLowerCase();
    localStorage.setItem('nx_admin_user_view', NX.adminUserView);
    safeRender();
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
          notify_email: !!((el('nx-admin-user-notify-email')||{}).checked),
          send_email: !!((el('nx-admin-user-send-mail')||{}).checked)
        })
      });
      NX.adminUserView = 'active';
      localStorage.setItem('nx_admin_user_view', NX.adminUserView);
      await loadAdminData();
      NX.selectedAdminUserId = String(((data.user||{}).id) || '');
      if(NX.selectedAdminUserId) localStorage.setItem('nx_admin_selected_user_id', NX.selectedAdminUserId);
      safeRender();
      toast((data.emailed ? 'Access mailed: ' : 'User created: ') + (((data.user||{}).email) || ''));
      if(data.temporary_password) window.prompt('Temporary password for ' + (((data.user||{}).email) || 'user'), data.temporary_password);
    }catch(err){ toast(err.message); }
  };
  window.nxAdminSaveUser = async function(userId){
    try{
      const data = await nxApi('/api/admin/users/'+userId, {
        method:'PATCH',
        body: JSON.stringify({
          full_name:(el('nx-admin-edit-name')||{}).value,
          email:(el('nx-admin-edit-email')||{}).value,
          role:(el('nx-admin-edit-role')||{}).value,
          status:(el('nx-admin-edit-status')||{}).value,
          wallet_type:(el('nx-admin-edit-wallet-type')||{}).value,
          whatsapp_phone:(el('nx-admin-edit-whatsapp')||{}).value,
          telegram_chat_id:(el('nx-admin-edit-telegram')||{}).value,
          daily_loss_limit:Number((el('nx-admin-edit-daily-loss')||{}).value||0),
          max_trades_per_day:Number((el('nx-admin-edit-max-trades')||{}).value||0),
          max_open_signals:Number((el('nx-admin-edit-max-signals')||{}).value||0),
          notes:(el('nx-admin-edit-notes')||{}).value,
          notify_email: !!((el('nx-admin-edit-notify-email')||{}).checked),
          notify_telegram: !!((el('nx-admin-edit-notify-telegram')||{}).checked),
          notify_whatsapp: !!((el('nx-admin-edit-notify-whatsapp')||{}).checked),
          notify_token_reminder: !!((el('nx-admin-edit-token-reminder')||{}).checked)
        })
      });
      await loadAdminData();
      NX.selectedAdminUserId = String(((data.user||{}).id) || userId || '');
      safeRender();
      toast('User changes saved');
    }catch(err){ toast(err.message); }
  };
  window.nxAdminCredit = async function(userId){ const amount = window.prompt('Credit amount (use negative to debit)', '500'); if(amount===null) return; try{ await nxApi('/api/admin/users/'+userId+'/wallet/credit', { method:'POST', body: JSON.stringify({ amount: Number(amount||0), note:'Admin control hub adjustment' }) }); await loadAdminData(); safeRender(); toast('Wallet updated'); }catch(err){ toast(err.message); } };
  window.nxAdminResetUserPassword = async function(userId){
    const sendMail = !!((el('nx-admin-edit-send-mail')||{}).checked);
    const password = window.prompt('Temporary password (leave blank to auto-generate)', '');
    if(password===null) return;
    try{
      const data = await nxApi('/api/admin/users/'+userId+'/password-reset', {
        method:'POST',
        body: JSON.stringify({ password: password, send_email: sendMail })
      });
      await loadAdminData();
      safeRender();
      if(data.temporary_password) window.prompt('Temporary password ready', data.temporary_password);
      toast(data.emailed ? 'Password reset and mailed' : 'Temporary password reset');
    }catch(err){ toast(err.message); }
  };
  window.nxAdminArchiveUser = async function(userId){
    const reason = window.prompt('Archive reason', 'Client inactive / old desk');
    if(reason===null) return;
    try{
      const data = await nxApi('/api/admin/users/'+userId+'/archive', {
        method:'POST',
        body: JSON.stringify({ reason: reason, send_email: !!((el('nx-admin-edit-send-mail')||{}).checked) })
      });
      NX.adminUserView = 'archived';
      localStorage.setItem('nx_admin_user_view', NX.adminUserView);
      await loadAdminData();
      NX.selectedAdminUserId = String(((data.user||{}).id) || userId || '');
      safeRender();
      toast(data.emailed ? 'User archived and notified' : 'User archived');
    }catch(err){ toast(err.message); }
  };
  window.nxAdminRestoreUser = async function(userId){
    const note = window.prompt('Restore note', 'Reactivating desk');
    if(note===null) return;
    try{
      const data = await nxApi('/api/admin/users/'+userId+'/restore', {
        method:'POST',
        body: JSON.stringify({ note: note, send_email: !!((el('nx-admin-edit-send-mail')||{}).checked) })
      });
      NX.adminUserView = 'active';
      localStorage.setItem('nx_admin_user_view', NX.adminUserView);
      await loadAdminData();
      NX.selectedAdminUserId = String(((data.user||{}).id) || userId || '');
      safeRender();
      toast(data.emailed ? 'User restored and notified' : 'User restored');
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

  window.nxEnter = function(){ window.openNexusModal(); };

  safeRender();
  document.addEventListener('DOMContentLoaded', function(){ if(el('nx-root')) safeRender(); });
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
