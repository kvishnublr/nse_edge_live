// NSE EDGE v5 — Service Worker
const CACHE = 'nse-edge-v1';
const STATIC = ['/', '/index.html'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(STATIC)).catch(()=>{}));
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(ks => Promise.all(ks.filter(k=>k!==CACHE).map(k=>caches.delete(k)))));
  self.clients.claim();
});

self.addEventListener('fetch', e => {
  // Only cache GET requests for static assets; pass through API calls
  if(e.request.method !== 'GET') return;
  if(e.request.url.includes('/api/')) return;
  if(e.request.url.includes('/ws')) return;
  e.respondWith(
    caches.match(e.request).then(cached => cached || fetch(e.request).then(res => {
      if(res && res.status === 200 && res.type === 'basic'){
        const clone = res.clone();
        caches.open(CACHE).then(c => c.put(e.request, clone));
      }
      return res;
    })).catch(() => caches.match('/index.html'))
  );
});
