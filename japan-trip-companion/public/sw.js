/**
 * Minimal offline shell.
 *
 * Strategy: cache-first for the app's own files (so the itinerary opens with
 * no signal), network-only for everything else — weather, Maps, official sites
 * should never be served stale.
 */
const CACHE = 'jtc-v1'

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE).then((cache) => cache.addAll(['./', './index.html'])).then(() =>
      self.skipWaiting(),
    ),
  )
})

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim()),
  )
})

self.addEventListener('fetch', (event) => {
  const { request } = event
  if (request.method !== 'GET') return

  const url = new URL(request.url)
  if (url.origin !== self.location.origin) return // weather, maps, fonts: straight to network

  event.respondWith(
    caches.match(request).then((hit) => {
      if (hit) {
        // Refresh in the background so the next launch is current.
        event.waitUntil(
          fetch(request)
            .then((res) => caches.open(CACHE).then((c) => c.put(request, res.clone())))
            .catch(() => {}),
        )
        return hit
      }
      return fetch(request)
        .then((res) => {
          const copy = res.clone()
          event.waitUntil(caches.open(CACHE).then((c) => c.put(request, copy)).catch(() => {}))
          return res
        })
        .catch(() =>
          request.mode === 'navigate' ? caches.match('./index.html') : Promise.reject(),
        )
    }),
  )
})
