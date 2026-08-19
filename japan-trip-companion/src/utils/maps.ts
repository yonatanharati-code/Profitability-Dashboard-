/**
 * We never invent a place URL. Everything is either a verified official site
 * (held in the data files) or a Google Maps search / directions deep link
 * built here, which always resolves to something sensible on a phone.
 */

export function mapsSearch(query: string): string {
  return `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(query)}`
}

export function mapsDirections(destination: string, origin?: string): string {
  const params = new URLSearchParams({ api: '1', destination })
  if (origin) params.set('origin', origin)
  params.set('travelmode', 'transit')
  return `https://www.google.com/maps/dir/?${params.toString()}`
}

/**
 * A multi-stop route through today's places. Google Maps takes up to nine
 * waypoints between origin and destination, so we cap it and let the caller
 * know nothing was silently dropped.
 */
export function mapsRoute(stops: string[]): { url: string; used: number; dropped: number } {
  const clean = stops.map((s) => s.trim()).filter(Boolean)
  if (clean.length === 0) return { url: mapsSearch('Japan'), used: 0, dropped: 0 }
  if (clean.length === 1) return { url: mapsSearch(clean[0]), used: 1, dropped: 0 }

  const MAX = 11 // origin + 9 waypoints + destination
  const used = clean.slice(0, MAX)
  const origin = used[0]
  const destination = used[used.length - 1]
  const waypoints = used.slice(1, -1)

  const params = new URLSearchParams({ api: '1', origin, destination, travelmode: 'transit' })
  if (waypoints.length) params.set('waypoints', waypoints.join('|'))

  return {
    url: `https://www.google.com/maps/dir/?${params.toString()}`,
    used: used.length,
    dropped: clean.length - used.length,
  }
}
