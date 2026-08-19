import type { Activity, Day, MapPin } from '../data/types'
import { transportById } from '../data/transport'
import { hotelById } from '../data/hotels'
import { restaurantById } from '../data/restaurants'
import { minutesFromHHMM } from './date'

/**
 * Applies the user's saved ordering to a day's activities.
 * Ids we no longer recognise are dropped; activities the stored order does not
 * mention are appended, so editing the data file never loses an item.
 */
export function orderedActivities(day: Day, storedOrder?: string[]): Activity[] {
  if (!storedOrder?.length) return day.activities
  const byId = new Map(day.activities.map((a) => [a.id, a]))
  const out: Activity[] = []
  storedOrder.forEach((id) => {
    const hit = byId.get(id)
    if (hit) {
      out.push(hit)
      byId.delete(id)
    }
  })
  byId.forEach((a) => out.push(a))
  return out
}

export interface DayProgress {
  total: number
  done: number
  /** 0-100 */
  percent: number
}

export function dayProgress(
  activities: Activity[],
  completed: Record<string, true>,
  skipped: Record<string, true>,
): DayProgress {
  const live = activities.filter((a) => !skipped[a.id])
  const done = live.filter((a) => completed[a.id]).length
  return {
    total: live.length,
    done,
    percent: live.length === 0 ? 0 : Math.round((done / live.length) * 100),
  }
}

/**
 * The next thing that has not been done yet — what the "What's next" card shows.
 * Prefers the next timed item still ahead of the clock; otherwise the first
 * outstanding item in the day's order.
 */
export function nextUp(
  activities: Activity[],
  completed: Record<string, true>,
  skipped: Record<string, true>,
  nowMinutes: number | null,
): Activity | null {
  const live = activities.filter((a) => !skipped[a.id] && !completed[a.id])
  if (live.length === 0) return null
  if (nowMinutes == null) return live[0]

  const timedAhead = live
    .filter((a) => a.time && minutesFromHHMM(a.time) >= nowMinutes - 15)
    .sort((a, b) => minutesFromHHMM(a.time!) - minutesFromHHMM(b.time!))

  return timedAhead[0] ?? live[0]
}

/** The next departure today that has a real clock time. */
export function nextTransport(day: Day, nowMinutes: number | null) {
  const legs = day.transportIds.map(transportById).filter(Boolean) as NonNullable<
    ReturnType<typeof transportById>
  >[]
  if (legs.length === 0) return null
  if (nowMinutes == null) return legs[0]
  const ahead = legs.filter((l) => l.depart && minutesFromHHMM(l.depart) >= nowMinutes - 10)
  return ahead[0] ?? legs[legs.length - 1]
}

/**
 * Every place worth a pin today: hotel, stations and terminals from the
 * transport legs, each activity with a map query, and the day's food picks.
 */
export function pinsForDay(day: Day, activities: Activity[]): MapPin[] {
  const pins: MapPin[] = []
  const seen = new Set<string>()

  const push = (pin: MapPin) => {
    const key = pin.query.toLowerCase()
    if (seen.has(key)) return
    seen.add(key)
    pins.push(pin)
  }

  const hotel = hotelById(day.hotelId)
  if (hotel) push({ label: hotel.name, kind: 'hotel', query: hotel.mapsQuery })

  day.transportIds.forEach((id) => {
    const leg = transportById(id)
    if (!leg) return
    const kind = leg.mode === 'bus' || leg.mode === 'shuttle' ? 'terminal' : 'station'
    push({ label: leg.from, kind, query: leg.from })
    push({ label: leg.to, kind, query: leg.to })
  })

  activities.forEach((a) => {
    if (!a.mapsQuery) return
    if (a.kind === 'hotel' || a.kind === 'transport' || a.kind === 'logistics') return
    push({ label: a.title, kind: a.kind === 'food' ? 'food' : 'sight', query: a.mapsQuery })
  })

  day.restaurantIds.forEach((id) => {
    const r = restaurantById(id)
    if (r) push({ label: r.name, kind: 'food', query: r.mapsQuery })
  })

  day.extraPins?.forEach(push)

  return pins
}

/** Stops for the "view today's route" link — sights and walks, in order. */
export function routeStops(day: Day, activities: Activity[]): string[] {
  const hotel = hotelById(day.hotelId)
  const stops = activities
    .filter((a) => a.mapsQuery && ['sight', 'walk', 'shopping'].includes(a.kind))
    .map((a) => a.mapsQuery!)
  if (hotel && stops.length) return [hotel.mapsQuery, ...stops]
  return stops
}
