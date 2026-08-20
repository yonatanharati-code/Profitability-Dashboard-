import { trip } from '../data/trip'

export const JAPAN_TZ = 'Asia/Tokyo'

/** "YYYY-MM-DD" for a Date, as seen in Japan. */
export function isoInJapan(d: Date = new Date()): string {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: JAPAN_TZ,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(d)
  return parts
}

/** "HH:MM" in Japan. */
export function timeInJapan(d: Date = new Date()): string {
  return new Intl.DateTimeFormat('en-GB', {
    timeZone: JAPAN_TZ,
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(d)
}

/** "HH:MM:SS" in Japan. */
export function clockInJapan(d: Date = new Date()): string {
  return new Intl.DateTimeFormat('en-GB', {
    timeZone: JAPAN_TZ,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(d)
}

/** Minutes since midnight in Japan. */
export function minutesInJapan(d: Date = new Date()): number {
  const [h, m] = timeInJapan(d).split(':').map(Number)
  return h * 60 + m
}

export function minutesFromHHMM(hhmm: string): number {
  const [h, m] = hhmm.split(':').map(Number)
  return h * 60 + m
}

export function hhmmFromMinutes(total: number): string {
  const wrapped = ((total % 1440) + 1440) % 1440
  const h = Math.floor(wrapped / 60)
  const m = wrapped % 60
  return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`
}

function utcFromIso(iso: string): number {
  const [y, m, d] = iso.split('-').map(Number)
  return Date.UTC(y, m - 1, d)
}

/** Difference in whole days between two ISO dates. */
export function daysBetween(fromIso: string, toIso: string): number {
  return Math.round((utcFromIso(toIso) - utcFromIso(fromIso)) / 86400000)
}

export type DayStatus = 'past' | 'today' | 'upcoming'

export function statusForDate(date: string, today: string): DayStatus {
  if (date === today) return 'today'
  return date < today ? 'past' : 'upcoming'
}

/** "SEPTEMBER 18" */
export function bigDateLabel(iso: string): string {
  const [, m, d] = iso.split('-').map(Number)
  const month = [
    'JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE',
    'JULY', 'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER',
  ][m - 1]
  return `${month} ${d}`
}

/** "Sep 18" */
export function shortDateLabel(iso: string): string {
  const [, m, d] = iso.split('-').map(Number)
  const month = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][m - 1]
  return `${month} ${d}`
}

/** "18/9" — the format the PDF uses. */
export function pdfDateLabel(iso: string): string {
  const [, m, d] = iso.split('-').map(Number)
  return `${d}/${m}`
}

/**
 * Which day of the trip to show.
 * Before the trip we show day 1; after it, the last day. During it, today.
 */
export function resolveActiveDate(today: string): string {
  if (today < trip.startDate) return trip.startDate
  if (today > trip.endDate) return trip.endDate
  return today
}

export function isDuringTrip(today: string): boolean {
  return today >= trip.startDate && today <= trip.endDate
}

/** "in 3 h 12 m", "in 14 m", "now" — for a HH:MM later today. */
export function untilLabel(targetHHMM: string, nowMinutes: number): string | null {
  const diff = minutesFromHHMM(targetHHMM) - nowMinutes
  if (diff < -30) return null
  if (diff < 0) return 'now'
  if (diff === 0) return 'now'
  if (diff < 60) return `in ${diff} min`
  const h = Math.floor(diff / 60)
  const m = diff % 60
  return m === 0 ? `in ${h} h` : `in ${h} h ${m} min`
}

/** Days remaining until the outbound flight leaves. */
export function daysUntilTrip(today: string): number {
  return daysBetween(today, trip.departureDate)
}
