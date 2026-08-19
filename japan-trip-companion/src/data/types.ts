/**
 * Data model for the Japan Trip Companion.
 *
 * Everything the UI renders comes from the objects in /src/data.
 * Editing the trip means editing data — never a component.
 *
 * Provenance convention used throughout:
 *   source: 'pdf'       -> taken verbatim from Japan_Trip_Itinerary_Sep_2026.pdf. Do not change.
 *   source: 'research'   -> verified against an official/reputable source in Aug 2026.
 *   source: 'unverified' -> a sensible suggestion; shown in the UI with a "check before relying on it" hint.
 */

export type Source = 'pdf' | 'research' | 'unverified'

export type CityId =
  | 'tokyo'
  | 'kamikochi'
  | 'hirayu'
  | 'takayama'
  | 'shirakawago'
  | 'kanazawa'
  | 'kyoto'
  | 'nara'
  | 'osaka'
  | 'kix'

export interface City {
  id: CityId
  name: string
  /** Used for weather lookups and map centring. */
  lat: number
  lon: number
  /** Typical late-September conditions. Fallback only — clearly labelled in the UI. */
  septemberNormal: { highC: number; lowC: number; note: string }
}

/** How much of the day an activity should claim. */
export type Priority = 'core' | 'optional' | 'if-time'

export type ActivityKind =
  | 'sight'
  | 'walk'
  | 'shopping'
  | 'food'
  | 'transport'
  | 'hotel'
  | 'free'
  | 'logistics'

export interface Activity {
  id: string
  /** 24h local time, "HH:MM". Omit for items with no anchor time in the PDF. */
  time?: string
  /**
   * True when the clock time is our suggestion for pacing rather than something
   * the PDF fixed. The UI marks these with a tilde so a real 08:50 bus never
   * looks the same as a suggested 10:00 temple.
   */
  timeIsSuggested?: boolean
  title: string
  /** One-line subtitle shown collapsed. */
  subtitle?: string
  kind: ActivityKind
  priority: Priority
  /** Longer body, shown expanded. Keep to 1-3 short sentences. */
  description?: string
  /** e.g. "1.5-2 h" */
  duration?: string
  /** Free-text; only filled where verified. */
  hours?: string
  /** Free-text; only filled where verified. */
  price?: string
  /** True only where a real timed-ticket/reservation rule was verified. */
  bookingRecommended?: boolean
  bookingNote?: string
  nearestStation?: string
  /** How to get here from the previous stop. */
  gettingThere?: string
  /** Short practical tip. */
  tip?: string
  /** Search string for Google Maps. Prefer this over inventing a place URL. */
  mapsQuery?: string
  /** Official site only. Never a guess. */
  website?: string
  /** Official ticketing page. */
  ticketsUrl?: string
  /** Where the facts above came from. */
  source: Source
  /** Fields we could not confirm — surfaced in the UI as a review flag. */
  reviewFlag?: string
  /** Links this activity to a transport leg in transport.ts. */
  transportId?: string
}

export type ReservationStatus =
  | 'booked'
  | 'no-reservation'
  | 'book-later'
  | 'local'

export interface TransportLeg {
  id: string
  /** ISO date, YYYY-MM-DD */
  date: string
  service: string
  mode: 'train' | 'bus' | 'shuttle' | 'walk' | 'local-rail'
  from: string
  to: string
  /** "HH:MM" or undefined when the PDF gives no fixed time. */
  depart?: string
  arrive?: string
  /** Free-text when the PDF is deliberately approximate. */
  departNote?: string
  arriveNote?: string
  status: ReservationStatus
  /** Rendered as a vertical transfer diagram when present. */
  transfer?: {
    at: string
    firstLeg: string
    secondLeg: string
  }
  /** Minutes to allow at the station before departure. */
  arriveAtStationMinutes?: number
  durationNote?: string
  platformNote?: string
  officialUrl?: string
  note?: string
  source: Source
  reviewFlag?: string
}

export interface Hotel {
  id: string
  name: string
  city: CityId
  cityLabel: string
  /** ISO check-in / check-out dates. */
  checkIn: string
  checkOut: string
  nights: number
  status: 'confirmed'
  goodFor: string[]
  locationNote: string
  nearestStation?: string
  checkInTime?: string
  checkOutTime?: string
  mapsQuery: string
  website?: string
  source: Source
  reviewFlag?: string
}

export type BookingBucket = 'confirmed' | 'should-book' | 'optional' | 'none'

export interface BookingItem {
  id: string
  title: string
  detail: string
  bucket: BookingBucket
  /** ISO date it relates to, when relevant. */
  date?: string
  why: string
  officialUrl?: string
  urlLabel?: string
  source: Source
}

export type MealSlot = 'quick' | 'local' | 'dinner' | 'special' | 'cafe'

/** Our two hard rules: no pork, no seafood. */
export type DietVerdict = 'good' | 'ask' | 'avoid'

export interface Restaurant {
  id: string
  name: string
  city: CityId
  cuisine: string
  slot: MealSlot
  /** ¥ .. ¥¥¥¥ */
  price: string
  why: string
  neighbourhood: string
  mapsQuery: string
  website?: string
  reservation: 'recommended' | 'walk-in' | 'required'
  /** Plain-language note about pork/seafood. */
  dietNote: string
  dietVerdict: DietVerdict
  source: Source
}

export interface ShoppingSpot {
  id: string
  name: string
  city: CityId
  area: string
  what: string
  mapsQuery: string
  note?: string
  source: Source
}

export type NoticeTone = 'warning' | 'info' | 'luggage' | 'crowd'

export interface Notice {
  id: string
  tone: NoticeTone
  title: string
  body: string
  source: Source
}

export interface MapPin {
  label: string
  kind: 'hotel' | 'sight' | 'food' | 'station' | 'terminal'
  query: string
}

export interface Day {
  /** ISO date, YYYY-MM-DD */
  date: string
  /** e.g. "Wed" */
  dow: string
  /** Route line, e.g. "Kanazawa -> Kyoto". */
  route: string
  cities: CityId[]
  /**
   * Which city's weather actually matters today — the place you spend the day,
   * not necessarily the last one on the route line. On 18 Sep that is Kanazawa
   * even though you sleep in Kyoto.
   */
  weatherCity: CityId
  /** Short evocative label, e.g. "Old Town & Hida beef". */
  theme: string
  /** One sentence: what today is really about. */
  headline: string
  hotelId: string | null
  activities: Activity[]
  /** ids into transport.ts, in order. */
  transportIds: string[]
  notices: Notice[]
  restaurantIds: string[]
  shoppingIds: string[]
  /** Extra pins beyond those derived from activities. */
  extraPins?: MapPin[]
}
