/**
 * What each stay actually cost, and until when it can still be cancelled.
 *
 * Source: the booking confirmations, transcribed. Amounts are held in the
 * currency the confirmation states and converted for display, so when the rate
 * moves there is exactly one constant to change.
 *
 * Deliberately NOT stored here: booking IDs and reference numbers. A reference
 * plus a surname is enough to alter or cancel a reservation on most platforms,
 * and this app gets published to a URL. Enter those in More → Confirmations,
 * which keeps them on your device only. Property phone numbers are included —
 * those are published on the hotels' own sites.
 */

export type Platform = 'agoda' | 'japanican' | 'booking' | 'direct' | 'unknown'

/** When the money actually leaves. */
export type PaymentTiming = 'pay-later' | 'on-arrival' | 'prepaid'

export interface Reservation {
  id: string
  /** Joins to hotels.ts. */
  hotelId: string
  /** The property name exactly as the confirmation states it. */
  propertyAsBooked: string
  platform: Platform
  checkIn: string
  checkOut: string
  nights: number
  /** Currency the confirmation quotes. */
  currency: 'JPY' | 'CNY' | 'USD'
  /** Total for the whole stay, in that currency. */
  amount: number
  /** Last date on which cancelling is still free. Undefined when unknown. */
  freeCancelUntil?: string
  cancelNote: string
  payment: PaymentTiming
  roomType?: string
  promotion?: string
  phone?: string
  addressAsBooked?: string
  /** Costs the confirmation says are charged separately at the property. */
  extras?: string[]
  /**
   * A real disagreement between this confirmation and the itinerary PDF.
   * Rendered loudly — these are the things worth acting on today.
   */
  conflict?: string
}

/**
 * Rates used for the approximate USD figures.
 * Checked 21 August 2026. Update the two numbers and the label together.
 */
export const fx = {
  asOf: '21 Aug 2026',
  /** Yen per US dollar. */
  jpyPerUsd: 159.0,
  /** US dollars per yuan. */
  usdPerCny: 0.1487,
}

export function toUsd(amount: number, currency: Reservation['currency']): number {
  if (currency === 'USD') return amount
  if (currency === 'JPY') return amount / fx.jpyPerUsd
  return amount * fx.usdPerCny
}

export const reservations: Reservation[] = [
  {
    id: 'res-bespoke',
    hotelId: 'bespoke-shinjuku',
    propertyAsBooked: 'Bespoke Hotel Shinjuku',
    platform: 'agoda',
    checkIn: '2026-09-09',
    checkOut: '2026-09-14',
    nights: 5,
    currency: 'CNY',
    amount: 5362.31,
    freeCancelUntil: '2026-09-07',
    cancelNote:
      'Free until 7 Sep. Cancelling within 2 days of arrival is charged for the entire stay.',
    payment: 'pay-later',
    roomType: 'Deluxe Double Room',
    phone: '+81 3 6278 9559',
    addressAsBooked: 'Kabukicho 2-2-17, Shinjuku, Tokyo 160-0021',
    conflict:
      'This booking runs 9 to 14 September — five nights. The itinerary says six nights, 9 to 15, and you leave the hotel at 06:15 on the 15th for the Azusa. As booked, the night of 14-15 September is not covered. Either extend by a night or there is nowhere to sleep before the Alps day.',
  },
  {
    id: 'res-takayama',
    hotelId: 'takayama-ouan',
    propertyAsBooked: 'Tokyu Stay Hida Takayama Musubinoyu',
    platform: 'agoda',
    checkIn: '2026-09-16',
    checkOut: '2026-09-17',
    nights: 1,
    currency: 'JPY',
    amount: 15904,
    freeCancelUntil: '2026-09-14',
    cancelNote:
      'Free until 14 Sep. Cancelling within 2 days of arrival is charged for the entire stay.',
    payment: 'pay-later',
    roomType: 'Superior Twin Room, non-smoking',
    promotion: 'Early Booking Saver — includes 8% discount',
    phone: '+81 577 36 1109',
    addressAsBooked: '4-301 Hanasatomachi, Takayama 506-0026',
    extras: ['Bathing tax about USD 1.88, paid at the property'],
    conflict:
      'The itinerary PDF names Takayama Ouan for this night. This confirmation is for Tokyu Stay Hida Takayama Musubinoyu — a different hotel, same dates. Both are near the station. Tell me which is right and I will correct the itinerary.',
  },
  {
    id: 'res-soki',
    hotelId: 'soki-kanazawa',
    propertyAsBooked: 'SOKI KANAZAWA',
    platform: 'japanican',
    checkIn: '2026-09-17',
    checkOut: '2026-09-18',
    nights: 1,
    currency: 'JPY',
    amount: 15963,
    freeCancelUntil: '2026-09-15',
    cancelNote: 'Free until 15 Sep. 20% from 16 Sep, 80% on 17 Sep.',
    payment: 'on-arrival',
    phone: '076-210-0270',
    addressAsBooked: '2-1 Fukuro-machi, Kanazawa, Ishikawa',
    extras: [
      'JPY 17,600 before a JPY 1,637 coupon',
      'Bath tax and accommodation tax may be charged at the property',
      'The hotel is cashless only',
    ],
  },
  {
    id: 'res-resol-kyoto',
    hotelId: 'resol-trinity-kyoto',
    propertyAsBooked: 'Hotel Resol Trinity Kyoto',
    platform: 'booking',
    checkIn: '2026-09-18',
    checkOut: '2026-09-23',
    nights: 5,
    currency: 'JPY',
    amount: 95453,
    freeCancelUntil: '2026-09-17',
    cancelNote:
      'Free until 17 Sep, one day before arrival. Cancelling inside that day is 50% of the total.',
    payment: 'pay-later',
    roomType: 'Large Double Room · one king bed · tatami · no meals included',
    phone: '+81 75 211 9269',
    addressAsBooked: 'Fuyachodori Oikeagaru Kamihakusancho 249, Nakagyo-ku, Kyoto 604-0943',
    extras: [
      'JPY 104,998 before a JPY 9,545 Booking.com contribution',
      // Kyoto's lodging tax was restructured on 1 March 2026 into five bands.
      // This room is about JPY 19,100 a night, which falls in the JPY 6,000
      // to 19,999 band at JPY 400 per person per night — so roughly JPY 4,000
      // for two people over five nights. Worth checking at the desk: the
      // pre-discount rate is only just under the next band up.
      'Kyoto accommodation tax, paid at the property — around JPY 4,000 total for two of you over five nights',
      'Public bathing areas may be closed to guests with visible tattoos',
    ],
  },
  {
    id: 'res-forza',
    hotelId: 'forza-osaka-namba',
    propertyAsBooked: 'Hotel Forza Osaka Namba Dotonbori',
    platform: 'agoda',
    checkIn: '2026-09-23',
    checkOut: '2026-09-25',
    nights: 2,
    currency: 'JPY',
    amount: 38133,
    freeCancelUntil: '2026-09-22',
    cancelNote: 'Free until 22 Sep. 80% within 1 day of arrival.',
    payment: 'pay-later',
    roomType: 'Standard Double Room for 2 adults',
    promotion: 'Super Saver',
    phone: '+81 6 6214 3111',
    addressAsBooked: '3F 1-4-22 Dotonbori, Namba, Osaka 542-0071',
  },
]

/** Stays with no confirmation on file yet. */
export const missingReservations: { hotelId: string; note: string }[] = [
  {
    hotelId: 'miyama-ouan',
    note:
      'No confirmation received. This is the ryokan night on 15-16 September, with dinner and the onsen — and the one where the no-pork, no-seafood request has to be on record.',
  },
]

export const platformLabel: Record<Platform, string> = {
  agoda: 'Agoda',
  japanican: 'Japanican',
  booking: 'Booking.com',
  direct: 'Booked direct',
  unknown: 'Unknown platform',
}

/** Where to manage a booking on each platform. */
export const platformUrl: Partial<Record<Platform, string>> = {
  agoda: 'https://www.agoda.com/account/bookings.html',
  japanican: 'https://www.japanican.com/en/mypage/',
  booking: 'https://secure.booking.com/mytrips.html',
}

export const paymentLabel: Record<PaymentTiming, string> = {
  'pay-later': 'Pay later',
  'on-arrival': 'Pay on arrival',
  prepaid: 'Already paid',
}

export const reservationForHotel = (hotelId: string) =>
  reservations.find((r) => r.hotelId === hotelId)

/** Totals across everything we have a price for. */
export function knownTotals() {
  const usd = reservations.reduce((sum, r) => sum + toUsd(r.amount, r.currency), 0)
  const nights = reservations.reduce((sum, r) => sum + r.nights, 0)
  return { usd, nights, perNight: nights ? usd / nights : 0, count: reservations.length }
}
