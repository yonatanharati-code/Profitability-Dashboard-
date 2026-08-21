import { AlertTriangle, CalendarX, MapPin, Phone, Search, Sparkles } from 'lucide-react'
import type { Reservation } from '../data/reservations'
import {
  paymentLabel, platformLabel, platformUrl, toUsd, fx,
} from '../data/reservations'
import { hotelById } from '../data/hotels'
import { hotelPriceSearch, mapsSearch } from '../utils/maps'
import { Ext } from './ExternalLink'
import { LinkButton, Pill } from './ui'
import { daysBetween, shortDateLabel } from '../utils/date'

/**
 * One stay: what it cost, how long you can still walk away, and a way to check
 * whether the same nights are cheaper somewhere else.
 *
 * The cancellation deadline is the reason this screen exists. Up to that date a
 * better price is free money; after it, looking is just annoying. So the card
 * leads with how many days are left and hides the compare button once the
 * window has closed.
 */
export function ReservationCard({
  reservation: r,
  today,
}: {
  reservation: Reservation
  today: string
}) {
  const hotel = hotelById(r.hotelId)
  const usd = toUsd(r.amount, r.currency)
  const perNight = usd / r.nights

  const daysLeft = r.freeCancelUntil ? daysBetween(today, r.freeCancelUntil) : null
  const stillFree = daysLeft != null && daysLeft >= 0
  const nameMismatch = hotel && hotel.name !== r.propertyAsBooked

  return (
    <article className="card overflow-hidden">
      <div className="px-4 pt-4">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <p className="eyebrow">
              {hotel?.cityLabel ?? '—'} · {platformLabel[r.platform]}
            </p>
            <h3 className="mt-1 text-[15px] font-semibold leading-snug text-sumi-800">
              {r.propertyAsBooked}
            </h3>
            <p className="tabular mt-1 text-[12.5px] text-sumi-500">
              {shortDateLabel(r.checkIn)} → {shortDateLabel(r.checkOut)}
              <span className="ml-1.5 text-sumi-400">
                · {r.nights} {r.nights === 1 ? 'night' : 'nights'}
              </span>
            </p>
          </div>
          <Pill tone={r.payment === 'prepaid' ? 'sage' : 'gold'}>{paymentLabel[r.payment]}</Pill>
        </div>

        {/* price */}
        <div className="mt-3.5 flex items-end justify-between gap-3 rounded-xl bg-sumi-50 px-3.5 py-3">
          <div>
            <p className="eyebrow">Per night</p>
            <p className="tabular mt-0.5 font-display text-[24px] font-medium leading-none text-sumi-800">
              ${Math.round(perNight)}
            </p>
          </div>
          <div className="text-right">
            <p className="eyebrow">Total</p>
            <p className="tabular mt-0.5 font-display text-[24px] font-medium leading-none text-sumi-800">
              ${Math.round(usd)}
            </p>
            <p className="tabular mt-1 text-[11px] text-sumi-400">
              {r.currency} {r.amount.toLocaleString()}
            </p>
          </div>
        </div>

        {/* cancellation */}
        <div
          className={[
            'mt-3 flex items-start gap-2 rounded-xl px-3 py-2.5 text-[12.5px] leading-relaxed',
            stillFree ? 'bg-sage-50 text-sage-600' : 'bg-shu-50 text-shu-600',
          ].join(' ')}
        >
          <CalendarX size={14} className="mt-[2px] shrink-0" />
          <span>
            <span className="font-semibold">
              {r.freeCancelUntil
                ? stillFree
                  ? daysLeft === 0
                    ? 'Free cancellation ends today'
                    : `Free cancellation for ${daysLeft} more ${daysLeft === 1 ? 'day' : 'days'}`
                  : 'Free cancellation has passed'
                : 'Cancellation terms unknown'}
            </span>
            {r.freeCancelUntil && (
              <span className="opacity-80"> · until {shortDateLabel(r.freeCancelUntil)}</span>
            )}
            <span className="mt-1 block opacity-90">{r.cancelNote}</span>
          </span>
        </div>

        {r.conflict && (
          <div className="mt-3 flex items-start gap-2 rounded-xl border border-shu-100 bg-shu-50 px-3 py-2.5 text-[12.5px] leading-relaxed text-shu-600">
            <AlertTriangle size={14} className="mt-[2px] shrink-0" />
            <span>
              <span className="font-semibold">Does not match the itinerary. </span>
              {r.conflict}
            </span>
          </div>
        )}

        {nameMismatch && !r.conflict && (
          <p className="mt-2 text-[12px] text-sumi-400">
            Itinerary calls this {hotel!.name}.
          </p>
        )}

        {/* details */}
        <dl className="mt-3 space-y-1.5 text-[12.5px]">
          {r.roomType && (
            <div className="flex gap-2">
              <dt className="shrink-0 text-sumi-400">Room</dt>
              <dd className="text-sumi-600">{r.roomType}</dd>
            </div>
          )}
          {r.promotion && (
            <div className="flex gap-2">
              <dt className="shrink-0 text-sumi-400">Rate</dt>
              <dd className="flex items-center gap-1 text-sumi-600">
                <Sparkles size={11} className="text-gold-500" />
                {r.promotion}
              </dd>
            </div>
          )}
          {r.addressAsBooked && (
            <div className="flex gap-2">
              <dt className="shrink-0 text-sumi-400">Address</dt>
              <dd className="text-sumi-600">{r.addressAsBooked}</dd>
            </div>
          )}
        </dl>

        {r.extras && r.extras.length > 0 && (
          <div className="mt-3">
            <p className="eyebrow mb-1.5">Charged separately</p>
            <ul className="space-y-1">
              {r.extras.map((x) => (
                <li key={x} className="flex gap-2 text-[12px] leading-relaxed text-sumi-500">
                  <span className="mt-[6px] h-1 w-1 shrink-0 rounded-full bg-sumi-300" aria-hidden />
                  {x}
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>

      {/* actions */}
      <div className="mt-4 space-y-2 border-t border-sumi-100 bg-sumi-50/40 px-4 py-3">
        {stillFree && (
          <LinkButton
            href={hotelPriceSearch(r.propertyAsBooked, r.checkIn, r.checkOut)}
            variant="primary"
            className="w-full"
          >
            <Search size={14} /> Check these nights for a better price
          </LinkButton>
        )}
        <div className="grid grid-cols-2 gap-2">
          {platformUrl[r.platform] && (
            <LinkButton href={platformUrl[r.platform]!}>
              My {platformLabel[r.platform]} bookings
            </LinkButton>
          )}
          {r.addressAsBooked && (
            <LinkButton href={mapsSearch(`${r.propertyAsBooked} ${r.addressAsBooked}`)}>
              <MapPin size={14} /> Maps
            </LinkButton>
          )}
        </div>
        {r.phone && (
          <Ext
            href={`tel:${r.phone.replace(/[^+\d]/g, '')}`}
            className="btn-secondary w-full"
            ariaLabel={`Call ${r.propertyAsBooked}`}
          >
            <Phone size={14} /> {r.phone}
          </Ext>
        )}
        <p className="pt-1 text-[10.5px] leading-relaxed text-sumi-300">
          Dollar figures are approximate, converted at {fx.jpyPerUsd} JPY and{' '}
          {(1 / fx.usdPerCny).toFixed(2)} CNY to the dollar as of {fx.asOf}. Your card will settle at
          the rate on the day.
        </p>
      </div>
    </article>
  )
}
