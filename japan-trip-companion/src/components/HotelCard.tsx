import { BedDouble, MapPin, Navigation, Train } from 'lucide-react'
import type { Hotel } from '../data/types'
import { LinkButton, Pill, ReviewFlag } from './ui'
import { mapsDirections, mapsSearch } from '../utils/maps'
import { shortDateLabel } from '../utils/date'

export function HotelCard({ hotel, compact = false }: { hotel: Hotel; compact?: boolean }) {
  if (compact) {
    return (
      <a
        href={mapsSearch(hotel.mapsQuery)}
        target="_blank"
        rel="noopener noreferrer"
        className="card flex items-center gap-3 px-4 py-3.5"
      >
        <span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-ai-50 text-ai-500">
          <BedDouble size={16} />
        </span>
        <span className="min-w-0 flex-1">
          <span className="eyebrow block">Tonight</span>
          <span className="mt-0.5 block truncate text-[14px] font-semibold text-sumi-800">
            {hotel.name}
          </span>
          <span className="mt-0.5 block truncate text-[12px] text-sumi-400">
            {hotel.nearestStation}
          </span>
        </span>
        <MapPin size={15} className="shrink-0 text-sumi-300" />
      </a>
    )
  }

  return (
    <article className="card overflow-hidden">
      <div className="px-4 pt-4">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <p className="eyebrow">{hotel.cityLabel}</p>
            <h3 className="mt-1 text-[16px] font-semibold leading-snug text-sumi-800">
              {hotel.name}
            </h3>
          </div>
          <Pill tone="sage">Confirmed</Pill>
        </div>

        <p className="tabular mt-2.5 text-[13px] font-medium text-sumi-600">
          {shortDateLabel(hotel.checkIn)} → {shortDateLabel(hotel.checkOut)}
          <span className="ml-2 font-normal text-sumi-400">
            {hotel.nights} {hotel.nights === 1 ? 'night' : 'nights'}
          </span>
        </p>

        {hotel.nearestStation && (
          <p className="mt-2 flex items-center gap-1.5 text-[12.5px] text-sumi-500">
            <Train size={13} className="text-sumi-300" />
            {hotel.nearestStation}
          </p>
        )}

        <p className="mt-3 text-[12.5px] leading-relaxed text-sumi-500">{hotel.locationNote}</p>

        <div className="mt-3">
          <p className="eyebrow mb-1.5">Great for</p>
          <div className="flex flex-wrap gap-1.5">
            {hotel.goodFor.map((g) => (
              <Pill key={g} tone="outline">
                {g}
              </Pill>
            ))}
          </div>
        </div>

        {hotel.reviewFlag && (
          <div className="mt-3">
            <ReviewFlag text={hotel.reviewFlag} />
          </div>
        )}
      </div>

      <div className="mt-4 grid grid-cols-2 gap-2 border-t border-sumi-100 bg-sumi-50/40 px-4 py-3">
        <LinkButton href={mapsSearch(hotel.mapsQuery)}>
          <MapPin size={14} /> Maps
        </LinkButton>
        <LinkButton href={mapsDirections(hotel.mapsQuery)} variant="primary">
          <Navigation size={14} /> Navigate
        </LinkButton>
      </div>
    </article>
  )
}
