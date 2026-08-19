import { MapPin, ShoppingBag } from 'lucide-react'
import type { ShoppingSpot } from '../data/types'
import { mapsSearch } from '../utils/maps'

export function ShoppingCard({ spot }: { spot: ShoppingSpot }) {
  return (
    <article className="card px-4 py-3.5">
      <div className="flex items-start gap-3">
        <span className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-gold-50 text-gold-600">
          <ShoppingBag size={15} />
        </span>
        <div className="min-w-0 flex-1">
          <p className="eyebrow">{spot.area}</p>
          <h3 className="mt-0.5 text-[14px] font-semibold leading-snug text-sumi-800">
            {spot.name}
          </h3>
          <p className="mt-1.5 text-[12.5px] leading-relaxed text-sumi-600">{spot.what}</p>
          {spot.note && (
            <p className="mt-1.5 text-[12px] leading-relaxed text-sumi-400">{spot.note}</p>
          )}
          <a
            href={mapsSearch(spot.mapsQuery)}
            target="_blank"
            rel="noopener noreferrer"
            className="mt-2.5 inline-flex min-h-[36px] items-center gap-1.5 text-[12.5px] font-semibold text-sumi-600 hover:text-shu-500"
          >
            <MapPin size={13} /> Open in Maps
          </a>
        </div>
      </div>
    </article>
  )
}
