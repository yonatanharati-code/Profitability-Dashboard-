import { Heart, MapPin } from 'lucide-react'
import type { Restaurant } from '../data/types'
import { DietPill, Pill } from './ui'
import { mapsSearch } from '../utils/maps'
import type { TripState } from '../hooks/useTripState'

const SLOT_LABEL: Record<Restaurant['slot'], string> = {
  quick: 'Quick',
  local: 'Local',
  dinner: 'Dinner',
  special: 'Special',
  cafe: 'Cafe',
}

const RES_LABEL: Record<Restaurant['reservation'], string> = {
  recommended: 'Reservation recommended',
  'walk-in': 'Walk in',
  required: 'Reservation required',
}

export function RestaurantCard({
  restaurant: r,
  state,
}: {
  restaurant: Restaurant
  state: TripState
}) {
  const fav = !!state.favourites[r.id]

  return (
    <article className="card px-4 py-3.5">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-1.5">
            <Pill tone="dark">{SLOT_LABEL[r.slot]}</Pill>
            <DietPill verdict={r.dietVerdict} />
          </div>
          <h3 className="mt-2 text-[14.5px] font-semibold leading-snug text-sumi-800">{r.name}</h3>
          <p className="mt-0.5 text-[12px] text-sumi-400">
            {r.cuisine} · {r.price} · {r.neighbourhood}
          </p>
        </div>

        <button
          type="button"
          onClick={() => state.toggleFavourite(r.id)}
          aria-label={fav ? `Remove ${r.name} from favourites` : `Save ${r.name} to favourites`}
          aria-pressed={fav}
          className="icon-btn -mr-2 -mt-2 shrink-0"
        >
          <Heart size={17} className={fav ? 'fill-shu-500 text-shu-500' : ''} />
        </button>
      </div>

      <p className="mt-2.5 text-[12.5px] leading-relaxed text-sumi-600">{r.why}</p>

      {/* A filled box is for a note you must act on. A cafe that says "coffee"
          does not need one — that would be alarm fatigue by design. */}
      {r.dietVerdict === 'good' && r.dietNote.length < 45 ? (
        <p className="mt-2 text-[12px] leading-relaxed text-sumi-400">{r.dietNote}</p>
      ) : (
        <p
          className={[
            'mt-2.5 rounded-lg px-2.5 py-2 text-[12px] leading-relaxed',
            r.dietVerdict === 'good'
              ? 'bg-sage-50 text-sage-600'
              : r.dietVerdict === 'ask'
                ? 'bg-gold-50 text-gold-600'
                : 'bg-shu-50 text-shu-600',
          ].join(' ')}
        >
          {r.dietNote}
        </p>
      )}

      <div className="mt-3 flex items-center justify-between gap-3 border-t border-sumi-100 pt-3">
        <span className="text-[11.5px] font-medium text-sumi-400">{RES_LABEL[r.reservation]}</span>
        <a
          href={mapsSearch(r.mapsQuery)}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex min-h-[36px] items-center gap-1.5 text-[12.5px] font-semibold text-sumi-600 hover:text-shu-500"
        >
          <MapPin size={13} /> Maps
        </a>
      </div>
    </article>
  )
}
