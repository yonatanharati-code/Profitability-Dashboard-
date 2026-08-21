import { useState } from 'react'
import { BedDouble, Landmark, Utensils, Train, Bus, Navigation, Map as MapIcon } from 'lucide-react'
import { days } from '../data/days'
import { hotels } from '../data/hotels'
import type { MapPin } from '../data/types'
import type { TripState } from '../hooks/useTripState'
import { LinkButton, SectionHeader, Pill } from '../components/ui'
import { orderedActivities, pinsForDay, routeStops } from '../utils/day'
import { mapsDirections, mapsRoute, mapsSearch } from '../utils/maps'
import { bigDateLabel, resolveActiveDate, shortDateLabel } from '../utils/date'
import { Ext } from '../components/ExternalLink'

const KIND: Record<
  MapPin['kind'],
  { icon: typeof Landmark; label: string; cls: string }
> = {
  hotel: { icon: BedDouble, label: 'Hotel', cls: 'bg-ai-50 text-ai-500' },
  sight: { icon: Landmark, label: 'Place', cls: 'bg-shu-50 text-shu-500' },
  food: { icon: Utensils, label: 'Food', cls: 'bg-gold-50 text-gold-600' },
  station: { icon: Train, label: 'Station', cls: 'bg-sumi-100 text-sumi-500' },
  terminal: { icon: Bus, label: 'Terminal', cls: 'bg-sumi-100 text-sumi-500' },
}

/**
 * A real embedded map needs a billed API key, and the brief said not to let
 * that block the app. So this is a pin list that deep-links straight into
 * Google Maps — which on a phone opens the native app, which is what you
 * actually want while walking.
 */
export function MapView({ state, today }: { state: TripState; today: string }) {
  const [selected, setSelected] = useState<string>(() => resolveActiveDate(today))
  const day = days.find((d) => d.date === selected)!
  const activities = orderedActivities(day, state.order[day.date])
  const pins = pinsForDay(day, activities)
  const route = mapsRoute(routeStops(day, activities))

  const grouped = (['hotel', 'station', 'terminal', 'sight', 'food'] as MapPin['kind'][])
    .map((kind) => ({ kind, items: pins.filter((p) => p.kind === kind) }))
    .filter((g) => g.items.length > 0)

  return (
    <div className="space-y-8">
      <header>
        <p className="eyebrow-lg">Map</p>
        <h1 className="mt-1.5 font-display text-[30px] font-medium leading-tight tracking-tight text-sumi-800">
          Where everything is
        </h1>
        <p className="mt-2 text-[13.5px] leading-relaxed text-sumi-500">
          Every place for a chosen day, straight through to Google Maps. Tapping a pin on your
          phone opens the Maps app itself.
        </p>
      </header>

      {/* day picker */}
      <section>
        <SectionHeader title="Pick a day" />
        <div className="no-scrollbar edge-bleed flex gap-2 overflow-x-auto pb-1">
          {days.map((d) => {
            const on = d.date === selected
            return (
              <button
                key={d.date}
                type="button"
                onClick={() => setSelected(d.date)}
                className={[
                  'shrink-0 rounded-xl border px-3 py-2 text-left transition',
                  on
                    ? 'border-sumi-700 bg-sumi-700 text-paper'
                    : 'border-sumi-200 bg-surface text-sumi-500',
                ].join(' ')}
              >
                <span className="block text-[10px] uppercase tracking-wider opacity-70">
                  {d.dow}
                </span>
                <span className="tabular block text-[15px] font-semibold leading-tight">
                  {shortDateLabel(d.date)}
                </span>
              </button>
            )
          })}
        </div>
      </section>

      {/* the selected day */}
      <section>
        <div className="card px-4 py-4">
          <p className="eyebrow">{bigDateLabel(day.date)}</p>
          <p className="mt-1 font-display text-[17px] font-medium text-sumi-800">{day.route}</p>
          <p className="mt-1 text-[12.5px] text-sumi-400">
            {pins.length} places · {day.theme}
          </p>

          {route.used > 1 && (
            <div className="mt-3.5 space-y-2">
              <LinkButton href={route.url} variant="primary" className="w-full">
                <MapIcon size={15} /> View this day's route in Google Maps
              </LinkButton>
              {route.dropped > 0 && (
                <p className="text-[11px] leading-relaxed text-sumi-400">
                  Google Maps accepts ten stops in one link, so {route.dropped} of today's places
                  are left out of the route. They are all listed below.
                </p>
              )}
            </div>
          )}
        </div>
      </section>

      {/* pins */}
      {grouped.map(({ kind, items }) => {
        const meta = KIND[kind]
        return (
          <section key={kind}>
            <SectionHeader title={`${meta.label}${items.length > 1 ? 's' : ''}`} />
            <ul className="space-y-2">
              {items.map((pin) => (
                <li key={`${kind}-${pin.query}`} className="card flex items-center gap-3 px-3.5 py-3">
                  <span
                    className={`flex h-9 w-9 shrink-0 items-center justify-center rounded-xl ${meta.cls}`}
                  >
                    <meta.icon size={16} />
                  </span>
                  <span className="min-w-0 flex-1">
                    <span className="block truncate text-[13.5px] font-semibold text-sumi-800">
                      {pin.label}
                    </span>
                  </span>
                  <Ext
                    href={mapsSearch(pin.query)}
                    className="icon-btn shrink-0"
                    ariaLabel={`Open ${pin.label} in Google Maps`}
                  >
                    <MapIcon size={16} />
                  </Ext>
                  <Ext
                    href={mapsDirections(pin.query)}
                    className="icon-btn shrink-0"
                    ariaLabel={`Navigate to ${pin.label}`}
                  >
                    <Navigation size={16} />
                  </Ext>
                </li>
              ))}
            </ul>
          </section>
        )
      })}

      {/* all hotels, always reachable */}
      <section>
        <SectionHeader title="All six hotels" hint="Whatever day you are looking at." />
        <ul className="space-y-2">
          {hotels.map((h) => (
            <li key={h.id} className="card flex items-center gap-3 px-3.5 py-3">
              <span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-ai-50 text-ai-500">
                <BedDouble size={16} />
              </span>
              <span className="min-w-0 flex-1">
                <span className="block truncate text-[13.5px] font-semibold text-sumi-800">
                  {h.name}
                </span>
                <span className="block text-[11.5px] text-sumi-400">
                  {h.cityLabel} · {shortDateLabel(h.checkIn)}–{shortDateLabel(h.checkOut)}
                </span>
              </span>
              <Ext
                href={mapsDirections(h.mapsQuery)}
                className="icon-btn shrink-0"
                ariaLabel={`Navigate to ${h.name}`}
              >
                <Navigation size={16} />
              </Ext>
            </li>
          ))}
        </ul>
      </section>

      <p className="flex flex-wrap items-center gap-2 text-[11px] text-sumi-300">
        <Pill tone="outline">No API key needed</Pill>
        Deep links work offline-installed and open the native Maps app.
      </p>
    </div>
  )
}
