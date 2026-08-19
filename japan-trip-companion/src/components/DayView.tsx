import { useMemo } from 'react'
import { ArrowRight, RotateCcw, Utensils, ShoppingBag, Map as MapIcon } from 'lucide-react'
import type { Day } from '../data/types'
import { hotelById } from '../data/hotels'
import { transportById } from '../data/transport'
import { restaurantById } from '../data/restaurants'
import { shoppingById } from '../data/shopping'
import { cities } from '../data/cities'
import type { TripState } from '../hooks/useTripState'
import { useWeather } from '../hooks/useWeather'
import { ActivityCard } from './ActivityCard'
import { TransportCard } from './TransportCard'
import { HotelCard } from './HotelCard'
import { RestaurantCard } from './RestaurantCard'
import { ShoppingCard } from './ShoppingCard'
import { WeatherCard } from './WeatherCard'
import { NoticeCard, SectionHeader, EmptyNote, LinkButton } from './ui'
import { dayProgress, nextUp, orderedActivities, pinsForDay, routeStops } from '../utils/day'
import { mapsRoute } from '../utils/maps'
import { bigDateLabel, minutesFromHHMM, shortDateLabel } from '../utils/date'

/**
 * The full body of a single day. Today's screen and the Trip screen's day
 * detail render exactly the same thing — the only difference is whether the
 * live clock is in play.
 */
export function DayView({
  day,
  state,
  isToday,
  nowMinutes,
  showBigHeader = false,
}: {
  day: Day
  state: TripState
  isToday: boolean
  nowMinutes: number
  showBigHeader?: boolean
}) {
  const activities = useMemo(
    () => orderedActivities(day, state.order[day.date]),
    [day, state.order],
  )
  const progress = dayProgress(activities, state.completed, state.skipped)
  const next = nextUp(activities, state.completed, state.skipped, isToday ? nowMinutes : null)

  const hotel = hotelById(day.hotelId)
  const legs = day.transportIds.map(transportById).filter(Boolean) as NonNullable<
    ReturnType<typeof transportById>
  >[]

  const weather = useWeather(day.cities)
  const otherCities = day.cities.filter((c) => c !== day.weatherCity)

  const meals = day.restaurantIds.map(restaurantById).filter(Boolean) as NonNullable<
    ReturnType<typeof restaurantById>
  >[]
  const shops = day.shoppingIds.map(shoppingById).filter(Boolean) as NonNullable<
    ReturnType<typeof shoppingById>
  >[]

  const route = mapsRoute(routeStops(day, activities))
  const pinCount = pinsForDay(day, activities).length
  const hasCustomOrder = !!state.order[day.date]?.length

  return (
    <div className="space-y-8">
      {showBigHeader && (
        <header>
          <p className="eyebrow-lg">
            {day.dow} · {day.theme}
          </p>
          <h1 className="mt-1.5 font-display text-[30px] font-medium leading-none tracking-tight text-sumi-800">
            {bigDateLabel(day.date)}
          </h1>
          <p className="mt-2 flex items-center gap-1.5 font-display text-[17px] font-medium text-shu-500">
            {day.route}
          </p>
          <p className="mt-2.5 text-[13.5px] leading-relaxed text-sumi-500">{day.headline}</p>
        </header>
      )}

      {/* progress */}
      <section>
        <div className="flex items-center justify-between text-[11.5px] font-semibold text-sumi-400">
          <span>
            {progress.done} of {progress.total} done
          </span>
          <span className="tabular">{progress.percent}%</span>
        </div>
        <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-sumi-100">
          <div
            className="h-full rounded-full bg-sage-400 transition-all duration-500"
            style={{ width: `${progress.percent}%` }}
          />
        </div>
      </section>

      {/* weather + hotel */}
      <section className="space-y-3">
        <WeatherCard
          weather={weather.get(day.weatherCity, day.date)}
          city={cities[day.weatherCity]}
        />
        {otherCities.length > 0 && (
          <div className="card flex flex-wrap items-center gap-x-5 gap-y-2 px-4 py-3">
            <span className="eyebrow">Also today</span>
            {otherCities.map((c) => (
              <span key={c} className="flex items-center gap-2">
                <span className="text-[12px] font-medium text-sumi-500">{cities[c].name}</span>
                <WeatherCard weather={weather.get(c, day.date)} city={cities[c]} compact />
              </span>
            ))}
          </div>
        )}
        {hotel ? (
          <HotelCard hotel={hotel} compact />
        ) : (
          <EmptyNote>No hotel tonight — this is the day you fly home.</EmptyNote>
        )}
      </section>

      {/* what's next */}
      {isToday && next && (
        <section>
          <SectionHeader title="What's next" />
          <div className="card border-l-[3px] border-l-shu-400 px-4 py-3.5">
            <div className="flex items-baseline gap-2.5">
              {next.time && (
                <span className="tabular text-[15px] font-semibold text-sumi-700">
                  {next.timeIsSuggested && <span className="text-sumi-300">~</span>}
                  {next.time}
                </span>
              )}
              <span className="min-w-0 flex-1 text-[14px] font-semibold leading-snug text-sumi-800">
                {next.title}
              </span>
            </div>
            {next.subtitle && (
              <p className="mt-1 text-[12.5px] text-sumi-400">{next.subtitle}</p>
            )}
            {next.time && !next.timeIsSuggested && (
              <p className="mt-2 text-[12px] font-medium text-shu-500">
                {leaveByLine(next.time, nowMinutes)}
              </p>
            )}
          </div>
        </section>
      )}

      {/* notices */}
      {day.notices.length > 0 && (
        <section className="space-y-2.5">
          {day.notices.map((n) => (
            <NoticeCard key={n.id} notice={n} />
          ))}
        </section>
      )}

      {/* transport */}
      {legs.length > 0 && (
        <section>
          <SectionHeader
            title="Transport"
            hint="Booked legs are fixed. Everything else in the day can move around them."
          />
          <div className="space-y-3">
            {legs.map((leg) => (
              <TransportCard
                key={leg.id}
                leg={leg}
                nowMinutes={nowMinutes}
                isToday={isToday}
                emphasise={
                  isToday && !!leg.depart && minutesFromHHMM(leg.depart) - nowMinutes < 180 &&
                  minutesFromHHMM(leg.depart) - nowMinutes > -30
                }
              />
            ))}
          </div>
        </section>
      )}

      {/* timeline */}
      <section>
        <SectionHeader
          title="The day"
          hint="Tap to expand. Reorder or skip anything — the transport stays put."
          action={
            hasCustomOrder ? (
              <button
                type="button"
                onClick={() => state.resetOrder(day.date)}
                className="inline-flex min-h-[36px] items-center gap-1.5 text-[11.5px] font-semibold text-sumi-400 hover:text-sumi-700"
              >
                <RotateCcw size={12} /> Reset order
              </button>
            ) : undefined
          }
        />
        {/* The vertical rail: one continuous line, a dot per stop. */}
        <div className="relative space-y-2.5 pl-[24px]">
          <span
            className="absolute left-[5px] top-3 bottom-3 w-px bg-sumi-200"
            aria-hidden
          />
          {activities.map((a, i) => (
            <ActivityCard
              key={a.id}
              activity={a}
              dayDate={day.date}
              state={state}
              isToday={isToday}
              nowMinutes={nowMinutes}
              leg={a.transportId ? transportById(a.transportId) : undefined}
              canMoveUp={i > 0}
              canMoveDown={i < activities.length - 1}
              onMove={(dir) =>
                state.moveActivity(day.date, activities.map((x) => x.id), a.id, dir)
              }
            />
          ))}
        </div>
      </section>

      {/* map shortcut */}
      {route.used > 1 && (
        <section>
          <SectionHeader title="Map" hint={`${pinCount} places pinned for today.`} />
          <LinkButton href={route.url} variant="primary" className="w-full">
            <MapIcon size={15} /> View today's route in Google Maps
          </LinkButton>
          {route.dropped > 0 && (
            <p className="mt-2 text-[11px] text-sumi-400">
              Google Maps takes ten stops at a time, so the last {route.dropped} are not in this
              link. All of them are pinned on the Map tab.
            </p>
          )}
        </section>
      )}

      {/* eat nearby */}
      {meals.length > 0 && (
        <section>
          <SectionHeader
            title="Eat nearby"
            hint="No pork, no seafood — every card says how to order."
          />
          <div className="space-y-2.5">
            {meals.map((r) => (
              <RestaurantCard key={r.id} restaurant={r} state={state} />
            ))}
          </div>
        </section>
      )}

      {/* shopping */}
      {shops.length > 0 && (
        <section>
          <SectionHeader title="Shopping" />
          <div className="space-y-2.5">
            {shops.map((s) => (
              <ShoppingCard key={s.id} spot={s} />
            ))}
          </div>
        </section>
      )}

      {meals.length === 0 && shops.length === 0 && (
        <section className="flex items-center gap-3 rounded-card border border-dashed border-sumi-200 px-4 py-5 text-[12.5px] text-sumi-400">
          <Utensils size={15} className="shrink-0" />
          <ShoppingBag size={15} className="shrink-0" />
          <span>Nothing curated for today — food and shopping are wherever the day takes you.</span>
        </section>
      )}
    </div>
  )
}

function leaveByLine(time: string, nowMinutes: number): string {
  const diff = minutesFromHHMM(time) - nowMinutes
  if (diff < 0) return 'Should be underway'
  if (diff < 60) return `Leave by ${time} — ${diff} min from now`
  const h = Math.floor(diff / 60)
  const m = diff % 60
  return `Leave by ${time} — ${h} h ${m} min from now`
}

/** Small header strip for the sticky bar. */
export function DayHeadline({ day }: { day: Day }) {
  return (
    <span className="flex min-w-0 items-center gap-2">
      <span className="tabular shrink-0 whitespace-nowrap text-[13px] font-semibold uppercase tracking-wide text-sumi-700">
        {shortDateLabel(day.date)}
      </span>
      <ArrowRight size={12} className="shrink-0 text-sumi-300" />
      <span className="truncate text-[13px] text-sumi-500">{day.route}</span>
    </span>
  )
}
