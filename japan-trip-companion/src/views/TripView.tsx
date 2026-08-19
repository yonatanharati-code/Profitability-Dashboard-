import { ChevronRight, ArrowLeft } from 'lucide-react'
import { days } from '../data/days'
import { hotelById } from '../data/hotels'
import { cities } from '../data/cities'
import type { TripState } from '../hooks/useTripState'
import { DayView } from '../components/DayView'
import { Pill, SectionHeader } from '../components/ui'
import { WeatherCard } from '../components/WeatherCard'
import { useWeather } from '../hooks/useWeather'
import { dayProgress, orderedActivities } from '../utils/day'
import { bigDateLabel, shortDateLabel, statusForDate } from '../utils/date'

/** The whole trip as day cards, and the detail screen behind each one. */
export function TripView({
  state,
  today,
  nowMinutes,
  openDate,
  onOpenDate,
}: {
  state: TripState
  today: string
  nowMinutes: number
  openDate: string | null
  onOpenDate: (date: string | null) => void
}) {
  const allCities = [...new Set(days.flatMap((d) => d.cities))]
  const weather = useWeather(allCities)

  if (openDate) {
    const day = days.find((d) => d.date === openDate)
    if (day) {
      return (
        <div className="space-y-6">
          <button
            type="button"
            onClick={() => onOpenDate(null)}
            className="inline-flex min-h-[40px] items-center gap-1.5 text-[13px] font-semibold text-sumi-500 hover:text-sumi-800"
          >
            <ArrowLeft size={15} /> All days
          </button>
          <DayView
            day={day}
            state={state}
            isToday={day.date === today}
            nowMinutes={nowMinutes}
            showBigHeader
          />
        </div>
      )
    }
  }

  return (
    <div className="space-y-8">
      <header>
        <p className="eyebrow-lg">9 – 25 September 2026</p>
        <h1 className="mt-1.5 font-display text-[30px] font-medium leading-tight tracking-tight text-sumi-800">
          Seventeen days
        </h1>
        <p className="mt-2 text-[13.5px] leading-relaxed text-sumi-500">
          Tokyo, the Northern Alps, Takayama, Shirakawa-go, Kanazawa, Kyoto and Osaka. Tap any day
          for the full plan.
        </p>
      </header>

      <section>
        <SectionHeader title="Every day" />
        <ol className="space-y-2.5">
          {days.map((day) => {
            const status = statusForDate(day.date, today)
            const hotel = hotelById(day.hotelId)
            const activities = orderedActivities(day, state.order[day.date])
            const progress = dayProgress(activities, state.completed, state.skipped)
            const w = weather.get(day.cities[day.cities.length - 1], day.date)

            return (
              <li key={day.date}>
                <button
                  type="button"
                  onClick={() => onOpenDate(day.date)}
                  className={[
                    'card w-full px-4 py-4 text-left transition active:scale-[0.99]',
                    status === 'today' ? 'ring-1 ring-shu-200 shadow-lift' : '',
                    status === 'past' ? 'opacity-60' : '',
                  ].join(' ')}
                >
                  <div className="flex items-start gap-3.5">
                    {/* date block */}
                    <div className="w-[46px] shrink-0">
                      <p className="eyebrow">{day.dow}</p>
                      <p className="tabular mt-0.5 font-display text-[22px] font-medium leading-none text-sumi-800">
                        {day.date.slice(-2)}
                      </p>
                      <p className="text-[10px] uppercase tracking-wider text-sumi-300">Sep</p>
                    </div>

                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-1.5">
                        {status === 'today' && <Pill tone="shu">Today</Pill>}
                        {status === 'past' && <Pill tone="outline">Past</Pill>}
                        {progress.done > 0 && (
                          <Pill tone="sage">
                            {progress.done}/{progress.total}
                          </Pill>
                        )}
                      </div>

                      <p className="mt-1.5 font-display text-[15.5px] font-medium leading-snug text-sumi-800">
                        {day.route}
                      </p>
                      <p className="mt-0.5 text-[12.5px] leading-snug text-sumi-400">{day.theme}</p>

                      <div className="mt-2.5 flex flex-wrap items-center gap-x-3 gap-y-1 text-[11.5px] text-sumi-400">
                        <WeatherCard
                          weather={w}
                          city={cities[day.cities[day.cities.length - 1]]}
                          compact
                        />
                        {hotel && <span className="truncate">{hotel.name}</span>}
                      </div>
                    </div>

                    <ChevronRight size={17} className="mt-1 shrink-0 text-sumi-300" />
                  </div>
                </button>
              </li>
            )
          })}
        </ol>
      </section>

      <section>
        <SectionHeader title="Stays" hint="All six confirmed in the PDF." />
        <ul className="space-y-2">
          {[...new Set(days.map((d) => d.hotelId).filter(Boolean))].map((id) => {
            const h = hotelById(id as string)!
            return (
              <li key={h.id} className="card flex items-baseline justify-between gap-3 px-4 py-3">
                <span className="min-w-0">
                  <span className="block truncate text-[13.5px] font-semibold text-sumi-800">
                    {h.name}
                  </span>
                  <span className="block text-[12px] text-sumi-400">{h.cityLabel}</span>
                </span>
                <span className="tabular shrink-0 text-[12px] text-sumi-500">
                  {shortDateLabel(h.checkIn)}–{shortDateLabel(h.checkOut)}
                </span>
              </li>
            )
          })}
        </ul>
      </section>

      <p className="text-[11px] leading-relaxed text-sumi-300">
        Trip runs {bigDateLabel(days[0].date)} to {bigDateLabel(days[days.length - 1].date)}, 2026.
      </p>
    </div>
  )
}
