import { Clock } from 'lucide-react'
import { days } from '../data/days'
import { trip } from '../data/trip'
import type { TripState } from '../hooks/useTripState'
import { DayView } from '../components/DayView'
import { NoticeCard } from '../components/ui'
import { tripNotices } from '../data/trip'
import {
  bigDateLabel, clockInJapan, daysUntilTrip, isDuringTrip, resolveActiveDate, shortDateLabel,
} from '../utils/date'

/**
 * The default screen: the app opens here and answers "where am I, what are we
 * doing, and what is the next train" without a single tap.
 */
export function TodayView({
  state,
  today,
  now,
  nowMinutes,
}: {
  state: TripState
  today: string
  now: Date
  nowMinutes: number
}) {
  const activeDate = resolveActiveDate(today)
  const day = days.find((d) => d.date === activeDate)!
  const live = isDuringTrip(today)
  const countdown = daysUntilTrip(today)

  return (
    <div className="space-y-8">
      {/* ------------------------------------------------------ big header */}
      <header>
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <p className="eyebrow-lg">
              {live ? `${day.dow} · Day ${days.indexOf(day) + 1} of ${days.length}` : 'Up next'}
            </p>
            <h1 className="mt-1.5 font-display text-[29px] font-medium leading-[1] tracking-tight text-sumi-800 xs:text-[32px] sm:text-[34px]">
              {bigDateLabel(day.date)}
            </h1>
            <p className="mt-2 font-display text-[19px] font-medium leading-tight text-shu-500">
              {day.route}
            </p>
          </div>

          <div className="shrink-0 text-right">
            <p className="eyebrow">Japan time</p>
            <p className="tabular mt-1 flex items-center justify-end gap-1 text-[15px] font-semibold text-sumi-700">
              <Clock size={13} className="text-sumi-300" />
              {clockInJapan(now)}
            </p>
          </div>
        </div>

        <p className="mt-3 text-[14px] leading-relaxed text-sumi-500">{day.headline}</p>
      </header>

      {/* --------------------------------------------- pre / post trip note */}
      {!live && (
        <div className="rounded-card border border-sumi-200 bg-surface px-4 py-3.5">
          <p className="text-[13px] font-semibold text-sumi-700">
            {countdown > 0
              ? `${countdown} ${countdown === 1 ? 'day' : 'days'} until you fly`
              : 'The trip is over — this is the last day, kept for reference.'}
          </p>
          <p className="mt-1 text-[12.5px] leading-relaxed text-sumi-500">
            {countdown > 0
              ? `Showing ${shortDateLabel(trip.startDate)}, the first day. On ${shortDateLabel(trip.startDate)} this screen switches to the real day automatically.`
              : 'Everything you marked, noted and saved is still here.'}
          </p>
        </div>
      )}

      {/* ------------------------------------------ trip-wide notices, once */}
      {!live && (
        <div className="space-y-2.5">
          {tripNotices.map((n) => (
            <NoticeCard key={n.id} notice={n} />
          ))}
        </div>
      )}

      <DayView day={day} state={state} isToday={live} nowMinutes={nowMinutes} />
    </div>
  )
}
