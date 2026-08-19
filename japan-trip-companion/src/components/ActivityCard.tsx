import { useState } from 'react'
import {
  Check, ChevronDown, Clock, MapPin, Navigation, StickyNote, Ticket,
  Train, SkipForward, ArrowUp, ArrowDown, Lightbulb, Route, Banknote, CalendarClock,
} from 'lucide-react'
import type { Activity, TransportLeg } from '../data/types'
import { LinkButton, Pill, PriorityPill, ReviewFlag } from './ui'
import { mapsDirections, mapsSearch } from '../utils/maps'
import type { TripState } from '../hooks/useTripState'
import { untilLabel } from '../utils/date'

const KIND_ACCENT: Record<Activity['kind'], string> = {
  sight: 'bg-shu-400',
  walk: 'bg-sage-400',
  shopping: 'bg-gold-500',
  food: 'bg-shu-400',
  transport: 'bg-sumi-700',
  hotel: 'bg-ai-400',
  free: 'bg-sumi-300',
  logistics: 'bg-ai-400',
}

/**
 * One item on the day's timeline. Collapsed it is a time, a title and a
 * subtitle; expanded it becomes the full brief. Tap anywhere on the header
 * to open it — the whole row is the target, which matters on a phone.
 */
export function ActivityCard({
  activity,
  dayDate,
  state,
  isToday,
  nowMinutes,
  leg,
  canMoveUp,
  canMoveDown,
  onMove,
  defaultOpen = false,
}: {
  activity: Activity
  dayDate: string
  state: TripState
  isToday: boolean
  nowMinutes: number
  leg?: TransportLeg
  canMoveUp: boolean
  canMoveDown: boolean
  onMove: (direction: -1 | 1) => void
  defaultOpen?: boolean
}) {
  const [open, setOpen] = useState(defaultOpen)
  const [noteOpen, setNoteOpen] = useState(false)

  const done = !!state.completed[activity.id]
  const skipped = !!state.skipped[activity.id]
  const note = state.notes[activity.id] ?? ''

  const countdown =
    isToday && activity.time && !activity.timeIsSuggested && !done
      ? untilLabel(activity.time, nowMinutes)
      : null

  const isNow =
    isToday && activity.time && !done && !skipped
      ? Math.abs(nowMinutes - toMinutes(activity.time)) <= 45
      : false

  return (
    <article
      className={[
        'card relative overflow-visible transition',
        skipped ? 'opacity-45' : '',
        isNow && !skipped ? 'ring-1 ring-shu-100 shadow-lift' : '',
      ].join(' ')}
    >
      {/* The dot on the day's timeline rail. Colour carries the item's kind;
          a completed item goes green so the rail reads as progress. */}
      <span
        className={[
          'absolute -left-[24px] top-[19px] h-[11px] w-[11px] rounded-full ring-[3px] ring-paper',
          done ? 'bg-sage-400' : KIND_ACCENT[activity.kind],
        ].join(' ')}
        aria-hidden
      />

      <div className="flex items-stretch overflow-hidden rounded-card">
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          aria-expanded={open}
          className="flex min-w-0 flex-1 items-start gap-3 px-3.5 py-3.5 text-left"
        >
          {/* time column */}
          <div className="w-[58px] shrink-0 pt-[2px]">
            {activity.time ? (
              <p
                className={[
                  'tabular text-[15px] font-semibold leading-none',
                  done ? 'text-sumi-300' : 'text-sumi-700',
                ].join(' ')}
              >
                {activity.timeIsSuggested && (
                  <span className="text-sumi-300" title="Suggested time, not fixed in the PDF">
                    ~
                  </span>
                )}
                {activity.time}
              </p>
            ) : (
              <p className="text-[11px] font-medium uppercase tracking-wider text-sumi-300">
                Flexible
              </p>
            )}
            {activity.duration && (
              <p className="mt-1 text-[10px] leading-[1.35] text-sumi-400">{activity.duration}</p>
            )}
          </div>

          {/* title block */}
          <div className="min-w-0 flex-1">
            <h3
              className={[
                'text-[14.5px] font-semibold leading-snug',
                done ? 'text-sumi-400 line-through decoration-sumi-200' : 'text-sumi-800',
              ].join(' ')}
            >
              {activity.title}
            </h3>
            {activity.subtitle && (
              <p className="mt-0.5 text-[12.5px] leading-snug text-sumi-400">{activity.subtitle}</p>
            )}
            <div className="mt-2 flex flex-wrap items-center gap-1.5">
              {activity.priority !== 'core' && <PriorityPill priority={activity.priority} />}
              {countdown && <Pill tone={countdown === 'now' ? 'shu' : 'dark'}>{countdown}</Pill>}
              {activity.bookingRecommended && <Pill tone="gold">Booking needed</Pill>}
              {skipped && <Pill tone="outline">Skipped</Pill>}
              {note && (
                <Pill tone="ai">
                  <StickyNote size={9} /> Note
                </Pill>
              )}
            </div>
          </div>

          <ChevronDown
            size={17}
            className={`mt-1 shrink-0 text-sumi-300 transition-transform ${open ? 'rotate-180' : ''}`}
          />
        </button>

        {/* completion toggle, deliberately outside the expand target */}
        <button
          type="button"
          onClick={() => state.toggleCompleted(activity.id)}
          aria-label={done ? `Mark ${activity.title} as not done` : `Mark ${activity.title} as done`}
          aria-pressed={done}
          className="flex w-11 shrink-0 items-center justify-center border-l border-sumi-100"
        >
          <span
            className={[
              'flex h-[26px] w-[26px] items-center justify-center rounded-full border transition',
              done
                ? 'border-sage-400 bg-sage-400 text-white'
                : 'border-sumi-200 text-transparent hover:border-sage-400',
            ].join(' ')}
          >
            <Check size={15} strokeWidth={3} />
          </span>
        </button>
      </div>

      {/* -------------------------------------------------------- expanded */}
      {open && (
        <div className="animate-fade-up space-y-3.5 rounded-b-card border-t border-sumi-100 bg-sumi-50/40 px-4 py-4">
          {activity.description && (
            <p className="text-[13px] leading-relaxed text-sumi-600">{activity.description}</p>
          )}

          {/* facts grid — only rows we actually have */}
          <dl className="space-y-2">
            {activity.hours && <Fact icon={<Clock size={13} />} label="Hours" value={activity.hours} />}
            {activity.price && (
              <Fact icon={<Banknote size={13} />} label="Price" value={activity.price} />
            )}
            {activity.nearestStation && (
              <Fact icon={<Train size={13} />} label="Nearest station" value={activity.nearestStation} />
            )}
            {activity.gettingThere && (
              <Fact icon={<Route size={13} />} label="Getting there" value={activity.gettingThere} />
            )}
            {activity.bookingNote && (
              <Fact
                icon={<CalendarClock size={13} />}
                label="Booking"
                value={activity.bookingNote}
              />
            )}
          </dl>

          {activity.tip && (
            <div className="flex gap-2 rounded-xl bg-sage-50 px-3 py-2.5 text-[12.5px] leading-relaxed text-sage-600">
              <Lightbulb size={14} className="mt-[2px] shrink-0" />
              <span>{activity.tip}</span>
            </div>
          )}

          {activity.reviewFlag && <ReviewFlag text={activity.reviewFlag} />}

          {/* the linked transport leg, rendered in full */}
          {leg && (
            <div className="rounded-xl border border-sumi-200 bg-surface px-3 py-3">
              <p className="eyebrow mb-2">This leg</p>
              <p className="text-[13px] font-semibold text-sumi-700">{leg.service}</p>
              <p className="mt-0.5 text-[12.5px] text-sumi-500">
                {leg.from} → {leg.to}
                {leg.depart && leg.arrive ? ` · ${leg.depart}–${leg.arrive}` : ''}
              </p>
            </div>
          )}

          {/* actions */}
          <div className="grid grid-cols-2 gap-2">
            {activity.mapsQuery && (
              <LinkButton href={mapsSearch(activity.mapsQuery)}>
                <MapPin size={14} /> Open in Maps
              </LinkButton>
            )}
            {activity.mapsQuery && (
              <LinkButton href={mapsDirections(activity.mapsQuery)}>
                <Navigation size={14} /> Navigate
              </LinkButton>
            )}
            {activity.website && <LinkButton href={activity.website}>Official website</LinkButton>}
            {activity.ticketsUrl && (
              <LinkButton href={activity.ticketsUrl} variant="accent">
                <Ticket size={14} /> Book tickets
              </LinkButton>
            )}
          </div>

          {/* note */}
          {noteOpen || note ? (
            <div>
              <label className="eyebrow mb-1.5 block" htmlFor={`note-${activity.id}`}>
                Your note
              </label>
              <textarea
                id={`note-${activity.id}`}
                value={note}
                onChange={(e) => state.setNote(activity.id, e.target.value)}
                rows={3}
                placeholder="Anything you want to remember here…"
                className="w-full resize-y rounded-xl border border-sumi-200 bg-surface px-3 py-2.5 text-[13px] leading-relaxed text-sumi-700 outline-none placeholder:text-sumi-300 focus:border-sumi-400"
              />
            </div>
          ) : (
            <button type="button" onClick={() => setNoteOpen(true)} className="btn-secondary w-full">
              <StickyNote size={14} /> Add note
            </button>
          )}

          {/* per-item controls */}
          <div className="flex items-center justify-between border-t border-sumi-100 pt-3">
            <button
              type="button"
              onClick={() => state.toggleSkipped(activity.id)}
              className="inline-flex min-h-[40px] items-center gap-1.5 rounded-lg px-2 text-[12px] font-semibold text-sumi-400 hover:text-shu-500"
            >
              <SkipForward size={13} />
              {skipped ? 'Un-skip' : 'Skip this'}
            </button>

            <div className="flex items-center gap-1">
              <span className="mr-1 text-[11px] text-sumi-300">Reorder</span>
              <button
                type="button"
                onClick={() => onMove(-1)}
                disabled={!canMoveUp}
                aria-label="Move earlier"
                className="icon-btn h-10 w-10 disabled:opacity-25"
              >
                <ArrowUp size={15} />
              </button>
              <button
                type="button"
                onClick={() => onMove(1)}
                disabled={!canMoveDown}
                aria-label="Move later"
                className="icon-btn h-10 w-10 disabled:opacity-25"
              >
                <ArrowDown size={15} />
              </button>
            </div>
          </div>

          <p className="text-[10.5px] text-sumi-300">
            {activity.source === 'pdf'
              ? 'From the itinerary PDF'
              : activity.source === 'research'
                ? 'Checked against official sources, Aug 2026'
                : 'Our suggestion — not from the PDF'}
            {dayDate ? '' : ''}
          </p>
        </div>
      )}
    </article>
  )
}

function Fact({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) {
  return (
    <div className="flex gap-2.5">
      <span className="mt-[3px] shrink-0 text-sumi-300">{icon}</span>
      <div className="min-w-0">
        <dt className="text-[10px] font-semibold uppercase tracking-[0.12em] text-sumi-400">
          {label}
        </dt>
        <dd className="text-[12.5px] leading-relaxed text-sumi-600">{value}</dd>
      </div>
    </div>
  )
}

function toMinutes(hhmm: string): number {
  const [h, m] = hhmm.split(':').map(Number)
  return h * 60 + m
}
