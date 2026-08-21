import { Check, CircleSlash, ExternalLink } from 'lucide-react'
import { bookings, bucketLabels, bucketOrder } from '../data/bookings'
import { transport } from '../data/transport'
import type { BookingItem } from '../data/types'
import type { TripState } from '../hooks/useTripState'
import { Pill, SectionHeader, type PillTone } from '../components/ui'
import { TransportCard } from '../components/TransportCard'
import { shortDateLabel } from '../utils/date'
import { Ext } from '../components/ExternalLink'

const BUCKET_TONE: Record<BookingItem['bucket'], PillTone> = {
  confirmed: 'sage',
  'should-book': 'shu',
  optional: 'gold',
  none: 'neutral',
}

const BUCKET_BLURB: Record<BookingItem['bucket'], string> = {
  confirmed: 'Already locked in. Nothing to do but keep the confirmations offline.',
  'should-book':
    'These genuinely need booking — verified timed entry, all-reserved seating, or a small room that fills.',
  optional: 'Worth booking if you care about it, fine to skip.',
  none: 'Listed so you know not to worry about them. Pay at the gate, or just turn up.',
}

export function BookingsView({ state }: { state: TripState }) {
  const bookedLegs = transport.filter((t) => t.status === 'booked')
  const shouldBookCount = bookings.filter(
    (b) => b.bucket === 'should-book' && !state.bookingStatus[b.id],
  ).length

  return (
    <div className="space-y-8">
      <header>
        <p className="eyebrow-lg">Bookings</p>
        <h1 className="mt-1.5 font-display text-[30px] font-medium leading-tight tracking-tight text-sumi-800">
          What needs booking
        </h1>
        <p className="mt-2 text-[13.5px] leading-relaxed text-sumi-500">
          Deliberately conservative. Nothing sits under <em>Should book</em> unless a real
          timed-entry or reserved-seat rule was checked — no invented urgency.
        </p>
        {shouldBookCount > 0 && (
          <p className="mt-3 inline-flex items-center gap-2 rounded-pill bg-shu-50 px-3 py-1.5 text-[12px] font-semibold text-shu-600">
            {shouldBookCount} still outstanding
          </p>
        )}
      </header>

      {bucketOrder.map((bucket) => {
        const items = bookings.filter((b) => b.bucket === bucket)
        if (items.length === 0) return null
        return (
          <section key={bucket}>
            <SectionHeader title={bucketLabels[bucket]} hint={BUCKET_BLURB[bucket]} />
            <ul className="space-y-2.5">
              {items.map((b) => {
                const status = state.bookingStatus[b.id]
                const done = status === 'done'
                const dismissed = status === 'skipped'
                const actionable = bucket === 'should-book' || bucket === 'optional'

                return (
                  <li
                    key={b.id}
                    className={[
                      'card px-4 py-3.5 transition',
                      done ? 'opacity-70' : '',
                      dismissed ? 'opacity-45' : '',
                    ].join(' ')}
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-1.5">
                          <Pill tone={BUCKET_TONE[bucket]}>{bucketLabels[bucket]}</Pill>
                          {b.date && <Pill tone="outline">{shortDateLabel(b.date)}</Pill>}
                          {done && <Pill tone="sage">Done</Pill>}
                          {dismissed && <Pill tone="outline">Skipped</Pill>}
                        </div>
                        <h3
                          className={[
                            'mt-2 text-[14.5px] font-semibold leading-snug',
                            done ? 'text-sumi-400 line-through decoration-sumi-200' : 'text-sumi-800',
                          ].join(' ')}
                        >
                          {b.title}
                        </h3>
                        <p className="mt-1 text-[12.5px] leading-relaxed text-sumi-500">
                          {b.detail}
                        </p>
                      </div>
                    </div>

                    <p className="mt-2.5 rounded-lg bg-sumi-50 px-3 py-2 text-[12px] leading-relaxed text-sumi-500">
                      {b.why}
                    </p>

                    <div className="mt-3 flex flex-wrap items-center gap-2">
                      {b.officialUrl && (
                        <Ext href={b.officialUrl} className="btn-secondary flex-1">
                          {b.urlLabel ?? 'Official page'}
                          <ExternalLink size={13} className="opacity-50" />
                        </Ext>
                      )}
                      {actionable && (
                        <>
                          <button
                            type="button"
                            onClick={() => state.setBooking(b.id, done ? null : 'done')}
                            aria-pressed={done}
                            className={done ? 'btn-primary flex-1' : 'btn-secondary flex-1'}
                          >
                            <Check size={14} /> {done ? 'Booked' : 'Mark booked'}
                          </button>
                          <button
                            type="button"
                            onClick={() => state.setBooking(b.id, dismissed ? null : 'skipped')}
                            aria-label={dismissed ? 'Un-skip this booking' : 'Skip this booking'}
                            className="icon-btn shrink-0"
                          >
                            <CircleSlash size={16} />
                          </button>
                        </>
                      )}
                    </div>
                  </li>
                )
              })}
            </ul>
          </section>
        )
      })}

      <section>
        <SectionHeader
          title="Your confirmed transport"
          hint="Exact times from the PDF. Keep these offline — the PDF checklist asks for it."
        />
        <div className="space-y-3">
          {bookedLegs.map((leg) => (
            <TransportCard key={leg.id} leg={leg} />
          ))}
        </div>
      </section>
    </div>
  )
}
