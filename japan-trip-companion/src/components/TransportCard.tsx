import { Bus, Train, Footprints, ArrowRight, Clock, MapPin } from 'lucide-react'
import type { TransportLeg } from '../data/types'
import { LinkButton, Pill, ReservationPill, ReviewFlag } from './ui'
import { hhmmFromMinutes, minutesFromHHMM, untilLabel } from '../utils/date'

function ModeIcon({ mode, size = 15 }: { mode: TransportLeg['mode']; size?: number }) {
  if (mode === 'bus' || mode === 'shuttle') return <Bus size={size} />
  if (mode === 'walk') return <Footprints size={size} />
  return <Train size={size} />
}

/**
 * Transport is the part of this trip that cannot flex, so it gets the loudest
 * card in the app: big times, the reservation status, and the transfer drawn
 * as an actual vertical diagram rather than described in a sentence.
 */
export function TransportCard({
  leg,
  nowMinutes,
  isToday,
  emphasise = false,
}: {
  leg: TransportLeg
  nowMinutes?: number
  isToday?: boolean
  emphasise?: boolean
}) {
  const countdown =
    isToday && leg.depart && nowMinutes != null ? untilLabel(leg.depart, nowMinutes) : null

  const leaveBy =
    leg.depart && leg.arriveAtStationMinutes
      ? hhmmFromMinutes(minutesFromHHMM(leg.depart) - leg.arriveAtStationMinutes)
      : null

  return (
    <article
      className={[
        'card overflow-hidden',
        emphasise ? 'ring-1 ring-shu-100 shadow-lift' : '',
      ].join(' ')}
    >
      {/* header */}
      <div className="flex items-start justify-between gap-3 px-4 pt-4">
        <div className="min-w-0">
          <div className="flex items-start gap-2 text-sumi-400">
            <ModeIcon mode={leg.mode} />
            <span className="eyebrow leading-[1.5]">{leg.service}</span>
          </div>
          {leg.durationNote && (
            <p className="mt-1 text-[12px] text-sumi-400">{leg.durationNote}</p>
          )}
        </div>
        <div className="flex shrink-0 flex-col items-end gap-1.5">
          <ReservationPill status={leg.status} />
          {countdown && (
            <Pill tone={countdown === 'now' ? 'shu' : 'dark'}>
              <Clock size={10} />
              {countdown}
            </Pill>
          )}
        </div>
      </div>

      {/* the journey itself */}
      <div className="px-4 py-4">
        {leg.transfer ? (
          <TransferDiagram leg={leg} />
        ) : (
          <div className="flex items-center gap-3">
            <Endpoint time={leg.depart} note={leg.departNote} place={leg.from} />
            <ArrowRight size={16} className="mt-4 shrink-0 text-sumi-200" />
            <Endpoint time={leg.arrive} note={leg.arriveNote} place={leg.to} align="right" />
          </div>
        )}
      </div>

      {/* footer detail */}
      {(leaveBy || leg.platformNote || leg.note || leg.reviewFlag || leg.officialUrl) && (
        <div className="space-y-2.5 border-t border-sumi-100 bg-sumi-50/40 px-4 py-3.5">
          {leaveBy && (
            <p className="flex items-center gap-2 text-[12.5px] font-semibold text-sumi-600">
              <Clock size={13} className="text-shu-400" />
              Be at the station by {leaveBy}
              <span className="font-normal text-sumi-400">
                · {leg.arriveAtStationMinutes} min before
              </span>
            </p>
          )}
          {leg.platformNote && (
            <p className="flex items-start gap-2 text-[12.5px] text-sumi-500">
              <MapPin size={13} className="mt-[2px] shrink-0 text-sumi-300" />
              {leg.platformNote}
            </p>
          )}
          {leg.note && <p className="text-[12.5px] leading-relaxed text-sumi-500">{leg.note}</p>}
          {leg.reviewFlag && <ReviewFlag text={leg.reviewFlag} />}
          {leg.officialUrl && (
            <LinkButton href={leg.officialUrl} className="w-full">
              Official page
            </LinkButton>
          )}
        </div>
      )}
    </article>
  )
}

function Endpoint({
  time,
  note,
  place,
  align = 'left',
}: {
  time?: string
  note?: string
  place: string
  align?: 'left' | 'right'
}) {
  return (
    <div className={`min-w-0 flex-1 ${align === 'right' ? 'text-right' : ''}`}>
      {time ? (
        <p className="tabular font-display text-[26px] font-medium leading-none text-sumi-800">
          {time}
        </p>
      ) : note ? (
        <p className="text-[13px] font-semibold leading-snug text-sumi-500">{note}</p>
      ) : null}
      {time && note && <p className="mt-0.5 text-[11px] text-sumi-400">{note}</p>}
      <p className="mt-1.5 text-[12.5px] leading-snug text-sumi-500">{place}</p>
    </div>
  )
}

/** The vertical transfer diagram the brief asked for: station, line, station. */
function TransferDiagram({ leg }: { leg: TransportLeg }) {
  const t = leg.transfer!
  return (
    <div className="relative pl-6">
      <span className="absolute left-[5px] top-2 bottom-2 w-px bg-sumi-200" aria-hidden />

      <Node dot="solid" label={leg.from} time={leg.depart} note={leg.departNote} />

      <p className="my-2 flex items-center gap-2 text-[12px] font-medium text-sumi-400">
        <span className="text-sumi-300">↓</span> {t.firstLeg}
      </p>

      <Node dot="hollow" label={t.at} sub="Change here" />

      <p className="my-2 flex items-center gap-2 text-[12px] font-medium text-sumi-400">
        <span className="text-sumi-300">↓</span> {t.secondLeg}
      </p>

      <Node dot="solid" label={leg.to} time={leg.arrive} note={leg.arriveNote} />
    </div>
  )
}

function Node({
  dot,
  label,
  time,
  note,
  sub,
}: {
  dot: 'solid' | 'hollow'
  label: string
  time?: string
  note?: string
  sub?: string
}) {
  return (
    <div className="relative">
      <span
        className={[
          'absolute -left-6 top-[7px] h-[11px] w-[11px] rounded-full',
          dot === 'solid' ? 'bg-sumi-700' : 'border-2 border-sumi-300 bg-surface',
        ].join(' ')}
        aria-hidden
      />
      <div className="flex items-baseline justify-between gap-3">
        <p className="text-[14px] font-semibold leading-tight text-sumi-700">{label}</p>
        {time && (
          <p className="tabular font-display text-[20px] font-medium leading-none text-sumi-800">
            {time}
          </p>
        )}
      </div>
      {(sub || note) && <p className="mt-0.5 text-[11.5px] text-sumi-400">{sub ?? note}</p>}
    </div>
  )
}
