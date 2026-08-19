import type { ReactNode } from 'react'
import { AlertTriangle, Info, Luggage, Users, ExternalLink } from 'lucide-react'
import type { Notice, Priority, ReservationStatus, Source, DietVerdict } from '../data/types'

/* ------------------------------------------------------------------ Pill */

const PILL_TONES = {
  neutral: 'bg-sumi-50 text-sumi-500',
  outline: 'border border-sumi-200 text-sumi-500',
  shu: 'bg-shu-50 text-shu-600',
  sage: 'bg-sage-50 text-sage-600',
  ai: 'bg-ai-50 text-ai-600',
  gold: 'bg-gold-50 text-gold-600',
  dark: 'bg-sumi-700 text-paper',
} as const

export type PillTone = keyof typeof PILL_TONES

export function Pill({
  children,
  tone = 'neutral',
  className = '',
}: {
  children: ReactNode
  tone?: PillTone
  className?: string
}) {
  return <span className={`pill ${PILL_TONES[tone]} ${className}`}>{children}</span>
}

/* --------------------------------------------------- Reservation status */

const RES_TONE: Record<ReservationStatus, PillTone> = {
  booked: 'sage',
  'no-reservation': 'ai',
  'book-later': 'gold',
  local: 'neutral',
}

const RES_LABEL: Record<ReservationStatus, string> = {
  booked: 'Booked',
  'no-reservation': 'No reservation needed',
  'book-later': 'Book later',
  local: 'Local transport',
}

export function ReservationPill({ status }: { status: ReservationStatus }) {
  return <Pill tone={RES_TONE[status]}>{RES_LABEL[status]}</Pill>
}

/* ----------------------------------------------------------- Priority */

const PRIORITY_LABEL: Record<Priority, string> = {
  core: 'Core',
  optional: 'Optional',
  'if-time': 'If time',
}

export function PriorityPill({ priority }: { priority: Priority }) {
  if (priority === 'core') return <Pill tone="shu">Core</Pill>
  return <Pill tone="outline">{PRIORITY_LABEL[priority]}</Pill>
}

/* ------------------------------------------------------------- Diet */

const DIET: Record<DietVerdict, { tone: PillTone; label: string }> = {
  good: { tone: 'sage', label: 'Works for us' },
  ask: { tone: 'gold', label: 'Order carefully' },
  avoid: { tone: 'shu', label: 'Not for a meal' },
}

export function DietPill({ verdict }: { verdict: DietVerdict }) {
  const { tone, label } = DIET[verdict]
  return <Pill tone={tone}>{label}</Pill>
}

/* ---------------------------------------------------------- Provenance */

/**
 * Honest sourcing, quietly. PDF facts get no badge at all — they are the
 * baseline. Only unverified suggestions are marked, so the marking means
 * something.
 */
export function SourceHint({ source }: { source: Source }) {
  if (source !== 'unverified') return null
  return (
    <span className="text-[10px] font-medium uppercase tracking-[0.1em] text-sumi-300">
      Our suggestion
    </span>
  )
}

export function ReviewFlag({ text }: { text: string }) {
  return (
    <div className="flex gap-2 rounded-xl bg-gold-50 px-3 py-2 text-[12px] leading-relaxed text-gold-600">
      <AlertTriangle size={14} className="mt-[2px] shrink-0" />
      <span>
        <span className="font-semibold">Check this: </span>
        {text}
      </span>
    </div>
  )
}

/* ------------------------------------------------------------- Notices */

const NOTICE_STYLE: Record<
  Notice['tone'],
  { wrap: string; icon: ReactNode; label: string }
> = {
  warning: {
    wrap: 'bg-shu-50 border-shu-100 text-shu-600',
    icon: <AlertTriangle size={15} />,
    label: 'Heads up',
  },
  luggage: {
    wrap: 'bg-ai-50 border-ai-100 text-ai-600',
    icon: <Luggage size={15} />,
    label: 'Luggage',
  },
  crowd: {
    wrap: 'bg-gold-50 border-gold-100 text-gold-600',
    icon: <Users size={15} />,
    label: 'Crowds',
  },
  info: {
    wrap: 'bg-sage-50 border-sage-100 text-sage-600',
    icon: <Info size={15} />,
    label: 'Note',
  },
}

export function NoticeCard({ notice }: { notice: Notice }) {
  const style = NOTICE_STYLE[notice.tone]
  return (
    <div className={`rounded-card border px-4 py-3 ${style.wrap}`}>
      <div className="flex items-center gap-2">
        <span className="shrink-0">{style.icon}</span>
        <span className="text-[10px] font-bold uppercase tracking-[0.16em] opacity-70">
          {style.label}
        </span>
      </div>
      <p className="mt-1.5 text-[13px] font-semibold leading-snug">{notice.title}</p>
      <p className="mt-1 text-[12.5px] leading-relaxed opacity-90">{notice.body}</p>
    </div>
  )
}

/* ------------------------------------------------------------- Buttons */

export function LinkButton({
  href,
  children,
  variant = 'secondary',
  className = '',
}: {
  href: string
  children: ReactNode
  variant?: 'primary' | 'secondary' | 'accent'
  className?: string
}) {
  const cls =
    variant === 'primary' ? 'btn-primary' : variant === 'accent' ? 'btn-accent' : 'btn-secondary'
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className={`${cls} ${className}`}
    >
      {children}
      <ExternalLink size={13} className="opacity-50" />
    </a>
  )
}

/* -------------------------------------------------------- Section head */

export function SectionHeader({
  title,
  hint,
  action,
}: {
  title: string
  hint?: string
  action?: ReactNode
}) {
  return (
    <div className="mb-3 flex items-end justify-between gap-3">
      <div>
        <h2 className="eyebrow-lg">{title}</h2>
        {hint && <p className="mt-1 text-[12px] leading-snug text-sumi-400">{hint}</p>}
      </div>
      {action}
    </div>
  )
}

/* ------------------------------------------------------------ Empty */

export function EmptyNote({ children }: { children: ReactNode }) {
  return (
    <p className="rounded-card border border-dashed border-sumi-200 px-4 py-6 text-center text-[13px] text-sumi-400">
      {children}
    </p>
  )
}
