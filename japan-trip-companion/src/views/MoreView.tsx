import { useState } from 'react'
import {
  Check, Heart, Luggage, StickyNote, Utensils, BedDouble, Train, ListChecks,
  Info, Trash2, ShoppingBag, WifiOff,
} from 'lucide-react'
import { dietRules, luggagePlan, preTripChecklist, trip, tripNotices } from '../data/trip'
import { hotels } from '../data/hotels'
import { transport } from '../data/transport'
import { restaurants, restaurantById } from '../data/restaurants'
import { shopping } from '../data/shopping'
import { days } from '../data/days'
import type { TripState } from '../hooks/useTripState'
import { HotelCard } from '../components/HotelCard'
import { TransportCard } from '../components/TransportCard'
import { RestaurantCard } from '../components/RestaurantCard'
import { ShoppingCard } from '../components/ShoppingCard'
import { EmptyNote, NoticeCard, Pill, SectionHeader } from '../components/ui'
import { shortDateLabel } from '../utils/date'

type Panel =
  | 'menu'
  | 'hotels'
  | 'transport'
  | 'luggage'
  | 'food'
  | 'shopping'
  | 'checklist'
  | 'favourites'
  | 'notes'
  | 'about'

const MENU: { id: Panel; label: string; hint: string; icon: typeof BedDouble }[] = [
  { id: 'hotels', label: 'Hotels', hint: 'All six stays, with dates and directions', icon: BedDouble },
  { id: 'transport', label: 'All transport', hint: 'Every leg of the trip in one list', icon: Train },
  { id: 'luggage', label: 'Luggage plan', hint: 'The forwarding strategy, day by day', icon: Luggage },
  { id: 'food', label: 'Food rules', hint: 'No pork, no seafood — and where it hides', icon: Utensils },
  { id: 'shopping', label: 'Shopping', hint: 'Every area, by city', icon: ShoppingBag },
  { id: 'checklist', label: 'Before you fly', hint: 'The PDF checklist, tickable', icon: ListChecks },
  { id: 'favourites', label: 'Saved places', hint: 'Restaurants you hearted', icon: Heart },
  { id: 'notes', label: 'Your notes', hint: 'Everything you wrote down', icon: StickyNote },
  { id: 'about', label: 'About this app', hint: 'Data sources, offline, resetting', icon: Info },
]

export function MoreView({ state }: { state: TripState }) {
  const [panel, setPanel] = useState<Panel>('menu')

  if (panel !== 'menu') {
    return (
      <div className="space-y-6">
        <button
          type="button"
          onClick={() => setPanel('menu')}
          className="inline-flex min-h-[40px] items-center gap-1.5 text-[13px] font-semibold text-sumi-500 hover:text-sumi-800"
        >
          ← More
        </button>
        {panel === 'hotels' && <HotelsPanel />}
        {panel === 'transport' && <TransportPanel />}
        {panel === 'luggage' && <LuggagePanel />}
        {panel === 'food' && <FoodPanel state={state} />}
        {panel === 'shopping' && <ShoppingPanel />}
        {panel === 'checklist' && <ChecklistPanel state={state} />}
        {panel === 'favourites' && <FavouritesPanel state={state} />}
        {panel === 'notes' && <NotesPanel state={state} />}
        {panel === 'about' && <AboutPanel state={state} />}
      </div>
    )
  }

  const savedCount = Object.keys(state.favourites).length
  const noteCount = Object.keys(state.notes).length
  const checkedCount = Object.keys(state.checklist).length

  return (
    <div className="space-y-8">
      <header>
        <p className="eyebrow-lg">More</p>
        <h1 className="mt-1.5 font-display text-[30px] font-medium leading-tight tracking-tight text-sumi-800">
          Everything else
        </h1>
      </header>

      <section className="space-y-2">
        {MENU.map(({ id, label, hint, icon: Icon }) => {
          const badge =
            id === 'favourites' && savedCount
              ? String(savedCount)
              : id === 'notes' && noteCount
                ? String(noteCount)
                : id === 'checklist'
                  ? `${checkedCount}/${preTripChecklist.length}`
                  : null
          return (
            <button
              key={id}
              type="button"
              onClick={() => setPanel(id)}
              className="card flex w-full items-center gap-3.5 px-4 py-3.5 text-left transition active:scale-[0.99]"
            >
              <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-sumi-50 text-sumi-500">
                <Icon size={17} />
              </span>
              <span className="min-w-0 flex-1">
                <span className="block text-[14px] font-semibold text-sumi-800">{label}</span>
                <span className="block text-[12px] leading-snug text-sumi-400">{hint}</span>
              </span>
              {badge && <Pill tone="neutral">{badge}</Pill>}
            </button>
          )
        })}
      </section>

      <section className="space-y-2.5">
        <SectionHeader title="Trip-wide notes" />
        {tripNotices.map((n) => (
          <NoticeCard key={n.id} notice={n} />
        ))}
      </section>
    </div>
  )
}

/* ------------------------------------------------------------ panels */

function HotelsPanel() {
  return (
    <div className="space-y-4">
      <SectionHeader
        title="Hotels"
        hint="All six confirmed in the PDF. Dates are the PDF's night ranges."
      />
      {hotels.map((h) => (
        <HotelCard key={h.id} hotel={h} />
      ))}
    </div>
  )
}

function TransportPanel() {
  return (
    <div className="space-y-4">
      <SectionHeader
        title="All transport"
        hint="Every leg, in date order. Times exactly as they appear in the PDF."
      />
      {transport.map((leg) => (
        <div key={leg.id}>
          <p className="eyebrow mb-1.5">{shortDateLabel(leg.date)}</p>
          <TransportCard leg={leg} />
        </div>
      ))}
    </div>
  )
}

function LuggagePanel() {
  return (
    <div className="space-y-5">
      <SectionHeader
        title="Luggage plan"
        hint="Straight from the PDF. The whole Alpine section depends on getting 14 Sep right."
      />
      <ol className="space-y-3">
        {luggagePlan.map((step, i) => (
          <li key={step.id} className="card px-4 py-3.5">
            <div className="flex items-start gap-3">
              <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-ai-50 text-[12px] font-bold text-ai-600">
                {i + 1}
              </span>
              <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-2">
                  <Pill tone="ai">{shortDateLabel(step.date)}</Pill>
                </div>
                <p className="mt-1.5 text-[14px] font-semibold leading-snug text-sumi-800">
                  {step.title}
                </p>
                <p className="mt-1 text-[12.5px] leading-relaxed text-sumi-500">{step.body}</p>
              </div>
            </div>
          </li>
        ))}
      </ol>
    </div>
  )
}

function FoodPanel({ state }: { state: TripState }) {
  return (
    <div className="space-y-5">
      <div>
        <SectionHeader title="Food rules" />
        <div className="card px-4 py-4">
          <p className="font-display text-[19px] font-medium text-shu-500">{dietRules.headline}</p>
          <p className="mt-1.5 text-[13px] text-sumi-500">{dietRules.fine}</p>
        </div>
      </div>

      <div className="space-y-2.5">
        <SectionHeader title="Where it hides" />
        {dietRules.watchList.map((w) => (
          <div key={w.title} className="card px-4 py-3.5">
            <p className="text-[13.5px] font-semibold text-sumi-800">{w.title}</p>
            <p className="mt-1 text-[12.5px] leading-relaxed text-sumi-500">{w.body}</p>
          </div>
        ))}
      </div>

      <div>
        <SectionHeader
          title="Every recommendation"
          hint={`${restaurants.length} places across the trip, all filtered against the two rules.`}
        />
        <div className="space-y-2.5">
          {restaurants.map((r) => (
            <div key={r.id}>
              <p className="eyebrow mb-1">{r.city}</p>
              <RestaurantCard restaurant={r} state={state} />
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

function ShoppingPanel() {
  const byCity = [...new Set(shopping.map((s) => s.city))]
  return (
    <div className="space-y-5">
      <SectionHeader
        title="Shopping"
        hint="Tokyo and Osaka carry the most, as the PDF intends."
      />
      {byCity.map((city) => (
        <div key={city}>
          <p className="eyebrow mb-2">{city}</p>
          <div className="space-y-2.5">
            {shopping
              .filter((s) => s.city === city)
              .map((s) => (
                <ShoppingCard key={s.id} spot={s} />
              ))}
          </div>
        </div>
      ))}
    </div>
  )
}

function ChecklistPanel({ state }: { state: TripState }) {
  const done = preTripChecklist.filter((c) => state.checklist[c.id]).length
  return (
    <div className="space-y-4">
      <SectionHeader
        title="Before you fly"
        hint={`${done} of ${preTripChecklist.length} done. The first five are the PDF's own checklist.`}
      />
      <ul className="space-y-2">
        {preTripChecklist.map((item) => {
          const on = !!state.checklist[item.id]
          return (
            <li key={item.id}>
              <button
                type="button"
                onClick={() => state.toggleChecklist(item.id)}
                aria-pressed={on}
                className="card flex w-full items-start gap-3 px-4 py-3.5 text-left"
              >
                <span
                  className={[
                    'mt-[1px] flex h-[22px] w-[22px] shrink-0 items-center justify-center rounded-md border transition',
                    on
                      ? 'border-sage-400 bg-sage-400 text-white'
                      : 'border-sumi-200 text-transparent',
                  ].join(' ')}
                >
                  <Check size={13} strokeWidth={3} />
                </span>
                <span
                  className={[
                    'text-[13px] leading-relaxed',
                    on ? 'text-sumi-400 line-through decoration-sumi-200' : 'text-sumi-700',
                  ].join(' ')}
                >
                  {item.label}
                </span>
              </button>
            </li>
          )
        })}
      </ul>
    </div>
  )
}

function FavouritesPanel({ state }: { state: TripState }) {
  const saved = Object.keys(state.favourites)
    .map(restaurantById)
    .filter(Boolean) as NonNullable<ReturnType<typeof restaurantById>>[]

  return (
    <div className="space-y-4">
      <SectionHeader title="Saved places" />
      {saved.length === 0 ? (
        <EmptyNote>
          Nothing saved yet. Tap the heart on any restaurant card and it turns up here.
        </EmptyNote>
      ) : (
        <div className="space-y-2.5">
          {saved.map((r) => (
            <RestaurantCard key={r.id} restaurant={r} state={state} />
          ))}
        </div>
      )}
    </div>
  )
}

function NotesPanel({ state }: { state: TripState }) {
  const entries = Object.entries(state.notes)
  const lookup = new Map(
    days.flatMap((d) => d.activities.map((a) => [a.id, { activity: a, day: d }] as const)),
  )

  return (
    <div className="space-y-4">
      <SectionHeader title="Your notes" />
      {entries.length === 0 ? (
        <EmptyNote>
          No notes yet. Expand any activity and use “Add note” — they are saved on this device.
        </EmptyNote>
      ) : (
        <ul className="space-y-2.5">
          {entries.map(([id, text]) => {
            const hit = lookup.get(id)
            return (
              <li key={id} className="card px-4 py-3.5">
                {hit && (
                  <p className="eyebrow">
                    {shortDateLabel(hit.day.date)} · {hit.activity.title}
                  </p>
                )}
                <p className="mt-1.5 whitespace-pre-wrap text-[13px] leading-relaxed text-sumi-600">
                  {text}
                </p>
              </li>
            )
          })}
        </ul>
      )}
    </div>
  )
}

function AboutPanel({ state }: { state: TripState }) {
  const [confirm, setConfirm] = useState(false)
  return (
    <div className="space-y-5">
      <SectionHeader title="About this app" />

      <div className="card space-y-3 px-4 py-4 text-[12.5px] leading-relaxed text-sumi-500">
        <p>
          <span className="font-semibold text-sumi-700">Where the data comes from.</span> The route,
          dates, hotels, transport times, daily plans and the luggage strategy are taken from{' '}
          <em>{trip.sourceDoc}</em> and are not changed anywhere in this app. Opening hours, prices
          and booking rules were checked against official sources in August 2026 — and where they
          could not be, the field is left out and the card links to the official site instead.
          Cards marked <span className="font-semibold">“Our suggestion”</span> are ours, not the
          PDF's.
        </p>
        <p>
          <span className="font-semibold text-sumi-700">Suggested times.</span> A tilde before a
          time — <span className="tabular">~10:00</span> — means we added it for pacing. A plain
          time is fixed in the PDF.
        </p>
        <p>
          <span className="font-semibold text-sumi-700">Check this.</span> Amber notes flag a
          specific field the PDF left open, rather than us inventing a value.
        </p>
      </div>

      <div className="card space-y-2.5 px-4 py-4">
        <p className="flex items-center gap-2 text-[13px] font-semibold text-sumi-700">
          <WifiOff size={15} className="text-sumi-400" /> Offline
        </p>
        <p className="text-[12.5px] leading-relaxed text-sumi-500">
          The whole itinerary is bundled into the app, so days, transport, hotels, food and notes
          work with no connection at all. Only three things need the network: the weather forecast,
          Google Maps links, and official websites. Add it to your home screen and it opens like an
          app.
        </p>
      </div>

      <div className="card space-y-3 px-4 py-4">
        <p className="text-[13px] font-semibold text-sumi-700">Your data</p>
        <p className="text-[12.5px] leading-relaxed text-sumi-500">
          Completed items, skips, reordering, favourites, notes and checklist ticks are stored on
          this device only. No account, no server.
        </p>
        {confirm ? (
          <div className="space-y-2">
            <p className="text-[12.5px] font-semibold text-shu-600">
              This erases everything you have marked. Sure?
            </p>
            <div className="grid grid-cols-2 gap-2">
              <button type="button" onClick={() => setConfirm(false)} className="btn-secondary">
                Cancel
              </button>
              <button
                type="button"
                onClick={() => {
                  state.resetEverything()
                  setConfirm(false)
                }}
                className="btn-accent"
              >
                <Trash2 size={14} /> Erase
              </button>
            </div>
          </div>
        ) : (
          <button type="button" onClick={() => setConfirm(true)} className="btn-secondary w-full">
            <Trash2 size={14} /> Reset everything
          </button>
        )}
      </div>
    </div>
  )
}
