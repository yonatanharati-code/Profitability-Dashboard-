import { CalendarDays, Map, Sun, Ticket, MoreHorizontal } from 'lucide-react'

export type Tab = 'today' | 'trip' | 'map' | 'bookings' | 'more'

const TABS: { id: Tab; label: string; icon: typeof Sun }[] = [
  { id: 'today', label: 'Today', icon: Sun },
  { id: 'trip', label: 'Trip', icon: CalendarDays },
  { id: 'map', label: 'Map', icon: Map },
  { id: 'bookings', label: 'Bookings', icon: Ticket },
  { id: 'more', label: 'More', icon: MoreHorizontal },
]

export function BottomNav({ active, onChange }: { active: Tab; onChange: (t: Tab) => void }) {
  return (
    <nav
      aria-label="Main"
      className="fixed inset-x-0 bottom-0 z-40 mx-auto max-w-2xl border-t border-sumi-100 bg-paper/90 shadow-nav backdrop-blur-xl sm:border-x"
      style={{ paddingBottom: 'env(safe-area-inset-bottom)' }}
    >
      <ul className="mx-auto flex max-w-2xl">
        {TABS.map(({ id, label, icon: Icon }) => {
          const on = active === id
          return (
            <li key={id} className="flex-1">
              <button
                type="button"
                onClick={() => onChange(id)}
                aria-current={on ? 'page' : undefined}
                className="flex min-h-[58px] w-full flex-col items-center justify-center gap-1 pt-1.5 pb-1 transition active:scale-95"
              >
                <Icon
                  size={20}
                  strokeWidth={on ? 2.2 : 1.7}
                  className={on ? 'text-shu-500' : 'text-sumi-400'}
                />
                <span
                  className={[
                    'text-[10px] font-semibold tracking-wide',
                    on ? 'text-sumi-800' : 'text-sumi-400',
                  ].join(' ')}
                >
                  {label}
                </span>
              </button>
            </li>
          )
        })}
      </ul>
    </nav>
  )
}
