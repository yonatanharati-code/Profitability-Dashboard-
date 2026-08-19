import { useCallback, useEffect, useMemo, useState } from 'react'
import { load, save, clearAll } from '../utils/storage'

/**
 * Everything the user changes lives here and is persisted to localStorage.
 * One hook, one source of truth, so any view can read or write trip state.
 */

type IdSet = Record<string, true>
type NoteMap = Record<string, string>
/** Per-day ordering: dayDate -> ordered activity ids. Missing = data order. */
type OrderMap = Record<string, string[]>
type BookingStatusMap = Record<string, 'done' | 'skipped'>

interface Persisted {
  completed: IdSet
  skipped: IdSet
  favourites: IdSet
  notes: NoteMap
  order: OrderMap
  checklist: IdSet
  bookingStatus: BookingStatusMap
}

const EMPTY: Persisted = {
  completed: {},
  skipped: {},
  favourites: {},
  notes: {},
  order: {},
  checklist: {},
  bookingStatus: {},
}

function useStored<K extends keyof Persisted>(key: K, fallback: Persisted[K]) {
  const [value, setValue] = useState<Persisted[K]>(() => load(key, fallback))
  useEffect(() => {
    save(key, value)
  }, [key, value])
  return [value, setValue] as const
}

export function useTripState() {
  const [completed, setCompleted] = useStored('completed', EMPTY.completed)
  const [skipped, setSkipped] = useStored('skipped', EMPTY.skipped)
  const [favourites, setFavourites] = useStored('favourites', EMPTY.favourites)
  const [notes, setNotes] = useStored('notes', EMPTY.notes)
  const [order, setOrder] = useStored('order', EMPTY.order)
  const [checklist, setChecklist] = useStored('checklist', EMPTY.checklist)
  const [bookingStatus, setBookingStatus] = useStored('bookingStatus', EMPTY.bookingStatus)

  const toggleIn = useCallback(
    (setter: (fn: (prev: IdSet) => IdSet) => void) => (id: string) => {
      setter((prev) => {
        const next = { ...prev }
        if (next[id]) delete next[id]
        else next[id] = true
        return next
      })
    },
    [],
  )

  const toggleCompleted = useMemo(() => toggleIn(setCompleted), [toggleIn, setCompleted])
  const toggleSkipped = useMemo(() => toggleIn(setSkipped), [toggleIn, setSkipped])
  const toggleFavourite = useMemo(() => toggleIn(setFavourites), [toggleIn, setFavourites])
  const toggleChecklist = useMemo(() => toggleIn(setChecklist), [toggleIn, setChecklist])

  const setNote = useCallback(
    (id: string, text: string) => {
      setNotes((prev) => {
        const next = { ...prev }
        const trimmed = text.trim()
        if (trimmed) next[id] = text
        else delete next[id]
        return next
      })
    },
    [setNotes],
  )

  const setBooking = useCallback(
    (id: string, status: 'done' | 'skipped' | null) => {
      setBookingStatus((prev) => {
        const next = { ...prev }
        if (status) next[id] = status
        else delete next[id]
        return next
      })
    },
    [setBookingStatus],
  )

  /**
   * Reorder within a day. Takes the current effective order so the stored
   * array always contains every id for that day — no drift if data changes.
   */
  const moveActivity = useCallback(
    (dayDate: string, currentIds: string[], id: string, direction: -1 | 1) => {
      const idx = currentIds.indexOf(id)
      const target = idx + direction
      if (idx === -1 || target < 0 || target >= currentIds.length) return
      const next = [...currentIds]
      ;[next[idx], next[target]] = [next[target], next[idx]]
      setOrder((prev) => ({ ...prev, [dayDate]: next }))
    },
    [setOrder],
  )

  const resetOrder = useCallback(
    (dayDate: string) => {
      setOrder((prev) => {
        const next = { ...prev }
        delete next[dayDate]
        return next
      })
    },
    [setOrder],
  )

  const resetEverything = useCallback(() => {
    clearAll()
    setCompleted({})
    setSkipped({})
    setFavourites({})
    setNotes({})
    setOrder({})
    setChecklist({})
    setBookingStatus({})
  }, [setCompleted, setSkipped, setFavourites, setNotes, setOrder, setChecklist, setBookingStatus])

  return {
    completed,
    skipped,
    favourites,
    notes,
    order,
    checklist,
    bookingStatus,
    toggleCompleted,
    toggleSkipped,
    toggleFavourite,
    toggleChecklist,
    setNote,
    setBooking,
    moveActivity,
    resetOrder,
    resetEverything,
  }
}

export type TripState = ReturnType<typeof useTripState>
