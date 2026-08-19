import { useEffect, useState } from 'react'
import { cities } from '../data/cities'
import type { CityId } from '../data/types'
import { fetchForecast, seasonalFallback, type DayWeather } from '../utils/weather'
import { trip } from '../data/trip'

type Status = 'idle' | 'loading' | 'ready' | 'offline'

/** cityId -> ISO date -> forecast */
type Cache = Partial<Record<CityId, Record<string, DayWeather>>>

/**
 * One fetch per city for the whole trip window, cached for the session.
 * Any city we fail to reach simply falls back to its labelled September
 * average, so the weather block always renders something honest.
 */
export function useWeather(cityIds: CityId[]) {
  const [cache, setCache] = useState<Cache>({})
  const [status, setStatus] = useState<Status>('idle')
  const key = [...new Set(cityIds)].sort().join(',')

  useEffect(() => {
    const wanted = key ? (key.split(',') as CityId[]) : []
    const missing = wanted.filter((id) => !(id in cache))
    if (missing.length === 0) return

    let cancelled = false
    setStatus('loading')

    Promise.all(
      missing.map(async (id) => {
        const city = cities[id]
        const data = await fetchForecast(city, trip.startDate, trip.endDate)
        return [id, data] as const
      }),
    ).then((results) => {
      if (cancelled) return
      const anyData = results.some(([, data]) => Object.keys(data).length > 0)
      setCache((prev) => {
        const next = { ...prev }
        results.forEach(([id, data]) => {
          next[id] = data
        })
        return next
      })
      setStatus(anyData ? 'ready' : 'offline')
    })

    return () => {
      cancelled = true
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key])

  function get(cityId: CityId, date: string): DayWeather {
    const hit = cache[cityId]?.[date]
    if (hit) return hit
    return seasonalFallback(cities[cityId], date)
  }

  return { get, status }
}
