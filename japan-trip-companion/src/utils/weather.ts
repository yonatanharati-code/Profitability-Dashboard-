import type { City } from '../data/types'

/**
 * Weather uses Open-Meteo, which is free and needs NO API KEY.
 * Nothing in this file is required for the itinerary to work — every failure
 * mode returns a labelled fallback and the rest of the app carries on.
 *
 * If you later want a different provider, this is the only file to change:
 * keep the DayWeather shape and everything else keeps working.
 */

const ENDPOINT = 'https://api.open-meteo.com/v1/forecast'

export interface DayWeather {
  date: string
  /** WMO weather code — see codeToConditions below. */
  code: number
  highC: number
  lowC: number
  /** Chance of precipitation, percent. */
  rainChance: number | null
  /** 'forecast' when it came from the API, 'seasonal' when it is the labelled average. */
  kind: 'forecast' | 'seasonal'
  summary: string
  /** Practical, weather-driven advice for the day. */
  advice: string[]
}

/** Open-Meteo publishes a rolling forecast, so far-future dates return nothing. */
export const FORECAST_HORIZON_DAYS = 16

const WMO: Record<number, string> = {
  0: 'Clear',
  1: 'Mainly clear',
  2: 'Partly cloudy',
  3: 'Overcast',
  45: 'Fog',
  48: 'Freezing fog',
  51: 'Light drizzle',
  53: 'Drizzle',
  55: 'Heavy drizzle',
  56: 'Freezing drizzle',
  57: 'Freezing drizzle',
  61: 'Light rain',
  63: 'Rain',
  65: 'Heavy rain',
  66: 'Freezing rain',
  67: 'Freezing rain',
  71: 'Light snow',
  73: 'Snow',
  75: 'Heavy snow',
  77: 'Snow grains',
  80: 'Rain showers',
  81: 'Rain showers',
  82: 'Violent showers',
  85: 'Snow showers',
  86: 'Snow showers',
  95: 'Thunderstorm',
  96: 'Thunderstorm with hail',
  99: 'Thunderstorm with hail',
}

export function describeCode(code: number): string {
  return WMO[code] ?? 'Unsettled'
}

export function isWet(code: number): boolean {
  return code >= 51 || (code >= 45 && code <= 48)
}

export function isStormy(code: number): boolean {
  return code >= 95
}

/**
 * Weather advice is where this stops being decoration. The rules below are
 * keyed to what each place actually demands of you.
 */
export function adviceFor(
  city: City,
  code: number,
  highC: number,
  rainChance: number | null,
  /**
   * True when this is the labelled September average rather than a forecast.
   * Place facts still apply, but we must not reassure that conditions are
   * good — we do not know that yet.
   */
  isSeasonal = false,
): string[] {
  const out: string[] = []
  const wet = isWet(code) || (rainChance != null && rainChance >= 50)

  if (city.id === 'kamikochi' || city.id === 'hirayu') {
    if (wet) {
      out.push('Rain in the valley — waterproof shoes and a rain jacket, not an umbrella. The riverside boardwalk gets slippery.')
    }
    out.push('It is 1,500 m up and much colder than Tokyo. Pack a warm layer for the morning even if the forecast looks mild.')
    if (isStormy(code)) out.push('Thunderstorms forecast — check trail conditions at the bus terminal information desk before setting out.')
  }

  if (city.id === 'shirakawago') {
    if (wet) out.push('Wet in the village — the Shiroyama viewpoint path gets muddy. The shuttle bus is the dry way up.')
  }

  if (city.id === 'kanazawa' && wet) {
    out.push('Kanazawa is the rainiest stop on this trip. Kenroku-en is genuinely beautiful in rain, and Omicho Market is covered.')
  }

  if ((city.id === 'kyoto' || city.id === 'nara' || city.id === 'osaka' || city.id === 'tokyo') && highC >= 30) {
    out.push(`${highC}C forecast — front-load the outdoor sights and keep the middle of the day for arcades, department stores or a long lunch.`)
  }

  if (city.id === 'nara' && highC >= 28) {
    out.push('Nara Park has almost no shade. Carry water and do Todai-ji before noon.')
  }

  if (city.id === 'osaka' && wet) {
    out.push('Shinsaibashi-suji and Kuromon Market are both fully covered — a wet day barely touches the Osaka plan.')
  }

  if (city.id === 'kyoto' && wet) {
    out.push('Rain suits Kyoto. Nanzen-ji and the Philosopher\'s Path are better in drizzle than in glare.')
  }

  if (city.id === 'tokyo' && wet) {
    out.push('Tokyo showers are usually short and heavy. A folding umbrella from any convenience store costs about ¥600.')
  }

  if (!isSeasonal && !wet && highC < 30 && out.length === 0) {
    out.push('Good conditions — no changes needed to the plan.')
  }

  return out
}

/** The labelled fallback used outside the forecast window or on any failure. */
export function seasonalFallback(city: City, date: string): DayWeather {
  return {
    date,
    code: 2,
    highC: city.septemberNormal.highC,
    lowC: city.septemberNormal.lowC,
    rainChance: null,
    kind: 'seasonal',
    summary: city.septemberNormal.note,
    advice: adviceFor(city, 2, city.septemberNormal.highC, null, true),
  }
}

/**
 * Fetch a forecast for one city across a date range.
 * Resolves to a map of ISO date -> DayWeather, possibly empty. Never throws.
 */
export async function fetchForecast(
  city: City,
  startDate: string,
  endDate: string,
): Promise<Record<string, DayWeather>> {
  const url =
    `${ENDPOINT}?latitude=${city.lat}&longitude=${city.lon}` +
    `&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max` +
    `&timezone=Asia%2FTokyo&start_date=${startDate}&end_date=${endDate}`

  try {
    const res = await fetch(url)
    if (!res.ok) return {}
    const json = (await res.json()) as {
      daily?: {
        time?: string[]
        weather_code?: number[]
        temperature_2m_max?: (number | null)[]
        temperature_2m_min?: (number | null)[]
        precipitation_probability_max?: (number | null)[]
      }
    }
    const d = json.daily
    if (!d?.time?.length) return {}

    const out: Record<string, DayWeather> = {}
    d.time.forEach((iso, i) => {
      const high = d.temperature_2m_max?.[i]
      const low = d.temperature_2m_min?.[i]
      const code = d.weather_code?.[i]
      if (high == null || low == null || code == null) return
      const rain = d.precipitation_probability_max?.[i] ?? null
      out[iso] = {
        date: iso,
        code,
        highC: Math.round(high),
        lowC: Math.round(low),
        rainChance: rain,
        kind: 'forecast',
        summary: describeCode(code),
        advice: adviceFor(city, code, Math.round(high), rain),
      }
    })
    return out
  } catch {
    return {}
  }
}
