import {
  Sun, CloudSun, Cloud, CloudRain, CloudDrizzle, CloudLightning, CloudFog, Snowflake, Umbrella,
} from 'lucide-react'
import type { DayWeather } from '../utils/weather'
import type { City } from '../data/types'

function Icon({ code, size = 22 }: { code: number; size?: number }) {
  if (code === 0) return <Sun size={size} />
  if (code === 1 || code === 2) return <CloudSun size={size} />
  if (code === 3) return <Cloud size={size} />
  if (code === 45 || code === 48) return <CloudFog size={size} />
  if (code >= 51 && code <= 57) return <CloudDrizzle size={size} />
  if (code >= 71 && code <= 77) return <Snowflake size={size} />
  if (code >= 95) return <CloudLightning size={size} />
  return <CloudRain size={size} />
}

/**
 * Weather earns its place by changing the advice, not by showing a number.
 * The seasonal fallback is labelled as such — we never dress an average up
 * as a forecast.
 */
export function WeatherCard({
  weather,
  city,
  compact = false,
}: {
  weather: DayWeather
  city: City
  compact?: boolean
}) {
  const seasonal = weather.kind === 'seasonal'

  if (compact) {
    return (
      <div className="flex items-center gap-2 text-sumi-500">
        <Icon code={weather.code} size={16} />
        <span className="tabular text-[13px] font-semibold text-sumi-700">{weather.highC}°</span>
        <span className="tabular text-[12px] text-sumi-400">{weather.lowC}°</span>
        {seasonal && <span className="text-[10px] uppercase tracking-wider text-sumi-300">avg</span>}
      </div>
    )
  }

  return (
    <div className="card px-4 py-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="eyebrow">{seasonal ? 'September average' : 'Forecast'} · {city.name}</p>
          <div className="mt-2 flex items-baseline gap-2">
            <span className="tabular font-display text-[30px] font-medium leading-none text-sumi-800">
              {weather.highC}°
            </span>
            <span className="tabular text-[15px] text-sumi-400">{weather.lowC}°</span>
          </div>
          <p className="mt-1.5 text-[12.5px] leading-snug text-sumi-500">{weather.summary}</p>
          {weather.rainChance != null && weather.rainChance > 10 && (
            <p className="mt-1.5 flex items-center gap-1.5 text-[12px] font-medium text-ai-500">
              <Umbrella size={12} />
              {weather.rainChance}% chance of rain
            </p>
          )}
        </div>
        <span className="shrink-0 text-sumi-300">
          <Icon code={weather.code} size={30} />
        </span>
      </div>

      {weather.advice.length > 0 && (
        <ul className="mt-3.5 space-y-2 border-t border-sumi-100 pt-3.5">
          {weather.advice.map((a, i) => (
            <li key={i} className="flex gap-2 text-[12.5px] leading-relaxed text-sumi-600">
              <span className="mt-[7px] h-1 w-1 shrink-0 rounded-full bg-shu-400" aria-hidden />
              {a}
            </li>
          ))}
        </ul>
      )}

      {seasonal && (
        <p className="mt-3 text-[10.5px] leading-relaxed text-sumi-300">
          No live forecast for this date yet — forecasts reach about 16 days ahead. This is a
          typical late-September average for {city.name}, shown so the advice still works.
        </p>
      )}
    </div>
  )
}
