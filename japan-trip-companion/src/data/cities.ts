import type { City, CityId } from './types'

/**
 * Coordinates are used for weather lookups and map centring only.
 * septemberNormal is a labelled fallback for dates outside the 16-day
 * forecast horizon — the UI never presents it as a forecast.
 */
export const cities: Record<CityId, City> = {
  tokyo: {
    id: 'tokyo',
    name: 'Tokyo',
    lat: 35.6895,
    lon: 139.6917,
    septemberNormal: {
      highC: 27,
      lowC: 21,
      note: 'Warm and humid, often 27-30C. Short heavy showers are common.',
    },
  },
  kamikochi: {
    id: 'kamikochi',
    name: 'Kamikochi',
    lat: 36.2494,
    lon: 137.6339,
    septemberNormal: {
      highC: 19,
      lowC: 9,
      note: 'At 1,500 m it is much cooler than Tokyo. Mornings can be 8-10C.',
    },
  },
  hirayu: {
    id: 'hirayu',
    name: 'Hirayu Onsen',
    lat: 36.1747,
    lon: 137.5544,
    septemberNormal: {
      highC: 21,
      lowC: 11,
      note: 'Mountain valley. Cool evenings — perfect onsen weather.',
    },
  },
  takayama: {
    id: 'takayama',
    name: 'Takayama',
    lat: 36.1461,
    lon: 137.2522,
    septemberNormal: {
      highC: 25,
      lowC: 14,
      note: 'Pleasant days, cool mornings. Good walking weather.',
    },
  },
  shirakawago: {
    id: 'shirakawago',
    name: 'Shirakawa-go',
    lat: 36.2578,
    lon: 136.9066,
    septemberNormal: {
      highC: 25,
      lowC: 15,
      note: 'Mountain village — can be misty early, clears through the morning.',
    },
  },
  kanazawa: {
    id: 'kanazawa',
    name: 'Kanazawa',
    lat: 36.5613,
    lon: 136.6562,
    septemberNormal: {
      highC: 27,
      lowC: 20,
      note: 'On the Sea of Japan side — the rainiest city on this trip. Carry an umbrella.',
    },
  },
  kyoto: {
    id: 'kyoto',
    name: 'Kyoto',
    lat: 35.0116,
    lon: 135.7681,
    septemberNormal: {
      highC: 28,
      lowC: 20,
      note: 'Basin city — humid and still. Early starts are genuinely cooler.',
    },
  },
  nara: {
    id: 'nara',
    name: 'Nara',
    lat: 34.6851,
    lon: 135.8048,
    septemberNormal: {
      highC: 28,
      lowC: 19,
      note: 'Similar to Kyoto. Nara Park is open and shadeless in the middle of the day.',
    },
  },
  osaka: {
    id: 'osaka',
    name: 'Osaka',
    lat: 34.6937,
    lon: 135.5023,
    septemberNormal: {
      highC: 29,
      lowC: 22,
      note: 'The warmest stop. Arcades and department stores are the cool escape.',
    },
  },
  kix: {
    id: 'kix',
    name: 'Kansai Airport',
    lat: 34.4342,
    lon: 135.2328,
    septemberNormal: {
      highC: 28,
      lowC: 23,
      note: 'Built on a bay island — breezier than central Osaka.',
    },
  },
}

export const cityList = Object.values(cities)
