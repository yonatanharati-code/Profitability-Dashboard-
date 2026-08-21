import type { Hotel } from './types'

/**
 * All six stays are marked "closed / confirmed" in the PDF.
 * Dates below are the PDF's night ranges, verbatim.
 * Check-in / check-out clock times are NOT in the PDF — they are flagged for review.
 */
export const hotels: Hotel[] = [
  {
    id: 'bespoke-shinjuku',
    name: 'Bespoke Hotel Shinjuku',
    city: 'tokyo',
    cityLabel: 'Tokyo',
    checkIn: '2026-09-09',
    checkOut: '2026-09-15',
    nights: 6,
    status: 'confirmed',
    goodFor: ['Shinjuku', 'Kabukicho', 'Golden Gai', 'Late dinners', 'Shopping'],
    locationNote:
      'Base for the whole Tokyo week. Shinjuku puts every train line you need within walking distance, and the neighbourhood is alive late — useful after an evening arrival.',
    nearestStation: 'Shinjuku Station',
    mapsQuery: 'Bespoke Hotel Shinjuku Tokyo',
    source: 'pdf',
    reviewFlag: 'Exact check-in / check-out times not given in the PDF.',
  },
  {
    id: 'miyama-ouan',
    name: 'Miyama Ouan',
    city: 'hirayu',
    cityLabel: 'Hirayu Onsen',
    checkIn: '2026-09-15',
    checkOut: '2026-09-16',
    nights: 1,
    status: 'confirmed',
    goodFor: ['Onsen', 'Ryokan dinner', 'Recovering after Kamikochi'],
    locationNote:
      'A ryokan in the Hirayu Onsen village, a short walk from the bus stop. This is the reward at the end of the Kamikochi walking day: bath, then dinner, then sleep.',
    nearestStation: 'Hirayu Onsen bus terminal',
    mapsQuery: 'Miyama Ouan Hirayu Onsen',
    source: 'pdf',
    reviewFlag:
      'Ryokan dinner is usually served at a fixed sitting — confirm your time at check-in, and re-confirm the no-pork / no-seafood request.',
  },
  {
    id: 'takayama-ouan',
    name: 'Takayama Ouan',
    city: 'takayama',
    cityLabel: 'Takayama',
    checkIn: '2026-09-16',
    checkOut: '2026-09-17',
    nights: 1,
    status: 'confirmed',
    goodFor: ['Old Town on foot', 'Rooftop onsen', 'Next-morning bus'],
    locationNote:
      'Close to Takayama Station and the Nohi Bus Center, which matters for the 08:50 bus to Shirakawa-go. Sanmachi Old Town is an easy walk.',
    nearestStation: 'JR Takayama Station',
    mapsQuery: 'Takayama Ouan hotel Takayama',
    source: 'pdf',
    reviewFlag: 'Exact check-in / check-out times not given in the PDF.',
  },
  {
    id: 'soki-kanazawa',
    name: 'SOKI Kanazawa',
    city: 'kanazawa',
    cityLabel: 'Kanazawa',
    checkIn: '2026-09-17',
    checkOut: '2026-09-18',
    nights: 1,
    status: 'confirmed',
    goodFor: ['Higashi Chaya District', 'Asanogawa river walk', 'Quiet design hotel'],
    locationNote:
      'On the Asano river side of the city, walkable to the Higashi Chaya teahouse district — which is exactly where the PDF sends you on arrival evening.',
    nearestStation: 'Kanazawa Station (bus or short taxi)',
    mapsQuery: 'SOKI KANAZAWA hotel',
    source: 'pdf',
    reviewFlag: 'Exact check-in / check-out times not given in the PDF.',
  },
  {
    id: 'resol-trinity-kyoto',
    name: 'Hotel Resol Trinity Kyoto',
    city: 'kyoto',
    cityLabel: 'Kyoto',
    checkIn: '2026-09-18',
    checkOut: '2026-09-23',
    nights: 5,
    status: 'confirmed',
    goodFor: ['Karasuma / Sanjo', 'Nishiki Market', 'Pontocho', 'Subway + Hankyu'],
    locationNote:
      'Central Kyoto, walking distance to Nishiki Market and the Kiyamachi / Pontocho evening streets. This is also the hotel your forwarded luggage is sent to on 14 Sep.',
    nearestStation: 'Karasuma-Oike / Kyoto Shiyakusho-mae',
    checkInTime: '15:00 – 00:00',
    checkOutTime: 'Until 11:00',
    mapsQuery: 'Hotel Resol Trinity Kyoto',
    source: 'pdf',
    reviewFlag:
      'Confirm the hotel will accept and hold the forwarded suitcases from 15 Sep, before you send them on 14 Sep.',
  },
  {
    id: 'forza-osaka-namba',
    name: 'Hotel Forza Osaka Namba Dotonbori',
    city: 'osaka',
    cityLabel: 'Osaka',
    checkIn: '2026-09-23',
    checkOut: '2026-09-25',
    nights: 2,
    status: 'confirmed',
    goodFor: ['Dotonbori', 'Shinsaibashi', 'Namba nightlife', 'Shopping'],
    locationNote:
      'Right in the Dotonbori / Namba block. Everything on the last two days — arcades, Glico sign, department stores — is on foot, and Nankai Namba for the airport train is close.',
    nearestStation: 'Namba (Nankai / Midosuji / Kintetsu)',
    mapsQuery: 'Hotel Forza Osaka Namba Dotonbori',
    source: 'pdf',
    reviewFlag: 'Exact check-in / check-out times not given in the PDF.',
  },
]

export const hotelById = (id: string | null) =>
  id ? hotels.find((h) => h.id === id) ?? null : null
