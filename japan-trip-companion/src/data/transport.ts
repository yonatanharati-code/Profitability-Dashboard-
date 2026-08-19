import type { TransportLeg } from './types'

/**
 * Every leg below comes from the "main transfers" table in the PDF.
 * Times and reservation status are verbatim. Do not edit without being asked.
 * "arriveAtStationMinutes" and official URLs are added by us and marked as such
 * in the note text where it matters.
 */
export const transport: TransportLeg[] = [
  {
    id: 'arrival-nrt-hnd-shinjuku',
    date: '2026-09-09',
    service: 'Airport transfer to hotel',
    mode: 'local-rail',
    from: 'Arrival airport',
    to: 'Shinjuku',
    departNote: 'Evening arrival',
    status: 'book-later',
    durationNote: 'Depends on the airport — roughly 60-100 min to Shinjuku.',
    note:
      'The PDF says only "evening arrival in Tokyo and transfer to the hotel". Fill in the real flight and airport once ticketed.',
    source: 'pdf',
    reviewFlag: 'Arrival airport, flight number and landing time are not in the PDF.',
  },
  {
    id: 'azusa-1',
    date: '2026-09-15',
    service: 'Limited Express Azusa 1',
    mode: 'train',
    from: 'Shinjuku',
    to: 'Matsumoto',
    depart: '07:00',
    arrive: '09:38',
    status: 'booked',
    arriveAtStationMinutes: 25,
    durationNote: '2 h 38 min',
    officialUrl: 'https://www.jreast.co.jp/multi/en/',
    note:
      'Reserved seats. Leave the hotel around 06:15 — Shinjuku Station is big and the Azusa platforms are not the first ones you reach.',
    source: 'pdf',
  },
  {
    id: 'national-park-liner',
    date: '2026-09-15',
    service: 'National Park Liner',
    mode: 'bus',
    from: 'Matsumoto',
    to: 'Kamikochi',
    depart: '10:15',
    arrive: '11:55',
    status: 'booked',
    arriveAtStationMinutes: 15,
    durationNote: '1 h 40 min',
    officialUrl: 'https://www.alpico.co.jp/en/',
    note:
      '37 minutes to connect at Matsumoto. Enough, but not enough to wander off — the bus departs from the terminal by the station.',
    source: 'pdf',
  },
  {
    id: 'kamikochi-hirayu',
    date: '2026-09-15',
    service: 'Local shuttle bus',
    mode: 'shuttle',
    from: 'Kamikochi Bus Terminal',
    to: 'Hirayu Onsen',
    departNote: 'Aiming for 16:30 or 17:00',
    status: 'no-reservation',
    durationNote: 'Roughly 25-30 min',
    officialUrl: 'https://www.nouhibus.co.jp/english/',
    note:
      'No reservation. Be back at the terminal by about 16:10 to collect the trolley from the baggage room (it closes at 17:00) and buy the ticket.',
    source: 'pdf',
  },
  {
    id: 'hirayu-takayama',
    date: '2026-09-16',
    service: 'Nohi Bus — Hirayu / Shin-Hotaka line',
    mode: 'bus',
    from: 'Hirayu Onsen',
    to: 'Takayama Nohi Bus Center',
    departNote: 'Around 09:30',
    arriveNote: 'Around 10:30',
    status: 'no-reservation',
    durationNote: 'About 1 h',
    officialUrl: 'https://www.nouhibus.co.jp/english/routebus/',
    note:
      'No reservation needed on this route bus, but buy the ticket at the window or machine before boarding. Roughly hourly — check tomorrow morning\'s exact departure the evening before.',
    source: 'pdf',
  },
  {
    id: 'takayama-shirakawago',
    date: '2026-09-17',
    service: 'Nohi Bus — Shirakawa-go line',
    mode: 'bus',
    from: 'Takayama Nohi Bus Center',
    to: 'Shirakawa-go',
    depart: '08:50',
    arrive: '09:40',
    status: 'no-reservation',
    arriveAtStationMinutes: 30,
    durationNote: '50 min',
    officialUrl: 'https://www.nouhibus.co.jp/english/',
    note:
      'The PDF has you at the Nohi Bus Center by about 08:20. This bus is popular with day-trippers — being early is the whole trick.',
    source: 'pdf',
  },
  {
    id: 'shirakawago-kanazawa',
    date: '2026-09-17',
    service: 'Highway Bus',
    mode: 'bus',
    from: 'Shirakawa-go',
    to: 'Kanazawa Station — West Exit Bus Stop No. 4',
    depart: '13:50',
    arrive: '15:05',
    status: 'booked',
    arriveAtStationMinutes: 20,
    durationNote: '1 h 15 min',
    officialUrl: 'https://www.nouhibus.co.jp/english/',
    note:
      'Reserved. Collect your bag from the Shirakawa-go locker around 13:20 so you are not doing it at 13:45.',
    platformNote: 'Arrives at Kanazawa Station West Exit, Bus Stop No. 4.',
    source: 'pdf',
  },
  {
    id: 'kanazawa-kyoto',
    date: '2026-09-18',
    service: 'JR West — Hokuriku Shinkansen + Thunderbird',
    mode: 'train',
    from: 'Kanazawa',
    to: 'Kyoto',
    depart: '17:55',
    arrive: '19:38',
    status: 'booked',
    arriveAtStationMinutes: 30,
    durationNote: '1 h 43 min including the change',
    transfer: {
      at: 'Tsuruga',
      firstLeg: 'Hokuriku Shinkansen',
      secondLeg: 'Limited Express Thunderbird',
    },
    officialUrl: 'https://www.westjr.co.jp/global/en/',
    note:
      'Reserved, both legs. The Tsuruga change is cross-platform and designed to be quick, but it is a real change — have the second ticket ready.',
    source: 'pdf',
  },
  {
    id: 'kyoto-osaka',
    date: '2026-09-23',
    service: 'JR / Hankyu / local rail',
    mode: 'local-rail',
    from: 'Kyoto',
    to: 'Osaka — Namba',
    departNote: 'After hotel check-out',
    status: 'no-reservation',
    durationNote: 'About 45-70 min depending on the line',
    officialUrl: 'https://www.westjr.co.jp/global/en/',
    note:
      'No booking required — tap in with Suica. JR to Osaka Station then Midosuji subway to Namba, or Hankyu from Karasuma. Either is fine with suitcases.',
    source: 'pdf',
  },
  {
    id: 'namba-kix',
    date: '2026-09-25',
    service: 'Nankai Rapi:t or Airport Express',
    mode: 'train',
    from: 'Nankai Namba',
    to: 'Kansai Airport (KIX)',
    departNote: 'Set from the final flight time',
    status: 'book-later',
    arriveAtStationMinutes: 20,
    durationNote: 'Rapi:t about 35-40 min; Airport Express a little longer',
    officialUrl: 'https://www.nankai.co.jp/en_railway/traffic/express/rapit.html',
    note:
      'Rapi:t is all-reserved, so it needs a seat reservation — that is the one to book if you want guaranteed space with luggage. The Airport Express needs nothing and runs constantly. Decide once the flight time is fixed.',
    source: 'pdf',
    reviewFlag: 'Final flight time from KIX is not in the PDF — the whole last day hangs on it.',
  },
]

export const transportById = (id: string) => transport.find((t) => t.id === id)

export const transportForDate = (date: string) =>
  transport.filter((t) => t.date === date)
