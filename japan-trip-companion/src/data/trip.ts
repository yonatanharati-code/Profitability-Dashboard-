import type { Notice } from './types'

export const trip = {
  title: 'Japan',
  subtitle: 'September 2026',
  startDate: '2026-09-09',
  endDate: '2026-09-25',
  /**
   * The PDF's trip is 9-25 Sep and that still defines the 17 days.
   * The flights bracket it: EK 2168 leaves Tel Aviv the night before, and
   * EK 2120 lands home the morning after. Countdowns use this date.
   */
  departureDate: '2026-09-08',
  departureNote: 'EK 2168 from Tel Aviv T3, 22:50',
  travellers: '2 adults',
  route: [
    'Tokyo',
    "Kamikochi",
    'Hirayu Onsen',
    'Takayama',
    'Shirakawa-go',
    'Kanazawa',
    'Kyoto',
    'Osaka',
    'Kansai Airport',
  ],
  timezone: 'Asia/Tokyo',
  sourceDoc: 'Japan_Trip_Itinerary_Sep_2026.pdf',
}

/** The two hard food rules, surfaced across the app. */
export const dietRules = {
  headline: 'No pork. No seafood.',
  fine: 'Beef, chicken, vegetarian, rice, noodles — all fine.',
  watchList: [
    {
      title: 'Bonito dashi is everywhere',
      body:
        'Soba and udon broth, okonomiyaki batter, tofu ponzu and miso soup are usually made with katsuobushi — dried bonito. Nothing fishy is visible on the plate. Where this is the only issue we have marked the restaurant "ask" rather than dropped it, so you can decide.',
    },
    {
      title: 'Ramen is mostly pork',
      body:
        'Tonkotsu and most shoyu broths are pork-based. The reliable answers are the dedicated vegan shops: T\'s TanTan in Tokyo and Vegan Ramen UZU in Kyoto.',
    },
    {
      title: 'Two markets to treat as sightseeing',
      body:
        'Tsukiji Outer Market and Omicho Market are both overwhelmingly seafood. Walk them, buy fruit, and eat somewhere else.',
    },
    {
      title: 'Say it at the ryokan',
      body:
        'Ryokan dinners are set courses cooked to a plan. The PDF already notes the request for Hirayu — re-confirm it at check-in, not at the table.',
    },
    {
      title: 'The useful phrase',
      body:
        'Butaniku to gyokai-rui wa taberaremasen — "we cannot eat pork or seafood". Showing it written down works better than saying it.',
    },
  ],
}

/** Straight from the PDF's luggage strategy section. */
export const luggagePlan = [
  {
    id: 'lug-forward',
    date: '2026-09-14',
    title: 'Send the big cases ahead',
    body:
      'On 14 Sep, forward the large suitcases from Bespoke Hotel Shinjuku directly to the Kyoto hotel using TA-Q-BIN / Yamato. The front desk handles this.',
    source: 'pdf' as const,
  },
  {
    id: 'lug-alpine',
    date: '2026-09-15',
    title: 'Small bag only, 15-18 Sep',
    body:
      'The Alps and Kanazawa section runs on a small trolley or bag only. Pack for four days: Kamikochi, Hirayu, Takayama, Shirakawa-go, Kanazawa.',
    source: 'pdf' as const,
  },
  {
    id: 'lug-kamikochi',
    date: '2026-09-15',
    title: 'Leave the trolley at Kamikochi Bus Terminal',
    body:
      'Baggage room on the first floor of the terminal — not coin lockers, a staffed room. Roughly ¥350-500 per item per day, open 06:00 to 17:00. Do not walk to Myojin with the trolley.',
    source: 'pdf' as const,
  },
  {
    id: 'lug-shirakawago',
    date: '2026-09-17',
    title: 'Lockers at Shirakawa-go Bus Terminal',
    body:
      'Coin lockers at the terminal, around ¥500 small and medium, ¥1,000 large. There is also a staffed storage service alongside, open until about 17:00. Collect by 13:20 for the 13:50 bus.',
    source: 'pdf' as const,
  },
  {
    id: 'lug-reunite',
    date: '2026-09-18',
    title: 'Reunited in Kyoto',
    body:
      'The big cases should be waiting at Hotel Resol Trinity when you arrive at 19:38 on 18 Sep. Confirm with the hotel before you send them.',
    source: 'pdf' as const,
  },
]

/** The PDF's pre-flight checklist, plus the items that follow from it. */
export const preTripChecklist = [
  {
    id: 'chk-confirmations',
    label:
      'Save offline copies of the Azusa, National Park Liner, Shirakawa-go to Kanazawa and Kanazawa to Kyoto confirmations',
    source: 'pdf' as const,
  },
  { id: 'chk-suica', label: 'Add Suica to Apple Wallet and load an initial balance', source: 'pdf' as const },
  {
    id: 'chk-luggage',
    label: 'On 14 Sep, actually hand the suitcases over for forwarding to Kyoto',
    source: 'pdf' as const,
  },
  {
    id: 'chk-buses',
    label:
      'Check the unreserved buses the evening before: Kamikochi to Hirayu, Hirayu to Takayama, Takayama to Shirakawa-go',
    source: 'pdf' as const,
  },
  {
    id: 'chk-vjw',
    label: 'Fill in Visit Japan Web before flying — it turns immigration into a QR scan',
    source: 'research' as const,
  },
  {
    id: 'chk-online-checkin',
    label: 'Check in online for EK 312 and EK 317',
    source: 'research' as const,
  },
  {
    id: 'chk-haneda-taxi',
    label: 'Have the hotel address in Japanese ready for the Haneda taxi at 23:20 on 9 Sep',
    source: 'research' as const,
  },
  { id: 'chk-teamlab', label: 'Book teamLab Borderless for 12 Sep', source: 'research' as const },
  { id: 'chk-shibuya-sky', label: 'Decide on Shibuya Sky for 11 Sep and book if yes', source: 'research' as const },
  { id: 'chk-hida', label: 'Reserve the Hida beef dinner in Takayama for 16 Sep', source: 'research' as const },
  {
    id: 'chk-kyoto-dinner',
    label: 'Reserve one or two Kyoto dinners — Silver Week runs 19-23 Sep',
    source: 'research' as const,
  },
  { id: 'chk-cash', label: 'Carry cash for the mountain section — buses, lockers and Myojin Pond', source: 'research' as const },
  { id: 'chk-shoes', label: 'Pack proper walking shoes and a light rain shell for Kamikochi', source: 'research' as const },
  { id: 'chk-ryokan', label: 'Re-confirm no pork / no seafood with Miyama Ouan', source: 'pdf' as const },
]

/** Notices that apply to the whole trip rather than one day. */
export const tripNotices: Notice[] = [
  {
    id: 'nt-tokyo-night-gap',
    tone: 'warning',
    title: 'The night of 14-15 September may not be booked',
    body:
      'The Agoda confirmation for Bespoke Hotel Shinjuku runs 9 to 14 September — five nights. The itinerary says six, 9 to 15, and you leave that hotel at 06:15 on the 15th for the Azusa. Either the booking needs extending by a night or there is a second reservation nobody has sent me. Free cancellation on it ends 7 September, so this is worth settling now.',
    source: 'research',
  },
  {
    id: 'nt-takayama-hotel',
    tone: 'warning',
    title: 'Two different hotels for the Takayama night',
    body:
      'The itinerary names Takayama Ouan for 16-17 September. The Agoda confirmation is for Tokyu Stay Hida Takayama Musubinoyu on exactly those dates. Both are near the station. Tell me which is right and I will correct the itinerary throughout.',
    source: 'research',
  },
  {
    id: 'nt-silver-week',
    tone: 'crowd',
    title: 'Silver Week — 19 to 23 September',
    body:
      '2026 has a rare five-day national holiday: Respect for the Aged Day on Mon 21, a bridging citizens\' holiday on Tue 22, and Autumnal Equinox Day on Wed 23. It falls exactly across your Kyoto days, which is why the PDF warns about crowds. Domestic travel peaks — start early, and book the dinners you care about.',
    source: 'research',
  },
  {
    id: 'nt-no-jr-pass',
    tone: 'info',
    title: 'No JR Pass on this trip',
    body:
      'The PDF is explicit: Suica on iPhone for city travel and local trains, and no nationwide JR Pass. The long-distance legs are already booked individually.',
    source: 'pdf',
  },
  {
    id: 'nt-flexible',
    tone: 'info',
    title: 'The plan is a guide, not a schedule',
    body:
      'From the PDF: booked transfers stay fixed, but within each city you can move the order of sights around weather, crowds and energy. Reorder or skip anything in this app — the transport legs stay locked.',
    source: 'pdf',
  },
]
