import type { BookingItem } from './types'

/**
 * Deliberately conservative. Nothing is listed as "should book" unless a real
 * timed-entry or all-reserved rule was verified in August 2026. Everything else
 * is either optional or explicitly listed as needing nothing.
 */
export const bookings: BookingItem[] = [
  // ------------------------------------------------------- CONFIRMED (from the PDF)
  {
    id: 'bk-flights',
    title: 'Emirates flights, both directions',
    detail:
      'Out: EK 2168 Tel Aviv 22:50 on 8 Sep, then EK 312 Dubai 07:40 to Haneda 22:20 on 9 Sep. Home: EK 317 Kansai 23:45 on 25 Sep, then EK 2120 Dubai 06:25 to Tel Aviv 08:55 on 26 Sep. Economy Flex, 30 kg checked each.',
    bucket: 'confirmed',
    date: '2026-09-09',
    why:
      'Ticketed and confirmed. Seats 33A outbound and 62K homebound are already assigned. Keep the booking reference in More → Confirmations so it is readable offline.',
    officialUrl: 'https://www.emirates.com/',
    urlLabel: 'Manage the booking',
    source: 'research',
  },
  {
    id: 'bk-hotels',
    title: 'All six hotels',
    detail:
      'Bespoke Hotel Shinjuku, Miyama Ouan, Takayama Ouan, SOKI Kanazawa, Hotel Resol Trinity Kyoto, Hotel Forza Osaka Namba Dotonbori.',
    bucket: 'confirmed',
    why: 'Marked closed / confirmed for every night in the PDF.',
    source: 'pdf',
  },
  {
    id: 'bk-azusa',
    title: 'Azusa 1 — Shinjuku to Matsumoto',
    detail: '15 Sep, 07:00 to 09:38. Reserved seats.',
    bucket: 'confirmed',
    date: '2026-09-15',
    why: 'Confirmed in the PDF. Save the confirmation offline before you fly.',
    source: 'pdf',
  },
  {
    id: 'bk-npl',
    title: 'National Park Liner — Matsumoto to Kamikochi',
    detail: '15 Sep, 10:15 to 11:55.',
    bucket: 'confirmed',
    date: '2026-09-15',
    why: 'Confirmed in the PDF. Private cars cannot enter Kamikochi, so this bus is the only way in.',
    source: 'pdf',
  },
  {
    id: 'bk-shirakawago-kanazawa',
    title: 'Highway Bus — Shirakawa-go to Kanazawa',
    detail: '17 Sep, 13:50 to 15:05, arriving Kanazawa Station West Exit Bus Stop No. 4.',
    bucket: 'confirmed',
    date: '2026-09-17',
    why: 'Confirmed in the PDF. This route sells out in high season, which is why it was booked.',
    source: 'pdf',
  },
  {
    id: 'bk-kanazawa-kyoto',
    title: 'Kanazawa to Kyoto — Shinkansen + Thunderbird',
    detail: '18 Sep, 17:55 to 19:38, changing at Tsuruga. Both legs reserved.',
    bucket: 'confirmed',
    date: '2026-09-18',
    why: 'Confirmed in the PDF.',
    source: 'pdf',
  },

  // ------------------------------------------------------- SHOULD BOOK
  {
    id: 'bk-teamlab',
    title: 'teamLab Borderless, Azabudai Hills',
    detail: 'For the afternoon of 12 Sep. Timed entry, roughly ¥3,600-5,600 per adult depending on the date.',
    bucket: 'should-book',
    date: '2026-09-12',
    why:
      'Genuinely required: tickets are sold online in advance on a timed-entry system and regularly sell out. Same-day tickets exist only if a slot has not sold. Book as soon as the window opens — around two months ahead.',
    officialUrl: 'https://www.teamlab.art/e/borderless-azabudai/',
    urlLabel: 'teamLab official',
    source: 'research',
  },
  {
    id: 'bk-shibuya-sky',
    title: 'Shibuya Sky',
    detail: 'For the evening of 11 Sep, if you want the observation deck. Timed slots; sunset slots go first.',
    bucket: 'should-book',
    date: '2026-09-11',
    why:
      'The PDF makes this optional ("an observation deck if you choose to book"). If you do want it, book online — same-day tickets are rarely available, and advance tickets are cheaper. Note some foreign cards are refused on the official site; an authorised reseller is the workaround.',
    officialUrl: 'https://www.shibuya-scramble-square.com/sky/en/',
    urlLabel: 'Shibuya Sky official',
    source: 'research',
  },
  {
    id: 'bk-hida-beef-dinner',
    title: 'Hida beef dinner in Takayama',
    detail: 'For the evening of 16 Sep — Kitchen Hida or Maruaki.',
    bucket: 'should-book',
    date: '2026-09-16',
    why:
      'Takayama is a small town with one night in it, and the good beef rooms are small. A same-day walk-in on a Wednesday in September is a real risk. Ask the hotel to phone ahead if booking online is awkward.',
    source: 'research',
  },
  {
    id: 'bk-kyoto-dinners',
    title: 'One or two Kyoto dinners during Silver Week',
    detail: 'For 19-22 Sep — Hafuu, Yakiniku Hiro, or Shoraian for an Arashiyama lunch.',
    bucket: 'should-book',
    date: '2026-09-19',
    why:
      '19-23 September 2026 is Silver Week, a rare five-day national holiday. Kyoto restaurants will be booked by domestic travellers, not just tourists. Reserve the two dinners you actually care about and leave the rest open.',
    source: 'research',
  },
  {
    id: 'bk-namba-kix',
    title: 'Nankai Rapi:t seat to Kansai Airport',
    detail: 'For 25 Sep — the 19:15 departure from Nankai Namba, arriving Terminal 1 about 19:55.',
    bucket: 'optional',
    date: '2026-09-25',
    why:
      'Now that the flight is known this is a comfort call, not a constraint. Rapi:t is all-reserved so it needs a seat reservation, and with two 30 kg cases that is worth having. The Airport Express needs nothing at all and runs constantly — either gets you there long before the 20:00 check-in.',
    officialUrl: 'https://www.nankai.co.jp/en_railway/ticket/rapit',
    urlLabel: 'Nankai Rapi:t official',
    source: 'research',
  },
  {
    id: 'bk-online-checkin',
    title: 'Online check-in for both Emirates flights',
    detail: 'EK 312 on 9 Sep and EK 317 on 25 Sep.',
    bucket: 'should-book',
    date: '2026-09-08',
    why:
      'Seats are already assigned, so this is about skipping a desk queue — which matters most on the 25th, when you arrive at Terminal 1 straight from a day out. Check the window on the Emirates site and do it from the hotel wifi.',
    officialUrl: 'https://www.emirates.com/',
    urlLabel: 'Emirates check-in',
    source: 'research',
  },
  {
    id: 'bk-luggage-forwarding',
    title: 'Yamato TA-Q-BIN luggage forwarding, Tokyo to Kyoto',
    detail: 'Arrange at the Bespoke Hotel Shinjuku front desk on 14 Sep.',
    bucket: 'should-book',
    date: '2026-09-14',
    why:
      'Not a reservation as such, but it is a hard deadline in the PDF and the whole Alpine section depends on it. Hand the cases over on 14 Sep and confirm the Kyoto hotel will receive and hold them.',
    officialUrl: 'https://www.kuronekoyamato.co.jp/ytc/en/send/services/takkyubin/',
    urlLabel: 'Yamato TA-Q-BIN',
    source: 'pdf',
  },

  // ------------------------------------------------------- OPTIONAL
  {
    id: 'bk-umeda-sky',
    title: 'Umeda Sky Building observatory',
    detail: 'For the evening of 24 Sep. Around ¥2,000 per adult; open until 22:30, last entry 22:00.',
    bucket: 'optional',
    date: '2026-09-24',
    why:
      'Tickets are sold on site at the 39th-floor desk, so nothing is required. Booking online just skips the queue at sunset, which is the busiest hour. The open-air rooftop can close in high wind.',
    officialUrl: 'https://www.skybldg.co.jp/en/',
    urlLabel: 'Umeda Sky Building official',
    source: 'research',
  },
  {
    id: 'bk-myojin-pond',
    title: 'Myojin Pond entry, Kamikochi',
    detail: '¥500 per adult, paid at the gate at Hotaka Shrine.',
    bucket: 'none',
    date: '2026-09-15',
    why: 'Cash at the gate. Nothing to book — just carry coins.',
    officialUrl: 'https://www.kamikochi.org/',
    urlLabel: 'Kamikochi official',
    source: 'research',
  },
  {
    id: 'bk-tea-house',
    title: 'A Higashi Chaya teahouse visit, Kanazawa',
    detail: 'For the evening of 17 Sep.',
    bucket: 'optional',
    date: '2026-09-17',
    why:
      'Most of the district can be walked freely. Some of the old geisha houses charge a small entry for their tatami rooms and close in the late afternoon — arriving by around 16:30 matters more than booking.',
    source: 'unverified',
  },
  {
    id: 'bk-splurge-dinner',
    title: 'One high-end wagyu dinner',
    detail: 'Yoroniku in Tokyo, or Matsusakagyu Yakiniku M in Osaka on the last night.',
    bucket: 'optional',
    date: '2026-09-24',
    why:
      'These need real reservations, sometimes weeks out. Entirely optional — but if you want one perfect beef dinner, this is the item that has to be booked early rather than decided on the day.',
    source: 'research',
  },

  // ------------------------------------------------------- NO BOOKING REQUIRED
  {
    id: 'bk-buses-no-res',
    title: 'The three unreserved buses',
    detail:
      'Kamikochi to Hirayu (15 Sep), Hirayu to Takayama (16 Sep), Takayama to Shirakawa-go (17 Sep).',
    bucket: 'none',
    why:
      'The PDF marks all three as needing no reservation, and Nohi Bus confirms its Hirayu route bus takes none. Buy the ticket at the window or machine before boarding, and check the exact departure the evening before as the PDF advises.',
    officialUrl: 'https://www.nouhibus.co.jp/english/',
    urlLabel: 'Nohi Bus timetables',
    source: 'pdf',
  },
  {
    id: 'bk-kyoto-osaka',
    title: 'Kyoto to Osaka on 23 Sep',
    detail: 'JR, Hankyu or local rail, whenever you check out.',
    bucket: 'none',
    date: '2026-09-23',
    why: 'The PDF says explicitly: no need to book. Tap in with Suica.',
    source: 'pdf',
  },
  {
    id: 'bk-temples',
    title: 'Kyoto and Nara temples and shrines',
    detail:
      'Kiyomizu-dera, Fushimi Inari, Tenryu-ji, Kinkaku-ji, Ginkaku-ji, Nanzen-ji, Todai-ji, Kasuga Taisha, Yasaka Shrine.',
    bucket: 'none',
    why:
      'None of these take advance bookings — you pay at the gate. Fushimi Inari and the Yasaka Shrine grounds are free. The only real lever is arriving early, which matters a lot during Silver Week.',
    source: 'research',
  },
  {
    id: 'bk-kanazawa-sights',
    title: 'Kenroku-en and Kanazawa Castle',
    detail:
      'Kenroku-en: ¥320 adult, open 07:00-18:00 in September. Kanazawa Castle park grounds free, 07:00-18:00; the paid turrets ¥320, 09:00-16:30.',
    bucket: 'none',
    date: '2026-09-18',
    why: 'Tickets at the gate. Kenroku-en also has a free early-admission window before official opening.',
    officialUrl: 'https://www.pref.ishikawa.jp/siro-niwa/english/',
    urlLabel: 'Kenroku-en / Kanazawa Castle official',
    source: 'research',
  },
  {
    id: 'bk-suica',
    title: 'Suica on iPhone',
    detail: 'Add it to Apple Wallet before you fly and load an initial balance.',
    bucket: 'none',
    why:
      'The PDF is explicit: Suica for city travel and local trains, and no nationwide JR Pass. Nothing to reserve — just set it up at home where the wifi is good.',
    source: 'pdf',
  },
]

export const bucketOrder: BookingItem['bucket'][] = [
  'confirmed',
  'should-book',
  'optional',
  'none',
]

export const bucketLabels: Record<BookingItem['bucket'], string> = {
  confirmed: 'Confirmed',
  'should-book': 'Should book',
  optional: 'Optional',
  none: 'No booking required',
}
