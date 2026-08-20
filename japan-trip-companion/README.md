# Japan Trip Companion — 9–25 September 2026

A mobile-first travel companion for our Japan trip, built from
`Japan_Trip_Itinerary_Sep_2026.pdf`. Open it in the morning and it tells you
where you are, where you're sleeping, what the plan is, when the next train
leaves and what you can eat nearby — without opening the PDF.

React + TypeScript + Tailwind. No backend, no login, no API keys required.

```bash
npm install
npm run dev        # http://localhost:5173
npm run build      # validates the data model, typechecks, then builds
npm run validate   # data-integrity check on its own
```

### Tests

`npm run validate` runs on every build and checks the data model: that every id
referenced from `days.ts` resolves, that ids are unique, that all 17 dates are
present and in order, that no transport leg falls outside the trip, and that no
URL is a shortener.

`npm run test:e2e` drives the real app in a browser. Because every interesting
state here is time-dependent — "today", the countdown to the next train, the
Silver Week warnings — each block freezes the clock to a specific moment in the
trip and asserts what the screen actually says: that 18 September resolves to
day 10 with a 9 h 10 min countdown to the 17:55 train, that 15 September at
06:00 shows *in 15 min* against the Azusa departure, that marking, skipping,
reordering, notes and favourites all survive a reload. Playwright is
deliberately not a dependency of the app:

```bash
npm run dev                        # one terminal
npm install --no-save playwright   # another
npm run test:e2e                   # E2E_URL= if not on :5173
```

## Hosting it

The app is a static build — `npm run build` produces `dist/`, which any static
host will serve (Vercel, Netlify, GitHub Pages, Cloudflare Pages). Nothing
server-side is required.

`npm run build:single` additionally produces `dist/japan-2026.html`: the whole
app inlined into one self-contained file, for hosts that serve a single page.
One caveat — under a strict content-security policy the Open-Meteo request is
blocked, so the weather block shows its labelled September averages rather than
a live forecast. Everything else is identical.

## Screens

| Tab | What it does |
| --- | --- |
| **Today** | Resolves the current date in Japan time and shows that day: route, weather, hotel, what's next, notices, transport, the timeline, food and shopping. Before the trip it shows day 1 with a countdown. |
| **Trip** | All 17 days as cards with `PAST` / `TODAY` status and per-day progress. Tap for the full day. |
| **Map** | Every place for a chosen day, grouped by hotel / station / terminal / place / food, deep-linked into Google Maps, plus a one-tap multi-stop route. |
| **Bookings** | `CONFIRMED` / `SHOULD BOOK` / `OPTIONAL` / `NO BOOKING REQUIRED`, tickable, with official links. |
| **More** | Twelve panels: hotels, all transport, the luggage plan, packing, food rules, Japanese phrases, shopping, the pre-flight checklist, confirmations, saved places, your notes, and how the data is sourced. |

## Three things built for use on the ground

**Japanese phrases** (`src/data/phrases.ts`) — grouped by situation, food rules
first, with the Japanese set large because the technique is to hand someone the
phone rather than attempt the pronunciation. A short "read this on the plane"
list sits above the full set. Entirely offline.

**Packing** (`src/data/packing.ts`) — organised by *which bag*, because that is
the decision the PDF's luggage plan actually forces: the big cases are forwarded
to Kyoto on 14 Sep and not seen again until the 18th, so the small trolley has
to carry four days on its own. Each item says why it is in that bag.

**Confirmations** — editable fields for booking references and phone numbers,
stored on the device. Nothing here ships with invented phone numbers; you paste
in what your confirmation emails say, and it is then readable in a mountain
valley with no signal. That is the PDF's "keep the confirmations offline"
checklist item, made actionable.

## The data model

Everything renders from `/src/data`. To change the trip, edit data — never a
component.

```
src/data/
  types.ts        the shape of everything, with the provenance convention
  trip.ts         trip meta, diet rules, luggage plan, pre-flight checklist
  cities.ts       coordinates for weather, plus labelled September averages
  days.ts         all 17 days: route, theme, activities, notices, links
  hotels.ts       the six confirmed stays
  transport.ts    every leg, with reservation status and the Tsuruga transfer
  restaurants.ts  53 places, filtered against no-pork / no-seafood
  shopping.ts     shopping areas by city
  bookings.ts     what actually needs booking, and what doesn't
  phrases.ts      the offline phrase card, grouped by situation
  packing.ts      packing list, split by which bag it goes in
```

`npm run validate` checks that every id referenced from `days.ts` resolves,
that ids are unique, that all 17 dates are present and in order, that no
transport leg falls outside the trip, and that no URL is a shortener.

### Provenance

Every fact carries a `source`:

- **`pdf`** — verbatim from the itinerary PDF. Dates, hotels, transport times,
  the route, the daily plans and the luggage strategy. Not changed anywhere.
- **`research`** — verified against an official or reputable source in
  August 2026 (hours, prices, booking rules).
- **`unverified`** — our suggestion. Shown in the UI as *"Our suggestion"*.

Two conventions follow from this:

- A **tilde** before a time (`~10:00`) means we added it for pacing. A plain
  time (`08:50`) is fixed in the PDF.
- An amber **"Check this"** note flags a field the PDF left open — the arrival
  flight, the KIX departure time, a ryokan dinner sitting — rather than
  inventing a value. Unverified opening hours are omitted and the card links to
  the official site instead.

## Food

Two hard rules: **no pork, no seafood.** Every restaurant carries a verdict:

- **Works for us** — the whole concept fits; ordering is easy.
- **Order carefully** — worth eating, but you must order around something. Most
  often bonito dashi in a broth or batter, which we call out rather than
  silently dropping half of Japan's best food.
- **Not for a meal** — listed so you know *not* to plan a meal there. Tsukiji
  Outer Market and Omicho Market are both in this category.

## Weather

Open-Meteo, which is free and needs **no API key**. Forecasts reach about 16
days ahead; outside that window the app shows a clearly labelled September
average for the city so the practical advice still works. Advice is
place-specific — rain in Kamikochi suggests waterproof shoes over an umbrella;
30°C in Kyoto suggests front-loading the outdoor sights.

To swap providers, `src/utils/weather.ts` is the only file to change. Keep the
`DayWeather` shape and everything else keeps working.

## Maps

A real embedded map needs a billed API key, so instead every place deep-links
into Google Maps — which on a phone opens the native app, which is what you
want while walking. Nothing is ever a guessed place URL: it's either a verified
official site or a Maps search built at runtime.

Google Maps accepts ten stops in one directions link. Where a day has more, the
app says how many were left out rather than silently truncating.

## Offline

The whole itinerary is bundled into the app: days, transport, hotels, food,
shopping and your own notes all work with no connection. Only the weather
forecast, Maps links and official websites need the network.

It installs as a PWA — add to home screen and it opens like an app. The service
worker is cache-first for the app's own files and network-only for everything
else, so a forecast is never served stale.

## Your data

Completed items, skips, reordering, favourites, notes, checklist ticks and
booking status are stored in `localStorage` on the device. No account, no
server. *More → About → Reset everything* clears it.

Reordering never touches transport: booked legs render in their own section and
stay fixed, which is exactly what the PDF asks for — the transfers are locked,
the order of sights inside a city is not.

## The flights, and what they changed

The Emirates ticket arrived after the first build and moved two days materially.
Both are now in `transport.ts` with real times.

**9 Sep — EK 312 lands at 22:20, not "the evening".** The PDF was written before
the ticket existed and left room for a Shinjuku walk and dinner. Out of Haneda
around 23:00–23:20, you reach the hotel near midnight, so that evening is gone.
The day now says so, and recommends a taxi: the regular Limousine Bus to
Shinjuku has usually finished by then, the late-night one does not start until
midnight, and Keikyu's last departure is around 00:30 with a change at
Shinagawa — not what you want with two 30 kg cases.

**25 Sep — EK 317 departs 23:45, which buys back a whole day.** The PDF sized
this as "a quiet morning per the flight time". In fact you have Osaka until
about 19:00. The day is now a real itinerary: Osaka Castle — the one major
Osaka sight the PDF never reached — then last shopping in Namba, bags collected
at 17:30, and the 19:15 Nankai for the 20:00 check-in printed on the ticket.

The trip's 17 days are still 9–25 Sep as the PDF defines them; the flights
bracket that window (out on the 8th, home on the 26th) and the countdown now
targets the actual departure.

**Booking references are deliberately not in this repo.** A reference plus a
surname is enough to alter a booking on most airline sites, and this app gets
published. Flight numbers, times, terminals and seats are here because they are
useful and harmless; the reference goes in *More → Confirmations*, which stores
it on your device only.

## Known gaps, by design

These are flagged in the app rather than filled in:

- **Hotel check-in / check-out times** — not in the PDF.
- **Some opening hours and fees** — Takayama Jinya, Tenryu-ji, Todai-ji,
  Kasuga Taisha, Nanzen-ji, Ginkaku-ji, Tsutenkaku. Each card links to the
  official site instead of asserting a number.

## One thing worth knowing

**19–23 September 2026 is Silver Week** — a rare five-day national holiday
(Respect for the Aged Day on Mon 21, a bridging citizens' holiday on Tue 22,
Autumnal Equinox Day on Wed 23). It falls exactly across the Kyoto days, which
is why the PDF warns about crowds there. It's the third Silver Week ever, after
2009 and 2015. The app surfaces it on each affected day.
