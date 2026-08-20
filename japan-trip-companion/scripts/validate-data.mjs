/**
 * Data integrity check for the trip model.
 *
 * The UI renders entirely from /src/data, so a typo in an id shows up as a
 * silently missing card rather than an error. This catches that at build time.
 * Run with: npm run validate
 */
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

const root = join(dirname(fileURLToPath(import.meta.url)), '..')
const read = (p) => readFileSync(join(root, p), 'utf8')

/** Pull every `id: 'x'` from a data file, in order. */
function ids(src) {
  return [...src.matchAll(/^\s{2,4}id: '([^']+)'/gm)].map((m) => m[1])
}
/** Pull every quoted string inside a named array field. */
function refs(src, field) {
  const out = []
  for (const m of src.matchAll(new RegExp(`${field}:\\s*\\[([^\\]]*)\\]`, 'g'))) {
    for (const q of m[1].matchAll(/'([^']+)'/g)) out.push(q[1])
  }
  return out
}

const daysSrc = read('src/data/days.ts')
const transportSrc = read('src/data/transport.ts')
const hotelsSrc = read('src/data/hotels.ts')
const restaurantsSrc = read('src/data/restaurants.ts')
const shoppingSrc = read('src/data/shopping.ts')
const bookingsSrc = read('src/data/bookings.ts')

const transportIds = new Set(ids(transportSrc))
const hotelIds = new Set(ids(hotelsSrc))
const restaurantIds = new Set(ids(restaurantsSrc))
const shoppingIds = new Set(ids(shoppingSrc))

const errors = []
const warnings = []

// ---------------------------------------------------------------- references
for (const id of refs(daysSrc, 'transportIds')) {
  if (!transportIds.has(id)) errors.push(`days.ts references unknown transport id "${id}"`)
}
for (const id of refs(daysSrc, 'restaurantIds')) {
  if (!restaurantIds.has(id)) errors.push(`days.ts references unknown restaurant id "${id}"`)
}
for (const id of refs(daysSrc, 'shoppingIds')) {
  if (!shoppingIds.has(id)) errors.push(`days.ts references unknown shopping id "${id}"`)
}
for (const m of daysSrc.matchAll(/hotelId: '([^']+)'/g)) {
  if (!hotelIds.has(m[1])) errors.push(`days.ts references unknown hotel id "${m[1]}"`)
}
for (const m of daysSrc.matchAll(/transportId: '([^']+)'/g)) {
  if (!transportIds.has(m[1])) errors.push(`days.ts activity references unknown leg "${m[1]}"`)
}

// ------------------------------------------------------------ unique ids
for (const [label, src] of [
  ['days activities', daysSrc],
  ['transport', transportSrc],
  ['hotels', hotelsSrc],
  ['restaurants', restaurantsSrc],
  ['shopping', shoppingSrc],
  ['bookings', bookingsSrc],
]) {
  const seen = new Set()
  for (const id of ids(src)) {
    if (seen.has(id)) errors.push(`${label}: duplicate id "${id}"`)
    seen.add(id)
  }
}

// ------------------------------------------------------------ trip coverage
const dates = [...daysSrc.matchAll(/^\s{4}date: '(\d{4}-\d{2}-\d{2})',$/gm)].map((m) => m[1])
const expected = []
for (let d = 9; d <= 25; d++) expected.push(`2026-09-${String(d).padStart(2, '0')}`)
for (const d of expected) if (!dates.includes(d)) errors.push(`days.ts is missing ${d}`)
for (const d of dates) if (!expected.includes(d)) errors.push(`days.ts has unexpected date ${d}`)
if (dates.join() !== [...dates].sort().join()) errors.push('days.ts dates are not in order')

// ------------------------------------------------- transport dates are real
// The outbound flight leaves Tel Aviv the night before day 1, and the homebound
// connection lands the morning after day 17, so those two dates are legitimately
// outside the 17 trip days. Anything else outside them is a mistake.
const flightBookends = ['2026-09-08', '2026-09-26']
for (const m of transportSrc.matchAll(/date: '(\d{4}-\d{2}-\d{2})'/g)) {
  if (!expected.includes(m[1]) && !flightBookends.includes(m[1])) {
    errors.push(`transport.ts leg dated ${m[1]} is outside the trip`)
  }
}

// ------------------------------------------------------ no invented URLs
// Every http link must be an official-looking domain, never a shortener or a
// Google Maps place URL (we build searches at runtime instead).
for (const [label, src] of [
  ['days', daysSrc], ['transport', transportSrc], ['hotels', hotelsSrc],
  ['restaurants', restaurantsSrc], ['bookings', bookingsSrc], ['shopping', shoppingSrc],
]) {
  for (const m of src.matchAll(/'(https?:\/\/[^']+)'/g)) {
    const url = m[1]
    if (/goo\.gl|bit\.ly|tinyurl|maps\.app/.test(url)) {
      errors.push(`${label}: shortened or opaque URL "${url}"`)
    }
    if (url.startsWith('http://') && !url.includes('inari.jp')) {
      warnings.push(`${label}: plain http URL "${url}"`)
    }
  }
}

// ----------------------------------------------------------- diet sanity
const avoidBlocks = restaurantsSrc.split('{').filter((b) => b.includes("dietVerdict: 'avoid'"))
for (const b of avoidBlocks) {
  if (!/seafood|pork/i.test(b)) {
    warnings.push('a restaurant is marked "avoid" without explaining why in dietNote')
  }
}

// ------------------------------------------------------------------ report
console.log(`Checked ${dates.length} days, ${transportIds.size} transport legs, ` +
  `${hotelIds.size} hotels, ${restaurantIds.size} restaurants, ${shoppingIds.size} shopping areas.`)

if (warnings.length) {
  console.log('\nWarnings:')
  warnings.forEach((w) => console.log('  ! ' + w))
}

if (errors.length) {
  console.error('\nErrors:')
  errors.forEach((e) => console.error('  x ' + e))
  process.exit(1)
}

console.log('\nData model is consistent.')
