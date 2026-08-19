/**
 * End-to-end behaviour checks.
 *
 * The interesting states of this app are all time-dependent — "today", the
 * countdown to the next train, Silver Week warnings — so each block freezes
 * the clock to a specific moment in the trip before any app code runs, then
 * asserts what the screen says.
 *
 * Playwright is deliberately NOT a dependency of the app. To run these:
 *
 *   npm run dev                        # in one terminal
 *   npm install --no-save playwright   # in another
 *   npm run test:e2e
 *
 * Set E2E_URL if the dev server is not on port 5173.
 *
 * Note: .pill is uppercased in CSS, so every text assertion here compares
 * lower-cased strings.
 */
import { chromium, devices } from 'playwright'

const url = process.env.E2E_URL ?? 'http://127.0.0.1:5173/'
// PLAYWRIGHT_BROWSERS_PATH is respected automatically; executablePath is only
// needed where a preinstalled Chromium sits outside Playwright's own cache.
const browser = await chromium.launch(
  process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {},
)
const iphone = { ...devices['iPhone 13'] }
const results = []
const fails = []

/** Expand a card only if it is currently collapsed. */
async function ensureExpanded(page, idx = 0) {
  const btn = page.locator('article button[aria-expanded]').nth(idx)
  if ((await btn.getAttribute('aria-expanded')) !== 'true') {
    await btn.click()
    await page.waitForTimeout(300)
  }
}

function check(name, ok, detail = '') {
  results.push(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ' — ' + detail : ''}`)
  if (!ok) fails.push(name + (detail ? ': ' + detail : ''))
}

/** Freeze the clock before any app code runs. */
function clockScript(iso) {
  return `(() => {
    const FIXED = new Date('${iso}').getTime();
    const RealDate = Date;
    class FakeDate extends RealDate {
      constructor(...a) { if (a.length === 0) super(FIXED); else super(...a); }
      static now() { return FIXED; }
    }
    window.Date = FakeDate;
  })()`
}

async function open(name, iso, ctxOpts = iphone) {
  const ctx = await browser.newContext(ctxOpts)
  await ctx.addInitScript(clockScript(iso))
  const page = await ctx.newPage()
  const errs = []
  page.on('pageerror', (e) => errs.push(e.message))
  await page.goto(url, { waitUntil: 'domcontentloaded' })
  await page.waitForTimeout(700)
  return { ctx, page, errs, name }
}

// ───────────────────────────────────────── 1. Mid-trip day, morning of 18 Sep
{
  // 2026-09-18 08:45 JST  ==  2026-09-17 23:45 UTC
  const { ctx, page, errs } = await open('18sep', '2026-09-17T23:45:00Z')
  // Pills are uppercased by CSS, so every text assertion here is case-insensitive.
  const body = (await page.locator('body').innerText()).toLowerCase()

  check('18 Sep: Today resolves to the real day', body.includes('september 18'))
  check('18 Sep: shows day number', /day 10 of 17/.test(body), body.match(/day \d+ of 17/)?.[0] ?? 'not found')
  check('18 Sep: route line', body.includes('kanazawa → kyoto'))
  check('18 Sep: no pre-trip countdown', !body.includes('until you fly'))
  check('18 Sep: Japan clock is 08:45', body.includes('08:45'), body.match(/\d\d:\d\d:\d\d/)?.[0] ?? '')
  check("18 Sep: What's next card present", body.includes("what's next"))
  check('18 Sep: next item is Omicho (08:30, in progress)', /omicho market/.test(body))
  check('18 Sep: leave-by line rendered', /leave by \d\d:\d\d/.test(body) || /should be underway/.test(body),
    body.match(/leave by [^\n]*/)?.[0] ?? body.match(/should be underway/)?.[0] ?? 'none')
  check('18 Sep: train countdown shown', /in \d+ h( \d+ min)?/.test(body), body.match(/in \d+ h[^\n]*/)?.[0] ?? 'none')
  check('18 Sep: Silver Week not yet flagged today', !body.includes('silver week starts today'))
  check('18 Sep: no page errors', errs.length === 0, errs.join('; '))
  await ctx.close()
}

// ───────────────────────────────────────── 2. Alps day, 06:00 before departure
{
  // 2026-09-15 06:00 JST == 2026-09-14 21:00 UTC
  const { ctx, page, errs } = await open('15sep', '2026-09-14T21:00:00Z')
  const body = (await page.locator('body').innerText()).toLowerCase()

  check('15 Sep: correct day', body.includes('september 15'))
  check('15 Sep: next is the 06:15 hotel departure', /leave the hotel for shinjuku station/.test(body))
  check('15 Sep: countdown to Azusa within the hour', /in \d+ min/.test(body), body.match(/in \d+ min/g)?.join(',') ?? 'none')
  check('15 Sep: Azusa leg shows be-at-station time', body.includes('be at the station by 06:35'),
    body.match(/be at the station by \d\d:\d\d/)?.[0] ?? 'none')
  check('15 Sep: luggage notice present', body.includes('leave the trolley at kamikochi bus terminal'))
  check('15 Sep: weather city is Kamikochi', /kamikochi/.test(body))
  check('15 Sep: no page errors', errs.length === 0, errs.join('; '))
  await ctx.close()
}

// ───────────────────────────────────────── 3. Silver Week day
{
  // 2026-09-19 09:00 JST
  const { ctx, page, errs } = await open('19sep', '2026-09-19T00:00:00Z')
  const body = (await page.locator('body').innerText()).toLowerCase()
  check('19 Sep: Silver Week flagged on the day', body.includes('silver week starts today'))
  check('19 Sep: Kiyomizu early-start warning', body.includes('kiyomizu-dera opens at 06:00'))
  check('19 Sep: no page errors', errs.length === 0, errs.join('; '))
  await ctx.close()
}

// ───────────────────────────────────────── 4. Interactions + persistence
{
  const { ctx, page, errs } = await open('state', '2026-09-17T23:45:00Z')

  // complete the first activity
  const doneBtn = page.locator('article button[aria-pressed]').first()
  await doneBtn.click()
  await page.waitForTimeout(250)
  let body = (await page.locator('body').innerText()).toLowerCase()
  check('state: progress advances after marking done', /1 of 10 done/.test(body),
    body.match(/\d+ of \d+ done/)?.[0] ?? 'none')

  // expand an activity and write a note
  await ensureExpanded(page, 1)
  await page.getByRole('button', { name: /Add note/ }).first().click()
  await page.waitForTimeout(200)
  const ta = page.locator('textarea').first()
  await ta.fill('Buy the knife here')
  await page.waitForTimeout(300)

  // favourite a restaurant
  await page.mouse.wheel(0, 6000)
  await page.waitForTimeout(400)
  const heart = page.locator('article button[aria-label*="favourites"], article button[aria-label*="Save"]').first()
  const heartCount = await heart.count()
  if (heartCount) { await heart.click(); await page.waitForTimeout(250) }
  check('state: a restaurant heart is clickable', heartCount > 0)

  // reload and confirm everything survived
  await page.reload({ waitUntil: 'domcontentloaded' })
  await page.waitForTimeout(800)
  body = (await page.locator('body').innerText()).toLowerCase()
  check('persist: completion survives reload', /1 of 10 done/.test(body),
    body.match(/\d+ of \d+ done/)?.[0] ?? 'none')
  check('persist: note badge survives reload', body.includes('note'))

  // the note itself, via the More > Your notes panel
  await page.getByRole('button', { name: 'More', exact: true }).click()
  await page.waitForTimeout(400)
  await page.getByRole('button', { name: /Your notes/ }).click()
  await page.waitForTimeout(400)
  body = (await page.locator('body').innerText()).toLowerCase()
  check('persist: note text readable in More', body.includes('buy the knife here'))

  // saved places
  await page.getByRole('button', { name: '← More' }).click()
  await page.waitForTimeout(300)
  await page.getByRole('button', { name: /Saved places/ }).click()
  await page.waitForTimeout(400)
  body = (await page.locator('body').innerText()).toLowerCase()
  check('persist: favourite appears in Saved places', !body.includes('nothing saved yet'), '')

  check('state: no page errors', errs.length === 0, errs.join('; '))
  await ctx.close()
}

// ───────────────────────────────────────── 5. Reorder + skip + reset
{
  const { ctx, page, errs } = await open('reorder', '2026-09-17T23:45:00Z')

  const firstTitle = async () =>
    (await page.locator('article h3').first().innerText()).trim()
  const before = await firstTitle()

  await ensureExpanded(page, 0)
  await page.getByRole('button', { name: 'Move later' }).first().click()
  await page.waitForTimeout(350)
  const after = await firstTitle()
  check('reorder: first item changes after Move later', before !== after, `${before} -> ${after}`)

  let body = (await page.locator('body').innerText()).toLowerCase()
  check('reorder: Reset order control appears', body.includes('reset order'))

  await page.getByRole('button', { name: /Reset order/ }).click()
  await page.waitForTimeout(350)
  check('reorder: reset restores the original order', (await firstTitle()) === before)

  // skip
  await ensureExpanded(page, 0)
  await page.getByRole('button', { name: /Skip this/ }).first().click()
  await page.waitForTimeout(350)
  body = (await page.locator('body').innerText()).toLowerCase()
  check('skip: total drops to 9 live items', /0 of 9 done/.test(body),
    body.match(/\d+ of \d+ done/)?.[0] ?? 'none')
  check('skip: no page errors', errs.length === 0, errs.join('; '))
  await ctx.close()
}

// ───────────────────────────────────────── 6. Bookings interactions
{
  const { ctx, page, errs } = await open('bookings', '2026-09-17T23:45:00Z')
  await page.getByRole('button', { name: 'Bookings', exact: true }).click()
  await page.waitForTimeout(500)
  let body = (await page.locator('body').innerText()).toLowerCase()
  check('bookings: outstanding counter present', /\d+ still outstanding/.test(body),
    body.match(/\d+ still outstanding/)?.[0] ?? 'none')
  check('bookings: all four buckets render',
    ['confirmed', 'should book', 'optional', 'no booking required'].every((b) => body.includes(b)))
  check('bookings: teamLab listed as should-book', body.includes('teamlab borderless'))
  check('bookings: temples listed as no-booking', body.includes('kyoto and nara temples'))

  const mark = page.getByRole('button', { name: /Mark booked/ }).first()
  await mark.scrollIntoViewIfNeeded()
  await mark.click()
  await page.waitForTimeout(350)
  body = (await page.locator('body').innerText()).toLowerCase()
  check('bookings: counter decrements after marking booked', /5 still outstanding/.test(body),
    body.match(/\d+ still outstanding/)?.[0] ?? 'none')
  check('bookings: no page errors', errs.length === 0, errs.join('; '))
  await ctx.close()
}

// ───────────────────────────────────────── 7. After the trip
{
  const { ctx, page, errs } = await open('after', '2026-10-05T00:00:00Z')
  const body = (await page.locator('body').innerText()).toLowerCase()
  check('post-trip: falls back to the last day', body.includes('september 25'))
  check('post-trip: says the trip is over', body.includes('the trip is over'))
  check('post-trip: no page errors', errs.length === 0, errs.join('; '))
  await ctx.close()
}

// ───────────────────────────────────────── 8. Every day renders
{
  const { ctx, page, errs } = await open('alldays', '2026-09-17T23:45:00Z')
  await page.getByRole('button', { name: 'Trip', exact: true }).click()
  await page.waitForTimeout(400)
  const count = await page.locator('ol li button').count()
  check('trip list: 17 day cards', count === 17, String(count))

  let broken = []
  for (let i = 0; i < count; i++) {
    await page.getByRole('button', { name: 'Trip', exact: true }).click()
    await page.waitForTimeout(120)
    await page.locator('ol li button').nth(i).click()
    await page.waitForTimeout(220)
    const t = await page.locator('body').innerText()
    if (!/THE DAY/i.test(t)) broken.push(i)
  }
  check('trip list: every day opens with a timeline', broken.length === 0, 'broken indexes: ' + broken.join(','))
  check('alldays: no page errors', errs.length === 0, errs.join('; '))
  await ctx.close()
}

// ───────────────────────────────────────── 9. Every More panel renders
{
  const { ctx, page, errs } = await open('more', '2026-09-17T23:45:00Z')
  await page.getByRole('button', { name: 'More', exact: true }).click()
  await page.waitForTimeout(400)
  // Each panel must show its own heading. Two of them are legitimately empty
  // until you save something, so a length threshold would be the wrong test.
  const panels = [
    ['Hotels', 'bespoke hotel shinjuku'],
    ['All transport', 'national park liner'],
    ['Luggage plan', 'send the big cases ahead'],
    ['Food rules', 'no pork. no seafood.'],
    ['Shopping', 'shinsaibashi'],
    ['Before you fly', 'add suica to apple wallet'],
    ['Saved places', 'nothing saved yet'],
    ['Your notes', 'no notes yet'],
    ['About this app', 'where the data comes from'],
  ]
  const broken = []
  for (const [name, needle] of panels) {
    await page.getByRole('button', { name: new RegExp(name) }).first().click()
    await page.waitForTimeout(320)
    const t = (await page.locator('body').innerText()).toLowerCase()
    if (!t.includes(needle)) broken.push(name)
    await page.getByRole('button', { name: '← More' }).click()
    await page.waitForTimeout(200)
  }
  check('more: all nine panels render their own content', broken.length === 0, broken.join(','))
  check('more: no page errors', errs.length === 0, errs.join('; '))
  await ctx.close()
}

console.log(results.join('\n'))
console.log(
  '\n' + (fails.length ? `${fails.length} FAILING:\n- ` + fails.join('\n- ') : 'All checks passed.'),
)
await browser.close()
process.exit(fails.length ? 1 : 0)
