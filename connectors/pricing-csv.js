'use strict';

/**
 * Quicklizard "Pricing Events" monthly CSV parser
 *
 * Expected CSV header:
 *   Account,System,Channel,Name,Meta,Amount,Uniq amount,Summary date
 *
 * Column meanings used here:
 *   A (0) – Account       : customer name (raw)
 *   F (5) – Amount        : pricing events count (integer)
 *   H (7) – Summary date  : DD-MM-YYYY  (e.g. "31-10-2025")
 *
 * Multiple rows per account (one per channel per day) are SUMMED per account
 * per month.  The resulting month key is YYYY-MM derived from Summary date.
 */

// ─── Internal / test accounts to skip ────────────────────────────────────────
const PRICING_SKIP = new Set([
  // QL internal / infra
  'qltest',
  'qltest_bulk',
  'qltest_po',
  'quicklizard',
  'techops',
  'demo account',
  'interaction settings',
  'pricemanager',
  'gxadvisor',
  'philips_da',
  'delivery',
  // Demo / sandbox accounts
  'demo',
  'new demo',
  'cl demo',
  'demo - retail',
  'demo at&t',
  'at&t demo',
  'at&t demo ',   // trailing-space variant seen in CSV
  'samsung global demo',
  'smeet demo',
  'demo-restaurants',
  // Internal campaign / test
  'marketing',
  // Scraping / infra sub-accounts
  'whirlpool us- scrapping',
  // Irrelevant / partner accounts
  'at&t',
  'ust',
  'iga supermarket',
  'dealavo',
  'signa sports united',
]);

// ─── Account-name aliases (raw lowercase → canonical) ────────────────────────
// Keys must be lowercase-trimmed.  Values are the canonical display name.
const PRICING_ALIASES = new Map([
  // ── Previously added ──────────────────────────────────────────────────────
  ['arcelik',                    'arçelik türkiye'],
  ['phh group',                  'pigu'],
  ['logitech',                   'logitech us'],
  ['logitech us',                'logitech us'],
  ['unito versand',              'otto austria group'],
  ['phoenix pharma switzerland', 'health and life'],
  ['hipercohen',                 'hippercohen'],
  ['doc morris',                 'docmorris'],

  // ── US-server CSV mappings ─────────────────────────────────────────────────
  ['atea',                       'atea a/s'],
  ['whirlpool us',               'whirlpool mda us'],
  ['whirlpool us sda',           'whirlpool sda us (kitchenaid)'],
  ['kitchenaid anz',             'whirlpool sda australia (kitchenaid)'],
  ['kitchenaidlamex',            'whirlpool sda latam (kitchenaid)'],
  ['edg production',             'endeavor global, inc'],
  ['samsung uk',                 'samsung'],          // merge all Samsung → one row
  ['websundhedcom',              'sundhed.dk'],
  ['byggmax',                    'byggmax group'],
  ['dk hardware',                'dk hardware supply'],
  ['feelunique',                 'sephora (feelunique)'],
  ['ikano',                      'ikano group'],
  ['indimade',                   'indimade brands'],
  ['interiorshop',               'interiør a/s'],
  ['intersport',                 'intersport digital gmbh'],
  ['lakuda',                     'lakuda aps'],
  ['saxo',                       'saxo.com a / s'],
  ['bauhaus.ch',                 'bauhaus schweiz'],
  ['jacob',                      'jacob hotels - מלונות ג\'ייקוב'],
]);

// ─── CSV line parser (handles RFC-4180 quoted fields) ────────────────────────
function parseLine(line) {
  const fields = [];
  let cur = '', inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQ && line[i + 1] === '"') { cur += '"'; i++; }  // escaped quote
      else inQ = !inQ;
    } else if (ch === ',' && !inQ) {
      fields.push(cur.trim());
      cur = '';
    } else {
      cur += ch;
    }
  }
  fields.push(cur.trim());
  return fields;
}

// ─── Date helper: "DD-MM-YYYY" → "YYYY-MM" ───────────────────────────────────
function toMonthKey(dateStr) {
  // dateStr is expected as "DD-MM-YYYY"
  const parts = dateStr.split('-');
  if (parts.length !== 3) return null;
  const [, mm, yyyy] = parts;
  if (!yyyy || !mm) return null;
  return `${yyyy}-${mm.padStart(2, '0')}`;
}

// ─── Canonicalise an account name ────────────────────────────────────────────
function canonicalName(raw) {
  const key = raw.toLowerCase().trim();
  if (PRICING_ALIASES.has(key)) return PRICING_ALIASES.get(key);
  return key;
}

// ─── Main export ──────────────────────────────────────────────────────────────
/**
 * Parse a Quicklizard pricing-events CSV export.
 *
 * @param {string} csvText  Full CSV file content as a string.
 * @returns {{
 *   events: { [account: string]: { [monthKey: string]: number } },
 *   meta: {
 *     parsed: number,
 *     skipped: number,
 *     accounts: number,
 *     month: string|null,
 *     importedAt: string
 *   }
 * }}
 */
function parsePricingCsv(csvText) {
  const lines = csvText.split('\n');
  const events = {};
  const monthsSeen = new Set();
  let parsed = 0, skipped = 0;

  if (!lines[0]) {
    return {
      events,
      meta: { parsed, skipped, accounts: 0, month: null, importedAt: new Date().toISOString() },
    };
  }

  // Skip the header row (line 0); process data rows from line 1 onward.
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const cols = parseLine(line);

    const rawAccount = (cols[0] || '').trim();
    const rawAmount  = (cols[5] || '').trim();
    const rawDate    = (cols[7] || '').trim();

    if (!rawAccount) { skipped++; continue; }

    // Skip internal / test accounts (compare lowercase)
    if (PRICING_SKIP.has(rawAccount.toLowerCase().trim())) { skipped++; continue; }

    const amount = parseInt(rawAmount, 10);
    if (isNaN(amount)) { skipped++; continue; }

    const monthKey = toMonthKey(rawDate);
    if (!monthKey) { skipped++; continue; }

    const account = canonicalName(rawAccount);

    // Accumulate
    if (!events[account]) events[account] = {};
    events[account][monthKey] = (events[account][monthKey] || 0) + amount;

    monthsSeen.add(monthKey);
    parsed++;
  }

  // Infer the month for this file (most common month key, or the only one).
  // In practice all rows share the same month, but we pick the mode to be safe.
  let month = null;
  if (monthsSeen.size === 1) {
    [month] = monthsSeen;
  } else if (monthsSeen.size > 1) {
    // Tally per-month row counts across all accounts and pick the top one.
    const freq = {};
    for (const acctData of Object.values(events)) {
      for (const [mk, v] of Object.entries(acctData)) {
        freq[mk] = (freq[mk] || 0) + v;
      }
    }
    month = Object.keys(freq).reduce((a, b) => freq[a] >= freq[b] ? a : b, null);
  }

  console.log(
    `   pricing-csv: ${parsed} rows parsed, ${skipped} skipped, ` +
    `${Object.keys(events).length} accounts, month=${month}`
  );

  return {
    events,
    meta: {
      parsed,
      skipped,
      accounts: Object.keys(events).length,
      month,
      importedAt: new Date().toISOString(),
    },
  };
}

module.exports = { parsePricingCsv, PRICING_SKIP, PRICING_ALIASES };
