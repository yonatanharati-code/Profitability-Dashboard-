'use strict';

/**
 * NDR connector — fetches the 7 published Google Sheets monthly tabs
 * and parses them into the ndr-snapshot.json format.
 *
 * Sheet columns (from published CSV):
 *   Client, total_hours_cost, total_pricing_cost, total_partner_cost,
 *   total_stock_cost, total_competitor_data_cost, Cost, Income,
 *   Profitability, Profitability Percentage
 */

const https = require('https');

// ─── Month → published CSV URL mapping ───────────────────────────────────────
// Each gid corresponds to a monthly tab in the Google Sheet.
const MONTH_TABS = [
  { month: '2025-09', gid: '1257271307' },
  { month: '2025-10', gid: '43034925'   },
  { month: '2025-11', gid: '1654013687' },
  { month: '2025-12', gid: '933015208'  },
  { month: '2026-01', gid: '2135359581' },
  { month: '2026-02', gid: '2145898483' },
  { month: '2026-03', gid: '354741632'  },
];

const BASE_URL =
  'https://docs.google.com/spreadsheets/d/e/' +
  '2PACX-1vTD5IWRpIVYFtyiGQLNU0nhHOCIZO_ALRxjzTzlNOpDlzed2MOW9hU9-345BgKCodLDhAqA2BXx1U-4' +
  '/pub?output=csv&gid=';

// ─── Helpers ──────────────────────────────────────────────────────────────────

function fetchUrl(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { timeout: 15000 }, (res) => {
      // Follow redirect (Google Sheets returns 302 to actual CSV)
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetchUrl(res.headers.location).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      let body = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => resolve(body));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

/** Parse a single CSV line, handling quoted fields with embedded commas */
function parseLine(line) {
  const fields = [];
  let cur = '', inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQ && line[i + 1] === '"') { cur += '"'; i++; }
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

/** Parse a number, handling commas, percent signs, and empty strings */
function parseNum(s) {
  if (!s) return 0;
  return parseFloat(String(s).replace(/[,%]/g, '').trim()) || 0;
}

/** Parse one month's CSV text into an array of client rows */
function parseCsvMonth(csvText) {
  const lines = csvText.split('\n').filter(l => l.trim());
  if (lines.length < 2) return [];

  const header = parseLine(lines[0]);
  const idx = {};
  header.forEach((h, i) => {
    idx[h.trim().toLowerCase().replace(/[^a-z0-9_]/g, '_')] = i;
  });

  // Column name normalisation (sheet may use different capitalisations)
  function col(names) {
    for (const n of names) {
      const k = n.toLowerCase().replace(/[^a-z0-9_]/g, '_');
      if (idx[k] !== undefined) return idx[k];
    }
    return -1;
  }

  const COL = {
    client:          col(['client', 'Client']),
    hours_cost:      col(['total_hours_cost',     'hours_cost']),
    pricing_cost:    col(['total_pricing_cost',   'pricing_cost']),
    partner_cost:    col(['total_partner_cost',   'partner_cost']),
    stock_cost:      col(['total_stock_cost',     'stock_cost']),
    competitor_cost: col(['total_competitor_data_cost', 'competitor_data_cost', 'competitor_cost']),
    cost:            col(['cost', 'Cost']),
    income:          col(['income', 'Income']),
    profit:          col(['profitability', 'Profitability']),
    profit_pct:      col(['profitability_percentage', 'profitability percentage', 'profit_pct']),
  };

  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = parseLine(lines[i]);
    const client = COL.client >= 0 ? (cols[COL.client] || '').trim() : '';
    if (!client || client.toLowerCase() === 'total' || client.toLowerCase() === 'grand total') continue;

    rows.push({
      client,
      hours_cost:      parseNum(COL.hours_cost      >= 0 ? cols[COL.hours_cost]      : ''),
      pricing_cost:    parseNum(COL.pricing_cost     >= 0 ? cols[COL.pricing_cost]    : ''),
      partner_cost:    parseNum(COL.partner_cost     >= 0 ? cols[COL.partner_cost]    : ''),
      stock_cost:      parseNum(COL.stock_cost       >= 0 ? cols[COL.stock_cost]      : ''),
      competitor_cost: parseNum(COL.competitor_cost  >= 0 ? cols[COL.competitor_cost] : ''),
      cost:            parseNum(COL.cost             >= 0 ? cols[COL.cost]            : ''),
      income:          parseNum(COL.income           >= 0 ? cols[COL.income]          : ''),
      profit:          parseNum(COL.profit           >= 0 ? cols[COL.profit]          : ''),
      profit_pct:      parseNum(COL.profit_pct       >= 0 ? cols[COL.profit_pct]      : ''),
    });
  }
  return rows;
}

// ─── Main export ──────────────────────────────────────────────────────────────

/**
 * Fetch all 7 monthly tabs and return the NDR snapshot object.
 * Shape: { "2025-09": [...rows], "2025-10": [...rows], ... }
 *
 * @param {(msg: string) => void} onProgress  optional progress callback
 * @returns {Promise<Object>}
 */
async function fetchNdrSnapshot(onProgress) {
  const snapshot = {};

  for (const { month, gid } of MONTH_TABS) {
    const url = BASE_URL + gid;
    if (onProgress) onProgress(`Fetching NDR ${month}…`);
    try {
      const csv  = await fetchUrl(url);
      const rows = parseCsvMonth(csv);
      snapshot[month] = rows;
      console.log(`   NDR ${month}: ${rows.length} clients`);
    } catch (err) {
      console.warn(`   NDR ${month} fetch failed: ${err.message}`);
      snapshot[month] = []; // keep month key so UI still shows it
    }
  }

  return snapshot;
}

module.exports = { fetchNdrSnapshot, MONTH_TABS };
