'use strict';
require('dotenv').config({ path: require('path').join(__dirname, '.env') });

const express = require('express');
const path    = require('path');
const fs      = require('fs');
const { refreshAll } = require('./transform');
const { parseCsvHours } = require('./connectors/csv-hours');
const { parsePricingCsv } = require('./connectors/pricing-csv');
const { fetchNdrSnapshot } = require('./connectors/ndr');

const app  = express();
const PORT = process.env.PORT || 3001;

// ─── Sync progress state (updated live during /api/refresh) ─────────────────
let syncState = { running: false, step: 'idle', done: 0, total: 0, error: null };

// ─── HubSpot portal info (fetched once on startup) ──────────────────────────
let HS_PORTAL_ID     = process.env.HS_PORTAL_ID     || 0;
let HS_PORTAL_DOMAIN = process.env.HS_PORTAL_DOMAIN || '';

async function fetchPortalInfo() {
  try {
    const hsKey = getHsKey();
    if (!hsKey) return;
    const https = require('https');
    await new Promise((resolve) => {
      https.get('https://api.hubapi.com/account-info/v3/details', {
        headers: { Authorization: `Bearer ${hsKey}` },
      }, (res) => {
        let raw = '';
        res.on('data', c => raw += c);
        res.on('end', () => {
          try {
            const d = JSON.parse(raw);
            if (d.portalId)  { HS_PORTAL_ID = d.portalId; }
            if (d.uiDomain)  { HS_PORTAL_DOMAIN = d.uiDomain; }
            console.log(`   HubSpot portal: ${HS_PORTAL_ID} (${HS_PORTAL_DOMAIN})`);
          } catch(e) { /* ignore */ }
          resolve();
        });
      }).on('error', resolve);
    });
  } catch(e) { /* ignore portal fetch errors */ }
}
fetchPortalInfo();

// Path to the original static dashboard HTML (template)
const DASHBOARD_HTML = process.env.DASHBOARD_HTML
  ? path.resolve(__dirname, process.env.DASHBOARD_HTML)
  : fs.existsSync(path.join(__dirname, 'profitability-dashboard.html'))
    ? path.join(__dirname, 'profitability-dashboard.html')            // same dir (Railway)
    : path.join(__dirname, '..', 'profitability-dashboard.html');     // parent dir (local dev)

// Cached data paths
const CACHE_FILE       = path.join(__dirname, 'data', 'cache.json');
const CSV_HOURS_FILE   = path.join(__dirname, 'data', 'csv-hours.json');
const OVERRIDES_FILE   = path.join(__dirname, 'data', 'overrides.json');
const NDR_FILE         = path.join(__dirname, 'data', 'ndr-snapshot.json');
const PRICING_FILE     = path.join(__dirname, 'data', 'pricing-events.json');
const CREDENTIALS_FILE = path.join(__dirname, 'data', 'credentials.json');

// ─── Credentials helpers ──────────────────────────────────────────────────────
// File-based credentials override env vars so they can be updated via the UI
// without touching Railway environment variables.
function readCredentials() {
  try {
    if (!fs.existsSync(CREDENTIALS_FILE)) return {};
    return JSON.parse(fs.readFileSync(CREDENTIALS_FILE, 'utf8'));
  } catch { return {}; }
}
function getHsKey()  {
  const creds = readCredentials();
  return creds.hubspotApiKey || process.env.HUBSPOT_API_KEY || '';
}
function getCuKey()  {
  const creds = readCredentials();
  return creds.clickupApiKey || process.env.CLICKUP_API_KEY || '';
}

app.use(express.json({ limit: '50mb' }));      // CSV text can be large
app.use(express.text({ limit: '50mb', type: 'text/csv' }));

// ─── Helpers ──────────────────────────────────────────────────────────────────

function readCache() {
  try {
    if (!fs.existsSync(CACHE_FILE)) return null;
    return JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
  } catch { return null; }
}

function readCsvHours() {
  try {
    if (!fs.existsSync(CSV_HOURS_FILE)) return null;
    return JSON.parse(fs.readFileSync(CSV_HOURS_FILE, 'utf8'));
  } catch { return null; }
}

function readNdr() {
  try {
    if (!fs.existsSync(NDR_FILE)) return null;
    return JSON.parse(fs.readFileSync(NDR_FILE, 'utf8'));
  } catch { return null; }
}

function readPricing() {
  try {
    if (!fs.existsSync(PRICING_FILE)) return { months: {}, imports: [] };
    return JSON.parse(fs.readFileSync(PRICING_FILE, 'utf8'));
  } catch { return { months: {}, imports: [] }; }
}

function writePricing(data) {
  fs.mkdirSync(path.dirname(PRICING_FILE), { recursive: true });
  fs.writeFileSync(PRICING_FILE, JSON.stringify(data, null, 2));
}

// ─── Fuzzy match (same as transform.js) ──────────────────────────────────────
function norm(s) {
  return (s || '').toLowerCase().normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9]/g, '');
}
function fuzzyMatch(a, b) {
  const na = norm(a), nb = norm(b);
  if (!na || na.length < 2) return false;
  if (na === nb) return true;
  if (na.startsWith(nb) || nb.startsWith(na)) return true;
  const ratio = Math.min(na.length, nb.length) / Math.max(na.length, nb.length);
  if (ratio >= 0.75) {
    if (na.length >= 4 && nb.includes(na)) return true;
    if (nb.length >= 4 && na.includes(nb)) return true;
  }
  return false;
}

// ─── Customer name aliases (ClickUp abbreviations → HubSpot names) ───────────
// Key   = lowercase word that appears standalone in the ClickUp CSV task/folder name.
// Value = norm() of the HubSpot company name.
const CSV_CUSTOMER_ALIASES = {
  // ClickUp CSV "Customer" field word → norm() prefix of HubSpot company name
  'edg':          'endeavorglobal',    // "EDG" → Endeavor Global, Inc
  'phh':          'pigult',            // "PHH Group" → pigu.lt
  'feelunique':   'sephora',           // "Feelunique" → sephora (FeelUnique)
  'pieper':       'stadtparfumerie',   // "Pieper" → stadt-parfümerie pieper
  'apotek':       'apotek',            // "Apotek1" → Apotek 1
  'hiper':        'hipercohen',        // "Hiper Cohen" → Hiper Cohen
  'ikano':        'ikano',             // "Ikano" → Ikano
  'istore':       'istore',            // "iStore" (Israel)
  'horze':        'horze',             // "Horze"
  'ducatillon':   'ducatillon',        // "Ducatillon"
  'andlight':     'andlight',          // "Andlight"
  'dehner':       'dehner',            // "Dehner"
  'allegro':      'allegro',           // "Allegro"
  'breuninger':   'breuninger',        // "Breuninger"
  'indimade':     'indimade',          // "Indimade"
  'weski':        'weski',             // "WeSki"
  'orchidea':     'orchidea',          // "orchidea"
};

/**
 * Direct full-name map: norm(CSV customer name) → norm(HubSpot company name).
 * Takes priority over fuzzy matching to prevent one CSV name matching multiple
 * HubSpot entities (e.g. "Whirlpool" would otherwise match all 4 Whirlpool companies).
 * Keys = norm(CSV name), Values = norm(HubSpot name).
 */
const CSV_NAME_MAP = {
  // Exact ClickUp Customer field value (normed) → norm() of HubSpot company name
  'whirlpool':                  'whirlpoolmdaus',                  // "Whirlpool"         → Whirlpool MDA US
  'whirlpoolus':                'whirlpoolmdaus',                  // "Whirlpool US"      → Whirlpool MDA US
  'whirlpoolmda':               'whirlpoolmdaus',                  // "Whirlpool MDA"     → Whirlpool MDA US
  'whirlpoolussda':             'whirlpoolsdauskitchenaid',        // "Whirlpool US SDA"  → Whirlpool SDA US (KitchenAid)
  'whirlpoolusda':              'whirlpoolsdauskitchenaid',        // "Whirlpool US DA"   → Whirlpool SDA US (KitchenAid)
  'kitchenaidanz':              'whirlpoolsdaaustraliakitchenaid', // "KitchenAid ANZ"    → Whirlpool SDA Australia
  'kitchenaidlamex':            'whirlpoolsdalatamkitchenaid',     // "KitchenAid Lamex"  → Whirlpool SDA LATAM
  'kitchenaidaustralia':        'whirlpoolsdaaustraliakitchenaid', // "KitchenAid Australia" → Whirlpool SDA Australia
  'verkokkaupa':                'verkkokauppacomoyj',              // ClickUp typo → verkkokauppa.com oyj
  'verkkokauppa':               'verkkokauppacomoyj',              // correct spelling → verkkokauppa.com oyj
  'xxxlutz':                    'xxxldigital',                     // "XXXLutz" — CS/SA task name for XXXL
  'xxxl':                       'xxxldigital',                     // "XXXL" → xxxldigital
  'phoenixpharmaswitzerland':   'healthandlife',                   // "Phoenix Pharma Switzerland"
  'phhgroup':                   'pigult',                          // "PHH Group" → pigu.lt
  'phh':                        'pigult',                          // "PHH" → pigu.lt
};

/** Return all CSV keys that match a customer, including via alias table. */
function findCsvKeys(customerName, csvKeys) {
  const cn = norm(customerName);

  // Guard: non-Latin company names (Hebrew, Cyrillic, etc.) normalise to '' — skip all matching
  if (!cn) return [];

  // 0. Direct name map — highest priority, prevents ambiguous fuzzy matches
  // Find all CSV keys whose norm() maps to this HubSpot company's norm()
  const directMatches = csvKeys.filter((k) => CSV_NAME_MAP[norm(k)] === cn);
  if (directMatches.length) return directMatches;

  // Also: if this HubSpot company IS the target of a direct map, don't fuzzy-match
  // (prevents the generic fuzzy match from pulling in keys that belong to other entities)
  const isDirectTarget = Object.values(CSV_NAME_MAP).includes(cn);
  if (isDirectTarget) return []; // no fuzzy fallback for companies in the name map

  // 1. Alias: this customer is the target of a word-level alias
  for (const [aliasWord, targetNorm] of Object.entries(CSV_CUSTOMER_ALIASES)) {
    if (cn === targetNorm || cn.startsWith(targetNorm) || targetNorm.startsWith(cn)) {
      const aliasMatches = csvKeys.filter((k) =>
        k.toLowerCase().split(/[\s\-_]+/).includes(aliasWord)
      );
      if (aliasMatches.length) return aliasMatches;
    }
  }

  // 2. Normal fuzzy match (only for companies not in the direct name map)
  const k = csvKeys.find((k) => fuzzyMatch(customerName, k));
  return k ? [k] : [];
}

// ─── Merge CSV hours into customer array ─────────────────────────────────────
function mergeHours(customers, csvHours) {
  const rnd = (v) => Math.round(v * 10) / 10;
  const csvKeys = Object.keys(csvHours);

  for (const c of customers) {
    // For the consolidated Samsung customer, collect ALL Samsung CSV variants.
    // For every other customer use alias + fuzzy match.
    const isSamsung = /samsung/i.test(c.name);
    const matched = isSamsung
      ? csvKeys.filter((k) => /samsung/i.test(k))
      : findCsvKeys(c.name, csvKeys);

    for (const csvKey of matched) {
      const src = csvHours[csvKey];
      if (!src) continue;
      for (const type of ['cs', 'sa', 'dev', 'bug']) {
        if (!src[type]) continue;
        if (!c[type]) c[type] = { m1: 0, m3: 0, m6: 0, monthly: {} };
        c[type].m1 = rnd(c[type].m1 + (src[type].m1 || 0));
        c[type].m3 = rnd(c[type].m3 + (src[type].m3 || 0));
        c[type].m6 = rnd(c[type].m6 + (src[type].m6 || 0));
        for (const [mk, hrs] of Object.entries(src[type].monthly || {})) {
          c[type].monthly[mk] = rnd((c[type].monthly[mk] || 0) + hrs);
        }
      }
    }
  }
  return customers;
}

/**
 * Walk a string from `start` (which must point to an opening bracket/brace)
 * and return the index of the matching closing bracket/brace.
 */
function findBlockEnd(str, start, open, close) {
  let depth = 0, inStr = null;
  for (let i = start; i < str.length; i++) {
    const ch = str[i];
    if (inStr) {
      if (ch === '\\') { i++; continue; }
      if (ch === inStr) { inStr = null; continue; }
      continue;
    }
    if (ch === '"' || ch === "'") { inStr = ch; continue; }
    if (ch === open)  { depth++; continue; }
    if (ch === close) { depth--; if (depth === 0) return i; }
  }
  return -1;
}

/**
 * Inject live customers and deals, merge CSV hours, add buttons & scripts.
 */
function injectData(html, cache, csvHours) {
  // Deep-clone customers so we don't mutate the cache object
  let customers = JSON.parse(JSON.stringify(cache.customers));
  if (csvHours) customers = mergeHours(customers, csvHours.hours || csvHours);

  // ── Replace DATA array ────────────────────────────────────────────────────
  const DATA_MARKER = 'const DATA = [';
  const dataStart   = html.indexOf(DATA_MARKER);
  if (dataStart !== -1) {
    const arrOpen = dataStart + DATA_MARKER.length - 1;
    const arrEnd  = findBlockEnd(html, arrOpen, '[', ']');
    if (arrEnd !== -1) {
      html = html.slice(0, dataStart) +
             `const DATA = ${JSON.stringify(customers)}` +
             html.slice(arrEnd + 1);
    }
  }

  // ── Inject HubSpot portal info ───────────────────────────────────────────
  if (html.includes('const HS_PORTAL_ID=0')) {
    html = html.replace('const HS_PORTAL_ID=0', `const HS_PORTAL_ID=${HS_PORTAL_ID}`);
  }
  if (html.includes('const HS_PORTAL_DOMAIN=""')) {
    html = html.replace('const HS_PORTAL_DOMAIN=""', `const HS_PORTAL_DOMAIN="${HS_PORTAL_DOMAIN}"`);
  }

  // ── Replace DEALS object ──────────────────────────────────────────────────
  const DEALS_MARKER = 'const DEALS={';
  const dealsStart   = html.indexOf(DEALS_MARKER);
  if (dealsStart !== -1) {
    const objOpen = dealsStart + DEALS_MARKER.length - 1;
    const objEnd  = findBlockEnd(html, objOpen, '{', '}');
    if (objEnd !== -1) {
      html = html.slice(0, dealsStart) +
             `const DEALS=${JSON.stringify(cache.deals)}` +
             html.slice(objEnd + 1);
    }
  }

  // ── Replace USER_HOURS object ─────────────────────────────────────────────
  const UH_MARKER = 'const USER_HOURS={';
  const uhStart   = html.indexOf(UH_MARKER);
  if (uhStart !== -1) {
    const uhOpen = uhStart + UH_MARKER.length - 1;
    const uhEnd  = findBlockEnd(html, uhOpen, '{', '}');
    if (uhEnd !== -1) {
      // Prefer CSV userHours (more complete — has per-user detail from export)
      const uhData = (csvHours?.userHours && Object.keys(csvHours.userHours).length > 0)
        ? csvHours.userHours
        : (cache.userHours || {});
      html = html.slice(0, uhStart) +
             `const USER_HOURS=${JSON.stringify(uhData)}` +
             html.slice(uhEnd + 1);
    }
  }

  // ── Replace NDR_DATA object ───────────────────────────────────────────────
  const ndrRaw = readNdr();
  if (ndrRaw) {
    // Strip internal metadata keys before injecting into client
    const { _syncedAt: ndrSyncedAt, ...ndrData } = ndrRaw;
    const NDR_MARKER = 'const NDR_DATA={';
    const ndrStart = html.indexOf(NDR_MARKER);
    if (ndrStart !== -1) {
      const ndrOpen = ndrStart + NDR_MARKER.length - 1;
      const ndrEnd  = findBlockEnd(html, ndrOpen, '{', '}');
      if (ndrEnd !== -1) {
        html = html.slice(0, ndrStart) +
               `const NDR_DATA=${JSON.stringify(ndrData)}` +
               html.slice(ndrEnd + 1);
      }
    }
    // Update the existing NDR_SYNCED_AT placeholder (replace in-place to avoid duplicate const)
    html = html.replace(
      /const NDR_SYNCED_AT="[^"]*"/,
      `const NDR_SYNCED_AT=${JSON.stringify(ndrSyncedAt || '')}`
    );
  }

  // ── Replace PRICING_DATA object ───────────────────────────────────────────────
  const pricingRaw = readPricing();
  const PRICING_MARKER = 'const PRICING_DATA={';
  const pricingStart = html.indexOf(PRICING_MARKER);
  if (pricingStart !== -1) {
    const pricingOpen = pricingStart + PRICING_MARKER.length - 1;
    const pricingEnd  = findBlockEnd(html, pricingOpen, '{', '}');
    if (pricingEnd !== -1) {
      html = html.slice(0, pricingStart) +
             `const PRICING_DATA=${JSON.stringify(pricingRaw.months || {})}` +
             html.slice(pricingEnd + 1);
    }
  }

  // ── Intercept sendChat at its first line so ALL call paths use local engine ──
  // This covers: onclick button, Enter key, askQuick preset buttons — everything.
  html = html.replace(
    'async function sendChat(){',
    'async function sendChat(){ if(typeof _qlLocalChat==="function"){_qlLocalChat();return;}'
  );

  // ── Update "last synced" text ─────────────────────────────────────────────
  if (cache.lastRefreshed) {
    const d     = new Date(cache.lastRefreshed);
    const label = d.toLocaleDateString('en-GB', {
      day: '2-digit', month: 'short', year: 'numeric',
      hour: '2-digit', minute: '2-digit',
    });
    html = html.replace(
      /Data from ClickUp export[^<.]*[.<]/,
      `Data refreshed from HubSpot + ClickUp · ${label}.`
    );
    html = html.replace(/· \d+ Customers ·/, `· ${customers.length} Customers ·`);
  }

  // ── Inject buttons next to existing Import CSV button ────────────────────
  const IMPORT_BTN = '>📤 Import ClickUp CSV</button>';
  if (html.includes(IMPORT_BTN)) {
    const csvStatus = csvHours
      ? ` title="CSV hours loaded: ${csvHours.meta?.parsed || '?'} rows · ${csvHours.meta?.importedAt?.substring(0,10) || ''}"`
      : '';
    html = html.replace(
      IMPORT_BTN,
      IMPORT_BTN +
      // ── Sync HubSpot button (hours come from CSV — ClickUp not fetched) ──
      `\n  <button class="import-btn" id="apiSyncBtn" onclick="_syncFromApis()"
    style="background:rgba(99,102,241,.12);border-color:rgba(99,102,241,.3);color:#818cf8;margin-left:6px;">
    ⟳ Sync HubSpot
  </button>` +
      // ── Import CSV Hours — use <label for="input"> so the file picker opens
      //    natively without JS .click() (which gets blocked by some browsers) ──
      `\n  <input type="file" id="csvHoursInput" accept=".csv" style="display:none"
    onchange="_importCsvHours(this)">` +
      `\n  <label for="csvHoursInput" id="csvHoursBtn" class="import-btn"
    style="background:rgba(52,211,153,.10);border-color:rgba(52,211,153,.3);color:#34d399;margin-left:6px;cursor:pointer;display:inline-flex;align-items:center;gap:4px;"${csvStatus}>
    📂 Import Hours CSV${csvHours ? ' ✓' : ''}
  </label>` +
      // ── Sync NDR button ──────────────────────────────────────────────────────
      `\n  <button class="import-btn" id="ndrSyncBtn" onclick="_syncNdr()"
    style="background:rgba(250,204,21,.10);border-color:rgba(250,204,21,.3);color:#facc15;margin-left:6px;">
    ↻ Sync NDR
  </button>` +
      `\n  <span id="ndrSyncedAtBadge" style="margin-left:6px;"></span>` +
      // ── Import Pricing CSVs (EU + US) ─────────────────────────────────────
      (() => {
        const pr = readPricing();
        const months = Object.keys(pr.months || {}).sort();
        const pricingTip = months.length
          ? ` title="Pricing data: ${months.length} months stored (${months[0]} → ${months[months.length-1]})"`
          : '';
        return `\n  <input type="file" id="pricingEuInput" accept=".csv" multiple style="display:none" onchange="_importPricingCsv(this,'EU')">` +
               `\n  <label for="pricingEuInput" class="import-btn" style="background:rgba(139,92,246,.10);border-color:rgba(139,92,246,.3);color:#a78bfa;margin-left:6px;cursor:pointer;display:inline-flex;align-items:center;gap:4px;"${pricingTip}>` +
               `📊 EU Pricing${months.length ? ' ✓' : ''}</label>` +
               `\n  <input type="file" id="pricingUsInput" accept=".csv" multiple style="display:none" onchange="_importPricingCsv(this,'US')">` +
               `\n  <label for="pricingUsInput" class="import-btn" style="background:rgba(139,92,246,.10);border-color:rgba(139,92,246,.3);color:#a78bfa;margin-left:4px;cursor:pointer;display:inline-flex;align-items:center;gap:4px;">` +
               `📊 US Pricing</label>` +
               `\n  <a href="/api/export-pricing" class="import-btn" style="background:rgba(16,185,129,.08);border-color:rgba(16,185,129,.3);color:#34d399;margin-left:4px;display:inline-flex;align-items:center;gap:4px;text-decoration:none;" title="Download pricing data backup (commit to git to preserve across deploys)">⬇ Backup</a>`;
      })()
    );
  }

  // ── Inject YTD button next to the 6M window button ──────────────────────
  html = html.replace(
    '<button class="window-btn active" data-w="6">6M</button>',
    '<button class="window-btn active" data-w="6">6M</button>' +
    '<button class="window-btn" data-w="0" id="ytdBtn" style="color:#34d399;">YTD</button>'
  );

  // ── Inject client-side scripts ─────────────────────────────────────────────
  const SCRIPTS = `
<script>
// ── YTD support ───────────────────────────────────────────────────────────────
(function() {
  // 1. Pre-compute per-customer YTD totals from monthly buckets
  const thisYear = new Date().getFullYear();
  DATA.forEach(c => {
    ['cs','sa','dev'].forEach(t => {
      const monthly = (c[t] && c[t].monthly) || {};
      c[t].ytd = Math.round(
        Object.entries(monthly)
          .filter(([mk]) => mk.startsWith(thisYear + '-'))
          .reduce((sum, [, h]) => sum + h, 0)
        * 10) / 10;
    });
  });

  // 2. Snapshot the real m6 values so we can restore them
  const _savedM6 = DATA.map(c => ({ cs: c.cs.m6, sa: c.sa.m6, dev: c.dev.m6 }));
  let _ytdActive = false;

  // Fractional months elapsed so far this year (e.g. mid-April ≈ 3.5)
  function _ytdMonths() {
    const n = new Date();
    return Math.max(n.getMonth() + n.getDate() / 30, 0.1);
  }

  // 3. Scale m6 so the existing 6M logic produces the correct YTD monthly rate:
  //    mrate(c, field, 6) = m6 / 6  →  set m6 = ytd / ytdMonths * 6
  function _applyYTD() {
    const mo = _ytdMonths();
    DATA.forEach(c => {
      c.cs.m6  = Math.round((c.cs.ytd  || 0) / mo * 6 * 10) / 10;
      c.sa.m6  = Math.round((c.sa.ytd  || 0) / mo * 6 * 10) / 10;
      c.dev.m6 = Math.round((c.dev.ytd || 0) / mo * 6 * 10) / 10;
    });
    _ytdActive = true;
  }

  function _restoreM6() {
    DATA.forEach((c, i) => {
      c.cs.m6  = _savedM6[i].cs;
      c.sa.m6  = _savedM6[i].sa;
      c.dev.m6 = _savedM6[i].dev;
    });
    _ytdActive = false;
  }

  // 4. Patch renderKPIs to rename "6M" labels to "YTD" while in YTD mode
  const _rkpi0 = window.renderKPIs;
  window.renderKPIs = function() {
    _rkpi0();
    if (!_ytdActive) return;
    document.querySelectorAll('#kpiGrid .kpi-label').forEach(el => {
      el.textContent = el.textContent.replace(/6M/g, 'YTD');
    });
  };

  // 5. Single capture-phase listener — fires BEFORE the original init() handlers.
  //    For the YTD button: we own the full render cycle and block the original handler.
  //    For other buttons: restore real m6 first so renderAll sees the correct data.
  document.addEventListener('click', function(e) {
    const btn = e.target.closest('.window-btn');
    if (!btn) return;

    if (btn.id === 'ytdBtn') {
      _applyYTD();
      win = 6;                          // use 6M window so all existing code works
      document.querySelectorAll('.window-btn').forEach(x => x.classList.remove('active'));
      btn.classList.add('active');
      renderAll();                      // renderKPIs patch renames "6M" → "YTD"
      _restoreM6();                     // restore immediately — chart data already copied
      e.stopImmediatePropagation();     // prevent original init() handler from re-rendering
    } else if (_ytdActive) {
      _restoreM6();                     // restore before original handler's renderAll
    }
  }, true); // capture phase — runs before bubble-phase handlers on the button
})();

// ── Local AI query engine injected from file ─────────────────────────────────

// ── Sync from HubSpot + ClickUp API ──────────────────────────────────────────
async function _syncFromApis() {
  const btn = document.getElementById('apiSyncBtn');
  if (!btn) return;
  const orig = btn.textContent;
  btn.disabled = true; btn.textContent = '⟳ Starting…';
  let pollTimer = null;
  function startPolling() {
    pollTimer = setInterval(async () => {
      try {
        const s = await fetch('/api/sync-status').then(r => r.json());
        if (s.running) {
          const pct = s.total > 0 ? ' ' + Math.round(s.done / s.total * 100) + '%' : '';
          btn.textContent = '⟳ ' + s.step + pct;
        } else if (s.step === 'complete') {
          clearInterval(pollTimer);
          btn.textContent = '✓ Done — reloading…';
          setTimeout(() => location.reload(), 800);
        } else if (s.error) {
          clearInterval(pollTimer);
          alert('Sync failed: ' + s.error);
          btn.textContent = orig; btn.disabled = false;
        }
      } catch (_) {}
    }, 1500);
  }
  try {
    await fetch('/api/refresh', { method: 'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ hubspotOnly: true }) });
    startPolling();
  } catch (e) {
    alert('Sync error: ' + e.message);
    btn.textContent = orig; btn.disabled = false;
  }
}

// ── Sync NDR data from Google Sheets ─────────────────────────────────────────
async function _syncNdr() {
  const btn = document.getElementById('ndrSyncBtn');
  if (!btn) return;
  const orig = btn.textContent;
  btn.disabled = true; btn.textContent = '↻ Fetching…';
  let pollTimer = null;
  function startPolling() {
    pollTimer = setInterval(async () => {
      try {
        const s = await fetch('/api/ndr-sync-status').then(r => r.json());
        if (s.running) {
          btn.textContent = '↻ ' + (s.step || '…');
        } else if (s.step === 'complete') {
          clearInterval(pollTimer);
          btn.textContent = '✓ NDR synced — reloading…';
          setTimeout(() => location.reload(), 800);
        } else if (s.error) {
          clearInterval(pollTimer);
          alert('NDR sync failed: ' + s.error);
          btn.textContent = orig; btn.disabled = false;
        }
      } catch (_) {}
    }, 1500);
  }
  try {
    await fetch('/api/sync-ndr', { method: 'POST' });
    startPolling();
  } catch (e) {
    alert('NDR sync error: ' + e.message);
    btn.textContent = orig; btn.disabled = false;
  }
}

// ── Import hours from ClickUp CSV export ─────────────────────────────────────
async function _importCsvHours(input) {
  const file = input.files[0];
  if (!file) return;
  const lbl = document.getElementById('csvHoursBtn');
  const orig = lbl ? lbl.textContent : '';
  // Disable label during upload (pointer-events works on label elements)
  if (lbl) { lbl.style.pointerEvents = 'none'; lbl.style.opacity = '0.6'; lbl.textContent = '⏳ Importing…'; }
  try {
    const text = await file.text();
    const r    = await fetch('/api/import-hours', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ csv: text, filename: file.name }),
    });
    const j = await r.json();
    if (j.ok) {
      if (lbl) lbl.textContent = '✓ ' + j.parsed + ' rows — reloading…';
      setTimeout(() => location.reload(), 800);
    } else {
      alert('Import failed: ' + (j.error || 'unknown'));
      if (lbl) { lbl.textContent = orig; lbl.style.pointerEvents = ''; lbl.style.opacity = ''; }
    }
  } catch (e) {
    alert('Import error: ' + e.message);
    if (lbl) { lbl.textContent = orig; lbl.style.pointerEvents = ''; lbl.style.opacity = ''; }
  }
  input.value = '';
}

async function _importPricingCsv(input, region) {
  const files = Array.from(input.files);
  if (!files.length) return;
  const inputId = region === 'EU' ? 'pricingEuInput' : 'pricingUsInput';
  const lblFor  = document.querySelector(\`label[for="\${inputId}"]\`);
  const origText = lblFor ? lblFor.textContent : '';
  if (lblFor) { lblFor.style.opacity = '0.6'; lblFor.style.pointerEvents = 'none'; }
  let lastResult = null;
  try {
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      if (lblFor) lblFor.textContent = files.length > 1
        ? \`⏳ Uploading \${i+1}/\${files.length}…\`
        : '⏳ Uploading…';
      const text = await file.text();
      const r = await fetch('/api/import-pricing', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ csv: text, filename: file.name, region }),
      });
      const j = await r.json();
      if (!j.ok) {
        alert(\`Pricing import failed for \${file.name}: \${j.error || 'unknown'}\`);
        if (lblFor) { lblFor.textContent = origText; lblFor.style.opacity = ''; lblFor.style.pointerEvents = ''; }
        input.value = '';
        return;
      }
      lastResult = j;
    }
    const s = files.length > 1 ? \`\${files.length} files\` : \`\${lastResult?.month || files[0].name}\`;
    if (lblFor) lblFor.textContent = \`✓ \${s} imported (\${lastResult?.totalMonths||'?'} months total) — reloading…\`;
    setTimeout(() => location.reload(), 1100);
  } catch (e) {
    alert('Pricing import error: ' + e.message);
    if (lblFor) { lblFor.textContent = origText; lblFor.style.opacity = ''; lblFor.style.pointerEvents = ''; }
  }
  input.value = '';
}
</script>`;

  // ── Inject local chat engine from file ───────────────────────────────────────
  const LOCAL_CHAT_FILE = path.join(__dirname, 'connectors', 'local-chat-client.js');
  const localChatJs = fs.existsSync(LOCAL_CHAT_FILE)
    ? fs.readFileSync(LOCAL_CHAT_FILE, 'utf8')
    : '/* local-chat-client.js not found */';

  // Hide the API key row via CSS — more reliable than JS querySelector
  const CHAT_CSS = '<style>.chat-key-row{display:none!important}</style>';

  // Use a replacer function — the string form of replace() interprets $' as a
  // special pattern ("text after match"), which corrupts any JS that contains '$'.
  const injection = CHAT_CSS + SCRIPTS + `\n<script>\n${localChatJs}\n</script>\n</body>`;
  html = html.replace('</body>', () => injection);
  return html;
}

// ─── Routes ───────────────────────────────────────────────────────────────────

/** Serve the dashboard with server-side data injection */
app.get('/', (req, res) => {
  if (!fs.existsSync(DASHBOARD_HTML)) {
    return res.status(500).send(
      `Dashboard template not found at:<br><code>${DASHBOARD_HTML}</code><br><br>` +
      `Set <code>DASHBOARD_HTML</code> in .env or point to the correct file.`
    );
  }
  try {
    let html     = fs.readFileSync(DASHBOARD_HTML, 'utf8');
    const cache  = readCache();
    const csvHrs = readCsvHours();
    if (cache) {
      html = injectData(html, cache, csvHrs);
    } else {
      // No cache yet — inject buttons so user can sync / import CSV
      const IMPORT_BTN = '>📤 Import ClickUp CSV</button>';
      if (html.includes(IMPORT_BTN)) {
        html = html.replace(IMPORT_BTN, IMPORT_BTN +
          `\n  <button class="import-btn" id="apiSyncBtn" onclick="_syncFromApis()"
    style="background:rgba(99,102,241,.12);border-color:rgba(99,102,241,.3);color:#818cf8;margin-left:6px;">
    ⟳ Sync HubSpot
  </button>` +
          `\n  <input type="file" id="csvHoursInput" accept=".csv" style="display:none" onchange="_importCsvHours(this)">` +
          `\n  <label for="csvHoursInput" id="csvHoursBtn" class="import-btn"
    style="background:rgba(52,211,153,.10);border-color:rgba(52,211,153,.3);color:#34d399;margin-left:6px;cursor:pointer;display:inline-flex;align-items:center;gap:4px;">
    📂 Import Hours CSV
  </label>` +
          `\n  <button class="import-btn" id="ndrSyncBtn" onclick="_syncNdr()"
    style="background:rgba(250,204,21,.10);border-color:rgba(250,204,21,.3);color:#facc15;margin-left:6px;">
    ↻ Sync NDR
  </button>` +
          `\n  <span id="ndrSyncedAtBadge" style="margin-left:6px;"></span>`
        );
      }
      // Inject the _syncFromApis function so the button actually works
      html = html.replace('</body>', () => `<script>
async function _syncFromApis() {
  const btn = document.getElementById('apiSyncBtn');
  if (!btn) return;
  const orig = btn.textContent;
  btn.disabled = true; btn.textContent = '⟳ Starting…';
  let pollTimer = null;
  function startPolling() {
    pollTimer = setInterval(async () => {
      try {
        const s = await fetch('/api/sync-status').then(r => r.json());
        if (s.running) {
          const pct = s.total > 0 ? ' ' + Math.round(s.done / s.total * 100) + '%' : '';
          btn.textContent = '\\u27F3 ' + s.step + pct;
        } else if (s.step === 'complete') {
          clearInterval(pollTimer);
          btn.textContent = '\\u2713 Done \\u2014 reloading\\u2026';
          setTimeout(() => location.reload(), 800);
        } else if (s.error) {
          clearInterval(pollTimer);
          alert('Sync failed: ' + s.error);
          btn.textContent = orig; btn.disabled = false;
        }
      } catch (_) {}
    }, 1500);
  }
  try {
    await fetch('/api/refresh', { method: 'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ hubspotOnly: true }) });
    startPolling();
  } catch (e) {
    alert('Sync error: ' + e.message);
    btn.textContent = orig; btn.disabled = false;
  }
}
async function _importCsvHours(input) {
  const file = input.files[0];
  if (!file) return;
  const lbl = document.getElementById('csvHoursBtn');
  const orig = lbl ? lbl.textContent : '';
  if (lbl) { lbl.style.pointerEvents = 'none'; lbl.style.opacity = '0.6'; lbl.textContent = '⏳ Importing…'; }
  try {
    const text = await file.text();
    const r = await fetch('/api/import-hours', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ csv: text, filename: file.name }),
    });
    const j = await r.json();
    if (j.ok) {
      if (lbl) lbl.textContent = '✓ ' + j.parsed + ' rows — reloading…';
      setTimeout(() => location.reload(), 800);
    } else {
      alert('Import failed: ' + (j.error || 'unknown'));
      if (lbl) { lbl.textContent = orig; lbl.style.pointerEvents = ''; lbl.style.opacity = ''; }
    }
  } catch (e) {
    alert('Import error: ' + e.message);
    if (lbl) { lbl.textContent = orig; lbl.style.pointerEvents = ''; lbl.style.opacity = ''; }
  }
  input.value = '';
}
async function _syncNdr() {
  const btn = document.getElementById('ndrSyncBtn');
  if (!btn) return;
  const orig = btn.textContent;
  btn.disabled = true; btn.textContent = '↻ Fetching…';
  let pollTimer = null;
  function startPolling() {
    pollTimer = setInterval(async () => {
      try {
        const s = await fetch('/api/ndr-sync-status').then(r => r.json());
        if (s.running) {
          btn.textContent = '↻ ' + (s.step || '…');
        } else if (s.step === 'complete') {
          clearInterval(pollTimer);
          btn.textContent = '✓ NDR synced — reloading…';
          setTimeout(() => location.reload(), 800);
        } else if (s.error) {
          clearInterval(pollTimer);
          alert('NDR sync failed: ' + s.error);
          btn.textContent = orig; btn.disabled = false;
        }
      } catch (_) {}
    }, 1500);
  }
  try {
    await fetch('/api/sync-ndr', { method: 'POST' });
    startPolling();
  } catch (e) {
    alert('NDR sync error: ' + e.message);
    btn.textContent = orig; btn.disabled = false;
  }
}
</script></body>`);
    }
    res.setHeader('Content-Type', 'text/html');
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.send(html);
  } catch (err) {
    console.error('Render error:', err);
    res.status(500).send('Error rendering dashboard: ' + err.message);
  }
});

/** Live sync progress — polled by the client every 1.5 s */
app.get('/api/sync-status', (req, res) => res.json(syncState));

/** Debug: show all workspace members and their roles */
app.get('/api/debug/members', async (req, res) => {
  const { fetchAllMembers } = require('./connectors/clickup');
  try {
    const cuKey = getCuKey();
    if (!cuKey) return res.status(500).json({ error: 'CLICKUP_API_KEY not set' });

    const { cuGetRaw } = require('./connectors/clickup');
    // fetch raw team data to show all roles
    const https = require('https');
    const raw = await new Promise((resolve, reject) => {
      const req2 = https.request({
        hostname: 'api.clickup.com',
        path: '/api/v2/team',
        method: 'GET',
        headers: { Authorization: cuKey },
      }, (r) => {
        let d = '';
        r.on('data', c => d += c);
        r.on('end', () => resolve(JSON.parse(d)));
      });
      req2.on('error', reject);
      req2.end();
    });

    const ROLE_NAMES = { 1: 'Owner', 2: 'Admin', 3: 'Member', 4: 'Observer', 5: 'Guest' };
    const teams = raw.teams ?? [];
    const team  = teams.find(t => String(t.id) === '31065585') ?? teams[0];
    if (!team) return res.json({ error: 'No team found', raw });

    const members = (team.members ?? []).map(m => ({
      id:       m.user?.id,
      username: m.user?.username,
      email:    m.user?.email,
      role:     m.user?.role,
      roleName: ROLE_NAMES[m.user?.role] ?? `Unknown(${m.user?.role})`,
      willFetch: [1,2,3].includes(m.user?.role),
    })).sort((a, b) => (a.role ?? 99) - (b.role ?? 99));

    res.json({
      total:      members.length,
      willFetch:  members.filter(m => m.willFetch).length,
      members,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/**
 * Debug: show how the last sync classified entries — who got mapped to what type.
 * Visit /api/debug in browser after a sync to diagnose SA/Dev/Bug mismatches.
 */
app.get('/api/debug', (req, res) => {
  const cache = readCache();
  if (!cache) return res.json({ error: 'No cache yet — run a sync first' });

  // Summarise hours per customer per type
  const summary = cache.customers.map((c) => ({
    name:   c.name,
    cs_6m:  c.cs?.m6  ?? 0,
    sa_6m:  c.sa?.m6  ?? 0,
    dev_6m: c.dev?.m6 ?? 0,
    bug_6m: c.bug?.m6 ?? 0,
  })).sort((a, b) => (b.cs_6m + b.sa_6m + b.dev_6m + b.bug_6m) - (a.cs_6m + a.sa_6m + a.dev_6m + a.bug_6m));

  res.json({
    info: 'Visit /api/debug/unmatched to see ClickUp names that had no matching customer.',
    lastRefreshed: cache.lastRefreshed,
    meta: cache.meta,
    customerBreakdown: summary,
  });
});

/**
 * Debug: show the top ClickUp names that didn't match any customer in the last sync.
 * Use this to find missing aliases (e.g. ClickUp tracks XXXL as "XXXL Group" but
 * HubSpot has a different name).
 */
app.get('/api/debug/unmatched', (req, res) => {
  const cache = readCache();
  if (!cache) return res.json({ error: 'No cache yet — run a sync first' });

  const names = cache.meta?.topUnmatchedNames || [];
  const totalUnmatched = cache.meta?.unmatchedHours || 0;

  // Also show all HubSpot customer names so you can spot near-misses manually
  const hubspotNames = cache.customers
    .map((c) => c.name)
    .sort((a, b) => a.localeCompare(b));

  res.json({
    info: `${totalUnmatched} time entries had no matching HubSpot customer. Top unmatched ClickUp folder/list/task names shown below. To fix: add the name to CUSTOMER_ALIASES in transform.js.`,
    lastRefreshed: cache.lastRefreshed,
    typeCount: cache.meta?.typeCount || {},
    topUnmatchedClickUpNames: names,
    hubspotCustomerCount: hubspotNames.length,
    hubspotCustomers: hubspotNames,
  });
});

/**
 * Debug: fetch a sample of recent time entries for every user in USER_TYPES
 * and group them by customer-name candidate. Answers two questions instantly:
 *   1. Why does XXXL show only dev? → CS/SA users' entries will have different folder/list names
 *   2. What do bug task names look like? → shows actual task names for TechOps entries
 *
 * Usage: /api/debug/clickup-names   (takes ~30s to complete)
 */
app.get('/api/debug/clickup-names', async (req, res) => {
  const cuKey = getCuKey();
  if (!cuKey) return res.status(500).json({ error: 'CLICKUP_API_KEY not set' });

  const { USER_TYPES } = require('./connectors/clickup');
  const https = require('https');
  const now   = Date.now();
  const start = now - 30 * 24 * 60 * 60 * 1000; // last 30 days

  function cuFetch(path) {
    return new Promise((resolve) => {
      const req2 = https.request(
        { hostname: 'api.clickup.com', path, method: 'GET', headers: { Authorization: cuKey } },
        (r) => { let d = ''; r.on('data', c => d += c); r.on('end', () => { try { resolve(JSON.parse(d)); } catch { resolve({}); } }); }
      );
      req2.on('error', () => resolve({}));
      req2.setTimeout(20000, () => { req2.destroy(); resolve({}); });
      req2.end();
    });
  }

  const results = [];

  for (const [userId, userType] of Object.entries(USER_TYPES)) {
    for (const [spaceLabel, spaceId] of [['CS', '54974334'], ['TechOps', '66622361']]) {
      const qs = new URLSearchParams({
        start_date: String(start), end_date: String(now),
        space_id: spaceId, assignee: userId,
        include_location_names: 'true', page: '0',
      });
      const data = await cuFetch(`/api/v2/team/31065585/time_entries?${qs}`);
      const entries = (data.data || []).slice(0, 5); // first 5 entries per user per space
      if (!entries.length) continue;

      for (const e of entries) {
        results.push({
          userId,
          userType,
          space:       spaceLabel,
          user:        e.user?.username || e.user?.id,
          folder_name: e.task_location?.folder_name || null,
          list_name:   e.task_location?.list_name   || null,
          task_name:   e.task?.name                 || null,
          duration_h:  Math.round((parseInt(e.duration) || 0) / 3600000 * 10) / 10,
        });
      }
    }
  }

  // Group by user+space so it's easy to read
  const byUser = {};
  for (const r of results) {
    const key = `${r.userType} | user:${r.userId} | ${r.space}`;
    if (!byUser[key]) byUser[key] = [];
    byUser[key].push({ folder: r.folder_name, list: r.list_name, task: r.task_name, h: r.duration_h });
  }

  res.json({ note: 'Up to 5 recent entries per user per space. Check CS entries for XXXL-related users to see their folder/list names.', entries: byUser });
});

/** Trigger a live data refresh from HubSpot + ClickUp (non-blocking) */
app.post('/api/refresh', (req, res) => {
  if (syncState.running) {
    return res.json({ ok: true, alreadyRunning: true });
  }

  // hubspotOnly=true → skip ClickUp, keep CSV hours (default mode now)
  // hubspotOnly=false → full sync including ClickUp hours (clears CSV)
  const hubspotOnly = req.body?.hubspotOnly !== false; // default true

  syncState = { running: true, step: 'Connecting…', done: 0, total: 0, error: null };
  res.json({ ok: true, started: true });

  if (!hubspotOnly && fs.existsSync(CSV_HOURS_FILE)) {
    // Full ClickUp sync takes over hours — remove CSV to prevent doubling
    fs.unlinkSync(CSV_HOURS_FILE);
    console.log('   🗑  Cleared uploaded CSV hours (full ClickUp sync takes precedence)');
  }

  console.log(`\n⟳  Refresh triggered — mode: ${hubspotOnly ? 'HubSpot only' : 'HubSpot + ClickUp'}…`);
  (async () => {
    try {
      const data = await refreshAll((update) => { Object.assign(syncState, update); }, { hubspotOnly, hsKey: getHsKey(), cuKey: getCuKey() });
      fs.mkdirSync(path.dirname(CACHE_FILE), { recursive: true });
      fs.writeFileSync(CACHE_FILE, JSON.stringify(data, null, 2));
      console.log(`✓  Refresh complete — ${data.meta.customerCount} customers, ${data.meta.dealCount} deals`);
      syncState = { running: false, step: 'complete', done: 0, total: 0, error: null, meta: data.meta };
    } catch (err) {
      const msg = err?.message || err?.toString() || 'unknown error';
      console.error('Refresh failed:', msg);
      syncState = { running: false, step: 'error', done: 0, total: 0, error: msg };
    }
  })();
});

/** Import hours from a ClickUp CSV export */
app.post('/api/import-hours', (req, res) => {
  try {
    const csv      = req.body?.csv || req.body;
    const filename = req.body?.filename || 'upload.csv';
    if (!csv || typeof csv !== 'string') {
      return res.status(400).json({ ok: false, error: 'No CSV text provided' });
    }

    console.log(`\n📂  Importing hours CSV: ${filename} (${(csv.length / 1024).toFixed(1)} KB)`);
    const { hours, userHours, parsed, skipped } = parseCsvHours(csv);
    const customers = Object.keys(hours);

    // CSV upload is now the sole source of hours — zero out any API-synced hours in
    // the cache so the two sources never add together and double-count.
    const cache = readCache();
    if (cache) {
      const emptyBkt = () => ({ m1: 0, m3: 0, m6: 0, monthly: {} });
      cache.customers.forEach((c) => {
        c.cs  = emptyBkt();
        c.sa  = emptyBkt();
        c.dev = emptyBkt();
        c.bug = emptyBkt();
      });
      fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2));
      console.log('   🗑  Zeroed API hours in cache (CSV takes precedence)');
    }

    const payload = {
      hours,
      userHours,
      meta: { parsed, skipped, customers: customers.length, importedAt: new Date().toISOString(), filename },
    };

    fs.mkdirSync(path.dirname(CSV_HOURS_FILE), { recursive: true });
    fs.writeFileSync(CSV_HOURS_FILE, JSON.stringify(payload, null, 2));
    console.log(`✓  CSV import done — ${parsed} rows, ${customers.length} customers`);

    res.json({ ok: true, parsed, skipped, customers: customers.length });
  } catch (err) {
    console.error('CSV import failed:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

/**
 * Import a pricing events CSV (EU or US region).
 *
 * Storage model — prevents double-counting on re-upload:
 *   stored.regions[REGION][YYYY-MM] = { account: count, … }
 *
 * Each upload REPLACES the region+month slice it covers, then the merged
 * `stored.months` view is rebuilt from scratch by summing all regions.
 * Re-uploading the same region+month is therefore always idempotent.
 * Uploading EU for December never touches US data for December (and vice versa).
 */
app.post('/api/import-pricing', (req, res) => {
  try {
    const csv      = req.body?.csv || req.body;
    const filename = req.body?.filename || 'pricing.csv';
    const region   = (req.body?.region || 'EU').toUpperCase();
    if (!csv || typeof csv !== 'string') {
      return res.status(400).json({ ok: false, error: 'No CSV text provided' });
    }
    console.log(`\n📊  Importing pricing CSV (${region}): ${filename} (${(csv.length/1024).toFixed(1)} KB)`);
    const { events, meta } = parsePricingCsv(csv);
    const { month } = meta;
    if (!month) return res.status(400).json({ ok: false, error: 'Could not determine month from CSV dates' });

    const stored = readPricing();
    if (!stored.regions) stored.regions = {};
    if (!stored.imports) stored.imports = [];

    // Determine which month-keys this file contains
    const monthsInFile = new Set();
    for (const monthData of Object.values(events)) {
      for (const mk of Object.keys(monthData)) monthsInFile.add(mk);
    }

    // REPLACE this region's data for those months (clear first, then write)
    if (!stored.regions[region]) stored.regions[region] = {};
    for (const mk of monthsInFile) {
      stored.regions[region][mk] = {};          // wipe previous upload for same region+month
    }
    for (const [account, monthData] of Object.entries(events)) {
      for (const [mk, count] of Object.entries(monthData)) {
        stored.regions[region][mk][account] = (stored.regions[region][mk][account] || 0) + count;
      }
    }

    // Rebuild merged months view (EU + US summed together)
    const merged = {};
    for (const regionData of Object.values(stored.regions)) {
      for (const [mk, accounts] of Object.entries(regionData)) {
        if (!merged[mk]) merged[mk] = {};
        for (const [account, count] of Object.entries(accounts)) {
          merged[mk][account] = (merged[mk][account] || 0) + count;
        }
      }
    }
    stored.months = merged;

    // Record import log
    stored.imports.push({ month, region, filename, accounts: meta.accounts, parsed: meta.parsed, importedAt: meta.importedAt });

    writePricing(stored);
    console.log(`✓  Pricing import done — region=${region}, month=${month}, ${meta.accounts} accounts, ${meta.parsed} rows`);
    res.json({ ok: true, month, accounts: meta.accounts, parsed: meta.parsed, totalMonths: Object.keys(stored.months).length });
  } catch (err) {
    console.error('Pricing import failed:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

/** Return pricing status */
app.get('/api/pricing-status', (req, res) => {
  const pr = readPricing();
  const months = Object.keys(pr.months || {}).sort();
  res.json({ loaded: months.length > 0, months, imports: pr.imports || [] });
});

/** Export / download pricing-events.json (for backup / git commit) */
app.get('/api/export-pricing', (req, res) => {
  const pr = readPricing();
  res.setHeader('Content-Disposition', 'attachment; filename="pricing-events.json"');
  res.setHeader('Content-Type', 'application/json');
  res.send(JSON.stringify(pr, null, 2));
});

/** Return raw cached data as JSON */
app.get('/api/data', (req, res) => {
  const cache = readCache();
  if (!cache) return res.status(404).json({ error: 'No cache yet.' });
  const csvHrs = readCsvHours();
  if (csvHrs) {
    const customers = JSON.parse(JSON.stringify(cache.customers));
    cache.customers = mergeHours(customers, csvHrs.hours || csvHrs);
  }
  res.json(cache);
});

/** Return CSV hours status */
app.get('/api/csv-hours', (req, res) => {
  const h = readCsvHours();
  if (!h) return res.status(404).json({ loaded: false });
  res.json({ loaded: true, ...h.meta });
});

/** Return NDR snapshot status */
app.get('/api/ndr-status', (req, res) => {
  const ndr = readNdr();
  if (!ndr) return res.json({ ok: false });
  const months = Object.keys(ndr).sort();
  const lastKey = months[months.length - 1] || null;
  res.json({ ok: true, months, lastMonth: lastKey });
});

/** Fetch fresh NDR data from published Google Sheets CSVs and update snapshot */
// Seed syncedAt from file if it exists
let _ndrFileSyncedAt = null;
try {
  const _snap = JSON.parse(fs.readFileSync(NDR_FILE, 'utf8'));
  if (_snap._syncedAt) _ndrFileSyncedAt = _snap._syncedAt;
} catch {}
let ndrSyncState = { running: false, step: 'idle', error: null, syncedAt: _ndrFileSyncedAt };

app.get('/api/ndr-sync-status', (req, res) => res.json(ndrSyncState));

app.post('/api/sync-ndr', (req, res) => {
  if (ndrSyncState.running) return res.json({ ok: true, alreadyRunning: true });
  ndrSyncState = { running: true, step: 'Starting…', error: null };
  res.json({ ok: true, started: true });

  console.log('\n⟳  NDR sync triggered…');
  (async () => {
    try {
      const snapshot = await fetchNdrSnapshot((msg) => {
        ndrSyncState.step = msg;
      });
      fs.mkdirSync(path.dirname(NDR_FILE), { recursive: true });
      const syncedAt = new Date().toISOString();
      fs.writeFileSync(NDR_FILE, JSON.stringify({ _syncedAt: syncedAt, ...snapshot }, null, 2));
      const months = Object.keys(snapshot).sort();
      console.log(`✓  NDR sync complete — ${months.length} months`);
      ndrSyncState = { running: false, step: 'complete', error: null, months, syncedAt };
    } catch (err) {
      const msg = err?.message || String(err);
      console.error('NDR sync failed:', msg);
      ndrSyncState = { running: false, step: 'error', error: msg };
    }
  })();
});

/** Save user overrides (devHoursPurchased, devPaidManual, excluded, etc.) */
app.post('/api/overrides', (req, res) => {
  try {
    const overrides = req.body; // array of {id, ...fields}
    if (!Array.isArray(overrides)) return res.status(400).json({ ok: false, error: 'Expected array' });
    fs.writeFileSync(OVERRIDES_FILE, JSON.stringify(overrides, null, 2));
    res.json({ ok: true, saved: overrides.length });
  } catch (err) {
    console.error('Overrides save failed:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

/** Load user overrides */
app.get('/api/overrides', (req, res) => {
  try {
    if (!fs.existsSync(OVERRIDES_FILE)) return res.json([]);
    res.json(JSON.parse(fs.readFileSync(OVERRIDES_FILE, 'utf8')));
  } catch { res.json([]); }
});

/** Health / status */
app.get('/api/status', (req, res) => {
  const cache = readCache();
  res.json({
    ok: true,
    dashboardHtml: DASHBOARD_HTML,
    cache: cache ? { lastRefreshed: cache.lastRefreshed, customers: cache.meta?.customerCount } : null,
    csvHours: readCsvHours()?.meta || null,
  });
});

// ─── Credentials endpoints ────────────────────────────────────────────────────
/** GET /api/credentials — returns masked keys so UI can show status */
app.get('/api/credentials', (req, res) => {
  const creds = readCredentials();
  const mask = (k) => k ? k.slice(0, 6) + '…' + k.slice(-4) : null;
  res.json({
    hubspotApiKey: mask(creds.hubspotApiKey || process.env.HUBSPOT_API_KEY),
    clickupApiKey: mask(creds.clickupApiKey || process.env.CLICKUP_API_KEY),
    source: {
      hubspot: creds.hubspotApiKey ? 'file' : (process.env.HUBSPOT_API_KEY ? 'env' : 'missing'),
      clickup: creds.clickupApiKey ? 'file' : (process.env.CLICKUP_API_KEY ? 'env' : 'missing'),
    },
  });
});

/** POST /api/credentials — save new keys to data/credentials.json */
app.post('/api/credentials', (req, res) => {
  try {
    const { hubspotApiKey, clickupApiKey } = req.body || {};
    if (!hubspotApiKey && !clickupApiKey) {
      return res.status(400).json({ error: 'Provide at least one key to update' });
    }
    const existing = readCredentials();
    const updated = {
      ...existing,
      ...(hubspotApiKey ? { hubspotApiKey: hubspotApiKey.trim() } : {}),
      ...(clickupApiKey ? { clickupApiKey: clickupApiKey.trim() } : {}),
      updatedAt: new Date().toISOString(),
    };
    fs.mkdirSync(path.dirname(CREDENTIALS_FILE), { recursive: true });
    fs.writeFileSync(CREDENTIALS_FILE, JSON.stringify(updated, null, 2));
    console.log(`✓  Credentials updated via UI (${Object.keys(req.body).join(', ')})`);
    res.json({ ok: true, message: 'Keys saved. Click "Sync" to apply.' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Start ────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`\n╔══════════════════════════════════════════════════════╗`);
  console.log(`║  QL Profitability Dashboard (Live)                   ║`);
  console.log(`║  http://localhost:${PORT}                               ║`);
  console.log(`╚══════════════════════════════════════════════════════╝\n`);

  if (!fs.existsSync(DASHBOARD_HTML)) {
    console.warn(`⚠  Dashboard template not found: ${DASHBOARD_HTML}`);
  }

  const cache = readCache();
  if (cache) {
    console.log(`   Cache: ${cache.meta?.customerCount} customers · refreshed ${cache.lastRefreshed}`);
  } else {
    console.log(`   No cache yet. Open http://localhost:${PORT} and click "⟳ Sync HubSpot + ClickUp".`);
  }

  const csvH = readCsvHours();
  if (csvH) {
    console.log(`   CSV hours: ${csvH.meta?.parsed} rows · ${csvH.meta?.customers} customers · ${csvH.meta?.importedAt?.substring(0,10)}`);
  }

  if (!getHsKey() || !getCuKey()) {
    console.warn('⚠  API keys missing — set via dashboard Settings or .env\n');
  }
});
