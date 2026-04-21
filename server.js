'use strict';
require('dotenv').config({ path: require('path').join(__dirname, '.env') });

const express = require('express');
const path    = require('path');
const fs      = require('fs');
const { refreshAll } = require('./transform');
const { parseCsvHours } = require('./connectors/csv-hours');

const app  = express();
const PORT = process.env.PORT || 3001;

// ─── Sync progress state (updated live during /api/refresh) ─────────────────
let syncState = { running: false, step: 'idle', done: 0, total: 0, error: null };

// ─── HubSpot portal info (fetched once on startup) ──────────────────────────
let HS_PORTAL_ID     = process.env.HS_PORTAL_ID     || 0;
let HS_PORTAL_DOMAIN = process.env.HS_PORTAL_DOMAIN || '';

async function fetchPortalInfo() {
  try {
    const hsKey = process.env.HUBSPOT_API_KEY;
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
  : path.join(__dirname, '..', 'profitability-dashboard.html');

// Cached data paths
const CACHE_FILE     = path.join(__dirname, 'data', 'cache.json');
const CSV_HOURS_FILE = path.join(__dirname, 'data', 'csv-hours.json');
const OVERRIDES_FILE = path.join(__dirname, 'data', 'overrides.json');

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
  'edg': 'endeavorglobal', // ClickUp "EDG" / "EDG TO" → Endeavor Global, Inc
};

/**
 * Direct full-name map: norm(CSV customer name) → norm(HubSpot company name).
 * Takes priority over fuzzy matching to prevent one CSV name matching multiple
 * HubSpot entities (e.g. "Whirlpool" would otherwise match all 4 Whirlpool companies).
 * Keys = norm(CSV name), Values = norm(HubSpot name).
 */
const CSV_NAME_MAP = {
  'whirlpool':           'whirlpoolmdaus',                  // "Whirlpool" → Whirlpool MDA US
  'whirlpoolus':         'whirlpoolmdaus',                  // "Whirlpool US" → Whirlpool MDA US
  'whirlpoolussda':      'whirlpoolsdauskitchenaid',        // "Whirlpool US SDA" → Whirlpool SDA US (KitchenAid)
  'kitchenaidanz':       'whirlpoolsdaaustraliakitchenaid', // "KitchenAid ANZ" → Whirlpool SDA Australia (KitchenAid)
  'kitchenaidlamex':     'whirlpoolsdalatamkitchenaid',     // "KitchenAid Lamex" → Whirlpool SDA LATAM (KitchenAid)
  'kitchenaidaustralia': 'whirlpoolsdaaustraliakitchenaid', // "KitchenAid Australia" → Whirlpool SDA Australia (KitchenAid)
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
      // ── Sync from API button ──
      `\n  <button class="import-btn" id="apiSyncBtn" onclick="_syncFromApis()"
    style="background:rgba(99,102,241,.12);border-color:rgba(99,102,241,.3);color:#818cf8;margin-left:6px;">
    ⟳ Sync HubSpot + ClickUp
  </button>` +
      // ── Hidden file input ──
      `\n  <input type="file" id="csvHoursInput" accept=".csv" style="display:none"
    onchange="_importCsvHours(this)">` +
      // ── Import CSV Hours button ──
      `\n  <button class="import-btn" id="csvHoursBtn"
    onclick="document.getElementById('csvHoursInput').click()"
    style="background:rgba(52,211,153,.10);border-color:rgba(52,211,153,.3);color:#34d399;margin-left:6px;"${csvStatus}>
    📂 Import Hours CSV${csvHours ? ' ✓' : ''}
  </button>`
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
    await fetch('/api/refresh', { method: 'POST' });
    startPolling();
  } catch (e) {
    alert('Sync error: ' + e.message);
    btn.textContent = orig; btn.disabled = false;
  }
}

// ── Import hours from ClickUp CSV export ─────────────────────────────────────
async function _importCsvHours(input) {
  const file = input.files[0];
  if (!file) return;
  const btn = document.getElementById('csvHoursBtn');
  const orig = btn.textContent;
  btn.disabled = true; btn.textContent = '⏳ Importing…';
  try {
    const text = await file.text();
    const r    = await fetch('/api/import-hours', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ csv: text, filename: file.name }),
    });
    const j = await r.json();
    if (j.ok) {
      btn.textContent = '✓ ' + j.parsed + ' rows — reloading…';
      setTimeout(() => location.reload(), 800);
    } else {
      alert('Import failed: ' + (j.error || 'unknown'));
      btn.textContent = orig; btn.disabled = false;
    }
  } catch (e) {
    alert('Import error: ' + e.message);
    btn.textContent = orig; btn.disabled = false;
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
      // No cache yet — inject the Sync button + its script so user can trigger first sync
      const IMPORT_BTN = '>📤 Import ClickUp CSV</button>';
      if (html.includes(IMPORT_BTN)) {
        html = html.replace(IMPORT_BTN, IMPORT_BTN +
          `\n  <button class="import-btn" id="apiSyncBtn" onclick="_syncFromApis()"
    style="background:rgba(99,102,241,.12);border-color:rgba(99,102,241,.3);color:#818cf8;margin-left:6px;">
    ⟳ Sync HubSpot + ClickUp
  </button>`);
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
    await fetch('/api/refresh', { method: 'POST' });
    startPolling();
  } catch (e) {
    alert('Sync error: ' + e.message);
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
    const cuKey = process.env.CLICKUP_API_KEY;
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

/** Trigger a live data refresh from HubSpot + ClickUp (non-blocking) */
app.post('/api/refresh', (req, res) => {
  if (syncState.running) {
    return res.json({ ok: true, alreadyRunning: true });
  }
  syncState = { running: true, step: 'Connecting…', done: 0, total: 0, error: null };
  res.json({ ok: true, started: true }); // respond immediately so client can start polling

  console.log('\n⟳  Refresh triggered via API…');
  // Run the heavy work in the background
  (async () => {
    try {
      const data = await refreshAll((update) => { Object.assign(syncState, update); });
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
    const { hours, parsed, skipped } = parseCsvHours(csv);
    const customers = Object.keys(hours);

    const payload = {
      hours,
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

  if (!process.env.HUBSPOT_API_KEY || !process.env.CLICKUP_API_KEY) {
    console.warn('⚠  API keys missing in .env\n');
  }
});
