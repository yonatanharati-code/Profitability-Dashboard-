'use strict';
require('dotenv').config({ path: require('path').join(__dirname, '.env') });
const { fetchAllCompanies, fetchDeals, fetchPropertyHistories, toUSD, VALID_CSMS } = require('./connectors/hubspot');
const { streamAllTimeEntries, USER_TYPES, BUG_TASK_RE, BUG_LIST_RE } = require('./connectors/clickup');

// ─── Bug detection helpers ────────────────────────────────────────────────────
// Matches "No - Performance / Bug" in ClickUp's Billable custom field
const BUG_BILLABLE_RE = /performance|\/\s*bug/i;

// ─── String helpers ───────────────────────────────────────────────────────────
/** Normalise a string: lowercase, strip non-alphanumeric, strip diacritics */
function norm(s) {
  return (s || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]/g, '');
}

/** Fuzzy customer-name matcher (same logic as the dashboard CSV import) */
function fuzzyMatch(a, b) {
  const na = norm(a);
  const nb = norm(b);
  if (!na || na.length < 2) return false;
  if (na === nb) return true;
  if (na.startsWith(nb) || nb.startsWith(na)) return true;
  // Substring match:
  // • If the shorter name is ≥6 chars AND appears inside the longer one, match.
  //   (e.g. "sephora" inside "sephorafeelunique" → match)
  //   The 6-char floor prevents short generics ("shop", "store") from matching.
  // • Legacy 75% ratio check kept as a secondary guard.
  const shorter = na.length <= nb.length ? na : nb;
  const longer  = na.length <= nb.length ? nb : na;
  if (shorter.length >= 6 && longer.includes(shorter)) return true;
  const ratio = shorter.length / longer.length;
  if (ratio >= 0.75) {
    if (na.length >= 4 && nb.includes(na)) return true;
    if (nb.length >= 4 && na.includes(nb)) return true;
  }
  return false;
}

/** URL-safe slug from a company name (used as DATA id and DEALS key) */
function slugify(name) {
  return (name || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '') // strip diacritics
    .replace(/[^a-z0-9]+/g, '-')    // non-alphanumeric → hyphen
    .replace(/^-|-$/g, '')           // trim leading/trailing hyphens
    || 'unknown';
}

// ─── Deal classification ──────────────────────────────────────────────────────
/**
 * Returns 'dev' for Development Hours / Setup Fees;
 * returns 'upsell' for everything else.
 * Mirrors the existing hardcoded DEALS data convention.
 */
function classifyDeal(dealName) {
  const n = (dealName || '').toLowerCase();
  if (
    n.includes('dev hour') ||
    n.includes('setup fee') ||
    n.includes('set up fee') ||
    n.includes('setup_fee') ||
    n.includes('development hour') ||
    n.includes('dev package') ||
    n.includes(' dev ') && n.includes('hour')
  ) return 'dev';
  return 'upsell';
}

// ─── Stage / health mapping ───────────────────────────────────────────────────
function mapStage(s) {
  if (!s) return null;
  const l = s.toLowerCase();
  if (l === 'live' || l === 'ongoing') return 'Live';
  if (l.includes('onboarding')) return 'Onboarding';
  if (l.includes('development') || l === 'dev') return 'Development';
  if (l.includes('uat')) return 'UAT';
  if (l.includes('poc')) return 'POC';
  if (l.includes('pricing') || l.includes('workshop')) return 'Pricing Workshop';
  return s;
}

function mapHealth(h) {
  if (!h) return 'green';
  switch (h.toLowerCase()) {
    case 'red':    return 'red';
    case 'yellow': return 'yellow';
    default:       return 'green';
  }
}

// ─── Hours accumulator ────────────────────────────────────────────────────────
const MS1  = 30  * 24 * 60 * 60 * 1000;
const MS3  = 90  * 24 * 60 * 60 * 1000;
const MS6  = 180 * 24 * 60 * 60 * 1000;
const MS12 = 365 * 24 * 60 * 60 * 1000;

function rnd(v) { return Math.round(v * 10) / 10; }

function emptyHours() {
  return { m1: 0, m3: 0, m6: 0, m12: 0, monthly: {} };
}

function addHours(bucket, durationMs, startMs, now) {
  const hours = durationMs / 3600_000;
  bucket.m12 += hours;                           // all entries within the 12M fetch window
  if (startMs >= now - MS6)  bucket.m6  += hours;
  if (startMs >= now - MS3)  bucket.m3  += hours;
  if (startMs >= now - MS1)  bucket.m1  += hours;
  const d  = new Date(startMs);
  const mk = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
  bucket.monthly[mk] = (bucket.monthly[mk] || 0) + hours;
}

function roundHours(bucket) {
  bucket.m1  = rnd(bucket.m1);
  bucket.m3  = rnd(bucket.m3);
  bucket.m6  = rnd(bucket.m6);
  bucket.m12 = rnd(bucket.m12);
  for (const k of Object.keys(bucket.monthly)) {
    bucket.monthly[k] = rnd(bucket.monthly[k]);
  }
}

// ─── ClickUp list name → customer extraction ──────────────────────────────────
/**
 * TechOps lists follow the pattern "💻 CustomerName - TO" (or "💻 CustomerName TO").
 * Strip the emoji prefix and " - TO" suffix to get the raw customer name.
 * This is used as the first candidate — much more reliable than fuzzy-matching
 * the full decorated list name.
 *
 * Examples:
 *   "💻 XXXL - TO"              → "XXXL"
 *   "💻 John Lewis - TO"        → "John Lewis"
 *   "💻 PHH Group - TO"         → "PHH Group"
 *   "💻 Whirlpool US SDA - TO"  → "Whirlpool US SDA"
 *   "💻 Hema- TO"               → "Hema"
 *   "💻 Doc Morris TO"          → "Doc Morris"
 *   "💻 Universidad_Europea-TO" → "Universidad Europea"
 */
function extractFromListName(listName) {
  if (!listName) return null;
  let s = listName.trim();
  // Strip leading emoji / non-alphanumeric chars
  s = s.replace(/^[^\w\s(]+\s*/u, '').trim();
  // Strip trailing " - TO", "- TO", " TO" (case-insensitive)
  s = s.replace(/\s*[-–]?\s*TO\s*$/i, '').trim();
  // Replace underscores with spaces
  s = s.replace(/_/g, ' ').trim();
  return s || null;
}

// ─── Direct norm → HubSpot name map for names that don't fuzzy-match ──────────
// Key   = norm(extracted ClickUp customer name)
// Value = norm() prefix of the HubSpot company name
const CLICKUP_NAME_MAP = {
  // Whirlpool variants — multiple HubSpot entities, can't rely on fuzzy
  'whirlpoolusda':          'whirlpoolsdauskitchenaid',   // "Whirlpool US SDA"
  'whirlpoolussda':         'whirlpoolsdauskitchenaid',   // "Whirlpool US SDA" (alt)
  'whirlpoolus':            'whirlpoolmdaus',              // "Whirlpool US"
  'whirlpoolmda':           'whirlpoolmdaus',              // "Whirlpool MDA"
  'kitchenaidanz':          'whirlpoolsdaaustraliakitchenaid',
  'kitchenaidaustralia':    'whirlpoolsdaaustraliakitchenaid',
  'kitchenaidlamex':        'whirlpoolsdalatamkitchenaid',
  // ClickUp abbreviations / name mismatches
  'phh':                    'pigult',           // "PHH Group" → pigu.lt
  'phhgroup':               'pigult',
  'xxxl':                   'xxxldigital',      // "XXXL" → xxxldigital – part of xxxl group
  'xxxlutz':                'xxxldigital',      // "XXXLutz" — CS/SA task name for XXXL
  'edg':                    'endeavorglobal',   // "EDG" → Endeavor Global
  'feelunique':             'sephora',          // "FeelUnique" → sephora (FeelUnique)
  'pieper':                 'stadtparfumerie',  // "Pieper" → stadt-parfümerie pieper
  'mbs':                    'modernbuilders',   // "MBS"
  'verkokkaupa':            'verkkokauppacomoyj', // typo in ClickUp
  'verkkokauppa':           'verkkokauppacomoyj',
  'kishreyteufa':           'kishrey',          // Hebrew name in ClickUp
  'apotek1':                'apotek',
  'docmorris':              'docmorris',        // normalises correctly but belt-and-suspenders
  'universitadeuropea':     'universidadeuropea',
  'phoenixpharmaswitzerland': 'healthandlife',  // "Phoenix Pharma Switzerland"
  'orchid':                 'orchidea',         // "Orchid" → Orchidea
};

/**
 * Look up a customer by normalized name using CLICKUP_NAME_MAP first,
 * then CUSTOMER_ALIASES word-level matching, then fuzzy matching.
 */
function findCustomerByName(candidateName, customers) {
  if (!candidateName) return null;
  const cn = norm(candidateName);

  // 1. Direct map — catches Whirlpool variants and other tricky names
  const directTarget = CLICKUP_NAME_MAP[cn];
  if (directTarget) {
    const found = customers.find((c) => {
      const hn = norm(c.name);
      return hn && (hn === directTarget || hn.startsWith(directTarget) || directTarget.startsWith(hn));
    });
    if (found) return found;
  }

  // 2. Word-level alias — catches single-word abbreviations anywhere in the name
  const words = candidateName.toLowerCase().split(/[\s\-_]+/);
  for (const [aliasWord, targetNorm] of Object.entries(CUSTOMER_ALIASES)) {
    if (words.includes(aliasWord)) {
      const found = customers.find((c) => {
        const hn = norm(c.name);
        return hn && (hn === targetNorm || hn.startsWith(targetNorm) || targetNorm.startsWith(hn));
      });
      if (found) return found;
    }
  }

  // 3. Fuzzy match
  return customers.find((c) => fuzzyMatch(c.name, candidateName)) || null;
}

// ─── Word-level aliases (single keyword → HubSpot name prefix) ───────────────
const CUSTOMER_ALIASES = {
  'edg':           'endeavorglobal',
  'xxxl':          'xxxldigital',
  'xxxlutz':       'xxxldigital',    // "XXXLutz" — how CS/SA log XXXL time
  'phh':           'pigult',
  'mbs':           'modernbuilders',
  'feelunique':    'sephora',
  'verkkokauppa':  'verkkokauppacomoyj',
  'euronics':      'euronics',
  'byggmax':       'byggmax',
  'pieper':        'stadtparfumerie',
  'apotek':        'apotek',
  'orchid':        'orchidea',
};

// ─── Deal → customer matching ─────────────────────────────────────────────────
/**
 * Explicit aliases: normalised deal-name token → normalised customer name.
 * Add entries here when a deal name and company name are too different for
 * automatic fuzzy matching (e.g. brand variants, subsidiaries).
 */
const DEAL_ALIASES = {
  'arcelikhitachi': 'arcelikturkiye', // "Arcelik Hitachi" deals → arçelik türkiye
  'ahha':           'arcelikturkiye', // "AHHA rollout" deals   → arçelik türkiye
};

/**
 * Find the customer ID that a deal belongs to.
 * Strategy:
 *   1. Explicit alias table (catches brand/subsidiary mismatches)
 *   2. Full deal name fuzzy match
 *   3. Each '-'-separated segment fuzzy match
 */
function matchDealToCustomer(dealName, customerIds, idToName) {
  const dn = norm(dealName);

  // 1. Alias table — check if any alias token appears in the normalised deal name
  for (const [aliasToken, targetNorm] of Object.entries(DEAL_ALIASES)) {
    if (dn.includes(aliasToken)) {
      const id = customerIds.find((id) => norm(idToName[id]) === targetNorm);
      if (id) return id;
    }
  }

  // 2. Full deal name
  for (const id of customerIds) {
    if (fuzzyMatch(idToName[id], dealName)) return id;
  }

  // 3. Each '-' / '–' separated segment
  const parts = dealName.split(/\s*[-–]\s*/);
  for (const part of parts) {
    if (part.length < 3) continue;
    for (const id of customerIds) {
      if (fuzzyMatch(idToName[id], part)) return id;
    }
  }

  return null;
}

// ─── Main ─────────────────────────────────────────────────────────────────────
/**
 * @param {function} onProgress
 * @param {{ hubspotOnly?: boolean }} opts
 *   hubspotOnly: skip ClickUp fetch entirely (hours come from CSV upload)
 */
async function refreshAll(onProgress = () => {}, opts = {}) {
  const { hubspotOnly = false } = opts;
  // Keys can be passed in directly (from server.js credentials helper) or fall back to env
  const hsKey = opts.hsKey || process.env.HUBSPOT_API_KEY;
  const cuKey = opts.cuKey || process.env.CLICKUP_API_KEY;
  if (!hsKey) throw new Error('HubSpot API key is not set — add it in Settings (⚙)');
  if (!hubspotOnly && !cuKey) throw new Error('ClickUp API key is not set — add it in Settings (⚙)');

  // ── Parallel fetch ──────────────────────────────────────────────────────────
  onProgress({ step: 'Fetching HubSpot data…' });
  console.log(`⟳  Fetching HubSpot companies + deals${hubspotOnly ? ' (HubSpot only — hours from CSV)' : ' + ClickUp time entries'}...`);
  const [companies, rawDeals] = await Promise.all([
    fetchAllCompanies(hsKey).then((r) => { console.log(`   ✓ ${r.length} companies`); return r; }),
    fetchDeals(hsKey).then((r)         => { console.log(`   ✓ ${r.length} deals`);     return r; }),
  ]);

  // ── Fetch property histories for ALL companies (churn date + ARR timeline) ──
  onProgress({ step: 'Fetching ARR & churn history…' });
  const allHsIds = companies.map(c => c.id);  // raw HubSpot numeric IDs
  const propHistories = await fetchPropertyHistories(hsKey, allHsIds);

  // ── Build customer records from HubSpot companies ───────────────────────────
  const customers = companies
    .map((c) => {
      const p = c.properties;
      const isChurn = (p.company_status || '').toLowerCase().includes('churn');
      const hist = propHistories[c.id] || { churnDate: null, arrHistory: [] };
      return {
        id:         slugify(p.name),
        hsId:       c.id,               // raw HubSpot numeric ID
        name:       p.name   || '',
        arr:        parseInt(p.arr)  || 0,
        arrHistory: hist.arrHistory,    // [{ date, arr }] sorted asc — for period GRR
        rd:         p.renewal_date   || null,
        stage:      isChurn ? 'Churn' : mapStage(p.onboarding_stage),
        rank:       p.rank           || null,
        owner:      VALID_CSMS.has(p.csm) ? p.csm : ((p.csms || '').split(';')[0].trim() || null),
        flag:       isChurn ? 'red' : mapHealth(p.health),
        churnDate:  hist.churnDate,     // 'YYYY-MM-DD' or null
        devPipe:    0,
        cs:  emptyHours(),
        sa:  emptyHours(),
        dev: emptyHours(),
        bug: emptyHours(),
      };
    });

  const idToName = Object.fromEntries(customers.map((c) => [c.id, c.name]));

  // ── Stream ClickUp time entries — skipped when hubspotOnly=true ─────────────
  let unmatched = 0;
  let totalEntries = 0;
  const typeCount   = { cs: 0, sa: 0, dev: 0, bug: 0 };
  const userTypeMap = {};
  const unmatchedNames = {};
  const userHours = {};

  if (hubspotOnly) {
    onProgress({ step: 'HubSpot synced — hours will come from CSV upload ✓' });
    console.log('   ↷  ClickUp skipped (hubspotOnly mode — hours sourced from CSV)');
  } else {
  onProgress({ step: 'HubSpot done — fetching ClickUp…' });
  const now = Date.now();

  const SKIP_NAMES = new Set([
    'Sprint Folder', 'Customer Management', 'General Meetings',
    'TechOps', 'Tech Ops', 'Internal', 'None', 'All',
    '💼 Customer Management',
  ]);

  await streamAllTimeEntries(cuKey, (entry) => {
    totalEntries++;

    const spaceName =
      entry.task_location?.space_name ||
      entry.task?.space?.name         ||
      '';
    const username =
      entry.user?.username ||
      entry.user?.email    ||
      '';
    const durationMs = parseInt(entry.duration) || 0;
    const startMs    = parseInt(entry.start)    || 0;
    if (durationMs <= 0 || startMs <= 0) return;

    // ── Customer matching ──────────────────────────────────────────────────
    // Strategy (in priority order):
    // 1. Extract clean name from TechOps list pattern "💻 CustomerName - TO"
    // 2. folder_name (CS space: often the customer name directly)
    // 3. list_name raw, task name
    // Each candidate is checked via CLICKUP_NAME_MAP → word-alias → fuzzy match.
    const rawListName = entry.task_location?.list_name || entry.task?.list?.name || '';
    const extractedName = extractFromListName(rawListName);

    const candidateNames = [
      extractedName,                          // "XXXL" from "💻 XXXL - TO"
      entry.task_location?.folder_name,       // CS space: customer folder
      entry.task_location?.list_name,         // raw list name (fallback)
      entry.task?.folder?.name,
      entry.task?.name,                       // task title (last resort)
    ].filter((n) => n && !SKIP_NAMES.has(n));

    let customer = null;
    for (const name of candidateNames) {
      customer = findCustomerByName(name, customers);
      if (customer) break;
    }
    if (!customer) {
      unmatched++;
      // Track what names we saw — helps diagnose missing customers like XXXL
      for (const n of candidateNames) {
        if (n && Object.keys(unmatchedNames).length < 300) {
          unmatchedNames[n] = (unmatchedNames[n] || 0) + 1;
        }
      }
      return;
    }

    // ── Classification ────────────────────────────────────────────────────
    // Look up the user's role by their ClickUp ID. Entries from users not in
    // USER_TYPES are silently skipped — they're guests/observers/irrelevant staff.
    const userId   = entry.user?.id ? Number(entry.user.id) : null;
    const baseType = userId ? USER_TYPES[userId] : null;
    if (!baseType) { unmatched++; return; }

    // Bug detection: applies to dev AND sa users.
    // SA engineers working on escalations in TechOps should count as bug,
    // not just devs. CS users are never re-classified as bug.
    //
    // Mirrors CSV logic (csv-hours.js isBugEntry):
    //  1. task.custom_type === 'bug'  — ClickUp "Item Type" task type
    //  2. Billable custom field = "No - Performance / Bug"
    //  3. TO Priority custom field = "Bug"
    //  4. task name keyword regex (BUG_TASK_RE)
    //  5. list name keyword regex (BUG_LIST_RE)
    //  6. entry tags contain bug/escalation keyword
    const individualTask = entry.task?.name || '';
    const listName       = entry.task_location?.list_name || entry.task?.list?.name || '';

    // (1) ClickUp task type / item_type  (returned as task.custom_type or task.item_type)
    const itemType = (entry.task?.custom_type || entry.task?.item_type || '').toString().toLowerCase();

    // (2) & (3) Custom fields on the task — ClickUp returns these in task.custom_fields[]
    //     when the time-entry was fetched with include_location_names=true.
    //     Field names: "Billable" and "⚠️ TO Priority"
    const taskCFs      = entry.task?.custom_fields || [];
    const billableCF   = taskCFs.find((cf) => /^billable$/i.test(cf.name || ''));
    const billableVal  = (billableCF?.value ?? billableCF?.type_config?.default ?? '').toString();
    const toPriCF      = taskCFs.find((cf) => /to\s*priority/i.test(cf.name || ''));
    const toPriVal     = (toPriCF?.value ?? '').toString().toLowerCase();

    // (6) Entry-level tags (time entry can carry tags like "bug", "escalation")
    const entryTagStr  = (entry.tags || []).map((t) => t.name || t).join(' ');

    const isBug = (baseType === 'dev' || baseType === 'sa') && (
      itemType === 'bug'                    ||  // (1) item type
      BUG_BILLABLE_RE.test(billableVal)    ||  // (2) "No - Performance / Bug"
      toPriVal === 'bug'                   ||  // (3) TO Priority = Bug
      BUG_TASK_RE.test(individualTask)     ||  // (4) task name keyword
      BUG_LIST_RE.test(listName)           ||  // (5) list name keyword
      /bug|escalat/i.test(entryTagStr)         // (6) entry tags
    );
    const type = isBug ? 'bug' : baseType;

    addHours(customer[type], durationMs, startMs, now);
    typeCount[type]++;
    if (username && !userTypeMap[username]) userTypeMap[username] = { type, space: spaceName };

    // ── Per-user per-customer monthly tracking ────────────────────────────
    if (username && startMs > 0) {
      if (!userHours[username]) userHours[username] = { type: baseType, customers: {} };
      const uc = userHours[username].customers;
      if (!uc[customer.id]) uc[customer.id] = { name: customer.name, monthly: {} };
      const d  = new Date(startMs);
      const mk = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
      uc[customer.id].monthly[mk] = rnd((uc[customer.id].monthly[mk] || 0) + durationMs / 3_600_000);
    }
  }, onProgress);

  console.log(`   ✓ ${totalEntries} time entries processed (streamed)`);

  if (unmatched > 0) {
    console.log(`   ⚠  ${unmatched} time entries had no matching customer (check folder names)`);
    // Log top unmatched names so we can spot missing aliases
    const topUnmatched = Object.entries(unmatchedNames)
      .sort(([, a], [, b]) => b - a)
      .slice(0, 30);
    if (topUnmatched.length) {
      console.log('   Top unmatched ClickUp names:');
      for (const [n, cnt] of topUnmatched) {
        console.log(`      ${String(cnt).padStart(4)}×  "${n}"`);
      }
    }
  }

  // Log classification breakdown so we can spot SA/Dev/Bug mismatches
  console.log(`\n   ── Entry classification breakdown ──`);
  console.log(`   CS: ${typeCount.cs}  SA: ${typeCount.sa}  Dev: ${typeCount.dev}  Bug: ${typeCount.bug}`);
  console.log(`   ── Users found (username → type) ──`);
  const sortedUsers = Object.entries(userTypeMap).sort(([,a],[,b]) => a.type.localeCompare(b.type));
  for (const [u, { type, space }] of sortedUsers) {
    console.log(`   ${type.padEnd(4)} | ${space.padEnd(20)} | ${u}`);
  }
  console.log(`   ── End classification ──\n`);
  } // end if (!hubspotOnly)

  // Round all hours
  customers.forEach((c) => {
    roundHours(c.cs);
    roundHours(c.sa);
    roundHours(c.dev);
    roundHours(c.bug);
  });

  // ── Samsung consolidation ──────────────────────────────────────────────────
  // Merge all "Samsung *" entities into one "Samsung" record so ARR and hours
  // are not duplicated across HQ / UK / Greece / Baltics / etc.
  const samsungGroup = customers.filter((c) => /samsung/i.test(c.name));
  if (samsungGroup.length > 1) {
    const merged = {
      id:      'samsung',
      name:    'Samsung',
      arr:     samsungGroup.reduce((s, c) => s + (c.arr  || 0), 0),
      devPipe: samsungGroup.reduce((s, c) => s + (c.devPipe || 0), 0),
      // Take the latest renewal date, earliest non-null stage, best owner
      rd:    samsungGroup.map((c) => c.rd).filter(Boolean).sort().pop() || null,
      stage: samsungGroup.find((c) => c.stage)?.stage || null,
      rank:  samsungGroup.find((c) => c.rank)?.rank   || null,
      owner: samsungGroup.find((c) => c.owner)?.owner || null,
      // Worst health wins: red > yellow > green
      flag:  samsungGroup.some((c) => c.flag === 'red')    ? 'red'
           : samsungGroup.some((c) => c.flag === 'yellow') ? 'yellow'
           : 'green',
      cs:  emptyHours(),
      sa:  emptyHours(),
      dev: emptyHours(),
      bug: emptyHours(),
    };
    // Sum already-rounded hours (they were rounded per entity above)
    for (const c of samsungGroup) {
      for (const type of ['cs', 'sa', 'dev', 'bug']) {
        merged[type].m1  += c[type].m1;
        merged[type].m3  += c[type].m3;
        merged[type].m6  += c[type].m6;
        merged[type].m12 += c[type].m12 || 0;
        for (const [mk, hrs] of Object.entries(c[type].monthly || {})) {
          merged[type].monthly[mk] = (merged[type].monthly[mk] || 0) + hrs;
        }
      }
    }
    roundHours(merged.cs); roundHours(merged.sa); roundHours(merged.dev); roundHours(merged.bug);

    // Rebuild customers array: remove individual Samsung entries, add merged
    const kept = customers.filter((c) => !/samsung/i.test(c.name));
    kept.push(merged);
    customers.length = 0;
    customers.push(...kept);

    // Rebuild the idToName lookup (used by deals matching below)
    for (const k of Object.keys(idToName)) delete idToName[k];
    for (const c of customers) idToName[c.id] = c.name;

    console.log(`   ✓ Consolidated ${samsungGroup.length} Samsung entities → single "Samsung" (ARR $${merged.arr.toLocaleString()})`);
  }

  // ── Build DEALS from HubSpot deals ─────────────────────────────────────────
  const deals = {};
  const customerIds = customers.map((c) => c.id);
  let unmatchedDeals = 0;

  for (const deal of rawDeals) {
    const p = deal.properties;
    const dealName = p.dealname || '';
    const amount   = parseFloat(p.amount) || 0;
    const currency = p.deal_currency_code || 'USD';
    const amountUSD = currency === 'USD' ? Math.round(amount) : toUSD(amount, currency);
    const closeDate = p.closedate ? p.closedate.substring(0, 10) : null;
    const isWon     = p.hs_is_closed_won === 'true';
    const stage     = isWon ? 'Closed Won' : 'Commit';
    const type      = classifyDeal(dealName);

    const customerId = matchDealToCustomer(dealName, customerIds, idToName);
    if (!customerId) { unmatchedDeals++; continue; }

    if (!deals[customerId]) deals[customerId] = [];

    const record = { n: dealName, a: amountUSD, d: closeDate, s: stage, id: deal.id, t: type };
    if (currency !== 'USD') {
      record.orig = `${currency} ${Math.round(amount).toLocaleString()}`;
    }
    deals[customerId].push(record);
  }

  if (unmatchedDeals > 0) {
    console.log(`   ⚠  ${unmatchedDeals} deals had no matching customer (check deal names)`);
  }

  console.log(`✓  Transform complete — ${customers.length} customers, ${Object.keys(deals).length} with deals`);

  return {
    customers,
    deals,
    userHours,
    lastRefreshed: new Date().toISOString(),
    meta: {
      customerCount:   customers.length,
      dealCount:       rawDeals.length,
      timeEntryCount:  totalEntries,
      unmatchedHours:  unmatched,
      unmatchedDeals:  unmatchedDeals,
      typeCount,
      // Top unmatched ClickUp names — sorted by frequency, for alias debugging
      topUnmatchedNames: Object.entries(unmatchedNames)
        .sort(([, a], [, b]) => b - a)
        .slice(0, 50)
        .map(([name, count]) => ({ name, count })),
    },
  };
}

module.exports = { refreshAll };
