'use strict';

/**
 * ClickUp "Time Entries" CSV parser — v2
 *
 * Key improvements over v1:
 *  • Uses header-row column names instead of hardcoded indices — survives ClickUp
 *    adding/removing custom columns without breaking.
 *  • Classifies by User ID → USER_TYPES map (same as the API path) instead of
 *    fuzzy username matching.  Rows from users not in USER_TYPES are skipped.
 *  • Bug detection uses Item Type = "Bug" AND/OR keyword regex on task name.
 *    "No - Performance / Bug" in the Billable label also counts as bug.
 */

const { USER_TYPES, BUG_TASK_RE, BUG_LIST_RE } = require('./clickup');

// ─── Bug detection helpers ────────────────────────────────────────────────────
const BUG_BILLABLE_RE  = /performance|\/\s*bug/i;  // "No - Performance / Bug"

function isBugEntry(baseType, itemType, billableLabel, taskName, listName) {
  if (baseType !== 'dev' && baseType !== 'sa') return false;
  return (
    (itemType     || '').toLowerCase() === 'bug' ||
    BUG_BILLABLE_RE.test(billableLabel || '')    ||
    BUG_TASK_RE.test(taskName || '')             ||
    BUG_LIST_RE.test(listName || '')
  );
}

// ─── CSV parser (handles quoted fields with embedded commas) ──────────────────
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

// ─── Hours bucket helpers ─────────────────────────────────────────────────────
const MS1 = 30  * 24 * 60 * 60 * 1000;
const MS3 = 90  * 24 * 60 * 60 * 1000;
const MS6 = 180 * 24 * 60 * 60 * 1000;

function emptyBucket() { return { m1: 0, m3: 0, m6: 0, monthly: {} }; }

function addToBucket(bucket, durationMs, startMs, now) {
  const hours = durationMs / 3_600_000;
  bucket.m6 += hours;
  if (startMs >= now - MS3) bucket.m3 += hours;
  if (startMs >= now - MS1) bucket.m1 += hours;
  const d  = new Date(startMs);
  const mk = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
  bucket.monthly[mk] = (bucket.monthly[mk] || 0) + hours;
}

function roundBucket(b) {
  const r = (v) => Math.round(v * 10) / 10;
  b.m1 = r(b.m1); b.m3 = r(b.m3); b.m6 = r(b.m6);
  for (const k of Object.keys(b.monthly)) b.monthly[k] = r(b.monthly[k]);
}

// ─── Customers to skip (internal/generic ClickUp entries) ────────────────────
const SKIP_CUSTOMERS = new Set([
  'all', 'to', 'tech ops', 'techops', 'none', '', 'internal',
  'general meetings', 'sprint folder', 'customer management',
]);

// ─── Main export ──────────────────────────────────────────────────────────────
/**
 * Parse a ClickUp "Time Entries" CSV export and return per-customer hour buckets.
 *
 * Returns: { hours: { [customerName]: { cs, sa, dev, bug } }, parsed, skipped, format }
 */
function parseCsvHours(csvText) {
  const lines  = csvText.split('\n');
  const now    = Date.now();
  const result = {};
  let parsed = 0, skipped = 0, skippedUser = 0;

  if (!lines[0]) return { hours: result, parsed, skipped, format: 'unknown' };

  // ── Build column index map from header row ────────────────────────────────
  const headerCols = parseLine(lines[0]);
  const idx = {};
  headerCols.forEach((h, i) => { idx[h.trim()] = i; });

  // Required columns
  const COL = {
    userId:        idx['User ID']           ?? 0,
    username:      idx['Username']          ?? 1,
    start:         idx['Start']             ?? 6,
    timeTracked:   idx['Time Tracked']      ?? 10,
    spaceName:     idx['Space Name']        ?? 13,
    listName:      idx['List Name']         ?? 17,
    taskName:      idx['Task Name']         ?? 19,
    bugsFound:     idx['🐞 Bugs Found']     ?? -1,
    billable:      idx['Billable']          ?? -1,   // task-level "No - Performance / Bug"
    customer:      idx['Customer']          ?? -1,
    toPriority:    idx['⚠️ TO Priority']    ?? -1,
    itemType:      idx['Item Type']         ?? -1,
  };

  // Detect format
  const format = headerCols[2]?.replace(/"/g,'').trim() === 'Time Entry ID' ? 'A' : 'B';
  console.log(`   CSV format: ${format === 'A' ? 'Time Entries (exact timestamps)' : 'Tasks (period totals)'}`);
  console.log(`   Key columns: User ID=${COL.userId}, Customer=${COL.customer}, ItemType=${COL.itemType}, Start=${COL.start}`);

  if (COL.customer === -1) {
    console.warn('   ⚠ "Customer" column not found in CSV — falling back to list/task name matching');
  }

  // Per-user per-customer monthly hours — powers the "Hours by Person" chart
  const userHours = {};

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const cols = parseLine(line);

    const userId     = parseInt(cols[COL.userId]) || 0;
    const startMs    = parseInt(cols[COL.start])  || 0;
    const durationMs = parseInt(cols[COL.timeTracked]) || 0;

    if (durationMs <= 0) { skipped++; continue; }

    // Filter to last 6 months
    if (startMs > 0 && startMs < now - MS6) { skipped++; continue; }

    // Classify by user ID
    const baseType = USER_TYPES[userId];
    if (!baseType) { skippedUser++; continue; }

    // Get customer name from the dedicated Customer custom field
    const customer = COL.customer >= 0
      ? (cols[COL.customer] || '').trim()
      : (cols[COL.listName] || cols[COL.taskName] || '').trim(); // fallback

    if (!customer || SKIP_CUSTOMERS.has(customer.toLowerCase())) { skipped++; continue; }

    // Bug detection
    const itemType     = COL.itemType  >= 0 ? (cols[COL.itemType]  || '') : '';
    const billable     = COL.billable  >= 0 ? (cols[COL.billable]  || '') : '';
    const taskName     = (cols[COL.taskName] || '');
    const listName     = (cols[COL.listName] || '');
    const toPriority   = COL.toPriority >= 0 ? (cols[COL.toPriority] || '') : '';

    // "Bugs Found" (🐞) counts bugs found during QA — NOT whether the task is a bug fix.
    // Bug detection: Item Type = Bug, OR Billable label = "No - Performance / Bug",
    // OR TO Priority explicitly = "Bug", OR task/list name keyword match.
    const isBug = isBugEntry(baseType, itemType, billable, taskName, listName) ||
                  (toPriority || '').toLowerCase() === 'bug';

    const type = isBug ? 'bug' : baseType;

    if (!result[customer]) {
      result[customer] = {
        cs: emptyBucket(), sa: emptyBucket(), dev: emptyBucket(), bug: emptyBucket(),
      };
    }

    if (startMs > 0) {
      addToBucket(result[customer][type], durationMs, startMs, now);
    } else {
      // No timestamp — add to m6 only (Format B fallback)
      result[customer][type].m6 += durationMs / 3_600_000;
    }
    parsed++;

    // ── Per-user tracking ─────────────────────────────────────────────────────
    const username = (cols[COL.username] || '').trim() || String(userId);
    if (username && startMs > 0) {
      if (!userHours[username]) userHours[username] = { type: baseType, customers: {} };
      const uc = userHours[username].customers;
      if (!uc[customer]) uc[customer] = { name: customer, monthly: {} };
      const d  = new Date(startMs);
      const mk = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
      uc[customer].monthly[mk] = Math.round(((uc[customer].monthly[mk] || 0) + durationMs / 3_600_000) * 10) / 10;
    }
  }

  // Round all buckets
  for (const buckets of Object.values(result)) {
    roundBucket(buckets.cs);
    roundBucket(buckets.sa);
    roundBucket(buckets.dev);
    roundBucket(buckets.bug);
  }

  console.log(`   CSV parsed: ${parsed} rows, ${skipped} skipped (outside range/no customer), ${skippedUser} skipped (user not in USER_TYPES), ${Object.keys(userHours).length} people tracked`);
  return { hours: result, userHours, parsed, skipped, format };
}

module.exports = { parseCsvHours };
