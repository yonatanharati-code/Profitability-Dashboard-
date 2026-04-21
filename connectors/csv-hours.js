'use strict';

// ─── SA team usernames (same as clickup.js) ───────────────────────────────────
const SA_USERNAMES = ['aleksandra', 'maher', 'ron koval', 'spitzer', 'ami'];

// ─── Bug / escalation detection ───────────────────────────────────────────────
/** Matches task names that indicate a bug-fix or escalation (not new dev). */
const BUG_TASK_RE = /\bbug\b|escalat|hotfix|incident|\bwrong\b|incorrect|\bpatch\b/i;

/**
 * Classify a CSV row as cs / sa / dev / bug.
 *
 * @param {string} spaceName  - ClickUp space name
 * @param {string} username   - ClickUp username
 * @param {string} taskName   - individual task name (for keyword detection)
 * @param {string} bugsFound  - value of the '🐞 Bugs Found' custom field
 */
function classifyRow(spaceName, username, taskName, bugsFound, billable, toPriority) {
  // Normalise space name: strip whitespace and compare case-insensitively
  // so "Tech Ops", "techops", "TECHOPS" all match regardless of export format.
  if ((spaceName || '').replace(/\s+/g, '').toLowerCase() === 'techops') {
    const bf = (bugsFound || '').trim();

    // "Billable" col 47 (human-readable label): "No - Performance / Bug" = bug work
    const bill = (billable || '').toLowerCase();
    const billableIsBug = bill.includes('performance') || bill.includes('bug');

    // "⚠️ TO Priority" explicitly set to "Bug" for bug tasks
    const priorityIsBug = (toPriority || '').trim().toLowerCase() === 'bug';

    const isBug = (bf !== '' && bf !== '0') || billableIsBug || priorityIsBug || BUG_TASK_RE.test(taskName || '');
    return isBug ? 'bug' : 'dev';
  }
  const u = (username || '').toLowerCase();
  if (SA_USERNAMES.some((k) => u.includes(k))) return 'sa';
  return 'cs';
}

// ─── CSV parser (handles quoted fields) ──────────────────────────────────────
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

/**
 * ClickUp's "Time Entries" CSV export writes date/time text fields (e.g. "Start Text",
 * "Stop Text") WITHOUT surrounding double-quotes.  The value "12/17/2025, 11:13:03 AM IST"
 * gets split at the comma into two fields, shifting every subsequent column index by +1.
 * Five such fields appear before the "Customer" column (cols 7, 9, 22, 24, 38), causing
 * Space Name, Bugs Found, and Customer to be read from the wrong indices.
 *
 * fixDateSplits() merges them back: ["12/17/2025", " 11:13:03 AM IST"] → one field.
 */
function fixDateSplits(fields) {
  const dateOnlyRe = /^\d{1,2}\/\d{1,2}\/\d{4}$/;
  const timePart   = /^\s*\d{1,2}:\d{2}:\d{2}/;
  const result = [];
  for (let i = 0; i < fields.length; i++) {
    if (dateOnlyRe.test(fields[i]) && i + 1 < fields.length && timePart.test(fields[i + 1])) {
      result.push(fields[i] + ', ' + fields[i + 1].trim());
      i++;
    } else {
      result.push(fields[i]);
    }
  }
  return result;
}

// ─── Detect CSV format from header ───────────────────────────────────────────
/**
 * Two ClickUp export formats:
 *
 * FORMAT A — "Time Entries" export (preferred):
 *   col 1:  Username
 *   col 6:  Start       (exact entry start ms)
 *   col 10: Time Tracked (entry duration ms)
 *   col 13: Space Name
 *   col 48: Customer
 *   Detection: header col 2 === "Time Entry ID"
 *
 * FORMAT B — "Tasks" export (fallback):
 *   col 1:  Username
 *   col 4:  Space Name
 *   col 15: Start Date  (task start ms, often empty)
 *   col 27: User Period Time Spent (ms)
 *   col 38: Customer
 *   Detection: header col 2 === "Task Count"
 */
function detectFormat(headerCols) {
  const col2 = (headerCols[2] || '').replace(/"/g, '').trim();
  if (col2 === 'Time Entry ID') return 'A';
  if (col2 === 'Task Count')    return 'B';
  return 'A'; // best guess
}

// ─── Hours bucket helpers ─────────────────────────────────────────────────────
const MS1 = 30  * 24 * 60 * 60 * 1000;
const MS3 = 90  * 24 * 60 * 60 * 1000;
const MS6 = 180 * 24 * 60 * 60 * 1000;
const MS_MONTH = 30 * 24 * 60 * 60 * 1000;

function emptyBucket() { return { m1: 0, m3: 0, m6: 0, monthly: {} }; }

/** Add hours with an exact known timestamp. */
function addToBucket(bucket, durationMs, startMs, now) {
  const hours = durationMs / 3_600_000;
  bucket.m6 += hours;
  if (startMs >= now - MS3) bucket.m3 += hours;
  if (startMs >= now - MS1) bucket.m1 += hours;
  const d  = new Date(startMs);
  const mk = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
  bucket.monthly[mk] = (bucket.monthly[mk] || 0) + hours;
}

/**
 * Distribute hours evenly across 6 months when no timestamp is known.
 * Used only for Format B rows that lack a start date.
 */
function addDistributedHours(bucket, durationMs, now) {
  const totalHours = durationMs / 3_600_000;
  bucket.m6 += totalHours;
  bucket.m3 += totalHours / 2;
  bucket.m1 += totalHours / 6;
  const share = totalHours / 6;
  for (let i = 0; i < 6; i++) {
    const d  = new Date(now - i * MS_MONTH);
    const mk = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    bucket.monthly[mk] = (bucket.monthly[mk] || 0) + share;
  }
}

function roundBucket(b) {
  const r = (v) => Math.round(v * 10) / 10;
  b.m1 = r(b.m1); b.m3 = r(b.m3); b.m6 = r(b.m6);
  for (const k of Object.keys(b.monthly)) b.monthly[k] = r(b.monthly[k]);
}

// ─── Main export ──────────────────────────────────────────────────────────────
/**
 * Parse a ClickUp CSV export (time entries or tasks) and return per-customer
 * hour buckets, correctly distributed by month.
 *
 * Returns: { hours: { [customerName]: {cs,sa,dev} }, parsed, skipped, format }
 */
function parseCsvHours(csvText) {
  const lines  = csvText.split('\n');
  const now    = Date.now();
  const result = {};
  let parsed = 0, skipped = 0;

  // Detect format from header
  const headerCols = parseLine(lines[0] || '');
  const format     = detectFormat(headerCols);

  console.log(`   CSV format detected: ${format === 'A' ? 'Time Entries (exact timestamps)' : 'Tasks (period totals)'}`);

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    // fixDateSplits merges unquoted date/time fields that ClickUp exports without quotes,
    // restoring correct column alignment for Space Name (13), Bugs Found (42), Customer (48).
    const cols = fixDateSplits(parseLine(line));

    let username, spaceName, customer, durationMs, startMs, taskName, bugsFound, billable, toPriority;

    if (format === 'A') {
      // ── Format A: Time Entries ──
      username   = cols[1]  || '';
      startMs    = parseInt(cols[6])  || 0;   // exact entry start
      durationMs = parseInt(cols[10]) || 0;   // this entry's duration
      spaceName  = cols[13] || '';
      taskName   = cols[19] || '';            // individual task name (bug detection)
      bugsFound  = cols[42] || '';            // 🐞 Bugs Found custom field
      billable   = cols[47] || '';            // Billable (col 47, human-readable: "No - Performance / Bug")
      customer   = cols[48] || '';            // Customer (col 48 in Format A, after date/time fix)
      toPriority = cols[50] || '';            // ⚠️ TO Priority ("Bug" = bug task)
    } else {
      // ── Format B: Tasks ──
      username   = cols[1]  || '';
      spaceName  = cols[4]  || '';
      taskName   = cols[11] || '';            // Task Name
      startMs    = parseInt(cols[15]) || 0;   // task start date (often empty)
      durationMs = parseInt(cols[27]) || 0;   // period time spent
      bugsFound  = cols[32] || '';            // 🐞 Bugs Found custom field
      customer   = cols[38] || '';
      billable   = '';                        // Format B doesn't have Billable label column
      toPriority = '';
    }

    if (!customer || durationMs <= 0) { skipped++; continue; }

    const type = classifyRow(spaceName, username, taskName, bugsFound, billable, toPriority);
    if (!result[customer]) {
      result[customer] = { cs: emptyBucket(), sa: emptyBucket(), dev: emptyBucket(), bug: emptyBucket() };
    }

    if (startMs > 0 && startMs >= now - MS6) {
      // Known timestamp within 6-month window
      addToBucket(result[customer][type], durationMs, startMs, now);
      parsed++;
    } else if (startMs > 0 && startMs < now - MS6) {
      // Too old — skip
      skipped++;
    } else {
      // No timestamp (Format B without start date) — distribute evenly
      addDistributedHours(result[customer][type], durationMs, now);
      parsed++;
    }
  }

  // Round all buckets
  for (const buckets of Object.values(result)) {
    roundBucket(buckets.cs);
    roundBucket(buckets.sa);
    roundBucket(buckets.dev);
    if (buckets.bug) roundBucket(buckets.bug);
  }

  return { hours: result, parsed, skipped, format };
}

module.exports = { parseCsvHours, classifyRow };
