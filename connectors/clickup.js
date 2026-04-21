'use strict';
const https = require('https');

// ─── Workspace constants ──────────────────────────────────────────────────────
const TEAM_ID = '31065585';
const SPACES = {
  CUSTOMER_SUCCESS: { id: '54974334', name: 'Customer Success' },
  TECHOPS:          { id: '66622361', name: 'TechOps' },
};

// SA team members — usernames that classify as SA even in Customer Success space
const SA_USERNAMES = ['aleksandra', 'maher', 'ron koval', 'spitzer', 'ami'];

// ─── Classification ───────────────────────────────────────────────────────────
// Match bug/escalation keywords in task titles
const BUG_TASK_RE = /\bbug\b|escalat|hotfix|incident|\bwrong\b|incorrect|\bpatch\b/i;
// Match bug/escalation keywords in LIST names (e.g. a list called "Bugs" or "Escalations")
const BUG_LIST_RE = /\bbug|escalat|hotfix|incident|\bfix\b|patch/i;

/**
 * Classify a time entry as cs / sa / dev / bug.
 *
 * @param {string} spaceName  ClickUp space name
 * @param {string} username   ClickUp username or email
 * @param {string} taskName   Individual task title (used for bug keyword detection)
 * @param {string} listName   ClickUp list name — lists named "Bugs"/"Escalations" → bug
 */
function classifyEntry(spaceName, username, taskName, listName) {
  if ((spaceName || '').replace(/\s+/g, '').toLowerCase() === 'techops') {
    const isBug = BUG_TASK_RE.test(taskName || '') || BUG_LIST_RE.test(listName || '');
    return isBug ? 'bug' : 'dev';
  }
  const u = (username || '').toLowerCase();
  if (SA_USERNAMES.some((k) => u.includes(k.trim()))) return 'sa';
  return 'cs';
}

// ─── HTTP helper ─────────────────────────────────────────────────────────────
function cuGet(path, apiKey) {
  return new Promise((resolve, reject) => {
    const url = new URL('https://api.clickup.com' + path);
    const req = https.request(
      {
        hostname: url.hostname,
        path: url.pathname + url.search,
        method: 'GET',
        headers: { Authorization: apiKey },
      },
      (res) => {
        let raw = '';
        res.on('data', (c) => (raw += c));
        res.on('end', () => {
          try {
            const json = JSON.parse(raw);
            if (json.err) reject(new Error(`ClickUp API error: ${JSON.stringify(json.err)}`));
            else resolve(json);
          } catch (e) {
            reject(new Error(`ClickUp non-JSON (HTTP ${res.statusCode}): ${raw.slice(0, 300)}`));
          }
        });
      }
    );
    req.on('error', (e) => reject(new Error(`ClickUp network error: ${e.message}`)));
    req.setTimeout(30000, () => { req.destroy(new Error('ClickUp request timed out after 30s')); });
    req.end();
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Wrap any promise with a hard timeout so it never hangs forever */
function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`Timed out after ${ms}ms`)), ms)),
  ]);
}

// ─── Members ──────────────────────────────────────────────────────────────────
async function fetchAllMembers(apiKey) {
  const res = await cuGet(`/api/v2/team`, apiKey);
  const teams = res.teams ?? [];
  const team  = teams.find((t) => String(t.id) === String(TEAM_ID)) ?? teams[0];
  if (!team) throw new Error(`ClickUp: no team found. Response keys: ${Object.keys(res).join(', ')}`);
  const users = (team.members ?? []).map((m) => m.user).filter(Boolean);
  console.log(`   Members found: ${users.map((u) => u.username || u.email || u.id).join(', ')}`);
  return users;
}

// ─── Time entry fetching ──────────────────────────────────────────────────────

/**
 * Fetch all time entries for one member in one space.
 */
async function fetchOneMemberSpace(apiKey, spaceId, startMs, endMs, memberId) {
  const entries = [];
  let page = 0;
  while (true) {
    const qs = new URLSearchParams({
      start_date: String(startMs),
      end_date:   String(endMs),
      space_id:   spaceId,
      assignee:   String(memberId),
      include_location_names: 'true',
      page: String(page),
    });
    const res = await cuGet(`/api/v2/team/${TEAM_ID}/time_entries?${qs}`, apiKey);
    const batch = res.data ?? [];
    entries.push(...batch);
    if (batch.length < 100) break;
    page++;
  }
  return entries;
}

/**
 * Fetch all time entries for the last 6 months from CS + TechOps spaces,
 * for every workspace member (sequential with small delay to avoid rate limits).
 * Calls onProgress({ step, done, total }) after each member so the UI can show
 * a live progress bar.
 */
/**
 * Stream entries for one space page-by-page, calling onEntry for each one.
 * Never accumulates a large array — memory stays flat regardless of total count.
 * Returns { count, uniqueUsers } for diagnostics.
 */
async function streamSpaceEntries(apiKey, spaceId, startMs, endMs, onEntry, assigneeId = null) {
  let page = 0;
  let count = 0;
  const uniqueUsers = new Set();

  while (true) {
    const qs = new URLSearchParams({
      start_date: String(startMs),
      end_date:   String(endMs),
      space_id:   spaceId,
      include_location_names: 'true',
      page: String(page),
    });
    if (assigneeId) qs.set('assignee', String(assigneeId));

    const res = await withTimeout(
      cuGet(`/api/v2/team/${TEAM_ID}/time_entries?${qs}`, apiKey),
      25000
    );
    const batch = res.data ?? [];
    for (const entry of batch) {
      if (entry.user?.id) uniqueUsers.add(entry.user.id);
      onEntry(entry);
      count++;
    }
    if (batch.length < 100) break;
    page++;
  }
  return { count, uniqueUsers };
}

/**
 * Stream all time entries to a callback — never accumulates a large array.
 *
 * FAST PATH (2 API calls): no assignee filter. With an admin token, ClickUp
 *   returns all members' entries. Verified by checking uniqueUsers > 1.
 *
 * SLOW PATH: per-member sequential with 20s per-request hard timeout.
 *
 * @param {string}   apiKey
 * @param {function} onEntry     - called for every raw entry object
 * @param {function} onProgress  - called with { step, done, total }
 */
async function streamAllTimeEntries(apiKey, onEntry, onProgress = () => {}) {
  const now          = Date.now();
  const sixMonthsAgo = now - 180 * 24 * 60 * 60 * 1000;

  // ── FAST PATH ────────────────────────────────────────────────────────────────
  onProgress({ step: 'Fetching entries (admin mode)…', done: 0, total: 1 });
  console.log('   Trying admin fast-path (no assignee filter)…');

  const allUsers = new Set();
  let fastCount  = 0;

  // Buffer fast-path entries temporarily just long enough to check uniqueUsers
  const fastBuffer = [];
  for (const spaceId of [SPACES.CUSTOMER_SUCCESS.id, SPACES.TECHOPS.id]) {
    const { count, uniqueUsers } = await streamSpaceEntries(
      apiKey, spaceId, sixMonthsAgo, now,
      (e) => { fastBuffer.push(e); },
    );
    fastCount += count;
    uniqueUsers.forEach((u) => allUsers.add(u));
  }

  console.log(`   Fast path: ${fastCount} entries from ${allUsers.size} unique users`);

  if (allUsers.size > 1) {
    // Admin token works — flush buffer through onEntry and we're done
    const seen = new Set();
    for (const e of fastBuffer) {
      if (e.id && !seen.has(e.id)) { seen.add(e.id); onEntry(e); }
    }
    onProgress({ step: `Got ${fastCount} entries from ${allUsers.size} members ✓`, done: 1, total: 1 });
    console.log(`   ✓ Fast path complete`);
    return;
  }
  // Free the buffer immediately
  fastBuffer.length = 0;

  // ── SLOW PATH ────────────────────────────────────────────────────────────────
  console.log('   Fast path = 1 user — per-member fallback…');
  onProgress({ step: 'Getting workspace members…', done: 0, total: 0 });

  const members   = await fetchAllMembers(apiKey);
  const memberIds = members.map((m) => m.id).filter(Boolean);
  if (memberIds.length === 0) throw new Error('ClickUp: team has 0 members');

  const spaces = [['CS', SPACES.CUSTOMER_SUCCESS.id], ['TechOps', SPACES.TECHOPS.id]];
  const total  = memberIds.length * spaces.length;
  console.log(`   Slow path: ${memberIds.length} members × 2 spaces = ${total}`);

  const seen = new Set();
  let done   = 0;

  for (const [label, spaceId] of spaces) {
    for (const memberId of memberIds) {
      done++;
      onProgress({ step: `${label}: ${done}/${total}`, done, total });
      try {
        await withTimeout(
          streamSpaceEntries(apiKey, spaceId, sixMonthsAgo, now, (e) => {
            if (e.id && !seen.has(e.id)) { seen.add(e.id); onEntry(e); }
          }, memberId),
          20000
        );
      } catch (err) {
        console.warn(`   ⚠  ${label} member ${memberId}: ${err.message}`);
      }
      await sleep(200);
    }
  }
}

// Keep old name as alias so nothing else breaks
async function fetchAllTimeEntries(apiKey, onProgress = () => {}) {
  const entries = [];
  await streamAllTimeEntries(apiKey, (e) => entries.push(e), onProgress);
  return entries;
}

module.exports = { streamAllTimeEntries, fetchAllTimeEntries, classifyEntry, SPACES, SA_USERNAMES };
