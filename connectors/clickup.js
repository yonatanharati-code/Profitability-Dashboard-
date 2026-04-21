'use strict';
const https = require('https');

// ─── Workspace constants ──────────────────────────────────────────────────────
const TEAM_ID = '31065585';
const SPACES = {
  CUSTOMER_SUCCESS: { id: '54974334', name: 'Customer Success' },
  TECHOPS:          { id: '66622361', name: 'TechOps' },
};

// SA team members — usernames that classify as SA even in Customer Success space
const SA_USERNAMES = ['aleksandra', 'maher', 'ron koval', 'spitzer', 'ami '];

// ─── Classification ───────────────────────────────────────────────────────────
const BUG_TASK_RE = /\bbug\b|escalat|hotfix|incident|\bwrong\b|incorrect|\bpatch\b/i;

function classifyEntry(spaceName, username, taskName) {
  if ((spaceName || '').replace(/\s+/g, '').toLowerCase() === 'techops') {
    return BUG_TASK_RE.test(taskName || '') ? 'bug' : 'dev';
  }
  const u = (username || '').toLowerCase();
  if (SA_USERNAMES.some((k) => u.includes(k))) return 'sa';
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
 * Fetch entries for one space with NO assignee filter.
 * With an admin token, ClickUp returns ALL members' entries this way.
 */
async function fetchSpaceAllUsers(apiKey, spaceId, startMs, endMs) {
  const entries = [];
  let page = 0;
  while (true) {
    const qs = new URLSearchParams({
      start_date: String(startMs),
      end_date:   String(endMs),
      space_id:   spaceId,
      include_location_names: 'true',
      page: String(page),
    });
    const res = await withTimeout(
      cuGet(`/api/v2/team/${TEAM_ID}/time_entries?${qs}`, apiKey),
      25000
    );
    const batch = res.data ?? [];
    entries.push(...batch);
    if (batch.length < 100) break;
    page++;
  }
  return entries;
}

/**
 * Per-member fallback: fetch one member's entries for one space with a hard timeout.
 */
async function fetchOneMemberSafe(apiKey, spaceId, startMs, endMs, memberId) {
  try {
    return await withTimeout(
      fetchOneMemberSpace(apiKey, spaceId, startMs, endMs, memberId),
      20000
    );
  } catch (_) {
    return [];
  }
}

/**
 * Fetch all time entries — fast path first, slow path as fallback.
 *
 * FAST PATH (2 API calls, ~3 seconds):
 *   Fetch each space with no assignee filter. With an admin token, ClickUp
 *   returns all members' entries. Verified by checking that multiple unique
 *   user IDs appear in the results.
 *
 * SLOW PATH (per member, ~60 s for large workspaces):
 *   If the fast path only returns 1 unique user (token is restricted),
 *   fall back to fetching each member individually in batches of 5.
 */
async function fetchAllTimeEntries(apiKey, onProgress = () => {}) {
  const now          = Date.now();
  const sixMonthsAgo = now - 180 * 24 * 60 * 60 * 1000;

  // ── FAST PATH ───────────────────────────────────────────────────────────────
  onProgress({ step: 'Fetching all entries (admin mode)…', done: 0, total: 1 });
  console.log('   Trying admin fast-path (no assignee filter)…');

  const [csFast, techFast] = await Promise.all([
    fetchSpaceAllUsers(apiKey, SPACES.CUSTOMER_SUCCESS.id, sixMonthsAgo, now),
    fetchSpaceAllUsers(apiKey, SPACES.TECHOPS.id,          sixMonthsAgo, now),
  ]);

  const fastAll     = [...csFast, ...techFast];
  const uniqueUsers = new Set(fastAll.map((e) => e.user?.id).filter(Boolean));
  console.log(`   Fast path: ${fastAll.length} entries from ${uniqueUsers.size} unique users`);

  if (uniqueUsers.size > 1) {
    // Admin token returned multi-user data — we're done!
    onProgress({ step: `Got ${fastAll.length} entries from ${uniqueUsers.size} team members!`, done: 1, total: 1 });
    const seen = new Set();
    return fastAll.filter((e) => { if (!e.id || seen.has(e.id)) return false; seen.add(e.id); return true; });
  }

  // ── SLOW PATH ────────────────────────────────────────────────────────────────
  console.log('   Fast path returned 1 user — falling back to per-member fetch…');
  onProgress({ step: 'Getting workspace members…', done: 0, total: 0 });

  const members   = await fetchAllMembers(apiKey);
  const memberIds = members.map((m) => m.id).filter(Boolean);
  if (memberIds.length === 0) throw new Error('ClickUp: team has 0 members');

  const spaces = [['CS', SPACES.CUSTOMER_SUCCESS.id], ['TechOps', SPACES.TECHOPS.id]];
  const total  = memberIds.length * spaces.length;
  console.log(`   Slow path: ${memberIds.length} members × 2 spaces = ${total} fetches (batch 5)`);

  const seen       = new Set();
  const allEntries = [];
  let done = 0;

  for (const [label, spaceId] of spaces) {
    // Process members in batches of 5 with a hard 20s per-request timeout
    for (let i = 0; i < memberIds.length; i += 5) {
      const batch = memberIds.slice(i, i + 5);
      const results = await Promise.all(
        batch.map((id) => fetchOneMemberSafe(apiKey, spaceId, sixMonthsAgo, now, id))
      );
      for (const entries of results) {
        done++;
        for (const e of entries) {
          if (e.id && !seen.has(e.id)) { seen.add(e.id); allEntries.push(e); }
        }
      }
      onProgress({ step: `${label}: ${Math.min(done, total)}/${total}`, done: Math.min(done, total), total });
      await sleep(300);
    }
  }

  onProgress({ step: `Processing ${allEntries.length} entries…`, done: total, total });
  return allEntries;
}

module.exports = { fetchAllTimeEntries, classifyEntry, SPACES, SA_USERNAMES };
