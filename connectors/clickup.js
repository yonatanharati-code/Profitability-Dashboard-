'use strict';
const https = require('https');

// ─── Workspace constants (discovered from live workspace) ─────────────────────
const TEAM_ID = '31065585';
const SPACES = {
  CUSTOMER_SUCCESS: { id: '54974334', name: 'Customer Success' },
  TECHOPS:          { id: '66622361', name: 'TechOps' },
};

// SA team members — usernames that classify as SA even in Customer Success space
const SA_USERNAMES = ['aleksandra', 'maher', 'ron koval', 'spitzer', 'ami '];

// ─── Classification ───────────────────────────────────────────────────────────
/**
 * Keywords in a ClickUp task name that signal a bug-fix / escalation entry
 * (used when the '🐞 Bugs Found' custom field is not available via the API).
 */
const BUG_TASK_RE = /\bbug\b|escalat|hotfix|incident|\bwrong\b|incorrect|\bpatch\b/i;

/**
 * Classify a time entry as cs / sa / dev / bug.
 * Rules:
 *   - TechOps space + bug task name → bug (escalation / bug-fix)
 *   - TechOps space, otherwise → dev (new development)
 *   - SA team member in any space → sa
 *   - everything else → cs
 */
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
            if (json.err) reject(new Error(`ClickUp: ${json.err}`));
            else resolve(json);
          } catch (e) {
            reject(new Error(`ClickUp non-JSON (${res.statusCode}): ${raw.slice(0, 200)}`));
          }
        });
      }
    );
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(new Error('ClickUp request timed out')); });
    req.end();
  });
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/** Sleep for ms milliseconds (used to back off on rate-limit hits) */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Fetch all workspace members.
 * GET /api/v2/team returns all authorized workspaces; each workspace object
 * contains a `members` array with full user records.
 */
async function fetchAllMembers(apiKey) {
  const res = await cuGet(`/api/v2/team`, apiKey);
  const teams = res.teams ?? [];
  const team  = teams.find((t) => String(t.id) === String(TEAM_ID)) ?? teams[0];
  if (!team) throw new Error('ClickUp: no team found in /api/v2/team response');
  return (team.members ?? []).map((m) => m.user).filter(Boolean);
}

// ─── Time entry fetching ──────────────────────────────────────────────────────

/**
 * Fetch all time entries for a given space + list of assignees within a date window.
 * Passes all assignee IDs in a single request using assignee[] array params.
 * Falls back to sequential per-member fetching if the batch call returns nothing.
 */
async function fetchSpaceEntries(apiKey, spaceId, startMs, endMs, memberIds) {
  const entries = [];
  let page = 0;

  while (true) {
    const qs = new URLSearchParams({
      start_date: String(startMs),
      end_date:   String(endMs),
      space_id:   spaceId,
      include_location_names: 'true',
      page:       String(page),
    });
    // Pass all member IDs — ClickUp supports assignee[] array
    for (const id of memberIds) qs.append('assignee[]', String(id));

    const res = await cuGet(
      `/api/v2/team/${TEAM_ID}/time_entries?${qs}`,
      apiKey
    );

    const batch = res.data ?? [];
    entries.push(...batch);
    if (batch.length < 100) break;
    page++;
  }

  return entries;
}

/**
 * Sequential fallback: fetch entries one member at a time with a small delay
 * to avoid rate limits. Used when the batched assignee[] call returns 0 entries.
 */
async function fetchSpaceEntriesSequential(apiKey, spaceId, startMs, endMs, memberIds) {
  const entries = [];
  const seen    = new Set();

  for (const memberId of memberIds) {
    let page = 0;
    while (true) {
      const qs = new URLSearchParams({
        start_date: String(startMs),
        end_date:   String(endMs),
        space_id:   spaceId,
        assignee:   String(memberId),
        include_location_names: 'true',
        page:       String(page),
      });

      let res;
      try {
        res = await cuGet(`/api/v2/team/${TEAM_ID}/time_entries?${qs}`, apiKey);
      } catch (e) {
        // On rate limit, wait 2 s and retry once
        if (e.message.includes('rate') || e.message.includes('429')) {
          await sleep(2000);
          res = await cuGet(`/api/v2/team/${TEAM_ID}/time_entries?${qs}`, apiKey);
        } else {
          throw e;
        }
      }

      const batch = res.data ?? [];
      for (const e of batch) {
        if (e.id && !seen.has(e.id)) { seen.add(e.id); entries.push(e); }
      }
      if (batch.length < 100) break;
      page++;
    }
    // Small delay between members to stay under rate limits
    await sleep(200);
  }

  return entries;
}

/**
 * Fetch all time entries for the last 6 months from both Customer Success
 * and TechOps spaces, across ALL workspace members.
 *
 * Strategy:
 *   1. Get all workspace members.
 *   2. Try a single batched request per space (assignee[] array) — fast.
 *   3. If batched call returns 0 entries, fall back to sequential per-member
 *      fetching — slower but guaranteed to work.
 */
async function fetchAllTimeEntries(apiKey) {
  const now          = Date.now();
  const sixMonthsAgo = now - 180 * 24 * 60 * 60 * 1000;

  // Step 1: get all workspace members
  const members   = await fetchAllMembers(apiKey);
  const memberIds = members.map((m) => m.id).filter(Boolean);
  console.log(`   Found ${memberIds.length} workspace members`);

  // Step 2: try batched fetch first (fast — 2 API calls total)
  console.log('   Trying batched fetch (assignee[] per space)…');
  const [csBatch, techBatch] = await Promise.all([
    fetchSpaceEntries(apiKey, SPACES.CUSTOMER_SUCCESS.id, sixMonthsAgo, now, memberIds),
    fetchSpaceEntries(apiKey, SPACES.TECHOPS.id,          sixMonthsAgo, now, memberIds),
  ]);

  const batchTotal = csBatch.length + techBatch.length;
  console.log(`   Batched result: ${csBatch.length} CS + ${techBatch.length} TechOps entries`);

  if (batchTotal > 0) {
    // Deduplicate and return
    const seen = new Set();
    return [...csBatch, ...techBatch].filter((e) => {
      if (!e.id || seen.has(e.id)) return false;
      seen.add(e.id); return true;
    });
  }

  // Step 3: batched returned nothing — fall back to sequential
  console.log('   Batched fetch returned 0 — falling back to sequential per-member fetch…');
  const [csSeq, techSeq] = await Promise.all([
    fetchSpaceEntriesSequential(apiKey, SPACES.CUSTOMER_SUCCESS.id, sixMonthsAgo, now, memberIds),
    fetchSpaceEntriesSequential(apiKey, SPACES.TECHOPS.id,          sixMonthsAgo, now, memberIds),
  ]);

  console.log(`   Sequential result: ${csSeq.length} CS + ${techSeq.length} TechOps entries`);

  // Deduplicate
  const seen2 = new Set();
  return [...csSeq, ...techSeq].filter((e) => {
    if (!e.id || seen2.has(e.id)) return false;
    seen2.add(e.id); return true;
  });
}

module.exports = { fetchAllTimeEntries, classifyEntry, SPACES, SA_USERNAMES };
