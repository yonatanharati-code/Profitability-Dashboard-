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
 *
 * @param {string} spaceName  - ClickUp space name
 * @param {string} username   - ClickUp username / email
 * @param {string} [taskName] - individual task name (optional, used for bug detection)
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
    req.end();
  });
}

// ─── Time entry fetching ──────────────────────────────────────────────────────

/**
 * Fetch all workspace members so we can pull every member's time entries.
 * Without an explicit assignee filter, ClickUp only returns the token owner's
 * entries even when the token has admin permissions.
 *
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

/**
 * Fetch all time entries for a single member in a given space within a date window.
 * ClickUp returns max 100 per page; we page through all pages.
 */
async function fetchMemberSpaceEntries(apiKey, spaceId, startMs, endMs, assigneeId) {
  const entries = [];
  let page = 0;

  while (true) {
    const qs = new URLSearchParams({
      start_date: String(startMs),
      end_date: String(endMs),
      space_id: spaceId,
      assignee: String(assigneeId),
      include_location_names: 'true',
      page: String(page),
    });

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
 * Fetch all time entries for the last 6 months from both Customer Success
 * and TechOps spaces, across ALL workspace members.
 *
 * Strategy: get all members first, then fetch each member's entries for each
 * space in parallel. Deduplicates by entry ID at the end.
 */
async function fetchAllTimeEntries(apiKey) {
  const now = Date.now();
  const sixMonthsAgo = now - 180 * 24 * 60 * 60 * 1000;

  // Step 1: get all workspace members
  const members = await fetchAllMembers(apiKey);
  const memberIds = members.map((m) => m.id).filter(Boolean);
  console.log(`   Found ${memberIds.length} workspace members — fetching entries for each…`);

  // Step 2: fetch CS + TechOps entries for every member in parallel
  const promises = [];
  for (const memberId of memberIds) {
    promises.push(fetchMemberSpaceEntries(apiKey, SPACES.CUSTOMER_SUCCESS.id, sixMonthsAgo, now, memberId));
    promises.push(fetchMemberSpaceEntries(apiKey, SPACES.TECHOPS.id,          sixMonthsAgo, now, memberId));
  }

  const results = await Promise.all(promises);
  const allEntries = results.flat();

  // Step 3: deduplicate by entry ID (same entry can appear under different assignees)
  const seen = new Set();
  return allEntries.filter((e) => {
    if (!e.id || seen.has(e.id)) return false;
    seen.add(e.id);
    return true;
  });
}

module.exports = { fetchAllTimeEntries, classifyEntry, SPACES, SA_USERNAMES };
