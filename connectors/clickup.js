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
 * Fetch all time entries for a given space within a date window.
 * ClickUp returns max 100 per page; we page through all pages.
 */
async function fetchSpaceEntries(apiKey, spaceId, startMs, endMs) {
  const entries = [];
  let page = 0;

  while (true) {
    const qs = new URLSearchParams({
      start_date: String(startMs),
      end_date: String(endMs),
      space_id: spaceId,
      include_location_names: 'true',
      page: String(page),
    });

    const res = await cuGet(
      `/api/v2/team/${TEAM_ID}/time_entries?${qs}`,
      apiKey
    );

    const batch = res.data ?? [];
    entries.push(...batch);

    // ClickUp paginates by page; if fewer than 100 returned we've reached the end
    if (batch.length < 100) break;
    page++;
  }

  return entries;
}

/**
 * Fetch all time entries for the last 6 months from both Customer Success
 * and TechOps spaces, then return them merged.
 */
async function fetchAllTimeEntries(apiKey) {
  const now = Date.now();
  const sixMonthsAgo = now - 180 * 24 * 60 * 60 * 1000;

  const [csEntries, techEntries] = await Promise.all([
    fetchSpaceEntries(apiKey, SPACES.CUSTOMER_SUCCESS.id, sixMonthsAgo, now),
    fetchSpaceEntries(apiKey, SPACES.TECHOPS.id, sixMonthsAgo, now),
  ]);

  return [...csEntries, ...techEntries];
}

module.exports = { fetchAllTimeEntries, classifyEntry, SPACES, SA_USERNAMES };
