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
 */
async function fetchAllTimeEntries(apiKey) {
  const now          = Date.now();
  const sixMonthsAgo = now - 180 * 24 * 60 * 60 * 1000;

  // Get all workspace members
  const members   = await fetchAllMembers(apiKey);
  const memberIds = members.map((m) => m.id).filter(Boolean);
  if (memberIds.length === 0) throw new Error('ClickUp: team has 0 members — check API key permissions');
  console.log(`   Fetching entries for ${memberIds.length} members across 2 spaces…`);

  const allEntries = [];
  const seen       = new Set();

  for (const [spaceLabel, spaceId] of [
    ['CS',       SPACES.CUSTOMER_SUCCESS.id],
    ['TechOps',  SPACES.TECHOPS.id],
  ]) {
    let spaceCount = 0;
    for (const memberId of memberIds) {
      try {
        const entries = await fetchOneMemberSpace(apiKey, spaceId, sixMonthsAgo, now, memberId);
        for (const e of entries) {
          if (e.id && !seen.has(e.id)) { seen.add(e.id); allEntries.push(e); spaceCount++; }
        }
      } catch (e) {
        // Log and skip — don't let one member failure abort the whole sync
        console.warn(`   ⚠  Could not fetch ${spaceLabel} entries for member ${memberId}: ${e.message}`);
      }
      await sleep(150); // 150ms between calls to stay under rate limits
    }
    console.log(`   ✓ ${spaceLabel}: ${spaceCount} unique entries`);
  }

  return allEntries;
}

module.exports = { fetchAllTimeEntries, classifyEntry, SPACES, SA_USERNAMES };
