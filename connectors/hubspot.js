'use strict';
const https = require('https');

// ─── Constants ────────────────────────────────────────────────────────────────
const HS_HOST = 'api.hubapi.com';
const PORTAL_ID = '139723745';

// CSM Pipeline (ID discovered from live data)
const CSM_PIPELINE = '323637953';
const STAGE_CLOSED_WON = '512617702';
const STAGE_COMMIT = '3650317499';

// FX snapshot rates (Apr 2026) — same as hardcoded dashboard
const FX = { EUR: 1.1714, GBP: 1.3427, USD: 1, ILS: 0.27, NOK: 0.09, CHF: 1.11, AUD: 0.63 };

// 9-month cutoff: July 10, 2025
const DEAL_CUTOFF_DATE = '2025-07-10';

// ─── HTTP helpers ─────────────────────────────────────────────────────────────
function hsPost(path, apiKey, payload) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify(payload);
    const req = https.request(
      {
        hostname: HS_HOST,
        path,
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
        },
      },
      (res) => {
        let raw = '';
        res.on('data', (c) => (raw += c));
        res.on('end', () => {
          try {
            const json = JSON.parse(raw);
            if (json.status === 'error') reject(new Error(`HubSpot: ${json.message}`));
            else resolve(json);
          } catch (e) {
            reject(new Error(`HubSpot non-JSON (${res.statusCode}): ${raw.slice(0, 200)}`));
          }
        });
      }
    );
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

// ─── Companies ────────────────────────────────────────────────────────────────
const COMPANY_PROPS = [
  'name', 'arr', 'csm', 'csms', 'rank', 'health', 'onboarding_stage', 'renewal_date',
  'hubspot_owner_id', 'lifecyclestage', 'company_status',
];

// Valid current CSM names from the csm enumeration — stale values (e.g. ex-employees) are nulled out
// Fetched from /crm/v3/properties/companies/csm on 2026-04-14
const VALID_CSMS = new Set([
  'Ami Spitzer', 'Jonathan Kidushim', 'Lee Kleiman', 'Vanessa Barki',
  'Yigal Gillis', 'Yonatan Harati', 'Yuval Taxa', 'Cecile Blau',
  'Adi Aharon Eldad', 'Tal Graziani', 'Ben Horovitz', 'Zoe Wachman',
  'Tamar Cohen', 'Shani Leimzider', 'Britt', 'Marcella', 'Amy',
]);

async function fetchAllCompanies(apiKey) {
  const companies = [];
  let after;

  while (true) {
    const payload = {
      filterGroups: [
        {
          filters: [
            { propertyName: 'company_status', operator: 'IN', values: ['Active Customer', 'Churn'] },
          ],
        },
      ],
      properties: COMPANY_PROPS,
      limit: 100,
      ...(after ? { after } : {}),
    };

    const res = await hsPost('/crm/v3/objects/companies/search', apiKey, payload);
    if (res.results?.length) companies.push(...res.results);
    after = res.paging?.next?.after;
    if (!after) break;
  }

  return companies;
}

// ─── Deals ────────────────────────────────────────────────────────────────────
const DEAL_PROPS = [
  'dealname', 'amount', 'deal_currency_code', 'closedate',
  'dealstage', 'hs_is_closed_won', 'amount_in_home_currency',
];

async function fetchDeals(apiKey) {
  const deals = [];
  let after;

  while (true) {
    const payload = {
      filterGroups: [
        {
          filters: [
            { propertyName: 'pipeline', operator: 'EQ', value: CSM_PIPELINE },
            {
              propertyName: 'dealstage',
              operator: 'IN',
              values: [STAGE_CLOSED_WON, STAGE_COMMIT],
            },
            { propertyName: 'closedate', operator: 'GTE', value: DEAL_CUTOFF_DATE },
          ],
        },
      ],
      properties: DEAL_PROPS,
      limit: 200,
      ...(after ? { after } : {}),
    };

    const res = await hsPost('/crm/v3/objects/deals/search', apiKey, payload);
    if (res.results?.length) deals.push(...res.results);
    after = res.paging?.next?.after;
    if (!after) break;
  }

  return deals;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
function toUSD(amount, currency) {
  const rate = FX[currency] ?? 1;
  return Math.round(amount * rate);
}

module.exports = { fetchAllCompanies, fetchDeals, toUSD, FX, PORTAL_ID, CSM_PIPELINE, VALID_CSMS };
