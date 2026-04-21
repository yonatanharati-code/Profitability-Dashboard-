/* QL Intelligence — local query engine, no API key required */
(function () {
  'use strict';

  /* ── DOM tweaks (script is at bottom so DOM is ready) ───────────────────── */
  const keyRow = document.querySelector('.chat-key-row');
  if (keyRow) keyRow.style.display = 'none';
  const sub = document.querySelector('.chat-panel .chat-header div div:last-child');
  if (sub) sub.textContent = 'Powered by local data';

  /* ── Formatting helpers ─────────────────────────────────────────────────── */
  function fh(v)  { return v > 0 ? v.toFixed(1) + 'h' : '—'; }
  function f$(v)  {
    if (!v) return '—';
    if (v >= 1e6)  return '$' + (v / 1e6).toFixed(1) + 'M';
    if (v >= 1e3)  return '$' + Math.round(v / 1e3) + 'K';
    return '$' + Math.round(v);
  }
  function fp(v)  { return (v !== null && v !== undefined) ? v + '%' : '—'; }

  /* ── Build enriched row per customer ────────────────────────────────────── */
  function buildRows() {
    var rates = getRates();
    var w = wk[win];
    return DATA.map(function (c) {
      var cost        = Math.round(laborCost(c, rates));
      var deals       = dealReturn(c);
      var arrReturn   = Math.round(c.arr * (win / 12));
      var totalReturn = arrReturn + deals;
      var net         = totalReturn - cost;
      var roi         = cost > 0 ? Math.round((totalReturn - cost) / cost * 100) : null;
      var annualCost  = cost > 0 ? Math.round(cost * (12 / win)) : 0;
      var arrEffPct   = c.arr > 0 ? Math.round(annualCost / c.arr * 100) : null;
      var totalHrs    = (c.cs[w] || 0) + (c.sa[w] || 0) + (c.dev[w] || 0);
      var sadevHrs    = (c.sa[w] || 0) + (c.dev[w] || 0);
      var devCost     = Math.round((c.dev[w] || 0) * rates.dev);
      var devRecap    = devCost > 0 ? Math.round((c.devDealsAmt || 0) / devCost * 100) : null;
      return {
        name: c.name, owner: c.owner || null, arr: c.arr,
        stage: c.stage || null, rank: c.rank || null, flag: c.flag,
        cs: c.cs[w] || 0, sa: c.sa[w] || 0, dev: c.dev[w] || 0,
        totalHrs: totalHrs, sadevHrs: sadevHrs,
        devDeals: c.devDealsAmt || 0, upsells: c.upsellAmt || 0, pipeline: c.devPipe || 0,
        laborCost: cost, arrReturn: arrReturn, deals: deals,
        totalReturn: totalReturn, net: net, roi: roi,
        arrEffPct: arrEffPct, devRecapturePct: devRecap, risk: risk(c),
      };
    });
  }

  /* ── List formatter ─────────────────────────────────────────────────────── */
  function lst(items, fn) {
    return items.map(function (r, i) { return fn(r, i + 1); }).join('\n');
  }

  /* ── Main answer function ───────────────────────────────────────────────── */
  function answer(q) {
    var rows     = buildRows();
    var rates    = getRates();
    var winLabel = win === 1 ? '1M' : win === 3 ? '3M' : '6M';
    var ql       = q.toLowerCase();

    /* ── Specific customer lookup ── */
    // Words that must never trigger a customer match even if they appear in a name
    var STOPWORDS = new Set([
      'the','a','an','is','are','be','was','were','been','am',
      'of','in','on','at','to','for','by','or','and','but','so','if',
      'it','its','we','he','she','they','you','i','me','us','my','our',
      'who','what','where','when','which','how','why','do','does','did',
      'best','worst','most','least','top','all','any','new','old','big','no',
      'customer','customers','show','list','give','tell','find','get'
    ]);
    var hit = null;
    for (var ci = 0; ci < rows.length; ci++) {
      var firstName = rows[ci].name.toLowerCase().split(/\s+/)[0];
      // Must be ≥4 chars AND not a common English word to avoid false matches
      if (firstName.length >= 4 && !STOPWORDS.has(firstName) && ql.includes(firstName)) {
        hit = rows[ci]; break;
      }
    }
    if (hit) {
      var r = hit;
      var dealParts = [];
      if (r.devDeals)  dealParts.push('Dev deals ' + f$(r.devDeals));
      if (r.upsells)   dealParts.push('Upsells '   + f$(r.upsells));
      if (r.pipeline)  dealParts.push('Pipeline '  + f$(r.pipeline));
      return '**' + r.name + '** (' + winLabel + ' window)\n' +
        'ARR: ' + f$(r.arr) + '  Owner: ' + (r.owner || '—') +
        '  Stage: ' + (r.stage || '—') + '  Rank: ' + (r.rank || '—') + '\n' +
        'CS: ' + fh(r.cs) + '  SA: ' + fh(r.sa) + '  Dev: ' + fh(r.dev) +
        '  **Total: ' + fh(r.totalHrs) + '**\n' +
        'Labor cost: ' + f$(r.laborCost) + '  ARR efficiency: ' + fp(r.arrEffPct) + '\n' +
        (dealParts.length ? dealParts.join('  ') + '\n' : '') +
        'Net: **' + f$(r.net) + '**  ROI: **' + fp(r.roi) + '**  Risk: ' + r.risk.t;
    }

    /* ── Owner lookup ── */
    var owners = [];
    DATA.forEach(function (c) { if (c.owner && owners.indexOf(c.owner) < 0) owners.push(c.owner); });
    var ownerHit = null;
    for (var oi = 0; oi < owners.length; oi++) {
      var o = owners[oi];
      if (ql.includes(o.toLowerCase()) || ql.includes(o.split(' ')[0].toLowerCase())) {
        ownerHit = o; break;
      }
    }
    if (ownerHit) {
      var owned = rows.filter(function (r) { return r.owner === ownerHit; });
      var tARR  = owned.reduce(function (s, r) { return s + r.arr; }, 0);
      var tHrs  = owned.reduce(function (s, r) { return s + r.totalHrs; }, 0);
      return '**' + ownerHit + " — " + owned.length + ' customers** (' + winLabel + ')\n' +
        'Total ARR: ' + f$(tARR) + '  Total hours: ' + fh(tHrs) + '\n\n' +
        lst(owned, function (r, i) {
          return i + '. **' + r.name + '** — ARR ' + f$(r.arr) +
            '  ' + fh(r.totalHrs) + ' hrs  ROI ' + fp(r.roi) + '  ' + r.risk.t;
        });
    }

    /* ── Worst ROI ── */
    if (/worst.{0,10}roi|unprofitable|losing|negative roi|most expensive/i.test(q)) {
      var sorted = rows.filter(function (r) { return r.roi !== null; })
        .sort(function (a, b) { return a.roi - b.roi; }).slice(0, 8);
      return '**Worst ROI customers** (' + winLabel + '):\n\n' +
        lst(sorted, function (r, i) {
          return i + '. **' + r.name + '** — ROI ' + fp(r.roi) +
            '  Labor ' + f$(r.laborCost) + '  Net ' + f$(r.net);
        });
    }

    /* ── Best ROI ── */
    if (/best.{0,10}roi|most profitable|profitable|best return/i.test(q)) {
      var sorted2 = rows.filter(function (r) { return r.roi !== null; })
        .sort(function (a, b) { return b.roi - a.roi; }).slice(0, 8);
      return '**Most profitable customers** (' + winLabel + '):\n\n' +
        lst(sorted2, function (r, i) {
          return i + '. **' + r.name + '** — ROI ' + fp(r.roi) +
            '  Net ' + f$(r.net) + '  ARR ' + f$(r.arr);
        });
    }

    /* ── Churn / at risk ── */
    if (/churn|at risk|who.{0,5}risk|flag|red/i.test(q)) {
      var risky = rows
        .filter(function (r) { return r.risk.l === 'high' || r.risk.l === 'med' || r.flag === 'red'; })
        .sort(function (a, b) { return b.risk.s - a.risk.s; }).slice(0, 10);
      if (!risky.length) return 'No customers flagged as at-risk in the **' + winLabel + '** window.';
      return '**At-risk customers** (' + winLabel + '):\n\n' +
        lst(risky, function (r, i) {
          return i + '. **' + r.name + '** — ' + r.risk.t +
            '  SA+Dev: ' + fh(r.sadevHrs) + '  Deals: ' + f$(r.deals) + '  Owner: ' + (r.owner || '—');
        });
    }

    /* ── Over-invested / no return ── */
    if (/over.?invest|no return|no deal|leakage|burning|which.{0,15}over/i.test(q)) {
      var over = rows
        .filter(function (r) { return r.sadevHrs > 10 && r.deals === 0 && r.stage === 'Live'; })
        .sort(function (a, b) { return b.sadevHrs - a.sadevHrs; }).slice(0, 8);
      if (!over.length) return 'No Live customers with SA+Dev > 10h and zero deals.';
      return '**Over-invested with no return** (SA+Dev > 10h, no deals, Live, ' + winLabel + '):\n\n' +
        lst(over, function (r, i) {
          return i + '. **' + r.name + '** — SA+Dev: ' + fh(r.sadevHrs) +
            '  Labor cost: ' + f$(r.laborCost) + '  Owner: ' + (r.owner || '—');
        });
    }

    /* ── Top by investment ── */
    if (/top.{0,10}invest|most hours|most time|investment.{0,10}return|highest hours/i.test(q)) {
      var sorted3 = rows.slice().sort(function (a, b) { return b.totalHrs - a.totalHrs; }).slice(0, 8);
      return '**Top customers by hours invested** (' + winLabel + '):\n\n' +
        lst(sorted3, function (r, i) {
          return i + '. **' + r.name + '** — ' + fh(r.totalHrs) +
            '  Labor ' + f$(r.laborCost) + '  ROI ' + fp(r.roi);
        });
    }

    /* ── ARR efficiency ── */
    if (/arr.{0,10}effic|effic|benchmark/i.test(q)) {
      var sorted4 = rows.filter(function (r) { return r.arrEffPct !== null; })
        .sort(function (a, b) { return b.arrEffPct - a.arrEffPct; }).slice(0, 8);
      return '**ARR Efficiency** — benchmark \u2264' + rates.bench + '% (' + winLabel + '):\n\n' +
        lst(sorted4, function (r, i) {
          return i + '. **' + r.name + '** — ' + fp(r.arrEffPct) +
            '  Labor: ' + f$(r.laborCost) + '  ARR: ' + f$(r.arr) +
            (r.arrEffPct > rates.bench ? '  \u26a0\ufe0f' : '  \u2705');
        });
    }

    /* ── Deal / upsell opportunities ── */
    if (/upsell|deal.{0,10}opportunit|where.{0,10}focus|deal.{0,10}clos|opportunit/i.test(q)) {
      var opps = rows
        .filter(function (r) {
          return r.sadevHrs > 5 &&
            (r.stage === 'Live' || r.stage === 'Development') &&
            r.deals < r.laborCost * 0.5;
        })
        .sort(function (a, b) { return b.sadevHrs - a.sadevHrs; }).slice(0, 8);
      if (!opps.length) return 'All high-SA+Dev customers have deals covering \u226550% of labor cost.';
      return '**Deal-closing opportunities** (' + winLabel + '):\n\n' +
        lst(opps, function (r, i) {
          return i + '. **' + r.name + '** — SA+Dev: ' + fh(r.sadevHrs) +
            '  Exposed labor: ' + f$(r.laborCost) +
            '  Deals: ' + f$(r.deals) + '  Owner: ' + (r.owner || '—');
        });
    }

    /* ── Dev recapture ── */
    if (/dev.{0,10}recapture|dev.{0,10}bill|dev.{0,10}paid|recapture/i.test(q)) {
      var sorted5 = rows.filter(function (r) { return r.dev > 0; })
        .sort(function (a, b) { return (a.devRecapturePct || 0) - (b.devRecapturePct || 0); }).slice(0, 8);
      return '**Dev recapture rates** (lower = more cost exposure, ' + winLabel + '):\n\n' +
        lst(sorted5, function (r, i) {
          return i + '. **' + r.name + '** — Recapture: ' + fp(r.devRecapturePct) +
            '  Dev hrs: ' + fh(r.dev) + '  Billed: ' + f$(r.devDeals);
        });
    }

    /* ── Portfolio summary ── */
    if (/summar|overview|total|how many|all customer|portfolio/i.test(q)) {
      var tARR2  = rows.reduce(function (s, r) { return s + r.arr; }, 0);
      var tHrs2  = rows.reduce(function (s, r) { return s + r.totalHrs; }, 0);
      var tCost  = rows.reduce(function (s, r) { return s + r.laborCost; }, 0);
      var nProf  = rows.filter(function (r) { return r.roi !== null && r.roi >= 0; }).length;
      var nRoiTot = rows.filter(function (r) { return r.roi !== null; }).length;
      var nHigh  = rows.filter(function (r) { return r.risk.l === 'high'; }).length;
      return '**Portfolio Summary** (' + winLabel + ')\n\n' +
        '\u2022 **' + rows.length + ' customers**  Total ARR: ' + f$(tARR2) + '\n' +
        '\u2022 Total hours: ' + fh(tHrs2) + '  Labor cost: ' + f$(tCost) + '\n' +
        '\u2022 Profitable (ROI \u2265 0): **' + nProf + '** / ' + nRoiTot + '\n' +
        '\u2022 High leakage risk: **' + nHigh + '** customers';
    }

    /* ── Fallback ── */
    return 'I can answer questions about:\n\n' +
      '\u2022 **ROI** \u2014 "worst ROI", "most profitable"\n' +
      '\u2022 **Risk** \u2014 "who is at risk of churning?"\n' +
      '\u2022 **Leakage** \u2014 "which customers are over-invested with no return?"\n' +
      '\u2022 **Hours** \u2014 "top 5 by investment vs return"\n' +
      '\u2022 **Deals** \u2014 "where should we focus deal-closing?"\n' +
      '\u2022 **ARR efficiency** \u2014 "show ARR efficiency"\n' +
      '\u2022 **By owner** \u2014 "Jonathan customers"\n' +
      '\u2022 **Customer lookup** \u2014 type any customer name';
  }

  /* store answer function on window so _qlLocalChat can reach it */
  window._qlAnswer = answer;

})();

/* ── _qlLocalChat defined OUTSIDE the IIFE so it's always reachable ───────── */
function _qlLocalChat() {
  var input = document.getElementById('chatInput');
  var q = (input ? input.value || '' : '').trim();
  if (!q) return;
  input.value = '';
  if (typeof addMsg === 'function') addMsg('user', q);
  var btn = document.getElementById('chatSend');
  if (btn) btn.disabled = true;
  try {
    var reply = (typeof window._qlAnswer === 'function')
      ? window._qlAnswer(q)
      : 'Chat engine not loaded yet — please refresh the page.';
    if (typeof addMsg === 'function') addMsg('assistant', reply);
  } catch (err) {
    if (typeof addMsg === 'function') addMsg('system', 'Error: ' + err.message);
    console.error('[QL Intelligence]', err);
  } finally {
    if (btn) btn.disabled = false;
  }
}
