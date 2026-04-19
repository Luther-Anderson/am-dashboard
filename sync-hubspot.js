import fetch from 'node-fetch';
import { readFileSync, writeFileSync, existsSync } from 'fs';

// Load .env
if (existsSync('.env')) {
  for (const line of readFileSync('.env', 'utf8').split('\n')) {
    const eq = line.indexOf('=');
    if (eq > 0) process.env[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
  }
}

const API_KEY = process.env.HUBSPOT_API_KEY;
if (!API_KEY) { console.error('❌  Missing HUBSPOT_API_KEY in .env'); process.exit(1); }

const BASE     = 'https://api.hubapi.com';
const PIPELINE = '715276873'; // Retention

const AM_OWNERS = {
  '76991649':  'Patricia Chalmeta',
  '51221997':  'Antolin Lera Jeuk',
  '751897671': 'Edouard Daenen',
  '90532029':  'Viviana El Mouak',
};

const OPEN_STAGES = {
  '1044890541': { label: 'Onboarding',     order: 1 },
  '1044890542': { label: 'Renewal > 6 mo', order: 2 },
  '1044890544': { label: 'Renewal 3-6 mo', order: 3 },
  '1044890543': { label: 'Renewal < 3 mo', order: 4 },
  '1300671596': { label: 'Renewal in 30d', order: 5 },
};

const CLOSED_WON  = '1044890546';
const CLOSED_LOST = '1044890547';

const manual = JSON.parse(readFileSync('./manual-data.json', 'utf8'));
const QUARTER_START_MS = new Date(manual.quarter_start + 'T00:00:00Z').getTime();

function weekStart(dateVal) {
  const d = new Date(typeof dateVal === 'string' ? dateVal : Number(dateVal));
  const day = d.getUTCDay();
  const diff = day === 0 ? -6 : 1 - day;
  const mon = new Date(d);
  mon.setUTCDate(d.getUTCDate() + diff);
  return mon.toISOString().split('T')[0];
}

async function searchAll(payload) {
  const results = [];
  let after;
  while (true) {
    const body = { ...payload, limit: 100, ...(after ? { after } : {}) };
    const res = await fetch(`${BASE}/crm/v3/objects/deals/search`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`HubSpot: ${res.status} ${await res.text()}`);
    const json = await res.json();
    results.push(...(json.results || []));
    if (json.paging?.next?.after) after = json.paging.next.after;
    else break;
  }
  return results;
}

async function main() {
  console.log(`\n🔄  Syncing AM HubSpot data — ${manual.quarter}\n`);

  // ── Pipeline snapshot: one query per AM (keeps results small) ──────────────
  console.log('1/2  Fetching pipeline snapshot (1 query per AM)...');
  const pipelineMap = {};

  await Promise.all(Object.entries(AM_OWNERS).map(async ([ownerId, ownerName]) => {
    const deals = await searchAll({
      filterGroups: [{ filters: [
        { propertyName: 'pipeline',         operator: 'EQ',  value: PIPELINE },
        { propertyName: 'hubspot_owner_id', operator: 'EQ',  value: ownerId },
        { propertyName: 'dealstage',        operator: 'IN',  values: Object.keys(OPEN_STAGES) },
      ]}],
      properties: ['dealstage', 'amount'],
    });

    console.log(`     ${ownerName}: ${deals.length} open deals`);

    for (const r of deals) {
      const stage = r.properties.dealstage;
      const amt   = parseFloat(r.properties.amount || 0);
      if (!OPEN_STAGES[stage]) continue;
      const key = `${ownerId}|${stage}`;
      if (!pipelineMap[key]) pipelineMap[key] = {
        owner_id: ownerId, owner_name: ownerName,
        stage: OPEN_STAGES[stage].label, stage_order: OPEN_STAGES[stage].order,
        count: 0, value: 0,
      };
      pipelineMap[key].count++;
      pipelineMap[key].value += amt;
    }
  }));

  const pipelineSnapshot = Object.values(pipelineMap)
    .sort((a, b) => a.stage_order - b.stage_order || a.owner_name.localeCompare(b.owner_name));

  // ── Closed Lost this quarter ───────────────────────────────────────────────
  console.log('\n2/2  Fetching closed lost deals this quarter...');
  const clDeals = await searchAll({
    filterGroups: [{ filters: [
      { propertyName: 'pipeline',         operator: 'EQ',  value: PIPELINE },
      { propertyName: 'hubspot_owner_id', operator: 'IN',  values: Object.keys(AM_OWNERS) },
      { propertyName: 'dealstage',        operator: 'EQ',  value: CLOSED_LOST },
      { propertyName: 'closedate',        operator: 'GTE', value: String(QUARTER_START_MS) },
    ]}],
    properties: ['closedate', 'hubspot_owner_id'],
  });
  console.log(`     → ${clDeals.length} closed lost deals`);

  // Bucket by week + owner
  const clMap = {};
  for (const r of clDeals) {
    const oid  = r.properties.hubspot_owner_id;
    const date = r.properties.closedate;
    if (!oid || !AM_OWNERS[oid] || !date) continue;
    const week = weekStart(date);
    const key  = `${week}|${oid}`;
    if (!clMap[key]) clMap[key] = { week, owner_id: oid, owner_name: AM_OWNERS[oid], count: 0 };
    clMap[key].count++;
  }
  const closedLost = Object.values(clMap).sort((a, b) => a.week.localeCompare(b.week));

  // ── Merge into existing data.json (preserve Excel-parsed fields) ───────────
  const dataPath = './data.json';
  const existing = JSON.parse(readFileSync(dataPath, 'utf8'));

  const output = {
    ...existing,
    updated_at:       new Date().toISOString(),
    pipeline_snapshot: pipelineSnapshot,
    closed_lost:       closedLost,
  };

  writeFileSync(dataPath, JSON.stringify(output, null, 2));

  console.log('\n✅  data.json updated');
  console.log(`    Pipeline rows:  ${pipelineSnapshot.length}`);
  console.log(`    Closed Lost:    ${closedLost.length}\n`);
}

main().catch(err => { console.error('❌ ', err.message); process.exit(1); });
