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

const BASE = 'https://api.hubapi.com';

const AM_OWNERS = {
  '76991649':  'Patricia Chalmeta',
  '51221997':  'Antolin Lera Jeuk',
  '751897671': 'Edouard Daenen',
  '90532029':  'Viviana El Mouak',
};
const AM_IDS = Object.keys(AM_OWNERS);

// Retention pipeline ID
const PIPELINE = '715276873';

const STAGE = {
  ONBOARDING:      '1044890541',
  RENEWAL_6M_PLUS: '1044890542',
  RENEWAL_3_6M:    '1044890544',
  RENEWAL_SUB_3M:  '1044890543',
  RENEWAL_30D:     '1300671596',
  CLOSED_WON:      '1044890546',
  CLOSED_LOST:     '1044890547',
};

const STAGE_LABELS = {
  [STAGE.ONBOARDING]:      { label: 'Onboarding',     order: 1 },
  [STAGE.RENEWAL_6M_PLUS]: { label: 'Renewal > 6 mo', order: 2 },
  [STAGE.RENEWAL_3_6M]:    { label: 'Renewal 3–6 mo', order: 3 },
  [STAGE.RENEWAL_SUB_3M]:  { label: 'Renewal < 3 mo', order: 4 },
  [STAGE.RENEWAL_30D]:     { label: 'Renewal in 30d', order: 5 },
};

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

async function searchAll(objectType, payload) {
  const results = [];
  let after;
  while (true) {
    const body = { ...payload, limit: 100, ...(after ? { after } : {}) };
    const res = await fetch(`${BASE}/crm/v3/objects/${objectType}/search`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`HubSpot ${objectType}: ${res.status} ${await res.text()}`);
    const json = await res.json();
    results.push(...(json.results || []));
    if (json.paging?.next?.after) { after = json.paging.next.after; }
    else break;
  }
  return results;
}

function bucket(records, dateFn, ownerFn, amountFn = () => 0) {
  const map = {};
  for (const r of records) {
    const ownerId = ownerFn(r);
    if (!ownerId || !AM_OWNERS[ownerId]) continue;
    const raw = dateFn(r);
    if (!raw) continue;
    const week = weekStart(raw);
    const key = `${week}|${ownerId}`;
    if (!map[key]) map[key] = { week, owner_id: ownerId, owner_name: AM_OWNERS[ownerId], count: 0, amount: 0 };
    map[key].count++;
    map[key].amount += amountFn(r);
  }
  return Object.values(map).sort((a, b) => a.week.localeCompare(b.week));
}

async function main() {
  console.log(`\n🔄  Syncing AM data — quarter start: ${manual.quarter_start}\n`);

  // 1. Closed Won deals (Retention pipeline)
  console.log('1/4  Fetching closed-won deals...');
  const closedWonDeals = await searchAll('deals', {
    filterGroups: [{ filters: [
      { propertyName: 'pipeline',         operator: 'EQ',  value: PIPELINE },
      { propertyName: 'dealstage',        operator: 'EQ',  value: STAGE.CLOSED_WON },
      { propertyName: 'hubspot_owner_id', operator: 'IN',  values: AM_IDS },
      { propertyName: 'closedate',        operator: 'GTE', value: String(QUARTER_START_MS) },
    ]}],
    properties: ['amount', 'closedate', 'hubspot_owner_id'],
  });
  console.log(`     → ${closedWonDeals.length} deals`);

  const closedWon = bucket(
    closedWonDeals,
    r => r.properties.closedate,
    r => r.properties.hubspot_owner_id,
    r => parseFloat(r.properties.amount || 0),
  );

  // 2. Renewal proposals — deals entering "Renewal in 30 days" stage
  console.log('2/4  Fetching renewal proposals (Renewal in 30d stage entries)...');
  const renewalProp = `hs_v2_date_entered_${STAGE.RENEWAL_30D}`;
  const renewalDeals = await searchAll('deals', {
    filterGroups: [{ filters: [
      { propertyName: 'pipeline',         operator: 'EQ',  value: PIPELINE },
      { propertyName: 'hubspot_owner_id', operator: 'IN',  values: AM_IDS },
      { propertyName: renewalProp,        operator: 'GTE', value: String(QUARTER_START_MS) },
    ]}],
    properties: [renewalProp, 'hubspot_owner_id'],
  });
  console.log(`     → ${renewalDeals.length} renewal proposals`);

  const offersSent = bucket(
    renewalDeals,
    r => r.properties[renewalProp],
    r => r.properties.hubspot_owner_id,
  );

  // 3. Meetings (all completed meetings for AMs)
  console.log('3/4  Fetching completed meetings...');
  const allMeetings = await searchAll('meetings', {
    filterGroups: [{ filters: [
      { propertyName: 'hs_meeting_outcome',    operator: 'EQ',  value: 'COMPLETED' },
      { propertyName: 'hubspot_owner_id',      operator: 'IN',  values: AM_IDS },
      { propertyName: 'hs_meeting_start_time', operator: 'GTE', value: String(QUARTER_START_MS) },
    ]}],
    properties: ['hs_meeting_start_time', 'hubspot_owner_id', 'hs_activity_type'],
  });
  console.log(`     → ${allMeetings.length} completed meetings`);

  const meetings = bucket(
    allMeetings,
    r => r.properties.hs_meeting_start_time,
    r => r.properties.hubspot_owner_id,
  );

  // 4. Pipeline snapshot (open deals)
  console.log('4/4  Fetching open pipeline...');
  const openDeals = await searchAll('deals', {
    filterGroups: [{ filters: [
      { propertyName: 'pipeline',         operator: 'EQ', value: PIPELINE },
      { propertyName: 'hubspot_owner_id', operator: 'IN', values: AM_IDS },
      { propertyName: 'dealstage',        operator: 'IN', values: Object.keys(STAGE_LABELS) },
    ]}],
    properties: ['dealstage', 'amount', 'hubspot_owner_id'],
  });
  console.log(`     → ${openDeals.length} open deals`);

  const pipelineMap = {};
  for (const r of openDeals) {
    const ownerId = r.properties.hubspot_owner_id;
    const stage = r.properties.dealstage;
    if (!AM_OWNERS[ownerId] || !STAGE_LABELS[stage]) continue;
    const key = `${ownerId}|${stage}`;
    if (!pipelineMap[key]) pipelineMap[key] = {
      owner_id: ownerId,
      owner_name: AM_OWNERS[ownerId],
      stage: STAGE_LABELS[stage].label,
      stage_order: STAGE_LABELS[stage].order,
      count: 0,
      value: 0,
    };
    pipelineMap[key].count++;
    pipelineMap[key].value += parseFloat(r.properties.amount || 0);
  }
  const pipelineSnapshot = Object.values(pipelineMap)
    .sort((a, b) => a.stage_order - b.stage_order || a.owner_name.localeCompare(b.owner_name));

  const output = {
    updated_at: new Date().toISOString(),
    quarter: manual.quarter,
    quarter_start: manual.quarter_start,
    closed_won: closedWon,
    offers_sent: offersSent,
    meetings,
    pipeline_snapshot: pipelineSnapshot,
  };

  writeFileSync('./data.json', JSON.stringify(output, null, 2));

  console.log('\n✅  data.json written');
  console.log(`    Closed Won rows:    ${closedWon.length}`);
  console.log(`    Renewal 30d rows:   ${offersSent.length}`);
  console.log(`    Meeting rows:       ${meetings.length}`);
  console.log(`    Pipeline rows:      ${pipelineSnapshot.length}\n`);
}

main().catch(err => { console.error('❌ ', err.message); process.exit(1); });
