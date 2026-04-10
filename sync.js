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

const AE_OWNERS = {
  '76991650': 'Alberto Marchetti',
  '40312105': 'Célia Denouette',
  '84528640': 'Jakob Hauser',
  '87073113': 'Manuel Martinez',
  '89389021': 'Omri Daniel',
};
const AE_IDS = Object.keys(AE_OWNERS);

// NewBiz pipeline (default) stage IDs
const STAGE = {
  NEW_LEAD:     '241154736',
  QUALIFIED:    '241154737',
  EXPLORED:     '241154738',
  ADVISED:      '1289129531',
  OFFER_SIGNED: '1289129532',
  CLOSED_WON:   'current_client',
  CLOSED_LOST:  'bad_fit',
};

const STAGE_LABELS = {
  [STAGE.NEW_LEAD]:     { label: 'New lead',    order: 1 },
  [STAGE.QUALIFIED]:    { label: 'Qualified',    order: 2 },
  [STAGE.EXPLORED]:     { label: 'Explored',     order: 3 },
  [STAGE.ADVISED]:      { label: 'Advised',      order: 4 },
  [STAGE.OFFER_SIGNED]: { label: 'Offer Signed', order: 5 },
};

const manual = JSON.parse(readFileSync('./manual-data.json', 'utf8'));
const QUARTER_START_MS = new Date(manual.quarter_start + 'T00:00:00Z').getTime();

// Week start (Monday) for a date value (string or ms)
function weekStart(dateVal) {
  const d = new Date(typeof dateVal === 'string' ? dateVal : Number(dateVal));
  const day = d.getUTCDay();
  const diff = day === 0 ? -6 : 1 - day;
  const mon = new Date(d);
  mon.setUTCDate(d.getUTCDate() + diff);
  return mon.toISOString().split('T')[0];
}

// Paginate through HubSpot search API
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

// Bucket records by week+owner → [{ week, owner_id, owner_name, count, amount }]
function bucket(records, dateFn, ownerFn, amountFn = () => 0) {
  const map = {};
  for (const r of records) {
    const ownerId = ownerFn(r);
    if (!ownerId || !AE_OWNERS[ownerId]) continue;
    const raw = dateFn(r);
    if (!raw) continue;
    const week = weekStart(raw);
    const key = `${week}|${ownerId}`;
    if (!map[key]) map[key] = { week, owner_id: ownerId, owner_name: AE_OWNERS[ownerId], count: 0, amount: 0 };
    map[key].count++;
    map[key].amount += amountFn(r);
  }
  return Object.values(map).sort((a, b) => a.week.localeCompare(b.week));
}

// ── Q1 historical fetch helper ──────────────────────────────────────────────
const Q1_START = '2026-01-01';
const Q1_END   = '2026-03-31';
const Q1_FILE  = './q1-data.json';

async function fetchAndWriteQ1() {
  const startMs = new Date(Q1_START + 'T00:00:00Z').getTime();
  const endMs   = new Date(Q1_END   + 'T23:59:59Z').getTime();
  const advisedProp = `hs_v2_date_entered_${STAGE.ADVISED}`;
  const MEETING_TYPES = ['qualification', 'explore', 'advise', 'close'];

  console.log('\n🕐  Fetching Q1-2026 historical data...');

  const [cwDeals, offDeals, allMtgs] = await Promise.all([
    searchAll('deals', {
      filterGroups: [{ filters: [
        { propertyName: 'pipeline',         operator: 'EQ',  value: 'default' },
        { propertyName: 'dealstage',        operator: 'EQ',  value: STAGE.CLOSED_WON },
        { propertyName: 'hubspot_owner_id', operator: 'IN',  values: AE_IDS },
        { propertyName: 'closedate',        operator: 'GTE', value: String(startMs) },
        { propertyName: 'closedate',        operator: 'LTE', value: String(endMs) },
      ]}],
      properties: ['amount', 'closedate', 'hubspot_owner_id'],
    }),
    searchAll('deals', {
      filterGroups: [{ filters: [
        { propertyName: 'pipeline',         operator: 'EQ',  value: 'default' },
        { propertyName: 'hubspot_owner_id', operator: 'IN',  values: AE_IDS },
        { propertyName: advisedProp,        operator: 'GTE', value: String(startMs) },
        { propertyName: advisedProp,        operator: 'LTE', value: String(endMs) },
      ]}],
      properties: [advisedProp, 'hubspot_owner_id'],
    }),
    searchAll('meetings', {
      filterGroups: [{ filters: [
        { propertyName: 'hs_meeting_outcome',    operator: 'EQ',  value: 'COMPLETED' },
        { propertyName: 'hubspot_owner_id',      operator: 'IN',  values: AE_IDS },
        { propertyName: 'hs_meeting_start_time', operator: 'GTE', value: String(startMs) },
        { propertyName: 'hs_meeting_start_time', operator: 'LTE', value: String(endMs) },
      ]}],
      properties: ['hs_meeting_start_time', 'hubspot_owner_id', 'hs_activity_type'],
    }),
  ]);

  const closedWon  = bucket(cwDeals, r => r.properties.closedate, r => r.properties.hubspot_owner_id, r => parseFloat(r.properties.amount || 0));
  const offersSent = bucket(offDeals, r => r.properties[advisedProp], r => r.properties.hubspot_owner_id);
  const meetings   = bucket(allMtgs.filter(r => MEETING_TYPES.some(t => (r.properties.hs_activity_type || '').toLowerCase().includes(t))),
    r => r.properties.hs_meeting_start_time, r => r.properties.hubspot_owner_id);

  writeFileSync(Q1_FILE, JSON.stringify({
    updated_at: new Date().toISOString(),
    quarter: 'Q1-2026', quarter_start: Q1_START, quarter_end: Q1_END,
    closed_won: closedWon, offers_sent: offersSent, meetings, pipeline_snapshot: [],
  }, null, 2));
  console.log(`✅  q1-data.json written (${closedWon.length} CW rows, ${offersSent.length} offer rows, ${meetings.length} meeting rows)\n`);
}

async function main() {
  console.log(`\n🔄  Syncing AE data — quarter start: ${manual.quarter_start}\n`);

  // 1. Closed Won deals
  console.log('1/4  Fetching closed-won deals...');
  const closedWonDeals = await searchAll('deals', {
    filterGroups: [{ filters: [
      { propertyName: 'pipeline',          operator: 'EQ',  value: 'default' },
      { propertyName: 'dealstage',         operator: 'EQ',  value: STAGE.CLOSED_WON },
      { propertyName: 'hubspot_owner_id',  operator: 'IN',  values: AE_IDS },
      { propertyName: 'closedate',         operator: 'GTE', value: String(QUARTER_START_MS) },
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

  // 2. Offers Sent — deals entering Advised stage
  console.log('2/4  Fetching offers sent (Advised stage entries)...');
  const advisedProp = `hs_v2_date_entered_${STAGE.ADVISED}`;
  const advisedDeals = await searchAll('deals', {
    filterGroups: [{ filters: [
      { propertyName: 'pipeline',         operator: 'EQ',  value: 'default' },
      { propertyName: 'hubspot_owner_id', operator: 'IN',  values: AE_IDS },
      { propertyName: advisedProp,        operator: 'GTE', value: String(QUARTER_START_MS) },
    ]}],
    properties: [advisedProp, 'hubspot_owner_id'],
  });
  console.log(`     → ${advisedDeals.length} offers`);

  const offersSent = bucket(
    advisedDeals,
    r => r.properties[advisedProp],
    r => r.properties.hubspot_owner_id,
  );

  // 3. Meetings (completed, relevant types)
  console.log('3/4  Fetching completed meetings...');
  const MEETING_TYPES = ['qualification', 'explore', 'advise', 'close'];
  const allMeetings = await searchAll('meetings', {
    filterGroups: [{ filters: [
      { propertyName: 'hs_meeting_outcome',    operator: 'EQ',  value: 'COMPLETED' },
      { propertyName: 'hubspot_owner_id',      operator: 'IN',  values: AE_IDS },
      { propertyName: 'hs_meeting_start_time', operator: 'GTE', value: String(QUARTER_START_MS) },
    ]}],
    properties: ['hs_meeting_start_time', 'hubspot_owner_id', 'hs_activity_type'],
  });
  const filteredMeetings = allMeetings.filter(r => {
    const type = (r.properties.hs_activity_type || '').toLowerCase();
    return MEETING_TYPES.some(t => type.includes(t));
  });
  console.log(`     → ${allMeetings.length} completed meetings, ${filteredMeetings.length} with relevant types`);

  const meetings = bucket(
    filteredMeetings,
    r => r.properties.hs_meeting_start_time,
    r => r.properties.hubspot_owner_id,
  );

  // 4. Pipeline snapshot (open deals)
  console.log('4/4  Fetching open pipeline...');
  const openDeals = await searchAll('deals', {
    filterGroups: [{ filters: [
      { propertyName: 'pipeline',         operator: 'EQ', value: 'default' },
      { propertyName: 'hubspot_owner_id', operator: 'IN', values: AE_IDS },
      { propertyName: 'dealstage',        operator: 'IN', values: Object.keys(STAGE_LABELS) },
    ]}],
    properties: ['dealstage', 'amount', 'hubspot_owner_id'],
  });
  console.log(`     → ${openDeals.length} open deals`);

  const pipelineMap = {};
  for (const r of openDeals) {
    const ownerId = r.properties.hubspot_owner_id;
    const stage = r.properties.dealstage;
    if (!AE_OWNERS[ownerId] || !STAGE_LABELS[stage]) continue;
    const key = `${ownerId}|${stage}`;
    if (!pipelineMap[key]) pipelineMap[key] = {
      owner_id: ownerId,
      owner_name: AE_OWNERS[ownerId],
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
  console.log(`    Offers Sent rows:   ${offersSent.length}`);
  console.log(`    Meeting rows:       ${meetings.length}`);
  console.log(`    Pipeline rows:      ${pipelineSnapshot.length}\n`);

  // Fetch Q1 historical data only if not already present
  if (!existsSync(Q1_FILE)) {
    await fetchAndWriteQ1();
  } else {
    console.log(`ℹ️  q1-data.json already exists — skipping Q1 fetch (delete to re-fetch)\n`);
  }
}

main().catch(err => { console.error('❌ ', err.message); process.exit(1); });
