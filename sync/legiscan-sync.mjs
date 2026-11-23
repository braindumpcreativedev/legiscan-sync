#!/usr/bin/env node
// NFRMD daily LegiScan sync. Runs OUTSIDE Cloudflare (GitHub Actions), because
// legiscan.com answers Cloudflare Worker IPs with a 403 bot challenge.
//
// Flow per state (follows legiscan-notes.md: hashes, ≤2 req/s, current session only):
//   getSessionList → current session(s)
//   getMasterListRaw → { bill_id: change_hash }
//   GET  /api/sync-hashes?state=XX  → what our database already has
//   getBill for changed bills (+ blind refresh of bills on the Upcoming feed,
//   because calendar changes don't bump change_hash)
//   getBillText once per new HTML doc, getRollCall once per new roll call
//   POST /api/sync-legiscan in small batches
//
// Env: LEGISCAN_API_KEY, INGEST_SECRET, NFRMD_API (e.g. https://informed-and-loud.rork.app),
//      STATES ("CO,TX", or "ALL" = the 50 states), optional MAX_BILLS_PER_RUN (default 400),
//      MAX_QUERIES_PER_RUN (default 300, ~9k/month vs LegiScan's 10k free cap, cut from 30k on 2026-10-01), STATE_FILE.
// Node 20+, no dependencies.

import fs from "node:fs";

const ALL_STATES = [
  "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
  "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
  "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT",
  "VA", "WA", "WV", "WI", "WY",
];
const KEY = must("LEGISCAN_API_KEY");
const SECRET = must("INGEST_SECRET");
const API = must("NFRMD_API").replace(/\/$/, "");
const STATES = must("STATES").trim().toUpperCase() === "ALL"
  ? ALL_STATES
  : must("STATES").split(",").map((s) => s.trim().toUpperCase()).filter(Boolean);
const MAX_BILLS = Number(process.env.MAX_BILLS_PER_RUN ?? 400);
// Daily LegiScan query budget. The hash diff makes each run resume where the last
// stopped, so a full-coverage backfill paces itself across days inside the monthly cap.
// LegiScan's free Public tier is 10,000 queries/month (cut from 30,000 on 2026-10-01):
// 300/day ≈ 9k/month leaves headroom for manual runs and retries.
let budget = Number(process.env.MAX_QUERIES_PER_RUN ?? 300);
const STATE_FILE = process.env.STATE_FILE ?? ".legiscan-state.json";
const BATCH = 20;

// Remembers what never changes, so we never download it twice:
// docs[bill_id] = doc_id of the text we sent; rollCalls = roll_call_ids we sent.
const memo = loadMemo();
let queries = 0;

function must(name) {
  const v = process.env[name];
  if (!v) {
    console.error(`Missing env ${name}`);
    process.exit(1);
  }
  return v;
}

function loadMemo() {
  try {
    const m = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
    return { docs: m.docs ?? {}, rollCalls: new Set(m.rollCalls ?? []) };
  } catch {
    return { docs: {}, rollCalls: new Set() };
  }
}

function saveMemo() {
  fs.writeFileSync(STATE_FILE, JSON.stringify({ docs: memo.docs, rollCalls: [...memo.rollCalls] }));
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Raised when the daily LegiScan query budget is spent. Not a failure: the next
// run continues from the same hash diff.
class Quota extends Error {}

// One LegiScan call, ≤2 req/s, status-checked. Fails loudly on a Cloudflare page.
async function legiscan(op, params = {}) {
  if (budget <= 0) throw new Quota("daily LegiScan query budget used up");
  const qs = new URLSearchParams({ key: KEY, op, ...params });
  await sleep(550);
  queries++;
  budget--;
  const res = await fetch(`https://api.legiscan.com/?${qs}`, { headers: { Accept: "application/json" } });
  const text = await res.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    throw new Error(`LegiScan ${op}: HTTP ${res.status}, not JSON (${text.slice(0, 120).replace(/\s+/g, " ")}). If this is a Cloudflare "Just a moment" page, this runner's IP is blocked too.`);
  }
  if (body.status !== "OK") throw new Error(`LegiScan ${op}: ${body.status} ${JSON.stringify(body.alert ?? "")}`);
  return body;
}

async function ours(path, init = {}) {
  const res = await fetch(`${API}${path}`, {
    ...init,
    headers: { Authorization: `Bearer ${SECRET}`, "Content-Type": "application/json", ...(init.headers ?? {}) },
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`${path}: HTTP ${res.status} ${JSON.stringify(body)}`);
  return body;
}

function htmlToText(html) {
  return html
    .replace(/<(script|style)[\s\S]*?<\/\1>/gi, "")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(p|div|tr|li|h\d)>/gi, "\n")
    .replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

const today = new Date().toISOString().slice(0, 10);

async function syncState(state) {
  const { sessions } = await legiscan("getSessionList", { state });
  const current = sessions.filter((s) => s.prior === 0 && s.sine_die === 0);
  const active = current.length ? current : sessions.filter((s) => s.prior === 0).slice(0, 1);
  if (!active.length) {
    console.log(`${state}: no current session, skipping`);
    return;
  }

  const known = await ours(`/api/sync-hashes?state=${state}`);

  // Bills on our Upcoming feed get a blind refresh: calendars don't change the hash.
  const upcoming = await fetch(`${API}/v1/hearings?state=${state}`).then((r) => r.json()).catch(() => ({ hearings: [] }));
  const upcomingIds = new Set((upcoming.hearings ?? []).map((h) => Number(String(h.billId).replace("ls-", ""))));

  for (const session of active) {
    const { masterlist } = await legiscan("getMasterListRaw", { id: session.session_id });
    const entries = Object.entries(masterlist)
      .filter(([k]) => k !== "session")
      .map(([, b]) => b);
    const changed = entries
      .filter((b) => known[String(b.bill_id)] !== b.change_hash || upcomingIds.has(b.bill_id))
      .sort((a, b) => String(b.last_action_date ?? "").localeCompare(String(a.last_action_date ?? "")))
      .slice(0, MAX_BILLS);
    console.log(`${state} ${session.session_name}: ${entries.length} bills, ${changed.length} to refresh`);
    if (!changed.length) continue;

    const { sessionpeople } = await legiscan("getSessionPeople", { id: session.session_id });
    const people = new Map(sessionpeople.people.map((p) => [p.people_id, p]));

    let bills = [];
    let rollCalls = [];
    const flush = async () => {
      if (!bills.length && !rollCalls.length) return;
      const r = await ours("/api/sync-legiscan", { method: "POST", body: JSON.stringify({ state, bills, roll_calls: rollCalls }) });
      console.log(`  → sent ${bills.length} bills, ${rollCalls.length} roll calls (rejected ${r.rejected})`);
      if (r.rejected) console.log(r.errors);
      bills = [];
      rollCalls = [];
      saveMemo();
    };

    for (const entry of changed) {
      let bill;
      try {
        ({ bill } = await legiscan("getBill", { id: entry.bill_id }));
      } catch (err) {
        if (err instanceof Quota) await flush(); // save the 20-bill batches already built
        throw err;
      }

      // Latest text version; decode HTML once per new doc. PDFs keep a link.
      let full_text, text_url, text_date;
      const latest = [...(bill.texts ?? [])].sort((a, b) => String(a.date).localeCompare(String(b.date))).at(-1);
      if (latest) {
        text_url = latest.state_link || latest.url;
        text_date = latest.date;
        const isHTML = /html/i.test(latest.mime ?? "");
        if (isHTML && memo.docs[bill.bill_id] !== latest.doc_id) {
          const { text } = await legiscan("getBillText", { id: latest.doc_id });
          full_text = htmlToText(Buffer.from(text.doc, "base64").toString("utf8"));
          memo.docs[bill.bill_id] = latest.doc_id;
        }
      }

      const hearings = (bill.calendar ?? [])
        .filter((c) => c.date && c.date >= today)
        .map((c) => ({ date: c.date, time: c.time || null, type: c.type || null, location: c.location || null, description: c.description || null }));

      bills.push({
        bill_id: bill.bill_id,
        state,
        bill_number: bill.bill_number,
        title: bill.title,
        description: bill.description,
        status: bill.status,
        status_date: bill.status_date,
        change_hash: bill.change_hash,
        session_id: bill.session?.session_id,
        session_name: bill.session?.session_name,
        url: bill.url,
        state_link: bill.state_link,
        last_action: bill.history?.at(-1)?.action ?? entry.last_action,
        last_action_date: bill.history?.at(-1)?.date ?? entry.last_action_date,
        ...(full_text !== undefined ? { full_text } : {}),
        text_url,
        text_date,
        upcoming_hearings: hearings,
        sponsors: (bill.sponsors ?? []).map((s) => ({ name: s.name, party: s.party, role: s.role })),
        subjects: (bill.subjects ?? []).map((s) => s.subject_name),
      });

      for (const v of bill.votes ?? []) {
        if (memo.rollCalls.has(v.roll_call_id)) continue; // roll calls are static
        const { roll_call: rc } = await legiscan("getRollCall", { id: v.roll_call_id });
        rollCalls.push({
          roll_call_id: rc.roll_call_id,
          bill_id: rc.bill_id,
          state,
          date: rc.date,
          desc: rc.desc,
          chamber: rc.chamber,
          yea: rc.yea,
          nay: rc.nay,
          nv: rc.nv,
          absent: rc.absent,
          total: rc.total,
          passed: rc.passed === 1,
          votes: (rc.votes ?? []).map((x) => {
            const p = people.get(x.people_id);
            return { people_id: x.people_id, name: p?.name ?? `Legislator ${x.people_id}`, party: p?.party ?? null, district: p?.district ?? null, vote: x.vote_text };
          }),
        });
        memo.rollCalls.add(rc.roll_call_id);
      }

      if (bills.length >= BATCH) await flush();
    }
    await flush();
  }
}

let failed = false;
for (const st of STATES) {
  try {
    await syncState(st);
  } catch (err) {
    if (err instanceof Quota) {
      // Later states keep their old hashes; the next run finishes them first.
      console.log(`${st}: ${err.message} — remaining states deferred to the next run`);
      break;
    }
    failed = true;
    console.error(`${st}: ${err.message}`);
  }
}
saveMemo();
console.log(`Done. LegiScan queries used this run: ${queries}`);
process.exit(failed ? 1 : 0);
