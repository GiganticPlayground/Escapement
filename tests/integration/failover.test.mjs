/**
 * Black-box failover suite. Two real Escapement processes, a stub S3, and the
 * whole HTTP stack including auth.
 *
 * It covers what the unit tests structurally cannot: leader election between two
 * processes, a follower forwarding claims to the leader, hard-kill promotion,
 * graceful lease hand-off, and — the reason any of this exists — that no code is
 * ever issued twice across a change of leadership.
 *
 *   npm run test:failover
 */
import { spawn } from 'node:child_process';
import { SignJWT } from 'jose';

import { startFakeS3 } from './fake-s3.mjs';

const S3_PORT = 9531;
const A_PORT = 8631;
const B_PORT = 8632;
const PLAYER_SECRET = 'player-secret-for-tests-only-0123456789';
const ADMIN_TOKEN = 'admin-static-token-for-tests-only';
const POOL = 'launch-codes';
const CODES = Array.from({ length: 2000 }, (_, i) => `CODE-${String(i).padStart(5, '0')}`);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let failures = 0;
const check = (ok, msg) => {
  console.log(`${ok ? '  PASS' : '  FAIL'}  ${msg}`);
  if (!ok) failures++;
};

function boot(port, label) {
  const child = spawn('node', ['--import=tsx', 'src/index.ts'], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      AWS_REGION: 'us-east-1',
      AWS_ACCESS_KEY_ID: 'test',
      AWS_SECRET_ACCESS_KEY: 'test',
      ESCAPEMENT_S3_BUCKET: 'bucket',
      ESCAPEMENT_S3_ENDPOINT: `http://127.0.0.1:${S3_PORT}`,
      ESCAPEMENT_ENV: 'test',
      ESCAPEMENT_KEY_PREFIX: 'escapement',
      ESCAPEMENT_ENDPOINT_HOST: '127.0.0.1',
      ESCAPEMENT_CONFIG_PATH: 'tests/fixtures/auth.integration.yaml',
      TEST_PLAYER_SECRET: PLAYER_SECRET,
      TEST_ADMIN_TOKEN: ADMIN_TOKEN,
      // No JWT_* vars: the config file above supplies the whole strategy list,
      // and the env schema no longer demands the unused fallback group.
      PORT: String(port),
      API_DOCS_ENABLED: 'false',
      LOG_TYPE: 'hidden',
      LEASE_TTL_MS: '4000',
      FOLLOW_POLL_MS: '400',
      BATCH_WINDOW_MS: '25',
      DRAIN_TIMEOUT_MS: '2000',
      SHUTDOWN_TIMEOUT_MS: '8000',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const tag = (d) =>
    d
      .toString()
      .trimEnd()
      .split('\n')
      .map((l) => `    [${label}] ${l}`)
      .join('\n');
  child.stdout.on('data', (d) => console.log(tag(d)));
  child.stderr.on('data', (d) => console.log(tag(d)));
  return child;
}

async function playerToken(sub) {
  return new SignJWT({ app: 'test' })
    .setProtectedHeader({ alg: 'HS256' })
    .setIssuer('https://players.test')
    .setSubject(sub)
    .setIssuedAt()
    .setExpirationTime('1h')
    .sign(new TextEncoder().encode(PLAYER_SECRET));
}

const post = (port, path, body, token, key) =>
  fetch(`http://127.0.0.1:${port}${path}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      authorization: `Bearer ${token}`,
      ...(key ? { 'idempotency-key': key } : {}),
    },
    body: JSON.stringify(body ?? {}),
  });

const get = (port, path, token) =>
  fetch(`http://127.0.0.1:${port}${path}`, { headers: { authorization: `Bearer ${token}` } });

let readToken;
// Engine status names internal endpoints and the leader, so it is admin-only.
const engineStatus = (port) =>
  get(port, '/v1/escapement/admin/engine', ADMIN_TOKEN).then((r) => r.json());

async function waitForRole(port, role, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      if ((await engineStatus(port)).role === role) return Date.now();
    } catch {
      /* not up yet */
    }
    await sleep(150);
  }
  return null;
}

// A static admin token carries no claims, so it is refused on player-scoped
// routes by design; reads and status use a player JWT.
readToken = await playerToken('observer');

const { server: s3, counts } = await startFakeS3(S3_PORT);
console.log(`\nstub S3 on :${S3_PORT}\n`);

let a = boot(A_PORT, 'A');
await waitForRole(A_PORT, 'leader', 20_000);
let b = boot(B_PORT, 'B');
await waitForRole(B_PORT, 'follower', 20_000);

const sa = await engineStatus(A_PORT);
const sb = await engineStatus(B_PORT);
console.log(`\n  A=${sa.role}  B=${sb.role}\n`);
check(sa.role === 'leader' && sb.role === 'follower', 'exactly one leader elected');
check(
  Array.isArray(sa.machines) && sa.machines.includes('pool') && sa.machines.includes('quota'),
  `both state machines registered (${sa.machines?.join(', ')})`,
);

// ---- seeding requires the admin credential -------------------------------
const player = await playerToken('player-1');
const denied = await post(
  A_PORT,
  `/v1/escapement/admin/pools/${POOL}/seed`,
  { codes: ['X'] },
  player,
  'seed-denied-0001',
);
check(denied.status === 403 || denied.status === 401, `a player token cannot seed (${denied.status})`);

const statusDenied = await get(A_PORT, '/v1/escapement/admin/engine', player);
check(
  statusDenied.status === 403,
  `a player token cannot read cluster topology (${statusDenied.status})`,
);

const seeded = await post(
  A_PORT,
  `/v1/escapement/admin/pools/${POOL}/seed`,
  { codes: CODES },
  ADMIN_TOKEN,
  'seed-key-000001',
);
check(seeded.status === 200, `pool seeded (${seeded.status})`);

// ---- claims through BOTH nodes; the follower must forward ----------------
console.log('\n--- 400 concurrent claims, half sent to the follower ---');
const issued = new Map();
async function claim(port, key) {
  const token = await playerToken(key);
  const res = await post(port, `/v1/escapement/pools/${POOL}/claims`, {}, token, key);
  if (res.status !== 200) return { err: res.status, body: await res.text() };
  return res.json();
}

const first = await Promise.all(
  Array.from({ length: 400 }, (_, i) => claim(i % 2 ? B_PORT : A_PORT, `claim-key-${i}`)),
);
let errs = 0;
let dupes = 0;
for (const r of first) {
  if (r.err) {
    errs++;
    continue;
  }
  if (issued.has(r.code)) dupes++;
  issued.set(r.code, true);
}
check(errs === 0, `all 400 claims succeeded (errors=${errs})`);
check(dupes === 0, `no duplicate codes (dupes=${dupes})`);
check(issued.size === 400, `400 distinct codes issued (got ${issued.size})`);

const replay = await claim(B_PORT, 'claim-key-7');
check(
  replay.code === first[7].code,
  `retrying an idempotency key returns the same code (${replay.code} === ${first[7].code})`,
);

// ---- reads are served locally by the follower ----------------------------
// A follower answers from its own state, which trails the leader by up to
// FOLLOW_POLL_MS. That lag is the design, not a defect — reads never touch S3 —
// so the property worth asserting is that it converges.
const immediate = await get(B_PORT, `/v1/escapement/pools/${POOL}`, readToken).then((r) => r.json());
const convergeDeadline = Date.now() + 5000;
let followerStats = immediate;
while (followerStats.claimed !== 400 && Date.now() < convergeDeadline) {
  await sleep(150);
  followerStats = await get(B_PORT, `/v1/escapement/pools/${POOL}`, readToken).then((r) => r.json());
}
check(
  followerStats.claimed === 400 && followerStats.remaining === CODES.length - 400,
  `follower converges to the leader by tailing the log ` +
    `(saw ${immediate.claimed} immediately, ${followerStats.claimed} after catching up)`,
);
check(
  // A follower that has not tailed the seed yet 404s the pool, so `claimed` is
  // absent — that is maximal lag, not a failure.
  (immediate.claimed ?? 0) <= followerStats.claimed,
  'follower reads are served locally and lag rather than blocking on the leader',
);

// ---- what an idle cluster costs ------------------------------------------
// Not a behaviour check — a bill check. A follower polls forever, so anything it
// does per poll is a recurring charge. S3 prices LIST at the PUT rate and bills
// failed conditional requests like any other, so the two habits worth guarding
// against are listing the log to find the head and probing the lease with a PUT
// that is expected to 412. Both should now be plain GETs.
console.log('\n--- idle cost: two nodes, no traffic ---');
const idleFrom = { ...counts };
await sleep(3000);
const idle = {
  get: counts.get - idleFrom.get,
  put: counts.put - idleFrom.put,
  list: counts.list - idleFrom.list,
};
const idleSeconds = 3;
// Only the leader should write while idle, renewing its lease every TTL/3.
const maxRenewals = Math.ceil((idleSeconds * 1000) / (4000 / 3)) + 1;
check(
  idle.list === 0,
  `no LIST while idle — the log head is found by GET (saw ${idle.list})`,
);
check(
  idle.put <= maxRenewals,
  `writes while idle are lease renewals only (saw ${idle.put}, allow ${maxRenewals})`,
);
console.log(
  `    ${idleSeconds}s idle → ${idle.get} GET, ${idle.put} PUT, ${idle.list} LIST`,
);

// ---- quota machine shares the same engine --------------------------------
console.log('\n--- second state machine over the same log ---');
await post(A_PORT, '/v1/escapement/admin/quotas/daily', { limit: 3 }, ADMIN_TOKEN, 'quota-def-0001');
const consumes = [];
let rejectedBody = null;
for (let i = 0; i < 4; i++) {
  const res = await post(
    B_PORT,
    '/v1/escapement/quotas/daily/consume',
    { amount: 1 },
    player,
    `quota-key-${i}`,
  );
  consumes.push(res.status);
  if (res.status === 429) rejectedBody = await res.json();
}
check(
  consumes.filter((s) => s === 200).length === 3 && consumes[3] === 429,
  `quota enforced across nodes: ${consumes.join(',')}`,
);
// The refusal body is part of the published contract, so assert its shape rather
// than only its status — `validateResponses` is off, so nothing else would catch
// the spec and the middleware drifting apart.
check(
  rejectedBody?.code === 'QUOTA_EXCEEDED' &&
    rejectedBody?.errors?.used === 3 &&
    rejectedBody?.errors?.limit === 3 &&
    typeof rejectedBody?.message === 'string',
  `refusal carries code and figures: ${JSON.stringify(rejectedBody)}`,
);

// A per-subject quota has no whole-quota usage figure, so reading one without a
// subject is a 400 rather than a number comparing a total against a per-subject
// ceiling. The player JWT names a subject, so it gets an answer; the static admin
// token does not.
await post(
  A_PORT,
  '/v1/escapement/admin/quotas/per-player',
  { limit: 2, perSubject: true },
  ADMIN_TOKEN,
  'quota-def-0002',
);
await post(A_PORT, '/v1/escapement/quotas/per-player/consume', {}, player, 'quota-key-ps-1');
const asPlayer = await get(A_PORT, '/v1/escapement/quotas/per-player', player).then((r) => r.json());
check(
  asPlayer.used === 1 && asPlayer.remaining === 1 && asPlayer.limit === 2,
  `per-subject read scopes to the caller: used=${asPlayer.used} remaining=${asPlayer.remaining}`,
);
const asOther = await get(
  A_PORT,
  '/v1/escapement/quotas/per-player?subject=someone-else',
  player,
).then((r) => r.json());
check(
  asOther.subject === 'someone-else' && asOther.used === 0 && asOther.remaining === 2,
  `?subject= reports that subject, not the caller: ${JSON.stringify(asOther)}`,
);

// ---- hard kill the leader ------------------------------------------------
console.log('\n--- SIGKILL the leader; the standby must take over ---');
const leaderPort = sa.role === 'leader' ? A_PORT : B_PORT;
const survivorPort = leaderPort === A_PORT ? B_PORT : A_PORT;
(leaderPort === A_PORT ? a : b).kill('SIGKILL');

const t0 = Date.now();
const promotedAt = await waitForRole(survivorPort, 'leader', 20_000);
check(promotedAt !== null, `standby promoted in ${Date.now() - t0}ms (TTL=4000ms)`);

console.log('\n--- 200 more claims against the new leader ---');
const after = await Promise.all(
  Array.from({ length: 200 }, (_, i) => claim(survivorPort, `claim2-key-${i}`)),
);
let postErrs = 0;
let postDupes = 0;
for (const r of after) {
  if (r.err) {
    postErrs++;
    continue;
  }
  if (issued.has(r.code)) postDupes++;
  issued.set(r.code, true);
}
check(postErrs === 0, `all post-failover claims succeeded (errors=${postErrs})`);
check(postDupes === 0, `no code reissued across the leadership change (dupes=${postDupes})`);
check(issued.size === 600, `600 distinct codes total (got ${issued.size})`);

const finalStats = await get(survivorPort, `/v1/escapement/pools/${POOL}`, readToken).then((r) =>
  r.json(),
);
check(
  finalStats.remaining === CODES.length - 600,
  `pool accounting exact: remaining=${finalStats.remaining}, expected=${CODES.length - 600}`,
);

// ---- graceful stop hands the lease over immediately ----------------------
console.log('\n--- graceful SIGTERM: lease released, not waited out ---');
const c = boot(leaderPort, 'C');
await waitForRole(leaderPort, 'follower', 20_000);
(survivorPort === A_PORT ? a : b).kill('SIGTERM');
const t1 = Date.now();
const handedOver = await waitForRole(leaderPort, 'leader', 12_000);
const gracefulMs = Date.now() - t1;
check(handedOver !== null, `graceful handover completed in ${gracefulMs}ms`);
check(
  gracefulMs < 4000,
  `handover beat the 4000ms lease TTL (${gracefulMs}ms) — the lease was released, not expired`,
);

for (const child of [a, b, c]) {
  try {
    child.kill('SIGKILL');
  } catch {
    /* already gone */
  }
}
s3.close();
console.log(`\n${failures === 0 ? 'ALL CHECKS PASSED' : `${failures} CHECK(S) FAILED`}\n`);
process.exit(failures === 0 ? 0 : 1);
