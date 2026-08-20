/**
 * LogStore against a fault-injecting S3.
 *
 * The engine tests use an in-memory fake, and the failover suite uses a HEALTHY
 * stub — so neither exercises how LogStore itself classifies S3's error codes.
 * That classification IS the commit protocol: a 412 means "the slot is taken,
 * read back and compare", a 500/503/timeout means "outcome unknown, read back
 * and compare", a 404 on read-back means "never landed, retry the same seq",
 * and running out of attempts must throw rather than guess. Each test here pins
 * one of those paths with the actual HTTP status S3 would return.
 *
 * AWS_MAX_ATTEMPTS=1 disables the SDK's own internal retries so every request
 * LogStore makes maps to exactly one HTTP exchange — the retry behavior under
 * test is LogStore's, not the SDK's.
 */

process.env['AWS_ACCESS_KEY_ID'] = 'test';
process.env['AWS_SECRET_ACCESS_KEY'] = 'test';
process.env['AWS_MAX_ATTEMPTS'] = '1';

import assert from 'node:assert/strict';
import { after, before, beforeEach, describe, it } from 'node:test';

import { LogStore } from '../../src/engine/log-store';
import type { Lease, LogEntry } from '../../src/engine/types';
import { startFaultyS3, type FaultyS3 } from '../fakes/faulty-s3';

const PREFIX = 'escapement/test';

const entryAt = (seq: number, batchId: string): LogEntry => ({
  seq,
  batchId,
  writerId: 'me',
  at: '2026-08-20T12:00:00.000Z',
  commits: [],
});

const leaseOf = (writerId: string, ttlMs: number): Lease => ({
  writerId,
  endpoint: `http://${writerId}:3000`,
  expiresAt: Date.now() + ttlMs,
  seq: 0,
});

describe('LogStore against S3 error codes', () => {
  let s3: FaultyS3;
  let store: LogStore;

  before(async () => {
    s3 = await startFaultyS3(2 /* tiny LIST pages, so pagination is exercised */);
  });
  after(async () => {
    await s3.close();
  });
  beforeEach(() => {
    s3.store.clear();
    s3.faults.length = 0;
  });

  const build = (): LogStore =>
    new LogStore({ bucket: 'bucket', prefix: PREFIX, region: 'us-east-1', endpoint: s3.url });

  // -- append: the "did my write land?" disambiguation ------------------------

  it('commits through a transient 500 on the PUT', async () => {
    // InternalError with the write NOT landing: the read-back finds nothing
    // (404), which means "never landed — retry the same seq", and the retry
    // succeeds. The caller must see a clean 'committed', not the blip.
    store = build();
    s3.faults.push({
      method: 'PUT',
      keyIncludes: 'log/',
      times: 1,
      status: 500,
      code: 'InternalError',
    });

    assert.equal(await store.append(entryAt(1, 'batch-1')), 'committed');
    assert.ok(s3.store.has(`${PREFIX}/log/${'1'.padStart(12, '0')}.json`));
  });

  it('recognizes its own write when the success response was lost', async () => {
    // The ambiguous case the batchId exists for: the PUT lands but the 200 is
    // lost (timeout, dropped connection — modeled as a 500 with storeBody).
    // The retry's If-None-Match then hits its OWN earlier write with a 412,
    // and only the read-back comparing batchId can tell "that's me, committed"
    // from "that's another writer, fenced". Getting this wrong either fails a
    // durable commit or, worse, double-writes the slot.
    store = build();
    s3.faults.push({
      method: 'PUT',
      keyIncludes: 'log/',
      times: 1,
      status: 500,
      code: 'InternalError',
      storeBody: true,
    });

    assert.equal(await store.append(entryAt(1, 'batch-mine')), 'committed');
    const stored = JSON.parse(
      s3.store.get(`${PREFIX}/log/${'1'.padStart(12, '0')}.json`)!.body.toString(),
    ) as LogEntry;
    assert.equal(stored.batchId, 'batch-mine', 'the slot holds exactly the one ambiguous write');
  });

  it('returns fenced when the slot already belongs to another writer', async () => {
    // A genuine 412 from S3 (not injected): the conditional write is the
    // safety mechanism, and the read-back comparing batchId is what turns
    // "precondition failed" into the verdict the engine acts on.
    store = build();
    s3.setObject(`${PREFIX}/log/${'1'.padStart(12, '0')}.json`, entryAt(1, 'someone-elses'));

    assert.equal(await store.append(entryAt(1, 'batch-mine')), 'fenced');
  });

  it('survives a SlowDown 503 the same as any other transient', async () => {
    // S3's throttle code. It must land in the "outcome unknown" path (back
    // off, read back, retry) — not be treated as a fence or a hard failure.
    store = build();
    s3.faults.push({ method: 'PUT', keyIncludes: 'log/', times: 1, status: 503, code: 'SlowDown' });

    assert.equal(await store.append(entryAt(1, 'batch-1')), 'committed');
  });

  it('throws rather than guesses when the outcome stays unknown', async () => {
    // Every PUT fails and every read-back fails, so "did my write land?" has
    // no answer inside the attempt budget. Returning 'committed' could
    // double-issue; returning 'fenced' could deny a durable commit. The only
    // safe move is to throw — the engine turns that into fatal, and the
    // rebuild reads the log to learn the truth.
    store = build();
    s3.faults.push(
      { method: 'PUT', keyIncludes: 'log/', times: Infinity, status: 500, code: 'InternalError' },
      { method: 'GET', keyIncludes: 'log/', times: Infinity, status: 500, code: 'InternalError' },
    );

    await assert.rejects(
      () => store.append(entryAt(1, 'batch-1'), 2),
      /still unknown after 2 attempts/,
    );
    s3.faults.length = 0;
    assert.equal(await store.readLog(1), null, 'and indeed nothing was written');
  });

  // -- reads: a 500 is not a 404 ----------------------------------------------

  it('propagates a non-404 read error instead of reporting a hole', async () => {
    // catchUp treats null as "the head of the log" and stops replaying. If a
    // 500 were swallowed into null, a transient S3 error would silently
    // truncate recovery and the node would serve (and extend!) a shortened
    // history. Only NoSuchKey/404 may mean "not there".
    store = build();
    s3.setObject(`${PREFIX}/log/${'1'.padStart(12, '0')}.json`, entryAt(1, 'batch-1'));
    s3.faults.push({
      method: 'GET',
      keyIncludes: 'log/',
      times: 1,
      status: 500,
      code: 'InternalError',
    });

    await assert.rejects(() => store.readLog(1));
    assert.ok((await store.readLog(1)) !== null, 'the entry was there all along');
  });

  // -- snapshot discovery ------------------------------------------------------

  it('finds the newest snapshot across LIST pages', async () => {
    // The stub serves two keys per page, so three snapshots force pagination.
    // A LogStore that only read the first page would pick an OLD snapshot as
    // "latest" — recovery would silently start further back than it needs to,
    // or, combined with a pruned log, fail entirely.
    store = build();
    for (const seq of [10, 20, 30]) {
      await store.writeSnapshot({ seq, at: 'x', machines: {}, idempotency: [] });
    }

    const latest = await store.readLatestSnapshot();
    assert.equal(latest?.seq, 30, 'the newest snapshot came from the second page');
  });

  // -- lease races: each conditional-write failure reports the real winner -----

  it('reports the actual winner after losing the create race', async () => {
    // Two cold nodes race the If-None-Match create. The loser's 412 must be
    // followed by a re-read so it points its forwarder at the node that WON —
    // not at itself, and not at nothing.
    store = build();
    const winner = leaseOf('winner', 60_000);
    s3.faults.push({
      method: 'PUT',
      keyIncludes: 'lease.json',
      times: 1,
      status: 412,
      code: 'PreconditionFailed',
      // The winner's create lands "between" our GET and our PUT.
      then: () => s3.setObject(`${PREFIX}/lease.json`, winner),
    });

    const result = await store.tryAcquireLease(leaseOf('me', 60_000));
    assert.equal(result.won, false);
    assert.equal(
      (result as { held: Lease | null }).held?.writerId,
      'winner',
      'the loser learned who to forward to',
    );
  });

  it('reports the new holder after losing the expired-lease CAS', async () => {
    // Two followers watch the same expired lease; the If-Match swap lets only
    // one through. The loser must re-read and report the peer that took over,
    // not the DEAD leader it originally read.
    store = build();
    s3.setObject(`${PREFIX}/lease.json`, leaseOf('dead-leader', -1));
    const winner = leaseOf('winner', 60_000);
    s3.faults.push({
      method: 'PUT',
      keyIncludes: 'lease.json',
      times: 1,
      status: 412,
      code: 'PreconditionFailed',
      then: () => s3.setObject(`${PREFIX}/lease.json`, winner),
    });

    const result = await store.tryAcquireLease(leaseOf('me', 60_000));
    assert.equal(result.won, false);
    assert.equal((result as { held: Lease | null }).held?.writerId, 'winner');
  });

  it('renewal fails with a genuine 412 once the lease is replaced', async () => {
    // Not injected: the stub's real If-Match check refuses the stale etag.
    // This is the exact wire-level exchange behind the engine's step-down —
    // the etag from OUR last write no longer matches, so the renew cannot
    // silently overwrite the new leader.
    store = build();
    const acquired = await store.tryAcquireLease(leaseOf('me', 60_000));
    assert.equal(acquired.won, true);
    const myEtag = (acquired as { etag: string | undefined }).etag!;

    s3.setObject(`${PREFIX}/lease.json`, leaseOf('usurper', 60_000)); // new etag

    await assert.rejects(() => store.renewLease(leaseOf('me', 60_000), myEtag));
    const held = await store.readLease();
    assert.equal(held?.lease.writerId, 'usurper', 'the renew changed nothing');
  });

  it('release deletes only a lease that is still its own', async () => {
    // The verified delete: S3 has no conditional DELETE, so release reads
    // first. Deleting a peer's lease would force a needless second election.
    store = build();
    s3.setObject(`${PREFIX}/lease.json`, leaseOf('current-leader', 60_000));

    await store.releaseLease('a-node-that-lost-it');
    assert.equal((await store.readLease())?.lease.writerId, 'current-leader', 'left alone');

    await store.releaseLease('current-leader');
    assert.equal(await store.readLease(), null, 'the holder itself may release');
  });
});
