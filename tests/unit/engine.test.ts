import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { Engine } from '../../src/engine/engine';
import type { EngineOptions } from '../../src/engine/engine';
import type { Json } from '../../src/engine/types';
import { poolMachine } from '../../src/machines/pool.machine';
import { quotaMachine } from '../../src/machines/quota.machine';
import { MemoryLogStore, silentLogger } from '../fakes/memory-log-store';

const AT = '2026-08-19T12:00:00.000Z';

/** Poll for a condition instead of sleeping a fixed time — compaction is fire-and-forget. */
async function waitFor(cond: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 5));
  }
}

/** `pruneRetain: 0` unless a test says otherwise, so pruning is easy to observe. */
function buildEngine(store: MemoryLogStore, overrides: Partial<EngineOptions> = {}): Engine {
  return new Engine({
    store: store.asLogStore(),
    machines: [poolMachine, quotaMachine],
    endpoint: 'http://self:3000',
    batchWindowMs: 5,
    maxBatch: 500,
    leaseTtlMs: 30_000,
    followPollMs: 1_000,
    snapshotEvery: 1_000,
    pruneRetain: 0,
    idempotencyLimit: 1_000,
    logger: silentLogger,
    ...overrides,
  });
}

async function seed(engine: Engine, codes: string[]): Promise<void> {
  await engine.submit({
    machine: 'pool',
    key: `seed-${codes.length}-${codes[0] ?? 'none'}`,
    payload: { type: 'seed', pool: 'p', codes },
  });
}

const claim = (engine: Engine, key: string): Promise<Json> =>
  engine.submit({
    machine: 'pool',
    key,
    payload: { type: 'claimNext', pool: 'p', by: key, at: AT },
  });

describe('Engine', () => {
  it('elects itself leader when the lease is free', async () => {
    const store = new MemoryLogStore();
    const engine = buildEngine(store);
    await engine.start();
    assert.equal(engine.role, 'leader');
    assert.equal(store.lease?.endpoint, 'http://self:3000');
    await engine.shutdown(100);
  });

  it('starts as a follower when another node holds a live lease', async () => {
    const store = new MemoryLogStore();
    store.lease = {
      writerId: 'other',
      endpoint: 'http://other:3000',
      expiresAt: Date.now() + 60_000,
      seq: 0,
    };
    const engine = buildEngine(store);
    await engine.start();
    assert.equal(engine.role, 'follower');
    assert.equal(engine.leaderEndpoint, 'http://other:3000');
    await engine.shutdown(100);
  });

  it('refuses to submit on a follower — the HTTP layer forwards instead', async () => {
    const store = new MemoryLogStore();
    store.lease = {
      writerId: 'other',
      endpoint: 'http://other:3000',
      expiresAt: Date.now() + 60_000,
      seq: 0,
    };
    const engine = buildEngine(store);
    await engine.start();
    await assert.rejects(() => claim(engine, 'k'), /not the leader/);
    await engine.shutdown(100);
  });

  it('batches concurrent claims into ONE log entry and issues no duplicates', async () => {
    const store = new MemoryLogStore();
    const engine = buildEngine(store);
    await engine.start();
    await seed(
      engine,
      Array.from({ length: 200 }, (_, i) => `C${i}`),
    );

    const before = store.appendCalls;
    const results = await Promise.all(
      Array.from({ length: 100 }, (_, i) => claim(engine, `k-${i}`)),
    );
    const codes = new Set(results.map((r) => (r as { code: string }).code));

    assert.equal(codes.size, 100, 'every claim got a distinct code');
    assert.equal(
      store.appendCalls - before,
      1,
      'one hundred concurrent claims cost exactly one S3 write',
    );
    await engine.shutdown(100);
  });

  it('replays an idempotency key instead of consuming a second code', async () => {
    const store = new MemoryLogStore();
    const engine = buildEngine(store);
    await engine.start();
    await seed(engine, ['A', 'B', 'C']);

    const first = (await claim(engine, 'same-key')) as { code: string };
    const retry = (await claim(engine, 'same-key')) as { code: string };
    assert.equal(retry.code, first.code);

    const stats = engine.query<Map<string, { free: { size: number } }>, number>(
      'pool',
      (state) => state.get('p')!.free.size,
    );
    assert.equal(stats, 2, 'the retry did not burn a second code');
    await engine.shutdown(100);
  });

  it('collapses two requests that share a key inside the SAME batch', async () => {
    const store = new MemoryLogStore();
    const engine = buildEngine(store);
    await engine.start();
    await seed(engine, ['A', 'B', 'C']);

    // Both are queued before the batch window closes, so neither is in the
    // idempotency map yet — the batch-local guard is what saves us here.
    const [a, b] = await Promise.all([claim(engine, 'dup'), claim(engine, 'dup')]);
    assert.equal((a as { code: string }).code, (b as { code: string }).code);

    const remaining = engine.query<Map<string, { free: { size: number } }>, number>(
      'pool',
      (state) => state.get('p')!.free.size,
    );
    assert.equal(remaining, 2);
    await engine.shutdown(100);
  });

  it('surfaces a machine rejection without failing the rest of the batch', async () => {
    const store = new MemoryLogStore();
    const engine = buildEngine(store);
    await engine.start();
    await seed(engine, ['ONLY']);

    const settled = await Promise.allSettled([claim(engine, 'a'), claim(engine, 'b')]);
    const fulfilled = settled.filter((s) => s.status === 'fulfilled');
    const rejected = settled.filter((s) => s.status === 'rejected');
    assert.equal(fulfilled.length, 1);
    assert.equal(rejected.length, 1);
    assert.match(String((rejected[0] as PromiseRejectedResult).reason), /no unclaimed codes/);
    await engine.shutdown(100);
  });

  it('fails closed when another writer takes the sequence', async () => {
    const store = new MemoryLogStore();
    const fatals: string[] = [];
    const engine = buildEngine(store, { onFatal: (reason) => fatals.push(reason) });
    await engine.start();
    await seed(engine, ['A', 'B']);

    store.fenceNextAppend = true;
    await assert.rejects(() => claim(engine, 'fenced'), /fenced/);
    assert.equal(fatals.length, 1);
    assert.match(fatals[0]!, /another node/);
    // The engine must stop leading rather than keep issuing uncommitted codes.
    assert.notEqual(engine.role, 'leader');
  });

  it('rebuilds exact state from the log after a restart', async () => {
    const store = new MemoryLogStore();
    const first = buildEngine(store);
    await first.start();
    await seed(first, ['A', 'B', 'C', 'D']);
    const claimed = new Set<string>();
    for (const key of ['k1', 'k2']) {
      claimed.add(((await claim(first, key)) as { code: string }).code);
    }
    await first.shutdown(100);

    const second = buildEngine(store);
    await second.start();
    assert.equal(second.sequence, first.sequence);

    // The recovered node must not re-issue anything already handed out.
    const after = new Set<string>();
    for (const key of ['k3', 'k4']) {
      after.add(((await claim(second, key)) as { code: string }).code);
    }
    for (const code of after) assert.ok(!claimed.has(code), `re-issued ${code} after restart`);
    assert.equal(after.size, 2);
    await second.shutdown(100);
  });

  it('replays idempotency records across a restart', async () => {
    const store = new MemoryLogStore();
    const first = buildEngine(store);
    await first.start();
    await seed(first, ['A', 'B']);
    const original = (await claim(first, 'survives-restart')) as { code: string };
    await first.shutdown(100);

    const second = buildEngine(store);
    await second.start();
    const retry = (await claim(second, 'survives-restart')) as { code: string };
    assert.equal(retry.code, original.code, 'a retry after restart must not burn a new code');
    await second.shutdown(100);
  });

  it('recovers from a snapshot once the log has been pruned', async () => {
    const store = new MemoryLogStore();
    // snapshotEvery: 2 compacts aggressively.
    const engine = buildEngine(store, { batchWindowMs: 1, snapshotEvery: 2 });
    await engine.start();
    // seed = commit 1, then three claims. With snapshotEvery=2 that compacts at
    // seq 2 and again at seq 4, and the second pass prunes the whole log.
    await seed(engine, ['A', 'B', 'C', 'D']);
    await claim(engine, 'c1');
    await claim(engine, 'c2');
    await claim(engine, 'c3');
    await waitFor(() => store.snapshots.length >= 2 && store.log.size === 0);
    await engine.shutdown(100);

    assert.ok(store.snapshots.length >= 2, 'snapshots were written');
    assert.equal(store.log.size, 0, 'the log was fully pruned');
    assert.equal(store.snapshots[store.snapshots.length - 1]!.seq, 4);

    const revived = buildEngine(store);
    await revived.start();
    assert.equal(revived.sequence, 4);
    const remaining = revived.query<Map<string, { free: { size: number } }>, number>(
      'pool',
      (state) => state.get('p')!.free.size,
    );
    assert.equal(remaining, 1, 'snapshot alone rebuilt the pool exactly');
    // And idempotency survived compaction, not just the log.
    const replay = (await claim(revived, 'c1')) as { code: string };
    assert.equal(remaining, 1);
    assert.ok(typeof replay.code === 'string');
    await revived.shutdown(100);
  });

  it('rebuilds from a snapshot plus the log entries written after it', async () => {
    const store = new MemoryLogStore();
    const engine = buildEngine(store, { batchWindowMs: 1, snapshotEvery: 2 });
    await engine.start();
    await seed(engine, ['A', 'B', 'C', 'D']);
    await claim(engine, 'x1'); // snapshot at seq 2
    await waitFor(() => store.snapshots.length >= 1);
    await claim(engine, 'x2'); // seq 3 — after the snapshot, still in the log
    await engine.shutdown(100);

    assert.equal(store.snapshots[store.snapshots.length - 1]!.seq, 2);
    assert.deepEqual([...store.log.keys()], [3]);

    const revived = buildEngine(store);
    await revived.start();
    assert.equal(revived.sequence, 3);
    const remaining = revived.query<Map<string, { free: { size: number } }>, number>(
      'pool',
      (state) => state.get('p')!.free.size,
    );
    assert.equal(remaining, 2, 'snapshot + tail replay agree with live state');
    await revived.shutdown(100);
  });

  it('runs several machines over the one log', async () => {
    const store = new MemoryLogStore();
    const engine = buildEngine(store);
    await engine.start();
    await seed(engine, ['A']);
    await engine.submit({
      machine: 'quota',
      key: 'define-1',
      payload: { type: 'define', quota: 'daily', limit: 1, perSubject: false },
    });

    const [poolResult, quotaResult] = await Promise.all([
      claim(engine, 'mixed-pool'),
      engine.submit({
        machine: 'quota',
        key: 'mixed-quota',
        payload: { type: 'consume', quota: 'daily', subject: 'player-1', amount: 1 },
      }),
    ]);
    assert.equal((poolResult as { code: string }).code, 'A');
    assert.equal((quotaResult as { used: number }).used, 1);

    // Both machines' events rode the same batch.
    const lastEntry = [...store.log.values()].pop()!;
    assert.equal(lastEntry.commits.length, 2);
    assert.deepEqual(lastEntry.commits.map((c) => c.machine).sort(), ['pool', 'quota']);
    await engine.shutdown(100);
  });

  it('commits an isolated claim without waiting out the batch window', async () => {
    // Below ~1000/batchWindowMs commands per second the window merges nothing,
    // so paying it is pure latency. A generous window here would dominate the
    // measurement if it were being waited out.
    const store = new MemoryLogStore();
    const engine = buildEngine(store, { batchWindowMs: 400 });
    await engine.start();
    await seed(engine, ['A', 'B']);

    const started = Date.now();
    await claim(engine, 'solo');
    const elapsed = Date.now() - started;

    assert.ok(
      elapsed < 200,
      `an idle leader committed in ${elapsed}ms, well inside the 400ms window`,
    );
    await engine.shutdown(100);
  });

  it('still collapses a burst into one entry with the window skipped', async () => {
    const store = new MemoryLogStore();
    const engine = buildEngine(store, { batchWindowMs: 400 });
    await engine.start();
    await seed(
      engine,
      Array.from({ length: 50 }, (_, i) => `C${i}`),
    );
    const before = store.appendCalls;

    const results = await Promise.all(
      Array.from({ length: 20 }, (_, i) => claim(engine, `burst-${i}`)),
    );
    const codes = new Set(results.map((r) => (r as { code: string }).code));

    assert.equal(codes.size, 20, 'every claimant got a distinct code');
    assert.equal(store.appendCalls - before, 1, 'and they all rode one commit');
    await engine.shutdown(100);
  });

  it('stops at a hole in the log instead of replaying over it', async () => {
    // The old LIST-then-GET walk saw a pruned range simply missing and carried
    // straight on, applying later events onto state that never saw the earlier
    // ones. Stepping seq by seq cannot do that: the walk stops dead.
    const store = new MemoryLogStore();
    const engine = buildEngine(store);
    await engine.start();
    await seed(engine, ['A', 'B', 'C']); // seq 1
    await claim(engine, 'c1'); // seq 2
    await claim(engine, 'c2'); // seq 3
    await engine.shutdown(100);

    store.hideFromRead.add(2); // as though compaction reclaimed it

    const revived = buildEngine(store);
    await revived.start();
    assert.equal(revived.sequence, 1, 'it stopped at the hole rather than jumping it');
    const remaining = revived.query<Map<string, { free: { size: number } }>, number>(
      'pool',
      (state) => state.get('p')!.free.size,
    );
    assert.equal(remaining, 3, 'no event past the hole was applied');
    await revived.shutdown(100);
  });

  it('exits when it has fallen behind the prune horizon', async () => {
    // A 404 means both "end of the log" and "already pruned". The lease carries
    // the leader's committed seq so a follower can tell them apart; being behind
    // the horizon cannot be repaired in place, so it rebuilds.
    const store = new MemoryLogStore();
    store.lease = {
      writerId: 'other',
      endpoint: 'http://other:3000',
      expiresAt: Date.now() + 60_000,
      seq: 9, // the leader is well past anything this node can still read
    };
    const fatals: string[] = [];
    const engine = buildEngine(store, {
      followPollMs: 10,
      onFatal: (reason) => fatals.push(reason),
    });

    // Boot is quiet: nothing has told it where the head is yet.
    await engine.start();
    assert.equal(engine.role, 'follower');
    assert.equal(fatals.length, 0);

    // The first poll reads the lease, the next one acts on it.
    await waitFor(() => fatals.length > 0);
    assert.equal(fatals.length > 0, true, 'it refused to keep serving stale state');
    assert.match(fatals[0]!, /fell behind the prune horizon/);
    await engine.shutdown(100);
  });

  it('keeps a margin of log entries behind the snapshot for in-flight readers', async () => {
    const store = new MemoryLogStore();
    const engine = buildEngine(store, { batchWindowMs: 1, snapshotEvery: 2, pruneRetain: 2 });
    await engine.start();
    await seed(engine, ['A', 'B', 'C', 'D']);
    await claim(engine, 'c1');
    await claim(engine, 'c2');
    await claim(engine, 'c3');
    await waitFor(() => store.snapshots.length >= 1 && store.log.size <= 2);
    await engine.shutdown(100);

    assert.ok(store.snapshots.length >= 1, 'it still compacts');
    assert.deepEqual(
      [...store.log.keys()].sort((a, b) => a - b),
      [3, 4],
      'the two entries behind the head survived the prune',
    );
  });

  it('will not compact while history names a machine this build lacks', async () => {
    // The destructive shape: an older build boots, drops the machine it does not
    // know, then snapshots without it and prunes the log that was its last
    // record. Compaction has to stand down instead.
    const store = new MemoryLogStore();
    const full = buildEngine(store, { batchWindowMs: 1 });
    await full.start();
    await full.submit({
      machine: 'quota',
      key: 'define-1',
      payload: { type: 'define', quota: 'daily', limit: 5, perSubject: false },
    });
    await full.submit({
      machine: 'quota',
      key: 'consume-1',
      payload: { type: 'consume', quota: 'daily', subject: 'player-1', amount: 3 },
    });
    await full.shutdown(100);

    // A build that no longer registers `quota` — a rollback, or a rename.
    // snapshotEvery: 1 compacts at every opportunity.
    const rolledBack = buildEngine(store, {
      machines: [poolMachine],
      batchWindowMs: 1,
      snapshotEvery: 1,
    });
    await rolledBack.start();
    await seed(rolledBack, ['A', 'B']);
    await claim(rolledBack, 'c1');
    await new Promise((r) => setTimeout(r, 80));
    await rolledBack.shutdown(100);

    assert.equal(store.snapshots.length, 0, 'no snapshot may omit the unknown machine');
    assert.ok(store.log.size > 0, 'and the log that still records it is intact');

    // Roll forward: the machine's state is still there, to the event.
    const restored = buildEngine(store);
    await restored.start();
    const used = restored.query<Map<string, { used: number }>, number>(
      'quota',
      (state) => state.get('daily')!.used,
    );
    assert.equal(used, 3, 'quota survived a full rollback cycle');
    await restored.shutdown(100);
  });

  it('releases the lease on graceful shutdown so failover is immediate', async () => {
    const store = new MemoryLogStore();
    const engine = buildEngine(store);
    await engine.start();
    assert.ok(store.lease);
    await engine.shutdown(100);
    assert.equal(store.lease, null);
  });

  it('rejects, not answers, an in-batch duplicate key when the batch is fenced', async () => {
    // The second command shares the first one's key inside a single batch, so
    // its answer IS the first one's provisional result. If the batch fences,
    // that result never committed — delivering it would hand out a code the
    // rebuilt state still considers free.
    const store = new MemoryLogStore();
    const fatals: string[] = [];
    const engine = buildEngine(store, { onFatal: (reason) => fatals.push(reason) });
    await engine.start();
    await seed(engine, ['A', 'B', 'C']);

    store.fenceNextAppend = true;
    const settled = await Promise.allSettled([claim(engine, 'dup'), claim(engine, 'dup')]);
    assert.ok(
      settled.every((s) => s.status === 'rejected'),
      'neither caller got a 200 for a commit that never happened',
    );
    for (const s of settled) {
      assert.match(String(s.reason), /fenced/i);
    }
    assert.equal(fatals.length, 1);
  });

  it('withholds a state-dependent rejection until the batch is durable', async () => {
    // Pool has one code; two claims share a batch. The second is refused only
    // because the first PROVISIONALLY took the last code. When the batch then
    // fences, that history never committed — the refusal must become a
    // retryable 503, not a definitive "exhausted" that was never true.
    const store = new MemoryLogStore();
    const fatals: string[] = [];
    const engine = buildEngine(store, { onFatal: (reason) => fatals.push(reason) });
    await engine.start();
    await seed(engine, ['ONLY']);

    store.fenceNextAppend = true;
    const settled = await Promise.allSettled([claim(engine, 'a'), claim(engine, 'b')]);
    assert.ok(settled.every((s) => s.status === 'rejected'));
    for (const s of settled) {
      const reason = String(s.reason);
      assert.match(reason, /fenced/i, `retryable, not definitive: ${reason}`);
      assert.doesNotMatch(reason, /no unclaimed codes/);
    }
  });

  it('does not answer a write that landed in a pruned slot', async () => {
    // If-None-Match only fences a slot that still exists. A pauses at seq 1;
    // B takes over, commits past it, snapshots, and prunes — deleting the keys.
    // A wakes and appends seq 2: the key is gone, so the conditional write
    // SUCCEEDS. The post-commit leadership check is the only thing standing
    // between A and resolving callers with codes B may already have issued.
    const store = new MemoryLogStore();
    const fatals: string[] = [];
    const engine = buildEngine(store, { onFatal: (reason) => fatals.push(reason) });
    await engine.start();
    await seed(engine, ['A', 'B', 'C']); // seq 1 — this node believes head = 1

    // B's tenure while this node is "paused": B holds the lease, committed to
    // seq 6, and pruned the log behind its snapshot — seq 2 is creatable again.
    store.takeLease({
      writerId: 'B',
      endpoint: 'http://b:3000',
      expiresAt: Date.now() + 60_000,
      seq: 6,
    });
    store.log.delete(1);

    const settled = await Promise.allSettled([claim(engine, 'stale')]);
    assert.ok(store.log.has(2), 'the conditional write really did land in the pruned slot');
    assert.equal(settled[0].status, 'rejected', 'the stale leader must not answer');
    assert.match(String(settled[0].reason), /[Ll]eadership/);
    assert.equal(fatals.length, 1);
    assert.notEqual(engine.role, 'leader');
  });

  it('steps down on renew instead of stealing the lease back', async () => {
    // A leader paused past its TTL wakes to find a peer holding the lease. Its
    // renew is conditional on the etag it last wrote, so it fails — and the
    // node must demote itself, not overwrite the new leader's lease and start
    // advertising arbitrarily stale reads.
    const store = new MemoryLogStore();
    const fatals: string[] = [];
    const engine = buildEngine(store, { onFatal: (reason) => fatals.push(reason) });
    await engine.start();
    assert.equal(engine.role, 'leader');

    const theirs = {
      writerId: 'B',
      endpoint: 'http://b:3000',
      expiresAt: Date.now() + 60_000,
      seq: 5,
    };
    store.takeLease(theirs);

    // Drive the renewal directly rather than waiting out the timer.
    await (engine as unknown as { renewLease(): Promise<void> }).renewLease();

    assert.deepEqual(store.lease, theirs, "the renew did not overwrite the new leader's lease");
    assert.equal(fatals.length, 1);
    assert.match(fatals[0]!, /lease/i);
    assert.notEqual(engine.role, 'leader');
  });

  it("graceful shutdown leaves another node's lease alone", async () => {
    // A drain that outlives the lease TTL means a peer may already have taken
    // over; the shutdown's delete must be verified, or it removes the NEW
    // leader's lease and forces a second, avoidable election.
    const store = new MemoryLogStore();
    const engine = buildEngine(store);
    await engine.start();

    const theirs = {
      writerId: 'B',
      endpoint: 'http://b:3000',
      expiresAt: Date.now() + 60_000,
      seq: 5,
    };
    store.takeLease(theirs);

    await engine.shutdown(100);
    assert.deepEqual(store.lease, theirs, 'the release skipped a lease that is not ours');
  });

  it('settles commands queued behind a fenced batch instead of stranding them', async () => {
    // With maxBatch=1 the second claim is still in the queue when the first
    // one's batch fences. fatal() must flush it — with the default process-exit
    // onFatal nothing observes the leak, but an embedder that keeps the process
    // alive would hold its caller's request open forever.
    const store = new MemoryLogStore();
    const fatals: string[] = [];
    const engine = buildEngine(store, { maxBatch: 1, onFatal: (reason) => fatals.push(reason) });
    await engine.start();
    await seed(engine, ['A', 'B', 'C']);

    store.fenceNextAppend = true;
    const settled = await Promise.allSettled([claim(engine, 'first'), claim(engine, 'second')]);
    assert.ok(
      settled.every((s) => s.status === 'rejected'),
      'nothing behind the failed batch hangs',
    );
    assert.equal(fatals.length, 1);
  });
});
