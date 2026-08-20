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

    // Both submits push onto the queue synchronously (inside Promise.all's
    // argument evaluation), and commitLoop's splice only runs in a later
    // microtask — so sharing one batch is deterministic, not a window race.
    // Neither key is in the idempotency map yet; the batch-local guard is
    // what saves us here.
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
    const c1 = (await claim(engine, 'c1')) as { code: string };
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
    // And idempotency survived compaction, not just the log: the retry must
    // replay the ORIGINAL code and leave the pool untouched.
    const replay = (await claim(revived, 'c1')) as { code: string };
    assert.equal(replay.code, c1.code, 'the retry replayed the code issued before compaction');
    const afterReplay = revived.query<Map<string, { free: { size: number } }>, number>(
      'pool',
      (state) => state.get('p')!.free.size,
    );
    assert.equal(afterReplay, 1, 'the replay did not burn a fresh code');
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

    // The discriminant is the 400ms window itself: a leader that waited it out
    // cannot finish under it. The bound leaves headroom for CI scheduling
    // noise while still failing decisively if the window is ever paid.
    assert.ok(
      elapsed < 300,
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

    // The first poll reads the lease, the next one acts on it. waitFor throws
    // on timeout, so reaching the assertion means it refused to keep serving.
    await waitFor(() => fatals.length > 0);
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
    // seq 6, snapshotted, and pruned the log behind its retain margin — so
    // seqs 1–4 are deleted (2 is creatable again) while B's recent tail
    // survives. This node still believes the head is seq 1.
    store.takeLease({
      writerId: 'B',
      endpoint: 'http://b:3000',
      expiresAt: Date.now() + 60_000,
      seq: 6,
    });
    store.log.delete(1);
    for (const seq of [5, 6]) {
      store.log.set(seq, {
        seq,
        batchId: `b-batch-${seq}`,
        writerId: 'B',
        at: AT,
        commits: [],
      });
    }

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
    // leader's lease and forces a second, avoidable election. Scope note: the
    // fake mirrors LogStore's verified delete, so this proves the ENGINE asks
    // to release with its own nodeId; the production read-then-delete itself
    // is pinned by log-store.test.ts against real HTTP semantics.
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
  // -- batching across machines and callers -----------------------------------

  it('settles a mixed multi-machine batch per command, in one S3 write', async () => {
    // One batch carrying four commands across two machines, where each machine
    // both commits and rejects. This is the shape production traffic actually
    // has — many callers, several machines, one group commit — and the property
    // that matters is that outcomes stay per-command: one caller's rejection is
    // decided against the batch's provisional state and delivered as such,
    // while the whole thing still costs exactly one append.
    const store = new MemoryLogStore();
    const engine = buildEngine(store);
    await engine.start();
    await seed(engine, ['ONLY']); // pool has one code
    await engine.submit({
      machine: 'quota',
      key: 'define-mixed',
      payload: { type: 'define', quota: 'q', limit: 1, perSubject: false },
    });

    const before = store.appendCalls;
    const settled = await Promise.allSettled([
      claim(engine, 'pool-wins'),
      claim(engine, 'pool-loses'),
      engine.submit({
        machine: 'quota',
        key: 'quota-wins',
        payload: { type: 'consume', quota: 'q', subject: 's1', amount: 1 },
      }),
      engine.submit({
        machine: 'quota',
        key: 'quota-loses',
        payload: { type: 'consume', quota: 'q', subject: 's2', amount: 1 },
      }),
    ]);

    assert.equal(settled[0].status, 'fulfilled', 'first pool claim got the code');
    assert.equal(settled[2].status, 'fulfilled', 'first quota consume fit the ceiling');
    assert.equal(settled[1].status, 'rejected', 'second pool claim was refused');
    assert.match(String(settled[1].reason), /no unclaimed codes/);
    assert.equal(settled[3].status, 'rejected', 'second quota consume was refused');
    assert.match(String(settled[3].reason), /would be exceeded/);

    assert.equal(store.appendCalls - before, 1, 'four commands, two machines, one S3 write');
    const lastEntry = [...store.log.values()].pop()!;
    assert.deepEqual(
      lastEntry.commits.map((c) => c.machine).sort(),
      ['pool', 'quota'],
      'both machines committed into the same log entry',
    );
    await engine.shutdown(100);
  });

  it('a machine whose decide() throws fails only its own command', async () => {
    // decide() throwing is a machine bug, but the engine promises it cannot
    // take the batch down: the broken command gets a 500 and everything else
    // in the batch commits normally. Without this isolation one bad machine
    // would poison every concurrent caller sharing its 50ms window.
    const broken = {
      name: 'broken',
      init: (): Record<string, never> => ({}),
      decide: (): never => {
        throw new Error('machine bug');
      },
      apply: (s: Record<string, never>): Record<string, never> => s,
      snapshot: (): Json => ({}),
      restore: (): Record<string, never> => ({}),
    };
    const store = new MemoryLogStore();
    const engine = buildEngine(store, { machines: [poolMachine, broken] });
    await engine.start();
    await seed(engine, ['A']);

    const settled = await Promise.allSettled([
      engine.submit({ machine: 'broken', key: 'boom', payload: {} }),
      claim(engine, 'healthy'),
    ]);
    assert.equal(settled[0].status, 'rejected');
    assert.match(String(settled[0].reason), /failed to handle/);
    assert.equal(settled[1].status, 'fulfilled', 'the healthy command still committed');
    assert.equal(engine.role, 'leader', 'a machine bug is not a reason to stop leading');
    await engine.shutdown(100);
  });

  it("withholds an 'immediate' answer decided against provisional state on a fence", async () => {
    // An identical quota redefinition normally answers without an S3 write. But
    // when the ORIGINAL definition is a provisional commit in the same batch,
    // the "identical" comparison read state that may never become durable — so
    // if the batch fences, the immediate answer must be withheld too, exactly
    // like the rejections. Otherwise a caller is told "already defined" about
    // a definition that never happened.
    const store = new MemoryLogStore();
    const fatals: string[] = [];
    const engine = buildEngine(store, { onFatal: (reason) => fatals.push(reason) });
    await engine.start();

    store.fenceNextAppend = true;
    const define = (key: string): Promise<Json> =>
      engine.submit({
        machine: 'quota',
        key,
        payload: { type: 'define', quota: 'fresh', limit: 5, perSubject: false },
      });
    const settled = await Promise.allSettled([define('d1'), define('d2')]);
    assert.ok(
      settled.every((s) => s.status === 'rejected'),
      'neither the commit nor its in-batch echo was answered',
    );
    for (const s of settled) {
      assert.match(String(s.reason), /fenced/i);
    }
    assert.equal(fatals.length, 1);
  });

  it('splits a burst across maxBatch-sized commits without issuing duplicates', async () => {
    // maxBatch bounds the log-entry size, so a burst larger than it rides
    // several appends. The invariants must hold ACROSS the split, not just
    // within one batch: every claimant a distinct code, and exactly
    // ceil(claims / maxBatch) writes.
    const store = new MemoryLogStore();
    const engine = buildEngine(store, { maxBatch: 25, batchWindowMs: 1 });
    await engine.start();
    await seed(
      engine,
      Array.from({ length: 100 }, (_, i) => `C${i}`),
    );

    const before = store.appendCalls;
    const results = await Promise.all(
      Array.from({ length: 100 }, (_, i) => claim(engine, `burst-${i}`)),
    );
    const codes = new Set(results.map((r) => (r as { code: string }).code));
    assert.equal(codes.size, 100, 'no duplicate across batch boundaries');
    assert.equal(store.appendCalls - before, 4, '100 claims over maxBatch=25 is four writes');
    await engine.shutdown(100);
  });

  // -- error paths -------------------------------------------------------------

  it('fails closed when a commit outcome cannot be determined', async () => {
    // LogStore.append throws when, after all its retries, it still cannot tell
    // whether the write landed. Resolving would risk double-issuing; rejecting
    // with a definitive error would deny a commit that may exist. The only safe
    // answer is a retryable failure for every caller in the batch and an exit —
    // the rebuild reads the log and learns the truth.
    const store = new MemoryLogStore();
    const fatals: string[] = [];
    const engine = buildEngine(store, { onFatal: (reason) => fatals.push(reason) });
    await engine.start();
    await seed(engine, ['A', 'B']);

    store.throwNextAppend = true;
    const settled = await Promise.allSettled([claim(engine, 'u1'), claim(engine, 'u2')]);
    assert.ok(settled.every((s) => s.status === 'rejected'));
    assert.equal(fatals.length, 1);
    assert.match(fatals[0]!, /irrecoverably/);
    assert.notEqual(engine.role, 'leader');
  });

  it('rides out a transient error in the post-commit leadership check', async () => {
    // The leadership confirmation retries a few times before failing closed.
    // A single S3 blip on that GET must not turn a perfectly good commit into
    // an exit — only a persistent inability to answer does.
    const store = new MemoryLogStore();
    const fatals: string[] = [];
    const engine = buildEngine(store, { onFatal: (reason) => fatals.push(reason) });
    await engine.start();
    await seed(engine, ['A']);

    store.throwNextReadLease = true;
    const result = (await claim(engine, 'blip')) as { code: string };
    assert.equal(result.code, 'A', 'the commit was answered despite the blip');
    assert.equal(fatals.length, 0);
    assert.equal(engine.role, 'leader');
    await engine.shutdown(100);
  });

  it('a commit orphaned by a lost lease is still honored after the rebuild', async () => {
    // The post-commit leadership check rejects callers with a retryable 503
    // even though their write IS durable. That is only safe if a retry against
    // the next incarnation replays the committed result instead of issuing a
    // second code — this test walks that full recovery path.
    const store = new MemoryLogStore();
    const fatals: string[] = [];
    const engine = buildEngine(store, { onFatal: (reason) => fatals.push(reason) });
    await engine.start();
    await seed(engine, ['A', 'B']); // seq 1

    // A peer takes the lease while the claim's append is in flight; the claim
    // commits at seq 2 but must not be answered.
    store.takeLease({
      writerId: 'B',
      endpoint: 'http://b:3000',
      expiresAt: Date.now() - 1, // already expired, so the next node can win
      seq: 1,
    });
    await assert.rejects(() => claim(engine, 'orphaned'), /Leadership/);
    assert.equal(fatals.length, 1);
    assert.ok(store.log.has(2), 'the claim IS durable in the log');

    // The restarted node replays seq 2, so the caller's retry with the same
    // idempotency key gets the code that committed — not a fresh one.
    const revived = buildEngine(store);
    await revived.start();
    assert.equal(revived.role, 'leader');
    const retried = (await claim(revived, 'orphaned')) as { code: string };
    const other = (await claim(revived, 'someone-else')) as { code: string };
    assert.notEqual(retried.code, other.code);
    assert.deepEqual(
      [retried.code, other.code].sort(),
      ['A', 'B'],
      'exactly the two seeded codes exist: nothing double-issued, nothing lost',
    );
    await revived.shutdown(100);
  });

  it('a retry after idempotency eviction burns a new code — the documented bound', async () => {
    // IDEMPOTENCY_LIMIT is the window in which a client retry is safe; this
    // test pins that boundary so a change to the eviction policy is a
    // deliberate decision, not an accident. Outside the window a retry is a
    // new command, and a second code is the expected (documented) cost.
    const store = new MemoryLogStore();
    const engine = buildEngine(store, { idempotencyLimit: 1 });
    await engine.start();
    await seed(engine, ['A', 'B', 'C']);

    const first = (await claim(engine, 'evicted')) as { code: string };
    await claim(engine, 'filler'); // evicts 'evicted' from the 1-entry window
    const retry = (await claim(engine, 'evicted')) as { code: string };
    assert.notEqual(retry.code, first.code, 'outside the window, a retry is a new claim');

    const remaining = engine.query<Map<string, { free: { size: number } }>, number>(
      'pool',
      (state) => state.get('p')!.free.size,
    );
    assert.equal(remaining, 0, 'three claims spent all three codes');
    await engine.shutdown(100);
  });

  it('rejects a command for an unregistered machine without touching the queue', async () => {
    // A typo'd machine name must fail fast at submit — not sit in a batch, and
    // certainly not reach the log, where an unknown name would poison every
    // future replay of this history.
    const store = new MemoryLogStore();
    const engine = buildEngine(store);
    await engine.start();
    const before = store.appendCalls;
    await assert.rejects(
      () => engine.submit({ machine: 'nope', key: 'k', payload: {} }),
      /No state machine named 'nope'/,
    );
    assert.equal(store.appendCalls, before, 'nothing was written');
    await engine.shutdown(100);
  });

  // -- election under a shared store --------------------------------------------

  it('a follower promotes itself once the lease expires', async () => {
    // The unit-level version of hard-kill failover: the leader is simply gone
    // (its lease ages out), and the follower's poll must take over via the
    // expired-lease CAS rather than waiting for anything else.
    const store = new MemoryLogStore();
    store.lease = {
      writerId: 'dead-leader',
      endpoint: 'http://dead:3000',
      expiresAt: Date.now() + 150,
      seq: 0,
    };
    const engine = buildEngine(store, { followPollMs: 25 });
    await engine.start();
    assert.equal(engine.role, 'follower', 'the lease was live at boot');

    await waitFor(() => engine.role === 'leader');
    assert.equal(store.lease?.writerId, engine.nodeId, 'the lease now names this node');
    await engine.shutdown(100);
  });

  it('a follower tails the leader, then takes a graceful hand-off without re-issuing', async () => {
    // Two engines over one store — the unit-level version of the failover
    // suite's happy path. It pins three properties at once: a follower's state
    // converges by replaying the log (reads work there), a graceful shutdown
    // hands the lease over rather than waiting out the TTL, and the successor
    // issues only codes the first leader never touched.
    const store = new MemoryLogStore();
    const leader = buildEngine(store);
    await leader.start();
    await seed(leader, ['A', 'B', 'C', 'D']);

    const follower = buildEngine(store, { followPollMs: 20 });
    await follower.start();
    assert.equal(follower.role, 'follower');

    const taken = new Set<string>();
    taken.add(((await claim(leader, 'lk1')) as { code: string }).code);
    taken.add(((await claim(leader, 'lk2')) as { code: string }).code);

    await waitFor(() => follower.sequence === leader.sequence);
    const seen = follower.query<Map<string, { free: { size: number } }>, number>(
      'pool',
      (state) => state.get('p')!.free.size,
    );
    assert.equal(seen, 2, 'the follower converged by tailing the log');

    await leader.shutdown(100); // releases the lease — no TTL wait
    await waitFor(() => follower.role === 'leader');

    taken.add(((await claim(follower, 'fk1')) as { code: string }).code);
    taken.add(((await claim(follower, 'fk2')) as { code: string }).code);
    assert.equal(taken.size, 4, 'no code was issued twice across the hand-off');
    await follower.shutdown(100);
  });
});
