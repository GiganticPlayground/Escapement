/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-assignment */
/**
 * The escapement.
 *
 * Concurrent requests arrive continuously; this releases them one batch at a
 * time, in order, and only after the batch is durable. Every claim decision
 * happens in memory against authoritative state — microseconds — and the only
 * slow part is proving it durable before answering.
 *
 * Roles: exactly one node holds the lease and runs `commitLoop`. The others tail
 * the log so their state stays warm, forward mutations to the leader (see
 * `src/services/forwarder.ts`), and promote themselves when the lease goes
 * stale.
 */

import { randomUUID } from 'node:crypto';

import { LogStore } from './log-store';
import type {
  Command,
  CommitRecord,
  Decision,
  Json,
  Lease,
  LogEntry,
  SnapshotFile,
  StateMachine,
} from './types';
import { HttpError, UpstreamUnavailableError } from '../utils/http-error';

export type Role = 'starting' | 'leader' | 'follower';

export interface EngineOptions {
  store: LogStore;
  machines: StateMachine<any, any, any, any>[];
  /** Address peers use to reach this node. In Swarm: `http://{{.Task.Name}}:PORT`. */
  endpoint: string;
  /** Hold the door open this long collecting concurrent commands into one PUT. */
  batchWindowMs: number;
  maxBatch: number;
  leaseTtlMs: number;
  followPollMs: number;
  snapshotEvery: number;
  /**
   * Log entries kept behind a snapshot rather than pruned immediately.
   *
   * A reader LISTs the log and then GETs each key. Pruning to the head lets a
   * delete land between those two calls, so the reader asks for a key that no
   * longer exists. Retaining a margin keeps anything a reader could plausibly
   * have just listed fetchable; it must comfortably exceed the commits a
   * follower can miss in one `followPollMs`.
   */
  pruneRetain: number;
  /** How many idempotency records to keep in memory and in the snapshot. */
  idempotencyLimit: number;
  logger: {
    info: (msg: string, meta?: unknown) => void;
    warn: (msg: string, meta?: unknown) => void;
    error: (msg: string, meta?: unknown) => void;
  };
  /** Called when the engine determines it must not continue. Defaults to process.exit(1). */
  onFatal?: (reason: string) => void;
}

interface Registered {
  machine: StateMachine<any, any, any, any>;
  state: any;
}

interface Pending {
  command: Command;
  resolve: (result: Json) => void;
  reject: (err: Error) => void;
}

interface IdempotencyRecord {
  machine: string;
  result: Json;
}

export class Engine {
  readonly nodeId = randomUUID();
  role: Role = 'starting';
  draining = false;
  leaderEndpoint: string | undefined;

  private readonly opts: EngineOptions;
  private readonly store: LogStore;
  private readonly registry = new Map<string, Registered>();
  /** Insertion-ordered, so the oldest entry is the first key — a cheap LRU-by-age. */
  private readonly idempotency = new Map<string, IdempotencyRecord>();

  /**
   * Machines named by history that this build does not register. Non-empty means
   * this process cannot represent the full state, so it must not compact — see
   * `compact`.
   */
  private readonly unknownMachines = new Set<string>();

  /**
   * Highest `seq` any lease we have read claimed to have committed. A lower
   * bound on the log head, used only to tell the end of the log from a hole
   * compaction left behind it. Zero until we have seen someone else's lease.
   */
  private leaseHeadHint = 0;

  private queue: Pending[] = [];
  private wake: (() => void) | null = null;
  private seq = 0;
  private commitsSinceSnapshot = 0;
  private leaseTimer: NodeJS.Timeout | undefined;

  constructor(opts: EngineOptions) {
    this.opts = opts;
    this.store = opts.store;
    for (const machine of opts.machines) {
      this.registry.set(machine.name, { machine, state: machine.init() });
    }
  }

  // -- introspection --------------------------------------------------------

  /** Address peers use to reach this node; recorded in the lease. */
  get endpoint(): string {
    return this.opts.endpoint;
  }

  get machineNames(): string[] {
    return [...this.registry.keys()];
  }
  get sequence(): number {
    return this.seq;
  }
  get queueDepth(): number {
    return this.queue.length;
  }

  /**
   * Read a machine's state directly. This is the whole reason reads are cheap:
   * they never touch S3, and they work on followers too (a fraction of a second
   * behind the leader).
   */
  query<S, T>(machineName: string, fn: (state: S) => T): T {
    const entry = this.registry.get(machineName);
    if (!entry) throw new HttpError(500, `No state machine named '${machineName}'`);
    return fn(entry.state as S);
  }

  // -- boot -----------------------------------------------------------------

  async start(): Promise<void> {
    await this.catchUp(true);

    const acquired = await this.store.tryAcquireLease(this.buildLease());
    if (acquired.won) {
      this.becomeLeader();
    } else {
      this.leaderEndpoint = acquired.held.endpoint;
      this.noteLeaseHead(acquired.held);
      this.becomeFollower();
    }
  }

  /**
   * Remember how far the lease holder said it had committed.
   *
   * Monotonic, because a lease is renewed on a timer and can therefore be read
   * out of order with respect to the log; the highest value we have ever seen is
   * the only one that is still a valid lower bound on the head.
   */
  private noteLeaseHead(held: Lease): void {
    if (held.seq > this.leaseHeadHint) this.leaseHeadHint = held.seq;
  }

  /**
   * Load the newest snapshot (once), then replay forward one entry at a time.
   *
   * Walks `seq + 1`, `seq + 2`, ... and stops at the first key that is not
   * there. It does NOT list the log first, and that is a cost decision more than
   * a design one: sequence numbers are dense, so a LIST tells us nothing a GET
   * would not, and S3 prices LIST at the write rate — 12.5x a GET. A follower
   * runs this every `followPollMs` forever, so the listing was the single most
   * expensive thing an idle cluster did.
   *
   * The walk is also strictly safer than the listing was. A LIST reports the
   * keys that exist, so a follower lagging behind the prune horizon saw the
   * pruned range simply missing and replayed straight over the gap — applying
   * later events onto state that never saw the earlier ones, with `seq` ending
   * up at the head so nothing downstream could tell. Stepping one at a time
   * cannot skip: an absent key stops the walk dead.
   *
   * That leaves one ambiguity, since a 404 means both "nothing has been
   * committed past here" and "what was committed past here is already pruned".
   * `leaseHeadHint` separates them — see `Lease.seq`. Being behind the horizon
   * is unrecoverable in place, so it exits and rebuilds from the snapshot.
   */
  private async catchUp(includeSnapshot = false): Promise<void> {
    if (includeSnapshot && this.seq === 0) {
      const snap = await this.store.readLatestSnapshot();
      if (snap) this.restoreSnapshot(snap);
    }
    for (;;) {
      const next = this.seq + 1;
      const entry = await this.store.readLog(next);
      if (!entry) {
        if (this.leaseHeadHint > this.seq) {
          // The leader told us it had committed past here, yet the entry is
          // gone: this node fell far enough behind that compaction reclaimed
          // what it still needed. There is no way back from in-memory state, so
          // do what every other undeterminable case does and rebuild.
          const reason =
            `log entry ${next} is missing but the leader reports seq ${this.leaseHeadHint} — ` +
            `this node fell behind the prune horizon; restarting to rebuild`;
          this.fatal(reason);
          throw new UpstreamUnavailableError(reason);
        }
        return; // genuinely the head of the log
      }
      this.replay(entry);
      this.seq = entry.seq;
    }
  }

  private restoreSnapshot(snap: SnapshotFile): void {
    for (const [name, raw] of Object.entries(snap.machines)) {
      const entry = this.registry.get(name);
      if (!entry) {
        // A machine present in history but not registered in this build. Keeping
        // its snapshot out of memory is fine; refusing to boot is not, or a
        // rollback could never start.
        this.opts.logger.warn('snapshot contains an unregistered machine', { machine: name });
        this.unknownMachines.add(name);
        continue;
      }
      entry.state = entry.machine.restore(raw);
    }
    for (const rec of snap.idempotency) {
      this.rememberIdempotent(rec.key, { machine: rec.machine, result: rec.result });
    }
    this.seq = snap.seq;
  }

  /** Apply a committed entry. Never rejects — everything in the log already happened. */
  private replay(entry: LogEntry): void {
    for (const commit of entry.commits) {
      const reg = this.registry.get(commit.machine);
      if (!reg) {
        this.opts.logger.warn('log entry names an unregistered machine', {
          machine: commit.machine,
          seq: entry.seq,
        });
        this.unknownMachines.add(commit.machine);
        continue;
      }
      for (const event of commit.events) {
        reg.state = reg.machine.apply(reg.state, event);
      }
      this.rememberIdempotent(commit.key, { machine: commit.machine, result: commit.result });
    }
  }

  private rememberIdempotent(key: string, rec: IdempotencyRecord): void {
    // Re-inserting moves the key to the end, so active keys survive eviction.
    this.idempotency.delete(key);
    this.idempotency.set(key, rec);
    while (this.idempotency.size > this.opts.idempotencyLimit) {
      const oldest = this.idempotency.keys().next();
      if (oldest.done) break;
      this.idempotency.delete(oldest.value);
    }
  }

  // -- roles ----------------------------------------------------------------

  private buildLease(): Lease {
    return {
      writerId: this.nodeId,
      endpoint: this.opts.endpoint,
      expiresAt: Date.now() + this.opts.leaseTtlMs,
      seq: this.seq,
    };
  }

  private becomeLeader(): void {
    this.role = 'leader';
    this.leaderEndpoint = this.opts.endpoint;
    this.leaseTimer = setInterval(
      () => {
        void this.renewLease();
      },
      Math.max(1000, Math.floor(this.opts.leaseTtlMs / 3)),
    );
    // A heartbeat should never be the reason the process stays alive.
    this.leaseTimer.unref();
    this.opts.logger.info('became leader', { nodeId: this.nodeId, seq: this.seq });
    void this.commitLoop();
  }

  private becomeFollower(): void {
    this.role = 'follower';
    this.opts.logger.info('became follower', {
      nodeId: this.nodeId,
      seq: this.seq,
      leader: this.leaderEndpoint,
    });
    void this.followLoop();
  }

  private async renewLease(): Promise<void> {
    if (this.role !== 'leader' || this.draining) return;
    try {
      await this.store.renewLease(this.buildLease());
    } catch (err) {
      // Survivable: the log CAS still fences us if someone else takes over.
      this.opts.logger.warn('lease renew failed', { err: String(err) });
    }
  }

  /** Tail the log to stay warm; promote when the leader's lease goes stale. */
  private async followLoop(): Promise<void> {
    while (this.role === 'follower' && !this.draining) {
      await new Promise((r) => setTimeout(r, this.opts.followPollMs));
      try {
        await this.catchUp();
        const acquired = await this.store.tryAcquireLease(this.buildLease());
        if (acquired.won) {
          await this.catchUp(); // final read before accepting writes
          this.becomeLeader();
          return;
        }
        this.leaderEndpoint = acquired.held.endpoint;
        // Read after this poll's catchUp, so the next one gets to use it.
        this.noteLeaseHead(acquired.held);
      } catch (err) {
        this.opts.logger.warn('follower poll failed', { err: String(err) });
      }
    }
  }

  // -- submitting work ------------------------------------------------------

  /**
   * Queue a command for the next batch. Resolves only once the batch is durable.
   * Rejects with 503 on a node that is not the leader — the HTTP layer forwards
   * instead of calling this.
   */
  submit(command: Command): Promise<Json> {
    if (this.role !== 'leader') {
      return Promise.reject(new UpstreamUnavailableError('This node is not the leader'));
    }
    if (this.draining) {
      return Promise.reject(new UpstreamUnavailableError('Node is shutting down'));
    }
    if (!this.registry.has(command.machine)) {
      return Promise.reject(new HttpError(500, `No state machine named '${command.machine}'`));
    }
    return new Promise<Json>((resolve, reject) => {
      this.queue.push({ command, resolve, reject });
      this.wake?.();
    });
  }

  /**
   * Release one batch at a time, holding the door open only when holding it open
   * can actually catch anything.
   *
   * The batch window is skipped for the first commit after an idle stretch. A
   * window only merges requests if a second one arrives inside it, so below
   * roughly `1000 / batchWindowMs` commands per second it batches nothing and
   * simply adds its full length to every caller's latency. Above that rate it
   * does merge — but so does the commit itself, because anything arriving during
   * the in-flight PUT lands in the queue and rides the next batch. Waiting is
   * therefore only worth it when commits are already running back to back, which
   * is exactly when `queue` is non-empty on arrival here.
   *
   * Net effect: an isolated claim costs one S3 round trip instead of a round
   * trip plus the window, and a burst batches exactly as it did before.
   */
  private async commitLoop(): Promise<void> {
    let wasIdle = true;
    while (this.role === 'leader') {
      if (this.queue.length === 0) {
        if (this.draining) return;
        await new Promise<void>((resolve) => {
          // Poll floor so a draining node still notices and exits the loop.
          const timer = setTimeout(() => {
            if (this.wake) done();
          }, 250);
          timer.unref();
          const done = (): void => {
            clearTimeout(timer);
            this.wake = null;
            resolve();
          };
          this.wake = done;
        });
        wasIdle = true;
        continue;
      }

      if (!wasIdle) await new Promise((r) => setTimeout(r, this.opts.batchWindowMs));
      wasIdle = false;
      const batch = this.queue.splice(0, this.opts.maxBatch);
      try {
        await this.commitBatch(batch);
      } catch (err) {
        for (const p of batch) p.reject(err as Error);
        this.fatal(`commit failed irrecoverably: ${String(err)}`);
        return;
      }
    }
  }

  private async commitBatch(batch: Pending[]): Promise<void> {
    const entry: LogEntry = {
      seq: this.seq + 1,
      batchId: randomUUID(),
      writerId: this.nodeId,
      at: new Date().toISOString(),
      commits: [],
    };

    /** Resolve without touching S3: replays, rejections, no-op decisions. */
    const immediate: Array<[Pending, Json]> = [];
    const failed: Array<[Pending, HttpError]> = [];
    /** True only if the PUT lands. Index-aligned with `entry.commits`. */
    const provisional: Pending[] = [];
    /** Two commands in the SAME batch sharing an idempotency key. */
    const batchKeys = new Map<string, Json>();

    for (const pending of batch) {
      const { machine: machineName, key, payload } = pending.command;

      const prior = this.idempotency.get(key) ?? undefined;
      if (prior) {
        immediate.push([pending, prior.result]);
        continue;
      }
      const inBatch = batchKeys.get(key);
      if (inBatch !== undefined) {
        immediate.push([pending, inBatch]);
        continue;
      }

      const reg = this.registry.get(machineName)!;
      let decision: Decision<Json, Json>;
      try {
        decision = reg.machine.decide(reg.state, payload);
      } catch (err) {
        // A machine that throws is a bug, but it must not take the batch down.
        this.opts.logger.error('state machine decide() threw', {
          machine: machineName,
          err: String(err),
        });
        failed.push([pending, new HttpError(500, 'State machine failed to handle the command')]);
        continue;
      }

      if (decision.kind === 'reject') {
        const err = new HttpError(decision.status, decision.message);
        if (decision.code) err.name = decision.code;
        if (decision.body !== undefined) err.errors = decision.body;
        failed.push([pending, err]);
        continue;
      }
      if (decision.kind === 'immediate') {
        immediate.push([pending, decision.result]);
        continue;
      }

      // Apply into working state NOW so the next command in this same batch sees
      // it — otherwise two claims in one 50ms window take the same code. Safe
      // because a failed commit exits the process and rebuilds from the log.
      for (const event of decision.events) {
        reg.state = reg.machine.apply(reg.state, event);
      }
      const record: CommitRecord = {
        machine: machineName,
        key,
        events: decision.events,
        result: decision.result,
      };
      entry.commits.push(record);
      batchKeys.set(key, decision.result);
      provisional.push(pending);
    }

    for (const [pending, result] of immediate) pending.resolve(result);
    for (const [pending, err] of failed) pending.reject(err);
    if (entry.commits.length === 0) return; // nothing new to make durable

    const outcome = await this.store.append(entry);

    if (outcome === 'fenced') {
      for (const pending of provisional) {
        pending.reject(new UpstreamUnavailableError('Writer was fenced'));
      }
      this.fatal(
        `seq ${entry.seq} was written by another node — exiting so this one restarts as a follower`,
      );
      return;
    }

    // Durable. Only now is any of this true.
    this.seq = entry.seq;
    for (const commit of entry.commits) {
      this.rememberIdempotent(commit.key, { machine: commit.machine, result: commit.result });
    }
    for (const [i, pending] of provisional.entries()) {
      pending.resolve(entry.commits[i]!.result);
    }

    if (++this.commitsSinceSnapshot >= this.opts.snapshotEvery) {
      this.commitsSinceSnapshot = 0;
      void this.compact();
    }
  }

  /**
   * Fold the log into a snapshot so recovery stays a couple of GETs.
   *
   * Refuses to run when history names a machine this build does not register.
   * A snapshot is built from the registry, so compacting here would write a
   * snapshot missing that machine and then prune the log entries that were the
   * only remaining record of it — destroying its state permanently, during the
   * rollback that dropping unknown machines exists to make survivable. Skipping
   * compaction instead costs a longer log and slower recovery, both of which are
   * visible and both of which undo themselves once the machine is registered
   * again.
   */
  private async compact(): Promise<void> {
    if (this.unknownMachines.size > 0) {
      this.opts.logger.warn('skipping compaction — history names unregistered machines', {
        machines: [...this.unknownMachines],
        seq: this.seq,
      });
      return;
    }
    try {
      const upTo = this.seq;
      const machines: Record<string, Json> = {};
      for (const [name, reg] of this.registry) {
        machines[name] = reg.machine.snapshot(reg.state);
      }
      const snapshot: SnapshotFile = {
        seq: upTo,
        at: new Date().toISOString(),
        machines,
        idempotency: [...this.idempotency.entries()].map(([key, rec]) => ({
          key,
          machine: rec.machine,
          result: rec.result,
        })),
      };
      await this.store.writeSnapshot(snapshot);
      // Never prune to the head: a reader that listed these keys may not have
      // fetched them yet. See `pruneRetain`.
      const pruneTo = upTo - this.opts.pruneRetain;
      const pruned = pruneTo > 0 ? await this.store.pruneLog(pruneTo) : 0;
      this.opts.logger.info('snapshot written', {
        seq: upTo,
        prunedThrough: pruneTo > 0 ? pruneTo : null,
        prunedLogEntries: pruned,
      });
    } catch (err) {
      // Harmless: the log is still complete, recovery is just slower.
      this.opts.logger.warn('compaction failed', { err: String(err) });
    }
  }

  private fatal(reason: string): void {
    this.role = 'starting';
    if (this.leaseTimer) clearInterval(this.leaseTimer);
    this.opts.logger.error('fatal', { reason });
    if (this.opts.onFatal) {
      this.opts.onFatal(reason);
      return;
    }
    process.exit(1);
  }

  // -- shutdown -------------------------------------------------------------

  /** Drain in-flight commands, then hand the lease over so failover is instant. */
  async shutdown(drainTimeoutMs: number): Promise<void> {
    this.draining = true;
    if (this.leaseTimer) clearInterval(this.leaseTimer);
    this.wake?.();

    const deadline = Date.now() + drainTimeoutMs;
    while (this.queue.length > 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 25));
    }

    if (this.role === 'leader') {
      try {
        await this.store.releaseLease();
        this.opts.logger.info('lease released — standby can take over immediately');
      } catch (err) {
        this.opts.logger.warn('lease release failed', { err: String(err) });
      }
    }
  }
}
