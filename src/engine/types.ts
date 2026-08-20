/**
 * The state machine contract.
 *
 * Escapement is a tiny replicated state machine over object storage: the S3
 * object log is the RSM log, an in-memory object is the machine, and S3's
 * conditional write is a degenerate single-writer consensus. Everything the
 * service does — claiming codes, enforcing quotas, whatever comes next — is a
 * `StateMachine` plugged into that one engine.
 *
 * Two rules make it work:
 *
 * 1. `decide` never mutates. It inspects state and returns either events to
 *    persist, an immediate result, or a rejection. It runs BEFORE durability,
 *    so nothing it returns is true yet.
 * 2. `apply` is deterministic and total. It is the only thing that changes
 *    state, it runs identically during live commits and during replay, and it
 *    must never throw — an event in the log already happened.
 *
 * `apply` MAY mutate its state argument in place, which matters when the state
 * is a 100k-entry pool. That is safe because the engine has no rollback path:
 * events are applied only after the commit lands, and any commit whose outcome
 * cannot be determined exits the process, which rebuilds state from the log.
 */

/** JSON-compatible value. Snapshots and events must round-trip through this. */
export type Json = null | boolean | number | string | Json[] | { [k: string]: Json };

/** What `decide` returns. */
export type Decision<E, R> =
  /** Persist these events, then answer the caller with `result`. */
  | { kind: 'commit'; events: E[]; result: R }
  /** No state change; answer immediately without touching S3. */
  | { kind: 'immediate'; result: R }
  /** No state change; fail the caller with this error. */
  | { kind: 'reject'; status: number; message: string; code?: string; body?: Json };

export interface StateMachine<S = unknown, C = unknown, E = unknown, R = unknown> {
  /** Stable identifier. Appears in the log, so renaming it invalidates history. */
  readonly name: string;

  /** Empty state for a bucket with no history. */
  init(): S;

  /** Pure. Must not mutate `state`. */
  decide(state: S, command: C): Decision<E, R>;

  /** Deterministic and total. May mutate and return the same reference. */
  apply(state: S, event: E): S;

  /** Serialize for the periodic snapshot. */
  snapshot(state: S): Json;

  /** Rebuild from a snapshot written by `snapshot`. */
  restore(raw: Json): S;
}

/** One caller's request, as it reaches the engine. */
export interface Command<C = unknown> {
  /** Which registered machine handles it. */
  machine: string;
  /** Caller-supplied idempotency key. Engine-level, not the machine's problem. */
  key: string;
  payload: C;
}

/** One machine's committed effect within a batch. */
export interface CommitRecord {
  machine: string;
  key: string;
  events: Json[];
  /** Replayed verbatim when the same idempotency key returns. */
  result: Json;
}

/**
 * One batch, written to `log/<seq>.json` with `If-None-Match: *`.
 *
 * `batchId` is what makes an ambiguous write decidable: on a timeout we re-read
 * the key and compare, which answers both "did my write land?" and "has someone
 * fenced me?" with a single GET.
 */
export interface LogEntry {
  seq: number;
  batchId: string;
  writerId: string;
  at: string;
  commits: CommitRecord[];
}

export interface SnapshotFile {
  seq: number;
  at: string;
  /** machine name -> that machine's `snapshot()` output */
  machines: Record<string, Json>;
  /** Recent idempotency records, newest last. Bounded; see IDEMPOTENCY_LIMIT. */
  idempotency: Array<{ key: string; machine: string; result: Json }>;
}

export interface Lease {
  writerId: string;
  endpoint: string;
  expiresAt: number;
  /**
   * The leader's committed sequence when it last wrote this lease.
   *
   * A hint, never an authority. Followers read the lease every poll anyway, so
   * this rides along for free and gives them a lower bound on the log head —
   * enough to tell "there is nothing after my seq" from "what came after my seq
   * has already been pruned", which a 404 alone cannot distinguish. It is only
   * ever stale in the safe direction: the leader advances `seq` after a commit
   * is durable and only renews every `leaseTtlMs / 3`, so this can lag the true
   * head but can never run ahead of it.
   */
  seq: number;
}
