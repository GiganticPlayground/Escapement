/**
 * Pool — claim-once allocation from a finite set of codes.
 *
 * Bounded stock, one item handed out per claim, and the possibility of running
 * out. Claiming the *next* code is what forces a single writer: it is a decision
 * about the whole pool rather than about one key, so two processes answering it
 * concurrently hand the same code to two callers.
 *
 * Codes are issued in reverse seed order. The free list is an array with an
 * index, and `takeNext()` pops the end, which is what makes both "next" and "this
 * specific one" O(1). Nothing depends on issue order; do not read the sequence as
 * meaningful.
 */

import type { Decision, Json, StateMachine } from '../engine/types';

// -- commands / events --------------------------------------------------------

export type PoolCommand =
  | { type: 'seed'; pool: string; codes: string[] }
  | { type: 'claimNext'; pool: string; by: string; at: string; metadata?: Json }
  | { type: 'claimCode'; pool: string; code: string; by: string; at: string; metadata?: Json }
  | { type: 'release'; pool: string; code: string; reason?: string };

export type PoolEvent =
  | { t: 'seeded'; pool: string; codes: string[] }
  | { t: 'claimed'; pool: string; code: string; by: string; at: string; metadata?: Json }
  | { t: 'released'; pool: string; code: string };

export interface ClaimRecord {
  code: string;
  by: string;
  claimedAt: string;
  metadata?: Json;
}

// -- free list ----------------------------------------------------------------

/**
 * O(1) take-next AND O(1) take-specific, via swap-remove. An array with a cursor
 * cannot do take-specific without an O(n) splice, and redemption of a known code
 * is a first-class operation here.
 */
export class CodePool {
  private codes: string[] = [];
  private index = new Map<string, number>();

  add(code: string): boolean {
    if (this.index.has(code)) return false;
    this.index.set(code, this.codes.length);
    this.codes.push(code);
    return true;
  }

  takeNext(): string | undefined {
    const code = this.codes.pop();
    if (code === undefined) return undefined;
    this.index.delete(code);
    return code;
  }

  take(code: string): boolean {
    const i = this.index.get(code);
    if (i === undefined) return false;
    const last = this.codes.pop()!;
    this.index.delete(code);
    if (i < this.codes.length) {
      this.codes[i] = last;
      this.index.set(last, i);
    }
    return true;
  }

  /** The code `takeNext()` would return, without removing it — `decide` must not mutate. */
  peekNext(): string | undefined {
    return this.codes[this.codes.length - 1];
  }

  has(code: string): boolean {
    return this.index.has(code);
  }
  get size(): number {
    return this.codes.length;
  }
  toArray(): string[] {
    return [...this.codes];
  }
}

// -- state --------------------------------------------------------------------

export interface SinglePool {
  free: CodePool;
  claims: Map<string, ClaimRecord>;
  /**
   * Codes ever seeded into this pool. Invariant: `total === free.size +
   * claims.size`. Releasing a code moves it back to `free` and leaves this alone.
   */
  total: number;
}

export type PoolState = Map<string, SinglePool>;

export interface PoolStats {
  pool: string;
  remaining: number;
  claimed: number;
  total: number;
}

function emptyPool(): SinglePool {
  return { free: new CodePool(), claims: new Map(), total: 0 };
}

// -- queries (read paths, served from memory on any node) ---------------------

export function poolStats(state: PoolState, pool: string): PoolStats | null {
  const p = state.get(pool);
  if (!p) return null;
  return { pool, remaining: p.free.size, claimed: p.claims.size, total: p.total };
}

export function lookupClaim(state: PoolState, pool: string, code: string): ClaimRecord | null {
  return state.get(pool)?.claims.get(code) ?? null;
}

export function poolExists(state: PoolState, pool: string): boolean {
  return state.has(pool);
}

// -- the machine --------------------------------------------------------------

export const poolMachine: StateMachine<PoolState, PoolCommand, PoolEvent, Json> = {
  name: 'pool',

  init(): PoolState {
    return new Map();
  },

  decide(state, command): Decision<PoolEvent, Json> {
    switch (command.type) {
      case 'seed': {
        const existing = state.get(command.pool);
        // Filter here rather than in apply: the count of genuinely new codes is
        // part of the answer, and apply must not need to compute anything.
        const fresh = existing
          ? command.codes.filter((c) => !existing.free.has(c) && !existing.claims.has(c))
          : [...new Set(command.codes)];
        const deduped = [...new Set(fresh)];

        if (deduped.length === 0) {
          return {
            kind: 'immediate',
            result: {
              pool: command.pool,
              added: 0,
              remaining: existing?.free.size ?? 0,
            },
          };
        }
        return {
          kind: 'commit',
          events: [{ t: 'seeded', pool: command.pool, codes: deduped }],
          result: {
            pool: command.pool,
            added: deduped.length,
            remaining: (existing?.free.size ?? 0) + deduped.length,
          },
        };
      }

      case 'claimNext': {
        const p = state.get(command.pool);
        if (!p) {
          return { kind: 'reject', status: 404, message: `No pool named '${command.pool}'` };
        }
        if (p.free.size === 0) {
          return {
            kind: 'reject',
            status: 409,
            message: `Pool '${command.pool}' has no unclaimed codes left`,
            code: 'POOL_EXHAUSTED',
          };
        }
        // Peek without mutating — decide must stay pure. `apply` pops it.
        const code = p.free.peekNext()!;
        return claimDecision(command.pool, code, command.by, command.at, command.metadata);
      }

      case 'claimCode': {
        const p = state.get(command.pool);
        if (!p) {
          return { kind: 'reject', status: 404, message: `No pool named '${command.pool}'` };
        }
        if (p.claims.has(command.code)) {
          return {
            kind: 'reject',
            status: 409,
            message: `Code '${command.code}' is already claimed`,
            code: 'ALREADY_CLAIMED',
          };
        }
        if (!p.free.has(command.code)) {
          return {
            kind: 'reject',
            status: 404,
            message: `Code '${command.code}' is not in pool '${command.pool}'`,
          };
        }
        return claimDecision(command.pool, command.code, command.by, command.at, command.metadata);
      }

      case 'release': {
        const p = state.get(command.pool);
        if (!p) {
          return { kind: 'reject', status: 404, message: `No pool named '${command.pool}'` };
        }
        if (!p.claims.has(command.code)) {
          return {
            kind: 'reject',
            status: 404,
            message: `Code '${command.code}' is not currently claimed`,
          };
        }
        return {
          kind: 'commit',
          events: [{ t: 'released', pool: command.pool, code: command.code }],
          result: { pool: command.pool, code: command.code, remaining: p.free.size + 1 },
        };
      }
    }
  },

  apply(state, event): PoolState {
    switch (event.t) {
      case 'seeded': {
        const p = state.get(event.pool) ?? emptyPool();
        for (const code of event.codes) {
          if (p.free.add(code)) p.total++;
        }
        state.set(event.pool, p);
        return state;
      }
      case 'claimed': {
        const p = state.get(event.pool) ?? emptyPool();
        p.free.take(event.code);
        p.claims.set(event.code, {
          code: event.code,
          by: event.by,
          claimedAt: event.at,
          ...(event.metadata !== undefined ? { metadata: event.metadata } : {}),
        });
        state.set(event.pool, p);
        return state;
      }
      case 'released': {
        const p = state.get(event.pool);
        if (!p) return state;
        p.claims.delete(event.code);
        p.free.add(event.code);
        return state;
      }
    }
  },

  snapshot(state): Json {
    const out: Record<string, Json> = {};
    for (const [name, p] of state) {
      out[name] = {
        free: p.free.toArray(),
        total: p.total,
        claims: [...p.claims.values()].map((c) => ({
          code: c.code,
          by: c.by,
          claimedAt: c.claimedAt,
          ...(c.metadata !== undefined ? { metadata: c.metadata } : {}),
        })) as Json,
      };
    }
    return out;
  },

  restore(raw): PoolState {
    const state: PoolState = new Map();
    if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) return state;
    for (const [name, value] of Object.entries(raw)) {
      if (value === null || typeof value !== 'object' || Array.isArray(value)) continue;
      const p = emptyPool();
      const free = (value as { free?: Json }).free;
      if (Array.isArray(free)) {
        for (const code of free) if (typeof code === 'string') p.free.add(code);
      }
      const claims = (value as { claims?: Json }).claims;
      if (Array.isArray(claims)) {
        for (const c of claims) {
          if (c === null || typeof c !== 'object' || Array.isArray(c)) continue;
          const rec = c as unknown as ClaimRecord;
          p.claims.set(rec.code, rec);
        }
      }
      const total = (value as { total?: Json }).total;
      p.total = typeof total === 'number' ? total : p.free.size + p.claims.size;
      state.set(name, p);
    }
    return state;
  },
};

function claimDecision(
  pool: string,
  code: string,
  by: string,
  at: string,
  metadata: Json | undefined,
): Decision<PoolEvent, Json> {
  const event: PoolEvent = {
    t: 'claimed',
    pool,
    code,
    by,
    at,
    ...(metadata !== undefined ? { metadata } : {}),
  };
  return {
    kind: 'commit',
    events: [event],
    result: {
      pool,
      code,
      by,
      claimedAt: at,
      ...(metadata !== undefined ? { metadata } : {}),
    },
  };
}
