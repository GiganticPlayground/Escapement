/**
 * Quota — counters with an enforced ceiling.
 *
 * A named ceiling that must never be exceeded — either in total, or per subject.
 * It is the second machine on purpose: a different state shape, a different
 * rejection code, and an increment rather than a take, which is what proves the
 * engine interface generalizes past allocation.
 */

import type { Decision, Json, StateMachine } from '../engine/types';

export type QuotaCommand =
  | { type: 'define'; quota: string; limit: number; perSubject: boolean }
  | { type: 'consume'; quota: string; subject: string; amount: number };

export type QuotaEvent =
  | { t: 'defined'; quota: string; limit: number; perSubject: boolean }
  | { t: 'consumed'; quota: string; subject: string; amount: number };

export interface SingleQuota {
  limit: number;
  perSubject: boolean;
  /** Total across all subjects. Authoritative when `perSubject` is false. */
  used: number;
  /** Per-subject usage. Authoritative when `perSubject` is true. */
  subjects: Map<string, number>;
}

export type QuotaState = Map<string, SingleQuota>;

export interface QuotaView {
  quota: string;
  limit: number;
  used: number;
  remaining: number;
  perSubject: boolean;
  subject?: string;
  subjects?: number;
}

/**
 * A quota's definition and usage.
 *
 * Returns `null` both for an unknown quota and for a per-subject quota asked
 * about without a subject — the caller has to resolve one first. There is no
 * sensible answer to that question: `limit` is the ceiling for one subject, so
 * pairing it with a total across every subject would report, say, a limit of 3
 * with 5 used. The controller resolves the subject (falling back to the caller's
 * own) and rejects with 400 when it cannot, so this branch is unreachable over
 * HTTP; it stays defensive for any other caller.
 */
export function quotaView(state: QuotaState, quota: string, subject?: string): QuotaView | null {
  const q = state.get(quota);
  if (!q) return null;
  if (q.perSubject) {
    if (subject === undefined) return null;
    const used = q.subjects.get(subject) ?? 0;
    return {
      quota,
      limit: q.limit,
      used,
      remaining: Math.max(0, q.limit - used),
      perSubject: true,
      subject,
      subjects: q.subjects.size,
    };
  }
  return {
    quota,
    limit: q.limit,
    used: q.used,
    remaining: Math.max(0, q.limit - q.used),
    perSubject: false,
    subjects: q.subjects.size,
  };
}

export const quotaMachine: StateMachine<QuotaState, QuotaCommand, QuotaEvent, Json> = {
  name: 'quota',

  init(): QuotaState {
    return new Map();
  },

  decide(state, command): Decision<QuotaEvent, Json> {
    switch (command.type) {
      case 'define': {
        const existing = state.get(command.quota);
        // Defining answers with the definition, not with a usage position.
        // Usage is per subject when the ceiling is, so there is no single
        // used/remaining pair that means anything here — ask GET for that, with
        // a subject. Redefining preserves the counters as recorded: a lowered
        // ceiling can land already exhausted rather than silently forgiving
        // spend. Flipping `perSubject`, though, changes which counter is
        // authoritative — a global quota never tracked subjects, so redefining
        // it per-subject starts every subject at 0 (and the reverse enforces
        // against the accumulated global total). Flip with that in mind.
        const definition = {
          quota: command.quota,
          limit: command.limit,
          perSubject: command.perSubject,
          subjects: existing?.subjects.size ?? 0,
        };
        if (existing?.limit === command.limit && existing?.perSubject === command.perSubject) {
          return { kind: 'immediate', result: definition };
        }
        return {
          kind: 'commit',
          events: [
            {
              t: 'defined',
              quota: command.quota,
              limit: command.limit,
              perSubject: command.perSubject,
            },
          ],
          result: definition,
        };
      }

      case 'consume': {
        const q = state.get(command.quota);
        if (!q) {
          return { kind: 'reject', status: 404, message: `No quota named '${command.quota}'` };
        }
        // The spec pins `amount` to a positive integer, so this is unreachable
        // over HTTP. It lives here anyway because the ceiling is this machine's
        // invariant to hold: a negative amount would commit a decrement and hand
        // a caller its own spend back.
        if (!Number.isInteger(command.amount) || command.amount < 1) {
          return {
            kind: 'reject',
            status: 400,
            message: 'Amount must be a positive integer',
          };
        }
        const used = q.perSubject ? (q.subjects.get(command.subject) ?? 0) : q.used;
        const next = used + command.amount;
        if (next > q.limit) {
          return {
            kind: 'reject',
            status: 429,
            message: `Quota '${command.quota}' would be exceeded`,
            code: 'QUOTA_EXCEEDED',
            body: { used, limit: q.limit },
          };
        }
        return {
          kind: 'commit',
          events: [
            {
              t: 'consumed',
              quota: command.quota,
              subject: command.subject,
              amount: command.amount,
            },
          ],
          result: {
            quota: command.quota,
            subject: command.subject,
            used: next,
            limit: q.limit,
            remaining: q.limit - next,
          },
        };
      }
    }
  },

  apply(state, event): QuotaState {
    switch (event.t) {
      case 'defined': {
        const existing = state.get(event.quota);
        state.set(event.quota, {
          limit: event.limit,
          perSubject: event.perSubject,
          used: existing?.used ?? 0,
          subjects: existing?.subjects ?? new Map<string, number>(),
        });
        return state;
      }
      case 'consumed': {
        const q = state.get(event.quota);
        if (!q) return state; // defined-then-deleted is impossible today; ignore defensively
        q.used += event.amount;
        // Per-subject usage is only tracked when the ceiling is per-subject. A
        // global quota reads nothing from this map, and recording every distinct
        // caller in it would grow the state — and every snapshot — without bound
        // for a number nobody enforces.
        if (q.perSubject) {
          q.subjects.set(event.subject, (q.subjects.get(event.subject) ?? 0) + event.amount);
        }
        return state;
      }
    }
  },

  snapshot(state): Json {
    const out: Record<string, Json> = {};
    for (const [name, q] of state) {
      out[name] = {
        limit: q.limit,
        perSubject: q.perSubject,
        used: q.used,
        subjects: Object.fromEntries(q.subjects) as Json,
      };
    }
    return out;
  },

  restore(raw): QuotaState {
    const state: QuotaState = new Map();
    if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) return state;
    for (const [name, value] of Object.entries(raw)) {
      if (value === null || typeof value !== 'object' || Array.isArray(value)) continue;
      const v = value as { limit?: Json; perSubject?: Json; used?: Json; subjects?: Json };
      const subjects = new Map<string, number>();
      if (typeof v.subjects === 'object' && v.subjects !== null && !Array.isArray(v.subjects)) {
        for (const [name, used] of Object.entries(v.subjects)) {
          if (typeof used === 'number') subjects.set(name, used);
        }
      }
      state.set(name, {
        limit: typeof v.limit === 'number' ? v.limit : 0,
        perSubject: v.perSubject === true,
        used: typeof v.used === 'number' ? v.used : 0,
        subjects,
      });
    }
    return state;
  },
};
