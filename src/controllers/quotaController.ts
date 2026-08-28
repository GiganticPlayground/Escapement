import type { Request, Response } from 'express';

import { quotaView, type QuotaState } from '../machines/index';
import { dispatch, actorOf, idempotencyKey, requireAdmin } from '../services/dispatch';
import { engine } from '../services/index';
import { HttpError } from '../utils/index';

// No try/catch in these handlers: Express 5 forwards a rejected async handler
// to the error middleware itself.

function quotaName(req: Request): string {
  return String(req.params['quota']);
}

/**
 * Define or redefine a quota. Admin only.
 *
 * @route POST /v1/escapement/admin/quotas/{quota}
 */
export const defineQuota = async (req: Request, res: Response): Promise<void> => {
  requireAdmin(req);
  const body = req.body as { limit: number; perSubject?: boolean };
  await dispatch(req, res, {
    machine: 'quota',
    key: idempotencyKey(req),
    payload: {
      type: 'define',
      quota: quotaName(req),
      limit: body.limit,
      perSubject: body.perSubject === true,
    },
  });
};

/**
 * Consume quota, atomically, or be told it would exceed the ceiling.
 *
 * @route POST /v1/escapement/quotas/{quota}/consume
 */
export const consumeQuota = async (req: Request, res: Response): Promise<void> => {
  const body = (req.body ?? {}) as { subject?: string; amount?: number };
  await dispatch(req, res, {
    machine: 'quota',
    key: idempotencyKey(req),
    payload: {
      type: 'consume',
      quota: quotaName(req),
      subject: actorOf(req, body.subject),
      amount: body.amount ?? 1,
    },
  });
};

/**
 * Quota definition and usage. Served from memory.
 *
 * A per-subject quota needs a subject to answer about: the ceiling applies to one
 * subject, so there is no whole-quota usage figure to report. `?subject=` names
 * one explicitly; otherwise it falls back to the caller's own identity, matching
 * how `consume` picks a subject. A credential that carries no identity (an admin
 * static token) has to pass one; a service credential names itself, so one asking
 * about a player passes `?subject=` rather than reading its own usage.
 *
 * @route GET /v1/escapement/quotas/{quota}
 */
export const getQuota = async (req: Request, res: Response): Promise<void> => {
  const quota = quotaName(req);
  const explicit = typeof req.query['subject'] === 'string' ? req.query['subject'] : undefined;
  const caller = req.auth?.userId;

  // One pass over the state, so the quota's shape and the answer about it
  // cannot disagree.
  const outcome = engine.query<
    QuotaState,
    | { kind: 'ok'; view: NonNullable<ReturnType<typeof quotaView>> }
    | { kind: 'unknown' }
    | { kind: 'needsSubject' }
  >('quota', (state) => {
    const q = state.get(quota);
    if (!q) return { kind: 'unknown' };
    const subject = explicit ?? (q.perSubject ? caller : undefined);
    if (q.perSubject && subject === undefined) return { kind: 'needsSubject' };
    return { kind: 'ok', view: quotaView(state, quota, subject)! };
  });

  if (outcome.kind === 'unknown') throw new HttpError(404, `No quota named '${quota}'`);
  if (outcome.kind === 'needsSubject') {
    throw new HttpError(
      400,
      `Quota '${quota}' is per-subject; pass ?subject= or use a credential that names one`,
    );
  }
  res.status(200).json(outcome.view);
};
