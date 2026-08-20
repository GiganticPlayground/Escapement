import type { Request, Response } from 'express';

import type { Json } from '../engine/index';
import { lookupClaim, poolStats, type PoolState } from '../machines/index';
import { dispatch, actorOf, idempotencyKey, requireAdmin } from '../services/dispatch';
import { engine } from '../services/index';
import { HttpError } from '../utils/index';

// No try/catch in these handlers: Express 5 forwards a rejected async handler
// to the error middleware itself.

/** Path params are validated by the spec before we get here. */
function poolName(req: Request): string {
  return String(req.params['pool']);
}

/** The shared claim payload; `claimNext` and `claimCode` differ only in target. */
function claimPayload(req: Request): { pool: string; by: string; at: string; metadata?: Json } {
  const body = (req.body ?? {}) as { by?: string; metadata?: Json };
  return {
    pool: poolName(req),
    by: actorOf(req, body.by),
    // The timestamp is generated here, not inside the machine, so `decide`
    // stays pure and unit-testable. It travels in the event, so replay
    // reproduces the original value rather than the replay-time clock.
    at: new Date().toISOString(),
    ...(body.metadata !== undefined ? { metadata: body.metadata } : {}),
  };
}

/**
 * Seed a pool with codes. Admin only.
 *
 * @route POST /v1/escapement/admin/pools/{pool}/seed
 */
export const seedPool = async (req: Request, res: Response): Promise<void> => {
  requireAdmin(req);
  const body = req.body as { codes: string[] };
  await dispatch(req, res, {
    machine: 'pool',
    key: idempotencyKey(req),
    payload: { type: 'seed', pool: poolName(req), codes: body.codes },
  });
};

/**
 * Claim the next available code.
 *
 * @route POST /v1/escapement/pools/{pool}/claims
 */
export const claimNext = async (req: Request, res: Response): Promise<void> => {
  await dispatch(req, res, {
    machine: 'pool',
    key: idempotencyKey(req),
    payload: { type: 'claimNext', ...claimPayload(req) },
  });
};

/**
 * Claim one specific code — redemption of a code the caller already holds.
 *
 * @route POST /v1/escapement/pools/{pool}/claims/{code}
 */
export const claimCode = async (req: Request, res: Response): Promise<void> => {
  await dispatch(req, res, {
    machine: 'pool',
    key: idempotencyKey(req),
    payload: { type: 'claimCode', code: String(req.params['code']), ...claimPayload(req) },
  });
};

/**
 * Return a claimed code to the pool. Admin only.
 *
 * @route POST /v1/escapement/admin/pools/{pool}/releases
 */
export const releaseCode = async (req: Request, res: Response): Promise<void> => {
  requireAdmin(req);
  const body = req.body as { code: string; reason?: string };
  await dispatch(req, res, {
    machine: 'pool',
    key: idempotencyKey(req),
    payload: {
      type: 'release',
      pool: poolName(req),
      code: body.code,
      ...(body.reason !== undefined ? { reason: body.reason } : {}),
    },
  });
};

/**
 * Pool statistics. Read straight from memory — no S3 call, and a follower
 * answers as happily as the leader.
 *
 * @route GET /v1/escapement/pools/{pool}
 */
export const getPool = async (req: Request, res: Response): Promise<void> => {
  const pool = poolName(req);
  const stats = engine.query<PoolState, ReturnType<typeof poolStats>>('pool', (state) =>
    poolStats(state, pool),
  );
  if (!stats) throw new HttpError(404, `No pool named '${pool}'`);
  res.status(200).json(stats);
};

/**
 * Look up who holds a code.
 *
 * @route GET /v1/escapement/pools/{pool}/claims/{code}
 */
export const getClaim = async (req: Request, res: Response): Promise<void> => {
  const pool = poolName(req);
  const code = String(req.params['code']);
  const claim = engine.query<PoolState, ReturnType<typeof lookupClaim>>('pool', (state) =>
    lookupClaim(state, pool, code),
  );
  if (!claim) throw new HttpError(404, `Code '${code}' is not claimed in pool '${pool}'`);
  res.status(200).json({ pool, ...claim });
};
