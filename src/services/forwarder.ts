/**
 * Follower → leader request forwarding.
 *
 * Followers are healthy and receive traffic like any other replica; they simply
 * proxy mutations to whichever node holds the lease. Steering traffic this way
 * rather than by failing health checks is deliberate: an orchestrator restarts
 * unhealthy tasks, so a permanently-unhealthy standby would crash-loop.
 *
 * The extra hop costs about a millisecond on a container network, against a
 * commit that costs 50–100ms in S3.
 */

import type { Request, Response } from 'express';

import { engine } from './index';
import { config } from '../config/index';
import { logger } from '../utils/index';

/** Set on a forwarded request so the leader never forwards it onward. */
export const FORWARD_HEADER = 'x-escapement-forwarded-by';

export function isForwarded(req: Request): boolean {
  return typeof req.header(FORWARD_HEADER) === 'string';
}

export async function forwardToLeader(req: Request, res: Response): Promise<void> {
  const leader = engine.leaderEndpoint;

  if (!leader || leader === engine.endpoint) {
    res.status(503).json({
      message: 'No leader is currently available',
      code: 'NO_LEADER',
      retryAfterMs: config.LEASE_TTL_MS,
    });
    return;
  }

  const target = `${leader}${req.originalUrl}`;
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    [FORWARD_HEADER]: engine.nodeId,
  };
  for (const name of ['authorization', 'idempotency-key', 'x-request-id']) {
    const value = req.header(name);
    if (value) headers[name] = value;
  }

  try {
    const upstream = await fetch(target, {
      method: req.method,
      headers,
      ...(req.method === 'GET' || req.method === 'HEAD'
        ? {}
        : { body: JSON.stringify(req.body ?? {}) }),
      // Bounded by the failover horizon: if the leader is stuck, a replacement
      // exists within one lease TTL, so waiting longer than that only pins the
      // client — and it keeps the 503's `retryAfterMs` advice honest.
      signal: AbortSignal.timeout(config.LEASE_TTL_MS),
    });
    const text = await upstream.text();
    res.status(upstream.status);
    res.type('application/json');
    res.send(text.length > 0 ? text : '{}');
  } catch (err) {
    // The leader is gone; a follower is promoted within the lease TTL and the
    // client's idempotency key makes the retry safe.
    logger.warn('forward to leader failed', { target, err: String(err) });
    res.status(503).json({
      message: 'Leader is unreachable',
      code: 'LEADER_UNREACHABLE',
      retryAfterMs: config.LEASE_TTL_MS,
    });
  }
}
