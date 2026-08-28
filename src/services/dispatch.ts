/**
 * The one path every mutation takes.
 *
 * On the leader it queues the command and waits for the batch to be durable. On
 * a follower it proxies to the leader instead. Controllers never talk to the
 * engine directly, so the leader/follower distinction lives in exactly one place.
 */

import type { Request, Response } from 'express';

import { forwardToLeader, isForwarded } from './forwarder';
import { engine } from './index';
import { HttpError } from '../utils/index';

/** The idempotency key, guaranteed present by the OpenAPI validator. */
export function idempotencyKey(req: Request): string {
  const key = req.header('idempotency-key');
  if (!key) {
    // The spec marks the header required, so the validator rejects first; this
    // keeps the engine from ever seeing an empty key.
    throw new HttpError(400, 'Idempotency-Key header is required');
  }
  return key;
}

/**
 * Who a claim is for: an explicit value if given, else the caller's own identity —
 * a JWT's subject, or the actor a static service strategy is configured to act as.
 * A service credential claiming on a player's behalf should therefore pass `by`;
 * without it the claim is attributed to the service, which is what actually acted.
 */
export function actorOf(req: Request, explicit?: unknown): string {
  if (typeof explicit === 'string' && explicit.length > 0) return explicit;
  return req.auth?.userId ?? 'anonymous';
}

/** Admin operations re-check the privilege next to the code that acts on it. */
export function requireAdmin(req: Request): void {
  if (!req.authStrategy?.admin) {
    throw new HttpError(403, 'This credential is not allowed on the admin operations');
  }
}

/**
 * The idempotency key as the engine stores it: scoped to the authenticated
 * caller and the target machine, never the raw client header alone. A raw key
 * is a single global namespace — one caller reusing (or guessing) another's key
 * would be handed the other caller's stored result, and a key first used on a
 * pool claim would answer a quota consume. Scoping makes a key collide only
 * with the same caller retrying the same kind of operation, which is the one
 * collision idempotency exists to serve. A follower forwards the raw header and
 * the leader re-derives the same scope from the forwarded credential.
 *
 * A static service credential resolves to the one identity its config names, so
 * everything holding that token shares a namespace — narrower than the global one,
 * but not per-player. Such a caller makes its keys unique itself — a key naming
 * the player and the operation it is for, not just a request id — which is the
 * same discipline any client needs anyway: a key is only as good as the operation
 * it names.
 */
export function scopedKey(req: Request, machine: string, clientKey: string): string {
  const app = req.auth?.app ?? '-';
  const user = req.auth?.userId ?? 'anonymous';
  return `${app}:${user}:${machine}:${clientKey}`;
}

export async function dispatch(
  req: Request,
  res: Response,
  command: { machine: string; key: string; payload: unknown },
): Promise<void> {
  if (engine.role !== 'leader') {
    if (isForwarded(req)) {
      // We were forwarded to but are not the leader — the sender's view is
      // stale. Bounce rather than forward again, which could loop.
      throw new HttpError(503, 'Forwarded to a node that is not the leader');
    }
    await forwardToLeader(req, res);
    return;
  }

  const result = await engine.submit({
    ...command,
    key: scopedKey(req, command.machine, command.key),
  });
  res.status(200).json(result);
}
