import type { Request } from 'express';
import type { JWTPayload } from 'jose';
import { createAuthMiddleware } from 'token-weaver/auth';

import {
  ADMIN_ROUTE_PREFIX,
  loadAuthStrategies,
  type CompiledAuthStrategy,
} from '../config/escapement-config';
import { config } from '../config/index';
import type { AuthContext, AuthStrategyContext } from '../types/express';
import { HttpError, logger } from '../utils/index';

/**
 * Authentication middleware.
 *
 * Verification is delegated to the shared middleware published by token-weaver
 * (`token-weaver/auth`). Escapement supplies the strategy list — one per kind of
 * caller it accepts — and maps the verified payload onto its own request shape.
 *
 * Strategies are tried in order and the first that accepts the request wins. If
 * every one rejects, the most informative failure is surfaced (a `403` is
 * preferred over a `401`, since "authenticated but not allowed here" tells the
 * caller more than "bad token"). Nothing reaches S3 before this passes.
 *
 * Where the strategies come from is a deployment decision — see
 * `src/config/escapement-config.ts`. That module takes the environment as an
 * argument rather than importing it, so binding the two happens here.
 */
const strategies = loadAuthStrategies(config);

// Without an audience, verification checks only signature and issuer — a token
// the same issuer minted for a DIFFERENT service passes here too. Legal, but
// worth a loud note per strategy so the omission is a decision, not a default.
for (const strategy of strategies) {
  if (strategy.issuer !== undefined && !(strategy.options as { audience?: string }).audience) {
    logger.warn(
      `auth strategy "${strategy.label}" verifies no audience — ` +
        `tokens minted by this issuer for other services will be accepted`,
    );
  }
}

/** Strategies indexed by the issuer of the tokens they verify (JWT strategies only). */
const byIssuer = new Map<string, CompiledAuthStrategy>(
  strategies
    .filter((strategy): strategy is CompiledAuthStrategy & { issuer: string } =>
      Boolean(strategy.issuer),
    )
    .map((strategy) => [strategy.issuer, strategy]),
);

/**
 * The `static` strategy, if any — a static payload carries nothing to match on, so
 * the compiler allows at most one and this is unambiguous.
 */
const staticStrategy = strategies.find((strategy) => strategy.type === 'static');

/**
 * Trace a verified payload back to the strategy that accepted it.
 *
 * token-weaver does not report which strategy won, so we identify it by `iss` —
 * which the config compiler guarantees is unique per JWT strategy. A payload with
 * no `iss` came from a static token, which carries no claims at all.
 */
function strategyFor(payload: JWTPayload): CompiledAuthStrategy | undefined {
  const issuer = payload.iss;
  if (typeof issuer === 'string' && issuer.length > 0) {
    return byIssuer.get(issuer);
  }
  return staticStrategy;
}

/** The path the request resolves to, matching what token-weaver checks. */
function requestPath(req: Request): string {
  return `${req.baseUrl}${req.path}`;
}

/**
 * Read the identity out of a JWT payload.
 *
 * On the player routes this is mandatory — the S3 key is built from it, so a
 * token that cannot name a player is rejected. On the admin routes the target
 * comes from the URL instead, so an identity is recorded when the token happens
 * to carry one and its absence is not an error.
 */
function jwtIdentity(
  payload: JWTPayload,
  strategy: CompiledAuthStrategy,
  optional: boolean,
): AuthContext | undefined {
  const appClaimName = strategy.appClaim;
  const userId = payload.sub;
  const app = appClaimName ? payload[appClaimName] : undefined;

  const hasUserId = typeof userId === 'string' && userId.length > 0;
  const hasApp = typeof app === 'string' && app.length > 0;

  if (hasUserId && hasApp) {
    return { userId, app };
  }

  if (optional) {
    return undefined;
  }

  if (!hasUserId) {
    throw new HttpError(401, 'Token is missing the subject (sub) claim');
  }
  throw new HttpError(401, `Token is missing the '${appClaimName ?? 'app'}' claim`);
}

/** What a verified request is allowed to be, once the strategy that won is known. */
export interface ResolvedAuth {
  /** Caller identity — absent when nothing names one (an admin token without a `sub`). */
  auth?: AuthContext;
  /** Which strategy accepted, recorded for the admin re-check and for logs. */
  strategy: AuthStrategyContext;
}

/**
 * Decide what a verified payload means for this path: which routes it may use,
 * and who it acts as. Pure, so it can be exercised without a booted deployment.
 *
 * Two gates live here rather than in the strategy's `paths` block, because an
 * inline path list would override the token's own whitelist/blacklist claims and
 * both restrictions are properties of the service, not something a deployment
 * should be able to weaken by writing its own patterns:
 *
 * - the admin routes require `admin: true`;
 * - a static token reaches anything else only if the config declared it a service
 *   caller and thereby gave it an identity.
 */
export function resolveAuthContext(
  payload: JWTPayload,
  path: string,
  strategy: CompiledAuthStrategy,
): ResolvedAuth {
  const isAdminRoute = path.startsWith(ADMIN_ROUTE_PREFIX);

  if (isAdminRoute && !strategy.admin) {
    throw new HttpError(403, 'This credential is not allowed on the admin routes');
  }

  if (strategy.type === 'static') {
    // The compiler already refuses a static strategy that is neither admin nor a
    // service caller; this is the belt to that suspenders. A static token has no
    // `sub`, so without a configured identity it could only act on a player it
    // cannot name.
    const service = strategy.service;
    if (!isAdminRoute && !service) {
      throw new HttpError(
        403,
        'A static token cannot access player-scoped routes — it carries no player identity',
      );
    }

    return {
      // The configured identity, when there is one, is used on the admin routes
      // too: it is what scopes this caller's idempotency keys, and an admin
      // request is as entitled to its own namespace as any other.
      ...(service ? { auth: { userId: service.actor, app: service.app } } : {}),
      strategy: {
        type: 'static',
        admin: strategy.admin,
        ...(service ? { service: true } : {}),
      },
    };
  }

  const auth = jwtIdentity(payload, strategy, isAdminRoute);
  return {
    ...(auth ? { auth } : {}),
    strategy: {
      type: strategy.type,
      admin: strategy.admin,
      ...(strategy.issuer ? { issuer: strategy.issuer } : {}),
    },
  };
}

export const authMiddleware = createAuthMiddleware({
  strategies: strategies.map((strategy) => strategy.options),
  onVerified: (payload, req) => {
    const strategy = strategyFor(payload);
    if (!strategy) {
      // Only reachable if a token verifies under a strategy we cannot trace back,
      // which would mean the issuer map and the strategy list disagree.
      throw new HttpError(401, 'Verified token could not be matched to an auth strategy');
    }

    const resolved = resolveAuthContext(payload, requestPath(req), strategy);
    if (resolved.auth) req.auth = resolved.auth;
    req.authStrategy = resolved.strategy;
  },
});

logger.info(
  `auth: ${strategies.length} ${strategies.length === 1 ? 'strategy' : 'strategies'} (${strategies
    .map(
      (strategy) =>
        `${strategy.type}${strategy.admin ? ':admin' : ''}${strategy.service ? ':service' : ''}`,
    )
    .join(', ')})`,
);
