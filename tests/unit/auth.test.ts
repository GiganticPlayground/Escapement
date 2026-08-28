import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, it } from 'node:test';

import type { Request } from 'express';
import type { JWTPayload } from 'jose';

import {
  compileAuthConfigFile,
  type CompiledAuthStrategy,
  type ServiceIdentity,
} from '../../src/config/escapement-config';

// The two modules exercised below reach the validated environment at import
// (`src/config/index` fails fast on a bad one), so they are imported after this
// rather than at the top of the file. Nothing here talks to S3: the engine's
// constructor only registers machines, and the log store only builds a client.
process.env['AWS_REGION'] = 'us-east-1';
process.env['ESCAPEMENT_S3_BUCKET'] = 'test-bucket';
process.env['ESCAPEMENT_ENV'] = 'test';
process.env['JWT_ISSUER'] = 'https://players.test';
process.env['JWKS_URI'] = 'https://players.test/.well-known/jwks.json';
process.env['LOG_TYPE'] = 'hidden';

const { resolveAuthContext } = await import('../../src/middlewares/auth.middleware');
const { scopedKey } = await import('../../src/services/dispatch');

const POOL_ROUTE = '/v1/escapement/pools/launch-codes/claims';
const ADMIN_ROUTE = '/v1/escapement/admin/pools/launch-codes/seed';

/** Write a config file into a throwaway directory and compile it. */
function compile(yaml: string): CompiledAuthStrategy[] {
  const file = join(mkdtempSync(join(tmpdir(), 'escapement-auth-')), 'escapement.yaml');
  writeFileSync(file, yaml, 'utf8');
  return compileAuthConfigFile(file, 'app');
}

/** A compiled static strategy, built directly so the gate can be tested in isolation. */
function staticStrategy(opts: {
  admin?: boolean;
  service?: ServiceIdentity;
}): CompiledAuthStrategy {
  return {
    type: 'static',
    label: 'auth (static)',
    admin: opts.admin ?? false,
    ...(opts.service ? { service: opts.service } : {}),
    options: { mode: 'static', staticToken: 'shared-secret' },
  };
}

function jwtStrategy(admin = false): CompiledAuthStrategy {
  return {
    type: 'hs256',
    label: 'auth (hs256)',
    admin,
    issuer: 'https://players.test',
    appClaim: 'app',
    options: { mode: 'jwt-hs256', secret: 's', issuer: 'https://players.test' },
  };
}

/** A static token verifies to a payload with nothing in it at all. */
const STATIC_PAYLOAD: JWTPayload = {};

const PLAYER_PAYLOAD: JWTPayload = { iss: 'https://players.test', sub: 'player-42', app: 'game' };

/** Just enough request for `scopedKey`, which reads only `req.auth`. */
function requestWith(auth?: { app: string; userId: string }): Request {
  return { auth } as unknown as Request;
}

function statusOf(fn: () => unknown): number {
  try {
    fn();
  } catch (error) {
    return (error as { status?: number }).status ?? 0;
  }
  throw new assert.AssertionError({ message: 'expected the call to throw' });
}

describe('static strategy configuration', () => {
  it('refuses a static strategy that is neither an admin nor a service caller', () => {
    assert.throws(
      () =>
        compile(`
auth:
  - type: static
    token: shared-secret
`),
      /carries no claims/,
    );
  });

  it('compiles a static strategy that declares a service identity, without granting admin', () => {
    const [strategy] = compile(`
auth:
  - type: static
    token: shared-secret
    service:
      app: platform
      actor: platform-service
`);

    assert.equal(strategy?.type, 'static');
    assert.deepEqual(strategy?.service, { app: 'platform', actor: 'platform-service' });
    // `service` says "may use the normal routes", never "may use the admin ones".
    assert.equal(strategy?.admin, false);
  });

  it('keeps admin and service independent, so one strategy can be both', () => {
    const [strategy] = compile(`
auth:
  - type: static
    token: shared-secret
    admin: true
    service:
      app: platform
      actor: platform-service
`);

    assert.equal(strategy?.admin, true);
    assert.equal(strategy?.service?.actor, 'platform-service');
  });

  it('resolves placeholders in the service identity and validates what they resolve to', () => {
    process.env['TEST_SERVICE_ACTOR'] = 'platform-service';
    const [strategy] = compile(`
auth:
  - type: static
    token: shared-secret
    service:
      app: platform
      actor: \${env:TEST_SERVICE_ACTOR}
`);
    assert.equal(strategy?.service?.actor, 'platform-service');

    // A ':' would let this caller spell another's idempotency scope, so the
    // resolved value is checked, not the placeholder that produced it.
    process.env['TEST_SERVICE_ACTOR'] = 'other-app:victim';
    assert.throws(
      () =>
        compile(`
auth:
  - type: static
    token: shared-secret
    service:
      app: platform
      actor: \${env:TEST_SERVICE_ACTOR}
`),
      /auth.service.actor must be letters/,
    );
    delete process.env['TEST_SERVICE_ACTOR'];
  });

  it('refuses a second static strategy, which would take the first one’s privileges', () => {
    assert.throws(
      () =>
        compile(`
auth:
  - type: static
    token: admin-secret
    admin: true
  - type: static
    token: service-secret
    service:
      app: platform
      actor: platform-service
`),
      /More than one "static" strategy/,
    );
  });

  it('compiles JWT strategies as before', () => {
    const [strategy] = compile(`
auth:
  - type: hs256
    issuer: https://players.test
    secret: shhh
    audience: escapement
`);

    assert.equal(strategy?.type, 'hs256');
    assert.equal(strategy?.issuer, 'https://players.test');
    assert.equal(strategy?.admin, false);
    assert.equal(strategy?.service, undefined);
  });
});

describe('route gating', () => {
  const service: ServiceIdentity = { app: 'platform', actor: 'platform-service' };

  it('lets a service static token onto a pool route, acting as its configured identity', () => {
    const resolved = resolveAuthContext(STATIC_PAYLOAD, POOL_ROUTE, staticStrategy({ service }));

    assert.deepEqual(resolved.auth, { app: 'platform', userId: 'platform-service' });
    assert.deepEqual(resolved.strategy, { type: 'static', admin: false, service: true });
  });

  it('still refuses a plain static token on a pool route', () => {
    assert.equal(
      statusOf(() =>
        resolveAuthContext(STATIC_PAYLOAD, POOL_ROUTE, staticStrategy({ admin: true })),
      ),
      403,
    );
  });

  it('still refuses a service static token on an admin route unless it is also admin', () => {
    assert.equal(
      statusOf(() => resolveAuthContext(STATIC_PAYLOAD, ADMIN_ROUTE, staticStrategy({ service }))),
      403,
    );

    const both = resolveAuthContext(
      STATIC_PAYLOAD,
      ADMIN_ROUTE,
      staticStrategy({ admin: true, service }),
    );
    assert.equal(both.strategy.admin, true);
    // The identity is recorded on the admin routes too — it is what scopes the keys.
    assert.deepEqual(both.auth, { app: 'platform', userId: 'platform-service' });
  });

  it('records no identity for an admin-only static token', () => {
    const resolved = resolveAuthContext(
      STATIC_PAYLOAD,
      ADMIN_ROUTE,
      staticStrategy({ admin: true }),
    );

    assert.equal(resolved.auth, undefined);
    assert.deepEqual(resolved.strategy, { type: 'static', admin: true });
  });

  it('maps a JWT onto its own identity, as before', () => {
    const resolved = resolveAuthContext(PLAYER_PAYLOAD, POOL_ROUTE, jwtStrategy());

    assert.deepEqual(resolved.auth, { app: 'game', userId: 'player-42' });
    assert.deepEqual(resolved.strategy, {
      type: 'hs256',
      admin: false,
      issuer: 'https://players.test',
    });
  });

  it('still refuses a non-admin JWT on the admin routes, and a JWT with no subject', () => {
    assert.equal(
      statusOf(() => resolveAuthContext(PLAYER_PAYLOAD, ADMIN_ROUTE, jwtStrategy())),
      403,
    );
    assert.equal(
      statusOf(() =>
        resolveAuthContext({ iss: 'https://players.test', app: 'game' }, POOL_ROUTE, jwtStrategy()),
      ),
      401,
    );
  });
});

describe('idempotency key scoping', () => {
  it('gives a service credential its own namespace rather than the shared anonymous one', () => {
    const resolved = resolveAuthContext(
      STATIC_PAYLOAD,
      POOL_ROUTE,
      staticStrategy({ service: { app: 'platform', actor: 'platform-service' } }),
    );

    assert.equal(
      scopedKey(requestWith(resolved.auth), 'pool', 'player-42:item-9'),
      'platform:platform-service:pool:player-42:item-9',
    );
  });

  it('leaves a credential that names nobody in the shared namespace', () => {
    assert.equal(scopedKey(requestWith(), 'pool', 'k'), '-:anonymous:pool:k');
  });

  it('leaves a JWT caller scoped to its own subject', () => {
    const resolved = resolveAuthContext(PLAYER_PAYLOAD, POOL_ROUTE, jwtStrategy());
    assert.equal(scopedKey(requestWith(resolved.auth), 'pool', 'k'), 'game:player-42:pool:k');
  });
});
