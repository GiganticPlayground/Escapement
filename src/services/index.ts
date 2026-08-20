/**
 * Wiring. One engine, one log store, the registered state machines.
 *
 * Adding a machine is two lines here plus its file in `src/machines/` and its
 * operations in `api/openapi.yaml` — no engine changes.
 */

import { hostname } from 'node:os';

import { config } from '../config/index';
import { Engine, LogStore } from '../engine/index';
import { poolMachine, quotaMachine } from '../machines/index';
import { logger } from '../utils/index';

const endpointHost = config.ESCAPEMENT_ENDPOINT_HOST ?? hostname();

/** Address peers use to reach this node. Recorded in the lease. */
export const selfEndpoint = `http://${endpointHost}:${config.PORT}`;

/**
 * ONE engine per process, even across module realms.
 *
 * express-openapi-validator loads controllers from disk at request time, and
 * that can produce a second instance of this module in a different module
 * registry (the ESM graph the app boots in, versus the loader eov uses). Two
 * `new Engine(...)` calls would leave the HTTP layer talking to an engine that
 * never elected itself — which presents as a node stuck in `starting` that
 * forwards to nobody, while the log says it became leader. Pinning both
 * singletons to global symbols makes the duplication harmless.
 */
const STORE_KEY = Symbol.for('giganticplayground.escapement.logstore');
const ENGINE_KEY = Symbol.for('giganticplayground.escapement.engine');

interface Singletons {
  [STORE_KEY]?: LogStore;
  [ENGINE_KEY]?: Engine;
}
const shared = globalThis as typeof globalThis & Singletons;

function buildLogStore(): LogStore {
  return new LogStore({
    bucket: config.ESCAPEMENT_S3_BUCKET,
    prefix: `${config.ESCAPEMENT_KEY_PREFIX}/${config.ESCAPEMENT_ENV}`,
    region: config.AWS_REGION,
    endpoint: config.ESCAPEMENT_S3_ENDPOINT,
  });
}

function buildEngine(store: LogStore): Engine {
  return new Engine({
    store,
    machines: [poolMachine, quotaMachine],
    endpoint: selfEndpoint,
    batchWindowMs: config.BATCH_WINDOW_MS,
    maxBatch: config.MAX_BATCH,
    leaseTtlMs: config.LEASE_TTL_MS,
    followPollMs: config.FOLLOW_POLL_MS,
    snapshotEvery: config.SNAPSHOT_EVERY,
    pruneRetain: config.PRUNE_RETAIN,
    idempotencyLimit: config.IDEMPOTENCY_LIMIT,
    logger: {
      info: (msg, meta) => logger.info(msg, meta),
      warn: (msg, meta) => logger.warn(msg, meta),
      error: (msg, meta) => logger.error(msg, meta),
    },
  });
}

shared[STORE_KEY] ??= buildLogStore();
export const logStore: LogStore = shared[STORE_KEY];

shared[ENGINE_KEY] ??= buildEngine(logStore);
export const engine: Engine = shared[ENGINE_KEY];
