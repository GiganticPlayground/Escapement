import { z } from 'zod';

/** Characters accepted in a key path segment, kept deliberately narrower than S3's. */
const KEY_SEGMENT_PATTERN = /^[A-Za-z0-9._-]+(?:\/[A-Za-z0-9._-]+)*$/;

/**
 * A variable that becomes part of the S3 object key (`ESCAPEMENT_KEY_PREFIX`,
 * `ESCAPEMENT_ENV`).
 *
 * These are concatenated into the key, so a stray slash or a `..` silently moves
 * every state object — to the bucket root, or into another producer's tree when
 * the bucket is shared. Surrounding slashes are normalized away so `saves`,
 * `/saves` and `saves/` name the same place; anything that could still corrupt
 * the key fails the boot instead of producing a surprising layout.
 *
 * Multi-level values are allowed (`team-a/escapement`), the bucket root is not:
 * Escapement shares buckets with other services, so "no prefix" is a mistake rather
 * than a configuration.
 */
function keyPathVar(label: string, defaultValue?: string) {
  const base =
    defaultValue === undefined ? z.string() : z.string().optional().default(defaultValue);

  return base
    .transform((value) => value.trim().replace(/^\/+|\/+$/g, ''))
    .pipe(
      z
        .string()
        .min(1, {
          error: `${label} cannot be empty — Escapement does not write to the bucket root`,
        })
        .refine((value) => KEY_SEGMENT_PATTERN.test(value), {
          error:
            `${label} must be one or more '/'-separated segments of letters, digits, ` +
            `'.', '_' or '-' (got an empty segment, whitespace, or an unsupported character)`,
        })
        .refine(
          (value) => !value.split('/').some((segment) => segment === '.' || segment === '..'),
          {
            error: `${label} cannot contain '.' or '..' path segments`,
          },
        ),
    );
}

/** An integer env var with a default, parsed from its string form. */
function intVar(defaultValue: number) {
  return z
    .string()
    .transform((val) => parseInt(val, 10))
    .pipe(z.number().int().positive())
    .optional()
    .default(defaultValue);
}

/**
 * Environment variables validation schema.
 *
 * - Optional variables will use their default values if not provided
 * - Required variables will cause the application to fail on startup if missing
 */
export const envSchema = z
  .object({
    PORT: z
      .string()
      .transform((val) => parseInt(val, 10))
      .pipe(z.number().min(1).max(65535))
      .optional()
      .default(3000),
    NODE_ENV: z.enum(['development', 'production', 'test']).optional().default('development'),
    LOG_LEVEL: z
      .enum(['silly', 'trace', 'debug', 'info', 'warn', 'error', 'fatal'])
      .optional()
      .default('debug'),
    LOG_TYPE: z.enum(['json', 'pretty', 'hidden']).optional().default('pretty'),
    CORS_ORIGINS: z
      .string()
      .optional()
      .transform((value) => {
        if (!value) {
          return undefined;
        }

        if (value === '*') {
          return '*';
        }

        const origins = value
          .split(',')
          .map((origin) => origin.trim())
          .filter((origin) => origin.length > 0);

        return origins.length > 0 ? origins : undefined;
      }),
    TRUST_PROXY: z
      .string()
      .optional()
      .default('false')
      .transform((value) => {
        if (!value || value === 'false') {
          return false;
        }

        if (value === 'true') {
          return true;
        }

        const parsed = parseInt(value, 10);
        return Number.isNaN(parsed) ? false : parsed;
      }),
    API_DOCS_ENABLED: z
      .enum(['true', 'false'])
      .optional()
      .default('true')
      .transform((value) => value === 'true'),
    RATE_LIMIT_ENABLED: z
      .enum(['true', 'false'])
      .optional()
      .default('false')
      .transform((value) => value === 'true'),
    RATE_LIMIT_WINDOW_MS: intVar(60_000),
    RATE_LIMIT_MAX: intVar(300),

    // --- Storage ---------------------------------------------------------
    AWS_REGION: z.string().min(1),
    ESCAPEMENT_S3_BUCKET: z.string().min(1),
    /** Both of these become S3 key segments, so they go through keyPathVar(). */
    ESCAPEMENT_KEY_PREFIX: keyPathVar('ESCAPEMENT_KEY_PREFIX', 'escapement'),
    ESCAPEMENT_ENV: keyPathVar('ESCAPEMENT_ENV'),
    /** Point at MinIO or a stub. Unset uses the default AWS endpoint. */
    ESCAPEMENT_S3_ENDPOINT: z.url().optional(),

    // --- Engine ----------------------------------------------------------
    /**
     * Host peers use to reach THIS node. Under Docker Swarm set it to the task
     * name template (`ENDPOINT_HOST={{.Task.Name}}`), which resolves on the
     * overlay network. Defaults to the container hostname.
     */
    ESCAPEMENT_ENDPOINT_HOST: z.string().min(1).optional(),
    /**
     * Group commit window. The leader holds the door open this long collecting
     * concurrent commands so they share one S3 PUT. Higher = fewer, larger
     * writes and more throughput; lower = less latency per claim.
     */
    BATCH_WINDOW_MS: intVar(50),
    MAX_BATCH: intVar(500),
    /**
     * Bounds worst-case failover after a hard kill. A graceful stop releases the
     * lease immediately and does not wait this out.
     */
    LEASE_TTL_MS: intVar(15_000),
    /** How often a follower tails the log and re-checks the lease. */
    FOLLOW_POLL_MS: intVar(1_000),
    /** Commits between snapshots. Lower = faster recovery, more S3 writes. */
    SNAPSHOT_EVERY: intVar(1_000),
    /**
     * Log entries left in place behind a snapshot instead of being pruned.
     * Pruning to the head lets a delete land between a reader's LIST and its
     * GET; the margin keeps recently listed keys fetchable. Must comfortably
     * exceed the commits a follower can miss in one FOLLOW_POLL_MS.
     */
    PRUNE_RETAIN: intVar(200),
    /**
     * Idempotency records kept in memory and in each snapshot. Oldest are
     * evicted first, so this is the window in which a client retry is safe.
     */
    IDEMPOTENCY_LIMIT: intVar(50_000),
    DRAIN_TIMEOUT_MS: intVar(10_000),
    ESCAPEMENT_MAX_BODY_BYTES: z.string().optional().default('16mb'),

    // --- JWT verification (tokens issued elsewhere, e.g. Token Weaver) ----
    JWT_AUTH_MODE: z.enum(['jwt-jwks', 'jwt-hs256']).optional().default('jwt-jwks'),
    JWKS_URI: z.url().optional(),
    JWT_SECRET: z.string().min(1).optional(),
    JWT_ISSUER: z.string().min(1),
    JWT_AUDIENCE: z.string().min(1).optional(),
    JWT_APP_CLAIM: z.string().min(1).optional().default('app'),
    JWT_WHITELIST_CLAIM: z.string().min(1).optional().default('whitelist'),
    JWT_BLACKLIST_CLAIM: z.string().min(1).optional().default('blacklist'),
    JWT_PATH_PREFIX: z.string().min(1).optional(),
    /** Deployment config file carrying the `auth:` section. */
    ESCAPEMENT_CONFIG_PATH: z.string().min(1).optional(),

    // --- Request analytics (reqcast) -------------------------------------
    REQCAST_CONFIG: z.string().optional(),
    SHUTDOWN_TIMEOUT_MS: intVar(30_000),
  })
  .superRefine((env, ctx) => {
    if (env.JWT_AUTH_MODE === 'jwt-jwks' && !env.JWKS_URI) {
      ctx.addIssue({
        code: 'custom',
        path: ['JWKS_URI'],
        message: 'JWKS_URI is required when JWT_AUTH_MODE=jwt-jwks',
      });
    }
    if (env.JWT_AUTH_MODE === 'jwt-hs256' && !env.JWT_SECRET) {
      ctx.addIssue({
        code: 'custom',
        path: ['JWT_SECRET'],
        message: 'JWT_SECRET is required when JWT_AUTH_MODE=jwt-hs256',
      });
    }
    if (env.LEASE_TTL_MS <= env.FOLLOW_POLL_MS * 2) {
      // A follower needs at least a couple of polls inside the TTL window to
      // notice a healthy leader; otherwise it races a live leader every cycle.
      ctx.addIssue({
        code: 'custom',
        path: ['LEASE_TTL_MS'],
        message: 'LEASE_TTL_MS must be more than twice FOLLOW_POLL_MS',
      });
    }
  });

/** Inferred TypeScript type from the environment schema */
export type Env = z.infer<typeof envSchema>;
