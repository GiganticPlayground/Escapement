import { validateEnv } from './configuration';

/**
 * Validated environment configuration.
 *
 * Evaluated at module import so the process fails fast on a bad configuration
 * rather than at the first request that needs the missing value.
 */
export const config = validateEnv();

export type { Env } from './env.validation';
