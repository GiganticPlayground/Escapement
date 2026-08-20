export class HttpError extends Error {
  status: number;
  errors?: unknown;

  constructor(status: number, message: string, errors?: unknown) {
    super(message);
    this.name = 'HTTP_ERROR';
    this.status = status;
    this.errors = errors;
  }
}

/**
 * The node cannot commit right now: no leader elected, the leader is
 * unreachable from this follower, S3 is failing, or the writer was fenced.
 * Always retryable — a follower is promoted within the lease TTL.
 */
export class UpstreamUnavailableError extends HttpError {
  constructor(message = 'Cannot commit right now') {
    super(503, message);
    this.name = 'UPSTREAM_UNAVAILABLE';
  }
}

/** A pool has no unclaimed codes left. */
export class PoolExhaustedError extends HttpError {
  constructor(pool: string) {
    super(409, `Pool '${pool}' has no unclaimed codes left`);
    this.name = 'POOL_EXHAUSTED';
  }
}
