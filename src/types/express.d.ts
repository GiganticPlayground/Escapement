/**
 * Express request augmentation.
 *
 * `req.auth` is populated by the JWT verification middleware. Controllers read
 * it to decide who a claim is for when the request body does not say, and the
 * admin operations read `req.authStrategy` to re-check their privilege.
 */
export interface AuthContext {
  /** Caller identity, taken from the JWT `sub` claim. */
  userId: string;
  /** Application namespace, taken from the configured app claim. */
  app: string;
}

/**
 * Which configured strategy accepted the request. Recorded so the admin
 * controllers can re-check the privilege they depend on instead of trusting
 * that the middleware ran, and so logs can tell the callers apart.
 */
export interface AuthStrategyContext {
  type: 'jwks' | 'hs256' | 'static';
  /** Whether this credential is allowed on the admin routes. */
  admin: boolean;
  /** Issuer of the verified token — absent for a static token. */
  issuer?: string;
  /**
   * Whether this credential is a configured service caller: a static token the
   * deployment gave a fixed identity, so it can use the normal routes without
   * naming a player of its own. Only ever set for `static`.
   */
  service?: boolean;
}

declare global {
  namespace Express {
    interface Request {
      auth?: AuthContext;
      authStrategy?: AuthStrategyContext;
    }
  }
}
