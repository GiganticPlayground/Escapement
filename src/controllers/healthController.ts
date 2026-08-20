import type { Request, Response } from 'express';

import { engine } from '../services/index';

/**
 * Health check.
 *
 * A follower reports **healthy** on purpose. Orchestrators restart unhealthy
 * tasks, so a standby that advertised itself as unhealthy would crash-loop;
 * traffic is steered by forwarding instead. See the README.
 *
 * @route GET /health
 */
export const getHealth = async (_req: Request, res: Response): Promise<void> => {
  const serving = engine.role !== 'starting' && !engine.draining;
  res.status(serving ? 200 : 503).json({
    status: engine.draining ? 'draining' : engine.role === 'starting' ? 'starting' : 'ok',
    service: 'escapement',
    role: engine.role,
    timestamp: new Date().toISOString(),
  });
};
