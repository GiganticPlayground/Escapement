import type { Request, Response } from 'express';

import { requireAdmin } from '../services/dispatch';
import { engine } from '../services/index';

/**
 * Engine status. Served from memory on any node. Admin only: it names internal
 * endpoints, the current leader, and live queue depth — cluster topology that a
 * player credential has no business mapping.
 *
 * @route GET /v1/escapement/engine
 */
export const getEngineStatus = async (req: Request, res: Response): Promise<void> => {
  requireAdmin(req);
  res.status(200).json({
    nodeId: engine.nodeId,
    role: engine.role,
    seq: engine.sequence,
    endpoint: engine.endpoint,
    ...(engine.leaderEndpoint ? { leaderEndpoint: engine.leaderEndpoint } : {}),
    queued: engine.queueDepth,
    draining: engine.draining,
    machines: engine.machineNames,
  });
};
