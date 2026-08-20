import type { NextFunction, Request, Response } from 'express';

import { engine } from '../services/index';

/**
 * Engine status. Served from memory on any node.
 *
 * @route GET /v1/escapement/engine
 */
export const getEngineStatus = async (
  _req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> => {
  try {
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
  } catch (error) {
    next(error);
  }
};
