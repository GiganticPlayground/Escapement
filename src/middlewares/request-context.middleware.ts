import { randomUUID } from 'crypto';

import type { NextFunction, Request, Response } from 'express';
import { addLogContext, runLogContext } from 'logra';

import { logger } from '../utils/index';

/**
 * A client-supplied correlation id is only honored when it looks like one:
 * bounded length, no control characters or line breaks. Anything else would be
 * reflected verbatim into every log line and the response header — a log
 * injection and forged-correlation vector — so it is replaced, not sanitized.
 */
const REQUEST_ID_PATTERN = /^[\x20-\x7E]{1,128}$/;

export function requestContextMiddleware(req: Request, res: Response, next: NextFunction): void {
  const supplied = req.header('x-request-id');
  const requestId =
    supplied !== undefined && REQUEST_ID_PATTERN.test(supplied) ? supplied : randomUUID();
  // Make the (possibly generated) id readable by downstream consumers such as
  // the reqcast analytics middleware, which reads it from the request headers.
  req.headers['x-request-id'] = requestId;
  const startedAt = Date.now();

  runLogContext(() => {
    addLogContext('requestId', requestId);
    addLogContext('method', req.method);
    addLogContext('path', req.path);
    addLogContext('ip', req.ip);

    res.setHeader('x-request-id', requestId);

    logger.info('START - INCOMING HTTP REQUEST');

    res.on('finish', () => {
      logger.info('END - INCOMING HTTP REQUEST', {
        durationMs: Date.now() - startedAt,
        statusCode: res.statusCode,
      });
    });

    next();
  });
}
