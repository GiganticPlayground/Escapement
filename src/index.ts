import { readFileSync } from 'fs';
import { join } from 'path';

import cors from 'cors';
import express from 'express';
import helmet from 'helmet';
import swaggerUi from 'swagger-ui-express';
import YAML from 'yaml';

import { config } from './config/index';
import {
  authMiddleware,
  authRateLimitMiddleware,
  createOpenApiValidatorMiddleware,
  errorHandlerMiddleware,
  requestContextMiddleware,
} from './middlewares/index';
import { engine, selfEndpoint, logStore } from './services/index';
import { buildAnalytics, logger } from './utils/index';
import { setupShutdown } from './utils/shutdown';

export const apiSpecPath: string = join(process.cwd(), 'api/openapi.yaml');
const apiSpecContent: string = readFileSync(apiSpecPath, 'utf8');
const apiSpec: swaggerUi.JsonObject = YAML.parse(apiSpecContent) as swaggerUi.JsonObject;

const app = express();
app.set('trust proxy', config.TRUST_PROXY);

const corsOptions =
  !config.CORS_ORIGINS || config.CORS_ORIGINS === '*' ? undefined : { origin: config.CORS_ORIGINS };

app.use(helmet());
app.use(cors(corsOptions));
// JSON only — the spec declares no other request content type, so a urlencoded
// parser would be attack surface for a body the validator rejects anyway.
app.use(express.json({ limit: config.ESCAPEMENT_MAX_BODY_BYTES }));
app.use(requestContextMiddleware);

// Request/response analytics — opt-in, enabled only when a reqcast config is present.
const analytics = buildAnalytics();
if (analytics?.enabled) {
  app.use(analytics.middleware);
  logger.info('request analytics enabled');
}

if (config.API_DOCS_ENABLED) {
  app.use(
    '/api-docs',
    swaggerUi.serve,
    swaggerUi.setup(apiSpec, {
      explorer: true,
      customCss: '.swagger-ui .topbar { display: none }',
      customSiteTitle: 'Escapement API',
    }),
  );
}

if (config.RATE_LIMIT_ENABLED) {
  app.use('/v1/escapement', authRateLimitMiddleware);
}

// JWT verification guards every Escapement route before anything is committed.
app.use('/v1/escapement', authMiddleware);

app.use(createOpenApiValidatorMiddleware(apiSpecPath));
app.use(errorHandlerMiddleware);

const server = app.listen(config.PORT, () => {
  logger.info(`Escapement listening on port ${config.PORT}`);
  // The resolved layout, printed once. The bucket is often shared, and a wrong
  // prefix does not fail — it silently starts an empty history — so this line is
  // what makes a misconfiguration visible.
  logger.info(`Log: s3://${config.ESCAPEMENT_S3_BUCKET}/${logStore.logKey(1)}`);
  logger.info(`Peer endpoint: ${selfEndpoint}`);
});

// Elect, recover, and start serving. A failure here is fatal: the node cannot
// safely answer anything before it knows the committed history.
void engine.start().catch((err: unknown) => {
  logger.error('engine failed to start', { err: String(err) });
  process.exit(1);
});

// Drain in-flight commands and hand the lease over, so a rolling deploy fails
// over in milliseconds instead of waiting out the lease TTL. Then flush
// analytics sinks.
setupShutdown(server, config.SHUTDOWN_TIMEOUT_MS, {
  onDrained: async () => {
    await engine.shutdown(config.DRAIN_TIMEOUT_MS);
    if (analytics) await analytics.close();
  },
});
