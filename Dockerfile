# Escapement — build TypeScript ahead of time, run the compiled output.

FROM node:24-slim AS base
RUN apt-get update && apt-get install -y \
  ca-certificates \
  curl \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app

FROM base AS builder

# logra, token-weaver and reqcast are Git dependencies. npm fetches each as a
# tarball, runs its `prepare` script, and packs the result — and each of them
# publishes `files: ["dist"]`, so what lands in node_modules is already built.
# Nothing here needs to compile them, and nothing here needs git.
COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

FROM base AS runtime

COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY --from=builder /app/dist ./dist
COPY --from=builder /app/api ./api

# The auth config file is deliberately NOT baked in: it is a per-deployment
# artifact. Mount it and set ESCAPEMENT_CONFIG_PATH, or leave both out to run
# with the single strategy described by the JWT_* env vars.

EXPOSE 3000
RUN chown -R node:node /app
USER node

# Followers report healthy on purpose — see README, "Running a warm standby".
HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=40s \
  CMD curl -f http://localhost:3000/health || exit 1

CMD ["node", "dist/src/index.js"]
