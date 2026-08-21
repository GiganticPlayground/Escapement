# Escapement — build TypeScript ahead of time, run the compiled output.
#
# All npm work runs on the build host's native platform ($BUILDPLATFORM).
# CI builds linux/amd64 and linux/arm64 in one pass; the non-native half runs
# under QEMU, where npm's git-dependency preparation stalls on network reads
# and times out (~27 minutes, exit 146). Nothing in the production dependency
# tree is a native addon — the only arch-specific packages in the lockfile are
# esbuild's dev-only binaries — so node_modules and dist are architecture-
# independent and the per-arch runtime stage only assembles files. If a native
# production dependency is ever added, this shortcut stops being valid and the
# installs have to move back into the target-arch stage.

FROM --platform=$BUILDPLATFORM node:24-slim AS builder
WORKDIR /app

# logra, token-weaver and reqcast are Git dependencies. npm fetches each as a
# tarball, runs its `prepare` script, and packs the result — and each of them
# publishes `files: ["dist"]`, so what lands in node_modules is already built.
# Nothing here needs to compile them, and nothing here needs git.
COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

# Reinstall with dev dependencies omitted to get the runtime node_modules.
RUN rm -rf node_modules && npm ci --omit=dev && npm cache clean --force

FROM node:24-slim AS runtime
RUN apt-get update && apt-get install -y \
  ca-certificates \
  curl \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY package.json package-lock.json ./
COPY --from=builder /app/node_modules ./node_modules
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
