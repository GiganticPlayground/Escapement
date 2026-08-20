# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## Project

Escapement — single-writer state machines over object storage. It serializes
every mutation through one writer, commits batches to S3 as an append-only log
using conditional writes, and serves reads from memory. `pool` (claim-once
allocation) and `quota` (counters with a ceiling) are the two state machines that
ship; the engine is generic.

Sibling of [Memcard](https://github.com/GiganticPlayground/memcard), from which
the scaffolding was taken: OpenAPI-first routing, `token-weaver` auth, `logra`
logging, `reqcast` analytics, Zod-validated env, node:test. Memcard handles state
that is independent per entity; Escapement handles state with invariants that
span entities. That line decides where a new feature belongs.

## Conventions (see `.claude/rules.md`)

- **npm**, not yarn — the one deliberate divergence from Memcard, explained in
  `docs/HANDOFF.md`.
- **No file extensions in relative imports.** `moduleResolution: "bundler"`
  resolves them; `scripts/fix-dist-esm-imports.js` re-adds `.js` in the build.
- ESM throughout, run with `tsx` in dev.
- `tsconfig` has `noUncheckedIndexedAccess` and `exactOptionalPropertyTypes` on.
- **One handoff document, ever: `docs/HANDOFF.md`.** Correct it in place; never
  add a dated or per-feature copy.

## Commands

```bash
npm run dev              # nodemon + tsx
npm run validate         # type-check + lint + format:check — run before finishing
npm test                 # unit suite
npm run test:failover    # two processes + stub S3: election, forwarding, failover
npm run gen-types        # regenerate src/types/schema.d.ts from the spec
npm run gen-controllers  # scaffold missing controllers (skips existing files)
```

## Architecture

**Routing is OpenAPI-driven, not manually registered.** `api/openapi.yaml`
validates every request and dispatches to a controller export by
`x-eov-operation-handler` (file in `src/controllers/`) and `x-eov-operation-id`
(exported function). There is no router file.

**Every mutation is a POST.** `PUT`/`PATCH`/`DELETE` are unused on purpose —
CDN and WAF layers routinely reject them by default, and enabling one at the edge
is an infrastructure change outside this repo. Do not add an endpoint that needs
one.

**Request flow** (`src/index.ts`): helmet → cors → json → `requestContext` →
optional reqcast → optional Swagger UI → optional rate limit on `/v1/escapement`
→ `authMiddleware` on `/v1/escapement` → OpenAPI validator/dispatch → error
handler.

**Engine** (`src/engine/`):
- `types.ts` — the `StateMachine` contract. `decide` is pure and runs before
  durability; `apply` is deterministic, total, and may mutate in place.
- `log-store.ts` — the only module that talks to S3. `append()` treats a `412`
  and a timeout identically: read the key back and compare `batchId`, which
  answers "did my write land?" and "have I been fenced?" in one GET.
- `engine.ts` — group-commit loop, idempotency, lease-based role election,
  follower tailing, snapshot/replay. After every committed batch the leader GETs
  the lease and refuses to answer callers unless it still holds it
  (`confirmLeadership`) — `If-None-Match` only fences a slot that still exists,
  and compaction deletes log keys, so a long-paused writer could otherwise
  re-create a pruned slot invisibly. In-batch key collisions, immediate results
  and rejections all settle only after the append lands; a fenced batch turns
  them into retryable 503s. Idempotency keys are stored scoped
  (`{app}:{userId}:{machine}:{clientKey}`, built in `src/services/dispatch.ts`)
  so one caller cannot replay another's key or cross machines.

**Reads are shaped by S3's price list.** `catchUp` walks `seq + 1` until a 404
instead of listing the log, and `tryAcquireLease` GETs before it PUTs. Both are
cost decisions — LIST is billed at the PUT rate and failed conditional writes are
billed like successful ones — and both are also strictly safer than what they
replaced. Do not reintroduce a LIST on the follower path; `test:failover` fails
if you do.

**Compaction is conservative on purpose.** It keeps `PRUNE_RETAIN` log entries
behind the snapshot so a prune cannot delete the very entry a lagging follower's
GET-walk is about to fetch — a follower that falls behind the horizon anyway
detects it via the lease's committed-`seq` hint and exits to rebuild — and it
refuses to run at all while the log or snapshot names a machine this build does
not register — otherwise an old binary snapshots that machine out of existence
and prunes the log that recorded it. A replay whose GET-walk 404s while the
lease reports a higher seq exits and rebuilds rather than skipping the gap.

**Failing closed is deliberate.** A fenced writer or an undeterminable commit
calls `fatal()`, which exits. Returning codes to the pool risks double-issuing;
rejecting the callers risks losing codes that committed. Exiting rebuilds from
the log, which is the only source of truth.

**The engine and log store are pinned to global symbols** in
`src/services/index.ts`. The OpenAPI validator loads controllers from disk at
request time and can instantiate this module a second time in another module
registry; without the pin you get two engines and a node stuck in `starting`
while the log says it became leader. That bug is caught by `npm run test:failover`,
not by the unit tests.

**Leader/follower.** One node holds `lease.json` and writes. Followers tail the
log, serve reads from their own (slightly stale) state, and forward mutations via
`src/services/forwarder.ts` (timeout: `LEASE_TTL_MS`, the failover horizon).
Followers report **healthy** — an unhealthy standby would be restarted in a loop
by the orchestrator. The lease is an optimisation; safety comes from the log's
conditional write. Renewal is `If-Match` on the etag the leader last wrote — a
412 means a peer took over, and the node exits rather than stealing the lease
back and serving stale reads. Shutdown releases the lease only after reading it
back and confirming it is still this node's (S3 has no conditional DELETE), so a
drain that outlives the TTL cannot delete the new leader's lease.

## Adding a state machine

`docs/state-machines.md` has the walkthrough. Short version: a file in
`src/machines/`, two lines in `src/services/index.ts`, operations in
`api/openapi.yaml`, a controller that calls `dispatch()`. No engine changes.

## Configuration

Zod-validated at import (`src/config/env.validation.ts`), so the process fails
fast. `ESCAPEMENT_KEY_PREFIX` and `ESCAPEMENT_ENV` become S3 key segments and go
through `keyPathVar()`, which rejects anything that could silently relocate the
log. Auth strategies come from a deployment config file
(`ESCAPEMENT_CONFIG_PATH`, else `config/escapement.yaml`); with no file, one
strategy is built from the `JWT_*` vars.

## Tests

`node:test`. `tests/unit/` covers the machines (decide purity, no double issue,
exhaustion, snapshot round-trip) and the engine against an in-memory log store
that models S3's conditional-write semantics (group commit, idempotency replay,
same-key-in-one-batch, fencing, recovery from log and from snapshot).

`tests/integration/failover.test.mjs` is **not** part of `npm test`. It spawns two
real processes against a stub S3 and covers what the unit tests structurally
cannot: election, follower forwarding, hard-kill promotion, graceful lease
hand-off, and that no code is issued twice across a leadership change.
