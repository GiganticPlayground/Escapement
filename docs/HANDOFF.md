# Handoff

Where the work stands, decisions whose reasoning is not visible in the code, and
unclaimed open items. Architecture lives in `docs/ARCHITECTURE.md`, usage in
`README.md`, and anything git history records belongs in neither.

## Status

Complete and exercised end to end against a stub S3: OpenAPI-first HTTP layer,
auth via `token-weaver`, the engine (group commit, conditional-write log,
snapshot/replay, lease-based leader election, follower tailing and forwarding),
and two state machines (`pool`, `quota`).

- `npm test` — 73 unit tests, green.
- `npm run test:failover` — 26 black-box checks across two live processes, green.
  Covers election, follower forwarding, follower read convergence, quota
  enforcement across nodes and the shape of its refusal, per-subject quota
  scoping, the idle request mix, SIGKILL promotion inside the lease TTL, graceful
  hand-off in ~300–500ms, and no code reissued across a leadership change.
- Docker image builds and runs: elects itself leader against a stub S3, serves
  `/health`, and refuses an unauthenticated request.
- `.github/workflows/docker-publish.yml` builds and pushes to GHCR on `main`
  (`:latest-main`) and on tags (`:<git-tag>`), for `linux/amd64` and
  `linux/arm64`. The image is gated: `validate`, the unit suite and the failover
  suite must all pass before anything is built or pushed.

Not yet done: never run against real S3, and no load test.

## Decisions worth knowing

**npm, not yarn.** Memcard mandates yarn and this repo does not. The lockfile was
generated with npm because yarn could not resolve the three GitHub dependencies
in the environment where this was authored. Nothing depends on the choice — run
`yarn import && rm package-lock.json` and change the Dockerfile's two `npm ci`
lines back to `yarn install --frozen-lockfile`. Do that before this diverges
further from Memcard's tooling.

**The Dockerfile does not build the Git dependencies.** It used to try, with three
explicit `tsc` invocations, and it could never have worked: `logra`,
`token-weaver` and `reqcast` all publish `files: ["dist"]`, so npm runs their
`prepare` at fetch time and what lands in `node_modules` is already built with no
`tsconfig.json` to point at. The image also needs no `git`. If a future dependency
does need building, check what its tarball actually contains first.

**Every mutation is POST.** Not a style preference. Edge layers in front of a
deployment routinely reject non-POST write methods by default, and enabling one
is an infrastructure change outside this service's control. `PUT`, `PATCH` and
`DELETE` are therefore unused here and the API is shaped so none is ever needed.
Adding one would reintroduce a problem the design already avoids.

**Failing closed on an undeterminable commit.** `fatal()` exits the process. The
alternative paths are both worse: returning the codes to the pool risks issuing
them twice, and rejecting the callers risks losing codes that actually
committed. Exiting rebuilds from the log, which is the only thing that knows.

**Leadership is confirmed after every committed batch.** `If-None-Match` only
fences a slot that still exists, and compaction deletes log keys — so a writer
paused long enough for a successor to commit past its seq *and* prune it could
re-create the slot invisibly and double-issue. `confirmLeadership` GETs the
lease after the PUT lands and refuses to answer callers unless this node still
holds it; a commit is one PUT plus one GET, roughly an 8% add. Failing closed
here is safe: the commit is durable, so rejected callers retrying against the
next leader are answered from the idempotency record, never re-issued.

**Batch responses are deferred until durability.** In-batch idempotency-key
collisions, immediate results and machine rejections may all have been decided
against provisional state an earlier command in the same batch changed, so all
of them settle only after the append lands — a fenced batch turns every one into
a retryable 503. Only replays of already-durable idempotency records are
answered before the PUT.

**Idempotency keys are scoped.** The engine stores
`{app}:{userId}:{machine}:{clientKey}` (`scopedKey` in
`src/services/dispatch.ts`), never the raw header alone — a raw key is a single
global namespace, so one caller reusing or guessing another's key would be
handed the other caller's stored result, and a key first used on a pool claim
would answer a quota consume. Client-visible semantics are unchanged: retry the
same operation with the same key and the same credential.

**The lease is not load-bearing for safety.** It only stops two nodes from
wasting effort fighting over the writer role. Correctness comes from
`If-None-Match: *` on the log entry, so a takeover that guesses wrong about
liveness still cannot double-issue — the loser is fenced on its first commit and
exits. Do not "harden" the lease on the assumption that safety depends on it;
tighten the commit path instead.

**Lease renewal is conditional; release is verified.** Renewal is `If-Match` on
the etag the leader last wrote — a 412 means a peer took over, and the node
exits to rejoin as a follower instead of stealing the lease back and serving
arbitrarily stale reads (an unconditional renew was exactly how a resurrected
leader used to re-advertise itself). Shutdown reads the lease and deletes only
if it is still this node's — S3 has no conditional DELETE — so a drain that
outlives the TTL no longer deletes the new leader's lease and forces a second,
avoidable election.

**The forwarder times out at `LEASE_TTL_MS`, not `SHUTDOWN_TIMEOUT_MS`.** The
lease TTL is the failover horizon: if the leader is stuck, a replacement exists
within one TTL, so waiting longer only pins the client — and it keeps the 503's
`retryAfterMs` advice honest.

**Followers report healthy.** The obvious design — standby fails its health check
so the routing mesh skips it — crash-loops, because orchestrators restart
unhealthy tasks. Traffic steering lives in the app (`src/services/forwarder.ts`).
If someone "fixes" the health check to report unhealthy on a follower, Swarm will
churn that task forever.

**The engine is pinned to a global symbol.** `src/services/index.ts` stores the
engine and log store on `Symbol.for(...)` keys. express-openapi-validator loads
controllers from disk at request time and can instantiate the module a second
time in a different module registry; without the pin the HTTP layer talks to an
engine that never elected itself, and the node reports `starting` forever while
the log says it became leader. This was a real bug found by the failover suite
and invisible to the unit tests.

**The batch window is skipped after idle.** A window only merges requests when a
second one arrives inside it — below roughly `1000 / BATCH_WINDOW_MS` commands
per second it batches nothing and adds its full length to every caller. Above
that rate the in-flight PUT already does the merging, since anything arriving
during a commit rides the next batch. So the wait is only worth paying when
commits are already back to back, which is precisely when the queue is non-empty
at the top of the loop. An isolated claim now costs one S3 round trip rather than
a round trip plus 50ms; bursts batch exactly as before. Do not "restore" the
unconditional wait for consistency — at this service's arrival rates it is pure
latency.

**Followers cost GETs, not LISTs or PUTs.** Two habits dominated the bill of an
idle cluster, and neither bought anything. `catchUp` listed the log every poll to
find the head, and S3 prices LIST at the PUT rate — 12.5x a GET — for information
a dense sequence already implies. `tryAcquireLease` led with a create-only PUT
that a follower expects to 412, and AWS bills failed conditional requests at
normal rates, so the common "no, someone else has it" answer arrived at write
prices. Both are now GETs: the log head is found by walking `seq + 1` until a
404, and the lease is read before it is written. Roughly $27/month per follower
down to about $2. The conditional headers still do all the fencing; they are just
no longer how the question gets asked. `test:failover` asserts the idle
request mix so neither habit comes back.

**Replay never skips a hole.** The log is never listed — a follower GET-walks
`seq + 1` until a 404 — so a hole shows up as a 404 while the lease's
committed-`seq` hint says the log runs further: compaction pruned an entry this
node still needed. Continuing past it would drop everything it committed and
leave the node diverged with `seq` at the log head, so nothing downstream — not
even the commit fence — could notice. It exits instead, and `PRUNE_RETAIN` keeps
a margin behind each snapshot so a prune cannot delete the very entry a lagging
follower is about to walk into. Two mechanisms for one bug is deliberate: the
margin makes it rare, the exit makes it loud.

**Compaction stands down when a machine is missing.** Dropping unregistered
machines on boot is what makes a rollback possible, but a snapshot is built from
the registry — so an old build would write a snapshot without that machine and
then prune the log entries that were its last record, destroying it permanently
during the exact scenario the dropping exists to support. It needed
`SNAPSHOT_EVERY` commits to trigger, so a quick rollback was fine and a slow one
was not. Compaction now refuses to run at all while `unknownMachines` is
non-empty. The cost is an unbounded log and slower recovery for as long as the
old build runs, both visible in the logs and both self-correcting once the
machine is registered again.

**`apply` may mutate.** Deliberate, so a 100k-entry pool is not copied per event.
Safe only because of the fail-closed rule above. If the engine ever grows an
in-memory rollback path, this has to change with it.

**Timestamps come from controllers, not machines.** Keeps `decide` pure and
unit-testable, and puts the value in the event so replay reproduces the original
rather than the replay-time clock.

**Engine status is admin-only.** `GET /v1/escapement/engine` moved to
`GET /v1/escapement/admin/engine`: it maps internal cluster topology (node ids,
endpoints, the leader's address), which no player credential needs.

**The `JWT_*` group is conditional.** When a config file supplies the auth
strategies (`ESCAPEMENT_CONFIG_PATH`, or `config/escapement.yaml` when present),
`JWT_ISSUER`/`JWKS_URI`/`JWT_SECRET` are not required — requiring them anyway
was pure deployment friction. `JWT_AUDIENCE` is strongly recommended: without an
audience, verification checks only signature and issuer, so tokens the same
issuer minted for a different service are accepted here too; each JWT strategy
that omits one logs a startup warning.

**Request surfaces are tightened.** Bodies reject unknown fields
(`additionalProperties: false`), `ClaimRequest.metadata` is bounded (32
properties, flat string values ≤ 1024 chars — it lives in memory and in every
snapshot for the life of the claim), and `5xx` responses are generic, with
internal error detail going to the logs only.

## Open items

- **Shard the pool** if one writer stops being enough. Disjoint code sets per
  writer, each with its own key prefix, is the only version worth building; a
  shared log with optimistic retry gets *slower* as writers are added. Uneven
  drain (one shard empty while another is full) is the part that needs design —
  forward-on-exhaustion is the cheap fix, shard stealing the proper one.
- **Idempotency eviction is age-based and bounded by `IDEMPOTENCY_LIMIT`.** A
  client retrying after more than that many intervening commits gets a fresh
  claim rather than its original code. Fine at current volumes; revisit if a
  caller can retry hours later.
- **A follower that throws in `apply` stalls silently.** `followLoop` catches the
  replay error, logs `follower poll failed`, keeps reporting healthy and never
  promotes. It cannot diverge — `seq` does not advance — but it serves
  increasingly stale reads forever. Only reachable via a machine that violates
  the total-`apply` rule, so it is a bug amplifier rather than a bug.
- **A restarted single instance waits out its own dead lease.** With no standby
  the TTL stops bounding failover and starts bounding restart: the new process
  reads a still-valid lease left by its predecessor, becomes a follower, and
  `forwardToLeader` correctly refuses to forward to itself, so callers get
  `503 NO_LEADER` until the lease expires. Recognising `held.endpoint ===
  this.endpoint` as "that was me" and taking over via the existing `If-Match`
  path would remove the stall — safe either way, since a wrong guess is fenced at
  the first commit. Only works where the endpoint is stable across restarts,
  which rules out Swarm's `{{.Task.Name}}`.
- **Seeding writes one large object.** 100k codes is a few MB in a single log
  entry. S3 is fine with it, but a very large pool would be better chunked.
- **No response validation.** `validateResponses` is off, matching Memcard.
- **Real-S3 verification.** There is no suite that drives a live AWS bucket. The
  conditional-write behaviour everything depends on is modelled by a stub, and a
  stub cannot prove AWS agrees. Worth a small harness that seeds a pool, claims
  concurrently from two processes, and asserts no code is issued twice.
