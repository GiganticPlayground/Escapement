# Writing a state machine

Everything Escapement does is a `StateMachine` plugged into one engine. Adding a
capability touches four files and never the engine.

## The contract

```ts
interface StateMachine<S, C, E, R> {
  readonly name: string;          // appears in the log; renaming invalidates history
  init(): S;                      // empty state for a fresh bucket
  decide(state: S, command: C): Decision<E, R>;
  apply(state: S, event: E): S;
  snapshot(state: S): Json;
  restore(raw: Json): S;
}
```

`decide` returns one of three things:

```ts
{ kind: 'commit', events, result }          // persist, then answer
{ kind: 'immediate', result }               // no state change, no S3 write
{ kind: 'reject', status, message, code? }  // no state change, error to the caller
```

## The two rules

**`decide` must not mutate.** It runs before durability, so nothing it returns is
true yet. If the commit fails, the state it inspected must be unchanged. Peeking
at the next free code is fine; popping it is not — that belongs in `apply`.

**`apply` must be deterministic and total.** It runs identically on a live commit
and during replay, and it must never throw, because an event in the log already
happened. Validation belongs in `decide`.

`apply` **may** mutate its argument and return the same reference. That matters
when the state is a 100k-entry pool. It is safe because the engine has no
rollback path: a commit whose outcome cannot be determined exits the process, and
the log rebuilds state from scratch.

## Idempotency is the engine's job

Do not carry idempotency keys in your commands. The engine records each key's
result, replays it on a retry, and collapses two requests that share a key inside
one batch. Your machine only ever sees commands it should actually perform.

## Timestamps and randomness

Generate them in the **controller** and pass them in the command, so `decide`
stays pure and unit-testable. They travel in the event, so replay reproduces the
original value rather than the replay-time clock. See `claimNext` in
`src/controllers/poolController.ts`.

## Snapshots

`snapshot()` must produce something `JSON.stringify` handles — no `Map`, `Set`,
`Date` or class instances at the leaves. `restore()` must tolerate a snapshot
written by an older build; treat unknown fields as absent rather than throwing,
or a rollback can never start.

## The checklist

1. `src/machines/<name>.machine.ts` — the machine, plus any read helpers (pure
   functions over its state; the controllers call them through `engine.query`).
2. `src/machines/index.ts` — re-export it.
3. `src/services/index.ts` — add it to the `machines: [...]` array.
4. `api/openapi.yaml` — the operations, with `x-eov-operation-handler` and
   `x-eov-operation-id`. Mutations are **POST**, take a required
   `Idempotency-Key`, and live under `/v1/escapement/admin/...` if they should be
   restricted to admin credentials.
5. `src/controllers/<name>Controller.ts` — thin. Mutations call `dispatch()`;
   reads call `engine.query()`.
6. `tests/unit/<name>.machine.test.ts` — at minimum: `decide` does not mutate,
   the invariant cannot be violated, rejections carry the right status, and
   snapshot/restore round-trips through JSON.

## Is it actually a state machine for this engine?

Ask whether the decision depends on a fact that **spans entities**. "Only 5,000
of these exist", "no more than 3 per user per day", "this slot is taken" — yes.
"This player's save file" — no; that is
[Memcard](https://github.com/GiganticPlayground/memcard), which is stateless and
scales horizontally precisely because it never needs to know about other keys.

And check the state fits comfortably in memory. Escapement holds everything in
RAM by design; a machine whose state grows without bound turns a small correct
service into a bad database.
