# Project Rules

## Package Manager

- This repo uses **npm** (`package-lock.json`). Memcard uses yarn; Escapement
  deviates deliberately — see `docs/HANDOFF.md`. To move it onto yarn, run
  `yarn import && rm package-lock.json` and switch the Dockerfile's two
  `npm ci` lines back to `yarn install --frozen-lockfile`.

## TypeScript Import Extensions

- Do NOT use file extensions in relative imports
- The project uses `moduleResolution: "bundler"` which handles extensions automatically
  - ✅ `import { foo } from './bar'`
  - ❌ `import { foo } from './bar.js'`

## The engine is not the place to add features

- New behaviour belongs in a `StateMachine` under `src/machines/`, not in
  `src/engine/`. If something cannot be expressed as decide/apply/snapshot,
  say so in the PR rather than widening the engine.
- `decide` must not mutate state. `apply` must be deterministic and must never
  throw — an event in the log already happened.
