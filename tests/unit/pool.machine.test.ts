import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { Json } from '../../src/engine/types';
import { CodePool, poolMachine, poolStats, lookupClaim } from '../../src/machines/pool.machine';
import type { PoolCommand, PoolState } from '../../src/machines/pool.machine';

const AT = '2026-08-19T12:00:00.000Z';

/** Narrow a committed decision's result without repeating the cast everywhere. */
function resultOf<T>(decision: { kind: string; result?: unknown }): T {
  assert.equal(decision.kind, 'commit');
  return decision.result as T;
}

/** Run a command the way the engine does: decide, then apply on commit. */
function run(state: PoolState, command: PoolCommand) {
  const decision = poolMachine.decide(state, command);
  if (decision.kind === 'commit') {
    for (const event of decision.events) poolMachine.apply(state, event);
  }
  return decision;
}

function seeded(codes: string[]): PoolState {
  const state = poolMachine.init();
  run(state, { type: 'seed', pool: 'p', codes });
  return state;
}

describe('CodePool', () => {
  it('never issues or loses a code under mixed take patterns', () => {
    const N = 5000;
    const all = Array.from({ length: N }, (_, i) => `CODE-${i}`);
    const pool = new CodePool();
    for (const c of all) pool.add(c);
    pool.add(all[0]!); // duplicate add is a no-op
    assert.equal(pool.size, N);

    const taken = new Set<string>();
    let rng = 12345;
    const rand = (): number => (rng = (rng * 1103515245 + 12345) % 2147483648) / 2147483648;

    while (pool.size > 0) {
      if (rand() < 0.5) {
        const code = pool.takeNext()!;
        assert.ok(!taken.has(code), `takeNext issued ${code} twice`);
        taken.add(code);
      } else {
        const guess = all[Math.floor(rand() * N)]!;
        if (pool.take(guess)) {
          assert.ok(!taken.has(guess), `take() issued ${guess} twice`);
          taken.add(guess);
        } else {
          assert.ok(taken.has(guess), `take() refused unclaimed ${guess}`);
        }
      }
    }
    assert.equal(taken.size, N);
    assert.equal(pool.takeNext(), undefined);
  });

  it('peekNext does not remove', () => {
    const pool = new CodePool();
    pool.add('a');
    assert.equal(pool.peekNext(), 'a');
    assert.equal(pool.size, 1);
  });
});

describe('poolMachine.decide', () => {
  it('does not mutate state — nothing is true before the commit lands', () => {
    const state = seeded(['A', 'B']);
    poolMachine.decide(state, { type: 'claimNext', pool: 'p', by: 'player-1', at: AT });
    assert.equal(poolStats(state, 'p')!.remaining, 2);
  });

  it('claims each code exactly once until the pool is exhausted', () => {
    const state = seeded(['A', 'B', 'C']);
    const got = new Set<string>();
    for (let i = 0; i < 3; i++) {
      const d = run(state, { type: 'claimNext', pool: 'p', by: 'player-1', at: AT });
      assert.equal(d.kind, 'commit');
      const code = resultOf<{ code: string }>(d).code;
      assert.ok(!got.has(code));
      got.add(code);
    }
    assert.equal(got.size, 3);

    const exhausted = run(state, { type: 'claimNext', pool: 'p', by: 'player-1', at: AT });
    assert.equal(exhausted.kind, 'reject');
    assert.equal((exhausted as { status: number }).status, 409);
    assert.equal((exhausted as { code?: string }).code, 'POOL_EXHAUSTED');
  });

  it('rejects an unknown pool with 404 rather than inventing one', () => {
    const state = poolMachine.init();
    const d = poolMachine.decide(state, { type: 'claimNext', pool: 'nope', by: 'x', at: AT });
    assert.equal(d.kind, 'reject');
    assert.equal((d as { status: number }).status, 404);
  });

  it('claims a specific code, then reports 409 for the second claimant', () => {
    const state = seeded(['A', 'B']);
    const first = run(state, { type: 'claimCode', pool: 'p', code: 'A', by: 'player-1', at: AT });
    assert.equal(first.kind, 'commit');

    const second = poolMachine.decide(state, {
      type: 'claimCode',
      pool: 'p',
      code: 'A',
      by: 'eve',
      at: AT,
    });
    assert.equal(second.kind, 'reject');
    assert.equal((second as { status: number }).status, 409);
    assert.equal((second as { code?: string }).code, 'ALREADY_CLAIMED');
  });

  it('404s a code that is not in the pool at all', () => {
    const state = seeded(['A']);
    const d = poolMachine.decide(state, {
      type: 'claimCode',
      pool: 'p',
      code: 'ZZZ',
      by: 'player-1',
      at: AT,
    });
    assert.equal(d.kind, 'reject');
    assert.equal((d as { status: number }).status, 404);
  });

  it('seeding is additive and ignores codes already present', () => {
    const state = seeded(['A', 'B']);
    const again = run(state, { type: 'seed', pool: 'p', codes: ['B', 'C'] });
    assert.equal(resultOf<{ added: number }>(again).added, 1);
    assert.equal(poolStats(state, 'p')!.remaining, 3);
    assert.equal(poolStats(state, 'p')!.total, 3);
  });

  it('a re-seed of only known codes needs no S3 write at all', () => {
    const state = seeded(['A']);
    const d = poolMachine.decide(state, { type: 'seed', pool: 'p', codes: ['A'] });
    assert.equal(d.kind, 'immediate');
  });

  it('release returns a claimed code to the pool', () => {
    const state = seeded(['A']);
    run(state, { type: 'claimCode', pool: 'p', code: 'A', by: 'player-1', at: AT });
    assert.equal(poolStats(state, 'p')!.remaining, 0);

    run(state, { type: 'release', pool: 'p', code: 'A' });
    assert.equal(poolStats(state, 'p')!.remaining, 1);
    assert.equal(lookupClaim(state, 'p', 'A'), null);

    const again = poolMachine.decide(state, { type: 'release', pool: 'p', code: 'A' });
    assert.equal(again.kind, 'reject');
    assert.equal((again as { status: number }).status, 404);
  });

  it('keeps pools independent', () => {
    const state = poolMachine.init();
    run(state, { type: 'seed', pool: 'a', codes: ['X'] });
    run(state, { type: 'seed', pool: 'b', codes: ['X'] });
    run(state, { type: 'claimCode', pool: 'a', code: 'X', by: 'player-1', at: AT });
    assert.equal(poolStats(state, 'a')!.remaining, 0);
    assert.equal(poolStats(state, 'b')!.remaining, 1);
  });
});

describe('poolMachine snapshot/restore', () => {
  it('round-trips free codes, claims and totals', () => {
    const state = seeded(['A', 'B', 'C']);
    run(state, { type: 'claimCode', pool: 'p', code: 'B', by: 'player-1', at: AT });

    const restored = poolMachine.restore(poolMachine.snapshot(state));
    assert.deepEqual(poolStats(restored, 'p'), poolStats(state, 'p'));
    assert.deepEqual(lookupClaim(restored, 'p', 'B'), lookupClaim(state, 'p', 'B'));

    // And the restored pool must not re-issue the claimed code.
    const codes = new Set<string>();
    for (let i = 0; i < 2; i++) {
      const d = run(restored, { type: 'claimNext', pool: 'p', by: 'x', at: AT });
      codes.add(resultOf<{ code: string }>(d).code);
    }
    assert.ok(!codes.has('B'));
  });

  it('survives a snapshot that JSON has round-tripped', () => {
    const state = seeded(['A']);
    const raw = JSON.parse(JSON.stringify(poolMachine.snapshot(state))) as Json;
    assert.equal(poolStats(poolMachine.restore(raw), 'p')!.remaining, 1);
  });
});
