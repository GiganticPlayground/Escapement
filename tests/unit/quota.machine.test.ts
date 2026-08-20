import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { Json } from '../../src/engine/types';
import { quotaMachine, quotaView } from '../../src/machines/quota.machine';
import type { QuotaCommand, QuotaState } from '../../src/machines/quota.machine';

function run(state: QuotaState, command: QuotaCommand) {
  const decision = quotaMachine.decide(state, command);
  if (decision.kind === 'commit') {
    for (const event of decision.events) quotaMachine.apply(state, event);
  }
  return decision;
}

function defined(limit: number, perSubject = false): QuotaState {
  const state = quotaMachine.init();
  run(state, { type: 'define', quota: 'q', limit, perSubject });
  return state;
}

describe('quotaMachine', () => {
  it('consumes up to the ceiling and then rejects with 429', () => {
    const state = defined(3);
    for (let i = 0; i < 3; i++) {
      assert.equal(
        run(state, { type: 'consume', quota: 'q', subject: 's', amount: 1 }).kind,
        'commit',
      );
    }
    const over = run(state, { type: 'consume', quota: 'q', subject: 's', amount: 1 });
    assert.equal(over.kind, 'reject');
    assert.equal((over as { status: number }).status, 429);
    assert.equal((over as { code?: string }).code, 'QUOTA_EXCEEDED');
    // A rejected consume must not have spent anything.
    assert.equal(quotaView(state, 'q')!.used, 3);
  });

  it('rejects an oversized single consume without partially applying it', () => {
    const state = defined(10);
    const d = run(state, { type: 'consume', quota: 'q', subject: 's', amount: 11 });
    assert.equal(d.kind, 'reject');
    assert.equal(quotaView(state, 'q')!.used, 0);
  });

  it('tracks per-subject ceilings independently', () => {
    const state = defined(2, true);
    run(state, { type: 'consume', quota: 'q', subject: 'alice', amount: 2 });
    const alice = run(state, { type: 'consume', quota: 'q', subject: 'alice', amount: 1 });
    assert.equal(alice.kind, 'reject');

    const bob = run(state, { type: 'consume', quota: 'q', subject: 'bob', amount: 1 });
    assert.equal(bob.kind, 'commit');
    assert.equal(quotaView(state, 'q', 'bob')!.used, 1);
    assert.equal(quotaView(state, 'q', 'alice')!.remaining, 0);
  });

  it('404s consumption against an undefined quota', () => {
    const state = quotaMachine.init();
    const d = quotaMachine.decide(state, {
      type: 'consume',
      quota: 'nope',
      subject: 's',
      amount: 1,
    });
    assert.equal(d.kind, 'reject');
    assert.equal((d as { status: number }).status, 404);
  });

  it('redefining preserves usage, so a lowered ceiling stays exhausted', () => {
    const state = defined(10);
    run(state, { type: 'consume', quota: 'q', subject: 's', amount: 8 });
    run(state, { type: 'define', quota: 'q', limit: 5, perSubject: false });
    assert.equal(quotaView(state, 'q')!.used, 8);
    assert.equal(quotaView(state, 'q')!.remaining, 0);
    assert.equal(
      run(state, { type: 'consume', quota: 'q', subject: 's', amount: 1 }).kind,
      'reject',
    );
  });

  it('an identical redefinition costs no S3 write', () => {
    const state = defined(5);
    assert.equal(
      quotaMachine.decide(state, { type: 'define', quota: 'q', limit: 5, perSubject: false }).kind,
      'immediate',
    );
  });

  it('refuses to describe a per-subject quota without a subject', () => {
    // The ceiling is per subject, so pairing it with a total across every
    // subject would report a limit of 3 against 5 used. There is no answer.
    const state = defined(3, true);
    run(state, { type: 'consume', quota: 'q', subject: 'a', amount: 3 });
    run(state, { type: 'consume', quota: 'q', subject: 'b', amount: 2 });

    assert.equal(quotaView(state, 'q'), null, 'no subject, no answer');
    assert.deepEqual(
      { used: quotaView(state, 'q', 'a')!.used, remaining: quotaView(state, 'q', 'a')!.remaining },
      { used: 3, remaining: 0 },
    );
    assert.deepEqual(
      { used: quotaView(state, 'q', 'b')!.used, remaining: quotaView(state, 'q', 'b')!.remaining },
      { used: 2, remaining: 1 },
    );
  });

  it('does not accumulate subjects for a whole-quota ceiling', () => {
    // Recording every caller in a map nothing enforces would grow the state, and
    // every snapshot, without bound.
    const state = defined(100, false);
    for (const who of ['a', 'b', 'c', 'd']) {
      run(state, { type: 'consume', quota: 'q', subject: who, amount: 1 });
    }
    assert.equal(quotaView(state, 'q')!.used, 4, 'the total is still tracked');
    assert.equal(quotaView(state, 'q')!.subjects, 0, 'but not who spent it');
  });

  it('rejects a non-positive amount even though the spec blocks it first', () => {
    const state = defined(10);
    for (const amount of [0, -5, 1.5]) {
      const d = quotaMachine.decide(state, { type: 'consume', quota: 'q', subject: 's', amount });
      assert.equal(d.kind, 'reject', `amount ${amount} must not commit`);
      assert.equal((d as { status: number }).status, 400);
    }
    assert.equal(quotaView(state, 'q')!.used, 0, 'nothing was applied');
  });

  it('answers a define with the definition, not a usage position', () => {
    const state = defined(3, true);
    run(state, { type: 'consume', quota: 'q', subject: 'a', amount: 2 });
    const d = quotaMachine.decide(state, {
      type: 'define',
      quota: 'q',
      limit: 3,
      perSubject: true,
    });
    assert.equal(d.kind, 'immediate', 'an identical redefinition costs no S3 write');
    assert.deepEqual((d as { result: unknown }).result, {
      quota: 'q',
      limit: 3,
      perSubject: true,
      subjects: 1,
    });
  });

  it('round-trips through snapshot/restore', () => {
    const state = defined(4, true);
    run(state, { type: 'consume', quota: 'q', subject: 'alice', amount: 3 });
    const restored = quotaMachine.restore(
      JSON.parse(JSON.stringify(quotaMachine.snapshot(state))) as Json,
    );
    assert.deepEqual(quotaView(restored, 'q', 'alice'), quotaView(state, 'q', 'alice'));
    assert.equal(
      run(restored, { type: 'consume', quota: 'q', subject: 'alice', amount: 2 }).kind,
      'reject',
    );
  });
});
