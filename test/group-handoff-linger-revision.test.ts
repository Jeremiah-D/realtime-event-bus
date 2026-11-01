import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type GroupRebalanceEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-handoff-linger-'));

const noop = (): void => {};

describe('setGroupHandoffLinger (EB-335)', () => {
  test('the revised window governs the next departure', async () => {
    let now = 1_000_000;
    const dir = freshDir();
    const bus = new EventBus({ durableLogDir: dir, now: () => now });
    const events: GroupRebalanceEvent[] = [];
    const member = bus.subscribeToGroup('g', 'jobs.*', noop, { handoffLingerMs: 30_000 });
    bus.subscribeToGroup('g', 'jobs.*', noop, { onRebalance: (e) => events.push(e) });
    for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', {});
    await flush();
    bus.commitOffset('g', 'jobs.email', 3);

    bus.setGroupHandoffLinger(member.id, 60_000);
    member.unsubscribe();

    const leave = events.find((e) => e.trigger === 'leave');
    assert.ok(leave !== undefined);
    assert.equal(leave.lingerUntil, now + 60_000);
    assert.deepEqual(leave.lingering, [{ topic: 'jobs.email', fromSeq: 3, toSeq: 5 }]);

    const audit = bus.getSubscriptionAudit({ action: 'group_handoff_linger_revised' });
    assert.equal(audit.length, 1);
    assert.equal(audit[0].subId, member.id);
    assert.equal(audit[0].groupId, 'g');
    assert.deepEqual(audit[0].detail, { handoffLingerMs: 60_000, oldHandoffLingerMs: 30_000 });
  });

  test('a window already armed keeps the until it was stamped with', async () => {
    let now = 1_000_000;
    const dir = freshDir();
    const bus = new EventBus({ durableLogDir: dir, now: () => now });
    const events: GroupRebalanceEvent[] = [];
    const a = bus.subscribeToGroup('g', 'jobs.*', noop, { handoffLingerMs: 30_000 });
    const b = bus.subscribeToGroup('g', 'jobs.*', noop, { handoffLingerMs: 30_000 });
    bus.subscribeToGroup('g', 'jobs.*', noop, { onRebalance: (e) => events.push(e) });
    for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', {});
    await flush();
    bus.commitOffset('g', 'jobs.email', 3);

    // A leaves first: its window is armed at the original 30s value.
    a.unsubscribe();
    const leaveA = events.find((e) => e.trigger === 'leave' && e.memberId === a.id);
    assert.ok(leaveA !== undefined);
    assert.equal(leaveA.lingerUntil, now + 30_000);

    // Revising B must not rewrite A's armed window.
    bus.setGroupHandoffLinger(b.id, 1_000);
    b.unsubscribe();
    const leaveB = events.find((e) => e.trigger === 'leave' && e.memberId === b.id);
    assert.ok(leaveB !== undefined);
    assert.equal(leaveB.lingerUntil, now + 1_000);

    // Past B's revised window but inside A's original one, the
    // in-flight seqs are still covered — A's window was not shortened.
    now += 2_000;
    const replayed: number[] = [];
    bus.subscribeToGroup('g', 'jobs.*', (m) => replayed.push(m.seq), { resumeFromSeq: 3 });
    await flush();
    assert.deepEqual(replayed, [], "A's armed window still suppresses the replay");

    // Past A's window the backlog is replayable again (at-least-once).
    now += 29_000;
    const replayedLate: number[] = [];
    bus.subscribeToGroup('g', 'jobs.*', (m) => replayedLate.push(m.seq), { resumeFromSeq: 3 });
    await flush();
    assert.deepEqual(replayedLate, [4, 5]);
  });

  test('0 closes future windows and a never-opted-in member can opt in', async () => {
    let now = 1_000_000;
    const dir = freshDir();
    const bus = new EventBus({ durableLogDir: dir, now: () => now });
    const events: GroupRebalanceEvent[] = [];
    const closer = bus.subscribeToGroup('g', 't', noop, {
      handoffLingerMs: 30_000,
      onRebalance: (e) => events.push(e),
    });
    bus.subscribeToGroup('g', 't', noop, { onRebalance: (e) => events.push(e) });
    bus.publish('t', {});
    await flush();
    bus.setGroupHandoffLinger(closer.id, 0);
    closer.unsubscribe();
    const leaveClosed = events.find((e) => e.trigger === 'leave' && e.memberId === closer.id);
    assert.ok(leaveClosed !== undefined);
    assert.equal(leaveClosed.lingerUntil, undefined);
    assert.equal(leaveClosed.lingering, undefined);

    // Opt-in: a member that never set a linger can gain one by revision.
    const joiner = bus.subscribeToGroup('h', 't', noop, { handoffLingerMs: 0 });
    bus.subscribeToGroup('h', 't', noop, { onRebalance: (e) => events.push(e) });
    bus.publish('t', {});
    await flush();
    bus.setGroupHandoffLinger(joiner.id, 30_000);
    joiner.unsubscribe();
    const leaveOptedIn = events.find((e) => e.trigger === 'leave' && e.memberId === joiner.id);
    assert.ok(leaveOptedIn !== undefined);
    assert.equal(leaveOptedIn.lingerUntil, now + 30_000);

    const audit = bus.getSubscriptionAudit({ action: 'group_handoff_linger_revised' });
    assert.equal(audit.length, 2);
    assert.deepEqual(audit[0].detail, { handoffLingerMs: 0, oldHandoffLingerMs: 30_000 });
    assert.deepEqual(audit[1].detail, { handoffLingerMs: 30_000, oldHandoffLingerMs: 0 });
  });

  test('an identical revision is a no-op with no audit', () => {
    const bus = new EventBus();
    const member = bus.subscribeToGroup('g', 't', noop, { handoffLingerMs: 30_000 });
    bus.setGroupHandoffLinger(member.id, 30_000);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'group_handoff_linger_revised' }).length,
      0,
      're-affirming the current window audits nothing',
    );

    // Re-affirming the 0 default on a never-opted-in member is a no-op too.
    const plain = bus.subscribeToGroup('h', 't', noop);
    bus.setGroupHandoffLinger(plain.id, 0);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'group_handoff_linger_revised' }).length,
      0,
      're-affirming the unset window audits nothing',
    );
    member.unsubscribe();
    plain.unsubscribe();
  });

  test('is fail-closed: unknown subscribers, non-group members, and illegal values', async () => {
    let now = 1_000_000;
    const dir = freshDir();
    const bus = new EventBus({ durableLogDir: dir, now: () => now });
    const events: GroupRebalanceEvent[] = [];
    const member = bus.subscribeToGroup('g', 'jobs.*', noop, { handoffLingerMs: 30_000 });
    bus.subscribeToGroup('g', 'jobs.*', noop, { onRebalance: (e) => events.push(e) });
    const plain = bus.subscribe('t', noop);

    assert.throws(() => bus.setGroupHandoffLinger('sub-999', 100), RangeError);
    assert.throws(() => bus.setGroupHandoffLinger('', 100), RangeError);
    assert.throws(
      () => bus.setGroupHandoffLinger(plain.id, 100),
      RangeError,
      'a plain subscriber has no groupId to revise',
    );
    for (const bad of [Number.NaN, -1, Number.POSITIVE_INFINITY, -0.5, 'x' as unknown as number]) {
      assert.throws(
        () => bus.setGroupHandoffLinger(member.id, bad),
        RangeError,
        `expected RangeError for ${String(bad)}`,
      );
    }
    assert.equal(
      bus.getSubscriptionAudit({ action: 'group_handoff_linger_revised' }).length,
      0,
      'failed revisions audit nothing',
    );

    // The live window is untouched: the member still leaves with the
    // original 30s linger.
    for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', {});
    await flush();
    bus.commitOffset('g', 'jobs.email', 3);
    member.unsubscribe();
    const leave = events.find((e) => e.trigger === 'leave');
    assert.ok(leave !== undefined);
    assert.equal(leave.lingerUntil, now + 30_000);
    assert.deepEqual(leave.lingering, [{ topic: 'jobs.email', fromSeq: 3, toSeq: 5 }]);
    plain.unsubscribe();
  });
});
