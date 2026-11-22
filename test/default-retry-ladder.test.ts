/**
 * Bus-level default pre-DLQ retry ladder live revision (EB-525).
 *
 * `setDefaultRetryLadder` decides the rung delays new reliable
 * subscribers fall back to when they opt into a dead-letter queue and
 * omit `retryLadder` — `[100, 500]` to give every future opted-in
 * reliable subscriber two delayed retries before the DLQ with one
 * call, `null` to clear the default back to immediate dead-lettering
 * on budget exhaustion. The priority chain is explicit `retryLadder`
 * > bus default > disabled; an explicit empty array still disables
 * the ladder even while a default is installed. The default never
 * enables a dead-letter queue — a subscriber that does not pass
 * `deadLetter` still has none. A revision only affects reliable
 * subscribers created after it, and a revision identical to the
 * current default is a no-op with no audit. Audited as
 * `default_retry_ladder_revised` (`{ previous, retryLadder }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const defaultAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'default_retry_ladder_revised' });

/** A reliable subscriber whose handler always nacks, tracking redelivery counts. */
function failingSub(
  bus: EventBus,
  topic: string,
  deadLetter: { maxRedeliveries: number; retryLadder?: number[] | null } | true,
) {
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    topic,
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter },
  );
  return { seen, sub };
}

test('EB-525: factory default is unset — omit retryLadder dead-letters immediately', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultRetryLadder(), undefined);
  const { seen, sub } = failingSub(bus, 't', { maxRedeliveries: 0 });
  bus.publish('t', 'doomed');
  await flush();
  await sleep(150);
  assert.deepEqual(seen, [0], 'no ladder: no delayed rung follows the first failure');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
});

test('EB-525: a bus default ladder walks new opted-in reliable subscribers', async () => {
  const bus = new EventBus();
  bus.setDefaultRetryLadder([100, 200]);
  assert.deepEqual(bus.getDefaultRetryLadder(), [100, 200]);
  const { seen, sub } = failingSub(bus, 't', { maxRedeliveries: 0 });
  bus.publish('t', 'flaky');
  await flush();
  assert.deepEqual(seen, [0]);
  await sleep(200);
  assert.deepEqual(seen, [0, 1], 'the default first rung fires at ~100 ms');
  await sleep(300);
  assert.deepEqual(seen, [0, 1, 2], 'the default second rung fires at ~200 ms after the first');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1, 'the spent default ladder dead-letters');
  assert.equal(defaultAudits(bus).length, 1);
  assert.deepEqual(defaultAudits(bus)[0].detail, { previous: null, retryLadder: [100, 200] });
});

test('EB-525: deadLetter true opts in with defaults and still takes the bus default ladder', async () => {
  const bus = new EventBus();
  bus.setDefaultRetryLadder([200, 300]);
  const { seen, sub } = failingSub(bus, 't', true);
  bus.publish('t', 'flaky');
  await sleep(100);
  assert.deepEqual(seen, [0, 1, 2, 3, 4, 5], 'the instant budget (5) spends immediately');
  await sleep(250);
  assert.deepEqual(seen, [0, 1, 2, 3, 4, 5, 6], 'the default first rung fires at ~200 ms');
  await sleep(400);
  assert.deepEqual(seen, [0, 1, 2, 3, 4, 5, 6, 7], 'the default second rung fires at ~300 ms after the first');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
});

test('EB-525: an explicit retryLadder always beats the bus default', async () => {
  const bus = new EventBus();
  bus.setDefaultRetryLadder([400, 400]);
  const { seen, sub } = failingSub(bus, 't', { maxRedeliveries: 0, retryLadder: [30] });
  bus.publish('t', 'flaky');
  await flush();
  await sleep(150);
  assert.deepEqual(seen, [0, 1], 'the explicit 30 ms rung fired, not the default 400 ms one');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1, 'the single explicit rung spent, then DLQ');
});

test('EB-525: an explicit empty array still disables the ladder while a default is installed', async () => {
  const bus = new EventBus();
  bus.setDefaultRetryLadder([30, 60]);
  const { seen, sub } = failingSub(bus, 't', { maxRedeliveries: 0, retryLadder: [] });
  bus.publish('t', 'doomed');
  await flush();
  await sleep(250);
  assert.deepEqual(seen, [0], 'an explicit empty array is an explicit close, not a fallback');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
});

test('EB-525: the default never enables a dead-letter queue', async () => {
  const bus = new EventBus();
  bus.setDefaultRetryLadder([30, 60]);
  const seen: number[] = [];
  let failures = 0;
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      failures += 1;
      if (failures <= 2) d.nack();
      else d.ack();
    },
    { ackTimeoutMs: 1000 },
  );
  bus.publish('t', 'hiccup');
  await flush();
  await sleep(250);
  assert.deepEqual(seen, [0, 1, 2], 'immediate redeliveries only — no delayed rung, no DLQ');
  assert.deepEqual(bus.getDeadLetterMessages(sub.id), []);
});

test('EB-525: clearing the default returns new subscribers to immediate dead-lettering', async () => {
  const bus = new EventBus();
  bus.setDefaultRetryLadder([30, 60]);
  bus.setDefaultRetryLadder(null);
  assert.equal(bus.getDefaultRetryLadder(), undefined);
  const { seen, sub } = failingSub(bus, 't', { maxRedeliveries: 0 });
  bus.publish('t', 'doomed');
  await flush();
  await sleep(150);
  assert.deepEqual(seen, [0]);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  assert.equal(defaultAudits(bus).length, 2);
  assert.deepEqual(defaultAudits(bus)[1].detail, { previous: [30, 60], retryLadder: null });
});

test('EB-525: illegal values throw RangeError and leave the live default and audit untouched', () => {
  const bus = new EventBus();
  bus.setDefaultRetryLadder([30, 60]);
  assert.throws(() => bus.setDefaultRetryLadder(undefined as never), RangeError, 'undefined is not a clear');
  assert.throws(() => bus.setDefaultRetryLadder(1000 as never), RangeError);
  assert.throws(() => bus.setDefaultRetryLadder('1000' as never), RangeError);
  assert.throws(() => bus.setDefaultRetryLadder([30, 60], ''), RangeError, 'an illegal actor throws');
  for (const bad of [[-1], [Number.NaN], [Number.POSITIVE_INFINITY], [1000, -5], ['1000']]) {
    assert.throws(() => bus.setDefaultRetryLadder(bad as never), RangeError);
  }
  assert.deepEqual(bus.getDefaultRetryLadder(), [30, 60], 'no failed revision moved the live default');
  assert.equal(defaultAudits(bus).length, 1, 'no failed revision is audited');
});

test('EB-525: subscribe-time retryLadder null still throws instead of falling back to the default', () => {
  const bus = new EventBus();
  bus.setDefaultRetryLadder([30, 60]);
  assert.throws(
    () =>
      bus.subscribeReliable(
        't',
        (d) => d.nack(),
        { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, retryLadder: null } },
      ),
    RangeError,
    'an explicit null is rejected, not swallowed into the default',
  );
});

test('EB-525: re-affirming the current default is a silent no-op', () => {
  const bus = new EventBus();
  bus.setDefaultRetryLadder(null);
  assert.equal(defaultAudits(bus).length, 0, 'clearing an already-unset default audits nothing');
  bus.setDefaultRetryLadder([]);
  assert.equal(defaultAudits(bus).length, 0, 'an empty array normalizes to unset on a factory bus');
  assert.equal(bus.getDefaultRetryLadder(), undefined);
  bus.setDefaultRetryLadder([30, 60]);
  bus.setDefaultRetryLadder([30, 60]);
  assert.equal(defaultAudits(bus).length, 1, 're-setting the same rungs audits nothing');
  bus.setDefaultRetryLadder(null);
  bus.setDefaultRetryLadder(null);
  assert.equal(defaultAudits(bus).length, 2, 'clearing an already-cleared default audits nothing');
});

test('EB-525: a revision never moves an already-subscribed subscriber', async () => {
  const bus = new EventBus();
  // Subscribed before any default: baked with no ladder.
  const early = failingSub(bus, 'early', { maxRedeliveries: 0 });
  // Subscribed before any default with an explicit ladder: baked with it.
  const explicit = failingSub(bus, 'explicit', { maxRedeliveries: 0, retryLadder: [30, 60] });
  bus.setDefaultRetryLadder([200, 200]);
  bus.publish('early', 'doomed');
  bus.publish('explicit', 'flaky');
  await flush();
  await sleep(250);
  assert.deepEqual(early.seen, [0], 'the pre-default subscriber keeps immediate dead-lettering');
  assert.equal(bus.getDeadLetterMessages(early.sub.id).length, 1);
  assert.deepEqual(explicit.seen, [0, 1, 2], 'the explicit ladder ran its own rungs, not the default 200 ms ones');
  assert.equal(bus.getDeadLetterMessages(explicit.sub.id).length, 1);
  // Subscribed after the default: takes it.
  const late = failingSub(bus, 'late', { maxRedeliveries: 0 });
  bus.publish('late', 'flaky');
  await flush();
  await sleep(700);
  assert.deepEqual(late.seen, [0, 1, 2], 'the post-default subscriber walks the default ladder');
  assert.equal(bus.getDeadLetterMessages(late.sub.id).length, 1);
});

test('EB-525: a default-baked subscriber is still revisable per subscriber, and a clear is not re-applied', async () => {
  const bus = new EventBus();
  bus.setDefaultRetryLadder([30, 60]);
  const { seen, sub } = failingSub(bus, 't', { maxRedeliveries: 0 });
  bus.publish('t', 'flaky');
  await flush();
  assert.deepEqual(seen, [0]);
  // The nack above armed rung 0 at 30 ms; revising must not move it.
  bus.setRetryLadder(sub.id, [400, 400]);
  await sleep(150);
  assert.deepEqual(seen, [0, 1], 'the armed rung fires on its original schedule');
  await sleep(150);
  assert.deepEqual(seen, [0, 1], 'the new 400 ms rung has not fired yet');
  await sleep(300);
  assert.deepEqual(seen, [0, 1, 2], 'the new ladder spaces the second rung');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1, 'the spent new ladder dead-letters');
  // Clear the per-subscriber ladder, then move the bus default: the
  // cleared subscriber must not pick the new default up. (EB-539: a
  // re-enable after the clear is now allowed, so no refusal is
  // asserted here — the point below is that the default is not
  // re-applied to a cleared subscriber.)
  bus.setRetryLadder(sub.id, null);
  bus.setDefaultRetryLadder([50, 50]);
  bus.publish('t', 'doomed');
  await flush();
  await sleep(200);
  assert.deepEqual(seen, [0, 1, 2, 0], 'the cleared subscriber dead-letters immediately, no rung');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 2);
});

test('EB-525: the getter follows the live value and returns a defensive copy', () => {
  const bus = new EventBus();
  const offered = [30, 60];
  bus.setDefaultRetryLadder(offered);
  offered.push(999);
  assert.deepEqual(bus.getDefaultRetryLadder(), [30, 60], 'mutating the offered array never moves the live default');
  const got = bus.getDefaultRetryLadder()!;
  got[0] = 999;
  got.push(999);
  assert.deepEqual(bus.getDefaultRetryLadder(), [30, 60], 'mutating the returned array never moves the live default');
  bus.setDefaultRetryLadder([100]);
  assert.deepEqual(bus.getDefaultRetryLadder(), [100]);
  assert.equal(defaultAudits(bus).length, 2);
  assert.deepEqual(defaultAudits(bus)[1].detail, { previous: [30, 60], retryLadder: [100] });
});
