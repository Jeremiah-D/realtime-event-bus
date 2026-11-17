/**
 * Bus-level default pre-DLQ immediate redelivery budget live revision
 * (EB-526).
 *
 * `setDefaultMaxRedeliveries` decides how many times a message may be
 * requeued (via `nack()` or ack timeout) before it moves to the DLQ for
 * new reliable subscribers that opt into a dead-letter queue and omit
 * `maxRedeliveries` — `2` to tighten every future opted-in reliable
 * subscriber to two immediate redeliveries with one call, `0` to
 * dead-letter every future opted-in subscriber on its first failure,
 * `null` to clear the default back to 5. The priority chain is explicit
 * `maxRedeliveries` > bus default > 5; an explicit `0` still
 * dead-letters on the first failure even while a default is installed.
 * The default never enables a dead-letter queue — a subscriber that
 * does not pass `deadLetter` still has none. A revision only affects
 * reliable subscribers created after it, and a revision identical to
 * the current default (compared on the effective budget, including
 * setting `5` on a factory bus) is a no-op with no audit. Audited as
 * `default_max_redeliveries_revised` (`{ previous, maxRedeliveries }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const defaultAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'default_max_redeliveries_revised' });

/** A reliable subscriber whose handler always nacks, tracking redelivery counts. */
function failingSub(
  bus: EventBus,
  topic: string,
  deadLetter: { maxRedeliveries?: number | null } | true,
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

test('EB-526: a bus default budget of 2 dead-letters on the third failure', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultMaxRedeliveries(), undefined);
  bus.setDefaultMaxRedeliveries(2);
  assert.equal(bus.getDefaultMaxRedeliveries(), 2);
  const { seen, sub } = failingSub(bus, 't', true);
  bus.publish('t', 'flaky');
  await flush();
  await sleep(200);
  assert.deepEqual(seen, [0, 1, 2], 'the default budget allows exactly two redeliveries');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1, 'the third failure dead-letters');
  assert.equal(defaultAudits(bus).length, 1);
  assert.deepEqual(defaultAudits(bus)[0].detail, { previous: null, maxRedeliveries: 2 });
});

test('EB-526: an explicit maxRedeliveries always beats the bus default', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxRedeliveries(0);
  const { seen, sub } = failingSub(bus, 't', { maxRedeliveries: 2 });
  bus.publish('t', 'flaky');
  await flush();
  await sleep(200);
  assert.deepEqual(seen, [0, 1, 2], 'the explicit budget of 2 ran, not the default 0');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  // An explicit `null` keeps the subscribe-time gate's own behavior
  // (budget 5) — it is not swallowed into the bus default.
  const bus2 = new EventBus();
  bus2.setDefaultMaxRedeliveries(2);
  const nulled = failingSub(bus2, 't', { maxRedeliveries: null });
  bus2.publish('t', 'flaky');
  await flush();
  await sleep(200);
  assert.deepEqual(nulled.seen, [0, 1, 2, 3, 4, 5], 'an explicit null still means the old gate budget of 5');
  assert.equal(bus2.getDeadLetterMessages(nulled.sub.id).length, 1);
});

test('EB-526: an explicit 0 still dead-letters on the first failure while a default is installed', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxRedeliveries(2);
  const { seen, sub } = failingSub(bus, 't', { maxRedeliveries: 0 });
  bus.publish('t', 'doomed');
  await flush();
  await sleep(200);
  assert.deepEqual(seen, [0], 'an explicit 0 is an explicit close, not a fallback');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
});

test('EB-526: the default never enables a dead-letter queue', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxRedeliveries(2);
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
  await sleep(200);
  assert.deepEqual(seen, [0, 1, 2], 'immediate redeliveries only — no DLQ was created');
  assert.deepEqual(bus.getDeadLetterMessages(sub.id), []);
});

test('EB-526: clearing the default returns new subscribers to a budget of 5', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxRedeliveries(2);
  bus.setDefaultMaxRedeliveries(null);
  assert.equal(bus.getDefaultMaxRedeliveries(), undefined);
  const { seen, sub } = failingSub(bus, 't', true);
  bus.publish('t', 'flaky');
  await flush();
  await sleep(200);
  assert.deepEqual(seen, [0, 1, 2, 3, 4, 5], 'the cleared default is back to the factory budget of 5');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  assert.equal(defaultAudits(bus).length, 2);
  assert.deepEqual(defaultAudits(bus)[1].detail, { previous: 2, maxRedeliveries: null });
});

test('EB-526: illegal values throw RangeError and leave the live default and audit untouched', () => {
  const bus = new EventBus();
  bus.setDefaultMaxRedeliveries(2);
  assert.throws(() => bus.setDefaultMaxRedeliveries(undefined as never), RangeError, 'undefined is not a clear');
  assert.throws(() => bus.setDefaultMaxRedeliveries('2' as never), RangeError);
  assert.throws(() => bus.setDefaultMaxRedeliveries({} as never), RangeError);
  assert.throws(() => bus.setDefaultMaxRedeliveries(2, ''), RangeError, 'an illegal actor throws');
  for (const bad of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
    assert.throws(() => bus.setDefaultMaxRedeliveries(bad as never), RangeError);
  }
  assert.equal(bus.getDefaultMaxRedeliveries(), 2, 'no failed revision moved the live default');
  assert.equal(defaultAudits(bus).length, 1, 'no failed revision is audited');
});

test('EB-526: re-affirming the current default is a silent no-op', () => {
  const bus = new EventBus();
  bus.setDefaultMaxRedeliveries(null);
  assert.equal(defaultAudits(bus).length, 0, 'clearing an already-unset default audits nothing');
  bus.setDefaultMaxRedeliveries(5);
  assert.equal(defaultAudits(bus).length, 0, 'setting 5 on a factory bus audits nothing');
  assert.equal(bus.getDefaultMaxRedeliveries(), undefined, 'setting 5 on a factory bus installs no default');
  bus.setDefaultMaxRedeliveries(2);
  bus.setDefaultMaxRedeliveries(2);
  assert.equal(defaultAudits(bus).length, 1, 're-setting the same budget audits nothing');
  bus.setDefaultMaxRedeliveries(5);
  assert.equal(defaultAudits(bus).length, 2, 'moving 2 to 5 audits');
  assert.equal(bus.getDefaultMaxRedeliveries(), 5);
  bus.setDefaultMaxRedeliveries(null);
  assert.equal(defaultAudits(bus).length, 3, 'clearing an installed default audits, even when it was 5');
  assert.equal(bus.getDefaultMaxRedeliveries(), undefined);
  bus.setDefaultMaxRedeliveries(null);
  assert.equal(defaultAudits(bus).length, 3, 'clearing an already-cleared default audits nothing');
});

test('EB-526: a revision never moves an already-subscribed subscriber', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxRedeliveries(2);
  // Subscribed under the default: baked with budget 2.
  const early = failingSub(bus, 'early', true);
  bus.publish('early', 'flaky');
  await flush();
  await sleep(200);
  assert.deepEqual(early.seen, [0, 1, 2], 'the first message spent its baked budget of 2');
  assert.equal(bus.getDeadLetterMessages(early.sub.id).length, 1);
  // Tighten the default to 0: the existing subscriber keeps budget 2
  // and its consumed counts — the next message still gets two
  // redeliveries, not zero.
  bus.setDefaultMaxRedeliveries(0);
  bus.publish('early', 'flaky');
  await flush();
  await sleep(200);
  assert.deepEqual(early.seen, [0, 1, 2, 0, 1, 2], 'the pre-revision subscriber keeps its baked budget');
  assert.equal(bus.getDeadLetterMessages(early.sub.id).length, 2);
  // Subscribed after the revision: takes the new default.
  const late = failingSub(bus, 'late', true);
  bus.publish('late', 'doomed');
  await flush();
  await sleep(200);
  assert.deepEqual(late.seen, [0], 'the post-revision subscriber dead-letters on the first failure');
  assert.equal(bus.getDeadLetterMessages(late.sub.id).length, 1);
});

test('EB-526: a default-baked subscriber is still revisable per subscriber, and a later default revision does not re-apply', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxRedeliveries(2);
  const { seen, sub } = failingSub(bus, 't', true);
  bus.setDeadLetterBudget(sub.id, { maxRedeliveries: 0 });
  bus.publish('t', 'doomed');
  await flush();
  await sleep(200);
  assert.deepEqual(seen, [0], 'the per-subscriber revision to 0 governs');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  // Move the bus default afterwards: the already-revised subscriber
  // must not pick the new default up.
  bus.setDefaultMaxRedeliveries(5);
  bus.publish('t', 'doomed');
  await flush();
  await sleep(200);
  assert.deepEqual(seen, [0, 0], 'the revised subscriber keeps budget 0 under the new default');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 2);
});

test('EB-526: the audit records previous and maxRedeliveries exactly and the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setDefaultMaxRedeliveries(2, 'ops');
  assert.equal(bus.getDefaultMaxRedeliveries(), 2);
  assert.equal(defaultAudits(bus).length, 1);
  assert.deepEqual(defaultAudits(bus)[0].detail, { previous: null, maxRedeliveries: 2 });
  assert.equal(defaultAudits(bus)[0].actor, 'ops');
  bus.setDefaultMaxRedeliveries(0);
  assert.equal(bus.getDefaultMaxRedeliveries(), 0);
  assert.deepEqual(defaultAudits(bus)[1].detail, { previous: 2, maxRedeliveries: 0 });
  assert.equal(defaultAudits(bus)[1].actor, 'system');
  bus.setDefaultMaxRedeliveries(null);
  assert.equal(bus.getDefaultMaxRedeliveries(), undefined);
  assert.deepEqual(defaultAudits(bus)[2].detail, { previous: 0, maxRedeliveries: null });
});
