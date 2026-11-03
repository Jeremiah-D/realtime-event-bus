import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

test('setTopicFairness rejects an unknown subscriber and an empty subId', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('*', () => {}, { topicFairness: true });
  assert.throws(() => bus.setTopicFairness('no-such-sub', true), RangeError);
  assert.throws(() => bus.setTopicFairness('', true), RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'topic_fairness_revised' }).length,
    0,
    'a failed revision audits nothing',
  );
  sub.unsubscribe();
});

test('setTopicFairness rejects a subscriber that never opted into fairness', () => {
  const bus = new EventBus();
  const plain = bus.subscribe('*', () => {});
  assert.throws(
    () => bus.setTopicFairness(plain.id, { weights: { hot: 3 } }),
    /has no topicFairness set/,
  );
  assert.equal(
    bus.getSubscriptionAudit({ action: 'topic_fairness_revised' }).length,
    0,
    'a failed revision audits nothing',
  );
  plain.unsubscribe();
});

test('setTopicFairness rejects illegal weights and leaves the live weights untouched', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`), {
    topicFairness: { weights: { hot: 1, cold: 3 } },
  });
  for (const bad of [
    { weights: { hot: 0 } },
    { weights: { cold: 1.5 } },
    { weights: { '': 2 } },
    'yes',
    undefined,
    false,
  ]) {
    assert.throws(() => bus.setTopicFairness(sub.id, bad as never), RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'topic_fairness_revised' }).length,
    0,
    'failed revisions audit nothing',
  );
  for (let i = 0; i < 6; i++) bus.publish('hot', i);
  for (let i = 0; i < 6; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(
    received,
    [
      'hot:0', 'cold:0', 'cold:1', 'cold:2',
      'hot:1', 'cold:3', 'cold:4', 'cold:5',
      'hot:2', 'hot:3', 'hot:4', 'hot:5',
    ],
    'the live 1:3 weights still govern the drain after failed revisions',
  );
  sub.unsubscribe();
});

test('setTopicFairness changes the weight ratio from the next drain round', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`), {
    topicFairness: { weights: { hot: 1, cold: 3 } },
  });
  for (let i = 0; i < 6; i++) bus.publish('hot', i);
  for (let i = 0; i < 6; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(received, [
    'hot:0', 'cold:0', 'cold:1', 'cold:2',
    'hot:1', 'cold:3', 'cold:4', 'cold:5',
    'hot:2', 'hot:3', 'hot:4', 'hot:5',
  ]);

  bus.setTopicFairness(sub.id, { weights: { hot: 3, cold: 1 } });
  for (let i = 0; i < 6; i++) bus.publish('hot', `h${i}`);
  for (let i = 0; i < 6; i++) bus.publish('cold', `c${i}`);
  await flush();
  await flush();
  assert.deepEqual(
    received.slice(12),
    [
      'hot:h0', 'hot:h1', 'hot:h2', 'cold:c0',
      'hot:h3', 'hot:h4', 'hot:h5', 'cold:c1',
      'cold:c2', 'cold:c3', 'cold:c4', 'cold:c5',
    ],
    'the new 3:1 ratio governs the next drain round',
  );

  const events = bus.getSubscriptionAudit({ action: 'topic_fairness_revised' });
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.deepEqual(events[0].detail, {
    topicFairness: { weights: { hot: 3, cold: 1 } },
    oldTopicFairness: { weights: { hot: 1, cold: 3 } },
  });
  sub.unsubscribe();
});

test('setTopicFairness keeps the deficit carryover across a revision', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: string[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`), {
    topicFairness: { weights: { hot: 3, cold: 1 } },
    deliveryShaping: { messagesPerSec: 2, burst: 2 },
  });
  for (let i = 0; i < 4; i++) bus.publish('hot', i);
  for (let i = 0; i < 4; i++) bus.publish('cold', i);
  await flush();
  // Round 1: the 2-message budget drains two hot messages; hot keeps
  // a deficit of 1 into the next round. (The selector credits every
  // group it visits in the pass, so cold is credited 1 too even
  // though the budget was already spent before its turn.)
  assert.deepEqual(received, ['hot:0', 'hot:1']);
  assert.deepEqual(bus.getStats().topicFairness.deficits, { hot: 1, cold: 1 });

  bus.setTopicFairness(sub.id, { weights: { hot: 1, cold: 1 } });
  // The carried deficits survive the revision: the next round serves
  // two hot messages (1 carried + 1 new credit), not one hot and one
  // cold as a fresh 1:1 ratio would.
  clock.advance(1000);
  bus.publish('cold', 'pad0');
  await flush();
  await flush();
  assert.deepEqual(received, ['hot:0', 'hot:1', 'hot:2', 'hot:3']);
  assert.deepEqual(bus.getStats().topicFairness.deficits, { cold: 2 }, 'hot drained fully, so its deficit clears; cold keeps its carried credit');
  assert.deepEqual(bus.getStats().topicFairness.scheduledByTopic, { hot: 4 });
  sub.unsubscribe();
});

test('setTopicFairness with the same effective weights is a no-op with no audit', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  const allOne = bus.subscribe('*', () => {}, { topicFairness: true });
  bus.setTopicFairness(allOne.id, true);
  // `true` and an explicit all-1 mapping are the same revision.
  bus.setTopicFairness(allOne.id, { weights: { hot: 1 } });
  bus.setTopicFairness(allOne.id, { weights: {} });
  const weighted = bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`), {
    topicFairness: { weights: { hot: 1, cold: 3 } },
  });
  bus.setTopicFairness(weighted.id, { weights: { hot: 1, cold: 3 } });
  bus.setTopicFairness(weighted.id, { weights: { cold: 3, hot: 1 } });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'topic_fairness_revised' }).length,
    0,
    're-stating the current weights audits nothing',
  );
  for (let i = 0; i < 2; i++) bus.publish('hot', i);
  for (let i = 0; i < 6; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(received, [
    'hot:0', 'cold:0', 'cold:1', 'cold:2',
    'hot:1', 'cold:3', 'cold:4', 'cold:5',
  ]);
  allOne.unsubscribe();
  weighted.unsubscribe();
});

test('setTopicFairness null clears back to FIFO, and a later revision is refused', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`), {
    topicFairness: { weights: { hot: 1, cold: 3 } },
  });
  bus.setTopicFairness(sub.id, null);
  const events = bus.getSubscriptionAudit({ action: 'topic_fairness_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    topicFairness: null,
    oldTopicFairness: { weights: { hot: 1, cold: 3 } },
  });
  for (let i = 0; i < 3; i++) bus.publish('hot', i);
  for (let i = 0; i < 3; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(received, ['hot:0', 'hot:1', 'hot:2', 'cold:0', 'cold:1', 'cold:2']);
  assert.deepEqual(bus.getStats().topicFairness, { scheduledByTopic: {}, deficits: {} });
  assert.throws(
    () => bus.setTopicFairness(sub.id, { weights: { hot: 2 } }),
    /has no topicFairness set/,
    'a cleared subscriber refuses revision until it opts in again',
  );
  sub.unsubscribe();
});

test('setTopicFairness audits the exact new and old weight mappings', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('*', () => {}, {
    topicFairness: { weights: { hot: 1, cold: 3 } },
  });
  bus.setTopicFairness(sub.id, true);
  bus.setTopicFairness(sub.id, { weights: { hot: 2 } });
  bus.setTopicFairness(sub.id, null);
  const events = bus.getSubscriptionAudit({ action: 'topic_fairness_revised' });
  assert.equal(events.length, 3);
  assert.equal(events[0].subId, sub.id);
  assert.equal(events[0].pattern, '*');
  assert.deepEqual(events[0].detail, {
    topicFairness: { weights: {} },
    oldTopicFairness: { weights: { hot: 1, cold: 3 } },
  });
  assert.deepEqual(events[1].detail, {
    topicFairness: { weights: { hot: 2 } },
    oldTopicFairness: { weights: {} },
  });
  assert.deepEqual(events[2].detail, {
    topicFairness: null,
    oldTopicFairness: { weights: { hot: 2 } },
  });
  sub.unsubscribe();
});

test('setTopicFairness keeps getStats().topicFairness aggregating live counts', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('*', () => {}, {
    topicFairness: { weights: { hot: 1, cold: 3 } },
  });
  for (let i = 0; i < 2; i++) bus.publish('hot', i);
  for (let i = 0; i < 6; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(bus.getStats().topicFairness.scheduledByTopic, { hot: 2, cold: 6 });

  bus.setTopicFairness(sub.id, { weights: { hot: 3, cold: 1 } });
  for (let i = 0; i < 3; i++) bus.publish('hot', `h${i}`);
  for (let i = 0; i < 3; i++) bus.publish('cold', `c${i}`);
  await flush();
  await flush();
  assert.deepEqual(bus.getStats().topicFairness.scheduledByTopic, { hot: 5, cold: 9 });
  sub.unsubscribe();
});
