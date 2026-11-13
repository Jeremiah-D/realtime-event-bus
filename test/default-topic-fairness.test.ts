import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/**
 * Bus-level default topic-fairness weights (EB-489):
 * `setDefaultTopicFairness` installs the per-topic weights new opt-in
 * fair-scheduled subscribers fall back to when they omit a topic —
 * the priority chain is subscriber-explicit > bus default > 1 — and
 * the default never enables fair scheduling: a subscriber that does
 * not opt into `topicFairness` is untouched by it.
 *
 * Drain-order assertions follow `topic-fairness-revision.test.ts`:
 * subscribe to `*`, publish a known backlog per topic, flush twice,
 * and read the interleaved delivery order off the handler.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('EB-489: the factory bus default is unset (topicFairness: true still means all weights 1)', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultTopicFairness(), undefined);
  const received: string[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`), {
    topicFairness: true,
  });
  for (let i = 0; i < 6; i++) bus.publish('hot', i);
  for (let i = 0; i < 6; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(
    received,
    [
      'hot:0', 'cold:0', 'hot:1', 'cold:1', 'hot:2', 'cold:2',
      'hot:3', 'cold:3', 'hot:4', 'cold:4', 'hot:5', 'cold:5',
    ],
    'no default installed: true drains 1:1',
  );
  assert.equal(bus.getSubscriptionAudit({ action: 'default_topic_fairness_revised' }).length, 0);
  sub.unsubscribe();
});

test('EB-489: topicFairness: true takes the whole bus default map', async () => {
  const bus = new EventBus();
  bus.setDefaultTopicFairness({ weights: { hot: 1, cold: 3 } });
  const received: string[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`), {
    topicFairness: true,
  });
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
    'true takes the installed 1:3 default map',
  );
  sub.unsubscribe();
});

test('EB-489: a partial weights object merges per topic — explicit wins, omitted topics take the default', async () => {
  const bus = new EventBus();
  bus.setDefaultTopicFairness({ weights: { hot: 1, cold: 3 } });
  const received: string[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`), {
    topicFairness: { weights: { hot: 2 } },
  });
  for (let i = 0; i < 6; i++) bus.publish('hot', i);
  for (let i = 0; i < 6; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(
    received,
    [
      'hot:0', 'hot:1', 'cold:0', 'cold:1', 'cold:2',
      'hot:2', 'hot:3', 'cold:3', 'cold:4', 'cold:5',
      'hot:4', 'hot:5',
    ],
    'explicit hot:2 wins, cold falls back to the default 3',
  );
  sub.unsubscribe();
});

test('EB-489: a topic absent from both the subscriber and the default drains at weight 1', async () => {
  const bus = new EventBus();
  bus.setDefaultTopicFairness({ weights: { hot: 1, cold: 3 } });
  const received: string[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`), {
    topicFairness: true,
  });
  for (let i = 0; i < 3; i++) bus.publish('hot', i);
  for (let i = 0; i < 6; i++) bus.publish('cold', i);
  for (let i = 0; i < 3; i++) bus.publish('warm', i);
  await flush();
  await flush();
  assert.deepEqual(
    received,
    [
      'hot:0', 'cold:0', 'cold:1', 'cold:2', 'warm:0',
      'hot:1', 'cold:3', 'cold:4', 'cold:5', 'warm:1',
      'hot:2', 'warm:2',
    ],
    'warm is in neither map, so it drains at weight 1',
  );
  sub.unsubscribe();
});

test('EB-489: clearing with null returns topicFairness: true to all weights 1', async () => {
  const bus = new EventBus();
  bus.setDefaultTopicFairness({ weights: { hot: 1, cold: 3 } });
  bus.setDefaultTopicFairness(null);
  assert.equal(bus.getDefaultTopicFairness(), undefined);
  const received: string[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`), {
    topicFairness: true,
  });
  for (let i = 0; i < 3; i++) bus.publish('hot', i);
  for (let i = 0; i < 3; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(
    received,
    ['hot:0', 'cold:0', 'hot:1', 'cold:1', 'hot:2', 'cold:2'],
    'after the clear, true is back to 1:1',
  );
  sub.unsubscribe();
});

test('EB-489: the default never enables fair scheduling for a subscriber that does not opt in', async () => {
  const bus = new EventBus();
  bus.setDefaultTopicFairness({ weights: { hot: 1, cold: 3 } });
  const received: string[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`));
  for (let i = 0; i < 3; i++) bus.publish('hot', i);
  for (let i = 0; i < 3; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(
    received,
    ['hot:0', 'hot:1', 'hot:2', 'cold:0', 'cold:1', 'cold:2'],
    'no opt-in: plain FIFO publish order, untouched by the default',
  );
  sub.unsubscribe();
});

test('EB-489: revising the bus default never moves an already-subscribed subscriber', async () => {
  const bus = new EventBus();
  bus.setDefaultTopicFairness({ weights: { hot: 1, cold: 3 } });
  const earlyReceived: string[] = [];
  const early = bus.subscribe(
    '*',
    (msg) => earlyReceived.push(`${msg.topic}:${String(msg.payload)}`),
    { topicFairness: true },
  );
  for (let i = 0; i < 6; i++) bus.publish('hot', i);
  for (let i = 0; i < 6; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(
    earlyReceived,
    [
      'hot:0', 'cold:0', 'cold:1', 'cold:2',
      'hot:1', 'cold:3', 'cold:4', 'cold:5',
      'hot:2', 'hot:3', 'hot:4', 'hot:5',
    ],
    'the early subscriber baked the 1:3 default at subscribe time',
  );
  bus.setDefaultTopicFairness({ weights: { hot: 3, cold: 1 } });
  for (let i = 10; i < 16; i++) bus.publish('hot', i);
  for (let i = 10; i < 16; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(
    earlyReceived.slice(12),
    [
      'hot:10', 'cold:10', 'cold:11', 'cold:12',
      'hot:11', 'cold:13', 'cold:14', 'cold:15',
      'hot:12', 'hot:13', 'hot:14', 'hot:15',
    ],
    'the early subscriber still drains 1:3 after the revision to 3:1',
  );
  early.unsubscribe();

  // A subscriber joining after the revision takes the new default.
  const lateReceived: string[] = [];
  const late = bus.subscribe(
    '*',
    (msg) => lateReceived.push(`${msg.topic}:${String(msg.payload)}`),
    { topicFairness: true },
  );
  for (let i = 20; i < 26; i++) bus.publish('hot', i);
  for (let i = 20; i < 26; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(
    lateReceived,
    [
      'hot:20', 'hot:21', 'hot:22', 'cold:20',
      'hot:23', 'hot:24', 'hot:25', 'cold:21',
      'cold:22', 'cold:23', 'cold:24', 'cold:25',
    ],
    'the late subscriber takes the revised 3:1 default',
  );
  late.unsubscribe();
});

test('EB-489: setTopicFairness still replaces the whole mapping and null clears without the default re-applying', async () => {
  const bus = new EventBus();
  bus.setDefaultTopicFairness({ weights: { hot: 1, cold: 3 } });
  const received: string[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`), {
    topicFairness: true,
  });
  // A per-subscriber revision replaces the whole mapping: the bus
  // default does not fill the omitted cold topic back in.
  bus.setTopicFairness(sub.id, { weights: { hot: 2 } });
  for (let i = 0; i < 4; i++) bus.publish('hot', i);
  for (let i = 0; i < 4; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(
    received,
    [
      'hot:0', 'hot:1', 'cold:0', 'hot:2', 'hot:3',
      'cold:1', 'cold:2', 'cold:3',
    ],
    'the revised subscriber drains 2:1, not 2:3',
  );
  bus.setTopicFairness(sub.id, null);
  for (let i = 10; i < 13; i++) bus.publish('hot', i);
  for (let i = 10; i < 13; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(
    received.slice(8),
    ['hot:10', 'hot:11', 'hot:12', 'cold:10', 'cold:11', 'cold:12'],
    'the cleared subscriber drains plain FIFO even though a default is installed',
  );
  sub.unsubscribe();
});

test('EB-489: revising to the current default is a no-op with no audit, and the getter follows the live value and returns a copy', () => {
  const bus = new EventBus();
  bus.setDefaultTopicFairness(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_topic_fairness_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultTopicFairness({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_topic_fairness_revised' }).length,
    0,
    '{} installs no topic coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultTopicFairness(), undefined);
  const opts = { weights: { hot: 1, cold: 3 } };
  bus.setDefaultTopicFairness(opts);
  opts.weights.cold = 999;
  assert.deepEqual(
    bus.getDefaultTopicFairness(),
    { weights: { hot: 1, cold: 3 } },
    'the setter stores a defensive copy',
  );
  bus.setDefaultTopicFairness({ weights: { hot: 1, cold: 3 } });
  bus.setDefaultTopicFairness({ weights: { hot: 1, cold: 3 } });
  bus.setDefaultTopicFairness({ weights: { hot: 1, cold: 3, warm: 1 } });
  const audit = bus.getSubscriptionAudit({ action: 'default_topic_fairness_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing — an explicit weight-1 entry is the same revision');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    topicFairness: { weights: { hot: 1, cold: 3 } },
  });
  const held = bus.getDefaultTopicFairness();
  assert.ok(held !== undefined);
  held.weights.cold = 999;
  assert.deepEqual(
    bus.getDefaultTopicFairness(),
    { weights: { hot: 1, cold: 3 } },
    'the getter returns a copy',
  );
  bus.setDefaultTopicFairness({ weights: { hot: 2, cold: 3 } }, 'ops');
  assert.deepEqual(bus.getDefaultTopicFairness(), { weights: { hot: 2, cold: 3 } });
  const revised = bus.getSubscriptionAudit({ action: 'default_topic_fairness_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { weights: { hot: 1, cold: 3 } },
    topicFairness: { weights: { hot: 2, cold: 3 } },
  });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultTopicFairness(null);
  assert.equal(bus.getDefaultTopicFairness(), undefined);
  const cleared = bus.getSubscriptionAudit({ action: 'default_topic_fairness_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, {
    previous: { weights: { hot: 2, cold: 3 } },
    topicFairness: null,
  });
  bus.setDefaultTopicFairness(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_topic_fairness_revised' }).length,
    3,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-489: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultTopicFairness({ weights: { hot: 1, cold: 3 } });
  const bad = [
    0,
    'yes',
    true,
    [],
    { weights: { hot: 0 } },
    { weights: { hot: -1 } },
    { weights: { hot: 1.5 } },
    { weights: { hot: Number.NaN } },
    { weights: { hot: '3' } },
    { weights: { '': 2 } },
    { weights: null },
    { weights: [] },
    { weights: 'hot' },
    undefined,
  ] as unknown[];
  for (const opts of bad) {
    assert.throws(
      () => bus.setDefaultTopicFairness(opts as never),
      RangeError,
      `opts=${String(opts)}`,
    );
    assert.deepEqual(bus.getDefaultTopicFairness(), { weights: { hot: 1, cold: 3 } });
  }
  assert.throws(() => bus.setDefaultTopicFairness({ weights: { hot: 2 } }, ''), RangeError);
  assert.deepEqual(bus.getDefaultTopicFairness(), { weights: { hot: 1, cold: 3 } });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_topic_fairness_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});
