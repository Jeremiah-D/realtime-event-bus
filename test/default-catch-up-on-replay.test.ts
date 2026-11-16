import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';

/**
 * Bus-level default time-travel replay catch-up mode (EB-518):
 * `setDefaultCatchUpOnReplay` installs the switch a new subscriber
 * without an explicit `catchUpOnReplay` falls back to — the precedence
 * chain is subscriber-explicit > bus default > `false` — and the
 * default is baked into the subscriber record at subscribe time, so a
 * later revision never moves an already-subscribed subscriber.
 *
 * Order assertions follow `catch-up-on-replay-revision.test.ts`:
 * `replayRange` at speed 3 over two records 30ms apart keeps the
 * second record ~10ms out, so a live publish 4ms into the replay
 * either interleaves (switch off) or waits for the replay to end
 * (switch on).
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** The `default_catch_up_on_replay_revised` audit events on the bus. */
function defaultCatchUpAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'default_catch_up_on_replay_revised' });
}

/**
 * Two history records 30ms apart, so a replay at speed 3 keeps the
 * second record ~10ms out — long enough to publish live traffic
 * mid-replay, as in `catch-up-on-replay-revision.test.ts`.
 */
async function twoRecordHistory(bus: EventBus): Promise<void> {
  bus.publish('t', 'h1');
  await new Promise((r) => setTimeout(r, 30));
  bus.publish('t', 'h2');
}

test('EB-518: the factory default is unset (an omitted switch behaves as false)', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultCatchUpOnReplay(), undefined);
  await twoRecordHistory(bus);
  const order: string[] = [];
  const sub = bus.subscribe('t', (msg: BusMessage) =>
    order.push(msg.replayed === true ? `R${msg.seq}` : `L${msg.seq}`),
  );
  await flush();
  const replay = bus.replayRange('t', { from: 1, to: 2, speed: 3 });
  await new Promise((r) => setTimeout(r, 4));
  bus.publish('t', 'live');
  await replay;
  await flush();
  await flush();
  // The live message drains mid-replay, before the second record.
  assert.deepEqual(order, ['R1', 'L3', 'R2']);
  assert.equal(defaultCatchUpAudits(bus).length, 0);
  void sub;
});

test('EB-518: set true then a new subscriber omitting the switch pauses live drain on the next replay', async () => {
  const bus = new EventBus();
  bus.setDefaultCatchUpOnReplay(true);
  assert.equal(bus.getDefaultCatchUpOnReplay(), true);
  await twoRecordHistory(bus);
  const order: string[] = [];
  const sub = bus.subscribe('t', (msg: BusMessage) =>
    order.push(msg.replayed === true ? `R${msg.seq}` : `L${msg.seq}`),
  );
  await flush();
  const replay = bus.replayRange('t', { from: 1, to: 2, speed: 3 });
  await new Promise((r) => setTimeout(r, 4));
  bus.publish('t', 'live');
  await replay;
  await flush();
  await flush();
  // History first, the live message only after the replay finished.
  assert.deepEqual(order, ['R1', 'R2', 'L3']);
  void sub;
});

test('EB-518: an explicit false beats a true default', async () => {
  const bus = new EventBus();
  bus.setDefaultCatchUpOnReplay(true);
  await twoRecordHistory(bus);
  const order: string[] = [];
  const sub = bus.subscribe(
    't',
    (msg: BusMessage) => order.push(msg.replayed === true ? `R${msg.seq}` : `L${msg.seq}`),
    { catchUpOnReplay: false },
  );
  await flush();
  const replay = bus.replayRange('t', { from: 1, to: 2, speed: 3 });
  await new Promise((r) => setTimeout(r, 4));
  bus.publish('t', 'live');
  await replay;
  await flush();
  await flush();
  assert.deepEqual(order, ['R1', 'L3', 'R2']);
  void sub;
});

test('EB-518: an explicit true beats a false default', async () => {
  const bus = new EventBus();
  bus.setDefaultCatchUpOnReplay(false);
  await twoRecordHistory(bus);
  const order: string[] = [];
  const sub = bus.subscribe(
    't',
    (msg: BusMessage) => order.push(msg.replayed === true ? `R${msg.seq}` : `L${msg.seq}`),
    { catchUpOnReplay: true },
  );
  await flush();
  const replay = bus.replayRange('t', { from: 1, to: 2, speed: 3 });
  await new Promise((r) => setTimeout(r, 4));
  bus.publish('t', 'live');
  await replay;
  await flush();
  await flush();
  assert.deepEqual(order, ['R1', 'R2', 'L3']);
  void sub;
});

test('EB-518: set(null) clears the default back to false', async () => {
  const bus = new EventBus();
  bus.setDefaultCatchUpOnReplay(true);
  bus.setDefaultCatchUpOnReplay(null);
  assert.equal(bus.getDefaultCatchUpOnReplay(), undefined);
  await twoRecordHistory(bus);
  const order: string[] = [];
  const sub = bus.subscribe('t', (msg: BusMessage) =>
    order.push(msg.replayed === true ? `R${msg.seq}` : `L${msg.seq}`),
  );
  await flush();
  const replay = bus.replayRange('t', { from: 1, to: 2, speed: 3 });
  await new Promise((r) => setTimeout(r, 4));
  bus.publish('t', 'live');
  await replay;
  await flush();
  await flush();
  assert.deepEqual(order, ['R1', 'L3', 'R2']);
  void sub;
});

test('EB-518: illegal values and illegal actors throw RangeError and leave the live default untouched', () => {
  const bus = new EventBus();
  bus.setDefaultCatchUpOnReplay(true);
  for (const bad of ['yes', 1, 0, undefined, {}, []]) {
    assert.throws(
      () => bus.setDefaultCatchUpOnReplay(bad as never),
      RangeError,
      `enabled=${String(bad)}`,
    );
  }
  assert.throws(() => bus.setDefaultCatchUpOnReplay(false, ''), RangeError);
  assert.throws(() => bus.setDefaultCatchUpOnReplay(false, 42 as never), RangeError);
  assert.equal(bus.getDefaultCatchUpOnReplay(), true);
  assert.equal(
    defaultCatchUpAudits(bus).length,
    1,
    'the failed calls audited nothing',
  );
});

test('EB-518: a same-value revision is a no-op with no audit', () => {
  const bus = new EventBus();
  // Setting `false` on a factory bus and clearing an already-unset
  // default both audit nothing.
  bus.setDefaultCatchUpOnReplay(false);
  bus.setDefaultCatchUpOnReplay(null);
  assert.equal(defaultCatchUpAudits(bus).length, 0);
  assert.equal(bus.getDefaultCatchUpOnReplay(), undefined);
  bus.setDefaultCatchUpOnReplay(true);
  bus.setDefaultCatchUpOnReplay(true);
  const audit = defaultCatchUpAudits(bus);
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, { previous: null, catchUpOnReplay: true });
});

test('EB-518: a revision never moves an already-subscribed subscriber switch', async () => {
  const bus = new EventBus();
  await twoRecordHistory(bus);
  const order: string[] = [];
  const sub = bus.subscribe('t', (msg: BusMessage) =>
    order.push(msg.replayed === true ? `R${msg.seq}` : `L${msg.seq}`),
  );
  await flush();
  // The subscriber baked `false` at subscribe time; installing a
  // `true` default afterwards must not change its switch.
  bus.setDefaultCatchUpOnReplay(true);
  const replay = bus.replayRange('t', { from: 1, to: 2, speed: 3 });
  await new Promise((r) => setTimeout(r, 4));
  bus.publish('t', 'live');
  await replay;
  await flush();
  await flush();
  assert.deepEqual(order, ['R1', 'L3', 'R2']);
  void sub;
});

test('EB-518: per-subscriber setCatchUpOnReplay still revises one subscriber and a later default revision does not move it', async () => {
  const bus = new EventBus();
  bus.setDefaultCatchUpOnReplay(true);
  await twoRecordHistory(bus);
  const order: string[] = [];
  const sub = bus.subscribe('t', (msg: BusMessage) =>
    order.push(msg.replayed === true ? `R${msg.seq}` : `L${msg.seq}`),
  );
  await flush();
  // Per-subscriber revision to `false`: the next replay interleaves.
  bus.setCatchUpOnReplay(sub.id, false);
  // A later default revision must not move the switch
  // `setCatchUpOnReplay` installed.
  bus.setDefaultCatchUpOnReplay(false);
  const replay = bus.replayRange('t', { from: 1, to: 2, speed: 3 });
  await new Promise((r) => setTimeout(r, 4));
  bus.publish('t', 'live');
  await replay;
  await flush();
  await flush();
  assert.deepEqual(order, ['R1', 'L3', 'R2']);
  void sub;
});

test('EB-518: revising the default mid-replay does not touch the replay already running', async () => {
  const bus = new EventBus();
  bus.setDefaultCatchUpOnReplay(true);
  await twoRecordHistory(bus);
  const order: string[] = [];
  const sub = bus.subscribe('t', (msg: BusMessage) =>
    order.push(msg.replayed === true ? `R${msg.seq}` : `L${msg.seq}`),
  );
  await flush();
  const replay = bus.replayRange('t', { from: 1, to: 2, speed: 3 });
  await new Promise((r) => setTimeout(r, 4));
  // The replay already snapshotted its paused set (this subscriber)
  // at its start, so clearing the default now governs only the next
  // replay — the live message still waits for this replay to end.
  bus.setDefaultCatchUpOnReplay(null);
  bus.publish('t', 'live');
  await replay;
  await flush();
  await flush();
  assert.deepEqual(order, ['R1', 'R2', 'L3']);
  void sub;
});

test('EB-518: audit records previous/catchUpOnReplay exactly and the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setDefaultCatchUpOnReplay(true, 'ops');
  bus.setDefaultCatchUpOnReplay(false);
  bus.setDefaultCatchUpOnReplay(true);
  bus.setDefaultCatchUpOnReplay(null);
  const audit = defaultCatchUpAudits(bus);
  assert.equal(audit.length, 4);
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[0].detail, { previous: null, catchUpOnReplay: true });
  assert.deepEqual(audit[1].detail, { previous: true, catchUpOnReplay: false });
  assert.deepEqual(audit[2].detail, { previous: false, catchUpOnReplay: true });
  assert.deepEqual(audit[3].detail, { previous: true, catchUpOnReplay: null });
  assert.equal(bus.getDefaultCatchUpOnReplay(), undefined);
  // Setting `false` on the cleared bus is a no-op too: unset already
  // means the effective `false`, so it audits nothing.
  bus.setDefaultCatchUpOnReplay(false);
  assert.equal(defaultCatchUpAudits(bus).length, 4);
  // Clearing a bus already at the effective `false` is a no-op: the
  // fourth entry above already cleared it, so a second clear audits
  // nothing.
  bus.setDefaultCatchUpOnReplay(null);
  assert.equal(defaultCatchUpAudits(bus).length, 4);
});

test('EB-518: an explicit null at subscribe time still throws RangeError', () => {
  const bus = new EventBus();
  bus.setDefaultCatchUpOnReplay(true);
  assert.throws(
    () => bus.subscribe('t', () => {}, { catchUpOnReplay: null as never }),
    RangeError,
  );
  // The failed subscribe registered nothing and the live default is
  // untouched.
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getDefaultCatchUpOnReplay(), true);
  assert.equal(defaultCatchUpAudits(bus).length, 1);
});
