import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';

/**
 * Live revision of the time-travel replay catch-up switch (EB-457):
 * `setCatchUpOnReplay(subId, enabled)` flips whether the next
 * `replayRange` pauses a subscriber's live drain, without
 * re-subscribing. The switch always has a value (default `false`),
 * so there is no opt-in gate — any known subscription can be
 * revised. `replayRange` reads the live value once, at the start of
 * the next replay, and snapshots the paused set there: a revision
 * landing mid-replay never touches the replay already running.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** The `catch_up_on_replay_revised` audit events on the bus. */
function catchUpRevisionAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'catch_up_on_replay_revised' });
}

/**
 * Two history records 30ms apart, so a replay at speed 3 keeps the
 * second record ~10ms out — long enough to publish live traffic
 * mid-replay, as in `time-travel-replay.test.ts`.
 */
async function twoRecordHistory(bus: EventBus): Promise<void> {
  bus.publish('t', 'h1');
  await new Promise((r) => setTimeout(r, 30));
  bus.publish('t', 'h2');
}

describe('live revision of the replay catch-up switch (EB-457)', () => {
  test('opening the switch pauses live drain on the next replay and re-drains after it ends', async () => {
    const bus = new EventBus();
    await twoRecordHistory(bus);
    const order: string[] = [];
    const sub = bus.subscribe('t', (msg: BusMessage) =>
      order.push(msg.replayed === true ? `R${msg.seq}` : `L${msg.seq}`),
    );
    await flush();
    bus.setCatchUpOnReplay(sub.id, true);
    const replay = bus.replayRange('t', { from: 1, to: 2, speed: 3 });
    await new Promise((r) => setTimeout(r, 4));
    bus.publish('t', 'live');
    await replay;
    await flush();
    await flush();
    // History first, the live message only after the replay finished.
    assert.deepEqual(order, ['R1', 'R2', 'L3']);
    assert.deepEqual(catchUpRevisionAudits(bus)[0].detail, {
      catchUpOnReplay: true,
      oldCatchUpOnReplay: false,
    });
  });

  test('closing the switch lets the next replay interleave live delivery', async () => {
    const bus = new EventBus();
    await twoRecordHistory(bus);
    const order: string[] = [];
    const sub = bus.subscribe(
      't',
      (msg: BusMessage) => order.push(msg.replayed === true ? `R${msg.seq}` : `L${msg.seq}`),
      { catchUpOnReplay: true },
    );
    await flush();
    bus.setCatchUpOnReplay(sub.id, false);
    const replay = bus.replayRange('t', { from: 1, to: 2, speed: 3 });
    await new Promise((r) => setTimeout(r, 4));
    bus.publish('t', 'live');
    await replay;
    await flush();
    await flush();
    // The live message drains mid-replay, before the second record.
    assert.deepEqual(order, ['R1', 'L3', 'R2']);
  });

  test('unknown subscription throws RangeError', () => {
    const bus = new EventBus();
    bus.subscribe('t', () => {});
    assert.throws(() => bus.setCatchUpOnReplay('no-such-sub', true), RangeError);
    assert.throws(() => bus.setCatchUpOnReplay('', true), RangeError);
    assert.equal(catchUpRevisionAudits(bus).length, 0);
  });

  test('non-boolean value throws RangeError and the live switch is unchanged', () => {
    const bus = new EventBus();
    const sub = bus.subscribe('t', () => {});
    for (const bad of ['yes', 1, 0, null, undefined]) {
      assert.throws(
        () => bus.setCatchUpOnReplay(sub.id, bad as unknown as boolean),
        RangeError,
      );
    }
    // No audit was written by any refused call, and the live switch
    // still reads as the default `false`: the first successful
    // revision records `false` as the old value.
    assert.equal(catchUpRevisionAudits(bus).length, 0);
    bus.setCatchUpOnReplay(sub.id, true);
    assert.deepEqual(catchUpRevisionAudits(bus)[0].detail, {
      catchUpOnReplay: true,
      oldCatchUpOnReplay: false,
    });
  });

  test('a revision identical to the live switch is a silent no-op with no audit', () => {
    const bus = new EventBus();
    const sub = bus.subscribe('t', () => {}, { catchUpOnReplay: true });
    bus.setCatchUpOnReplay(sub.id, true);
    assert.equal(catchUpRevisionAudits(bus).length, 0);
    const plain = bus.subscribe('t', () => {});
    bus.setCatchUpOnReplay(plain.id, false);
    assert.equal(catchUpRevisionAudits(bus).length, 0);
  });

  test('opening the switch mid-replay does not pause the in-progress replay', async () => {
    const bus = new EventBus();
    await twoRecordHistory(bus);
    const order: string[] = [];
    const sub = bus.subscribe('t', (msg: BusMessage) =>
      order.push(msg.replayed === true ? `R${msg.seq}` : `L${msg.seq}`),
    );
    await flush();
    const replay = bus.replayRange('t', { from: 1, to: 2, speed: 3 });
    await new Promise((r) => setTimeout(r, 4));
    // The replay already snapshotted its paused set (empty) at its
    // start, so this revision governs only the next replay.
    bus.setCatchUpOnReplay(sub.id, true);
    bus.publish('t', 'live');
    await replay;
    await flush();
    await flush();
    assert.deepEqual(order, ['R1', 'L3', 'R2']);
  });

  test('closing the switch mid-replay does not early-release an already-paused subscriber', async () => {
    const bus = new EventBus();
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
    // The subscriber was paused at the replay start; closing the
    // switch now must not release it — the re-drain still waits for
    // the replay's own end path.
    bus.setCatchUpOnReplay(sub.id, false);
    bus.publish('t', 'live');
    await replay;
    await flush();
    await flush();
    assert.deepEqual(order, ['R1', 'R2', 'L3']);
  });

  test('live backlog published during a paused replay drains losslessly after it ends', async () => {
    const bus = new EventBus();
    await twoRecordHistory(bus);
    const order: string[] = [];
    const sub = bus.subscribe('t', (msg: BusMessage) =>
      order.push(msg.replayed === true ? `R${msg.seq}` : `L${msg.seq}`),
    );
    await flush();
    bus.setCatchUpOnReplay(sub.id, true);
    const replay = bus.replayRange('t', { from: 1, to: 2, speed: 3 });
    await new Promise((r) => setTimeout(r, 4));
    bus.publish('t', 'live-1');
    bus.publish('t', 'live-2');
    bus.publish('t', 'live-3');
    await replay;
    await flush();
    await flush();
    // Every live message arrives exactly once, in order, after the
    // history — nothing queued during the pause is lost or reordered.
    assert.deepEqual(order, ['R1', 'R2', 'L3', 'L4', 'L5']);
  });

  test('audit records the exact previous and current switch values', () => {
    const bus = new EventBus();
    const sub = bus.subscribe('t', () => {});
    bus.setCatchUpOnReplay(sub.id, true);
    bus.setCatchUpOnReplay(sub.id, false);
    const audits = catchUpRevisionAudits(bus);
    assert.equal(audits.length, 2);
    assert.equal(audits[0].action, 'catch_up_on_replay_revised');
    assert.equal(audits[0].subId, sub.id);
    assert.equal(audits[0].pattern, 't');
    assert.deepEqual(audits[0].detail, { catchUpOnReplay: true, oldCatchUpOnReplay: false });
    assert.deepEqual(audits[1].detail, { catchUpOnReplay: false, oldCatchUpOnReplay: true });
  });

  test('revising the switch does not touch backpressure callbacks or the drop policy', async () => {
    const bus = new EventBus();
    const onBackpressure = () => {};
    const onDrained = () => {};
    const seen: unknown[] = [];
    const sub = bus.subscribe('t', (msg) => seen.push(msg.payload), {
      queueSize: 2,
      dropPolicy: 'drop-newest',
      onBackpressure,
      onDrained,
    });
    bus.setCatchUpOnReplay(sub.id, true);
    // No sibling revision was audited.
    assert.equal(
      bus.getSubscriptionAudit({ action: 'backpressure_callbacks_revised' }).length,
      0,
    );
    assert.equal(bus.getSubscriptionAudit({ action: 'drop_policy_revised' }).length, 0);
    // The callbacks are still the same references: re-affirming them
    // is a no-op, which proves the revision did not swap or clear them.
    bus.setBackpressureCallbacks(sub.id, { onBackpressure });
    assert.equal(
      bus.getSubscriptionAudit({ action: 'backpressure_callbacks_revised' }).length,
      0,
    );
    bus.setBackpressureCallbacks(sub.id, { onDrained });
    assert.equal(
      bus.getSubscriptionAudit({ action: 'backpressure_callbacks_revised' }).length,
      0,
    );
    // The shed policy is still 'drop-newest': the third arrival is
    // the one shed, and the first two still deliver.
    bus.publish('t', 'a');
    bus.publish('t', 'b');
    bus.publish('t', 'c');
    await flush();
    assert.deepEqual(seen, ['a', 'b']);
    assert.equal(bus.droppedCount(sub.id), 1);
  });
});
