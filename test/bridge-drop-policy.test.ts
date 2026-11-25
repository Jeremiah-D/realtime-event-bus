/**
 * Bridge ingress buffer shed policy (EB-560).
 *
 * A full bridge ingress buffer sheds per `BridgeOptions.dropPolicy`:
 * the default `'drop-newest'` refuses the newcomer (the EB-51
 * behavior), while `'drop-oldest'` sheds the oldest queued envelope
 * to admit the newcomer. Either way the shed counts once in
 * `getStats().bridge.dropped`. The policy is a construction option
 * and revisable live via `setBridge({ dropPolicy? })` — a revision
 * takes effect at the next overflow, leaves the queued envelopes in
 * place, and is audited as `bridge_revised` with the policy in the
 * detail. Illegal policies fail closed with the live policy and the
 * audit log untouched; an identical or empty revision is a no-op.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type BridgeDropPolicy,
  type BridgeEnvelope,
  type BridgeTransport,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A transport that records every mirrored envelope, never failing. */
function recordingTransport(): BridgeTransport & { mirrored: BridgeEnvelope[] } {
  const t = {
    name: 'recording',
    mirrored: [] as BridgeEnvelope[],
    publish(envelope: BridgeEnvelope) {
      t.mirrored.push(envelope);
    },
  };
  return t;
}

/** A bus with a bridge, subscribed to 't', collecting received payloads. */
function bridgedBus(maxInboundQueue = 2, dropPolicy?: BridgeDropPolicy) {
  const transport = recordingTransport();
  const bus = new EventBus({
    bridge: {
      transport,
      maxInboundQueue,
      ...(dropPolicy === undefined ? {} : { dropPolicy }),
    },
  });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => {
    received.push(msg.payload);
  });
  return { bus, transport, received };
}

describe('bridge drop policy (EB-560)', () => {
  it('the default policy is drop-newest, matching the EB-51 behavior', async () => {
    const { bus, received } = bridgedBus(2);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 1 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 2 }).accepted, true);
    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 3 }), {
      accepted: false,
      reason: 'shed',
    });
    assert.equal(bus.getStats().bridge.dropped, 1);
    await flush();
    assert.deepEqual(received, [1, 2]);
    await bus.close();
  });

  it('drop-oldest sheds the oldest queued envelope and admits the newcomer', async () => {
    const { bus, received } = bridgedBus(2, 'drop-oldest');
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 1 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 2 }).accepted, true);
    // The buffer is full, but the newcomer is admitted — the oldest
    // queued envelope is shed instead.
    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 3 }), {
      accepted: true,
    });
    assert.equal(bus.getStats().bridge.dropped, 1);
    await flush();
    assert.deepEqual(received, [2, 3]);
    await bus.close();
  });

  it('drop-oldest sheds the oldest on every overflow and counts each shed', async () => {
    const { bus, received } = bridgedBus(2, 'drop-oldest');
    for (const payload of [1, 2, 3, 4]) {
      assert.equal(bus.receiveFromBridge({ topic: 't', payload }).accepted, true);
    }
    assert.equal(bus.getStats().bridge.dropped, 2);
    await flush();
    assert.deepEqual(received, [3, 4]);
    await bus.close();
  });

  it('a live revision to drop-oldest takes effect at the next overflow', async () => {
    const { bus, received } = bridgedBus(2);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 1 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 2 }).accepted, true);
    bus.setBridge({ dropPolicy: 'drop-oldest' });
    // The queued envelopes are untouched, but the next arrival now
    // sheds the oldest instead of refusing the newcomer.
    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 3 }), {
      accepted: true,
    });
    assert.equal(bus.getStats().bridge.dropped, 1);
    await flush();
    assert.deepEqual(received, [2, 3]);
    const revised = bus.getSubscriptionAudit({ action: 'bridge_revised' });
    assert.equal(revised.length, 1);
    assert.deepEqual(revised[0].detail, {
      maxInboundQueue: 2,
      oldMaxInboundQueue: 2,
      dropPolicy: 'drop-oldest',
      oldDropPolicy: 'drop-newest',
    });
    await bus.close();
  });

  it('a live revision back to drop-newest restores refusing the newcomer', async () => {
    const { bus, received } = bridgedBus(2, 'drop-oldest');
    bus.setBridge({ dropPolicy: 'drop-newest' });
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 1 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 2 }).accepted, true);
    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 3 }), {
      accepted: false,
      reason: 'shed',
    });
    assert.equal(bus.getStats().bridge.dropped, 1);
    await flush();
    assert.deepEqual(received, [1, 2]);
    await bus.close();
  });

  it('a full options object on a configured bus moves the policy but ignores the transport', async () => {
    const { bus, transport, received } = bridgedBus(2, 'drop-oldest');
    const other = recordingTransport();
    bus.setBridge({ transport: other, maxInboundQueue: 2, dropPolicy: 'drop-newest' });
    const revised = bus.getSubscriptionAudit({ action: 'bridge_revised' });
    assert.equal(revised.length, 1);
    assert.deepEqual(revised[0].detail, {
      maxInboundQueue: 2,
      oldMaxInboundQueue: 2,
      dropPolicy: 'drop-newest',
      oldDropPolicy: 'drop-oldest',
    });
    // The original transport still mirrors; the other one never does.
    bus.publish('t', 1);
    assert.equal(transport.mirrored.length, 1);
    assert.equal(other.mirrored.length, 0);
    // And the new policy governs inbound traffic.
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 10 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 11 }).accepted, true);
    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 12 }), {
      accepted: false,
      reason: 'shed',
    });
    await flush();
    assert.deepEqual(received, [1, 10, 11]);
    await bus.close();
  });

  it('setBridge rejects illegal policies and leaves the live policy untouched', async () => {
    const { bus, received } = bridgedBus(2, 'drop-oldest');
    const bad: Array<unknown> = ['block', 'DROP-OLDEST', '', 42, {}, ['drop-oldest']];
    for (const dropPolicy of bad) {
      assert.throws(
        () => bus.setBridge({ dropPolicy } as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(dropPolicy)}`,
      );
    }
    assert.equal(bus.getSubscriptionAudit({ action: 'bridge_revised' }).length, 0);
    // The failed calls changed nothing: the live policy is still
    // drop-oldest — a burst of three admits all three, shedding the
    // oldest once.
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 1 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 2 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 3 }).accepted, true);
    assert.equal(bus.getStats().bridge.dropped, 1);
    await flush();
    assert.deepEqual(received, [2, 3]);
    // A later valid revision still works after the failed ones.
    bus.setBridge({ dropPolicy: 'drop-newest' });
    assert.equal(bus.getSubscriptionAudit({ action: 'bridge_revised' }).length, 1);
    await bus.close();
  });

  it('the constructor rejects an illegal dropPolicy', async () => {
    assert.throws(
      () =>
        new EventBus({
          bridge: {
            transport: recordingTransport(),
            dropPolicy: 'block' as never,
          },
        }),
      RangeError,
    );
  });

  it('an identical or empty revision is a no-op with no audit', async () => {
    const { bus, received } = bridgedBus(2);
    bus.setBridge({ dropPolicy: 'drop-newest' });
    bus.setBridge({ maxInboundQueue: 2 });
    bus.setBridge({});
    assert.equal(bus.getSubscriptionAudit({ action: 'bridge_revised' }).length, 0);
    // A bound-only revision still audits the policy in its detail —
    // the detail shape is fixed — but the policy itself does not move.
    bus.setBridge({ maxInboundQueue: 3 });
    const revised = bus.getSubscriptionAudit({ action: 'bridge_revised' });
    assert.equal(revised.length, 1);
    assert.deepEqual(revised[0].detail, {
      maxInboundQueue: 3,
      oldMaxInboundQueue: 2,
      dropPolicy: 'drop-newest',
      oldDropPolicy: 'drop-newest',
    });
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 1 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 2 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 3 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 4 }).accepted, false);
    assert.equal(bus.getStats().bridge.dropped, 1);
    await flush();
    assert.deepEqual(received, [1, 2, 3]);
    await bus.close();
  });

  it('an unconfigured bus cannot be revised by policy alone, but the enable can carry one', async () => {
    const bus = new EventBus({});
    bus.subscribe('t', () => {});
    // A revision-shaped call carries no transport, so the enable half
    // (EB-559) refuses it — the bus stays unconfigured, and a later
    // full-options call still enables it, policy included.
    assert.throws(() => bus.setBridge({ dropPolicy: 'drop-oldest' }), RangeError);
    assert.equal(bus.getSubscriptionAudit({ action: 'bridge_revised' }).length, 0);
    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 1 }), {
      accepted: false,
      reason: 'not-configured',
    });

    const transport = recordingTransport();
    bus.setBridge({ transport, maxInboundQueue: 2, dropPolicy: 'drop-oldest' });
    const revised = bus.getSubscriptionAudit({ action: 'bridge_revised' });
    assert.equal(revised.length, 1);
    assert.deepEqual(revised[0].detail, {
      maxInboundQueue: 2,
      oldMaxInboundQueue: null,
      dropPolicy: 'drop-oldest',
      oldDropPolicy: null,
    });
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 1 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 2 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 3 }).accepted, true);
    assert.equal(bus.getStats().bridge.dropped, 1);
    await flush();
    await bus.close();
  });

  it('a restart restores the construction policy (the live revision is live-only)', async () => {
    const transport = recordingTransport();
    const bus = new EventBus({
      busId: 'bridge-drop-policy-restart',
      bridge: { transport, maxInboundQueue: 2, dropPolicy: 'drop-oldest' },
    });
    bus.subscribe('t', () => {});
    bus.setBridge({ dropPolicy: 'drop-newest' });
    await bus.close();

    const restarted = new EventBus({
      busId: 'bridge-drop-policy-restart',
      bridge: { transport, maxInboundQueue: 2, dropPolicy: 'drop-oldest' },
    });
    const received: unknown[] = [];
    restarted.subscribe('t', (msg) => {
      received.push(msg.payload);
    });
    assert.equal(restarted.receiveFromBridge({ topic: 't', payload: 1 }).accepted, true);
    assert.equal(restarted.receiveFromBridge({ topic: 't', payload: 2 }).accepted, true);
    assert.equal(restarted.receiveFromBridge({ topic: 't', payload: 3 }).accepted, true);
    assert.equal(restarted.getStats().bridge.dropped, 1);
    await flush();
    assert.deepEqual(received, [2, 3]);
    await restarted.close();
  });
});
