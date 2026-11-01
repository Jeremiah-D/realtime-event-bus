/**
 * Bus-level bridge inbound queue revision (EB-313).
 *
 * `setBridge` revises the bridge ingress buffer bound live — without
 * restarting the bus. Only a bus that already enabled `bridge` can be
 * revised; illegal values fail closed with the live bound and the audit
 * log untouched; an identical (or empty) revision is a no-op with no
 * audit. The new bound governs from the next inbound envelope onward:
 * tightening sheds at the new bound on the next arrival (envelopes
 * already queued still drain), loosening admits more before the next
 * shed. The transport and the health monitor are untouched by a
 * revision — outbound mirroring keeps working and the health state
 * survives.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BridgeEnvelope, type BridgeTransport } from '../src/bus.ts';

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
function bridgedBus(maxInboundQueue?: number) {
  const transport = recordingTransport();
  const bus = new EventBus({
    bridge: {
      transport,
      ...(maxInboundQueue === undefined ? {} : { maxInboundQueue }),
    },
  });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => {
    received.push(msg.payload);
  });
  return { bus, transport, received };
}

describe('bridge revision (EB-313)', () => {
  it('setBridge fails closed when bridge is not enabled', async () => {
    const bus = new EventBus({});
    assert.throws(() => bus.setBridge({ maxInboundQueue: 2 }), RangeError);
    assert.equal(bus.getSubscriptionAudit({ action: 'bridge_revised' }).length, 0);
    await bus.close();
  });

  it('setBridge rejects illegal values and leaves the live bound untouched', async () => {
    const { bus, received } = bridgedBus(2);
    const bad: Array<unknown> = [
      { maxInboundQueue: 0 },
      { maxInboundQueue: -1 },
      { maxInboundQueue: 1.5 },
      { maxInboundQueue: Number.NaN },
      { maxInboundQueue: Number.POSITIVE_INFINITY },
      { maxInboundQueue: 'many' },
      'nope',
      42,
      ['x'],
    ];
    for (const revision of bad) {
      assert.throws(
        () => bus.setBridge(revision as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(revision)}`,
      );
    }
    // The failed calls changed nothing: the live bound is still 2 — a
    // synchronous burst of three admits two and sheds the third.
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 1 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 2 }).accepted, true);
    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 3 }), {
      accepted: false,
      reason: 'shed',
    });
    assert.equal(bus.getStats().bridge.dropped, 1);
    await flush();
    assert.deepEqual(received, [1, 2]);
    assert.equal(bus.getSubscriptionAudit({ action: 'bridge_revised' }).length, 0);
    await bus.close();
  });

  it('setBridge with an identical value or an empty revision is a no-op with no audit', async () => {
    const { bus, received } = bridgedBus(2);
    bus.setBridge({ maxInboundQueue: 2 });
    bus.setBridge({});
    bus.setBridge(null as never);
    bus.setBridge(undefined as never);
    assert.equal(bus.getSubscriptionAudit({ action: 'bridge_revised' }).length, 0);
    // The bound is still 2: a burst of three sheds the third.
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 1 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 2 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 3 }).accepted, false);
    assert.equal(bus.getStats().bridge.dropped, 1);
    await flush();
    assert.deepEqual(received, [1, 2]);
    await bus.close();
  });

  it('tightening sheds at the new bound on the next inbound envelope', async () => {
    const { bus, received } = bridgedBus(3);
    // Two envelopes queued; the drain microtask has not run yet.
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 1 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 2 }).accepted, true);
    bus.setBridge({ maxInboundQueue: 1 });
    // The queued envelopes are untouched, but the queue is already at
    // (over) the new bound, so the next arrival sheds at once.
    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 3 }), {
      accepted: false,
      reason: 'shed',
    });
    assert.equal(bus.getStats().bridge.dropped, 1);
    await flush();
    assert.deepEqual(received, [1, 2]);
    const revised = bus.getSubscriptionAudit({ action: 'bridge_revised' });
    assert.equal(revised.length, 1);
    assert.deepEqual(revised[0].detail, { maxInboundQueue: 1, oldMaxInboundQueue: 3 });
    await bus.close();
  });

  it('loosening admits more before the next shed', async () => {
    const { bus, received } = bridgedBus(1);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 1 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 2 }).accepted, false);
    assert.equal(bus.getStats().bridge.dropped, 1);
    bus.setBridge({ maxInboundQueue: 3 });
    // The queue still holds the first envelope; the loosened bound now
    // admits two more before shedding again.
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 3 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 4 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 5 }).accepted, false);
    assert.equal(bus.getStats().bridge.dropped, 2);
    await flush();
    assert.deepEqual(received, [1, 3, 4]);
    const revised = bus.getSubscriptionAudit({ action: 'bridge_revised' });
    assert.equal(revised.length, 1);
    assert.deepEqual(revised[0].detail, { maxInboundQueue: 3, oldMaxInboundQueue: 1 });
    await bus.close();
  });

  it('setBridge revises the default bound and audits the old default', async () => {
    const { bus } = bridgedBus();
    bus.setBridge({ maxInboundQueue: 2 });
    const revised = bus.getSubscriptionAudit({ action: 'bridge_revised' });
    assert.equal(revised.length, 1);
    assert.deepEqual(revised[0].detail, { maxInboundQueue: 2, oldMaxInboundQueue: 1024 });
    // The new bound governs: a burst of three sheds the third.
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 1 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 2 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 3 }).accepted, false);
    assert.equal(bus.getStats().bridge.dropped, 1);
    await flush();
    await bus.close();
  });

  it('setBridge leaves the transport and the health monitor untouched', async () => {
    const transport = recordingTransport();
    const bus = new EventBus({
      bridge: {
        transport,
        maxInboundQueue: 2,
        health: { intervalMs: 30_000, failureThreshold: 3 },
      },
    });
    bus.subscribe('t', () => {});
    // Outbound mirroring works before the revision.
    bus.publish('t', 1);
    assert.equal(transport.mirrored.length, 1);
    assert.equal(bus.getStats().bridge.outbound, 1);
    assert.deepEqual(bus.getStats().bridge.health, {
      state: 'healthy',
      consecutiveFailures: 0,
    });

    bus.setBridge({ maxInboundQueue: 5 });

    // The same transport still mirrors, and the health state survives.
    bus.publish('t', 2);
    assert.equal(transport.mirrored.length, 2);
    assert.equal(bus.getStats().bridge.outbound, 2);
    assert.deepEqual(bus.getStats().bridge.health, {
      state: 'healthy',
      consecutiveFailures: 0,
    });
    // Only the bridge revision was audited — no health retune.
    assert.equal(bus.getSubscriptionAudit({ action: 'bridge_revised' }).length, 1);
    assert.equal(bus.getSubscriptionAudit({ action: 'bridge_health_revised' }).length, 0);
    // And the loosened bound governs inbound traffic.
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 10 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 11 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 12 }).accepted, true);
    assert.equal(bus.getStats().bridge.dropped, 0);
    await flush();
    await bus.close();
  });
});
