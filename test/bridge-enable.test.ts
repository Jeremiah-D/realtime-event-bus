/**
 * Bridge live enable (EB-559):
 * `setBridge` on a bus that never configured `bridge` installs one live —
 * no restart, no lost subscriptions — with the full `BridgeOptions`
 * resolved through the construction-time rule (`resolveBridgeOptions`:
 * the `transport` is the one knob with no default, `maxInboundQueue`
 * defaulting to 1024). The next publish mirrors to the new transport
 * and `receiveFromBridge` starts accepting inbound envelopes; a `health`
 * block builds the monitor exactly like the construction row. The
 * enable is audited like any revision, with `oldMaxInboundQueue`
 * `null`, and is live-only: a restart restores the unconfigured state.
 * A revision-shaped call without a `transport` cannot enable, `null`
 * is a silent no-op, and a closed bus refuses the enable.
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

/** A transport whose every publish throws, counting the attempts. */
function failingTransport(): BridgeTransport & { calls: number } {
  const t = {
    name: 'failing',
    calls: 0,
    publish(_envelope: BridgeEnvelope) {
      t.calls += 1;
      throw new Error('broker down');
    },
  };
  return t;
}

/** A transport whose custom probe counts its calls and succeeds. */
function probeCountingTransport(): BridgeTransport & { probeCalls: number } {
  const t = {
    name: 'probe-counting',
    probeCalls: 0,
    publish(_envelope: BridgeEnvelope) {},
    probe() {
      t.probeCalls += 1;
      return true;
    },
  };
  return t;
}

function auditEntries(bus: EventBus, action: string) {
  return bus.getSubscriptionAudit({ action: action as never });
}

describe('bridge live enable (EB-559)', () => {
  it('the next publish after the enable mirrors to the new transport with the envelope identity riding along', async () => {
    const transport = recordingTransport();
    const bus = new EventBus({ trace: true });
    bus.subscribe('t', () => {});
    bus.publish('t', 'before');
    assert.equal(transport.mirrored.length, 0);

    bus.setBridge({ transport });
    bus.publish('t', 'after', { key: 'k', messageId: 'm-1' });

    assert.equal(transport.mirrored.length, 1);
    const env = transport.mirrored[0];
    assert.equal(env.topic, 't');
    assert.equal(env.payload, 'after');
    assert.equal(env.key, 'k');
    assert.equal(env.keySeq, 1);
    assert.equal(env.messageId, 'm-1');
    assert.match(env.traceId ?? '', /^[0-9a-f]{32}$/);
    assert.equal(bus.getStats().bridge.outbound, 1);
    await bus.close();
  });

  it('the enable accepts inbound envelopes and fans them out locally; envelopes refused before it are not replayed', async () => {
    const transport = recordingTransport();
    const bus = new EventBus({});
    const received: unknown[] = [];
    bus.subscribe('t', (msg) => {
      received.push(msg.payload);
    });
    // Before the enable the bridge is not configured at all.
    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 1 }), {
      accepted: false,
      reason: 'not-configured',
    });
    await flush();
    assert.deepEqual(received, []);

    bus.setBridge({ transport });
    // The envelope refused above is not traced back or replayed.
    await flush();
    assert.deepEqual(received, []);
    assert.equal(bus.getStats().bridge.inbound, 0);

    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 2 }), {
      accepted: true,
    });
    await flush();
    assert.deepEqual(received, [2]);
    assert.equal(bus.getStats().bridge.inbound, 1);
    // An illegal envelope still refuses after the enable.
    assert.deepEqual(bus.receiveFromBridge({ topic: '', payload: 3 }), {
      accepted: false,
      reason: 'invalid-envelope',
    });
    await bus.close();
  });

  it('an enable without a transport throws and leaves the bus unconfigured', async () => {
    const bus = new EventBus({});
    const bad: Array<unknown> = [{}, { maxInboundQueue: 5 }];
    for (const opts of bad) {
      assert.throws(
        () => bus.setBridge(opts as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(opts)}`,
      );
    }
    assert.equal(auditEntries(bus, 'bridge_revised').length, 0);
    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 1 }), {
      accepted: false,
      reason: 'not-configured',
    });
    // A later valid call still enables it.
    const transport = recordingTransport();
    bus.setBridge({ transport });
    assert.equal(auditEntries(bus, 'bridge_revised').length, 1);
    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 1 }), {
      accepted: true,
    });
    await flush();
    await bus.close();
  });

  it('an illegal transport throws and leaves the bus unconfigured', async () => {
    const bus = new EventBus({});
    const bad: Array<unknown> = [
      { transport: {} },
      { transport: { name: 'x', publish: 'nope' } },
      { transport: 'loopback' },
      { transport: null },
    ];
    for (const opts of bad) {
      assert.throws(
        () => bus.setBridge(opts as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(opts)}`,
      );
    }
    assert.equal(auditEntries(bus, 'bridge_revised').length, 0);
    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 1 }), {
      accepted: false,
      reason: 'not-configured',
    });
    // A later valid call still enables it.
    bus.setBridge({ transport: recordingTransport() });
    assert.equal(auditEntries(bus, 'bridge_revised').length, 1);
    await bus.close();
  });

  it('illegal maxInboundQueue and health values throw and leave the bus unconfigured', async () => {
    const bus = new EventBus({});
    const transport = recordingTransport();
    const bad: Array<unknown> = [
      { transport, maxInboundQueue: 0 },
      { transport, maxInboundQueue: -1 },
      { transport, maxInboundQueue: 1.5 },
      { transport, maxInboundQueue: Number.NaN },
      { transport, maxInboundQueue: 'many' },
      { transport, health: { intervalMs: 0 } },
      { transport, health: { intervalMs: -5 } },
      { transport, health: { failureThreshold: 0 } },
      { transport, health: { failureThreshold: 1.5 } },
      { transport, health: 'fast' },
    ];
    for (const opts of bad) {
      assert.throws(
        () => bus.setBridge(opts as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(opts)}`,
      );
    }
    assert.equal(auditEntries(bus, 'bridge_revised').length, 0);
    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 1 }), {
      accepted: false,
      reason: 'not-configured',
    });
    // A later valid call still enables it.
    bus.setBridge({ transport });
    assert.equal(auditEntries(bus, 'bridge_revised').length, 1);
    await bus.close();
  });

  it('a non-object enable throws and leaves the bus unconfigured', async () => {
    const bus = new EventBus({});
    const bad: Array<unknown> = ['nope', 42, ['x']];
    for (const opts of bad) {
      assert.throws(
        () => bus.setBridge(opts as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(opts)}`,
      );
    }
    assert.equal(auditEntries(bus, 'bridge_revised').length, 0);
    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 1 }), {
      accepted: false,
      reason: 'not-configured',
    });
    // A later valid call still enables it.
    bus.setBridge({ transport: recordingTransport() });
    assert.equal(auditEntries(bus, 'bridge_revised').length, 1);
    await bus.close();
  });

  it('null on a never-enabled bus is a silent no-op with no audit', async () => {
    const bus = new EventBus({});
    bus.setBridge(null as never);
    bus.setBridge(undefined as never);
    assert.equal(auditEntries(bus, 'bridge_revised').length, 0);
    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 1 }), {
      accepted: false,
      reason: 'not-configured',
    });
    // The bus is still unconfigured: a later valid call enables it.
    bus.setBridge({ transport: recordingTransport() });
    assert.equal(auditEntries(bus, 'bridge_revised').length, 1);
    await bus.close();
  });

  it('a closed bus refuses the enable', async () => {
    const bus = new EventBus({});
    await bus.close();
    assert.throws(() => bus.setBridge({ transport: recordingTransport() }), RangeError);
    assert.equal(auditEntries(bus, 'bridge_revised').length, 0);
  });

  it('publishes before the enable are never back-mirrored', async () => {
    const transport = recordingTransport();
    const bus = new EventBus({});
    bus.subscribe('t', () => {});
    bus.publish('t', 1);
    bus.publish('t', 2);
    bus.setBridge({ transport });
    assert.equal(transport.mirrored.length, 0);
    assert.equal(bus.getStats().bridge.outbound, 0);
    bus.publish('t', 3);
    assert.equal(transport.mirrored.length, 1);
    assert.deepEqual(transport.mirrored[0].payload, 3);
    assert.equal(bus.getStats().bridge.outbound, 1);
    await bus.close();
  });

  it('after the enable, maxInboundQueue revises against the live values and the transport identity is untouched', async () => {
    const transport = recordingTransport();
    const other = recordingTransport();
    const bus = new EventBus({});
    bus.subscribe('t', () => {});
    bus.setBridge({ transport, maxInboundQueue: 2 });
    bus.publish('t', 1);
    assert.equal(transport.mirrored.length, 1);

    bus.setBridge({ maxInboundQueue: 5 });
    const revised = auditEntries(bus, 'bridge_revised');
    assert.equal(revised.length, 2);
    assert.deepEqual(revised[1].detail, { maxInboundQueue: 5, oldMaxInboundQueue: 2, dropPolicy: 'drop-newest', oldDropPolicy: 'drop-newest' });

    // A full options object on a configured bus moves only the bound:
    // the different transport is ignored, exactly like setOtlp.
    bus.setBridge({ transport: other, maxInboundQueue: 3 });
    const revisedAgain = auditEntries(bus, 'bridge_revised');
    assert.equal(revisedAgain.length, 3);
    assert.deepEqual(revisedAgain[2].detail, { maxInboundQueue: 3, oldMaxInboundQueue: 5, dropPolicy: 'drop-newest', oldDropPolicy: 'drop-newest' });
    bus.publish('t', 2);
    assert.equal(transport.mirrored.length, 2);
    assert.equal(other.mirrored.length, 0);

    // The new bound governs inbound traffic: three admit, the fourth sheds.
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 10 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 11 }).accepted, true);
    assert.equal(bus.receiveFromBridge({ topic: 't', payload: 12 }).accepted, true);
    assert.deepEqual(bus.receiveFromBridge({ topic: 't', payload: 13 }), {
      accepted: false,
      reason: 'shed',
    });
    assert.equal(bus.getStats().bridge.dropped, 1);
    await flush();
    await bus.close();
  });

  it('enabling with health starts the monitor: probes run and the health reads healthy', async () => {
    const transport = probeCountingTransport();
    const captured: Array<{ fn: () => void; ms: number }> = [];
    const cleared: unknown[] = [];
    const bus = new EventBus({});
    bus.setBridge({
      transport,
      health: {
        intervalMs: 50,
        failureThreshold: 3,
        setTimer: (fn, ms) => {
          captured.push({ fn, ms });
          return captured.length;
        },
        clearTimer: (h) => {
          cleared.push(h);
        },
      },
    });
    // The monitor starts healthy; the first probe fires after one interval.
    assert.deepEqual(bus.getStats().bridge.health, {
      state: 'healthy',
      consecutiveFailures: 0,
    });
    assert.equal(captured.length, 1);
    assert.equal(captured[0].ms, 50);
    assert.equal(transport.probeCalls, 0);
    // Fire the tick; the probe runs and the schedule re-arms.
    captured[0].fn();
    await flush();
    await flush();
    assert.equal(transport.probeCalls, 1);
    assert.equal(bus.getStats().bridge.health.state, 'healthy');
    assert.equal(bus.getStats().bridge.health.consecutiveFailures, 0);
    assert.ok(bus.getStats().bridge.health.lastProbeAt !== undefined);
    assert.equal(captured.length, 2);
    // The enable itself was audited with the old bound null.
    const revised = auditEntries(bus, 'bridge_revised');
    assert.equal(revised.length, 1);
    assert.deepEqual(revised[0].detail, { maxInboundQueue: 1024, oldMaxInboundQueue: null, dropPolicy: 'drop-newest', oldDropPolicy: null });
    await bus.close();
    // Close stops the monitor through the injected clearTimer.
    assert.equal(cleared.length, 1);
  });

  it('enabling with health pauses outbound mirroring at the failure threshold', async () => {
    const transport = failingTransport();
    const bus = new EventBus({});
    bus.subscribe('t', () => {});
    bus.setBridge({
      transport,
      health: { intervalMs: 30_000, failureThreshold: 1 },
    });
    bus.publish('t', 1);
    assert.equal(transport.calls, 1);
    assert.equal(bus.getStats().bridge.outbound, 1);
    assert.deepEqual(bus.getStats().bridge.health, {
      state: 'unhealthy',
      consecutiveFailures: 1,
    });
    assert.equal(auditEntries(bus, 'bridge_unhealthy').length, 1);
    // While unhealthy the next publish is not even attempted.
    bus.publish('t', 2);
    assert.equal(transport.calls, 1);
    assert.equal(bus.getStats().bridge.outbound, 1);
    await bus.close();
  });

  it('enabling without health leaves health disabled, and setBridgeHealth can enable probing later', async () => {
    const bus = new EventBus({});
    bus.setBridge({ transport: recordingTransport() });
    assert.deepEqual(bus.getStats().bridge.health, {
      state: 'disabled',
      consecutiveFailures: 0,
    });
    bus.setBridgeHealth({ intervalMs: 100 });
    assert.deepEqual(bus.getStats().bridge.health, {
      state: 'healthy',
      consecutiveFailures: 0,
    });
    const healthRevised = auditEntries(bus, 'bridge_health_revised');
    assert.equal(healthRevised.length, 1);
    assert.deepEqual(healthRevised[0].detail, {
      intervalMs: 100,
      oldIntervalMs: null,
      failureThreshold: 3,
      oldFailureThreshold: null,
    });
    await bus.close();
  });

  it('the enable is audited with oldMaxInboundQueue null', async () => {
    const bus = new EventBus({});
    bus.setBridge({ transport: recordingTransport(), maxInboundQueue: 7 });
    const revised = auditEntries(bus, 'bridge_revised');
    assert.equal(revised.length, 1);
    assert.equal(revised[0].actor, 'system');
    assert.deepEqual(revised[0].detail, { maxInboundQueue: 7, oldMaxInboundQueue: null, dropPolicy: 'drop-newest', oldDropPolicy: null });
    await bus.close();
  });

  it('a restart restores the unconfigured state', async () => {
    const bus = new EventBus({ busId: 'bridge-enable-restart' });
    bus.setBridge({ transport: recordingTransport() });
    assert.equal(auditEntries(bus, 'bridge_revised').length, 1);
    await bus.close();

    const restarted = new EventBus({ busId: 'bridge-enable-restart' });
    assert.deepEqual(restarted.receiveFromBridge({ topic: 't', payload: 1 }), {
      accepted: false,
      reason: 'not-configured',
    });
    assert.equal(restarted.getStats().bridge.outbound, 0);
    assert.equal(auditEntries(restarted, 'bridge_revised').length, 0);
    assert.throws(() => restarted.setBridge({ maxInboundQueue: 2 }), RangeError);
    await restarted.close();
  });
});
