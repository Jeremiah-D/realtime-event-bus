import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { ClusterHub, ClusterLink } from '../src/cluster.ts';
import { ReconnectController } from '../src/reconnect.ts';

/**
 * Cluster link reconnect backoff jitter revision (EB-395): `jitterMs`
 * joins the live revision surface (`ReconnectBackoffRevision` /
 * `ClusterLinkReconnectBackoffRevision` /
 * `setClusterLinkReconnectBackoff`). The controller tracks the live
 * jitter, `getReconnectBackoff()` returns it, and a retune rebuilds
 * the strategy at the merged jitter — so a plain base/cap revision no
 * longer silently resets a custom jitter to the 250ms default (the
 * EB-340 gap). Jitter is validated like the other numeric fields
 * (non-negative finite; 0 is a deterministic, jitter-free policy), a
 * same-value revision is a no-op with no audit, and an armed backoff
 * timer keeps the delay it was armed with.
 *
 * Jitter is the anti-thundering-herd control: after a hub outage a
 * whole fleet of nodes rejoining at the same base would stampede the
 * hub in lockstep; widening the jitter spreads the attempts, and
 * tightening it to 0 gives a deterministic reconnect cadence for
 * incident drills. The controller-level tests below prove the spread
 * is real (sampled delays), not just stored.
 */

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(cond: () => boolean, timeoutMs = 5000, label = 'condition'): Promise<void> {
  const start = Date.now();
  for (;;) {
    if (cond()) return;
    if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${label}`);
    await sleep(10);
  }
}

interface Fixture {
  hub: ClusterHub;
  port: number;
  buses: EventBus[];
  links: ClusterLink[];
}

async function makeHub(
  opts: ConstructorParameters<typeof ClusterHub>[0] = {},
): Promise<{ hub: ClusterHub; port: number }> {
  const hub = new ClusterHub({ host: '127.0.0.1', ...opts });
  const port = await hub.listen();
  return { hub, port };
}

async function makeNode(
  fx: Fixture,
  opts: {
    nodeId?: string;
    heartbeatMs?: number;
    reconnect?: false | { baseDelayMs?: number; maxDelayMs?: number; maxAttempts?: number; jitterMs?: number };
  } = {},
): Promise<{ bus: EventBus; link: ClusterLink }> {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const link = await bus.connectToHub({
    url: `tcp://127.0.0.1:${fx.port}`,
    nodeId: opts.nodeId,
    heartbeatMs: opts.heartbeatMs,
    reconnect: opts.reconnect ?? { baseDelayMs: 20, maxDelayMs: 200, maxAttempts: 50 },
  });
  fx.buses.push(bus);
  fx.links.push(link);
  return { bus, link };
}

describe('cluster link reconnect backoff jitter revision (EB-395)', () => {
  let fx: Fixture;

  beforeEach(async () => {
    const { hub, port } = await makeHub();
    fx = { hub, port, buses: [], links: [] };
  });

  afterEach(async () => {
    for (const bus of fx.buses) await bus.disconnectCluster().catch(() => {});
    await fx.hub.close().catch(() => {});
  });

  it('the default jitter reads back as 250ms at every level', () => {
    const controller = new ReconnectController({
      connectFn: async () => {},
      baseDelayMs: 100,
      maxDelayMs: 5000,
      maxAttempts: 10,
    });
    try {
      assert.deepEqual(controller.getReconnectBackoff(), {
        baseDelayMs: 100,
        maxDelayMs: 5000,
        maxAttempts: 10,
        jitterMs: 250,
      });
    } finally {
      controller.stop();
    }
    const link = new ClusterLink({
      url: 'tcp://127.0.0.1:1',
      onMessage: (_msg: BusMessage) => true,
      getPatterns: () => [],
      reconnect: { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10 },
    });
    assert.deepEqual(link.getReconnectBackoff(), {
      baseDelayMs: 200,
      maxDelayMs: 5000,
      maxAttempts: 10,
      jitterMs: 250,
    });
  });

  it('jitter 0 makes reconnect delays exact and deterministic', () => {
    const controller = new ReconnectController({
      connectFn: async () => {},
      baseDelayMs: 100,
      maxDelayMs: 500,
      maxAttempts: 10,
      jitterMs: 0,
    });
    try {
      assert.deepEqual(controller.getReconnectBackoff(), {
        baseDelayMs: 100,
        maxDelayMs: 500,
        maxAttempts: 10,
        jitterMs: 0,
      });
      // No randomness at all: attempt n waits exactly base * 2^(n-1),
      // capped at maxDelayMs.
      assert.equal(controller.computeDelayMs(1), 100);
      assert.equal(controller.computeDelayMs(2), 200);
      assert.equal(controller.computeDelayMs(3), 400);
      assert.equal(controller.computeDelayMs(4), 500);
      assert.equal(controller.computeDelayMs(9), 500);
      for (let i = 0; i < 10; i++) {
        assert.equal(controller.computeDelayMs(1), 100, 'repeated reads must not drift');
      }
    } finally {
      controller.stop();
    }
  });

  it('a widened jitter spreads the reconnect delay distribution', () => {
    const controller = new ReconnectController({
      connectFn: async () => {},
      baseDelayMs: 100,
      maxDelayMs: 100000,
      maxAttempts: 10,
      jitterMs: 1000,
    });
    try {
      const samples = Array.from({ length: 40 }, () => controller.computeDelayMs(1));
      const min = Math.min(...samples);
      const max = Math.max(...samples);
      assert.ok(min >= 100, `min delay ${min} below the base`);
      assert.ok(max < 1100, `max delay ${max} above base + jitter`);
      assert.ok(max - min > 100, `spread ${max - min} too narrow for a 1000ms jitter window`);
    } finally {
      controller.stop();
    }
  });

  it('a jitter-only retune moves the live delay range and nothing else', () => {
    const controller = new ReconnectController({
      connectFn: async () => {},
      baseDelayMs: 100,
      maxDelayMs: 5000,
      maxAttempts: 10,
      jitterMs: 0,
    });
    try {
      assert.equal(controller.computeDelayMs(1), 100);
      controller.retuneBackoff({ jitterMs: 1000 });
      assert.deepEqual(controller.getReconnectBackoff(), {
        baseDelayMs: 100,
        maxDelayMs: 5000,
        maxAttempts: 10,
        jitterMs: 1000,
      });
      const samples = Array.from({ length: 40 }, () => controller.computeDelayMs(1));
      assert.ok(Math.min(...samples) >= 100);
      assert.ok(Math.max(...samples) < 1100);
      assert.ok(Math.max(...samples) > 600, 'the widened jitter must actually reach into the new range');
      // Tightening back to 0 restores determinism.
      controller.retuneBackoff({ jitterMs: 0 });
      assert.equal(controller.computeDelayMs(1), 100);
      assert.equal(controller.getReconnectBackoff().jitterMs, 0);
    } finally {
      controller.stop();
    }
  });

  it('a base revision no longer resets a custom jitter (the EB-340 gap)', () => {
    const controller = new ReconnectController({
      connectFn: async () => {},
      baseDelayMs: 100,
      maxDelayMs: 5000,
      maxAttempts: 10,
      jitterMs: 1000,
    });
    try {
      controller.retuneBackoff({ baseDelayMs: 200 });
      assert.deepEqual(controller.getReconnectBackoff(), {
        baseDelayMs: 200,
        maxDelayMs: 5000,
        maxAttempts: 10,
        jitterMs: 1000,
      });
      const samples = Array.from({ length: 40 }, () => controller.computeDelayMs(1));
      assert.ok(Math.min(...samples) >= 200, 'delays must start at the new base');
      assert.ok(Math.max(...samples) < 1200, 'delays must still reach base + the preserved jitter');
      assert.ok(Math.max(...samples) > 700, 'the preserved jitter must still spread delays');
      // A cap-only revision preserves it too. (The cap binds the
      // exponential part: at attempt 9 the delay is cap + jitter.)
      controller.retuneBackoff({ maxDelayMs: 900 });
      assert.equal(controller.getReconnectBackoff().jitterMs, 1000);
      const d9 = controller.computeDelayMs(9);
      assert.ok(d9 >= 900 && d9 < 1900, `high-attempt delay ${d9} must be cap 900 + jitter in [0, 1000)`);
    } finally {
      controller.stop();
    }
  });

  it('illegal jitter values fail closed with the live policy untouched', () => {
    const controller = new ReconnectController({
      connectFn: async () => {},
      baseDelayMs: 100,
      maxDelayMs: 5000,
      maxAttempts: 10,
      jitterMs: 500,
    });
    try {
      for (const bad of [
        { jitterMs: -1 },
        { jitterMs: Number.NaN },
        { jitterMs: Number.POSITIVE_INFINITY },
        { jitterMs: '500' },
        // One illegal field vetoes the whole revision, including the
        // legal fields riding along with it.
        { baseDelayMs: 50, jitterMs: -1 },
        { jitterMs: 100, maxAttempts: 0 },
      ]) {
        assert.throws(() => controller.retuneBackoff(bad as never), RangeError);
      }
      assert.deepEqual(
        controller.getReconnectBackoff(),
        { baseDelayMs: 100, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 500 },
        'failed revisions must leave the live policy untouched',
      );
      // The live policy is untouched: delays still use the old jitter.
      const samples = Array.from({ length: 20 }, () => controller.computeDelayMs(1));
      assert.ok(Math.min(...samples) >= 100);
      assert.ok(Math.max(...samples) < 600);
    } finally {
      controller.stop();
    }
  });

  it('a jitter retune during backoff leaves the armed timer alone', async () => {
    let attempts = 0;
    const controller = new ReconnectController({
      connectFn: async () => {
        attempts += 1;
        throw new Error('hub down');
      },
      baseDelayMs: 20,
      maxDelayMs: 200,
      maxAttempts: 100,
      jitterMs: 0,
    });
    try {
      controller.start();
      await waitFor(() => controller.getAttempt() >= 2, 5000, 'two failed attempts');
      const attemptBefore = controller.getAttempt();
      const attemptsBefore = attempts;
      // Deterministic pre-retune delays: attempt 1 waits exactly 20ms.
      assert.equal(controller.computeDelayMs(1), 20);
      controller.retuneBackoff({ baseDelayMs: 5000, maxDelayMs: 20000, jitterMs: 5000 });
      assert.equal(controller.getAttempt(), attemptBefore, 'retune must not touch the attempt count');
      assert.deepEqual(controller.getReconnectBackoff(), {
        baseDelayMs: 5000,
        maxDelayMs: 20000,
        maxAttempts: 100,
        jitterMs: 5000,
      });
      // The timer armed under the old policy keeps running at the old
      // delay: the next attempt lands promptly. After that the new
      // policy governs — no further attempt for seconds.
      await waitFor(() => attempts > attemptsBefore, 5000, 'next attempt on the armed timer');
      const stalledAt = controller.getAttempt();
      await sleep(400);
      assert.equal(
        controller.getAttempt(),
        stalledAt,
        'the new jittered policy must stall the following backoff, not the armed one',
      );
    } finally {
      controller.stop();
    }
  });

  it('ClusterConnectOptions carries a constructor-time jitter', async () => {
    const { link: zero } = await makeNode(fx, {
      nodeId: 'node-j0',
      reconnect: { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 0 },
    });
    await waitFor(() => zero.getStats().connected, 5000, 'connect');
    assert.deepEqual(zero.getReconnectBackoff(), {
      baseDelayMs: 200,
      maxDelayMs: 5000,
      maxAttempts: 10,
      jitterMs: 0,
    });
    const { link: wide } = await makeNode(fx, {
      nodeId: 'node-j1',
      reconnect: { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 1000 },
    });
    await waitFor(() => wide.getStats().connected, 5000, 'connect');
    assert.equal(wide.getReconnectBackoff().jitterMs, 1000);
    // A link-level retune without a connection validates the same way.
    const bare = new ClusterLink({
      url: 'tcp://127.0.0.1:1',
      onMessage: (_msg: BusMessage) => true,
      getPatterns: () => [],
      reconnect: { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 0 },
    });
    bare.retuneReconnectBackoff({ jitterMs: 500 });
    assert.equal(bare.getReconnectBackoff().jitterMs, 500);
    assert.throws(() => bare.retuneReconnectBackoff({ jitterMs: -1 }), RangeError);
    assert.equal(bare.getReconnectBackoff().jitterMs, 500, 'a refused retune must leave jitter untouched');
  });

  it('a bus-level jitter revision audits old and new, and a later base revision keeps it', async () => {
    const { bus, link } = await makeNode(fx, {
      nodeId: 'node-a',
      reconnect: { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10 },
    });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    bus.setClusterLinkReconnectBackoff({ jitterMs: 0 }, 'ops-lead');
    assert.deepEqual(link.getReconnectBackoff(), {
      baseDelayMs: 200,
      maxDelayMs: 5000,
      maxAttempts: 10,
      jitterMs: 0,
    });
    const audit = bus.getSubscriptionAudit({ action: 'cluster_link_reconnect_backoff_revised' });
    assert.equal(audit.length, 1);
    assert.equal(audit[0].actor, 'ops-lead');
    assert.deepEqual(audit[0].detail, {
      baseDelayMs: 200,
      maxDelayMs: 5000,
      maxAttempts: 10,
      jitterMs: 0,
      oldBaseDelayMs: 200,
      oldMaxDelayMs: 5000,
      oldMaxAttempts: 10,
      oldJitterMs: 250,
    });
    // The EB-340 gap, at bus level: a plain base revision must not
    // reset the jitter the operator just tightened to 0.
    bus.setClusterLinkReconnectBackoff({ baseDelayMs: 50 });
    assert.deepEqual(link.getReconnectBackoff(), {
      baseDelayMs: 50,
      maxDelayMs: 5000,
      maxAttempts: 10,
      jitterMs: 0,
    });
    const audit2 = bus.getSubscriptionAudit({ action: 'cluster_link_reconnect_backoff_revised' });
    assert.equal(audit2.length, 2);
    assert.equal(audit2[1].detail.jitterMs, 0);
    assert.equal(audit2[1].detail.oldJitterMs, 0);
    assert.equal(link.getStats().connected, true, 'a backoff revision must not reconnect');
  });

  it('an illegal bus-level jitter fails closed, and a same-value revision is a no-op', async () => {
    const { bus, link } = await makeNode(fx, {
      nodeId: 'node-a',
      reconnect: { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 100 },
    });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    for (const bad of [
      { jitterMs: -1 },
      { jitterMs: Number.NaN },
      { jitterMs: Number.POSITIVE_INFINITY },
      { jitterMs: '100' },
      { baseDelayMs: 50, jitterMs: -1 },
    ]) {
      assert.throws(() => bus.setClusterLinkReconnectBackoff(bad as never), RangeError);
    }
    assert.deepEqual(link.getReconnectBackoff(), {
      baseDelayMs: 200,
      maxDelayMs: 5000,
      maxAttempts: 10,
      jitterMs: 100,
    });
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_reconnect_backoff_revised' }).length,
      0,
      'failed revisions must not audit',
    );
    // Re-affirming the current four-field policy, and an empty
    // revision, are both no-ops with no audit.
    bus.setClusterLinkReconnectBackoff({ baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 100 });
    bus.setClusterLinkReconnectBackoff({});
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_reconnect_backoff_revised' }).length,
      0,
      'a same-policy revision must not audit',
    );
  });

  it('jitter de-synchronizes a reconnect storm: identical bases, spread attempts', () => {
    // Five nodes share one base; with jitter 0 they would all retry in
    // lockstep, with jitter they spread. The delay sample is the
    // observable proof — no timing involved, so no flakiness.
    const storm = Array.from({ length: 5 }, () => {
      const c = new ReconnectController({
        connectFn: async () => {},
        baseDelayMs: 100,
        maxDelayMs: 5000,
        maxAttempts: 10,
        jitterMs: 900,
      });
      return c;
    });
    try {
      const firstAttempts = storm.map((c) => c.computeDelayMs(1));
      for (const d of firstAttempts) {
        assert.ok(d >= 100 && d < 1000, `storm delay ${d} outside [100, 1000)`);
      }
      assert.ok(
        new Set(firstAttempts).size > 1,
        `identical bases must not retry in lockstep: ${firstAttempts.join(', ')}`,
      );
      const deterministic = Array.from({ length: 5 }, () => {
        const c = new ReconnectController({
          connectFn: async () => {},
          baseDelayMs: 100,
          maxDelayMs: 5000,
          maxAttempts: 10,
          jitterMs: 0,
        });
        return c;
      });
      try {
        const zeroAttempts = deterministic.map((c) => c.computeDelayMs(1));
        assert.deepEqual(zeroAttempts, [100, 100, 100, 100, 100], 'jitter 0 retries in lockstep by design');
      } finally {
        for (const c of deterministic) c.stop();
      }
    } finally {
      for (const c of storm) c.stop();
    }
  });
});
