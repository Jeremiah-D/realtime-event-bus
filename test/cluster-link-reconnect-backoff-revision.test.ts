import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { ClusterHub, ClusterLink } from '../src/cluster.ts';
import { ReconnectController } from '../src/reconnect.ts';

/**
 * Cluster link reconnect backoff policy revision (EB-340):
 * `setClusterLinkReconnectBackoff` retunes the node-side wait between
 * reconnect attempts without reconnecting — a field left out keeps its
 * live value, the attempt counter and `onGiveUp` are untouched, and a
 * backoff timer already armed keeps running at the delay it was armed
 * with, so the new policy governs from the next backoff. The hub-side
 * silence bound (`heartbeatTimeoutMs`) and the route table are
 * untouched and unvalidated.
 *
 * The controller-level tests drive a `ReconnectController` with an
 * always-failing `connectFn` directly: the attempt cadence is the
 * observable proof the new policy is really in use, not just stored —
 * with the default 250ms jitter the pre-retune delays stay well under
 * a second, while a retune to a 5000ms base stalls the next attempt
 * for seconds.
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
    reconnect?: false | { baseDelayMs?: number; maxDelayMs?: number; maxAttempts?: number };
  } = {},
): Promise<{ bus: EventBus; link: ClusterLink }> {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const link = await bus.connectToHub({
    url: `tcp://127.0.0.1:${fx.port}`,
    nodeId: opts.nodeId,
    heartbeatMs: opts.heartbeatMs,
    reconnect: opts.reconnect ?? { baseDelayMs: 20, maxDelayMs: 200, maxAttempts: 50, jitterMs: 250 },
  });
  fx.buses.push(bus);
  fx.links.push(link);
  return { bus, link };
}

describe('cluster link reconnect backoff revision (EB-340)', () => {
  let fx: Fixture;

  beforeEach(async () => {
    const { hub, port } = await makeHub();
    fx = { hub, port, buses: [], links: [] };
  });

  afterEach(async () => {
    for (const bus of fx.buses) await bus.disconnectCluster().catch(() => {});
    await fx.hub.close().catch(() => {});
  });

  it('throws RangeError with no cluster link, before connect and after disconnect', async () => {
    const bus = new EventBus();
    assert.throws(() => bus.setClusterLinkReconnectBackoff({ baseDelayMs: 100 }), RangeError);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_reconnect_backoff_revised' }).length,
      0,
      'failed revisions must not audit',
    );
    const { link } = await makeNode(fx, { nodeId: 'node-a' });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    await bus.disconnectCluster();
    assert.throws(() => bus.setClusterLinkReconnectBackoff({ baseDelayMs: 100 }), RangeError);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_reconnect_backoff_revised' }).length,
      0,
      'a revision after disconnect must not audit',
    );
    void link;
  });

  it('a link built with reconnect: false has no policy to revise or read', async () => {
    const { bus, link } = await makeNode(fx, { nodeId: 'node-a', reconnect: false });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    assert.throws(() => bus.setClusterLinkReconnectBackoff({ baseDelayMs: 100 }), RangeError);
    assert.throws(() => link.getReconnectBackoff(), RangeError);
    assert.throws(() => link.retuneReconnectBackoff({ baseDelayMs: 100 }), RangeError);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_reconnect_backoff_revised' }).length,
      0,
      'a refused revision must not audit',
    );
  });

  it('illegal values fail closed with no audit and the policy untouched', async () => {
    const { bus, link } = await makeNode(fx, {
      nodeId: 'node-a',
      reconnect: { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 250 },
    });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    const before = link.getReconnectBackoff();
    assert.deepEqual(before, { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 250 });
    for (const bad of [
      { baseDelayMs: 0 },
      { baseDelayMs: -1 },
      { baseDelayMs: Number.NaN },
      { baseDelayMs: Number.POSITIVE_INFINITY },
      { baseDelayMs: '100' },
      { maxDelayMs: 0 },
      { maxDelayMs: -5 },
      { maxDelayMs: Number.NaN },
      { maxDelayMs: Number.POSITIVE_INFINITY },
      { maxAttempts: 0 },
      { maxAttempts: -1 },
      { maxAttempts: 1.5 },
      { maxAttempts: Number.NaN },
      { maxAttempts: '10' },
      // One illegal field vetoes the whole revision, including the
      // legal fields riding along with it.
      { baseDelayMs: 50, maxAttempts: 0 },
      { baseDelayMs: 50, maxDelayMs: -1, maxAttempts: 3 },
    ]) {
      assert.throws(() => bus.setClusterLinkReconnectBackoff(bad as never), RangeError);
    }
    // A non-object revision is refused outright.
    for (const bad of [null, undefined, 'fast', 42]) {
      assert.throws(() => bus.setClusterLinkReconnectBackoff(bad as never), RangeError);
    }
    assert.deepEqual(
      link.getReconnectBackoff(),
      { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 250 },
      'failed revisions must leave the live policy untouched',
    );
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_reconnect_backoff_revised' }).length,
      0,
      'failed revisions must not audit',
    );
    // The live policy is untouched: the link still reconnects on it.
    assert.equal(link.getStats().connected, true);
  });

  it('an illegal actor fails closed with no audit and the policy untouched', async () => {
    const { bus, link } = await makeNode(fx, { nodeId: 'node-a' });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    assert.throws(() => bus.setClusterLinkReconnectBackoff({ baseDelayMs: 300 }, ''), RangeError);
    assert.throws(() => bus.setClusterLinkReconnectBackoff({ baseDelayMs: 300 }, 7 as never), RangeError);
    assert.deepEqual(link.getReconnectBackoff(), { baseDelayMs: 20, maxDelayMs: 200, maxAttempts: 50, jitterMs: 250 });
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_reconnect_backoff_revised' }).length,
      0,
    );
  });

  it('a partial revision keeps the unprovided fields and audits old and new', async () => {
    const { bus, link } = await makeNode(fx, {
      nodeId: 'node-a',
      reconnect: { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 250 },
    });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    const connectsBefore = link.getStats().connects;
    const versionBefore = link.getStats().routeVersion;
    bus.setClusterLinkReconnectBackoff({ baseDelayMs: 50 }, 'ops-lead');
    assert.deepEqual(link.getReconnectBackoff(), { baseDelayMs: 50, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 250 });
    const audit = bus.getSubscriptionAudit({ action: 'cluster_link_reconnect_backoff_revised' });
    assert.equal(audit.length, 1);
    assert.equal(audit[0].actor, 'ops-lead');
    assert.deepEqual(audit[0].detail, {
      baseDelayMs: 50,
      maxDelayMs: 5000,
      maxAttempts: 10,
      jitterMs: 250,
      oldBaseDelayMs: 200,
      oldMaxDelayMs: 5000,
      oldMaxAttempts: 10,
      oldJitterMs: 250,
    });
    // No reconnect, no route re-sync: the same connection keeps
    // serving, and the route table is untouched.
    await sleep(150);
    assert.equal(link.getStats().connected, true);
    assert.equal(link.getStats().connects, connectsBefore);
    assert.equal(link.getStats().routeVersion, versionBefore);
    assert.deepEqual(link.getStats().members, ['node-a']);
    // The heartbeat interval is untouched by a backoff revision.
    assert.equal(link.getHeartbeatMs(), 5000);
  });

  it('re-affirming the current policy, and an empty revision, are no-ops with no audit', async () => {
    const { bus, link } = await makeNode(fx, {
      nodeId: 'node-a',
      reconnect: { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 250 },
    });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    bus.setClusterLinkReconnectBackoff({ baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 250 });
    bus.setClusterLinkReconnectBackoff({});
    bus.setClusterLinkReconnectBackoff({ baseDelayMs: undefined, maxDelayMs: undefined });
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_reconnect_backoff_revised' }).length,
      0,
      'a same-policy revision must not audit',
    );
    assert.deepEqual(link.getReconnectBackoff(), { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 250 });
  });

  it('connect-time defaults read back as 200/5000/10', async () => {
    const bus = new EventBus();
    bus.subscribe('t', () => {});
    const link = await bus.connectToHub({ url: `tcp://127.0.0.1:${fx.port}`, nodeId: 'node-a' });
    fx.buses.push(bus);
    fx.links.push(link);
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    assert.deepEqual(link.getReconnectBackoff(), { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 250 });
  });

  it('ClusterLink.retuneReconnectBackoff validates and reads back without a connection', () => {
    const link = new ClusterLink({
      url: 'tcp://127.0.0.1:1',
      onMessage: (_msg: BusMessage) => true,
      getPatterns: () => [],
      reconnect: { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 250 },
    });
    assert.deepEqual(link.getReconnectBackoff(), { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 10, jitterMs: 250 });
    link.retuneReconnectBackoff({ maxAttempts: 3 });
    assert.deepEqual(link.getReconnectBackoff(), { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 3, jitterMs: 250 });
    assert.throws(() => link.retuneReconnectBackoff({ baseDelayMs: 0 }), RangeError);
    assert.throws(() => link.retuneReconnectBackoff({ maxAttempts: 2.5 }), RangeError);
    assert.deepEqual(
      link.getReconnectBackoff(),
      { baseDelayMs: 200, maxDelayMs: 5000, maxAttempts: 3, jitterMs: 250 },
      'a refused retune must leave the stored policy untouched',
    );
  });

  it('ReconnectController.retuneBackoff changes the live delay without touching the attempt count', async () => {
    let attempts = 0;
    const controller = new ReconnectController({
      connectFn: async () => {
        attempts += 1;
        throw new Error('hub down');
      },
      baseDelayMs: 20,
      maxDelayMs: 200,
      maxAttempts: 100,
    });
    try {
      controller.start();
      await waitFor(() => controller.getAttempt() >= 2, 5000, 'two failed attempts');
      const attemptBefore = controller.getAttempt();
      assert.ok(attemptBefore >= 2);
      const attemptsBefore = attempts;
      // Delay for attempt 1 under the old policy: 20ms base + jitter.
      const oldDelay = controller.computeDelayMs(1);
      assert.ok(oldDelay >= 20 && oldDelay < 20 + 250 + 1, `old delay ${oldDelay} out of range`);
      // Raise the cap along with the base: the 200ms cap would
      // otherwise mask the new base entirely.
      controller.retuneBackoff({ baseDelayMs: 5000, maxDelayMs: 20000 });
      assert.equal(controller.getAttempt(), attemptBefore, 'retune must not touch the attempt count');
      assert.deepEqual(controller.getReconnectBackoff(), {
        baseDelayMs: 5000,
        maxDelayMs: 20000,
        maxAttempts: 100,
        jitterMs: 250,
      });
      const newDelay = controller.computeDelayMs(1);
      assert.ok(newDelay >= 5000 && newDelay < 5000 + 250 + 1, `new delay ${newDelay} out of range`);
      // The timer armed under the old policy keeps running at the old
      // delay: the next attempt lands promptly. After that the new
      // policy governs — no further attempt for seconds.
      await waitFor(() => attempts > attemptsBefore, 5000, 'next attempt on the armed timer');
      const stalledAt = controller.getAttempt();
      await sleep(400);
      assert.equal(
        controller.getAttempt(),
        stalledAt,
        'the new policy must stall the following backoff, not the armed one',
      );
    } finally {
      controller.stop();
    }
  });

  it('ReconnectController reads the live maxAttempts on every failure', async () => {
    // Lowering the budget below the current attempt count gives up on
    // the next failure.
    let gaveUpWith: number | null = null;
    const controller = new ReconnectController({
      connectFn: async () => {
        throw new Error('hub down');
      },
      baseDelayMs: 20,
      maxDelayMs: 200,
      maxAttempts: 10,
      onGiveUp: (n) => {
        gaveUpWith = n;
      },
    });
    try {
      controller.start();
      await waitFor(() => controller.getAttempt() >= 2, 5000, 'two failed attempts');
      controller.retuneBackoff({ maxAttempts: 2 });
      await waitFor(() => gaveUpWith !== null, 5000, 'give up');
      assert.equal(gaveUpWith, 2);
      assert.equal(controller.getState(), 'idle');
      assert.equal(controller.getAttempt(), 0);
    } finally {
      controller.stop();
    }

    // Raising the budget extends the runway mid-flight instead.
    let gaveUp2 = false;
    const controller2 = new ReconnectController({
      connectFn: async () => {
        throw new Error('hub down');
      },
      baseDelayMs: 20,
      maxDelayMs: 200,
      maxAttempts: 2,
      onGiveUp: () => {
        gaveUp2 = true;
      },
    });
    try {
      controller2.start();
      await waitFor(() => controller2.getAttempt() >= 1, 5000, 'first failed attempt');
      controller2.retuneBackoff({ maxAttempts: 10 });
      await waitFor(() => controller2.getAttempt() >= 3, 5000, 'third failed attempt past the old budget');
      assert.equal(gaveUp2, false, 'raising the budget must not give up at the old limit');
      assert.deepEqual(controller2.getReconnectBackoff(), {
        baseDelayMs: 20,
        maxDelayMs: 200,
        maxAttempts: 10,
        jitterMs: 250,
      });
    } finally {
      controller2.stop();
    }
  });

  it('a retune during backoff applies on the next connect', async () => {
    // Generous silence bound: the node heartbeats every 5000ms, so a
    // 250ms hub sweep would destroy its socket mid-test and flap the
    // final connected check. The retune under test does not touch it.
    const { hub: fastHub, port } = await makeHub({ heartbeatIntervalMs: 100, heartbeatTimeoutMs: 10000 });
    const fx2: Fixture = { hub: fastHub, port, buses: [], links: [] };
    try {
      const { bus, link } = await makeNode(fx2, {
        nodeId: 'node-a',
        heartbeatMs: 5000,
        reconnect: { baseDelayMs: 20, maxDelayMs: 100, maxAttempts: 100 },
      });
      await waitFor(() => link.getStats().connected, 5000, 'initial connect');
      const connectsBefore = link.getStats().connects;
      // Kill the hub: the link drops into backoff.
      await fastHub.close();
      await waitFor(() => !link.getStats().connected, 5000, 'drop observed');
      // Retune while disconnected: the policy is stored — and audited
      // — for the next backoff cycle.
      bus.setClusterLinkReconnectBackoff({ baseDelayMs: 200, maxDelayMs: 400 });
      assert.deepEqual(link.getReconnectBackoff(), { baseDelayMs: 200, maxDelayMs: 400, maxAttempts: 100, jitterMs: 250 });
      const audit = bus.getSubscriptionAudit({ action: 'cluster_link_reconnect_backoff_revised' });
      assert.equal(audit.length, 1);
      assert.deepEqual(audit[0].detail, {
        baseDelayMs: 200,
        maxDelayMs: 400,
        maxAttempts: 100,
        jitterMs: 250,
        oldBaseDelayMs: 20,
        oldMaxDelayMs: 100,
        oldMaxAttempts: 100,
        oldJitterMs: 250,
      });
      // Bring a NEW hub up on the same port (new epoch).
      const hub2 = new ClusterHub({ host: '127.0.0.1', port, heartbeatIntervalMs: 100, heartbeatTimeoutMs: 10000 });
      await hub2.listen();
      fx2.hub = hub2;
      await waitFor(() => link.getStats().connected, 8000, 'reconnect');
      // Exactly one reconnect: the retune neither reconnects nor
      // re-syncs by itself.
      assert.equal(link.getStats().connects, connectsBefore + 1);
      assert.deepEqual(link.getStats().members, ['node-a']);
      assert.equal(link.getHeartbeatMs(), 5000, 'the heartbeat interval is untouched by a backoff revision');
      assert.deepEqual(link.getReconnectBackoff(), { baseDelayMs: 200, maxDelayMs: 400, maxAttempts: 100, jitterMs: 250 });
      await sleep(300);
      assert.equal(link.getStats().connected, true);
    } finally {
      for (const bus of fx2.buses) await bus.disconnectCluster().catch(() => {});
      await fx2.hub.close().catch(() => {});
    }
  });
});
