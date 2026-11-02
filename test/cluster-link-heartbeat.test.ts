import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { ClusterHub, ClusterLink } from '../src/cluster.ts';

/**
 * Cluster link heartbeat interval revision (EB-339):
 * `setClusterLinkHeartbeat` retunes the node-side heartbeat send
 * interval without reconnecting — the armed timer is re-armed at the
 * new interval when connected, and a revision made during backoff is
 * stored for the next `startHeartbeat`. The hub-side silence bound
 * (`heartbeatTimeoutMs`) is untouched and unvalidated.
 *
 * The wire-level tests use a fast-sweeping hub (sweep every 100ms,
 * silence bound 250ms): a node heartbeating every 5000ms is swept
 * within ~350ms, while a node retuned to 50ms stays a member — the
 * hub's roster is the observable proof the new interval is really in
 * use, not just stored.
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
    reconnect: opts.reconnect ?? { baseDelayMs: 20, maxDelayMs: 200, maxAttempts: 50 },
  });
  fx.buses.push(bus);
  fx.links.push(link);
  return { bus, link };
}

describe('cluster link heartbeat revision (EB-339)', () => {
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
    assert.throws(() => bus.setClusterLinkHeartbeat(100), RangeError);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_heartbeat_revised' }).length,
      0,
      'failed revisions must not audit',
    );
    const { link } = await makeNode(fx, { nodeId: 'node-a' });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    await bus.disconnectCluster();
    assert.throws(() => bus.setClusterLinkHeartbeat(100), RangeError);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_heartbeat_revised' }).length,
      0,
      'a revision after disconnect must not audit',
    );
    void link;
  });

  it('illegal intervals fail closed with no audit and the interval untouched', async () => {
    const { bus, link } = await makeNode(fx, { nodeId: 'node-a', heartbeatMs: 5000 });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    assert.equal(link.getHeartbeatMs(), 5000);
    for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, '100', undefined]) {
      assert.throws(() => bus.setClusterLinkHeartbeat(bad as never), RangeError);
    }
    assert.equal(link.getHeartbeatMs(), 5000, 'failed revisions must leave the live interval untouched');
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_heartbeat_revised' }).length,
      0,
      'failed revisions must not audit',
    );
    // The live interval is untouched: the link still heartbeats at it.
    await sleep(150);
    assert.equal(link.getStats().connected, true);
  });

  it('re-affirming the current interval is a no-op with no audit', async () => {
    const { bus, link } = await makeNode(fx, { nodeId: 'node-a', heartbeatMs: 100 });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    bus.setClusterLinkHeartbeat(100);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_heartbeat_revised' }).length,
      0,
      'a same-interval revision must not audit',
    );
    assert.equal(link.getHeartbeatMs(), 100);
  });

  it('a connected retune is audited, re-arms the timer, and keeps the link connected', async () => {
    const { bus, link } = await makeNode(fx, { nodeId: 'node-a', heartbeatMs: 5000 });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    const versionBefore = link.getStats().routeVersion;
    const connectsBefore = link.getStats().connects;
    bus.setClusterLinkHeartbeat(100);
    const audit = bus.getSubscriptionAudit({ action: 'cluster_link_heartbeat_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, { heartbeatMs: 100, oldHeartbeatMs: 5000 });
    assert.equal(link.getHeartbeatMs(), 100);
    // No reconnect, no route re-sync: the same connection keeps
    // serving, and the route table is untouched.
    await sleep(150);
    assert.equal(link.getStats().connected, true);
    assert.equal(link.getStats().connects, connectsBefore);
    assert.equal(link.getStats().routeVersion, versionBefore);
    assert.deepEqual(link.getStats().members, ['node-a']);
  });

  it('ClusterLink.retuneHeartbeat validates and reads back without a connection', () => {
    const link = new ClusterLink({
      url: 'tcp://127.0.0.1:1',
      onMessage: (_msg) => true,
      getPatterns: () => [],
      heartbeatMs: 5000,
    });
    assert.equal(link.getHeartbeatMs(), 5000);
    link.retuneHeartbeat(100);
    assert.equal(link.getHeartbeatMs(), 100);
    assert.throws(() => link.retuneHeartbeat(0), RangeError);
    assert.throws(() => link.retuneHeartbeat(Number.NaN), RangeError);
    assert.equal(link.getHeartbeatMs(), 100, 'a refused retune must leave the stored interval untouched');
  });

  it('a connected retune takes effect on the wire: the hub keeps the retuned member and sweeps the unretuned one', async () => {
    const { hub: fastHub, port } = await makeHub({ heartbeatIntervalMs: 100, heartbeatTimeoutMs: 250 });
    const fx2: Fixture = { hub: fastHub, port, buses: [], links: [] };
    try {
      // Both nodes join heartbeating every 5000ms — far past the
      // hub's 250ms silence bound, so both would be swept.
      const { bus: busA, link: linkA } = await makeNode(fx2, {
        nodeId: 'node-a',
        heartbeatMs: 5000,
        reconnect: false,
      });
      const { link: linkB } = await makeNode(fx2, {
        nodeId: 'node-b',
        heartbeatMs: 5000,
        reconnect: false,
      });
      await waitFor(() => fastHub.getStats().members === 2, 3000, 'both members join');
      // Retune only A to 50ms: its heartbeats keep it a member, while
      // B goes silent and is swept within ~350ms.
      busA.setClusterLinkHeartbeat(50);
      assert.equal(linkA.getHeartbeatMs(), 50);
      assert.equal(linkB.getHeartbeatMs(), 5000);
      await waitFor(() => fastHub.getStats().members === 1, 5000, 'unretuned member swept');
      assert.equal(fastHub.getStats().members, 1);
      assert.equal(linkA.getStats().connected, true);
      // The hub-side sweep lands first; the swept node observes the
      // transport loss a tick later.
      await waitFor(() => linkB.getStats().connected === false, 5000, 'swept link observes the drop');
      assert.equal(linkB.getStats().connected, false);
      await sleep(400);
      assert.equal(fastHub.getStats().members, 1, 'the retuned member must stay a member');
      assert.equal(linkA.getStats().connected, true);
    } finally {
      for (const bus of fx2.buses) await bus.disconnectCluster().catch(() => {});
      await fastHub.close().catch(() => {});
    }
  });

  it('a retune during backoff applies on the next connect', async () => {
    const { hub: fastHub, port } = await makeHub({ heartbeatIntervalMs: 100, heartbeatTimeoutMs: 250 });
    const fx2: Fixture = { hub: fastHub, port, buses: [], links: [] };
    try {
      const { bus, link } = await makeNode(fx2, {
        nodeId: 'node-a',
        heartbeatMs: 5000,
        reconnect: { baseDelayMs: 20, maxDelayMs: 100, maxAttempts: 100 },
      });
      await waitFor(() => link.getStats().connected, 5000, 'initial connect');
      // Kill the hub: the link drops into backoff.
      await fastHub.close();
      await waitFor(() => !link.getStats().connected, 5000, 'drop observed');
      // Retune while disconnected: no armed timer, so the value is
      // stored — and audited — for the next startHeartbeat.
      bus.setClusterLinkHeartbeat(50);
      assert.equal(link.getHeartbeatMs(), 50);
      const audit = bus.getSubscriptionAudit({ action: 'cluster_link_heartbeat_revised' });
      assert.equal(audit.length, 1);
      assert.deepEqual(audit[0].detail, { heartbeatMs: 50, oldHeartbeatMs: 5000 });
      // Bring a NEW hub up on the same port (new epoch).
      const hub2 = new ClusterHub({ host: '127.0.0.1', port, heartbeatIntervalMs: 100, heartbeatTimeoutMs: 250 });
      await hub2.listen();
      fx2.hub = hub2;
      await waitFor(() => link.getStats().connected, 8000, 'reconnect');
      // With the old 5000ms interval the hub would sweep the member
      // within ~350ms of the welcome; on the retuned 50ms cadence it
      // stays a member.
      await sleep(800);
      assert.equal(hub2.getStats().members, 1);
      assert.equal(link.getStats().connected, true);
      assert.equal(link.getHeartbeatMs(), 50);
    } finally {
      for (const bus of fx2.buses) await bus.disconnectCluster().catch(() => {});
      await fx2.hub.close().catch(() => {});
    }
  });
});
