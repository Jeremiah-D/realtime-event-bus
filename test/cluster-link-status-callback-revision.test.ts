import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { ClusterHub, ClusterLink, type ClusterLinkStatus } from '../src/cluster.ts';

/**
 * Cluster link status-callback revision (EB-449):
 * `setClusterLinkStatusCallback` swaps the `onStatusChange` observer
 * live — the new callback governs from the next status transition
 * onward (a disconnect or a route update), already-emitted statuses
 * are never replayed, and `null` clears back to silent while the
 * link itself keeps working. `emitStatus` swallows a throwing
 * callback, so a broken observer can never break the link.
 *
 * A route update is triggered by a second node joining: the hub bumps
 * the route version and broadcasts, and the first node's link applies
 * the update and emits a status — the observable proof the swapped
 * callback is really live, not just stored.
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
    onStatusChange?: (status: ClusterLinkStatus) => void;
  } = {},
): Promise<{ bus: EventBus; link: ClusterLink }> {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const link = await bus.connectToHub({
    url: `tcp://127.0.0.1:${fx.port}`,
    nodeId: opts.nodeId,
    heartbeatMs: opts.heartbeatMs,
    reconnect: opts.reconnect ?? { baseDelayMs: 20, maxDelayMs: 200, maxAttempts: 50 },
    onStatusChange: opts.onStatusChange,
  });
  fx.buses.push(bus);
  fx.links.push(link);
  return { bus, link };
}

describe('cluster link status callback revision (EB-449)', () => {
  let fx: Fixture;

  beforeEach(async () => {
    const { hub, port } = await makeHub();
    fx = { hub, port, buses: [], links: [] };
  });

  afterEach(async () => {
    for (const bus of fx.buses) {
      await bus.disconnectCluster().catch(() => {});
      await bus.close().catch(() => {});
    }
    await fx.hub.close().catch(() => {});
  });

  it('throws RangeError with no cluster link, before connect and after disconnect', async () => {
    const bus = new EventBus();
    const cb = () => {};
    assert.throws(() => bus.setClusterLinkStatusCallback(cb), RangeError);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_status_callback_revised' }).length,
      0,
      'failed revisions must not audit',
    );
    const { bus: connected, link } = await makeNode(fx, { nodeId: 'node-a' });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    await connected.disconnectCluster();
    assert.throws(() => connected.setClusterLinkStatusCallback(cb), RangeError);
    assert.equal(
      connected.getSubscriptionAudit({ action: 'cluster_link_status_callback_revised' }).length,
      0,
      'a revision after disconnect must not audit',
    );
  });

  it('a non-function callback fails closed with the live callback untouched', async () => {
    const received: ClusterLinkStatus[] = [];
    const live = (s: ClusterLinkStatus) => received.push(s);
    const { bus, link } = await makeNode(fx, { nodeId: 'node-a', onStatusChange: live });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    // The connect itself already emitted once to the live callback;
    // drain it so only post-revision transitions are counted.
    received.length = 0;
    for (const bad of ['nope', 42, {}, true, undefined]) {
      assert.throws(() => bus.setClusterLinkStatusCallback(bad as never), RangeError);
    }
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_status_callback_revised' }).length,
      0,
      'failed revisions must not audit',
    );
    // The live callback is untouched: the next transition still
    // reaches it.
    await bus.disconnectCluster();
    assert.equal(received.length, 1, 'the live callback must still fire after refused revisions');
    assert.equal(received[0].connected, false);
  });

  it('re-installing the live callback, and clearing when none is set, are no-ops with no audit', async () => {
    const cb = () => {};
    const { bus, link } = await makeNode(fx, { nodeId: 'node-a' });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    // Clearing a link that never had a callback: no-op.
    bus.setClusterLinkStatusCallback(null);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_status_callback_revised' }).length,
      0,
      'clearing an unset callback must not audit',
    );
    bus.setClusterLinkStatusCallback(cb);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_status_callback_revised' }).length,
      1,
    );
    // Same reference again: no-op.
    bus.setClusterLinkStatusCallback(cb);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_status_callback_revised' }).length,
      1,
      'a same-reference reinstall must not audit',
    );
    // Clearing twice: the second clear is a no-op.
    bus.setClusterLinkStatusCallback(null);
    bus.setClusterLinkStatusCallback(null);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_status_callback_revised' }).length,
      2,
      'clearing an already-cleared callback must not audit',
    );
  });

  it('the swapped callback receives the next route update with exact fields; the old one stays silent', async () => {
    const oldReceived: ClusterLinkStatus[] = [];
    const newReceived: ClusterLinkStatus[] = [];
    const { bus, link } = await makeNode(fx, {
      nodeId: 'node-a',
      onStatusChange: (s) => oldReceived.push(s),
    });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    const versionBefore = link.getStats().routeVersion;
    // The connect itself already emitted once to the old callback;
    // drain it so only post-swap transitions are counted.
    oldReceived.length = 0;
    const next = (s: ClusterLinkStatus) => newReceived.push(s);
    bus.setClusterLinkStatusCallback(next);
    // A second node joining bumps the route version and broadcasts —
    // the next status transition for node-a.
    await makeNode(fx, { nodeId: 'node-b' });
    await waitFor(() => newReceived.length > 0, 5000, 'route-update status');
    assert.equal(oldReceived.length, 0, 'the replaced callback must stay silent');
    assert.equal(newReceived.length, 1);
    const status = newReceived[0];
    assert.equal(status.connected, true);
    assert.equal(status.degraded, false);
    assert.equal(status.nodeId, 'node-a');
    assert.equal(status.routeVersion, link.getStats().routeVersion);
    assert.ok(status.routeVersion > versionBefore, 'the route version must have bumped');
    assert.equal(status.hubEpoch, link.getStats().hubEpoch);
    assert.ok(status.hubEpoch !== null, 'a connected node knows the hub epoch');
    assert.deepEqual([...status.members].sort(), ['node-a', 'node-b']);
  });

  it('the swapped callback receives disconnect with exact fields', async () => {
    const received: ClusterLinkStatus[] = [];
    const { bus, link } = await makeNode(fx, { nodeId: 'node-a' });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    bus.setClusterLinkStatusCallback((s) => received.push(s));
    await bus.disconnectCluster();
    assert.equal(received.length, 1);
    // The emitted status is the link's own status at emit time.
    assert.deepEqual(received[0], link.getStatus());
    assert.equal(received[0].connected, false);
    assert.equal(received[0].degraded, false);
    assert.equal(received[0].nodeId, 'node-a');
  });

  it('clearing leaves the link working with no callback', async () => {
    const received: ClusterLinkStatus[] = [];
    const { bus, link } = await makeNode(fx, {
      nodeId: 'node-a',
      onStatusChange: (s) => received.push(s),
    });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    // The connect itself already emitted once; drain it so only
    // post-clear transitions are counted.
    received.length = 0;
    bus.setClusterLinkStatusCallback(null);
    const audit = bus.getSubscriptionAudit({ action: 'cluster_link_status_callback_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, { hasCallback: false, oldHasCallback: true });
    // The link keeps working: a second node joining still updates the
    // route table, with no callback firing.
    const versionBefore = link.getStats().routeVersion;
    await makeNode(fx, { nodeId: 'node-b' });
    await waitFor(() => link.getStats().routeVersion > versionBefore, 5000, 'route update after clear');
    assert.equal(received.length, 0, 'no callback may fire after clearing');
    assert.equal(link.getStats().connected, true);
    assert.deepEqual([...link.getStats().members].sort(), ['node-a', 'node-b']);
  });

  it('a throwing callback is swallowed by emitStatus and the link is unaffected', async () => {
    const { bus, link } = await makeNode(fx, { nodeId: 'node-a' });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    bus.setClusterLinkStatusCallback(() => {
      throw new Error('observer blew up');
    });
    // A route update through the throwing observer still applies.
    const versionBefore = link.getStats().routeVersion;
    await makeNode(fx, { nodeId: 'node-b' });
    await waitFor(() => link.getStats().routeVersion > versionBefore, 5000, 'route update past throwing callback');
    assert.equal(link.getStats().connected, true);
    // And disconnecting through it resolves cleanly.
    await bus.disconnectCluster();
    assert.equal(link.getStats().connected, false);
  });

  it('the audit event is exact and carries the actor; an illegal actor fails closed', async () => {
    const cb = () => {};
    const { bus, link } = await makeNode(fx, { nodeId: 'node-a' });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    assert.throws(() => bus.setClusterLinkStatusCallback(cb, ''), RangeError);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_status_callback_revised' }).length,
      0,
      'an illegal actor must not audit',
    );
    bus.setClusterLinkStatusCallback(cb, 'ops-lead');
    let audit = bus.getSubscriptionAudit({ action: 'cluster_link_status_callback_revised' });
    assert.equal(audit.length, 1);
    assert.equal(audit[0].actor, 'ops-lead');
    assert.deepEqual(audit[0].detail, { hasCallback: true, oldHasCallback: false });
    bus.setClusterLinkStatusCallback(null, 'ops-lead');
    audit = bus.getSubscriptionAudit({ action: 'cluster_link_status_callback_revised' });
    assert.equal(audit.length, 2);
    assert.equal(audit[1].actor, 'ops-lead');
    assert.deepEqual(audit[1].detail, { hasCallback: false, oldHasCallback: true });
  });

  it('a closed bus refuses (EB-154)', async () => {
    const { bus, link } = await makeNode(fx, { nodeId: 'node-a' });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    await bus.close();
    assert.throws(() => bus.setClusterLinkStatusCallback(() => {}), RangeError);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'cluster_link_status_callback_revised' }).length,
      0,
      'a revision on a closed bus must not audit',
    );
  });

  it('heartbeat and reconnect-backoff revisions are orthogonal to the callback revision', async () => {
    const received: ClusterLinkStatus[] = [];
    const { bus, link } = await makeNode(fx, { nodeId: 'node-a', heartbeatMs: 5000 });
    await waitFor(() => link.getStats().connected, 5000, 'connect');
    bus.setClusterLinkStatusCallback((s) => received.push(s));
    // The callback revision leaves heartbeat and backoff untouched
    // (the fixture connects with baseDelayMs 20).
    assert.equal(link.getHeartbeatMs(), 5000);
    assert.equal(link.getReconnectBackoff().baseDelayMs, 20);
    // And heartbeat/backoff revisions leave the callback live.
    bus.setClusterLinkHeartbeat(100);
    bus.setClusterLinkReconnectBackoff({ baseDelayMs: 50 });
    assert.equal(link.getHeartbeatMs(), 100);
    assert.equal(link.getReconnectBackoff().baseDelayMs, 50);
    await bus.disconnectCluster();
    assert.equal(received.length, 1, 'the callback must survive heartbeat/backoff revisions');
    assert.equal(received[0].connected, false);
  });

  it('ClusterLink.setOnStatusChange / getOnStatusChange round-trip without a connection', () => {
    const link = new ClusterLink({
      url: 'tcp://127.0.0.1:1',
      onMessage: (_msg) => true,
      getPatterns: () => [],
      heartbeatMs: 5000,
    });
    assert.equal(link.getOnStatusChange(), undefined);
    const cb = (_s: ClusterLinkStatus) => {};
    link.setOnStatusChange(cb);
    assert.equal(link.getOnStatusChange(), cb);
    link.setOnStatusChange(null);
    assert.equal(link.getOnStatusChange(), undefined);
  });
});
