import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { ClusterHub, type ClusterLink } from '../src/cluster.ts';

/**
 * ClusterHub heartbeat sweep interval live revision (EB-356):
 * `setHeartbeatInterval` retunes how often the hub sweeps silent
 * members — rebuilding the armed sweep timer at the new cadence
 * (a hub that has not started yet just stores the value for
 * `listen()`). The hub has no bus audit channel; the live value is
 * observed via `getStats().heartbeatIntervalMs`.
 *
 * The wire-level tests use a fast-sweeping hub: a node heartbeating
 * every 5000ms goes silent immediately, so the sweep cadence is the
 * observable proof the new interval is really in use, not just
 * stored — tightening the cadence sweeps a silent member far sooner
 * than the old 5000ms cadence ever would, and loosening it keeps a
 * member alive past the sweep that would have dropped it.
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

async function makeHub(
  opts: ConstructorParameters<typeof ClusterHub>[0] = {},
): Promise<{ hub: ClusterHub; port: number }> {
  const hub = new ClusterHub({ host: '127.0.0.1', ...opts });
  const port = await hub.listen();
  return { hub, port };
}

async function makeSilentNode(
  port: number,
): Promise<{ bus: EventBus; link: ClusterLink }> {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  // Heartbeats every 5000ms, no reconnect: the node goes silent right
  // after its hello, and a sweep that drops it stays dropped.
  const link = await bus.connectToHub({
    url: `tcp://127.0.0.1:${port}`,
    nodeId: 'node-a',
    heartbeatMs: 5000,
    reconnect: false,
  });
  return { bus, link };
}

describe('ClusterHub heartbeat interval live revision (EB-356)', () => {
  it('reports the configured interval and defaults to 5000ms', () => {
    const configured = new ClusterHub({ heartbeatIntervalMs: 100, heartbeatTimeoutMs: 300 });
    assert.equal(configured.getStats().heartbeatIntervalMs, 100);
    const def = new ClusterHub({});
    assert.equal(def.getStats().heartbeatIntervalMs, 5000);
    assert.equal(def.getStats().heartbeatTimeoutMs, 15000);
  });

  it('rejects non-positive or non-finite intervals and leaves the live value untouched', () => {
    const hub = new ClusterHub({ heartbeatIntervalMs: 100, heartbeatTimeoutMs: 300 });
    assert.throws(() => hub.setHeartbeatInterval(0), RangeError);
    assert.throws(() => hub.setHeartbeatInterval(-1), RangeError);
    assert.throws(() => hub.setHeartbeatInterval(Number.NaN), RangeError);
    assert.throws(() => hub.setHeartbeatInterval(Number.POSITIVE_INFINITY), RangeError);
    assert.throws(() => hub.setHeartbeatInterval('100' as never), RangeError);
    assert.equal(hub.getStats().heartbeatIntervalMs, 100);
  });

  it('rejects an interval at or above the live timeout and leaves the live value untouched', () => {
    const hub = new ClusterHub({ heartbeatIntervalMs: 100, heartbeatTimeoutMs: 300 });
    assert.throws(() => hub.setHeartbeatInterval(300), RangeError);
    assert.throws(() => hub.setHeartbeatInterval(500), RangeError);
    assert.equal(hub.getStats().heartbeatIntervalMs, 100);
    hub.setHeartbeatInterval(299);
    assert.equal(hub.getStats().heartbeatIntervalMs, 299);
  });

  it('re-affirming the current interval is a no-op', () => {
    const hub = new ClusterHub({ heartbeatIntervalMs: 100, heartbeatTimeoutMs: 300 });
    hub.setHeartbeatInterval(100);
    assert.equal(hub.getStats().heartbeatIntervalMs, 100);
    // Ordering: the no-op is checked before the timeout guard, so a
    // hub built (the constructor never enforced the guard) with an
    // interval at or above its timeout keeps running it when
    // re-affirmed — only a *different* value at or above the timeout
    // throws.
    const misconfigured = new ClusterHub({ heartbeatIntervalMs: 500, heartbeatTimeoutMs: 300 });
    misconfigured.setHeartbeatInterval(500);
    assert.equal(misconfigured.getStats().heartbeatIntervalMs, 500);
    assert.throws(() => misconfigured.setHeartbeatInterval(400), RangeError);
    assert.equal(misconfigured.getStats().heartbeatIntervalMs, 500);
    // Dropping below the timeout is still allowed on such a hub.
    misconfigured.setHeartbeatInterval(299);
    assert.equal(misconfigured.getStats().heartbeatIntervalMs, 299);
  });

  it('tightening rebuilds the armed timer: a silent member is swept on the new cadence', async () => {
    const { hub, port } = await makeHub({ heartbeatIntervalMs: 5000, heartbeatTimeoutMs: 150 });
    const { bus } = await makeSilentNode(port);
    try {
      await waitFor(() => hub.getStats().members === 1, 5000, 'member joins');
      // On the old 5000ms cadence the first sweep would land ~5s
      // out; retuned to 100ms, the member (silent past the 150ms
      // bound by the second sweep) is dropped within ~250ms.
      hub.setHeartbeatInterval(100);
      assert.equal(hub.getStats().heartbeatIntervalMs, 100);
      await waitFor(() => hub.getStats().members === 0, 2000, 'silent member swept on the new cadence');
      await sleep(300);
      assert.equal(hub.getStats().members, 0, 'no reconnect: the sweep stays in effect');
    } finally {
      await bus.disconnectCluster().catch(() => {});
      await hub.close().catch(() => {});
    }
  });

  it('loosening delays the sweep: a member survives past the sweep that would have dropped it', async () => {
    const { hub, port } = await makeHub({ heartbeatIntervalMs: 100, heartbeatTimeoutMs: 2000 });
    const { bus, link } = await makeSilentNode(port);
    try {
      await waitFor(() => hub.getStats().members === 1, 5000, 'member joins');
      // On the old 100ms cadence the member would be swept ~2s
      // after its hello; retuned to 1500ms (still below the 2000ms
      // timeout, so the guard allows it), the first sweep lands
      // 1.5s out and spares it, so the member is still there 2.5s
      // after joining.
      hub.setHeartbeatInterval(1500);
      assert.equal(hub.getStats().heartbeatIntervalMs, 1500);
      await sleep(2500);
      assert.equal(hub.getStats().members, 1, 'the loosened cadence must delay the sweep');
      assert.equal(link.getStats().connected, true);
      // The sweep still happens on the new cadence.
      await waitFor(() => hub.getStats().members === 0, 5000, 'silent member swept on the loosened cadence');
    } finally {
      await bus.disconnectCluster().catch(() => {});
      await hub.close().catch(() => {});
    }
  });

  it('retuning before listen stores the value: listen arms the timer at it', async () => {
    const hub = new ClusterHub({ host: '127.0.0.1', heartbeatIntervalMs: 5000, heartbeatTimeoutMs: 150 });
    hub.setHeartbeatInterval(100);
    assert.equal(hub.getStats().heartbeatIntervalMs, 100);
    const port = await hub.listen();
    const { bus } = await makeSilentNode(port);
    try {
      await waitFor(() => hub.getStats().members === 1, 5000, 'member joins');
      // The timer was armed at 100ms, not the constructed 5000ms:
      // the silent member is swept within ~250ms of joining.
      await waitFor(() => hub.getStats().members === 0, 2000, 'silent member swept on the stored cadence');
    } finally {
      await bus.disconnectCluster().catch(() => {});
      await hub.close().catch(() => {});
    }
  });
});
