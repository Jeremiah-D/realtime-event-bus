import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { ClusterHub, type ClusterLink } from '../src/cluster.ts';

/**
 * ClusterHub silent-member sweep timeout live revision (EB-352):
 * `setHeartbeatTimeout` retunes the hub-side silence bound without
 * rebuilding the sweep timer — the sweeps keep their
 * `heartbeatIntervalMs` cadence and read the live value each round.
 * The hub has no bus audit channel; the live value is observed via
 * `getStats().heartbeatTimeoutMs`.
 *
 * The wire-level tests use a fast-sweeping hub (sweep every 100ms): a
 * node heartbeating every 5000ms goes silent immediately, so a
 * tightened bound sweeps it on the next sweep while a loosened bound
 * spares it past the old deadline — the hub's roster is the observable
 * proof the new timeout is really in use, not just stored.
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

describe('ClusterHub heartbeat timeout live revision (EB-352)', () => {
  it('reports the configured timeout and defaults to three heartbeat intervals', () => {
    const configured = new ClusterHub({ heartbeatIntervalMs: 100, heartbeatTimeoutMs: 250 });
    assert.equal(configured.getStats().heartbeatTimeoutMs, 250);
    const def = new ClusterHub({ heartbeatIntervalMs: 1000 });
    assert.equal(def.getStats().heartbeatTimeoutMs, 3000);
  });

  it('rejects non-positive or non-finite timeouts and leaves the live value untouched', () => {
    const hub = new ClusterHub({ heartbeatIntervalMs: 100, heartbeatTimeoutMs: 300 });
    assert.throws(() => hub.setHeartbeatTimeout(0), RangeError);
    assert.throws(() => hub.setHeartbeatTimeout(-1), RangeError);
    assert.throws(() => hub.setHeartbeatTimeout(Number.NaN), RangeError);
    assert.throws(() => hub.setHeartbeatTimeout(Number.POSITIVE_INFINITY), RangeError);
    assert.throws(() => hub.setHeartbeatTimeout('300' as never), RangeError);
    assert.equal(hub.getStats().heartbeatTimeoutMs, 300);
  });

  it('rejects a timeout at or below the heartbeat interval and leaves the live value untouched', () => {
    const hub = new ClusterHub({ heartbeatIntervalMs: 100, heartbeatTimeoutMs: 300 });
    assert.throws(() => hub.setHeartbeatTimeout(100), RangeError);
    assert.throws(() => hub.setHeartbeatTimeout(50), RangeError);
    assert.equal(hub.getStats().heartbeatTimeoutMs, 300);
    hub.setHeartbeatTimeout(101);
    assert.equal(hub.getStats().heartbeatTimeoutMs, 101);
  });

  it('re-affirming the current timeout is a no-op', () => {
    const hub = new ClusterHub({ heartbeatIntervalMs: 100, heartbeatTimeoutMs: 300 });
    hub.setHeartbeatTimeout(300);
    assert.equal(hub.getStats().heartbeatTimeoutMs, 300);
    // Ordering: the no-op is checked before the interval guard, so a
    // hub built (the constructor never enforced the guard) with a
    // timeout at or below its interval keeps running it when
    // re-affirmed — only a *different* value below the interval throws.
    const misconfigured = new ClusterHub({ heartbeatIntervalMs: 100, heartbeatTimeoutMs: 50 });
    misconfigured.setHeartbeatTimeout(50);
    assert.equal(misconfigured.getStats().heartbeatTimeoutMs, 50);
    assert.throws(() => misconfigured.setHeartbeatTimeout(60), RangeError);
    assert.equal(misconfigured.getStats().heartbeatTimeoutMs, 50);
  });

  it('tightening takes effect from the next sweep: a silent member is dropped', async () => {
    const { hub, port } = await makeHub({ heartbeatIntervalMs: 100, heartbeatTimeoutMs: 10000 });
    const { bus } = await makeSilentNode(port);
    try {
      await waitFor(() => hub.getStats().members === 1, 5000, 'member joins');
      // The hello is ~300ms stale by now — past the tightened bound,
      // so the next sweep (at most one interval away) drops the member.
      await sleep(300);
      hub.setHeartbeatTimeout(200);
      assert.equal(hub.getStats().heartbeatTimeoutMs, 200);
      await waitFor(() => hub.getStats().members === 0, 5000, 'silent member swept');
      await sleep(300);
      assert.equal(hub.getStats().members, 0, 'no reconnect: the sweep stays in effect');
    } finally {
      await bus.disconnectCluster().catch(() => {});
      await hub.close().catch(() => {});
    }
  });

  it('loosening takes effect from the next sweep: a member survives past the old bound', async () => {
    const { hub, port } = await makeHub({ heartbeatIntervalMs: 100, heartbeatTimeoutMs: 500 });
    const { bus, link } = await makeSilentNode(port);
    try {
      await waitFor(() => hub.getStats().members === 1, 5000, 'member joins');
      // Raise the bound well before the old 500ms deadline: the next
      // sweeps read the live value, so the member is spared.
      await sleep(100);
      hub.setHeartbeatTimeout(5000);
      assert.equal(hub.getStats().heartbeatTimeoutMs, 5000);
      // Past the old bound, still a member — and still connected.
      await sleep(700);
      assert.equal(hub.getStats().members, 1, 'the loosened bound must spare the member');
      assert.equal(link.getStats().connected, true);
    } finally {
      await bus.disconnectCluster().catch(() => {});
      await hub.close().catch(() => {});
    }
  });
});
