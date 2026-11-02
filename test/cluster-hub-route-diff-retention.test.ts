import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { ClusterHub, type ClusterLink } from '../src/cluster.ts';

/**
 * ClusterHub route-diff retention live revision (EB-353):
 * `setRouteDiffRetention` retunes how many recent incremental diffs
 * the hub keeps in its bounded ring, without touching the route table,
 * the route version, the hub epoch, or the member connections. The hub
 * has no bus audit channel; the live value is observed via
 * `getStats().routeDiffRetention`.
 *
 * Honest note on observability: the ring itself is private and only
 * the latest diff is ever broadcast, so a shrink to a smaller nonzero
 * bound has no wire-visible effect — its effect is the memory bound.
 * The wire-level proof is the 0/non-0 switch: retention `0` makes the
 * next route change broadcast a full snapshot instead of a diff
 * (`routeDiffsSent` stalls, `routeSnapshotsSent` grows), and raising
 * it back above `0` makes diffs flow again. The eager trim inside the
 * setter is what guarantees a shrink to `0` leaves no stale diff in
 * the ring for the next broadcast to consider.
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

async function makeHub(opts: ConstructorParameters<typeof ClusterHub>[0] = {}): Promise<{ hub: ClusterHub; port: number }> {
  const hub = new ClusterHub({ host: '127.0.0.1', ...opts });
  const port = await hub.listen();
  return { hub, port };
}

async function makeNode(
  fx: Fixture,
  patterns: string[],
  nodeId: string,
): Promise<{ bus: EventBus; link: ClusterLink }> {
  const bus = new EventBus();
  for (const p of patterns) bus.subscribe(p, () => {});
  const link = await bus.connectToHub({
    url: `tcp://127.0.0.1:${fx.port}`,
    nodeId,
    reconnect: { baseDelayMs: 20, maxDelayMs: 200, maxAttempts: 50 },
  });
  fx.buses.push(bus);
  fx.links.push(link);
  return { bus, link };
}

/** Waits until every link has applied the hub's current route version. */
async function waitForConvergence(fx: Fixture, label: string): Promise<void> {
  await waitFor(
    () => fx.links.every((l) => l.getStats().routeVersion === fx.hub.getStats().routeVersion),
    5000,
    label,
  );
}

interface TableEntry {
  nodeId: string;
  patterns: string[];
}

/** Canonical form for route-table comparison. */
function normTable(t: TableEntry[]): TableEntry[] {
  return t
    .map((e) => ({ nodeId: e.nodeId, patterns: [...e.patterns].sort() }))
    .sort((a, b) => (a.nodeId < b.nodeId ? -1 : a.nodeId > b.nodeId ? 1 : 0));
}

/** One route churn (a new subscription) followed by convergence. */
async function churn(fx: Fixture, bus: EventBus, pattern: string, label: string): Promise<void> {
  const v0 = fx.hub.getStats().routeVersion;
  bus.subscribe(pattern, () => {});
  await waitFor(
    () =>
      fx.hub.getStats().routeVersion > v0 &&
      fx.links.every((l) => l.getStats().routeVersion === fx.hub.getStats().routeVersion),
    5000,
    label,
  );
}

async function closeFixture(fx: Fixture): Promise<void> {
  for (const bus of fx.buses) await bus.disconnectCluster().catch(() => {});
  await fx.hub.close().catch(() => {});
}

describe('ClusterHub route-diff retention live revision (EB-353)', () => {
  it('reports the configured retention and defaults to 64', () => {
    const configured = new ClusterHub({ routeDiffRetention: 8 });
    assert.equal(configured.getStats().routeDiffRetention, 8);
    const def = new ClusterHub({});
    assert.equal(def.getStats().routeDiffRetention, 64);
  });

  it('rejects non-integer or negative retentions and leaves the live value untouched', () => {
    const hub = new ClusterHub({ routeDiffRetention: 8 });
    assert.throws(() => hub.setRouteDiffRetention(-1), RangeError);
    assert.throws(() => hub.setRouteDiffRetention(1.5), RangeError);
    assert.throws(() => hub.setRouteDiffRetention(Number.NaN), RangeError);
    assert.throws(() => hub.setRouteDiffRetention(Number.POSITIVE_INFINITY), RangeError);
    assert.throws(() => hub.setRouteDiffRetention('8' as never), RangeError);
    assert.equal(hub.getStats().routeDiffRetention, 8);
  });

  it('re-affirming the current retention is a no-op', () => {
    const hub = new ClusterHub({ routeDiffRetention: 8 });
    hub.setRouteDiffRetention(8);
    assert.equal(hub.getStats().routeDiffRetention, 8);
    hub.setRouteDiffRetention(0);
    assert.equal(hub.getStats().routeDiffRetention, 0);
    // Re-affirming 0 is a no-op too — diffs stay off.
    hub.setRouteDiffRetention(0);
    assert.equal(hub.getStats().routeDiffRetention, 0);
  });

  it('revision moves only the retention: table, version, epoch and members are untouched', async () => {
    const { hub, port } = await makeHub({ routeDiffRetention: 8 });
    const fx: Fixture = { hub, port, buses: [], links: [] };
    try {
      await makeNode(fx, ['a.*'], 'ret-a');
      await makeNode(fx, ['b.*'], 'ret-b');
      await waitForConvergence(fx, 'mesh');
      const before = hub.getStats();
      hub.setRouteDiffRetention(3);
      const after = hub.getStats();
      assert.equal(after.routeDiffRetention, 3);
      assert.equal(after.members, before.members);
      assert.equal(after.routeVersion, before.routeVersion);
      assert.equal(after.hubEpoch, before.hubEpoch);
      assert.deepEqual(hub.getRouteTable(), [
        { nodeId: 'ret-a', patterns: ['a.*'] },
        { nodeId: 'ret-b', patterns: ['b.*'] },
      ]);
      // No member is dropped by the revision.
      assert.equal(fx.links.every((l) => l.getStats().connected), true);
    } finally {
      await closeFixture(fx);
    }
  });

  it('shrinking to a smaller nonzero bound keeps diffs flowing and members converged', async () => {
    const { hub, port } = await makeHub({ routeDiffRetention: 64 });
    const fx: Fixture = { hub, port, buses: [], links: [] };
    try {
      const { bus } = await makeNode(fx, ['p.*'], 'shrink-a');
      await makeNode(fx, ['q.*'], 'shrink-b');
      await waitForConvergence(fx, 'mesh');
      await churn(fx, bus, 'p.extra.*', 'churn before shrink');
      assert.ok(hub.getStats().routeDiffsSent > 0, 'diffs flowed before the shrink');
      const diffsBefore = hub.getStats().routeDiffsSent;
      hub.setRouteDiffRetention(2);
      assert.equal(hub.getStats().routeDiffRetention, 2);
      await churn(fx, bus, 'p.more.*', 'churn after shrink');
      assert.ok(
        hub.getStats().routeDiffsSent > diffsBefore,
        'diffs keep flowing after a shrink to a nonzero bound',
      );
      assert.deepEqual(
        fx.links.map((l) => normTable(l.getRouteTable())),
        fx.links.map(() => normTable(hub.getRouteTable())),
      );
    } finally {
      await closeFixture(fx);
    }
  });

  it('lowering to 0 switches diffs off: the next change broadcasts a full snapshot', async () => {
    const { hub, port } = await makeHub({ routeDiffRetention: 64 });
    const fx: Fixture = { hub, port, buses: [], links: [] };
    try {
      const { bus } = await makeNode(fx, ['p.*'], 'off-a');
      await makeNode(fx, ['q.*'], 'off-b');
      await waitForConvergence(fx, 'mesh');
      await churn(fx, bus, 'p.extra.*', 'churn before switch-off');
      assert.ok(hub.getStats().routeDiffsSent > 0, 'diffs flowed while retention was nonzero');
      hub.setRouteDiffRetention(0);
      assert.equal(hub.getStats().routeDiffRetention, 0);
      const diffsBefore = hub.getStats().routeDiffsSent;
      const snapsBefore = hub.getStats().routeSnapshotsSent;
      await churn(fx, bus, 'p.more.*', 'churn after switch-off');
      assert.equal(
        hub.getStats().routeDiffsSent,
        diffsBefore,
        'no diff is sent once retention is 0',
      );
      assert.ok(
        hub.getStats().routeSnapshotsSent > snapsBefore,
        'the change goes out as a full snapshot instead',
      );
      // Members still converge — the 0/non-0 switch changes the frame
      // type, never correctness.
      assert.ok(fx.links.every((l) => l.getStats().routeVersion === hub.getStats().routeVersion));
    } finally {
      await closeFixture(fx);
    }
  });

  it('raising from 0 switches diffs back on from the next change', async () => {
    const { hub, port } = await makeHub({ routeDiffRetention: 0 });
    const fx: Fixture = { hub, port, buses: [], links: [] };
    try {
      const { bus } = await makeNode(fx, ['p.*'], 'on-a');
      await makeNode(fx, ['q.*'], 'on-b');
      await waitForConvergence(fx, 'mesh');
      await churn(fx, bus, 'p.extra.*', 'churn while off');
      assert.equal(hub.getStats().routeDiffsSent, 0, 'no diffs while retention is 0');
      hub.setRouteDiffRetention(64);
      assert.equal(hub.getStats().routeDiffRetention, 64);
      await churn(fx, bus, 'p.more.*', 'churn after switch-on');
      assert.ok(hub.getStats().routeDiffsSent > 0, 'diffs flow again once retention is nonzero');
      assert.ok(fx.links.every((l) => l.getStats().routeVersion === hub.getStats().routeVersion));
    } finally {
      await closeFixture(fx);
    }
  });

  it('a full 0 -> nonzero -> 0 round-trip keeps the mesh converged', async () => {
    const { hub, port } = await makeHub({ routeDiffRetention: 64 });
    const fx: Fixture = { hub, port, buses: [], links: [] };
    try {
      const { bus } = await makeNode(fx, ['p.*'], 'rt-a');
      await makeNode(fx, ['q.*'], 'rt-b');
      await waitForConvergence(fx, 'mesh');
      hub.setRouteDiffRetention(0);
      await churn(fx, bus, 'p.one.*', 'churn at 0');
      hub.setRouteDiffRetention(4);
      await churn(fx, bus, 'p.two.*', 'churn back on');
      hub.setRouteDiffRetention(0);
      await churn(fx, bus, 'p.three.*', 'churn at 0 again');
      assert.equal(hub.getStats().routeDiffRetention, 0);
      assert.ok(fx.links.every((l) => l.getStats().routeVersion === hub.getStats().routeVersion));
      assert.deepEqual(
        fx.links.map((l) => normTable(l.getRouteTable())),
        fx.links.map(() => normTable(hub.getRouteTable())),
      );
    } finally {
      await closeFixture(fx);
    }
  });

  it('a member joining after a switch to 0 still receives the full snapshot', async () => {
    const { hub, port } = await makeHub({ routeDiffRetention: 64 });
    const fx: Fixture = { hub, port, buses: [], links: [] };
    try {
      await makeNode(fx, ['p.*'], 'join-a');
      await waitForConvergence(fx, 'first member');
      hub.setRouteDiffRetention(0);
      const { link } = await makeNode(fx, ['q.*'], 'join-b');
      await waitForConvergence(fx, 'second member joins at 0');
      assert.equal(link.getStats().routeVersion, hub.getStats().routeVersion);
      assert.deepEqual(normTable(link.getRouteTable()), normTable(hub.getRouteTable()));
      assert.equal(hub.getStats().routeDiffsSent, 0);
    } finally {
      await closeFixture(fx);
    }
  });

  it('repeated revisions in a row are all honoured, last one wins', () => {
    const hub = new ClusterHub({ routeDiffRetention: 64 });
    hub.setRouteDiffRetention(10);
    hub.setRouteDiffRetention(1);
    hub.setRouteDiffRetention(0);
    hub.setRouteDiffRetention(33);
    assert.equal(hub.getStats().routeDiffRetention, 33);
  });
});
