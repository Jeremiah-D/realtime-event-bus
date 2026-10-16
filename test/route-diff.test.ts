import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect } from 'node:net';
import { EventBus } from '../src/bus.ts';
import { ClusterHub, ClusterLink } from '../src/cluster.ts';

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

/** Deterministic PRNG (mulberry32) so the fuzz run is reproducible. */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
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
  opts: { nodeId?: string; reconnect?: false | { baseDelayMs?: number; maxDelayMs?: number; maxAttempts?: number } } = {},
): Promise<{ bus: EventBus; link: ClusterLink }> {
  const bus = new EventBus();
  for (const p of patterns) bus.subscribe(p, () => {});
  const link = await bus.connectToHub({
    url: `tcp://127.0.0.1:${fx.port}`,
    nodeId: opts.nodeId,
    reconnect: opts.reconnect ?? { baseDelayMs: 20, maxDelayMs: 200, maxAttempts: 50 },
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

function injectFrame(link: ClusterLink, frame: unknown): void {
  (link as unknown as { handleNodeFrame(frame: unknown): void }).handleNodeFrame(frame);
}

describe('cluster route diffs (EB-121)', () => {
  let fx: Fixture;

  beforeEach(async () => {
    const { hub, port } = await makeHub();
    fx = { hub, port, buses: [], links: [] };
  });

  afterEach(async () => {
    for (const bus of fx.buses) await bus.disconnectCluster().catch(() => {});
    await fx.hub.close().catch(() => {});
  });

  it('fuzz: random subscribe/unsubscribe churn keeps every member table equal to hub truth', async () => {
    const rand = mulberry32(121);
    const pool = ['fz.a.*', 'fz.b.*', 'fz.c.*', 'fz.d.*', 'fz.e.*', 'fz.f.*', 'exact.one', 'exact.two'];
    interface FuzzNode {
      bus: EventBus;
      patterns: Set<string>;
      unsubs: Map<string, () => void>;
    }
    const nodes: FuzzNode[] = [];
    // 3 members with overlapping initial patterns.
    for (let i = 0; i < 3; i++) {
      const { bus, link } = await makeNode(fx, [], { nodeId: `fz-${i}` });
      void link;
      const patterns = new Set<string>();
      const unsubs = new Map<string, () => void>();
      for (let k = 0; k < 2; k++) {
        const p = pool[Math.floor(rand() * pool.length)];
        if (!patterns.has(p)) {
          const sub = bus.subscribe(p, () => {});
          patterns.add(p);
          unsubs.set(p, () => sub.unsubscribe());
        }
      }
      nodes.push({ bus, patterns, unsubs });
    }
    await waitForConvergence(fx, 'initial mesh');

    for (let op = 0; op < 150; op++) {
      const n = nodes[Math.floor(rand() * nodes.length)];
      const v0 = fx.hub.getStats().routeVersion;
      if (n.patterns.size > 0 && rand() < 0.45) {
        // Unsubscribe a random live pattern.
        const arr = [...n.patterns];
        const p = arr[Math.floor(rand() * arr.length)];
        n.unsubs.get(p)?.();
        n.unsubs.delete(p);
        n.patterns.delete(p);
      } else {
        // Subscribe a pattern the node does not have yet.
        const fresh = pool.filter((p) => !n.patterns.has(p));
        if (fresh.length === 0) continue;
        const p = fresh[Math.floor(rand() * fresh.length)];
        const sub = n.bus.subscribe(p, () => {});
        n.patterns.add(p);
        n.unsubs.set(p, () => sub.unsubscribe());
      }
      // The op must have bumped the hub version (no no-op re-announces:
      // the harness never re-subscribes a live pattern), and every
      // member must apply the resulting diff before the next op.
      await waitFor(
        () =>
          fx.hub.getStats().routeVersion > v0 &&
          fx.links.every((l) => l.getStats().routeVersion === fx.hub.getStats().routeVersion),
        5000,
        `churn op ${op}`,
      );
    }

    // Steady-state changes actually travelled as diffs, not full frames.
    assert.ok(fx.hub.getStats().routeDiffsSent > 0, 'expected diff frames to be broadcast');
    const truth = normTable(fx.hub.getRouteTable());
    assert.deepEqual(
      truth,
      normTable(nodes.map((n, i) => ({ nodeId: `fz-${i}`, patterns: [...n.patterns] }))),
      'hub truth matches the harness expectation',
    );
    for (const link of fx.links) {
      assert.deepEqual(normTable(link.getRouteTable()), truth, `member ${link.getStats().nodeId} converged`);
    }
  });

  it('recovers via full snapshot when diffs are missed (sync requests coalesced)', async () => {
    const { link: linkB } = await makeNode(fx, ['b.*'], { nodeId: 'node-b' });
    await makeNode(fx, ['a.*'], { nodeId: 'node-a' });
    await waitForConvergence(fx, 'mesh');
    const v = linkB.getStats().routeVersion;
    const epoch = linkB.getStats().hubEpoch as string;

    // Two gap diffs injected back-to-back: the member must detect the gap
    // and send exactly one sync request (coalesced while in flight).
    const gap = { type: 'routes-diff', baseVersion: v + 5, version: v + 6, hubEpoch: epoch, added: [], removed: [] };
    injectFrame(linkB, gap);
    injectFrame(linkB, gap);
    assert.equal(linkB.getStats().routeSyncRequests, 1, 'one sync request for the gap burst');
    assert.equal(linkB.getStats().routeVersion, v, 'gap diff never applied');

    // The hub answers with the authoritative full snapshot.
    await waitFor(() => fx.hub.getStats().routeSyncRequests === 1, 5000, 'hub serves sync');
    assert.equal(fx.hub.getStats().routeSyncRequests, 1);
    assert.deepEqual(normTable(linkB.getRouteTable()), normTable(fx.hub.getRouteTable()));
  });

  it('never applies an out-of-order diff (future baseVersion is ignored)', async () => {
    const { link: linkB } = await makeNode(fx, ['b.*'], { nodeId: 'node-b' });
    await makeNode(fx, ['a.*'], { nodeId: 'node-a' });
    await waitForConvergence(fx, 'mesh');
    const v = linkB.getStats().routeVersion;
    const epoch = linkB.getStats().hubEpoch as string;

    // A diff from the "future" carrying a bogus member: must be ignored,
    // never merged into the cached table.
    injectFrame(linkB, {
      type: 'routes-diff',
      baseVersion: v + 3,
      version: v + 4,
      hubEpoch: epoch,
      added: [{ nodeId: 'ghost', patterns: ['g.*'] }],
      removed: [],
    });
    assert.ok(
      !linkB.getRouteTable().some((e) => e.nodeId === 'ghost'),
      'out-of-order diff must not inject members',
    );
    assert.equal(linkB.getStats().routeVersion, v);

    // Gap recovery rebuilds from the true snapshot; the ghost never lands.
    await waitFor(() => linkB.getStats().routeVersion === fx.hub.getStats().routeVersion, 5000, 'recovery');
    const table = normTable(linkB.getRouteTable());
    assert.deepEqual(table, normTable(fx.hub.getRouteTable()));
    assert.ok(!table.some((e) => e.nodeId === 'ghost'));
  });

  it('rejects diffs from a stale hub epoch and rebuilds on the new one', async () => {
    const { bus, link } = await makeNode(
      fx,
      ['e.*'],
      { nodeId: 'node-e', reconnect: { baseDelayMs: 20, maxDelayMs: 100, maxAttempts: 100 } },
    );
    await waitFor(() => link.getStats().connected, 5000, 'initial connect');
    const epoch1 = link.getStats().hubEpoch as string;

    // Restart the hub on the same port: new epoch, version space reset.
    await fx.hub.close();
    const hub2 = new ClusterHub({ host: '127.0.0.1', port: fx.port });
    await hub2.listen();
    fx.hub = hub2;
    await waitFor(() => link.getStats().connected && link.getStats().hubEpoch !== epoch1, 8000, 'reconnect on new epoch');
    assert.ok((link.getStats().hubEpoch as string).startsWith('hub-'));

    // Churn on the new epoch converges via diffs.
    const v0 = fx.hub.getStats().routeVersion;
    bus.subscribe('e.new.*', () => {});
    await waitFor(
      () =>
        fx.hub.getStats().routeVersion > v0 &&
        link.getStats().routeVersion === fx.hub.getStats().routeVersion,
      5000,
      'post-restart churn',
    );
    assert.deepEqual(normTable(link.getRouteTable()), normTable(fx.hub.getRouteTable()));

    // A diff stamped with the OLD epoch is meaningless (its baseVersion
    // belongs to a dead version space): ignored, never applied.
    const cur = link.getStats().routeVersion;
    const syncsBefore = link.getStats().routeSyncRequests;
    injectFrame(link, {
      type: 'routes-diff',
      baseVersion: cur,
      version: cur + 1,
      hubEpoch: epoch1,
      added: [{ nodeId: 'ghost', patterns: ['g.*'] }],
      removed: [],
    });
    assert.equal(link.getStats().routeVersion, cur, 'stale-epoch diff not applied');
    assert.ok(!link.getRouteTable().some((e) => e.nodeId === 'ghost'));
    assert.equal(link.getStats().routeSyncRequests, syncsBefore + 1, 'stale epoch triggers a sync');
    // The sync answer is a full snapshot on the NEW epoch: table intact.
    await sleep(200);
    assert.deepEqual(normTable(link.getRouteTable()), normTable(fx.hub.getRouteTable()));
  });

  it('serves full snapshots to members without the routesDiff capability', async () => {
    // Raw legacy member: hellos without the capability flag.
    const sock = tcpConnect({ host: '127.0.0.1', port: fx.port });
    await new Promise<void>((resolve) => sock.once('connect', resolve));
    const frames: Array<{ type: string }> = [];
    let pending = Buffer.alloc(0);
    sock.on('data', (chunk: Buffer) => {
      pending = Buffer.concat([pending, chunk]);
      for (;;) {
        if (pending.length < 4) return;
        const len = pending.readUInt32BE(0);
        if (pending.length < 4 + len) return;
        frames.push(JSON.parse(pending.subarray(4, 4 + len).toString('utf8')));
        pending = pending.subarray(4 + len);
      }
    });
    const writeFrame = (f: unknown): void => {
      const body = Buffer.from(JSON.stringify(f), 'utf8');
      const head = Buffer.allocUnsafe(4);
      head.writeUInt32BE(body.length, 0);
      sock.write(Buffer.concat([head, body]));
    };
    let sockClosed = false;
    sock.on('close', () => {
      sockClosed = true;
    });
    writeFrame({ type: 'hello', nodeId: 'legacy-1', patterns: ['l.*'] });
    await waitFor(() => frames.some((f) => f.type === 'welcome'), 3000, 'legacy welcome');
    void sockClosed;

    // A modern member joins and churns: diffs must flow to it while the
    // legacy member keeps receiving full snapshots.
    const { bus } = await makeNode(fx, ['m.*'], { nodeId: 'modern-1' });
    await waitForConvergence(fx, 'mesh with legacy member');
    const v0 = fx.hub.getStats().routeVersion;
    const sub = bus.subscribe('m.extra.*', () => {});
    await waitFor(
      () =>
        fx.hub.getStats().routeVersion > v0 &&
        fx.links.every((l) => l.getStats().routeVersion === fx.hub.getStats().routeVersion),
      5000,
      'churn with legacy member',
    );
    sub.unsubscribe();
    await sleep(200);

    assert.ok(fx.hub.getStats().routeDiffsSent > 0, 'diffs flowed to the capable member');
    const routeFrames = frames.filter((f) => f.type === 'routes' || f.type === 'routes-diff');
    assert.ok(routeFrames.length > 0, 'legacy member received route updates');
    assert.ok(
      routeFrames.every((f) => f.type === 'routes'),
      'legacy member only ever sees full snapshots',
    );
    sock.destroy();
  });

  it('validates the routeDiffRetention option', () => {
    assert.throws(() => new ClusterHub({ routeDiffRetention: -1 }), RangeError);
    assert.throws(() => new ClusterHub({ routeDiffRetention: 1.5 }), RangeError);
    assert.throws(() => new ClusterHub({ routeDiffRetention: NaN }), RangeError);
    // 0 disables diffs: steady-state changes fall back to full snapshots.
    const hub = new ClusterHub({ routeDiffRetention: 0 });
    assert.equal(hub.getStats().routeDiffRetention, 0);
  });

  it('routeDiffRetention: 0 disables diffs (full snapshots only)', async () => {
    const { hub: plainHub, port } = await makeHub({ routeDiffRetention: 0 });
    const fx2: Fixture = { hub: plainHub, port, buses: [], links: [] };
    try {
      const { bus } = await makeNode(fx2, ['p.*'], { nodeId: 'plain-a' });
      await makeNode(fx2, ['q.*'], { nodeId: 'plain-b' });
      await waitForConvergence(fx2, 'mesh');
      const v0 = plainHub.getStats().routeVersion;
      bus.subscribe('p.extra.*', () => {});
      await waitFor(
        () =>
          plainHub.getStats().routeVersion > v0 &&
          fx2.links.every((l) => l.getStats().routeVersion === plainHub.getStats().routeVersion),
        5000,
        'churn without diffs',
      );
      assert.equal(plainHub.getStats().routeDiffsSent, 0);
      assert.ok(plainHub.getStats().routeSnapshotsSent > 0);
      for (const link of fx2.links) {
        assert.deepEqual(normTable(link.getRouteTable()), normTable(plainHub.getRouteTable()));
      }
    } finally {
      for (const bus of fx2.buses) await bus.disconnectCluster().catch(() => {});
      await plainHub.close().catch(() => {});
    }
  });
});
