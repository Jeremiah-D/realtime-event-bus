import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { connect as tcpConnect } from 'node:net';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { ClusterHub, ClusterLink } from '../src/cluster.ts';

const here = dirname(fileURLToPath(import.meta.url));

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

/**
 * Waits until every link in the fixture sees all `n` members in its
 * cached route table — the publisher's cache is what decides a forward,
 * so tests must wait on the publisher's view, not just the receiver's.
 */
async function waitForMesh(fx: Fixture, n: number): Promise<void> {
  await waitFor(
    () => fx.links.length === n && fx.links.every((l) => l.getStats().members.length === n),
    5000,
    `full mesh of ${n}`,
  );
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
  patterns: Array<{ pattern: string; handler: (msg: BusMessage) => void }>,
  opts: { nodeId?: string; url?: string; reconnect?: false | { baseDelayMs?: number; maxDelayMs?: number; maxAttempts?: number } } = {},
): Promise<{ bus: EventBus; link: ClusterLink }> {
  const bus = new EventBus();
  for (const p of patterns) bus.subscribe(p.pattern, p.handler);
  const link = await bus.connectToHub({
    url: opts.url ?? `tcp://127.0.0.1:${fx.port}`,
    nodeId: opts.nodeId,
    reconnect: opts.reconnect ?? { baseDelayMs: 20, maxDelayMs: 200, maxAttempts: 50 },
  });
  fx.buses.push(bus);
  fx.links.push(link);
  return { bus, link };
}

describe('cluster federation (EB-37)', () => {
  let fx: Fixture;

  beforeEach(async () => {
    const { hub, port } = await makeHub();
    fx = { hub, port, buses: [], links: [] };
  });

  afterEach(async () => {
    for (const bus of fx.buses) await bus.disconnectCluster().catch(() => {});
    await fx.hub.close().catch(() => {});
  });

  it('joins the hub and syncs the route table to every member', async () => {
    const receivedA: BusMessage[] = [];
    const receivedB: BusMessage[] = [];
    const { link: linkA } = await makeNode(fx, [{ pattern: 'orders.*', handler: (m) => receivedA.push(m) }], { nodeId: 'node-a' });
    const { link: linkB } = await makeNode(fx, [{ pattern: 'orders.*', handler: (m) => receivedB.push(m) }], { nodeId: 'node-b' });
    await waitForMesh(fx, 2);
    assert.deepEqual(linkA.getStats().members.sort(), ['node-a', 'node-b']);
    assert.ok(linkA.getStats().routeVersion >= 2);
    assert.equal(linkA.getStats().hubEpoch, linkB.getStats().hubEpoch);
    assert.ok(linkA.getStats().hubEpoch?.startsWith('hub-'));
    assert.equal(fx.hub.getStats().members, 2);
    void receivedA;
    void receivedB;
  });

  it('forwards to the member whose pattern matches, with hub-global seq', async () => {
    const receivedB: BusMessage[] = [];
    const { bus: busA, link: linkA } = await makeNode(fx, [{ pattern: 'orders.*', handler: () => {} }], { nodeId: 'node-a' });
    const { bus: busB, link: linkB } = await makeNode(
      fx,
      [{ pattern: 'orders.*', handler: (m) => receivedB.push(m) }],
      { nodeId: 'node-b' },
    );
    // Wait for the PUBLISHER's route cache: it decides the forward.
    await waitForMesh(fx, 2);
    busA.publish('orders.created', { id: 1 });
    busA.publish('orders.created', { id: 2 });
    await waitFor(() => receivedB.length === 2, 5000, 'remote delivery');
    assert.deepEqual(receivedB.map((m) => m.seq), [1, 2]);
    assert.deepEqual(receivedB.map((m) => m.payload), [{ id: 1 }, { id: 2 }]);
    // Hub seqs are global: the receiving bus never ran its own publish
    // pipeline for the topic, so it has no local topic-stats entry —
    // per-node topicStats describe the node's own publish stream only.
    assert.equal(
      busB.getStats().topics.find((t) => t.topic === 'orders.created'),
      undefined,
    );
    assert.ok(receivedB[0].epoch?.startsWith('hub-'));
    assert.equal(fx.hub.getStats().forwardedMessages, 2);
  });

  it('does not forward when no remote member matches (route-aware, not broadcast)', async () => {
    let countB = 0;
    const { link: linkA } = await makeNode(fx, [{ pattern: 'a.*', handler: () => {} }], { nodeId: 'node-a' });
    await makeNode(fx, [{ pattern: 'b.*', handler: () => { countB += 1; } }], { nodeId: 'node-b' });
    await waitForMesh(fx, 2);
    fx.buses[0].publish('a.1', 'x');
    await sleep(150);
    assert.equal(countB, 0);
    assert.equal(fx.hub.getStats().forwardedMessages, 0);
    assert.equal(linkA.getStats().forwardedMessages, 0);
  });

  it('assigns hub seqs monotonically across publishers', async () => {
    const received: BusMessage[] = [];
    await makeNode(fx, [{ pattern: 't', handler: () => {} }], { nodeId: 'node-a' });
    await makeNode(fx, [{ pattern: 't', handler: () => {} }], { nodeId: 'node-b' });
    const { link: linkC } = await makeNode(fx, [{ pattern: 't', handler: (m) => received.push(m) }], { nodeId: 'node-c' });
    await waitForMesh(fx, 3);
    // Interleave publishes from a and b; the hub serializes them.
    for (let i = 0; i < 5; i++) {
      fx.buses[0].publish('t', `a${i}`);
      fx.buses[1].publish('t', `b${i}`);
    }
    await waitFor(() => received.length === 10, 5000, 'all cluster messages');
    assert.deepEqual(received.map((m) => m.seq), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  });

  it('keeps local seq for local subscribers and hub seq for remote ones', async () => {
    const localA: BusMessage[] = [];
    const remoteB: BusMessage[] = [];
    const { bus: busA, link: linkA } = await makeNode(
      fx,
      [{ pattern: 'orders.*', handler: (m) => localA.push(m) }],
      { nodeId: 'node-a' },
    );
    await makeNode(fx, [{ pattern: 'orders.*', handler: (m) => remoteB.push(m) }], { nodeId: 'node-b' });
    await waitForMesh(fx, 2);
    busA.publish('orders.x', 1);
    busA.publish('orders.x', 2);
    await waitFor(() => localA.length === 2 && remoteB.length === 2, 5000, 'both deliveries');
    // Local stream: node-local numbering starting at 1.
    assert.deepEqual(localA.map((m) => m.seq), [1, 2]);
    assert.ok(localA.every((m) => m.epoch === undefined));
    // Remote stream: hub numbering, also starting at 1 (independent space).
    assert.deepEqual(remoteB.map((m) => m.seq), [1, 2]);
    assert.ok(remoteB.every((m) => m.epoch?.startsWith('hub-')));
  });

  it('does not count phantom gaps when a subscriber sees both epochs', async () => {
    const { bus: busA, link: linkA } = await makeNode(fx, [{ pattern: 'mix.*', handler: () => {} }], { nodeId: 'node-a' });
    const { bus: busB } = await makeNode(fx, [{ pattern: 'zzz-never', handler: () => {} }], { nodeId: 'node-b' });
    await waitForMesh(fx, 2);
    // node-b publishes mix.* (hub epoch on node-a); node-a publishes mix.* locally too.
    busB.publish('mix.1', 'from-b-1');
    busA.publish('mix.1', 'from-a-1');
    busB.publish('mix.1', 'from-b-2');
    busA.publish('mix.1', 'from-a-2');
    await sleep(300);
    const topic = busA.getStats().topics.find((t) => t.topic === 'mix.1');
    assert.ok(topic !== undefined);
    assert.equal(topic.sequenceGaps, 0);
  });

  it('re-announces patterns on subscribe/unsubscribe (route version bumps)', async () => {
    const { bus: busA, link: linkA } = await makeNode(fx, [{ pattern: 'a.*', handler: () => {} }], { nodeId: 'node-a' });
    const { link: linkB } = await makeNode(fx, [{ pattern: 'b.*', handler: () => {} }], { nodeId: 'node-b' });
    await waitForMesh(fx, 2);
    const v0 = linkB.getStats().routeVersion;
    const sub = busA.subscribe('c.*', () => {});
    await waitFor(() => linkB.getStats().routeVersion > v0, 5000, 'version bump on subscribe');
    sub.unsubscribe();
    const v1 = linkB.getStats().routeVersion;
    await waitFor(() => linkB.getStats().routeVersion > v1, 5000, 'version bump on unsubscribe');
    // Subscribing a second subscriber on an already-advertised pattern is a no-op on the wire.
    const v2 = linkB.getStats().routeVersion;
    const sub2 = busA.subscribe('a.*', () => {});
    await sleep(150);
    assert.equal(linkB.getStats().routeVersion, v2);
    sub2.unsubscribe();
    void linkA;
  });

  it('degrades to local-only on transport loss and keeps cached routes', async () => {
    const local: BusMessage[] = [];
    const { bus, link } = await makeNode(
      fx,
      [{ pattern: 'orders.*', handler: (m) => local.push(m) }],
      { nodeId: 'node-a', reconnect: false },
    );
    await waitFor(() => link.getStats().members.length >= 1, 5000, 'initial routes');
    assert.equal(link.getStats().connected, true);
    // Kill the hub: the link must degrade, not throw.
    await fx.hub.close();
    await waitFor(() => link.getStats().connected === false, 5000, 'degraded');
    const st = link.getStats();
    assert.equal(st.degraded, true);
    assert.ok(st.members.length >= 1, 'cached routes retained');
    // Local publish still delivers locally.
    bus.publish('orders.x', 'still-here');
    await sleep(100);
    assert.equal(local.length, 1);
    assert.deepEqual(local[0].payload, 'still-here');
    assert.equal(bus.getStats().cluster?.connected, false);
    assert.equal(bus.getStats().cluster?.degraded, true);
  });

  it('reconnects and re-syncs routes after the hub comes back', async () => {
    const received: BusMessage[] = [];
    const { link } = await makeNode(
      fx,
      [{ pattern: 'orders.*', handler: (m) => received.push(m) }],
      { nodeId: 'node-a', reconnect: { baseDelayMs: 20, maxDelayMs: 100, maxAttempts: 100 } },
    );
    await waitFor(() => link.getStats().connected, 5000, 'initial connect');
    await fx.hub.close();
    await waitFor(() => !link.getStats().connected, 5000, 'drop observed');
    // Bring a NEW hub up on the same port (new epoch).
    const epochBefore = link.getStats().hubEpoch;
    const hub2 = new ClusterHub({ host: '127.0.0.1', port: fx.port });
    await hub2.listen();
    const oldClose = fx.hub.close.bind(fx.hub);
    fx.hub = hub2;
    void oldClose;
    await waitFor(() => link.getStats().connected, 8000, 'reconnect');
    assert.notEqual(link.getStats().hubEpoch, epochBefore);
    assert.ok(link.getStats().routeVersion >= 1);
    assert.ok(link.getStats().connects >= 2);
  });

  it('hub sweeps members that stop heartbeating (abrupt transport death)', async () => {
    const { hub: fastHub, port } = await makeHub({ heartbeatIntervalMs: 40, heartbeatTimeoutMs: 120 });
    const fx2: Fixture = { hub: fastHub, port, buses: [], links: [] };
    try {
      // Raw TCP member: hello, then die without goodbye.
      const sock = tcpConnect({ host: '127.0.0.1', port });
      await new Promise<void>((resolve) => sock.once('connect', resolve));
      const patterns = JSON.stringify(['x.*']);
      const body = Buffer.from(JSON.stringify({ type: 'hello', nodeId: 'raw-1', patterns: JSON.parse(patterns) }), 'utf8');
      const frame = Buffer.allocUnsafe(4 + body.length);
      frame.writeUInt32BE(body.length, 0);
      body.copy(frame, 4);
      sock.write(frame);
      await waitFor(() => fastHub.getStats().members === 1, 3000, 'raw member join');
      const v0 = fastHub.getStats().routeVersion;
      sock.destroy(); // abrupt: no goodbye, no more heartbeats
      await waitFor(() => fastHub.getStats().members === 0, 5000, 'sweep');
      assert.ok(fastHub.getStats().routeVersion > v0, 'sweep bumps the route version');
    } finally {
      for (const bus of fx2.buses) await bus.disconnectCluster().catch(() => {});
      await fastHub.close().catch(() => {});
    }
  });

  it('applies only newer route versions (stale broadcasts are ignored)', async () => {
    const { link } = await makeNode(fx, [{ pattern: 'a.*', handler: () => {} }], { nodeId: 'node-a' });
    await waitFor(() => link.getStats().routeVersion >= 1, 5000, 'routes');
    const v = link.getStats().routeVersion;
    // Inject a stale routes frame directly: same version must not wipe members.
    (link as unknown as { applyRoutes(version: number, epoch: string, members: []): void }).applyRoutes(
      v - 1,
      link.getStats().hubEpoch as string,
      [],
    );
    assert.equal(link.getStats().routeVersion, v);
    assert.deepEqual(link.getStats().members, ['node-a']);
  });

  it('supports TLS hubs', async () => {
    const cert = readFileSync(join(here, 'fixtures', 'cluster-cert.pem'));
    const key = readFileSync(join(here, 'fixtures', 'cluster-key.pem'));
    const { hub: tlsHub, port } = await makeHub({ tls: { key, cert } });
    const received: BusMessage[] = [];
    const busA = new EventBus();
    const busB = new EventBus();
    try {
      busA.subscribe('s.*', () => {});
      busB.subscribe('s.*', (m) => received.push(m));
      const linkA = await busA.connectToHub({
        url: `tls://127.0.0.1:${port}`,
        nodeId: 'tls-a',
        tls: { ca: cert },
        reconnect: false,
      });
      const linkB = await busB.connectToHub({
        url: `tls://127.0.0.1:${port}`,
        nodeId: 'tls-b',
        tls: { ca: cert },
        reconnect: false,
      });
      await waitFor(
        () => linkA.getStats().members.length === 2 && linkB.getStats().members.length === 2,
        5000,
        'tls route sync',
      );
      busA.publish('s.1', 'secret');
      await waitFor(() => received.length === 1, 5000, 'tls delivery');
      assert.deepEqual(received[0].payload, 'secret');
      assert.ok(received[0].epoch?.startsWith('hub-'));
      await busA.disconnectCluster();
      await busB.disconnectCluster();
      void linkA;
      void linkB;
    } finally {
      await tlsHub.close().catch(() => {});
    }
  });

  it('rejects a plain TCP client against a TLS hub', async () => {
    const cert = readFileSync(join(here, 'fixtures', 'cluster-cert.pem'));
    const key = readFileSync(join(here, 'fixtures', 'cluster-key.pem'));
    const { hub: tlsHub, port } = await makeHub({ tls: { key, cert } });
    const bus = new EventBus();
    try {
      await assert.rejects(
        bus.connectToHub({ url: `tcp://127.0.0.1:${port}`, nodeId: 'plain', reconnect: false }),
        /welcome timeout|transport lost|ECONNRESET|socket hang up/i,
      );
      await bus.disconnectCluster().catch(() => {});
    } finally {
      await tlsHub.close().catch(() => {});
    }
  });

  it('orders keyed messages per hub-assigned keySeq on receivers', async () => {
    const received: BusMessage[] = [];
    const { bus: busA, link: linkA } = await makeNode(fx, [{ pattern: 'k.*', handler: () => {} }], { nodeId: 'node-a' });
    await makeNode(fx, [{ pattern: 'k.*', handler: (m) => received.push(m) }], { nodeId: 'node-b' });
    await waitForMesh(fx, 2);
    // Two keys interleaved from one publisher: hub numbers each key independently.
    busA.publish('k.1', 'a1', { key: 'ka' });
    busA.publish('k.1', 'b1', { key: 'kb' });
    busA.publish('k.1', 'a2', { key: 'ka' });
    busA.publish('k.1', 'b2', { key: 'kb' });
    await waitFor(() => received.length === 4, 5000, 'keyed cluster delivery');
    assert.deepEqual(received.map((m) => m.payload), ['a1', 'b1', 'a2', 'b2']);
  });

  it('counts unserializable payloads as forward errors without failing the publish', async () => {
    let local = 0;
    const { bus, link } = await makeNode(fx, [{ pattern: 't', handler: () => { local += 1; } }], { nodeId: 'node-a' });
    await makeNode(fx, [{ pattern: 't', handler: () => {} }], { nodeId: 'node-b' });
    await waitForMesh(fx, 2);
    const accepted = bus.publish('t', { big: 10n });
    assert.equal(accepted, 1, 'local delivery unaffected');
    await sleep(150);
    assert.equal(local, 1);
    assert.equal(link.getStats().forwardErrors, 1);
  });

  it('validates connect options', async () => {
    const bus = new EventBus();
    await assert.rejects(bus.connectToHub({ url: 'http://127.0.0.1:9' }), RangeError);
    await assert.rejects(bus.connectToHub({ url: 'tcp://127.0.0.1' }), RangeError);
    await assert.rejects(bus.connectToHub({ url: 'tcp://127.0.0.1:9', heartbeatMs: 0 }), RangeError);
    // Refused with reconnect:false rejects promptly.
    await assert.rejects(bus.connectToHub({ url: 'tcp://127.0.0.1:1', reconnect: false }), Error);
  });

  it('throws on double connectToHub', async () => {
    const bus = new EventBus();
    const link = await bus.connectToHub({ url: `tcp://127.0.0.1:${fx.port}`, nodeId: 'dup', reconnect: false });
    fx.links.push(link);
    fx.buses.push(bus);
    await assert.rejects(bus.connectToHub({ url: `tcp://127.0.0.1:${fx.port}` }), /already connected/);
    await bus.disconnectCluster();
  });

  it('exposes cluster counters in getStats', async () => {
    const { bus, link } = await makeNode(fx, [{ pattern: 's.*', handler: () => {} }], { nodeId: 'node-a' });
    await makeNode(fx, [{ pattern: 's.*', handler: () => {} }], { nodeId: 'node-b' });
    await waitForMesh(fx, 2);
    bus.publish('s.1', 1);
    await waitFor(() => link.getStats().forwardedMessages === 1, 5000, 'forward counted');
    const cluster = bus.getStats().cluster;
    assert.ok(cluster !== undefined);
    assert.equal(cluster.connected, true);
    assert.equal(cluster.degraded, false);
    assert.equal(cluster.nodeId, 'node-a');
    assert.equal(cluster.forwardedMessages, 1);
    assert.equal(cluster.remoteMembers, 1);
  });

  it('forwards compressed payloads and inflates them on receipt', async () => {
    const rawSeen: unknown[] = [];
    const got: BusMessage[] = [];
    const { bus: busA, link: linkA } = await makeNode(fx, [{ pattern: 'z.*', handler: () => {} }], { nodeId: 'node-a' });
    await makeNode(
      fx,
      [{ pattern: 'z.*', handler: (m) => got.push(m) }],
      { nodeId: 'node-b' },
    );
    const busB = fx.buses[1];
    busB.subscribe('z.raw', () => {}, {
      filter: (payload) => {
        rawSeen.push(payload);
        return true;
      },
    });
    await waitForMesh(fx, 2);
    await sleep(200); // let node-a learn node-b's full pattern set
    busA.setTopicCompression('z.*', { thresholdBytes: 10 });
    busA.setTopicCompression('z.raw', { thresholdBytes: 10 });
    const payload = { data: 'x'.repeat(200) };
    busA.publish('z.1', payload);
    busA.publish('z.raw', payload);
    // 'z.1' and 'z.raw' both match node-b's 'z.*'; 'z.raw' additionally
    // passes through the raw-payload content filter.
    await waitFor(() => got.length === 2 && rawSeen.length === 1, 5000, 'compressed cluster delivery');
    assert.deepEqual(got[0].payload, payload);
    assert.deepEqual(got[1].payload, payload);
    assert.deepEqual(rawSeen[0], payload);
  });
});
