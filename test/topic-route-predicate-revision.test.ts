import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage, type RoutePredicate } from '../src/bus.ts';

/**
 * Live revision of topic route predicates (EB-459):
 * `reviseTopicRoutePredicate(src, predicate)` swaps a registered
 * route's `RoutePredicate` in place, without re-registering — the
 * fan-out path reads the live route entry on every publish, so the
 * new predicate governs from the next forward while `dst` and the
 * `forwarded` count keep running. `null` clears the predicate back
 * to full forwarding. Re-registering a `src` is still the only way
 * to move `dst`, and it still resets the count — the revision is
 * predicate-only by design.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

function collect() {
  const messages: BusMessage[] = [];
  return {
    messages,
    handler: (msg: BusMessage) => {
      messages.push(msg);
    },
  };
}

describe('live revision of topic route predicates (EB-459)', () => {
  test('swapping the predicate filters the next forward, forwarded count continuous', async () => {
    const bus = new EventBus();
    const high = (payload: unknown) => (payload as { level?: string }).level === 'high';
    bus.setTopicRoute('a', 'b', { predicate: high });
    const dst = collect();
    bus.subscribe('b', dst.handler);
    bus.publish('a', { level: 'high', n: 1 });
    await flush();
    assert.equal(dst.messages.length, 1);
    assert.deepEqual(bus.getStats().routes, [
      { src: 'a', dst: 'b', predicate: true, forwarded: 1 },
    ]);

    // Swap in a predicate that only accepts level "low": the next
    // publish is filtered by the NEW predicate, and the count keeps
    // running from 1 instead of resetting.
    const low = (payload: unknown) => (payload as { level?: string }).level === 'low';
    bus.reviseTopicRoutePredicate('a', low);
    assert.deepEqual(bus.getStats().routes, [
      { src: 'a', dst: 'b', predicate: true, forwarded: 1 },
    ]);
    bus.publish('a', { level: 'high', n: 2 });
    await flush();
    assert.equal(dst.messages.length, 1);
    assert.equal(bus.getStats().routes[0].forwarded, 1);
    bus.publish('a', { level: 'low', n: 3 });
    await flush();
    assert.equal(dst.messages.length, 2);
    assert.deepEqual(dst.messages[1].payload, { level: 'low', n: 3 });
    assert.equal(bus.getStats().routes[0].forwarded, 2);
  });

  test('clearing with null restores full forwarding', async () => {
    const bus = new EventBus();
    bus.setTopicRoute('a', 'b', {
      predicate: (payload) => (payload as { level?: string }).level === 'high',
    });
    const dst = collect();
    bus.subscribe('b', dst.handler);
    bus.publish('a', { level: 'low' });
    await flush();
    assert.equal(dst.messages.length, 0);

    bus.reviseTopicRoutePredicate('a', null);
    assert.deepEqual(bus.getStats().routes, [
      { src: 'a', dst: 'b', predicate: false, forwarded: 0 },
    ]);
    bus.publish('a', { level: 'low' });
    await flush();
    assert.equal(dst.messages.length, 1);
    assert.equal(bus.getStats().routes[0].forwarded, 1);
  });

  test('unknown src throws RangeError and the live table is untouched', () => {
    const bus = new EventBus();
    bus.setTopicRoute('a', 'b');
    const before = bus.getStats().routes;
    assert.throws(
      () => bus.reviseTopicRoutePredicate('nope', () => true),
      (err: unknown) => err instanceof RangeError && /unknown topic route/.test(err.message),
    );
    assert.deepEqual(bus.getStats().routes, before);
  });

  test('illegal predicate values throw RangeError and the live route is untouched', () => {
    const bus = new EventBus();
    const keep = (payload: unknown) => (payload as { ok?: boolean }).ok === true;
    bus.setTopicRoute('a', 'b', { predicate: keep });
    const dst = collect();
    bus.subscribe('b', dst.handler);
    for (const bad of ['yes', 42, {}, undefined] as const) {
      assert.throws(
        () => bus.reviseTopicRoutePredicate('a', bad as never),
        (err: unknown) =>
          err instanceof RangeError && /predicate must be a function or null/.test(err.message),
      );
    }
    // Still the original predicate: a non-matching message is filtered.
    assert.deepEqual(bus.getStats().routes, [
      { src: 'a', dst: 'b', predicate: true, forwarded: 0 },
    ]);
    bus.publish('a', { ok: false });
    bus.publish('a', { ok: true });
    return flush().then(() => {
      assert.equal(dst.messages.length, 1);
      assert.deepEqual(dst.messages[0].payload, { ok: true });
      assert.equal(bus.getStats().routes[0].forwarded, 1);
    });
  });

  test('same-reference revision and clearing a predicate-less route are silent no-ops', async () => {
    const bus = new EventBus();
    const pred: RoutePredicate = () => true;
    bus.setTopicRoute('a', 'b', { predicate: pred });
    const dst = collect();
    bus.subscribe('b', dst.handler);
    bus.publish('a', 'x');
    await flush();
    assert.equal(bus.getStats().routes[0].forwarded, 1);

    bus.reviseTopicRoutePredicate('a', pred);
    assert.deepEqual(bus.getStats().routes, [
      { src: 'a', dst: 'b', predicate: true, forwarded: 1 },
    ]);

    // A route that never had a predicate, cleared: still a no-op.
    bus.setTopicRoute('c', 'd');
    bus.reviseTopicRoutePredicate('c', null);
    assert.deepEqual(bus.getStats().routes, [
      { src: 'a', dst: 'b', predicate: true, forwarded: 1 },
      { src: 'c', dst: 'd', predicate: false, forwarded: 0 },
    ]);
  });

  test('re-registering still resets the forwarded count (control)', async () => {
    const bus = new EventBus();
    bus.setTopicRoute('a', 'b');
    bus.subscribe('b', () => {});
    bus.publish('a', 'x');
    await flush();
    assert.equal(bus.getStats().routes[0].forwarded, 1);

    // The revision path keeps the count; re-registration does not.
    bus.reviseTopicRoutePredicate('a', () => true);
    assert.equal(bus.getStats().routes[0].forwarded, 1);
    bus.setTopicRoute('a', 'b');
    assert.deepEqual(bus.getStats().routes, [
      { src: 'a', dst: 'b', predicate: false, forwarded: 0 },
    ]);
  });

  test('a throwing predicate still propagates to the publish caller', async () => {
    const bus = new EventBus();
    bus.setTopicRoute('a', 'b');
    const dst = collect();
    bus.subscribe('b', dst.handler);
    bus.publish('a', 'ok');
    await flush();
    assert.equal(dst.messages.length, 1);

    bus.reviseTopicRoutePredicate('a', () => {
      throw new Error('predicate boom');
    });
    assert.throws(() => bus.publish('a', 'bad'), /predicate boom/);
    // The source message was admitted and fanned out; only the
    // forward died with the predicate — and the route still stands.
    assert.equal(dst.messages.length, 1);
    assert.equal(bus.getStats().routes[0].forwarded, 1);
    bus.reviseTopicRoutePredicate('a', null);
    bus.publish('a', 'ok-again');
    await flush();
    assert.equal(dst.messages.length, 2);
    assert.equal(bus.getStats().routes[0].forwarded, 2);
  });

  test('dst is untouched and other routes are unaffected', async () => {
    const bus = new EventBus();
    bus.setTopicRoute('a', 'b', { predicate: () => false });
    bus.setTopicRoute('x', 'y', { predicate: () => false });
    const bDst = collect();
    const yDst = collect();
    bus.subscribe('b', bDst.handler);
    bus.subscribe('y', yDst.handler);
    bus.publish('a', 'one');
    bus.publish('x', 'one');
    await flush();
    assert.equal(bDst.messages.length, 0);
    assert.equal(yDst.messages.length, 0);

    // Revise only the a->b predicate: x->y keeps filtering, and a
    // still forwards to b (not anywhere else).
    bus.reviseTopicRoutePredicate('a', () => true);
    bus.publish('a', 'two');
    bus.publish('x', 'two');
    await flush();
    assert.equal(bDst.messages.length, 1);
    assert.equal(bDst.messages[0].topic, 'b');
    assert.equal(yDst.messages.length, 0);
    assert.deepEqual(bus.getStats().routes, [
      { src: 'a', dst: 'b', predicate: true, forwarded: 1 },
      { src: 'x', dst: 'y', predicate: true, forwarded: 0 },
    ]);
  });
});
