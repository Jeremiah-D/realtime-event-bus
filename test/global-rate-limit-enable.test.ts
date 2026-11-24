/**
 * Bus-level global publish rate-limit live enable (EB-551):
 * `setGlobalPublishRateLimit` on a bus that never enabled
 * `globalPublishRateLimit` installs the limiter live — no restart, no
 * lost subscriptions — with the revision resolved through the
 * construction-time rule (`resolveGlobalPublishRateLimit`:
 * `messagesPerSecond` is required, `burst` defaults to
 * `max(1, ceil(messagesPerSecond))`). The fresh bucket starts full,
 * so the first `burst` publishes pass at once and publishes before
 * the enable were never charged. The enable is audited like any
 * revision, with `oldMessagesPerSecond` / `oldBurst` `null`. `{}` cannot
 * enable, `null` is a silent no-op, and a closed bus refuses the
 * enable and the revision. The enable is live-only: a restart
 * restores the unconfigured state.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Injectable clock so bucket refills are deterministic. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

const flush = () => new Promise((resolve) => setImmediate(resolve));

describe('global publish rate-limit live enable (EB-551)', () => {
  it('enables with { messagesPerSecond } and defaults burst to max(1, ceil(rate))', async () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    assert.equal(bus.getStats().globalPublishRateLimit, null);
    bus.setGlobalPublishRateLimit({ messagesPerSecond: 2 });
    assert.deepEqual(bus.getStats().globalPublishRateLimit, {
      messagesPerSecond: 2,
      burst: 2,
    });
    const audit = bus.getSubscriptionAudit({ action: 'global_publish_rate_limit_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      messagesPerSecond: 2,
      oldMessagesPerSecond: null,
      burst: 2,
      oldBurst: null,
    });
    await bus.close();
  });

  it('enables with explicit rate and burst, audited precisely', async () => {
    const clock = makeClock();
    const received: unknown[] = [];
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', (msg) => received.push(msg.payload));
    bus.setGlobalPublishRateLimit({ messagesPerSecond: 1, burst: 5 });
    assert.deepEqual(bus.getStats().globalPublishRateLimit, {
      messagesPerSecond: 1,
      burst: 5,
    });
    const audit = bus.getSubscriptionAudit({ action: 'global_publish_rate_limit_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      messagesPerSecond: 1,
      oldMessagesPerSecond: null,
      burst: 5,
      oldBurst: null,
    });
    // The fresh bucket starts full: exactly 5 pass, the 6th sheds.
    for (let i = 0; i < 5; i++) assert.equal(bus.publish('t', i), 1);
    assert.equal(bus.publish('t', 'over'), 0);
    await flush();
    assert.equal(received.length, 5);
    await bus.close();
  });

  it('sheds over-budget publishes after the enable and counts them', async () => {
    const clock = makeClock();
    const received: unknown[] = [];
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', (msg) => received.push(msg.payload));
    bus.setGlobalPublishRateLimit({ messagesPerSecond: 1, burst: 2 });
    assert.equal(bus.publish('t', 'a'), 1);
    assert.equal(bus.publish('t', 'b'), 1);
    assert.equal(bus.publish('t', 'c'), 0);
    assert.equal(bus.getStats().globalRateLimitedMessages, 1);
    await flush();
    // The shed publish never reached the subscriber.
    assert.deepEqual(received, ['a', 'b']);
    await bus.close();
  });

  it('{} cannot enable and leaves the bus unconfigured; a later valid call still enables', async () => {
    const bus = new EventBus();
    assert.throws(() => bus.setGlobalPublishRateLimit({}), RangeError);
    assert.equal(bus.getStats().globalPublishRateLimit, null);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'global_publish_rate_limit_revised' }).length,
      0,
    );
    // Still unconfigured, so publishes stay unlimited.
    bus.subscribe('t', () => {});
    for (let i = 0; i < 10; i++) assert.equal(bus.publish('t', i), 1);
    bus.setGlobalPublishRateLimit({ messagesPerSecond: 1, burst: 2 });
    assert.deepEqual(bus.getStats().globalPublishRateLimit, {
      messagesPerSecond: 1,
      burst: 2,
    });
    await bus.close();
  });

  it('null on a never-enabled bus is a silent no-op with no audit and stays unconfigured', async () => {
    const bus = new EventBus();
    bus.setGlobalPublishRateLimit(null as never);
    assert.equal(bus.getStats().globalPublishRateLimit, null);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'global_publish_rate_limit_revised' }).length,
      0,
    );
    bus.subscribe('t', () => {});
    for (let i = 0; i < 10; i++) assert.equal(bus.publish('t', i), 1);
    await bus.close();
  });

  it('rejects non-object and illegal values and stays unconfigured; a later valid call still enables', async () => {
    const bus = new EventBus();
    const bad: Array<unknown> = [
      { messagesPerSecond: 0 },
      { messagesPerSecond: -5 },
      { messagesPerSecond: Number.NaN },
      { messagesPerSecond: Infinity },
      { messagesPerSecond: '10' },
      { messagesPerSecond: 2, burst: 0 },
      { messagesPerSecond: 2, burst: -1 },
      { messagesPerSecond: 2, burst: Number.NaN },
      { messagesPerSecond: 2, burst: Infinity },
      { messagesPerSecond: 2, burst: '5' },
      'nope',
      42,
      ['x'],
    ];
    for (const revision of bad) {
      assert.throws(
        () => bus.setGlobalPublishRateLimit(revision as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(revision)}`,
      );
    }
    assert.equal(bus.getStats().globalPublishRateLimit, null);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'global_publish_rate_limit_revised' }).length,
      0,
    );
    bus.setGlobalPublishRateLimit({ messagesPerSecond: 1, burst: 3 });
    assert.deepEqual(bus.getStats().globalPublishRateLimit, {
      messagesPerSecond: 1,
      burst: 3,
    });
    await bus.close();
  });

  it('the first burst passes at once after the enable; pre-enable publishes are never charged', async () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    // Five publishes while unlimited: they must not drain the bucket
    // the enable installs.
    for (let i = 0; i < 5; i++) assert.equal(bus.publish('t', i), 1);
    bus.setGlobalPublishRateLimit({ messagesPerSecond: 1, burst: 10 });
    let admitted = 0;
    for (let i = 0; i < 11; i++) admitted += bus.publish('t', `n${i}`);
    assert.equal(admitted, 10);
    await bus.close();
  });

  it('a rate-only revision after the enable keeps the live burst', async () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    bus.setGlobalPublishRateLimit({ messagesPerSecond: 2, burst: 10 });
    for (let i = 0; i < 3; i++) assert.equal(bus.publish('t', i), 1); // 7 tokens banked
    bus.setGlobalPublishRateLimit({ messagesPerSecond: 100 });
    // Had the burst been recomputed from the new rate's default
    // (max(1, ceil(100)) = 100), all 11 would pass; with the live burst
    // of 10 only the 7 banked tokens pass.
    let admitted = 0;
    for (let i = 0; i < 11; i++) admitted += bus.publish('t', `n${i}`);
    assert.equal(admitted, 7);
    const audit = bus.getSubscriptionAudit({ action: 'global_publish_rate_limit_revised' });
    assert.equal(audit.length, 2);
    assert.deepEqual(audit[1].detail, {
      messagesPerSecond: 100,
      oldMessagesPerSecond: 2,
      burst: 10,
      oldBurst: 10,
    });
    await bus.close();
  });

  it('an identical revision after the enable is a no-op with no audit', async () => {
    const bus = new EventBus();
    bus.setGlobalPublishRateLimit({ messagesPerSecond: 2, burst: 4 });
    bus.setGlobalPublishRateLimit({ messagesPerSecond: 2 });
    bus.setGlobalPublishRateLimit({ burst: 4 });
    bus.setGlobalPublishRateLimit({ messagesPerSecond: 2, burst: 4 });
    bus.setGlobalPublishRateLimit({});
    bus.setGlobalPublishRateLimit(null as never);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'global_publish_rate_limit_revised' }).length,
      1,
    );
    assert.deepEqual(bus.getStats().globalPublishRateLimit, {
      messagesPerSecond: 2,
      burst: 4,
    });
    await bus.close();
  });

  it('a restart restores the unconfigured state', async () => {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    bus.setGlobalPublishRateLimit({ messagesPerSecond: 1, burst: 2 });
    assert.equal(bus.publish('t', 'a'), 1);
    assert.equal(bus.publish('t', 'b'), 1);
    assert.equal(bus.publish('t', 'c'), 0);
    await bus.close();
    // A fresh bus built from the same construction options holds no
    // limiter: unlimited again, and it can still be enabled.
    const fresh = new EventBus({ now: clock.now });
    fresh.subscribe('t', () => {});
    assert.equal(fresh.getStats().globalPublishRateLimit, null);
    assert.equal(
      fresh.getSubscriptionAudit({ action: 'global_publish_rate_limit_revised' }).length,
      0,
    );
    for (let i = 0; i < 10; i++) assert.equal(fresh.publish('t', i), 1);
    fresh.setGlobalPublishRateLimit({ messagesPerSecond: 1, burst: 2 });
    assert.deepEqual(fresh.getStats().globalPublishRateLimit, {
      messagesPerSecond: 1,
      burst: 2,
    });
    await fresh.close();
  });

  it('a closed bus refuses the enable and the revision', async () => {
    const neverEnabled = new EventBus();
    await neverEnabled.close();
    assert.throws(
      () => neverEnabled.setGlobalPublishRateLimit({ messagesPerSecond: 1 }),
      /bus is closed/,
    );
    const configured = new EventBus({
      globalPublishRateLimit: { messagesPerSecond: 2, burst: 4 },
    });
    await configured.close();
    assert.throws(
      () => configured.setGlobalPublishRateLimit({ burst: 3 }),
      /bus is closed/,
    );
  });
});
