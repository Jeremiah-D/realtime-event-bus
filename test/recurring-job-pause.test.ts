import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so trigger tests are deterministic instead of racing wall time. Due
 * sweeps run at the start of every flush, so advancing this clock past a
 * trigger time and then publishing anything fires deterministically.
 */
function controllableClock(startMs: number) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-recurring-pause-'));

/** White-box read of the bus's private recurring timer. */
function recurringTimerOf(bus: EventBus): { hasRef(): boolean } | undefined {
  return (bus as unknown as { recurringTimer?: { hasRef(): boolean } }).recurringTimer;
}

test('a paused job does not fire when its trigger time passes', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  let fires = 0;
  const handle = bus.scheduleRecurring('tick', () => ++fires, '* * * * *');
  bus.setRecurringJobPaused(handle.id, true);

  const paused = bus.getStats().recurringJobs.jobs[0];
  assert.equal(paused.paused, true);
  assert.equal(bus.getStats().recurringJobs.active, 0);
  assert.equal(paused.nextTriggerMs, Date.UTC(2024, 0, 15, 10, 31, 0));

  clock.nowMs = Date.UTC(2024, 0, 15, 10, 31, 1);
  bus.publish('poke', 'x'); // any publish runs the flush sweep
  await flush();
  assert.equal(fires, 0);
  assert.deepEqual(received, []);
  // The pause never advances the trigger time.
  assert.equal(
    bus.getStats().recurringJobs.jobs[0].nextTriggerMs,
    Date.UTC(2024, 0, 15, 10, 31, 0),
  );
});

test('resuming recomputes the next trigger from now; missed triggers are not backfilled', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  let fires = 0;
  const handle = bus.scheduleRecurring('tick', () => ++fires, '* * * * *');

  bus.setRecurringJobPaused(handle.id, true);
  // Four trigger times (10:31–10:34) pass while paused.
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 35, 0);
  bus.publish('poke', 'x');
  await flush();
  assert.equal(fires, 0);

  bus.setRecurringJobPaused(handle.id, false);
  const resumed = bus.getStats().recurringJobs.jobs[0];
  assert.equal(resumed.paused, undefined);
  assert.equal(bus.getStats().recurringJobs.active, 1);
  // Next trigger is the first occurrence after the resume moment — the
  // missed 10:31–10:35 triggers are skipped, never replayed.
  assert.equal(resumed.nextTriggerMs, Date.UTC(2024, 0, 15, 10, 36, 0));

  clock.nowMs = Date.UTC(2024, 0, 15, 10, 36, 1);
  bus.publish('poke', 'x');
  await flush();
  assert.equal(fires, 1);
  assert.deepEqual(received, [1]);
});

test('triggerCount and errorCount stay continuous across a pause', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  let calls = 0;
  const handle = bus.scheduleRecurring(
    'tick',
    () => {
      calls += 1;
      if (calls === 2) throw new Error('boom');
      return calls;
    },
    '* * * * *',
  );
  // Fire 1 succeeds at 10:31; fire 2 throws at 10:32.
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 31, 1);
  bus.publish('poke', 'x');
  await flush();
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 32, 1);
  bus.publish('poke', 'x');
  await flush();
  assert.deepEqual(received, [1]);
  let job = bus.getStats().recurringJobs.jobs[0];
  assert.equal(job.triggerCount, 1);
  assert.equal(job.errorCount, 1);

  // Pause through 10:33–10:35; resume at 10:35.
  bus.setRecurringJobPaused(handle.id, true);
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 35, 0);
  bus.publish('poke', 'x');
  await flush();
  assert.deepEqual(received, [1], 'nothing fires while paused');
  bus.setRecurringJobPaused(handle.id, false);
  job = bus.getStats().recurringJobs.jobs[0];
  assert.equal(job.triggerCount, 1, 'counts survive the pause');
  assert.equal(job.errorCount, 1);
  assert.equal(job.nextTriggerMs, Date.UTC(2024, 0, 15, 10, 36, 0));

  clock.nowMs = Date.UTC(2024, 0, 15, 10, 36, 1);
  bus.publish('poke', 'x');
  await flush();
  assert.deepEqual(received, [1, 3]);
  job = bus.getStats().recurringJobs.jobs[0];
  assert.equal(job.triggerCount, 2);
  assert.equal(job.errorCount, 1);
});

test('setRecurringJobPaused rejects unknown, cancelled, and dormant jobs and non-boolean values', () => {
  const bus = new EventBus();
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  assert.throws(() => bus.setRecurringJobPaused('recurring-999', true), RangeError);
  assert.throws(() => bus.setRecurringJobPaused(handle.id, 'yes' as never), RangeError);
  assert.throws(() => bus.setRecurringJobPaused(handle.id, 1 as never), RangeError);
  assert.throws(() => bus.setRecurringJobPaused(handle.id, null as never), RangeError);
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'recurring_job_paused' }), []);
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'recurring_job_resumed' }), []);

  // Cancelling is terminal: pausing a cancelled job is refused.
  assert.equal(bus.cancelRecurringJob(handle.id), true);
  assert.throws(() => bus.setRecurringJobPaused(handle.id, true), RangeError);
  assert.throws(() => bus.setRecurringJobPaused(handle.id, false), RangeError);

  // A dormant post-restart entry has no live timer to pause.
  const dir = freshDir();
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus1 = new EventBus({ now: clock.now, durableLogDir: dir });
  const h1 = bus1.scheduleRecurring('tick', () => 'x', '* * * * *');
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  assert.equal(bus2.getStats().recurringJobs.jobs[0].dormant, true);
  assert.throws(() => bus2.setRecurringJobPaused(h1.id, true), RangeError);
  assert.deepEqual(bus2.getSubscriptionAudit({ action: 'recurring_job_paused' }), []);
});

test('setting the current pause state is a no-op with no audit', () => {
  const bus = new EventBus();
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  bus.setRecurringJobPaused(handle.id, true);
  bus.setRecurringJobPaused(handle.id, true);
  bus.setRecurringJobPaused(handle.id, false);
  bus.setRecurringJobPaused(handle.id, false);
  assert.equal(bus.getSubscriptionAudit({ action: 'recurring_job_paused' }).length, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'recurring_job_resumed' }).length, 1);
});

test('pause and resume are audited with id, topic, and cron', () => {
  const bus = new EventBus();
  const handle = bus.scheduleRecurring('tick', () => 'x', '*/5 * * * *');
  bus.setRecurringJobPaused(handle.id, true);
  bus.setRecurringJobPaused(handle.id, false);

  const paused = bus.getSubscriptionAudit({ action: 'recurring_job_paused' });
  assert.equal(paused.length, 1);
  assert.equal(paused[0]?.actor, 'system');
  assert.equal(paused[0]?.topic, 'tick');
  assert.deepEqual(paused[0]?.detail, { id: handle.id, topic: 'tick', cron: '*/5 * * * *' });
  const resumed = bus.getSubscriptionAudit({ action: 'recurring_job_resumed' });
  assert.equal(resumed.length, 1);
  assert.equal(resumed[0]?.actor, 'system');
  assert.equal(resumed[0]?.topic, 'tick');
  assert.deepEqual(resumed[0]?.detail, { id: handle.id, topic: 'tick', cron: '*/5 * * * *' });
});

test('a paused heap-top job does not starve the live jobs behind it', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  // A fires every minute (next 10:31); B every 5 minutes (next 10:35).
  const a = bus.scheduleRecurring('tick', () => 'A', '* * * * *');
  const b = bus.scheduleRecurring('tick', () => 'B', '*/5 * * * *');
  bus.setRecurringJobPaused(a.id, true);

  clock.nowMs = Date.UTC(2024, 0, 15, 10, 35, 1);
  bus.publish('poke', 'x');
  await flush();
  assert.deepEqual(received, ['B']);
  const jobs = bus.getStats().recurringJobs.jobs;
  const jobA = jobs.find((j) => j.id === a.id);
  const jobB = jobs.find((j) => j.id === b.id);
  // A is still parked on its original trigger; B fired and advanced.
  assert.equal(jobA?.nextTriggerMs, Date.UTC(2024, 0, 15, 10, 31, 0));
  assert.equal(jobA?.triggerCount, 0);
  assert.equal(jobB?.nextTriggerMs, Date.UTC(2024, 0, 15, 10, 40, 0));
  assert.equal(jobB?.triggerCount, 1);
});

test('pausing the only job disarms the shared timer; resuming re-arms it', () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now });
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  assert.equal(recurringTimerOf(bus) !== undefined, true);
  bus.setRecurringJobPaused(handle.id, true);
  assert.equal(recurringTimerOf(bus), undefined);
  bus.setRecurringJobPaused(handle.id, false);
  assert.equal(recurringTimerOf(bus) !== undefined, true);
});

test('a paused job can still be cancelled, and pausing it afterwards is refused', () => {
  const bus = new EventBus();
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  bus.setRecurringJobPaused(handle.id, true);
  assert.equal(bus.getStats().recurringJobs.active, 0);
  assert.equal(bus.cancelRecurringJob(handle.id), true);
  assert.equal(bus.getStats().recurringJobs.active, 0);
  assert.equal(bus.getStats().recurringJobs.jobs.length, 0);
  assert.throws(() => bus.setRecurringJobPaused(handle.id, true), RangeError);
  assert.throws(() => bus.setRecurringJobPaused(handle.id, false), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'recurring_job_paused' }).length, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'recurring_job_cancelled' }).length, 1);
});
