import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, BusClosedError } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

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

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-recurring-rev-'));

function recurringRecords(dir: string, topic: string) {
  const log = DurableTopicLog.open({ dir });
  return log.readSince(topic, -1).filter((r) => r.recurringJobId !== undefined);
}

/** White-box read of the bus's private recurring timer. */
function recurringTimerOf(bus: EventBus): { hasRef(): boolean } | undefined {
  return (bus as unknown as { recurringTimer?: { hasRef(): boolean } }).recurringTimer;
}

// ---------------------------------------------------------------------------
// cancelRecurringJob
// ---------------------------------------------------------------------------

test('cancel by id stops future triggers, writes one tombstone, and audits', async () => {
  const dir = freshDir();
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  assert.equal(recurringTimerOf(bus) !== undefined, true);
  assert.equal(bus.cancelRecurringJob(handle.id), true);
  assert.equal(bus.getStats().recurringJobs.active, 0);
  assert.equal(recurringTimerOf(bus), undefined);
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 35, 0);
  bus.publish('tick', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
  const tombstones = recurringRecords(dir, 'tick').filter((r) => r.cancelled === true);
  assert.equal(tombstones.length, 1);
  assert.equal(tombstones[0].recurringJobId, handle.id);
  assert.equal(tombstones[0].seq, 0);
  const events = bus.getSubscriptionAudit({ action: 'recurring_job_cancelled' });
  assert.equal(events.length, 1);
  assert.equal(events[0].topic, 'tick');
  assert.deepEqual(events[0].detail, { id: handle.id, topic: 'tick', cron: '* * * * *' });
});

test('cancel unknown id returns false with no audit and no tombstone', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  assert.equal(bus.cancelRecurringJob('recurring-999'), false);
  assert.equal(bus.cancelRecurringJob(''), false);
  assert.equal(bus.getStats().recurringJobs.active, 1);
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'recurring_job_cancelled' }), []);
  assert.equal(recurringRecords(dir, 'tick').filter((r) => r.cancelled === true).length, 0);
});

test('cancel twice: the second call returns false and writes no second tombstone', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  assert.equal(bus.cancelRecurringJob(handle.id), true);
  assert.equal(bus.cancelRecurringJob(handle.id), false);
  assert.equal(recurringRecords(dir, 'tick').filter((r) => r.cancelled === true).length, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'recurring_job_cancelled' }).length, 1);
});

test('cancel dormant returns false and leaves the dormant entry listed', () => {
  const dir = freshDir();
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  assert.equal(bus2.getStats().recurringJobs.jobs[0].dormant, true);
  assert.equal(bus2.cancelRecurringJob(handle.id), false);
  // No tombstone: the dormant entry stays rebuildable on the next restart.
  assert.equal(recurringRecords(dir, 'tick').filter((r) => r.cancelled === true).length, 0);
  const jobs = bus2.getStats().recurringJobs.jobs;
  assert.equal(jobs.length, 1);
  assert.equal(jobs[0].id, handle.id);
  assert.equal(jobs[0].dormant, true);
  assert.deepEqual(bus2.getSubscriptionAudit({ action: 'recurring_job_cancelled' }), []);
});

// ---------------------------------------------------------------------------
// reviseRecurringJob: cron expression
// ---------------------------------------------------------------------------

test('revise cron: fires on the new cron with id and triggerCount continuous', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  const handle = bus.scheduleRecurring('tick', () => 'every-minute', '* * * * *');
  // One fire on the old cron first, so continuity is observable.
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 31, 1);
  bus.publish('tick', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'every-minute']);
  assert.equal(bus.getStats().recurringJobs.jobs[0].triggerCount, 1);
  const revised = bus.reviseRecurringJob(handle.id, { cronExpr: '0 11 * * *' });
  assert.equal(revised.id, handle.id);
  assert.equal(revised.topic, 'tick');
  assert.equal(revised.cron, '0 11 * * *');
  assert.equal(revised.nextTriggerMs, Date.UTC(2024, 0, 15, 11, 0, 0, 0));
  assert.equal(revised.triggerCount, 1);
  assert.equal(revised.errorCount, 0);
  // The old cadence is gone: 10:32 passes with no fire.
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 32, 1);
  bus.publish('tick', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'every-minute', 'poke2']);
  // The new cron fires at 11:00.
  clock.nowMs = Date.UTC(2024, 0, 15, 11, 0, 1);
  bus.publish('tick', 'poke3');
  await flush();
  assert.deepEqual(received, ['poke', 'every-minute', 'poke2', 'poke3', 'every-minute']);
  const job = bus.getStats().recurringJobs.jobs[0];
  assert.equal(job.id, handle.id);
  assert.equal(job.triggerCount, 2);
  assert.equal(job.nextTriggerMs, Date.UTC(2024, 0, 16, 11, 0, 0, 0));
});

test('revise illegal cron throws RangeError and leaves the job untouched', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  const before = bus.getStats().recurringJobs.jobs[0];
  assert.throws(() => bus.reviseRecurringJob(handle.id, { cronExpr: 'not a cron' }), RangeError);
  assert.throws(() => bus.reviseRecurringJob(handle.id, { cronExpr: '0 0 30 2 *' }), RangeError);
  assert.throws(
    () => bus.reviseRecurringJob(handle.id, { cronExpr: 42 as unknown as string }),
    RangeError,
  );
  const after = bus.getStats().recurringJobs.jobs[0];
  assert.deepEqual(after, before);
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'recurring_job_revised' }), []);
  // The old cron still fires.
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 31, 1);
  bus.publish('tick', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'x']);
});

test('revise same cron is a no-op with no audit and no new schedule record', () => {
  const dir = freshDir();
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  const handle = bus.scheduleRecurring('tick', () => 'x', '*/5 * * * *');
  const before = bus.getStats().recurringJobs.jobs[0];
  const revised = bus.reviseRecurringJob(handle.id, { cronExpr: '*/5 * * * *' });
  assert.deepEqual(revised, before);
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'recurring_job_revised' }), []);
  const schedules = recurringRecords(dir, 'tick').filter(
    (r) => r.recurringTrigger !== true && r.cancelled !== true,
  );
  assert.equal(schedules.length, 1);
});

test('revise empty patch is a no-op with no audit', () => {
  const bus = new EventBus();
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  const before = bus.getStats().recurringJobs.jobs[0];
  assert.deepEqual(bus.reviseRecurringJob(handle.id, {}), before);
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'recurring_job_revised' }), []);
});

test('revise writes a new schedule record (no tombstone) and audits previous_cron', () => {
  const dir = freshDir();
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  bus.reviseRecurringJob(handle.id, { cronExpr: '0 11 * * *' });
  const schedules = recurringRecords(dir, 'tick').filter(
    (r) => r.recurringTrigger !== true && r.cancelled !== true,
  );
  assert.equal(schedules.length, 2);
  assert.equal(schedules[0].cronExpr, '* * * * *');
  assert.equal(schedules[1].cronExpr, '0 11 * * *');
  assert.equal(schedules[1].recurringJobId, handle.id);
  assert.equal(schedules[1].seq, 0);
  assert.equal(recurringRecords(dir, 'tick').filter((r) => r.cancelled === true).length, 0);
  const events = bus.getSubscriptionAudit({ action: 'recurring_job_revised' });
  assert.equal(events.length, 1);
  assert.equal(events[0].topic, 'tick');
  assert.deepEqual(events[0].detail, {
    id: handle.id,
    previous_cron: '* * * * *',
    cron: '0 11 * * *',
    payloadFactoryReplaced: false,
  });
});

// ---------------------------------------------------------------------------
// reviseRecurringJob: payload factory
// ---------------------------------------------------------------------------

test('revise payloadFactory: later triggers use the new factory', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  const handle = bus.scheduleRecurring('tick', () => 'old', '* * * * *');
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 31, 1);
  bus.publish('tick', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'old']);
  const revised = bus.reviseRecurringJob(handle.id, { payloadFactory: () => 'new' });
  assert.equal(revised.cron, '* * * * *');
  assert.equal(revised.triggerCount, 1);
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 32, 1);
  bus.publish('tick', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'old', 'poke2', 'new']);
  assert.equal(bus.getStats().recurringJobs.jobs[0].triggerCount, 2);
  // A factory-only revision is still audited (the cron is unchanged).
  const events = bus.getSubscriptionAudit({ action: 'recurring_job_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    id: handle.id,
    previous_cron: '* * * * *',
    cron: '* * * * *',
    payloadFactoryReplaced: true,
  });
});

// ---------------------------------------------------------------------------
// reviseRecurringJob: rejections
// ---------------------------------------------------------------------------

test('revise unknown id throws RangeError', () => {
  const bus = new EventBus();
  bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  assert.throws(
    () => bus.reviseRecurringJob('recurring-999', { cronExpr: '0 11 * * *' }),
    RangeError,
  );
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'recurring_job_revised' }), []);
});

test('revise non-function payloadFactory throws TypeError and leaves the job untouched', () => {
  const bus = new EventBus();
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  const before = bus.getStats().recurringJobs.jobs[0];
  assert.throws(
    () =>
      bus.reviseRecurringJob(handle.id, {
        payloadFactory: 'nope' as unknown as () => unknown,
      }),
    TypeError,
  );
  assert.deepEqual(bus.getStats().recurringJobs.jobs[0], before);
});

test('revise dormant throws RangeError and leaves the entry untouched', () => {
  const dir = freshDir();
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  assert.equal(bus2.getStats().recurringJobs.jobs[0].dormant, true);
  assert.throws(
    () => bus2.reviseRecurringJob(handle.id, { cronExpr: '0 11 * * *' }),
    RangeError,
  );
  assert.throws(() => bus2.reviseRecurringJob(handle.id, { payloadFactory: () => 'y' }), RangeError);
  const job = bus2.getStats().recurringJobs.jobs[0];
  assert.equal(job.cron, '* * * * *');
  assert.equal(job.dormant, true);
  assert.deepEqual(bus2.getSubscriptionAudit({ action: 'recurring_job_revised' }), []);
});

test('revise after close throws BusClosedError', async () => {
  const bus = new EventBus();
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  await bus.close();
  assert.throws(
    () => bus.reviseRecurringJob(handle.id, { cronExpr: '0 11 * * *' }),
    (err: unknown) => {
      assert.ok(err instanceof BusClosedError, `expected BusClosedError, got ${String(err)}`);
      assert.equal((err as Error).name, 'BusClosedError');
      return true;
    },
  );
});

// ---------------------------------------------------------------------------
// reviseRecurringJob: heap order
// ---------------------------------------------------------------------------

test('heap reorder: revising the later job to a later cron fires the other job first', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 9, 0, 0));
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  const early = bus.scheduleRecurring('tick', () => 'a', '30 9 * * *');
  const late = bus.scheduleRecurring('tick', () => 'b', '0 10 * * *');
  // Push the 09:30 job past the 10:00 one: it must fire second now.
  const revised = bus.reviseRecurringJob(early.id, { cronExpr: '15 10 * * *' });
  assert.equal(revised.nextTriggerMs, Date.UTC(2024, 0, 15, 10, 15, 0, 0));
  assert.equal(bus.getStats().recurringJobs.jobs.find((j) => j.id === late.id)?.nextTriggerMs,
    Date.UTC(2024, 0, 15, 10, 0, 0, 0));
  clock.nowMs = Date.UTC(2024, 0, 15, 11, 0, 0);
  bus.publish('tick', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'b', 'a']);
});
