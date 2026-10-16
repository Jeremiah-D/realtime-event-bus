import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';
import { parseCronExpression, nextCronOccurrence } from '../src/cronparse.ts';

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

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-recurring-'));

/** White-box read of the bus's private recurring timer. */
function recurringTimerOf(bus: EventBus): { hasRef(): boolean } | undefined {
  return (bus as unknown as { recurringTimer?: { hasRef(): boolean } }).recurringTimer;
}

// ---------------------------------------------------------------------------
// cron parser unit tests
// ---------------------------------------------------------------------------

test('parses the five basic field forms', () => {
  const s = parseCronExpression('*/15 9-17 1,15 1-6/2 *');
  assert.deepEqual([...s.minutes].sort((a, b) => a - b), [0, 15, 30, 45]);
  assert.deepEqual([...s.hours].sort((a, b) => a - b), [9, 10, 11, 12, 13, 14, 15, 16, 17]);
  assert.deepEqual([...s.dom.days].sort((a, b) => a - b), [1, 15]);
  assert.deepEqual([...s.months].sort((a, b) => a - b), [1, 3, 5]);
  assert.equal(s.domRestricted, true);
  assert.equal(s.dowRestricted, false);
});

test('month and weekday names are case-insensitive', () => {
  const s = parseCronExpression('0 9 * jan,Mar *');
  assert.deepEqual([...s.months].sort((a, b) => a - b), [1, 3]);
  const d = parseCronExpression('0 9 * * mon-FRI');
  assert.deepEqual([...d.dow.days].sort((a, b) => a - b), [1, 2, 3, 4, 5]);
});

test('day-of-week 7 means Sunday', () => {
  const s = parseCronExpression('0 0 * * 7');
  assert.deepEqual([...s.dow.days], [0]);
});

test('malformed expressions throw RangeError', () => {
  const bad = [
    '',
    '   ',
    '* * * *', // 4 fields
    '* * * * * *', // 6 fields
    '60 * * * *', // minute out of range
    '* 24 * * *', // hour out of range
    '* * 0 * *', // day-of-month out of range
    '* * * 13 *', // month out of range
    '* * * * 8', // day-of-week out of range
    '*/0 * * * *', // zero step
    '5-2 * * * *', // reversed range
    '* * * FOO *', // bad month name
    '* * * * FUNDAY', // bad weekday name
    '1.5 * * * *', // non-integer
    '* * * * MON#0', // # occurrence out of range
    '* * * * MON#6', // # occurrence out of range
    '0 0 32W * *', // W day out of range
    '* * *, * * *', // empty list item
  ];
  for (const expr of bad) {
    assert.throws(() => parseCronExpression(expr), RangeError, `expected RangeError for ${JSON.stringify(expr)}`);
  }
  assert.throws(() => parseCronExpression(42 as unknown as string), RangeError);
});

test('an expression that can never match is rejected (February 30)', () => {
  assert.throws(() => parseCronExpression('0 0 30 2 *'), RangeError);
  assert.throws(() => parseCronExpression('0 0 31 2 *'), RangeError);
});

test('L matches the last day of the month', () => {
  const s = parseCronExpression('0 0 L * *');
  // Mon 2024-01-15 10:30 UTC -> Wed 2024-01-31 00:00 UTC
  assert.equal(
    nextCronOccurrence(s, Date.UTC(2024, 0, 15, 10, 30)),
    Date.UTC(2024, 0, 31, 0, 0),
  );
  // After Jan 31 -> Feb 29 (2024 is a leap year)
  assert.equal(
    nextCronOccurrence(s, Date.UTC(2024, 0, 31, 0, 0)),
    Date.UTC(2024, 1, 29, 0, 0),
  );
});

test('LW matches the last weekday of the month', () => {
  const s = parseCronExpression('0 0 LW * *');
  // Jan 31 2024 is a Wednesday
  assert.equal(
    nextCronOccurrence(s, Date.UTC(2024, 0, 15)),
    Date.UTC(2024, 0, 31, 0, 0),
  );
  // Apr 30 2024 is a Tuesday -> Apr 30
  assert.equal(
    nextCronOccurrence(s, Date.UTC(2024, 3, 1)),
    Date.UTC(2024, 3, 30, 0, 0),
  );
});

test('nW matches the weekday nearest to day n', () => {
  const s = parseCronExpression('0 0 15W * *');
  // Jun 15 2024 is a Saturday -> Fri Jun 14
  assert.equal(
    nextCronOccurrence(s, Date.UTC(2024, 5, 1)),
    Date.UTC(2024, 5, 14, 0, 0),
  );
  // Jul 15 2024 is a Monday -> Jul 15 itself
  assert.equal(
    nextCronOccurrence(s, Date.UTC(2024, 6, 1)),
    Date.UTC(2024, 6, 15, 0, 0),
  );
  // Sep 15 2024 is a Sunday -> Mon Sep 16
  assert.equal(
    nextCronOccurrence(s, Date.UTC(2024, 8, 1)),
    Date.UTC(2024, 8, 16, 0, 0),
  );
});

test('5L matches the last Friday of the month', () => {
  const s = parseCronExpression('0 0 * * 5L');
  // Last Friday of Jan 2024 is the 26th
  assert.equal(
    nextCronOccurrence(s, Date.UTC(2024, 0, 15, 10, 30)),
    Date.UTC(2024, 0, 26, 0, 0),
  );
});

test('MON#3 matches the third Monday of the month', () => {
  const s = parseCronExpression('0 0 * * MON#3');
  // Jan 15 2024 IS the third Monday, but 00:00 is past 10:30 -> Feb 19
  assert.equal(
    nextCronOccurrence(s, Date.UTC(2024, 0, 15, 10, 30)),
    Date.UTC(2024, 1, 19, 0, 0),
  );
});

test('day-of-month and day-of-week both restricted: either may match', () => {
  const s = parseCronExpression('0 0 1 * 0');
  // Mon 2024-01-15 -> Sun 2024-01-21 (a Sunday, before the 1st of Feb)
  assert.equal(
    nextCronOccurrence(s, Date.UTC(2024, 0, 15, 10, 30)),
    Date.UTC(2024, 0, 21, 0, 0),
  );
});

test('nextCronOccurrence is strictly greater than afterMs', () => {
  const s = parseCronExpression('0 9 * * *');
  // Exactly on a trigger -> the NEXT day's trigger, never the same instant
  assert.equal(
    nextCronOccurrence(s, Date.UTC(2024, 0, 16, 9, 0, 0, 0)),
    Date.UTC(2024, 0, 17, 9, 0, 0, 0),
  );
});

test('triggers are evaluated in UTC', () => {
  const s = parseCronExpression('0 0 * * *');
  assert.equal(
    nextCronOccurrence(s, Date.UTC(2024, 0, 1, 0, 0, 0, 0)),
    Date.UTC(2024, 0, 2, 0, 0, 0, 0),
  );
});

test('nextCronOccurrence rejects a non-finite afterMs', () => {
  const s = parseCronExpression('* * * * *');
  assert.throws(() => nextCronOccurrence(s, NaN), RangeError);
  assert.throws(() => nextCronOccurrence(s, Infinity), RangeError);
});

test('leap-day schedules find the next leap year', () => {
  const s = parseCronExpression('0 0 29 2 *');
  assert.equal(
    nextCronOccurrence(s, Date.UTC(2024, 0, 15)),
    Date.UTC(2024, 1, 29, 0, 0),
  );
  // After 2024-02-29 -> 2028-02-29 (no 8-year gap surprise)
  assert.equal(
    nextCronOccurrence(s, Date.UTC(2024, 1, 29, 0, 0, 0, 1)),
    Date.UTC(2028, 1, 29, 0, 0),
  );
});

// ---------------------------------------------------------------------------
// scheduleRecurring admission
// ---------------------------------------------------------------------------

test('scheduleRecurring fail-fast: invalid expression throws RangeError with zero side effects', () => {
  const bus = new EventBus();
  assert.throws(() => bus.scheduleRecurring('t', () => 1, 'not a cron'), RangeError);
  assert.throws(() => bus.scheduleRecurring('t', () => 1, '0 0 30 2 *'), RangeError);
  assert.equal(bus.getStats().recurringJobs.active, 0);
  assert.deepEqual(bus.getStats().recurringJobs.jobs, []);
});

test('scheduleRecurring fail-fast: invalid topic throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.scheduleRecurring('', () => 1, '* * * * *'), RangeError);
  assert.throws(
    () => bus.scheduleRecurring(42 as unknown as string, () => 1, '* * * * *'),
    RangeError,
  );
  assert.equal(bus.getStats().recurringJobs.active, 0);
});

test('scheduleRecurring fail-fast: non-function factory throws TypeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.scheduleRecurring('t', 'nope' as unknown as () => unknown, '* * * * *'), TypeError);
  assert.equal(bus.getStats().recurringJobs.active, 0);
});

test('scheduleRecurring fail-fast: invalid opts throw RangeError', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.scheduleRecurring('t', () => 1, '* * * * *', { publisherId: 'bad id!' }),
    RangeError,
  );
  assert.equal(bus.getStats().recurringJobs.active, 0);
});

test('schedule returns a handle and exposes the job in getStats', () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now });
  const handle = bus.scheduleRecurring('tick', () => ({ n: 1 }), '*/5 * * * *');
  assert.match(handle.id, /^recurring-\d+$/);
  const stats = bus.getStats().recurringJobs;
  assert.equal(stats.active, 1);
  assert.equal(stats.jobs.length, 1);
  const job = stats.jobs[0];
  assert.equal(job.id, handle.id);
  assert.equal(job.topic, 'tick');
  assert.equal(job.cron, '*/5 * * * *');
  // 10:30 -> next :35 trigger
  assert.equal(job.nextTriggerMs, Date.UTC(2024, 0, 15, 10, 35, 0, 0));
  assert.equal(job.triggerCount, 0);
  assert.equal(job.errorCount, 0);
  assert.equal(job.dormant, undefined);
});

// ---------------------------------------------------------------------------
// triggering
// ---------------------------------------------------------------------------

test('a job fires when the clock passes its trigger time', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  let n = 0;
  bus.scheduleRecurring('tick', () => ({ n: ++n }), '* * * * *');
  await flush();
  assert.deepEqual(received, []); // 10:30:00, next trigger 10:31:00
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 31, 1);
  bus.publish('tick', 'poke'); // any publish runs the flush sweep
  await flush();
  assert.deepEqual(received, ['poke', { n: 1 }]);
  const job = bus.getStats().recurringJobs.jobs[0];
  assert.equal(job.triggerCount, 1);
  assert.equal(job.nextTriggerMs, Date.UTC(2024, 0, 15, 10, 32, 0, 0));
});

test('no catch-up: a job far behind fires exactly once per sweep', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  let fires = 0;
  bus.scheduleRecurring('tick', () => ++fires, '* * * * *');
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 40, 0); // 10 missed minutes
  bus.publish('tick', 'poke');
  await flush();
  assert.equal(fires, 1);
  assert.deepEqual(received, ['poke', 1]);
  // The next trigger is recomputed from now, not from the missed cadence
  const job = bus.getStats().recurringJobs.jobs[0];
  assert.equal(job.nextTriggerMs, Date.UTC(2024, 0, 15, 10, 41, 0, 0));
});

test('jobs fire in trigger-time order', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 9, 0, 0));
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  bus.scheduleRecurring('tick', () => 'late', '30 10 * * *');
  bus.scheduleRecurring('tick', () => 'early', '0 10 * * *');
  clock.nowMs = Date.UTC(2024, 0, 15, 11, 0, 0);
  bus.publish('tick', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'early', 'late']);
});

test('a throwing factory skips the trigger but never kills the schedule', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  let calls = 0;
  bus.scheduleRecurring('tick', () => {
    calls++;
    if (calls === 1) throw new Error('boom');
    return 'recovered';
  }, '* * * * *');
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 31, 1);
  bus.publish('tick', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']); // first trigger skipped
  assert.equal(bus.getStats().recurringJobs.jobs[0].errorCount, 1);
  assert.equal(bus.getStats().recurringJobs.jobs[0].triggerCount, 0);
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 32, 1);
  bus.publish('tick', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2', 'recovered']); // schedule alive
  const job = bus.getStats().recurringJobs.jobs[0];
  assert.equal(job.triggerCount, 1);
  assert.equal(job.errorCount, 1);
});

test('a trigger rejected by admission follows normal admission semantics', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now });
  bus.setTopicSchema('tick', () => false); // reject everything
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  bus.scheduleRecurring('tick', () => 'payload', '* * * * *');
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 31, 1);
  bus.publish('other', 'poke');
  await flush();
  assert.deepEqual(received, []); // rejected, not published
  assert.equal(bus.getStats().rejectedMessages, 1);
  // The job keeps its cadence: the trigger was attempted and counted
  const job = bus.getStats().recurringJobs.jobs[0];
  assert.equal(job.triggerCount, 1);
  assert.equal(job.nextTriggerMs, Date.UTC(2024, 0, 15, 10, 32, 0, 0));
});

test('a trigger can cancel its own job from inside the factory', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  let handle!: { cancel(): void };
  let calls = 0;
  handle = bus.scheduleRecurring('tick', () => {
    calls++;
    if (calls === 2) handle.cancel();
    return calls;
  }, '* * * * *');
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 31, 1);
  bus.publish('tick', 'poke');
  await flush();
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 32, 1);
  bus.publish('tick', 'poke2');
  await flush();
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 33, 1);
  bus.publish('tick', 'poke3');
  await flush();
  assert.deepEqual(received, ['poke', 1, 'poke2', 2, 'poke3']); // no third trigger
  assert.equal(bus.getStats().recurringJobs.active, 0);
});

// ---------------------------------------------------------------------------
// cancel
// ---------------------------------------------------------------------------

test('cancel stops future triggers and is idempotent', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  assert.equal(bus.getStats().recurringJobs.active, 1);
  handle.cancel();
  handle.cancel(); // idempotent, never throws
  assert.equal(bus.getStats().recurringJobs.active, 0);
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 35, 0);
  bus.publish('tick', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
});

test('the recurring timer is unrefed and disarmed on cancel', () => {
  const bus = new EventBus();
  const handle = bus.scheduleRecurring('t', () => 1, '0 0 * * *');
  const timer = recurringTimerOf(bus);
  assert.ok(timer !== undefined, 'expected an armed timer');
  assert.equal(timer.hasRef(), false, 'timer must be unrefed');
  handle.cancel();
  assert.equal(recurringTimerOf(bus), undefined);
});

// ---------------------------------------------------------------------------
// options plumbing
// ---------------------------------------------------------------------------

test('publisherId rides every trigger publish', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('tick', () => {});
  bus.scheduleRecurring('tick', () => 'x', '* * * * *', { publisherId: 'cron-agent' });
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 31, 1);
  bus.publish('tick', 'poke');
  await flush();
  const publishers = bus.getStats().publishers;
  const row = publishers.find((p) => p.publisher === 'cron-agent');
  assert.ok(row !== undefined, 'expected publisher attribution');
  assert.equal(row.publishedMessages, 1);
});

// ---------------------------------------------------------------------------
// durable log integration
// ---------------------------------------------------------------------------

function recurringRecords(dir: string, topic: string) {
  const log = DurableTopicLog.open({ dir });
  return log.readSince(topic, -1).filter((r) => r.recurringJobId !== undefined);
}

test('schedule writes a schedule record, each trigger a trigger record', async () => {
  const dir = freshDir();
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.subscribe('tick', () => {});
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  let records = recurringRecords(dir, 'tick');
  assert.equal(records.length, 1);
  assert.equal(records[0].seq, 0);
  assert.equal(records[0].recurring, true);
  assert.equal(records[0].recurringJobId, handle.id);
  assert.equal(records[0].cronExpr, '* * * * *');
  assert.equal(records[0].recurringTrigger, undefined);
  assert.equal(records[0].cancelled, undefined);
  // Two fires -> two trigger records
  for (const minute of [31, 32]) {
    clock.nowMs = Date.UTC(2024, 0, 15, 10, minute, 1);
    bus.publish('tick', 'poke');
    await flush();
  }
  records = recurringRecords(dir, 'tick');
  const triggers = records.filter((r) => r.recurringTrigger === true);
  assert.equal(triggers.length, 2);
  for (const t of triggers) {
    assert.equal(t.seq, 0);
    assert.equal(t.recurringJobId, handle.id);
    assert.equal(t.cronExpr, '* * * * *');
  }
});

test('cancel writes a tombstone', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  handle.cancel();
  const records = recurringRecords(dir, 'tick');
  const tombstones = records.filter((r) => r.cancelled === true);
  assert.equal(tombstones.length, 1);
  assert.equal(tombstones[0].recurringJobId, handle.id);
  assert.equal(tombstones[0].seq, 0);
});

test('schedule records are invisible to replay (seq-0)', async () => {
  const dir = freshDir();
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  bus.publish('tick', 'real');
  await flush();
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload), { resumeFromSeq: 0 });
  await flush();
  // Only the real message replays — schedule/trigger records never do
  assert.deepEqual(received, ['real']);
});

test('restart: a cancelled job is never revived', () => {
  const dir = freshDir();
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  handle.cancel();
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  assert.deepEqual(bus2.getStats().recurringJobs.jobs, []);
  assert.equal(bus2.getStats().recurringJobs.active, 0);
});

test('restart: a live job becomes a dormant entry with its next trigger recomputed', async () => {
  const dir = freshDir();
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  const handle = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  const jobs = bus2.getStats().recurringJobs.jobs;
  assert.equal(bus2.getStats().recurringJobs.active, 0);
  assert.equal(jobs.length, 1);
  assert.equal(jobs[0].id, handle.id);
  assert.equal(jobs[0].topic, 'tick');
  assert.equal(jobs[0].cron, '* * * * *');
  assert.equal(jobs[0].dormant, true);
  // Next trigger recomputed from now (no catch-up of missed history)
  assert.equal(jobs[0].nextTriggerMs, Date.UTC(2024, 0, 15, 10, 31, 0, 0));
  // The dormant job has no timer and never fires
  const received: unknown[] = [];
  bus2.subscribe('tick', (msg) => received.push(msg.payload));
  clock.nowMs = Date.UTC(2024, 0, 15, 10, 35, 0);
  bus2.publish('tick', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
});

test('restart: re-registering the same (topic, cron) supersedes the dormant entry', () => {
  const dir = freshDir();
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  const old = bus.scheduleRecurring('tick', () => 'x', '* * * * *');
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  assert.equal(bus2.getStats().recurringJobs.jobs.length, 1);
  const fresh = bus2.scheduleRecurring('tick', () => 'y', '* * * * *');
  const jobs = bus2.getStats().recurringJobs.jobs;
  assert.equal(jobs.length, 1);
  assert.equal(jobs[0].id, fresh.id);
  assert.equal(jobs[0].dormant, undefined);
  // The dormant generation was tombstoned, so a third restart stays clean
  const tombstoned = recurringRecords(dir, 'tick').filter(
    (r) => r.cancelled === true && r.recurringJobId === old.id,
  );
  assert.equal(tombstoned.length, 1);
  const bus3 = new EventBus({ now: clock.now, durableLogDir: dir });
  assert.equal(bus3.getStats().recurringJobs.jobs.length, 1);
  assert.equal(bus3.getStats().recurringJobs.jobs[0].id, fresh.id);
});

test('restart: recovered ids are never reused', () => {
  const dir = freshDir();
  const clock = controllableClock(Date.UTC(2024, 0, 15, 10, 30, 0));
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.scheduleRecurring('a', () => 1, '* * * * *');
  bus.scheduleRecurring('b', () => 2, '* * * * *');
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  const handle = bus2.scheduleRecurring('c', () => 3, '* * * * *');
  // The counter is kept ahead of every recovered id (the EB-24
  // convention): the new id is fresh, never a reuse.
  assert.ok(handle.id !== 'recurring-1' && handle.id !== 'recurring-2', handle.id);
  assert.match(handle.id, /^recurring-\d+$/);
});

test('cron L expression fires end-to-end on the last day of the month', async () => {
  const clock = controllableClock(Date.UTC(2024, 0, 30, 12, 0, 0));
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('tick', (msg) => received.push(msg.payload));
  bus.scheduleRecurring('tick', () => 'month-end', '0 0 L * *');
  assert.equal(
    bus.getStats().recurringJobs.jobs[0].nextTriggerMs,
    Date.UTC(2024, 0, 31, 0, 0, 0, 0),
  );
  clock.nowMs = Date.UTC(2024, 0, 31, 0, 0, 1);
  bus.publish('tick', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'month-end']);
});
