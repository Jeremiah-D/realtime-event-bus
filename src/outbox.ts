/**
 * EB-102: transactional outbox relay.
 *
 * The transactional outbox pattern closes the dual-write gap between a
 * business database and the event bus: the application writes its domain
 * rows *and* one outbox row in the same DB transaction, so a crash can
 * never leave "the order was saved but the event was never published".
 * A relay drains the outbox table and publishes each record to the bus.
 *
 * The store is the application's seam (`OutboxStore`): `append` assigns a
 * monotonic `seq`, `readSince(cursor, limit)` scans forward,
 * `ackCursor` durably advances the relay's progress (the store may prune
 * acked records), and `recover` restores the durable cursor after a
 * restart. `MemoryOutboxStore` is the in-process implementation used by
 * tests and by deployments that don't need a real table; the README shows
 * the SQL shape for a Postgres-backed store.
 *
 * The relay publishes through `publishAtomic` (EB-23): one drain cycle is
 * one atomic batch, so subscribers see a batch's messages across topics
 * all at once or not at all — never a torn multi-topic write. The cursor
 * advances only after the batch commits: a crash between commit and ack
 * replays the same records after `recover()`, and every record carries a
 * stable dedup identity (`messageId = "outbox:<seq>"`, EB-27), so the
 * replay is suppressed for subscribers with `deduplicateMessages`
 * enabled — the same backstop `publishIdempotent` uses. With a `consumerId`
 * and a durable log the window is rehydrated across restarts (no
 * double-delivery after a crash); without `deduplicateMessages` the relay
 * is at-least-once after an ack loss, exactly like the raw outbox
 * pattern — consumers stay idempotent.
 *
 * A rejected batch (schema, rate-limit, ACL…) advances nothing: the
 * cursor stays on the poison record and the relay surfaces the failure
 * on `onRelayError`. The record keeps blocking the relay until the
 * operator fixes the store or advances the cursor manually with
 * `ackCursor` — a poison record is never silently skipped.
 *
 * Relay lag (`pending` records, `oldestMs` age of the oldest pending
 * record) is exposed on `getLag()` and flows into `bus.getStats()` and
 * the Prometheus exposition (`eventbus_outbox_*`), so a stalled relay
 * pages before the backlog becomes a data-loss incident.
 */

/* eslint-disable no-empty-function */

import type {
  AtomicPublishRejection,
  AtomicPublishResult,
  BatchMessage,
  EventBus,
} from './bus.ts';

/**
 * One row of the outbox table.
 */
export interface OutboxRecord {
  /**
   * Monotonic sequence assigned by the store at `append` time. Strictly
   * increasing per store; the relay's dedup identity derives from it.
   */
  seq: number;
  /** Bus topic the record publishes to. */
  topic: string;
  /**
   * The payload. Must be JSON-serializable for real (DB-backed) stores;
   * the memory store keeps the reference.
   */
  payload: unknown;
  /**
   * Optional message key: forwarded to `publishAtomic` entries, so
   * keyed-compaction topics and per-key ordering see the same key the
   * business row carried.
   */
  key?: string;
  /**
   * Optional headers. `traceparent` is propagated onto the published
   * message when present; the rest is store-level fidelity (audit,
   * operator tooling) — the bus envelope carries no generic header map.
   */
  headers?: Record<string, string>;
  /** Wall-clock ms when the record was appended. */
  at: number;
}

/**
 * The row a producer appends: like `OutboxRecord` without the
 * store-assigned `seq`. `at` defaults to the store's clock.
 */
export type OutboxAppend = Omit<OutboxRecord, 'seq' | 'at'> & {
  at?: number;
};

/**
 * The application's persistence seam. The relay only ever moves the
 * cursor forward; producers only append. A DB-backed implementation
 * writes `append` in the business transaction and persists `ackCursor`
 * (plus the prune) in its own short transaction.
 */
export interface OutboxStore {
  /**
   * Append one record, returning its assigned `seq` (strictly greater
   * than every previously assigned seq).
   */
  append(record: OutboxAppend): number;
  /**
   * Records with `seq > cursor`, in seq order, at most `limit` of them.
   * `limit <= 0` returns an empty array.
   */
  readSince(cursor: number, limit: number): OutboxRecord[];
  /**
   * Durably advance the relay cursor: every record with `seq <= cursor`
   * is published and may be pruned. Implementations must reject (throw)
   * a cursor that moves backwards.
   */
  ackCursor(cursor: number): void;
  /**
   * Restore the durable cursor after a restart (crash recovery): the
   * relay re-reads from this cursor, replaying records that were
   * published but never acked.
   */
  recover(): { cursor: number };
}

/**
 * In-process `OutboxStore`: the whole outbox lives in memory. Acked
 * records are pruned, so memory stays bounded by the unacked backlog.
 * "Crash recovery" in tests is a new `OutboxRelay` over the same store:
 * the durable cursor survives because the store object does.
 */
export class MemoryOutboxStore implements OutboxStore {
  private records: OutboxRecord[] = [];
  private nextSeq = 1;
  private cursor = 0;
  private readonly now: () => number;

  constructor(now: () => number = Date.now) {
    this.now = now;
  }

  append(record: OutboxAppend): number {
    if (typeof record.topic !== 'string' || record.topic.length === 0) {
      throw new TypeError('outbox.append: topic must be a non-empty string');
    }
    const seq = this.nextSeq++;
    this.records.push({
      seq,
      topic: record.topic,
      payload: record.payload,
      ...(record.key === undefined ? {} : { key: record.key }),
      ...(record.headers === undefined ? {} : { headers: { ...record.headers } }),
      at: record.at ?? this.now(),
    });
    return seq;
  }

  readSince(cursor: number, limit: number): OutboxRecord[] {
    if (limit <= 0) return [];
    const out: OutboxRecord[] = [];
    // Records are appended in seq order and pruned only from the front,
    // so the first record with seq > cursor starts the window.
    for (const record of this.records) {
      if (record.seq > cursor) {
        out.push(record);
        if (out.length >= limit) break;
      }
    }
    return out;
  }

  ackCursor(cursor: number): void {
    if (!Number.isInteger(cursor) || cursor < this.cursor) {
      throw new RangeError(`outbox.ackCursor: cursor must be an integer >= ${this.cursor}`);
    }
    this.cursor = cursor;
    // Prune acked records from the front (they are in seq order).
    let pruned = 0;
    while (pruned < this.records.length && this.records[pruned].seq <= cursor) {
      pruned++;
    }
    if (pruned > 0) this.records.splice(0, pruned);
  }

  recover(): { cursor: number } {
    return { cursor: this.cursor };
  }
}

/**
 * Options for `OutboxRelay`.
 */
export interface OutboxRelayOptions {
  /**
   * Poll interval for the background drain timer, in ms. Default 100.
   * `0` or negative disables the timer: the relay only drains on explicit
   * `drain()` calls (and the single drain `start()` performs).
   */
  pollIntervalMs?: number;
  /**
   * Max records per drain cycle — one `publishAtomic` batch per cycle.
   * Default 100. Must be >= 1 when set.
   */
  batchSize?: number;
  /**
   * Cap on the `pending` probe in `getLag()`: the relay counts pending
   * records with one `readSince(cursor, probeLimit)`. Default 10000 —
   * observability only, draining itself is never capped by this.
   */
  probeLimit?: number;
  /** Clock for lag accounting. Defaults to the bus's clock. */
  now?: () => number;
  /**
   * Injectable timer for tests. Defaults to the global
   * `setInterval`/`clearInterval`; the handle is `unref`ed when it
   * supports it so the relay never holds the process open.
   */
  setTimer?: (fn: () => void, ms: number) => unknown;
  clearTimer?: (handle: unknown) => void;
  /**
   * Called when a drain cycle's `publishAtomic` rejects (the poison
   * record stays; the cursor does not move) or when the store throws.
   * Throwing from the hook is swallowed — the relay must not die on a
   * monitoring callback.
   */
  onRelayError?: (err: unknown, records: OutboxRecord[]) => void;
  /** Called after a drain cycle successfully committed a batch. */
  onRelayed?: (records: OutboxRecord[], result: AtomicPublishResult) => void;
}

/**
 * Validated outbox relay tuning: every field resolved to its live value.
 */
export interface ResolvedOutboxRelayOptions {
  pollIntervalMs: number;
  batchSize: number;
  probeLimit: number;
}

/**
 * Revision for the attached outbox relay's tuning (see
 * `EventBus.setOutboxRelay`, EB-302). Only the fields provided move;
 * the rest keep their live values.
 */
export interface OutboxRelayRevision {
  /**
   * Poll interval for the background drain timer, in ms. `0` or
   * negative disables the timer. Must be a non-NaN number when
   * provided — the construction-time rule.
   */
  pollIntervalMs?: number;
  /**
   * Max records per drain cycle. Must be an integer >= 1 when
   * provided — the construction-time rule.
   */
  batchSize?: number;
  /**
   * Cap on the `pending` probe in `getLag()`. Must be an integer >= 1
   * when provided — the construction-time rule.
   */
  probeLimit?: number;
}

/**
 * Validates `OutboxRelayOptions` fail-fast, applying the
 * construction-time defaults (`pollIntervalMs` 100, `batchSize` 100,
 * `probeLimit` 10000). Used by the constructor and by
 * `EventBus.setOutboxRelay` (EB-302) so a revision is validated by
 * exactly the rule a value would have to pass at construction.
 */
export function resolveOutboxRelayOptions(
  raw: OutboxRelayOptions | undefined,
  where = 'OutboxRelay',
): ResolvedOutboxRelayOptions {
  const batchSize = raw?.batchSize ?? 100;
  if (!Number.isInteger(batchSize) || batchSize < 1) {
    throw new RangeError(`${where}: batchSize must be an integer >= 1`);
  }
  const probeLimit = raw?.probeLimit ?? 10000;
  if (!Number.isInteger(probeLimit) || probeLimit < 1) {
    throw new RangeError(`${where}: probeLimit must be an integer >= 1`);
  }
  const pollIntervalMs = raw?.pollIntervalMs ?? 100;
  if (typeof pollIntervalMs !== 'number' || Number.isNaN(pollIntervalMs)) {
    throw new RangeError(`${where}: pollIntervalMs must be a number`);
  }
  return { pollIntervalMs, batchSize, probeLimit };
}

/**
 * Outcome of one `drain()` cycle.
 */
export interface OutboxDrainResult {
  /** Records committed via `publishAtomic` this cycle (0 when idle). */
  relayed: number;
  /**
   * Present when the batch was rejected: nothing was committed and the
   * cursor did not move (see `AtomicPublishRejection`).
   */
  rejected?: AtomicPublishRejection;
}

/**
 * Relay lag snapshot: what `getStats()` and Prometheus expose.
 */
export interface OutboxLag {
  /** Records with `seq > cursor` (capped at `probeLimit`). */
  pending: number;
  /** Age of the oldest pending record in ms; 0 when none pending. */
  oldestMs: number;
  /** Last cursor acked after a successful commit. */
  cursor: number;
  /** Cumulative records committed via `publishAtomic`. */
  relayed: number;
}

/**
 * Drains an `OutboxStore` into the bus. Construct via
 * `bus.attachOutboxRelay(store, opts)` so lag flows into `getStats()`.
 */
export class OutboxRelay {
  private readonly bus: EventBus;
  private readonly store: OutboxStore;
  // Live-tuned by `retune` (EB-302): `drain` reads `batchSize` on
  // every cycle and `getLag` reads `probeLimit` on every probe, so a
  // revision governs from the next drain/probe without draining or
  // rebuilding the relay; `pollIntervalMs` re-arms the poll timer when
  // the relay is running. The cursor and the relayed count are
  // untouched by a retune.
  private pollIntervalMs: number;
  private batchSize: number;
  private probeLimit: number;
  private readonly now: () => number;
  private readonly setTimerFn: (fn: () => void, ms: number) => unknown;
  private readonly clearTimerFn: (handle: unknown) => void;
  private readonly onRelayError?: (err: unknown, records: OutboxRecord[]) => void;
  private readonly onRelayed?: (records: OutboxRecord[], result: AtomicPublishResult) => void;
  private cursor = 0;
  private relayed = 0;
  private timer: unknown = undefined;
  private started = false;

  constructor(bus: EventBus, store: OutboxStore, opts: OutboxRelayOptions = {}) {
    if (bus === undefined || bus === null) {
      throw new TypeError('OutboxRelay: bus is required');
    }
    if (store === undefined || store === null) {
      throw new TypeError('OutboxRelay: store is required');
    }
    const resolved = resolveOutboxRelayOptions(opts, 'OutboxRelay');
    this.bus = bus;
    this.store = store;
    this.pollIntervalMs = resolved.pollIntervalMs;
    this.batchSize = resolved.batchSize;
    this.probeLimit = resolved.probeLimit;
    // The clock defaults to wall time; `attachOutboxRelay` injects the
    // bus's own clock so lag accounting follows `EventBusOptions.now`.
    this.now = opts.now ?? Date.now;
    this.setTimerFn =
      opts.setTimer ?? ((fn, ms) => setInterval(fn, ms));
    this.clearTimerFn = opts.clearTimer ?? ((handle) => clearInterval(handle as ReturnType<typeof setInterval>));
    this.onRelayError = opts.onRelayError;
    this.onRelayed = opts.onRelayed;
  }

  /** The three live relay tuning values, for bus-level revision (EB-302). */
  current(): ResolvedOutboxRelayOptions {
    return {
      pollIntervalMs: this.pollIntervalMs,
      batchSize: this.batchSize,
      probeLimit: this.probeLimit,
    };
  }

  /**
   * Retunes the three relay parameters live (EB-302). The caller must
   * have validated `next` through `resolveOutboxRelayOptions` first —
   * this method applies unconditionally. The cursor and the relayed
   * count are untouched: a new `batchSize` governs from the next drain
   * cycle and a new `probeLimit` from the next `getLag()` probe. When
   * the relay is running and `pollIntervalMs` moved, the poll timer is
   * re-armed at the new interval — the old timer is cleared and a new
   * one armed, with no extra drain triggered; a stopped relay picks
   * the new interval up at the next `start()`.
   */
  retune(next: ResolvedOutboxRelayOptions): void {
    const pollChanged = next.pollIntervalMs !== this.pollIntervalMs;
    this.pollIntervalMs = next.pollIntervalMs;
    this.batchSize = next.batchSize;
    this.probeLimit = next.probeLimit;
    if (this.started && pollChanged) {
      this.disarmTimer();
      this.armTimer();
    }
  }

  /**
   * Recover the durable cursor and start draining: one immediate drain,
   * then the poll timer when `pollIntervalMs > 0`. Idempotent — calling
   * `start()` twice is a no-op.
   */
  start(): void {
    if (this.started) return;
    this.started = true;
    this.cursor = this.store.recover().cursor;
    this.drain();
    this.armTimer();
  }

  /**
   * Stop the poll timer. In-flight state is untouched — the durable
   * cursor already reflects every committed batch.
   */
  stop(): void {
    this.disarmTimer();
    this.started = false;
  }

  /** Arms the poll timer at the live `pollIntervalMs` (no-op when disabled). */
  private armTimer(): void {
    if (this.pollIntervalMs > 0) {
      const handle = this.setTimerFn(() => this.drain(), this.pollIntervalMs);
      // Never hold the process open for a background poll.
      const maybeUnref = handle as { unref?: () => void } | null | undefined;
      if (maybeUnref !== null && maybeUnref !== undefined && typeof maybeUnref.unref === 'function') {
        maybeUnref.unref();
      }
      this.timer = handle;
    }
  }

  /** Clears the poll timer when one is armed. */
  private disarmTimer(): void {
    if (this.timer !== undefined) {
      this.clearTimerFn(this.timer);
      this.timer = undefined;
    }
  }

  /**
   * One synchronous drain cycle: read up to `batchSize` records past the
   * cursor, commit them with one `publishAtomic` (all-or-nothing across
   * topics), and advance the cursor past the batch only on success.
   *
   * Every entry carries `messageId = "outbox:<seq>"`: a replay of an
   * already-committed batch (crash between commit and `ackCursor`) is
   * suppressed for subscribers with `deduplicateMessages` enabled
   * (EB-27) — the stable identity doubles as the replay guard. Without
   * the subscriber-side window the replay is a redelivery
   * (at-least-once).
   */
  drain(): OutboxDrainResult {
    let records: OutboxRecord[];
    try {
      records = this.store.readSince(this.cursor, this.batchSize);
    } catch (err) {
      this.reportError(err, []);
      return { relayed: 0 };
    }
    if (records.length === 0) return { relayed: 0 };
    const entries: BatchMessage[] = records.map((record) => ({
      topic: record.topic,
      payload: record.payload,
      messageId: `outbox:${record.seq}`,
      ...(record.key === undefined ? {} : { key: record.key }),
      ...(record.headers?.traceparent === undefined
        ? {}
        : { traceparent: record.headers.traceparent }),
    }));
    let result: AtomicPublishResult;
    try {
      result = this.bus.publishAtomic(entries);
    } catch (err) {
      // A throw (e.g. a validator that throws, or a poison key) is the
      // same as a rejection: nothing committed, cursor unmoved.
      this.reportError(err, records);
      return { relayed: 0 };
    }
    if (result.rejected !== undefined) {
      this.reportError(
        new Error(
          `outbox relay: batch rejected at index ${result.rejected.index} ` +
            `(topic ${result.rejected.topic}): ${result.rejected.reason}`,
        ),
        records,
      );
      return { relayed: 0, rejected: result.rejected };
    }
    try {
      this.store.ackCursor(records[records.length - 1].seq);
    } catch (err) {
      // The batch committed but the ack did not durably land: the next
      // drain replays these records, and their `outbox:<seq>` messageIds
      // suppress the duplicates. Report loudly, but do not pretend the
      // cursor moved.
      this.reportError(err, records);
      return { relayed: 0 };
    }
    this.cursor = records[records.length - 1].seq;
    this.relayed += records.length;
    try {
      this.onRelayed?.(records, result);
    } catch {
      // A monitoring hook must never break the relay.
    }
    return { relayed: records.length };
  }

  /**
   * Current lag: pending backlog depth, oldest-pending age, the durable
   * cursor, and the cumulative relayed count. `pending` is probed with
   * one `readSince(cursor, probeLimit)` — capped for observability.
   */
  getLag(): OutboxLag {
    const probe = this.store.readSince(this.cursor, this.probeLimit);
    const oldest = probe.length === 0 ? 0 : Math.max(0, this.now() - probe[0].at);
    return {
      pending: probe.length,
      oldestMs: oldest,
      cursor: this.cursor,
      relayed: this.relayed,
    };
  }

  private reportError(err: unknown, records: OutboxRecord[]): void {
    try {
      this.onRelayError?.(err, records);
    } catch {
      // Swallowed: error reporting must never kill the relay.
    }
  }
}
