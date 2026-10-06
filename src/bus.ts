import { BoundedQueue, type DropPolicy } from './backpressure.ts';

export interface BusMessage {
  topic: string;
  payload: unknown;
}

export type MessageHandler = (msg: BusMessage) => void;

export interface SubscribeOptions {
  /** Per-subscriber bounded queue capacity (default 100). */
  queueSize?: number;
  /** Drop policy when the queue is full (default 'drop-oldest'). */
  dropPolicy?: DropPolicy;
  /**
   * Called when the subscriber falls behind: its queue reached the high-water
   * mark (80% of capacity). Fires once per excursion and re-arms after the
   * queue drains below the mark — use it to shed load upstream, alert an
   * operator, or degrade gracefully instead of silently dropping messages.
   */
  onBackpressure?: (event: BackpressureEvent) => void;
}

/** Snapshot of a subscriber's backpressure state when `onBackpressure` fires. */
export interface BackpressureEvent {
  /** The subscriber whose queue is filling up. */
  subscriberId: string;
  /** The topic pattern the lagging subscriber registered. */
  pattern: string;
  /** Queue size when the event fired. */
  queueSize: number;
  /** Configured per-subscriber queue capacity. */
  capacity: number;
  /** Total messages dropped for this subscriber so far (same as `droppedCount`). */
  dropped: number;
}

export interface Subscription {
  id: string;
  unsubscribe(): void;
}

/** Per-topic stats kept live by the bus. */
export interface TopicStats {
  /** The concrete topic name (as published, never a pattern). */
  topic: string;
  /**
   * Subscribers matched by the most recent publish to this topic — the
   * current fan-out width, so operators can see hot topics at a glance.
   */
  subscriberCount: number;
  /** Total messages published to this topic since the bus was created. */
  publishedMessages: number;
}

/** Point-in-time snapshot returned by `EventBus.getStats()`. */
export interface BusStats {
  /** Currently active subscriptions. */
  totalSubscribers: number;
  /** Subscriptions grouped by the exact pattern string that was registered. */
  subscribersByPattern: Record<string, number>;
  /** Total messages accepted via `publish`/`publishBatch`. */
  totalPublished: number;
  /**
   * Stats for every concrete topic that has seen at least one publish,
   * in order of first publish.
   */
  topics: TopicStats[];
}

interface Subscriber {
  id: string;
  pattern: string;
  handler: MessageHandler;
  queue: BoundedQueue<BusMessage>;
}

function matches(pattern: string, topic: string): boolean {
  // A lone `*` or `**` matches every topic.
  if (pattern === '*' || pattern === '**') return true;
  const patternParts = pattern.split('.');
  const topicParts = topic.split('.');
  return matchSegments(patternParts, topicParts);
}

/**
 * Segment matcher where `*` matches exactly one topic segment and `**`
 * matches zero or more segments (AMQP/MQTT-style multi-level wildcard, e.g.
 * `market.**` matches `market`, `market.btc` and `market.btc.trades`).
 */
function matchSegments(pattern: string[], topic: string[]): boolean {
  if (pattern.length === 0) return topic.length === 0;
  const [head, ...rest] = pattern;
  if (head === '**') {
    // Try consuming 0..topic.length segments for the wildcard.
    for (let i = 0; i <= topic.length; i += 1) {
      if (matchSegments(rest, topic.slice(i))) return true;
    }
    return false;
  }
  if (topic.length === 0) return false;
  if (head !== '*' && head !== topic[0]) return false;
  return matchSegments(rest, topic.slice(1));
}

export class EventBus {
  private subscribers = new Map<string, Subscriber>();
  private subscribersByPattern = new Map<string, number>();
  private topicStats = new Map<string, { subscriberCount: number; publishedMessages: number }>();
  private totalPublished = 0;
  private nextId = 0;
  private flushScheduled = false;

  /**
   * Registers a handler for topics matching `topicPattern`.
   * `*` matches a single topic segment (`market.*` matches `market.btc`);
   * `**` matches zero or more segments (`market.**` matches `market`,
   * `market.btc` and `market.btc.trades`); a bare `*` or `**` matches every
   * topic.
   *
   * Pass `onBackpressure` in `opts` to be notified when this subscriber falls
   * behind (its queue reaches the high-water mark); `droppedCount` reports how
   * many of its messages were shed.
   */
  subscribe(topicPattern: string, handler: MessageHandler, opts?: SubscribeOptions): Subscription {
    const id = `sub-${++this.nextId}`;
    const capacity = opts?.queueSize ?? 100;
    const onBackpressure = opts?.onBackpressure;
    const queue = new BoundedQueue<BusMessage>({
      capacity,
      policy: opts?.dropPolicy ?? 'drop-oldest',
      ...(onBackpressure == null
        ? {}
        : {
            onHighWaterMark: (size: number) =>
              onBackpressure({
                subscriberId: id,
                pattern: topicPattern,
                queueSize: size,
                capacity,
                dropped: queue.droppedCount,
              }),
          }),
    });
    const subscriber: Subscriber = { id, pattern: topicPattern, handler, queue };
    this.subscribers.set(id, subscriber);
    this.subscribersByPattern.set(topicPattern, (this.subscribersByPattern.get(topicPattern) ?? 0) + 1);

    return {
      id,
      unsubscribe: () => {
        if (!this.subscribers.delete(id)) return;
        const remaining = (this.subscribersByPattern.get(topicPattern) ?? 1) - 1;
        if (remaining <= 0) this.subscribersByPattern.delete(topicPattern);
        else this.subscribersByPattern.set(topicPattern, remaining);
      },
    };
  }

  /**
   * Fans the message out to every matching subscriber's bounded queue and
   * returns the number of subscribers whose queue accepted the message.
   * When a queue is full, the subscriber's drop policy sheds a message and
   * counts the drop (see `droppedCount`); delivery happens on the next
   * microtask so slow consumers exert real backpressure.
   */
  publish(topic: string, payload: unknown): number {
    const { accepted } = this.fanOut({ topic, payload });
    this.scheduleFlush();
    return accepted;
  }

  /**
   * Fans out every message in the batch to its matching subscribers' bounded
   * queues, then schedules a single flush for the whole batch — so N messages
   * cost one event-loop round instead of N, at the price of one larger drain.
   * Matching, drop policies, and backpressure callbacks behave exactly as
   * they do for `publish`; returns the total number of accepted deliveries.
   * An empty batch is a no-op that schedules no flush.
   */
  publishBatch(messages: BusMessage[]): number {
    if (messages.length === 0) return 0;
    let accepted = 0;
    for (const msg of messages) {
      accepted += this.fanOut({ topic: msg.topic, payload: msg.payload }).accepted;
    }
    this.scheduleFlush();
    return accepted;
  }

  /**
   * Pushes one message into every matching subscriber's queue and records
   * per-topic stats. Returns how many subscribers matched and how many
   * queues accepted the message (they differ when backpressure drops kick
   * in). Does not schedule a flush — callers do that once per batch.
   */
  private fanOut(msg: BusMessage): { matched: number; accepted: number } {
    let matched = 0;
    let accepted = 0;
    for (const subscriber of this.subscribers.values()) {
      if (!matches(subscriber.pattern, msg.topic)) continue;
      matched += 1;
      if (subscriber.queue.push(msg) === 'accepted') accepted += 1;
    }
    let stats = this.topicStats.get(msg.topic);
    if (stats == null) {
      stats = { subscriberCount: 0, publishedMessages: 0 };
      this.topicStats.set(msg.topic, stats);
    }
    stats.subscriberCount = matched;
    stats.publishedMessages += 1;
    this.totalPublished += 1;
    return { matched, accepted };
  }

  /**
   * Returns a point-in-time snapshot of live bus metrics: active
   * subscriptions (total and per pattern) plus per-topic fan-out widths and
   * publish counts. The snapshot is a plain-data copy — mutating it does
   * not affect the bus.
   */
  getStats(): BusStats {
    return {
      totalSubscribers: this.subscribers.size,
      subscribersByPattern: Object.fromEntries(this.subscribersByPattern),
      totalPublished: this.totalPublished,
      topics: [...this.topicStats.entries()].map(([topic, stats]) => ({
        topic,
        subscriberCount: stats.subscriberCount,
        publishedMessages: stats.publishedMessages,
      })),
    };
  }

  /** Number of active subscriptions. */
  subscriberCount(): number {
    return this.subscribers.size;
  }

  /** Messages currently buffered for a subscriber, awaiting delivery. */
  pendingCount(subId: string): number {
    const subscriber = this.subscribers.get(subId);
    if (subscriber == null) throw new Error(`unknown subscriber: ${subId}`);
    return subscriber.queue.size;
  }

  /** Messages dropped for a subscriber due to backpressure. */
  droppedCount(subId: string): number {
    const subscriber = this.subscribers.get(subId);
    if (subscriber == null) throw new Error(`unknown subscriber: ${subId}`);
    return subscriber.queue.droppedCount;
  }

  private scheduleFlush(): void {
    if (this.flushScheduled) return;
    this.flushScheduled = true;
    queueMicrotask(() => {
      this.flushScheduled = false;
      for (const subscriber of this.subscribers.values()) {
        for (const msg of subscriber.queue.drain()) {
          subscriber.handler(msg);
        }
      }
    });
  }
}
