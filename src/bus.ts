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

    return {
      id,
      unsubscribe: () => {
        this.subscribers.delete(id);
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
    let delivered = 0;
    const msg: BusMessage = { topic, payload };
    for (const subscriber of this.subscribers.values()) {
      if (!matches(subscriber.pattern, topic)) continue;
      if (subscriber.queue.push(msg) === 'accepted') delivered += 1;
    }
    this.scheduleFlush();
    return delivered;
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
