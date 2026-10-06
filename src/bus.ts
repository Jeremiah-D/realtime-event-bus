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
  // A lone `*` matches every topic.
  if (pattern === '*') return true;
  const patternParts = pattern.split('.');
  const topicParts = topic.split('.');
  if (patternParts.length !== topicParts.length) return false;
  return patternParts.every((part, i) => part === '*' || part === topicParts[i]);
}

export class EventBus {
  private subscribers = new Map<string, Subscriber>();
  private nextId = 0;
  private flushScheduled = false;

  /**
   * Registers a handler for topics matching `topicPattern`.
   * `*` matches a single topic segment (`market.*` matches `market.btc`);
   * a bare `*` pattern matches every topic.
   */
  subscribe(topicPattern: string, handler: MessageHandler, opts?: SubscribeOptions): Subscription {
    const id = `sub-${++this.nextId}`;
    const queue = new BoundedQueue<BusMessage>({
      capacity: opts?.queueSize ?? 100,
      policy: opts?.dropPolicy ?? 'drop-oldest',
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
