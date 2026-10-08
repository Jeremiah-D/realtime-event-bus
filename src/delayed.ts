/**
 * Delayed delivery scheduling primitives for the event bus.
 *
 * `publishDelayed` accepts a message for future delivery instead of fanning
 * it out immediately. Scheduled entries wait in a min-heap ordered by their
 * due time (`deliverAt`); the bus fans an entry out through the normal
 * publish pipeline once the bus clock reaches its due time. Cancellation is
 * lazy: `cancelDelayed` marks the entry and drops it from the id index, and
 * the heap skips marked entries when they surface at the top — so cancel is
 * O(1) and never needs an arbitrary-position heap removal.
 */

/** One scheduled delayed delivery waiting in the timer heap. */
export interface DelayedEntry {
  /** Opaque handle returned by `publishDelayed` (`delayed-1`, …). */
  id: string;
  /** Concrete topic the message will be published to at its due time. */
  topic: string;
  /** Payload as passed to `publishDelayed`; fanned out verbatim when due. */
  payload: unknown;
  /** Bus-clock timestamp in milliseconds when the message becomes due. */
  deliverAt: number;
  /**
   * TTL expiry deadline stamped at schedule time (bus clock), when a TTL
   * rule matched the topic. A message whose deadline passes before its due
   * time is dropped as expired at sweep time, never delivered.
   */
  expiresAt?: number;
  /** Set by `cancelDelayed`; the heap skips marked entries lazily. */
  cancelled: boolean;
}

interface HeapNode {
  entry: DelayedEntry;
  /** Insertion order; breaks `deliverAt` ties deterministically. */
  order: number;
}

/**
 * Binary min-heap of delayed entries ordered by `deliverAt` (insertion
 * order breaks ties, so same-due messages fan out in schedule order).
 * Supports peek/pop/push only — cancellation is lazy via
 * `DelayedEntry.cancelled`, never an in-place removal.
 */
export class DelayHeap {
  private nodes: HeapNode[] = [];
  private nextOrder = 0;

  /** Entries currently in the heap, including lazily-cancelled ones. */
  get size(): number {
    return this.nodes.length;
  }

  /** The earliest-due entry without removing it, if any. */
  peek(): DelayedEntry | undefined {
    return this.nodes[0]?.entry;
  }

  /** Adds an entry to the heap. */
  push(entry: DelayedEntry): void {
    const node: HeapNode = { entry, order: this.nextOrder++ };
    this.nodes.push(node);
    this.siftUp(this.nodes.length - 1);
  }

  /** Removes and returns the earliest-due entry, if any. */
  pop(): DelayedEntry | undefined {
    const nodes = this.nodes;
    const top = nodes[0];
    if (top == null) return undefined;
    const last = nodes.pop()!;
    if (nodes.length > 0) {
      nodes[0] = last;
      this.siftDown(0);
    }
    return top.entry;
  }

  private less(a: HeapNode, b: HeapNode): boolean {
    if (a.entry.deliverAt !== b.entry.deliverAt) return a.entry.deliverAt < b.entry.deliverAt;
    return a.order < b.order;
  }

  private siftUp(index: number): void {
    const nodes = this.nodes;
    while (index > 0) {
      const parent = (index - 1) >> 1;
      if (!this.less(nodes[index], nodes[parent])) break;
      [nodes[index], nodes[parent]] = [nodes[parent], nodes[index]];
      index = parent;
    }
  }

  private siftDown(index: number): void {
    const nodes = this.nodes;
    for (;;) {
      const left = index * 2 + 1;
      const right = left + 1;
      let smallest = index;
      if (left < nodes.length && this.less(nodes[left], nodes[smallest])) smallest = left;
      if (right < nodes.length && this.less(nodes[right], nodes[smallest])) smallest = right;
      if (smallest === index) break;
      [nodes[index], nodes[smallest]] = [nodes[smallest], nodes[index]];
      index = smallest;
    }
  }
}
