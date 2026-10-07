/**
 * Prefix-index before/after benchmark for publish-side fan-out.
 *
 * Uses only the public API (`EventBus`, `subscribe`, `publish`,
 * `subscriberCount`) so the same script measures both the old linear-scan
 * `fanOut` and the new prefix-inverted-index `fanOut` — see README "Prefix
 * index" for the before/after protocol (`git stash push -- src/bus.ts`).
 *
 * Scenario A (index-friendly): 5,000 subscribers with mutually distinct
 * literal prefixes (`t0.*` … `t4999.*`); each publish goes to `t42.price`,
 * which matches exactly one of them. The index reduces 5,000 regex tests
 * to a handful of map lookups plus one confirmation.
 *
 * Scenario B (degenerate control): 5,000 subscribers all on `**` — every
 * pattern files under the empty key, so the index degrades to the full
 * candidate set and the numbers isolate the index's own overhead.
 *
 * Each sample measures the synchronous `publish()` cost (pattern matching
 * + queue enqueues) and drains the queues on a microtask before the next
 * sample, so there are no backpressure drops in the measured path.
 *
 * Run: `node bench/index.bench.ts` (Node >= 22, no build step).
 */
import { EventBus } from '../src/bus.ts';

const SUBSCRIBERS = 5_000;
const WARMUP = 1_000;
const SAMPLES = 2_000;

function percentile(sorted: number[], p: number): number {
  // Nearest-rank: the smallest value with at least p% of samples <= it.
  const rank = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, Math.min(sorted.length - 1, rank))];
}

async function measure(topic: string, setup: (bus: EventBus) => void): Promise<number[]> {
  const bus = new EventBus();
  setup(bus);
  if (bus.subscriberCount() !== SUBSCRIBERS) {
    throw new Error(`expected ${SUBSCRIBERS} subscribers, got ${bus.subscriberCount()}`);
  }
  for (let i = 0; i < WARMUP; i += 1) {
    bus.publish(topic, { i });
    // eslint-disable-next-line no-await-in-loop
    await Promise.resolve(); // drain queues before the next sample
  }
  const samplesNs: number[] = new Array(SAMPLES);
  for (let i = 0; i < SAMPLES; i += 1) {
    const start = process.hrtime.bigint();
    bus.publish(topic, { i });
    samplesNs[i] = Number(process.hrtime.bigint() - start);
    // eslint-disable-next-line no-await-in-loop
    await Promise.resolve(); // drain queues before the next sample
  }
  return samplesNs;
}

function summarize(name: string, samplesNs: number[]): void {
  const sorted = [...samplesNs].sort((a, b) => a - b);
  const toUs = (ns: number): number => ns / 1_000;
  const mean = samplesNs.reduce((sum, v) => sum + v, 0) / samplesNs.length;
  console.log(`${name} (${SAMPLES} samples, warmup ${WARMUP})`);
  console.log(`  mean  ${toUs(mean).toFixed(2)} µs`);
  console.log(`  p50   ${toUs(percentile(sorted, 50)).toFixed(2)} µs`);
  console.log(`  p95   ${toUs(percentile(sorted, 95)).toFixed(2)} µs`);
  console.log(`  p99   ${toUs(percentile(sorted, 99)).toFixed(2)} µs`);
  console.log(`  min   ${toUs(sorted[0]).toFixed(2)} µs   max ${toUs(sorted[sorted.length - 1]).toFixed(2)} µs`);
}

async function main(): Promise<void> {
  console.log(`node ${process.version} (${process.platform}/${process.arch})`);

  // Scenario A: 5,000 distinct literal prefixes; the publish matches one.
  const distinct = await measure('t42.price', (bus) => {
    for (let i = 0; i < SUBSCRIBERS; i += 1) bus.subscribe(`t${i}.*`, () => {});
  });
  summarize('A  distinct prefixes (t0.*..t4999.*), publish t42.price -> 1 match', distinct);

  // Scenario B: 5,000 subscribers on '**' — index degrades to full scan.
  const degenerate = await measure('t42.price', (bus) => {
    for (let i = 0; i < SUBSCRIBERS; i += 1) bus.subscribe('**', () => {});
  });
  summarize('B  all ** (degenerate control), publish t42.price -> 5000 matches', degenerate);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
