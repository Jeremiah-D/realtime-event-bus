/**
 * 10k-subscriber fan-out latency benchmark for EventBus.
 *
 * Measures the synchronous cost of a single `publish()` — i.e. matching the
 * topic against 10,000 subscription patterns and enqueuing the message into
 * every matching subscriber's bounded queue. Each publish is followed by one
 * microtask yield so the bus flushes and every queue drains before the next
 * sample, keeping the measured path free of backpressure drops.
 *
 * Run: `node bench/fanout.bench.ts` (Node >= 22, no build step).
 */
import { EventBus } from '../src/bus.ts';

const SUBSCRIBERS = 10_000;
const WARMUP = 1_000;
const SAMPLES = 2_000;
const TOPIC = 'market.btc.trades';

function percentile(sorted: number[], p: number): number {
  // Nearest-rank: the smallest value with at least p% of samples <= it.
  const rank = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, Math.min(sorted.length - 1, rank))];
}

async function main(): Promise<void> {
  const bus = new EventBus();

  // Pattern mix a market-data feed would plausibly see: exact topic,
  // single-level wildcard, and multi-level wildcard subscribers.
  for (let i = 0; i < 6_000; i += 1) bus.subscribe(TOPIC, () => {});
  for (let i = 0; i < 2_500; i += 1) bus.subscribe('market.btc.*', () => {});
  for (let i = 0; i < 1_500; i += 1) bus.subscribe('market.**', () => {});
  if (bus.subscriberCount() !== SUBSCRIBERS) {
    throw new Error(`expected ${SUBSCRIBERS} subscribers, got ${bus.subscriberCount()}`);
  }

  // Warm up the JIT with realistic traffic.
  for (let i = 0; i < WARMUP; i += 1) {
    bus.publish(TOPIC, { seq: i });
    // eslint-disable-next-line no-await-in-loop
    await Promise.resolve(); // let the scheduled microtask flush drain queues
  }

  const samplesNs: number[] = new Array(SAMPLES);
  for (let i = 0; i < SAMPLES; i += 1) {
    const start = process.hrtime.bigint();
    bus.publish(TOPIC, { seq: i });
    const end = process.hrtime.bigint();
    samplesNs[i] = Number(end - start);
    // eslint-disable-next-line no-await-in-loop
    await Promise.resolve(); // drain before the next sample
  }

  const sorted = [...samplesNs].sort((a, b) => a - b);
  const toUs = (ns: number): number => ns / 1_000;
  const meanNs = samplesNs.reduce((sum, v) => sum + v, 0) / samplesNs.length;
  const meanS = meanNs / 1e9;

  const report = {
    environment: {
      node: process.version,
      platform: `${process.platform}/${process.arch}`,
      v8: process.versions.v8,
    },
    setup: {
      subscribers: SUBSCRIBERS,
      patternMix: { exact: 6_000, singleWildcard: 2_500, multiWildcard: 1_500 },
      topic: TOPIC,
      warmup: WARMUP,
      samples: SAMPLES,
      measured: 'publish() fan-out latency (pattern match + 10k queue enqueues), µs',
    },
    latencyUs: {
      mean: toUs(meanNs),
      p50: toUs(percentile(sorted, 50)),
      p95: toUs(percentile(sorted, 95)),
      p99: toUs(percentile(sorted, 99)),
      min: toUs(sorted[0]),
      max: toUs(sorted[sorted.length - 1]),
    },
    throughput: {
      publishesPerSec: 1 / meanS,
      deliveriesPerSec: SUBSCRIBERS / meanS,
    },
  };

  const { latencyUs, throughput } = report;
  console.log(`fan-out latency, ${SUBSCRIBERS} subscribers (${SAMPLES} samples, warmup ${WARMUP})`);
  console.log(`  mean  ${latencyUs.mean.toFixed(2)} µs`);
  console.log(`  p50   ${latencyUs.p50.toFixed(2)} µs`);
  console.log(`  p95   ${latencyUs.p95.toFixed(2)} µs`);
  console.log(`  p99   ${latencyUs.p99.toFixed(2)} µs`);
  console.log(`  min   ${latencyUs.min.toFixed(2)} µs   max ${latencyUs.max.toFixed(2)} µs`);
  console.log(
    `throughput: ${throughput.publishesPerSec.toFixed(0)} publishes/s ` +
      `(${(throughput.deliveriesPerSec / 1e6).toFixed(2)}M deliveries/s)`,
  );
  console.log(JSON.stringify(report));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
