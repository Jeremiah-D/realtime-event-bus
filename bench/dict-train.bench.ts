/**
 * Auto-trained dictionary benchmark (EB-60): `setTopicCompression` with
 * `autoTrain` derives the preset dictionary from the recent publish
 * corpus via the trailing-window heuristic, instead of a hand-built one.
 *
 * Compares wire size and deflate CPU for SMALL JSON payloads — a
 * market-data tick shape (~130 bytes serialized) — with no dictionary
 * versus a dictionary trained from the first 300 samples and then applied
 * to the full 2,000-message corpus (samples 300+ are unseen by the
 * trainer, so the ratio measures generalization, not memorization).
 *
 * Run: `node bench/dict-train.bench.ts` (Node >= 22, no build step).
 * Numbers below were measured on this machine; see README's compression
 * section for the published table.
 */
import { deflateSync, inflateSync } from 'node:zlib';
import { Buffer } from 'node:buffer';

/** One market-data tick: the small-JSON shape this feature targets. */
function tick(i: number): Record<string, unknown> {
  return {
    symbol: i % 3 === 0 ? 'BTCUSDT' : i % 3 === 1 ? 'ETHUSDT' : 'SOLUSDT',
    venue: 'binance-spot',
    side: i % 2 === 0 ? 'buy' : 'sell',
    price: 97123.45 + (i % 100) * 0.01,
    qty: 0.001 * (1 + (i % 7)),
    trade_id: 8_412_000_000 + i,
    ts: 1_759_400_000_000 + i * 37,
    taker: i % 5 === 0,
  };
}

function percentile(sorted: number[], p: number): number {
  const rank = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, Math.min(sorted.length - 1, rank))];
}

/**
 * The exact derivation the bus's auto-trainer uses: concatenate the
 * window's samples and take the trailing up-to-32 KiB as the dictionary
 * candidate.
 */
function trainDictionary(samples: string[]): Buffer {
  const joined = Buffer.from(samples.join('\n'), 'utf8');
  return joined.length > 32 * 1024 ? joined.subarray(joined.length - 32 * 1024) : joined;
}

function bench(label: string, samples: string[], dictionary?: Buffer): void {
  const WARMUP = 500;
  const N = 5_000;
  for (let i = 0; i < WARMUP; i += 1) {
    deflateSync(samples[i % samples.length], dictionary ? { dictionary } : undefined);
  }
  const sizes: number[] = [];
  const timesNs: number[] = [];
  let before = 0;
  for (let i = 0; i < N; i += 1) {
    const s = samples[(WARMUP + i) % samples.length];
    before += Buffer.byteLength(s, 'utf8');
    const t0 = process.hrtime.bigint();
    const out = deflateSync(s, dictionary ? { dictionary } : undefined);
    const t1 = process.hrtime.bigint();
    sizes.push(out.length);
    timesNs.push(Number(t1 - t0));
    // Sanity: every sample must round-trip byte-identically.
    const back = (
      dictionary ? inflateSync(out, { dictionary }) : inflateSync(out)
    ).toString('utf8');
    if (back !== s) throw new Error(`${label}: round-trip mismatch on sample ${i}`);
  }
  const after = sizes.reduce((a, b) => a + b, 0);
  timesNs.sort((a, b) => a - b);
  console.log(
    `${label}: ratio ${(after / before).toFixed(3)} ` +
      `(wire ${after}B / raw ${before}B), ` +
      `deflate p50 ${(percentile(timesNs, 50) / 1e3).toFixed(2)}µs ` +
      `p99 ${(percentile(timesNs, 99) / 1e3).toFixed(2)}µs`,
  );
}

function main(): void {
  const CORPUS = 2_000;
  const corpus = Array.from({ length: CORPUS }, (_, i) => JSON.stringify(tick(i)));
  const rawBytes = corpus.reduce((a, s) => a + Buffer.byteLength(s, 'utf8'), 0);
  console.log(`tick corpus: ${CORPUS} messages, mean ${(rawBytes / CORPUS).toFixed(0)} bytes serialized`);
  const t0 = process.hrtime.bigint();
  const dictionary = trainDictionary(corpus.slice(0, 300));
  const t1 = process.hrtime.bigint();
  console.log(
    `auto-trained dictionary: ${dictionary.length} bytes from 300 samples, ` +
      `derivation took ${(Number(t1 - t0) / 1e6).toFixed(2)}ms (one-time, off the publish path)`,
  );
  bench('no dictionary      ', corpus);
  bench('auto-trained dict  ', corpus, dictionary);
}

main();
