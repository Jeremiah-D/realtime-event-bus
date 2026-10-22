/**
 * Codec comparison benchmark for per-topic payload compression
 * (`setTopicCompression(..., { codec })`, EB-194).
 *
 * Compares wire size and CPU for the SAME market-data tick corpus across
 * the codec registry: deflate (level 6, the default) vs brotli at
 * quality 0 / 4 (the bus default) / 11. brotli usually wins on ratio for
 * JSON text at the cost of more CPU — the table tells you exactly how
 * much on this machine.
 *
 * Run: `node bench/compress-codec.bench.ts` (Node >= 22, no build step).
 * Numbers below were measured on this machine; see README's compression
 * section for the published table.
 */
import {
  brotliCompressSync,
  brotliDecompressSync,
  constants as zlibConstants,
  deflateSync,
  inflateSync,
} from 'node:zlib';
import { Buffer } from 'node:buffer';

/** One market-data tick: the small-JSON shape compression targets. */
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

type CodecCase = {
  label: string;
  compress: (input: Buffer) => Buffer;
  decompress: (input: Buffer) => Buffer;
};

function bench(label: string, samples: string[], codec: CodecCase): void {
  const WARMUP = 200;
  const N = 2_000;
  for (let i = 0; i < WARMUP; i += 1) {
    codec.compress(Buffer.from(samples[i % samples.length], 'utf8'));
  }
  const sizes: number[] = [];
  const timesNs: number[] = [];
  let before = 0;
  for (let i = 0; i < N; i += 1) {
    const s = samples[(WARMUP + i) % samples.length];
    const input = Buffer.from(s, 'utf8');
    before += input.length;
    const t0 = process.hrtime.bigint();
    const out = codec.compress(input);
    const t1 = process.hrtime.bigint();
    sizes.push(out.length);
    timesNs.push(Number(t1 - t0));
    // Sanity: every sample must round-trip byte-identically.
    const back = codec.decompress(out).toString('utf8');
    if (back !== s) throw new Error(`${label}: round-trip mismatch on sample ${i}`);
  }
  const after = sizes.reduce((a, b) => a + b, 0);
  timesNs.sort((a, b) => a - b);
  console.log(
    `${label}: ratio ${(after / before).toFixed(3)} ` +
      `(wire ${after}B / raw ${before}B), ` +
      `p50 ${(percentile(timesNs, 50) / 1e3).toFixed(2)}µs ` +
      `p99 ${(percentile(timesNs, 99) / 1e3).toFixed(2)}µs`,
  );
}

function brotliCase(quality: number): CodecCase {
  return {
    label: `brotli q${quality}`,
    compress: (input: Buffer) =>
      brotliCompressSync(input, {
        params: { [zlibConstants.BROTLI_PARAM_QUALITY]: quality },
      }),
    decompress: (input: Buffer) => brotliDecompressSync(input),
  };
}

function main(): void {
  const CORPUS = 2_000;
  const corpus = Array.from({ length: CORPUS }, (_, i) => JSON.stringify(tick(i)));
  const rawBytes = corpus.reduce((a, s) => a + Buffer.byteLength(s, 'utf8'), 0);
  console.log(
    `tick corpus: ${CORPUS} messages, mean ${(rawBytes / CORPUS).toFixed(0)} bytes serialized`,
  );
  bench('deflate level 6 (default)', corpus, {
    label: 'deflate',
    compress: (input: Buffer) => deflateSync(input, { level: 6 }),
    decompress: (input: Buffer) => inflateSync(input),
  });
  bench('brotli quality 0        ', corpus, brotliCase(0));
  bench('brotli quality 4 (bus default)', corpus, brotliCase(4));
  bench('brotli quality 11       ', corpus, brotliCase(11));
}

main();
