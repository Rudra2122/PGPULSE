import { pool, query } from '../db/postgres';
import { cachedQuery, flushCache, redis } from '../cache/redis';
import { profileQuery } from '../profiler/queryProfiler';
import { detectBottlenecks, printBottleneckReport } from '../profiler/bottleneckDetector';
import {
  recordQueryLatency,
  recordCacheHitRate,
  recordThroughput,
  recordBottleneck,
} from '../metrics/datadog';

const CONCURRENCY = 20;
const ITERATIONS = 100;

const TEST_QUERIES = [
  {
    name: 'orders_by_status',
    sql: `SELECT o.id, o.total, o.status, u.email, u.name
          FROM orders o
          JOIN users u ON o.user_id = u.id
          WHERE o.status = $1
          ORDER BY o.created_at DESC
          LIMIT 20`,
    params: ['pending'],
  },
  {
    name: 'user_order_history',
    sql: `SELECT o.id, o.total, o.status, o.created_at
          FROM orders o
          WHERE o.user_id = $1
          ORDER BY o.created_at DESC`,
    params: [42],
  },
  {
    name: 'category_revenue',
    sql: `SELECT p.category,
                 COUNT(oi.id) AS items_sold,
                 SUM(oi.quantity * oi.unit_price) AS revenue
          FROM order_items oi
          JOIN products p ON oi.product_id = p.id
          GROUP BY p.category
          ORDER BY revenue DESC`,
    params: [],
  },
];

async function runConcurrent(
  fn: () => Promise<{ duration: number; fromCache?: boolean }>,
  concurrency: number,
  iterations: number
): Promise<{ latencies: number[]; cacheHits: number }> {
  const latencies: number[] = [];
  let cacheHits = 0;
  const batches = Math.ceil(iterations / concurrency);

  for (let b = 0; b < batches; b++) {
    const batchSize = Math.min(concurrency, iterations - b * concurrency);
    const results = await Promise.all(Array.from({ length: batchSize }, fn));
    results.forEach((r) => {
      latencies.push(r.duration);
      if (r.fromCache) cacheHits++;
    });
  }

  return { latencies, cacheHits };
}

function pct(sorted: number[], p: number): number {
  return sorted[Math.max(0, Math.ceil((p / 100) * sorted.length) - 1)];
}

function printStats(label: string, latencies: number[], cacheHits: number, totalMs: number) {
  const sorted = [...latencies].sort((a, b) => a - b);
  const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length;
  const rps = (latencies.length / totalMs) * 1000;

  console.log(`\n   📊 ${label}`);
  console.log(
    `      avg:${avg.toFixed(1)}ms  p50:${pct(sorted, 50)}ms  p95:${pct(sorted, 95)}ms  p99:${pct(sorted, 99)}ms`
  );
  console.log(
    `      throughput:${rps.toFixed(1)} req/s  cache:${cacheHits}/${latencies.length} (${((cacheHits / latencies.length) * 100).toFixed(1)}%)`
  );

  return { avg, p95: pct(sorted, 95), rps };
}

async function addIndexes() {
  console.log('\n   🔧 Adding indexes...');
  await pool.query(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_status      ON orders(status)`);
  await pool.query(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_user_id     ON orders(user_id)`);
  await pool.query(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_created_at  ON orders(created_at DESC)`);
  await pool.query(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_oi_product_id      ON order_items(product_id)`);
  await pool.query(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_oi_order_id        ON order_items(order_id)`);
  console.log('   ✅ Indexes ready');
}

async function dropIndexes() {
  await pool.query(`DROP INDEX IF EXISTS idx_orders_status`);
  await pool.query(`DROP INDEX IF EXISTS idx_orders_user_id`);
  await pool.query(`DROP INDEX IF EXISTS idx_orders_created_at`);
  await pool.query(`DROP INDEX IF EXISTS idx_oi_product_id`);
  await pool.query(`DROP INDEX IF EXISTS idx_oi_order_id`);
}

export async function runBenchmark() {
  console.log(`\n🚀 PGPulse — Concurrency:${CONCURRENCY}  Iterations:${ITERATIONS}/query\n`);

  await dropIndexes();
  await flushCache();

  for (const q of TEST_QUERIES) {
    console.log('\n' + '═'.repeat(65));
    console.log(`Query: ${q.name}`);

    // Phase 1 — Profile without indexes
    const beforeProfile = await profileQuery(q.sql, q.params);
    const bottlenecks = detectBottlenecks(beforeProfile);
    printBottleneckReport(q.name, beforeProfile, bottlenecks);
    bottlenecks.forEach((b) => recordBottleneck(q.name, b.type, b.severity));

    // Phase 2 — No index, no cache
    await flushCache();
    const t1 = Date.now();
    const { latencies: l1, cacheHits: c1 } = await runConcurrent(
      async () => {
        const r = await query(q.sql, q.params);
        return { duration: r.duration };
      },
      CONCURRENCY,
      ITERATIONS
    );
    const s1 = printStats('No Index | No Cache (baseline)', l1, c1, Date.now() - t1);
    recordQueryLatency(q.name, s1.p95, false);
    recordThroughput(q.name, s1.rps);

    // Phase 3 — No index, with cache
    await flushCache();
    const t2 = Date.now();
    const { latencies: l2, cacheHits: c2 } = await runConcurrent(
      () => cachedQuery(`noindex:${q.name}`, q.sql, q.params),
      CONCURRENCY,
      ITERATIONS
    );
    const s2 = printStats('No Index | With Cache', l2, c2, Date.now() - t2);
    recordCacheHitRate(c2, ITERATIONS);

    // Phase 4 — With index, with cache
    await addIndexes();
    await flushCache();
    const afterProfile = await profileQuery(q.sql, q.params);
    const t3 = Date.now();
    const { latencies: l3, cacheHits: c3 } = await runConcurrent(
      () => cachedQuery(`opt:${q.name}`, q.sql, q.params),
      CONCURRENCY,
      ITERATIONS
    );
    const s3 = printStats('With Index | With Cache (optimized)', l3, c3, Date.now() - t3);
    recordQueryLatency(q.name, s3.p95, true);

    const latImp = (((s1.p95 - s3.p95) / s1.p95) * 100).toFixed(1);
    const rpsImp = (((s3.rps - s1.rps) / s1.rps) * 100).toFixed(1);
    console.log(`\n   ✅ p95 improved ${latImp}% | throughput improved ${rpsImp}%`);
    console.log(
      `      SeqScan before:${beforeProfile.hasSeqScan ? 'YES' : 'NO'} → after:${afterProfile.hasSeqScan ? 'YES' : 'NO'}`
    );

    await dropIndexes();
  }

  console.log('\n' + '═'.repeat(65));
  console.log('🏁 Done. Metrics emitted → pgpulse.* namespace in Datadog.\n');

  await pool.end();
  await redis.quit();
}