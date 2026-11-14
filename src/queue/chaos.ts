import { ChildProcess, spawn } from 'child_process';
import fs from 'fs';
import path from 'path';
import readline from 'readline';
import IORedis from 'ioredis';
import { Queue } from 'bullmq';
import dotenv from 'dotenv';
import { currentPrimary, isSentinelMode, redisOptions } from './connection';
import { JOB_RETRY, QUEUE_NAME, ensureSchema, jobIdFor } from './settlement';
import { PrimaryPool } from '../db/primaryPool';

dotenv.config();

// Chaos run: steady traffic for DURATION_S, crash things mid-run, measure availability.
//
//   FAULTS="redis@0.33,postgres@0.66"   crash the Redis primary at 33% of the run, the Postgres primary at 66%
//   FAULTS="none"                        same traffic, no faults (control run)
//
// Availability = share of 1-second windows (during the traffic window) in which at least one
// payment was settled. Every second where settlements stopped counts as downtime.
const RATE = Number(process.env.RATE || 200); // jobs per second offered
const DURATION_S = Number(process.env.DURATION_S || 300);
const WORKERS = Number(process.env.WORKERS || 4);
const DRAIN_TIMEOUT_S = Number(process.env.DRAIN_TIMEOUT_S || 180);
const REDRIVE_AFTER_S = Number(process.env.REDRIVE_AFTER_S || 15);
const FAULTS = (process.env.FAULTS ?? 'redis@0.33,postgres@0.66')
  .split(',')
  .map((f) => f.trim())
  .filter((f) => f && f !== 'none')
  .map((f) => {
    const [what, at] = f.split('@');
    return { what: what as 'redis' | 'postgres', at: Number(at), done: false };
  });

const TOTAL = RATE * DURATION_S;
const db = new PrimaryPool(4);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const counters = { failed: 0, duplicates: 0 };

function spawnWorker(i: number): Promise<ChildProcess> {
  const proc = spawn(process.execPath, [path.join(__dirname, 'worker.js')], {
    env: { ...process.env, WORKER_NAME: `w${i + 1}` },
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('worker never became ready')), 60000);
    readline.createInterface({ input: proc.stdout! }).on('line', (line) => {
      try {
        const m = JSON.parse(line);
        if (m.evt === 'ready') {
          clearTimeout(timer);
          resolve(proc);
        } else if (m.evt === 'failed') counters.failed++;
        else if (m.evt === 'duplicate') counters.duplicates++;
      } catch {
        /* not ours */
      }
    });
  });
}

// ---- fault injectors (in-band, no Docker socket needed) ----

async function crashRedisPrimary() {
  const p = await currentPrimary();
  if (!p) throw new Error('crashRedisPrimary needs REDIS_SENTINELS');
  const c = new IORedis({ host: p.host, port: p.port, maxRetriesPerRequest: 1, retryStrategy: () => null });
  c.on('error', () => {});
  await c.call('SHUTDOWN', 'NOSAVE').catch(() => {});
  c.disconnect();
  return `${p.host}:${p.port}`;
}

// Hard-kill the Patroni node that is currently primary: SIGKILL its Patroni agent AND its postmaster,
// so the node is really gone (killing only postgres would just make Patroni restart it in place).
// Runs through COPY ... TO PROGRAM (superuser), so it works the same natively and inside Docker:
//   - find the patroni process whose config file holds THIS node's data_dir (other nodes are left alone)
//   - kill it, then kill the postmaster (pid = first line of postmaster.pid). PID 1 is never touched.
async function crashPostgresPrimary() {
  const dataDir: string = (await db.queryRetry('SHOW data_directory')).rows[0].data_directory;
  const victim = db.current!;
  if (!/^[\w./-]+$/.test(dataDir)) throw new Error(`unexpected data_directory ${dataDir}`);
  const cmd =
    `for p in $(pgrep -f patroni); do ` +
    `c=$(ps -o args= -p $p | awk "{print \\$NF}"); ` +
    `[ "$p" != 1 ] && grep -qs "data_dir: ${dataDir}" "$c" && kill -9 $p; ` +
    `done; kill -9 $(head -1 ${dataDir}/postmaster.pid)`;
  await db.query(`COPY (SELECT 1) TO PROGRAM '${cmd}'`).catch(() => {}); // our own connection dies too
  return `${victim.host}:${victim.port}`;
}

async function main() {
  const faultNames = FAULTS.map((f) => `${f.what}@${f.at}`).join(', ') || 'none';
  console.log(`\nPGPulse chaos run: ${RATE} jobs/s for ${DURATION_S}s (${TOTAL} jobs), ${WORKERS} workers, faults: ${faultNames}`);
  if (FAULTS.some((f) => f.what === 'redis') && !isSentinelMode()) throw new Error('redis fault needs REDIS_SENTINELS');

  await db.queryRetry('SELECT 1', [], 120000);
  await ensureSchema({ query: (t, p) => db.queryRetry(t, p) });
  await db.queryRetry('TRUNCATE ledger_entries, settle_orders');
  await db.queryRetry(
    `INSERT INTO settle_orders (id, amount)
     SELECT g, round((random() * 490 + 10)::numeric, 2) FROM generate_series(1, $1) g`,
    [TOTAL]
  );
  const amounts = new Map<number, string>(
    (await db.queryRetry('SELECT id, amount FROM settle_orders')).rows.map((r: any) => [r.id, r.amount])
  );

  const queue = new Queue(QUEUE_NAME, { connection: redisOptions() });
  await queue.waitUntilReady();
  await queue.obliterate({ force: true });

  const workers = await Promise.all(Array.from({ length: WORKERS }, (_, i) => spawnWorker(i)));
  console.log(`  ${workers.length} workers ready`);

  const jobOpts = {
    ...JOB_RETRY,
    removeOnComplete: true,
    removeOnFail: false,
  };
  const toJob = (id: number) => ({ name: 'settle', data: { orderId: id, amount: amounts.get(id)! }, opts: { ...jobOpts, jobId: jobIdFor(id) } });

  // planned arrival time for each order = t0 + (id-1)/RATE, so latency includes any stall the order sat through
  const t0Db: Date = (await db.queryRetry('SELECT clock_timestamp() AS t')).rows[0].t;
  await db.queryRetry(`UPDATE settle_orders SET enqueued_at = $1::timestamptz + (id - 1) * (interval '1 second' / $2)`, [t0Db, RATE]);
  const t0 = Date.now();

  // ---- producer: offer RATE jobs/s; adds are chained so a Redis outage just delays them ----
  let next = 1;
  let producerChain: Promise<unknown> = Promise.resolve();
  const producer = setInterval(() => {
    const target = Math.min(TOTAL, Math.floor(((Date.now() - t0) / 1000) * RATE));
    if (target < next) return;
    const ids = Array.from({ length: target - next + 1 }, (_, i) => next + i);
    next = target + 1;
    producerChain = producerChain.then(() => queue.addBulk(ids.map(toJob))).catch(() => {}); // outbox sweep catches anything dropped
  }, 100);

  // ---- outbox sweep: an order still pending long after its arrival time with no job in Redis gets re-queued ----
  // (Redis replication is async, so a job written to a primary right before it crashes can vanish)
  let redriven = 0;
  let sweeping = false;
  const sweeper = setInterval(async () => {
    if (sweeping) return;
    sweeping = true;
    try {
      const rows = (
        await db.query(
          `SELECT id FROM settle_orders
           WHERE status = 'pending' AND enqueued_at < clock_timestamp() - $1 * interval '1 second' AND id < $2
           ORDER BY id LIMIT 2000`,
          [REDRIVE_AFTER_S, next]
        )
      ).rows;
      let missing: number[] = [];
      for (const r of rows) if (!(await queue.getJob(jobIdFor(r.id)))) missing.push(r.id);
      if (missing.length) {
        // re-check: a job may have finished (and been removed from Redis) between the two reads
        missing = (
          await db.query(`SELECT id FROM settle_orders WHERE id = ANY($1) AND status = 'pending'`, [missing])
        ).rows.map((r: any) => r.id);
      }
      if (missing.length) {
        await queue.addBulk(missing.map(toJob));
        redriven += missing.length;
      }
    } catch {
      /* DB or Redis mid-failover; try again next tick */
    } finally {
      sweeping = false;
    }
  }, 2000);

  // ---- fault schedule + progress ----
  const faultLog: { what: string; victim: string; at_s: number; at_db: Date }[] = [];
  let lastPrint = 0;
  while (Date.now() - t0 < DURATION_S * 1000) {
    const elapsed = (Date.now() - t0) / 1000;
    for (const f of FAULTS) {
      if (!f.done && elapsed >= f.at * DURATION_S) {
        f.done = true;
        const at_db: Date = (await db.queryRetry('SELECT clock_timestamp() AS t')).rows[0].t;
        const victim = f.what === 'redis' ? await crashRedisPrimary() : await crashPostgresPrimary();
        faultLog.push({ what: f.what, victim, at_s: Number(elapsed.toFixed(1)), at_db });
        console.log(`  [${elapsed.toFixed(1)}s] CRASH ${f.what} primary ${victim}`);
      }
    }
    if (elapsed - lastPrint >= 10) {
      lastPrint = elapsed;
      const s = await db.query(`SELECT count(*) FROM settle_orders WHERE status='settled'`).catch(() => null);
      console.log(`  [${elapsed.toFixed(0)}s] offered ${next - 1}, settled ${s ? s.rows[0].count : '(db failing over)'}`);
    }
    await sleep(200);
  }
  clearInterval(producer);
  // offer whatever the last tick didn't reach, so exactly TOTAL jobs are offered
  if (next <= TOTAL) {
    const rest = Array.from({ length: TOTAL - next + 1 }, (_, i) => next + i);
    next = TOTAL + 1;
    producerChain = producerChain.then(() => queue.addBulk(rest.map(toJob))).catch(() => {});
  }
  await producerChain;
  console.log(`  traffic window over, draining...`);

  // ---- drain ----
  let settled = 0;
  const drainStart = Date.now();
  while (Date.now() - drainStart < DRAIN_TIMEOUT_S * 1000) {
    settled = Number((await db.queryRetry(`SELECT count(*) FROM settle_orders WHERE status='settled'`)).rows[0].count);
    if (settled >= TOTAL) break;
    await sleep(500);
  }
  clearInterval(sweeper);

  // ---- measurements (Postgres is the source of truth) ----
  const q = (t: string, p?: any[]) => db.queryRetry(t, p).then((r) => r.rows[0]);
  const avail = await q(
    `SELECT count(DISTINCT floor(EXTRACT(EPOCH FROM settled_at - $1::timestamptz))) AS up
     FROM settle_orders WHERE settled_at >= $1::timestamptz AND settled_at < $1::timestamptz + $2 * interval '1 second'`,
    [t0Db, DURATION_S]
  );
  const lat = await q(
    `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY ms) AS p50, percentile_cont(0.99) WITHIN GROUP (ORDER BY ms) AS p99
     FROM (SELECT EXTRACT(EPOCH FROM settled_at - enqueued_at) * 1000 AS ms FROM settle_orders WHERE settled_at IS NOT NULL) x`
  );
  const doubled = await q(`SELECT count(*) AS n FROM (SELECT order_id FROM ledger_entries GROUP BY order_id HAVING count(*) > 1) d`);
  const recon = await q(
    `SELECT (SELECT coalesce(sum(amount),0) FROM settle_orders WHERE status='settled') AS orders_total,
            (SELECT coalesce(sum(amount),0) FROM ledger_entries) AS ledger_total,
            (SELECT coalesce(sum(attempts),0) FROM settle_orders) - (SELECT count(*) FROM ledger_entries) AS redeliveries`
  );

  // each fault's window runs until the next fault (or 120s), so one outage is never charged to another
  const outages = [];
  for (let i = 0; i < faultLog.length; i++) {
    const f = faultLog[i];
    const end = faultLog[i + 1]?.at_db ?? new Date(f.at_db.getTime() + 120000);
    const g = await q(
      `WITH s AS (SELECT $1::timestamptz AS t
                  UNION ALL SELECT settled_at FROM settle_orders
                  WHERE settled_at >= $1::timestamptz AND settled_at < $2::timestamptz)
       SELECT EXTRACT(EPOCH FROM max(d)) * 1000 AS ms FROM (SELECT t - lag(t) OVER (ORDER BY t) AS d FROM s) x`,
      [f.at_db, end]
    );
    outages.push({ fault: `${f.what} primary crash`, victim: f.victim, at_s: f.at_s, longest_stall_ms: Math.round(Number(g.ms)) });
  }

  const upSeconds = Number(avail.up);
  const summary = {
    mode: 'chaos',
    faults: faultNames,
    rate_jobs_s: RATE,
    duration_s: DURATION_S,
    workers: WORKERS,
    jobs: TOTAL,
    settled: settled,
    lost_jobs: TOTAL - settled,
    double_settled_orders: Number(doubled.n),
    ledger_reconciled: recon.orders_total === recon.ledger_total,
    redeliveries_absorbed: Number(recon.redeliveries),
    jobs_redriven_from_outbox: redriven,
    failed_attempts_retried: counters.failed,
    seconds_with_settlements: `${upSeconds}/${DURATION_S}`,
    availability_pct: Number(((upSeconds / DURATION_S) * 100).toFixed(2)),
    latency_p50_ms: Math.round(Number(lat.p50)),
    latency_p99_ms: Math.round(Number(lat.p99)),
    postgres_primary_now: db.current ? `${db.current.host}:${db.current.port}` : null,
    redis_primary_now: isSentinelMode() ? await currentPrimary().then((p) => (p ? `${p.host}:${p.port}` : null)) : 'single',
    outages,
  };

  console.log('\n=== result ===');
  for (const [k, v] of Object.entries(summary)) {
    if (k === 'outages') continue;
    console.log(`  ${k.padEnd(28)} ${v}`);
  }
  for (const o of outages) console.log(`  outage: ${o.fault} (${o.victim}) at ${o.at_s}s -> longest stall ${o.longest_stall_ms} ms`);

  const outDir = path.join(process.cwd(), 'results');
  fs.mkdirSync(outDir, { recursive: true });
  const file = path.join(outDir, `queue-chaos-${new Date().toISOString().replace(/[:.]/g, '-')}.json`);
  fs.writeFileSync(file, JSON.stringify(summary, null, 2));
  console.log(`\n  saved ${file}`);

  workers.forEach((w) => w.kill('SIGTERM'));
  await sleep(500);
  await queue.close().catch(() => {});
  await db.end();

  const ok = summary.lost_jobs === 0 && summary.double_settled_orders === 0 && summary.ledger_reconciled;
  console.log(ok ? '\n  PASS: 0 lost, 0 double-settled, ledger reconciled\n' : '\n  FAIL: see numbers above\n');
  process.exit(ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
