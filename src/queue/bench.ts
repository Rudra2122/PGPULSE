import { ChildProcess, spawn } from 'child_process';
import fs from 'fs';
import path from 'path';
import readline from 'readline';
import IORedis from 'ioredis';
import { Queue, QueueEvents } from 'bullmq';
import dotenv from 'dotenv';
import { currentPrimary, isSentinelMode, redisOptions } from './connection';
import { JOB_RETRY, QUEUE_NAME, ensureSchema, jobIdFor } from './settlement';
import { PrimaryPool } from '../db/primaryPool';

dotenv.config();

// Usage: node dist/queue/bench.js <baseline|kill|failover>
//   baseline  N jobs, W worker processes, no failures -> throughput
//   kill      SIGKILL some workers mid-run           -> do their in-flight jobs come back? any lost or doubled?
//   failover  kill the Redis primary mid-run (needs Sentinel) -> how long until jobs flow again? any lost or doubled?
const MODE = (process.argv[2] || 'baseline') as 'baseline' | 'kill' | 'failover';
const JOBS = Number(process.env.JOBS || 10000);
const WORKERS = Number(process.env.WORKERS || 4);
const KILL_WORKERS = Number(process.env.KILL_WORKERS || 2);
const TRIGGER_AT = Number(process.env.TRIGGER_AT || 0.4); // fraction settled before we break something
const TIMEOUT_S = Number(process.env.TIMEOUT_S || 300);

const pool = new PrimaryPool(4);

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function waitFor<T>(label: string, fn: () => Promise<T>, tries = 60): Promise<T> {
  for (let i = 0; i < tries; i++) {
    try {
      return await fn();
    } catch {
      await sleep(1000);
    }
  }
  throw new Error(`timed out waiting for ${label}`);
}

type WorkerProc = { proc: ChildProcess; name: string; alive: boolean };
const counters = { failed: 0, duplicates: 0, errors: 0 };

function spawnWorker(i: number): Promise<WorkerProc> {
  const name = `w${i + 1}`;
  const proc = spawn(process.execPath, [path.join(__dirname, 'worker.js')], {
    env: { ...process.env, WORKER_NAME: name },
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  const wp: WorkerProc = { proc, name, alive: true };
  proc.on('exit', () => (wp.alive = false));
  return new Promise((resolve, reject) => {
    const rl = readline.createInterface({ input: proc.stdout! });
    const timer = setTimeout(() => reject(new Error(`${name} never became ready`)), 30000);
    rl.on('line', (line) => {
      let msg: any;
      try {
        msg = JSON.parse(line);
      } catch {
        return;
      }
      if (msg.evt === 'ready') {
        clearTimeout(timer);
        resolve(wp);
      } else if (msg.evt === 'failed') counters.failed++;
      else if (msg.evt === 'duplicate') counters.duplicates++;
      else if (msg.evt === 'error') counters.errors++;
    });
  });
}

const settledCount = async () =>
  Number((await pool.query(`SELECT count(*) FROM settle_orders WHERE status = 'settled'`)).rows[0].count);

const dbNow = async (): Promise<Date> => (await pool.query('SELECT clock_timestamp() AS t')).rows[0].t;

async function main() {
  if (MODE === 'failover' && !isSentinelMode()) {
    throw new Error('failover mode needs REDIS_SENTINELS (see docker compose --profile ha, or scripts/ha-local.sh)');
  }

  console.log(`\nPGPulse queue bench: mode=${MODE} jobs=${JOBS} workers=${WORKERS} ` +
    `concurrency/worker=${process.env.WORKER_CONCURRENCY || 10} redis=${isSentinelMode() ? 'sentinel' : 'single'}`);

  await waitFor('postgres', () => pool.query('SELECT 1'));
  await ensureSchema(pool);
  await pool.query('TRUNCATE ledger_entries, settle_orders');
  await pool.query(
    `INSERT INTO settle_orders (id, amount)
     SELECT g, round((random() * 490 + 10)::numeric, 2) FROM generate_series(1, $1) g`,
    [JOBS]
  );

  const queue = new Queue(QUEUE_NAME, { connection: redisOptions() });
  await waitFor('redis', () => queue.waitUntilReady());
  await queue.obliterate({ force: true });

  // enqueue every order as a job; jobId = idempotency key
  const orders = (await pool.query('SELECT id, amount FROM settle_orders ORDER BY id')).rows;
  await pool.query('UPDATE settle_orders SET enqueued_at = clock_timestamp()');
  for (let i = 0; i < orders.length; i += 1000) {
    await queue.addBulk(
      orders.slice(i, i + 1000).map((o) => ({
        name: 'settle',
        data: { orderId: o.id, amount: o.amount },
        opts: {
          jobId: jobIdFor(o.id),
          ...JOB_RETRY,
          removeOnComplete: true,
          removeOnFail: false,
        },
      }))
    );
  }

  if (MODE === 'failover') {
    // make sure the replica has every job before we kill the primary,
    // so any loss we see later comes from the failover itself, not from enqueue lag
    const r = new IORedis(redisOptions());
    const acked = await r.call('WAIT', '1', '5000');
    r.disconnect();
    console.log(`  replica acked enqueue writes: ${acked} replica(s)`);
  }

  // stalled-job events only matter for the kill test
  const stalled = new Set<string>();
  let events: QueueEvents | null = null;
  if (MODE === 'kill') {
    events = new QueueEvents(QUEUE_NAME, { connection: redisOptions() });
    await events.waitUntilReady();
    events.on('stalled', ({ jobId }) => stalled.add(jobId));
  }

  const workers = await Promise.all(Array.from({ length: WORKERS }, (_, i) => spawnWorker(i)));
  console.log(`  ${workers.length} worker processes ready, ${JOBS} jobs queued`);

  const t0 = Date.now();
  const tStartDb = await dbNow();
  let triggered = false;
  let triggerAtMs = 0;
  let triggerDb: Date | null = null;
  let settledAtTrigger = 0;
  let oldPrimary: { host: string; port: number } | null = null;
  let newPrimary: { host: string; port: number } | null = null;
  let promotionMs: number | null = null;
  let settled = 0;

  while (true) {
    settled = await settledCount();
    const elapsed = (Date.now() - t0) / 1000;

    if (!triggered && MODE !== 'baseline' && settled >= JOBS * TRIGGER_AT) {
      triggered = true;
      settledAtTrigger = settled;
      triggerDb = await dbNow();
      triggerAtMs = Date.now();
      if (MODE === 'kill') {
        workers.slice(0, KILL_WORKERS).forEach((w) => w.proc.kill('SIGKILL'));
        console.log(`  [${elapsed.toFixed(1)}s] SIGKILL -> ${workers.slice(0, KILL_WORKERS).map((w) => w.name).join(', ')} at ${settled}/${JOBS} settled`);
      } else {
        oldPrimary = await currentPrimary();
        const p = new IORedis({ host: oldPrimary!.host, port: oldPrimary!.port, maxRetriesPerRequest: 1, retryStrategy: () => null });
        p.on('error', () => {}); // expected: we just killed the server this socket points at
        await p.call('SHUTDOWN', 'NOSAVE').catch(() => {}); // connection drops when the process dies
        p.disconnect();
        console.log(`  [${elapsed.toFixed(1)}s] SHUTDOWN NOSAVE -> Redis primary ${oldPrimary!.host}:${oldPrimary!.port} at ${settled}/${JOBS} settled`);
      }
    }

    if (MODE === 'failover' && triggered && promotionMs === null) {
      const p = await currentPrimary();
      if (p && oldPrimary && (p.host !== oldPrimary.host || p.port !== oldPrimary.port)) {
        newPrimary = p;
        promotionMs = Date.now() - triggerAtMs;
        console.log(`  Sentinel promoted ${p.host}:${p.port} after ${promotionMs} ms`);
      }
    }

    if (settled >= JOBS) break;
    if (elapsed > TIMEOUT_S) {
      console.log(`  timeout after ${TIMEOUT_S}s`);
      break;
    }
    await sleep(100);
  }

  const totalS = (Date.now() - t0) / 1000;

  // ---- correctness checks: Postgres is the source of truth ----
  const lost = JOBS - settled;
  const doubled = Number(
    (await pool.query(
      `SELECT count(*) FROM (SELECT order_id FROM ledger_entries GROUP BY order_id HAVING count(*) > 1) d`
    )).rows[0].count
  );
  const recon = (
    await pool.query(
      `SELECT (SELECT coalesce(sum(amount),0) FROM settle_orders WHERE status='settled') AS orders_total,
              (SELECT coalesce(sum(amount),0) FROM ledger_entries) AS ledger_total,
              (SELECT coalesce(sum(attempts),0) FROM settle_orders) - (SELECT count(*) FROM ledger_entries) AS redeliveries`
    )
  ).rows[0];
  const reconciled = recon.orders_total === recon.ledger_total;

  const summary: Record<string, unknown> = {
    mode: MODE,
    jobs: JOBS,
    workers: WORKERS,
    concurrency_per_worker: Number(process.env.WORKER_CONCURRENCY || 10),
    work_ms: Number(process.env.WORK_MS || 10),
    fail_rate: Number(process.env.FAIL_RATE || 0),
    total_s: Number(totalS.toFixed(2)),
    throughput_jobs_s: Number((settled / totalS).toFixed(1)),
    lost_jobs: lost,
    double_settled_orders: doubled,
    ledger_reconciled: reconciled,
    redeliveries_absorbed: Number(recon.redeliveries),
    failed_attempts_retried: counters.failed,
  };

  if (triggered && triggerDb) {
    // longest stretch with zero settlements after the fault = how long the system was "down"
    const gap = await pool.query(
      `WITH s AS (SELECT $1::timestamptz AS t UNION ALL SELECT settled_at FROM settle_orders WHERE settled_at >= $1)
       SELECT EXTRACT(EPOCH FROM max(d)) * 1000 AS ms
       FROM (SELECT t - lag(t) OVER (ORDER BY t) AS d FROM s) x`,
      [triggerDb]
    );
    summary.max_stall_after_fault_ms = Math.round(Number(gap.rows[0].ms));
    const before = settledAtTrigger / ((triggerAtMs - t0) / 1000);
    const after = (settled - settledAtTrigger) / ((Date.now() - triggerAtMs) / 1000);
    summary.throughput_before_fault = Number(before.toFixed(1));
    summary.throughput_after_fault = Number(after.toFixed(1));
  }

  if (MODE === 'kill') {
    summary.workers_killed = KILL_WORKERS;
    summary.stalled_jobs_recovered = stalled.size;
    if (stalled.size && triggerDb) {
      const ids = [...stalled].map((j) => Number(j.replace('settle-', '')));
      const r = await pool.query(
        `SELECT count(*) FILTER (WHERE status='settled') AS ok,
                EXTRACT(EPOCH FROM max(settled_at) - $2::timestamptz) * 1000 AS last_ms
         FROM settle_orders WHERE id = ANY($1)`,
        [ids, triggerDb]
      );
      summary.stalled_jobs_settled = Number(r.rows[0].ok);
      summary.last_stalled_job_settled_after_ms = Math.round(Number(r.rows[0].last_ms));
    }
  }

  if (MODE === 'failover') {
    summary.old_primary = oldPrimary ? `${oldPrimary.host}:${oldPrimary.port}` : null;
    summary.new_primary = newPrimary ? `${newPrimary.host}:${newPrimary.port}` : null;
    summary.sentinel_promotion_ms = promotionMs;
  }

  console.log('\n=== result ===');
  for (const [k, v] of Object.entries(summary)) console.log(`  ${k.padEnd(34)} ${v}`);

  const outDir = path.join(process.cwd(), 'results');
  fs.mkdirSync(outDir, { recursive: true });
  const file = path.join(outDir, `queue-${MODE}-${new Date().toISOString().replace(/[:.]/g, '-')}.json`);
  fs.writeFileSync(file, JSON.stringify({ ...summary, started_at: tStartDb }, null, 2));
  console.log(`\n  saved ${file}`);

  // ---- cleanup ----
  workers.filter((w) => w.alive).forEach((w) => w.proc.kill('SIGTERM'));
  await sleep(500);
  if (events) await events.close();
  await queue.close();
  await pool.end();

  const ok = lost === 0 && doubled === 0 && reconciled;
  console.log(ok ? '\n  PASS: 0 lost, 0 double-settled, ledger reconciled\n' : '\n  FAIL: see numbers above\n');
  process.exit(ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
