import { Worker } from 'bullmq';
import dotenv from 'dotenv';
import { redisOptions } from './connection';
import { PrimaryPool } from '../db/primaryPool';
import { QUEUE_NAME, SettleJob, backoffStrategy, settleOrder } from './settlement';

dotenv.config();

// One worker process. The bench spawns several of these (separate OS processes),
// all pulling from the same Redis queue, so killing one doesn't take the others down.
const CONCURRENCY = Number(process.env.WORKER_CONCURRENCY || 10);
const WORK_MS = Number(process.env.WORK_MS || 10);
const FAIL_RATE = Number(process.env.FAIL_RATE || 0);
const WORKER_NAME = process.env.WORKER_NAME || `worker-${process.pid}`;

// points at the current Postgres primary (follows Patroni failovers when PG_HOSTS is set)
const pool = new PrimaryPool(CONCURRENCY + 2);

const emit = (evt: string, extra: Record<string, unknown> = {}) =>
  process.stdout.write(JSON.stringify({ evt, worker: WORKER_NAME, ...extra }) + '\n');

const worker = new Worker<SettleJob>(
  QUEUE_NAME,
  async (job) => settleOrder(pool, { id: job.id as string, data: job.data }, { workMs: WORK_MS, failRate: FAIL_RATE }),
  {
    connection: redisOptions(),
    concurrency: CONCURRENCY,
    // short locks so a crashed worker's jobs come back fast:
    // if a worker dies, its lock stops being renewed, and after lockDuration the
    // stalled-job checker in a surviving worker moves the job back to "wait".
    lockDuration: Number(process.env.LOCK_MS || 5000),
    stalledInterval: Number(process.env.STALLED_MS || 2000),
    maxStalledCount: 10,
    settings: { backoffStrategy },
  }
);

worker.on('ready', () => emit('ready'));
worker.on('completed', (job, result) => {
  if (result === 'duplicate') emit('duplicate', { job: job.id });
});
worker.on('failed', (job, err) => emit('failed', { job: job?.id, attempt: job?.attemptsMade, err: err.message }));
worker.on('error', (err) => emit('error', { err: err.message }));

async function shutdown() {
  await worker.close();
  await pool.end();
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
