import { PrimaryPool, isFailoverError } from '../db/primaryPool';

type Queryable = { query: (text: string, params?: any[]) => Promise<any> };

export const QUEUE_NAME = 'settlements';

export type SettleJob = { orderId: number; amount: string };

export const jobIdFor = (orderId: number) => `settle-${orderId}`;

// Retry policy: exponential (100ms, 200ms, 400ms...) but capped at 1s with jitter, and many attempts.
// Uncapped exponential backoff made jobs sit idle 6-12s AFTER a failover had already finished.
export const JOB_RETRY = { attempts: 50, backoff: { type: 'custom' as const } };
export function backoffStrategy(attemptsMade: number): number {
  return Math.min(100 * 2 ** Math.max(0, attemptsMade - 1), 1000) + Math.floor(Math.random() * 100);
}

// Two tables:
// - settle_orders:   orders waiting to be settled (status pending -> settled)
// - ledger_entries:  one row per settlement, keyed by an idempotency key (the job id)
// The primary key on idempotency_key is what makes a retried / re-delivered job safe:
// the second attempt hits ON CONFLICT and does nothing, so an order can never be settled twice.
export async function ensureSchema(pool: Queryable) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS settle_orders (
      id           INT PRIMARY KEY,
      amount       NUMERIC(12,2) NOT NULL,
      status       TEXT NOT NULL DEFAULT 'pending',
      attempts     INT NOT NULL DEFAULT 0,
      enqueued_at  TIMESTAMPTZ,
      settled_at   TIMESTAMPTZ
    );
    CREATE TABLE IF NOT EXISTS ledger_entries (
      idempotency_key TEXT PRIMARY KEY,
      order_id        INT NOT NULL REFERENCES settle_orders(id),
      amount          NUMERIC(12,2) NOT NULL,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_settle_orders_settled_at ON settle_orders(settled_at);
  `);
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// Settle one order. Everything that changes money happens in ONE transaction:
//   attempts++  ->  ledger insert (idempotent)  ->  mark order settled
// If anything throws before COMMIT, Postgres rolls it all back and BullMQ retries the job.
export async function settleOrder(
  db: PrimaryPool,
  job: { id: string; data: SettleJob },
  opts: { workMs: number; failRate: number }
): Promise<'settled' | 'duplicate'> {
  // pretend to call a payment provider (outside the transaction, like you would in real life)
  if (opts.workMs > 0) await sleep(opts.workMs);

  const client = await db.connect();
  let broken = false;
  try {
    await client.query('BEGIN');
    await client.query('UPDATE settle_orders SET attempts = attempts + 1 WHERE id = $1', [job.data.orderId]);

    const ins = await client.query(
      `INSERT INTO ledger_entries (idempotency_key, order_id, amount)
       VALUES ($1, $2, $3)
       ON CONFLICT (idempotency_key) DO NOTHING`,
      [job.id, job.data.orderId, job.data.amount]
    );

    if (ins.rowCount === 1) {
      await client.query(
        `UPDATE settle_orders SET status = 'settled', settled_at = clock_timestamp()
         WHERE id = $1 AND status = 'pending'`,
        [job.data.orderId]
      );
    }

    // fault injection: a transient failure right before commit (e.g. a dropped DB connection)
    if (opts.failRate > 0 && Math.random() < opts.failRate) {
      throw new Error('injected transient failure before COMMIT');
    }

    await client.query('COMMIT');
    return ins.rowCount === 1 ? 'settled' : 'duplicate';
  } catch (err) {
    // if the primary died mid-transaction, nothing committed: Postgres never applied it.
    // (with synchronous replication, anything that DID commit is already on the replica)
    broken = isFailoverError(err);
    if (!broken) await client.query('ROLLBACK').catch(() => {});
    db.reportError(client, err);
    throw err;
  } finally {
    client.release(broken);
  }
}
