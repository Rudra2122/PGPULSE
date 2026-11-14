import { Client, Pool, PoolClient } from 'pg';

// A pg Pool that always points at the current Postgres PRIMARY.
//
// In HA mode (PG_HOSTS="node1:5432,node2:5432") Patroni can promote a replica at any time.
// Instead of putting HAProxy in front, the app finds the primary itself:
//   ask each host "SELECT pg_is_in_recovery()" -> the one that answers false is the primary.
// When a query fails with a connection / read-only error, we drop the pool and look again.
// With a single PG_HOST this behaves like a normal Pool.

type HostPort = { host: string; port: number };

const CONN = {
  user: process.env.PG_USER || 'pgpulse',
  password: process.env.PG_PASSWORD || 'pgpulse',
  database: process.env.PG_DATABASE || 'pgpulse',
};

// Every DB call gets a deadline. Without one, a COMMIT stuck waiting on a synchronous standby that
// no longer exists (mid-failover) never returns, and the job hangs forever while its lock keeps renewing.
const QUERY_TIMEOUT_MS = Number(process.env.PG_QUERY_TIMEOUT_MS || 8000);
const TIMEOUTS = {
  query_timeout: QUERY_TIMEOUT_MS,           // client side: reject the promise
  statement_timeout: QUERY_TIMEOUT_MS,       // server side: cancel the statement
  idle_in_transaction_session_timeout: 15000,
};

export function pgHosts(): HostPort[] {
  const list = process.env.PG_HOSTS;
  if (list) {
    return list.split(',').map((hp) => {
      const [host, port] = hp.trim().split(':');
      return { host, port: Number(port || 5432) };
    });
  }
  return [{ host: process.env.PG_HOST || 'localhost', port: Number(process.env.PG_PORT || 5432) }];
}

// Errors that mean "this node is not (or no longer) a usable primary".
const FAILOVER_CODES = new Set([
  '57014', // query_canceled (statement_timeout): treat a stuck commit like a dead node
  '25006', // read_only_sql_transaction: we're talking to a demoted node / replica
  '57P01', // admin_shutdown
  '57P02', // crash_shutdown
  '57P03', // cannot_connect_now (starting up / recovering)
  '08000', '08003', '08006', '08001', '08004', // connection exceptions
]);

export function isFailoverError(err: any): boolean {
  if (!err) return false;
  if (err.code && FAILOVER_CODES.has(err.code)) return true;
  const msg = String(err.message || '');
  return /no Postgres primary found|Query read timeout|ECONNREFUSED|ECONNRESET|EPIPE|ETIMEDOUT|EHOSTUNREACH|ENOTFOUND|Connection terminated|terminating connection|timeout exceeded when trying to connect|Client has encountered a connection error/i.test(msg);
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export class PrimaryPool {
  private pool: Pool | null = null;
  private generation = 0;
  private finding: Promise<Pool> | null = null;
  public current: HostPort | null = null;
  public switches = 0;

  constructor(private max = 10, private hosts = pgHosts(), private discoverTimeoutMs = 60000) {}

  private async probe(h: HostPort): Promise<boolean> {
    const c = new Client({ ...CONN, host: h.host, port: h.port, connectionTimeoutMillis: 1000 });
    c.on('error', () => {});
    try {
      await c.connect();
      const r = await c.query('SELECT pg_is_in_recovery() AS replica');
      return r.rows[0].replica === false;
    } catch {
      return false;
    } finally {
      c.end().catch(() => {});
    }
  }

  private async discover(): Promise<Pool> {
    const deadline = Date.now() + this.discoverTimeoutMs;
    while (Date.now() < deadline) {
      for (const h of this.hosts) {
        if (this.hosts.length === 1 || (await this.probe(h))) {
          const prev = this.current;
          this.current = h;
          if (prev && (prev.host !== h.host || prev.port !== h.port)) this.switches++;
          const pool = new Pool({ ...CONN, ...TIMEOUTS, host: h.host, port: h.port, max: this.max, connectionTimeoutMillis: 2000 });
          pool.on('error', () => {}); // idle client died (e.g. node crashed); next query rediscovers
          this.pool = pool;
          this.generation++;
          return pool;
        }
      }
      await sleep(300);
    }
    throw new Error('no Postgres primary found');
  }

  private async getPool(): Promise<{ pool: Pool; gen: number }> {
    if (!this.pool) {
      if (!this.finding) this.finding = this.discover().finally(() => (this.finding = null));
      await this.finding;
    }
    return { pool: this.pool!, gen: this.generation };
  }

  // Only the first failure of a given pool generation triggers rediscovery,
  // so 40 concurrent jobs failing at once don't rebuild the pool 40 times.
  private invalidate(gen: number) {
    if (gen !== this.generation || !this.pool) return;
    const old = this.pool;
    this.pool = null;
    old.end().catch(() => {});
  }

  async connect(): Promise<PoolClient & { _gen?: number }> {
    const { pool, gen } = await this.getPool();
    try {
      const c = (await pool.connect()) as PoolClient & { _gen?: number; _guarded?: boolean };
      c._gen = gen;
      if (!c._guarded) {
        c._guarded = true;
        c.on('error', () => {}); // a checked-out client whose node dies emits 'error'; the query itself rejects
      }
      return c;
    } catch (err) {
      if (isFailoverError(err)) this.invalidate(gen);
      throw err;
    }
  }

  // Call after a query error on a client from connect()
  reportError(client: { _gen?: number } | null, err: unknown) {
    if (isFailoverError(err)) this.invalidate(client?._gen ?? this.generation);
  }

  async query(text: string, params?: any[]) {
    const c = await this.connect();
    let broken = false;
    try {
      return await c.query(text, params);
    } catch (err) {
      broken = isFailoverError(err);
      this.reportError(c, err);
      throw err;
    } finally {
      c.release(broken); // destroy the socket if the node went away
    }
  }

  // Retry a query through a failover (used by the bench, not the workers; workers rely on BullMQ retries)
  async queryRetry(text: string, params?: any[], timeoutMs = 60000) {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      try {
        return await this.query(text, params);
      } catch (err) {
        if (!isFailoverError(err) || Date.now() > deadline) throw err;
        await sleep(300);
      }
    }
  }

  async end() {
    if (this.pool) await this.pool.end().catch(() => {});
    this.pool = null;
  }
}
