import IORedis, { RedisOptions } from 'ioredis';

// One place that decides how the queue talks to Redis.
// - REDIS_SENTINELS set  -> connect through Redis Sentinel (HA mode, survives a primary crash)
// - otherwise            -> plain single Redis (REDIS_HOST / REDIS_PORT)
// BullMQ needs maxRetriesPerRequest: null so blocking commands don't get cut off.
export function redisOptions(): RedisOptions {
  const base: RedisOptions = {
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
    retryStrategy: (times) => Math.min(times * 100, 1000),
  };

  const sentinels = process.env.REDIS_SENTINELS;
  if (sentinels) {
    return {
      ...base,
      name: process.env.REDIS_MASTER_NAME || 'mymaster',
      sentinels: sentinels.split(',').map((hp) => {
        const [host, port] = hp.trim().split(':');
        return { host, port: Number(port || 26379) };
      }),
      // after a failover, ask Sentinel again instead of retrying the dead primary
      sentinelRetryStrategy: (times) => Math.min(times * 100, 1000),
      failoverDetector: true,
    };
  }

  return {
    ...base,
    host: process.env.REDIS_HOST || 'localhost',
    port: Number(process.env.REDIS_PORT || 6379),
  };
}

export const isSentinelMode = () => Boolean(process.env.REDIS_SENTINELS);

export function newRedis(): IORedis {
  return new IORedis(redisOptions());
}

// Ask Sentinel which node is the primary right now.
export async function currentPrimary(): Promise<{ host: string; port: number } | null> {
  const sentinels = (process.env.REDIS_SENTINELS || '').split(',').filter(Boolean);
  const name = process.env.REDIS_MASTER_NAME || 'mymaster';
  for (const hp of sentinels) {
    const [host, port] = hp.trim().split(':');
    const s = new IORedis({ host, port: Number(port || 26379), lazyConnect: true, maxRetriesPerRequest: 1 });
    try {
      await s.connect();
      const addr = (await s.call('SENTINEL', 'get-master-addr-by-name', name)) as string[] | null;
      if (addr) return { host: addr[0], port: Number(addr[1]) };
    } catch {
      // try the next sentinel
    } finally {
      s.disconnect();
    }
  }
  return null;
}
