import Redis from 'ioredis';
import { query } from '../db/postgres';

export const redis = new Redis({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
});

const DEFAULT_TTL = 60;

export async function cachedQuery(
  cacheKey: string,
  sql: string,
  params?: any[],
  ttl = DEFAULT_TTL
): Promise<{ rows: any[]; duration: number; fromCache: boolean }> {
  const start = Date.now();

  const cached = await redis.get(cacheKey);
  if (cached) {
    return { rows: JSON.parse(cached), duration: Date.now() - start, fromCache: true };
  }

  const result = await query(sql, params);
  await redis.setex(cacheKey, ttl, JSON.stringify(result.rows));

  return { rows: result.rows, duration: Date.now() - start, fromCache: false };
}

export async function flushCache() {
  await redis.flushall();
}