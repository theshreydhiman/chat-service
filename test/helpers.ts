import { Redis } from 'ioredis';
import { pino } from 'pino';

export const silentLog = pino({ level: 'silent' });

// Tests that need Redis use REDIS_URL (default: a local instance on 6390) and
// are skipped when it isn't reachable, so `npm test` still runs the rest.
export async function connectTestRedis(): Promise<Redis | undefined> {
  const redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6390', {
    lazyConnect: true,
    maxRetriesPerRequest: 0,
    retryStrategy: () => null,
  });
  try {
    await redis.connect();
    await redis.select(15);
    await redis.flushdb();
    return redis;
  } catch {
    redis.disconnect();
    return undefined;
  }
}
