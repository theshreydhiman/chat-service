import type { Redis } from 'ioredis';

const HEARTBEAT_S = 25;
const STALE_AFTER_S = HEARTBEAT_S * 3;

// Presence is tracked per socket, not per user. Each live socket is a member
// of a sorted set scored by its last heartbeat, so:
//   - a user with a phone and a laptop stays online until both disconnect;
//   - a pod that dies without running disconnect handlers doesn't leave users
//     online forever, because their sockets stop heartbeating and age out.
export class Presence {
  static readonly heartbeatIntervalMs = HEARTBEAT_S * 1000;

  constructor(
    private readonly redis: Redis,
    private readonly now: () => number = Date.now,
  ) {}

  private key = (userId: string) => `presence:${userId}`;

  async connect(userId: string, socketId: string): Promise<boolean> {
    const wasOnline = await this.isOnline(userId);
    await this.touch(userId, socketId);
    return !wasOnline;
  }

  async touch(userId: string, socketId: string): Promise<void> {
    const key = this.key(userId);
    await this.redis
      .multi()
      .zadd(key, this.now(), socketId)
      .expire(key, STALE_AFTER_S * 2)
      .exec();
  }

  /** Returns true when this was the user's last live socket. */
  async disconnect(userId: string, socketId: string): Promise<boolean> {
    await this.redis.zrem(this.key(userId), socketId);
    return !(await this.isOnline(userId));
  }

  async isOnline(userId: string): Promise<boolean> {
    const key = this.key(userId);
    const cutoff = this.now() - STALE_AFTER_S * 1000;
    const results = await this.redis.multi().zremrangebyscore(key, '-inf', cutoff).zcard(key).exec();
    const live = results?.[1]?.[1];
    return typeof live === 'number' && live > 0;
  }
}
