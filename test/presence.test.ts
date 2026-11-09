import type { Redis } from 'ioredis';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { Presence } from '../src/presence.js';
import { connectTestRedis } from './helpers.js';

const redis = await connectTestRedis();

describe.skipIf(!redis)('Presence', () => {
  let now = 1_000_000;
  let presence: Presence;

  beforeAll(() => {
    presence = new Presence(redis as Redis, () => now);
  });
  beforeEach(async () => {
    await redis!.flushdb();
  });
  afterAll(() => redis?.disconnect());

  it('keeps a user online until their last device disconnects', async () => {
    expect(await presence.connect('u1', 'phone')).toBe(true);
    expect(await presence.connect('u1', 'laptop')).toBe(false);

    expect(await presence.disconnect('u1', 'phone')).toBe(false);
    expect(await presence.isOnline('u1')).toBe(true);

    expect(await presence.disconnect('u1', 'laptop')).toBe(true);
    expect(await presence.isOnline('u1')).toBe(false);
  });

  it('ages out sockets that stop heartbeating, e.g. after a pod crash', async () => {
    await presence.connect('u2', 'orphan');
    now += Presence.heartbeatIntervalMs * 2;
    expect(await presence.isOnline('u2')).toBe(true);

    now += Presence.heartbeatIntervalMs * 2;
    expect(await presence.isOnline('u2')).toBe(false);
  });

  it('a heartbeat keeps a socket alive', async () => {
    await presence.connect('u3', 's');
    for (let i = 0; i < 5; i++) {
      now += Presence.heartbeatIntervalMs;
      await presence.touch('u3', 's');
    }
    expect(await presence.isOnline('u3')).toBe(true);
  });
});
