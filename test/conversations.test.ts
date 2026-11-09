import { TransactWriteCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { Redis } from 'ioredis';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { ConversationStore } from '../src/conversations.js';
import { connectTestRedis } from './helpers.js';

const redis = await connectTestRedis();

// Just enough of DynamoDB to exercise open(): a transaction of conditional puts
// that either all apply or all fail, with some latency so concurrent callers
// genuinely overlap.
function fakeTable() {
  const rows = new Map<string, Record<string, unknown>>();
  const send = async (cmd: unknown) => {
    await new Promise((r) => setTimeout(r, Math.random() * 10));
    if (!(cmd instanceof TransactWriteCommand)) throw new Error('unexpected command');
    const puts = cmd.input.TransactItems!.map((t) => t.Put!);
    const key = (item: Record<string, unknown>) => `${item.user_id}#${item.conversation_id}`;
    if (puts.some((p) => rows.has(key(p.Item!)))) {
      throw Object.assign(new Error('Transaction cancelled'), {
        name: 'TransactionCanceledException',
        CancellationReasons: puts.map((p) => ({ Code: rows.has(key(p.Item!)) ? 'ConditionalCheckFailed' : 'None' })),
      });
    }
    for (const p of puts) rows.set(key(p.Item!), p.Item!);
    return {};
  };
  return { rows, db: { send } as unknown as DynamoDBDocumentClient };
}

describe.skipIf(!redis)('ConversationStore.open', () => {
  beforeEach(async () => {
    await redis!.flushdb();
  });
  afterAll(() => redis?.disconnect());

  it('creates exactly one conversation when both users open it at the same moment', async () => {
    const { rows, db } = fakeTable();
    const store = new ConversationStore(db, redis as Redis, 'conversations');

    const results = await Promise.all(
      Array.from({ length: 20 }, (_, i) => (i % 2 ? store.open('buyer', 'seller') : store.open('seller', 'buyer'))),
    );

    expect(new Set(results.map((r) => r.conversationId)).size).toBe(1);
    expect(results.filter((r) => r.created)).toHaveLength(1);
    expect(rows.size).toBe(2);
  });

  it('stays correct when Redis is unavailable', async () => {
    const { rows, db } = fakeTable();
    const down = {
      exists: async () => {
        throw new Error('ECONNREFUSED');
      },
      set: async () => {
        throw new Error('ECONNREFUSED');
      },
    } as unknown as Redis;
    const store = new ConversationStore(db, down, 'conversations');

    const results = await Promise.all(Array.from({ length: 10 }, () => store.open('buyer', 'seller')));

    expect(results.filter((r) => r.created)).toHaveLength(1);
    expect(rows.size).toBe(2);
  });
});
