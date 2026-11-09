import type { Redis } from 'ioredis';
import type { Logger } from 'pino';
import type { ChatMessage } from './types.js';

export interface PushSender {
  notify(userId: string, msg: ChatMessage): Promise<void>;
}

const tokensKey = (userId: string) => `push:tokens:${userId}`;

export async function registerDevice(redis: Redis, userId: string, token: string): Promise<void> {
  await redis.sadd(tokensKey(userId), token);
}

function previewOf(msg: ChatMessage): string {
  switch (msg.type) {
    case 'TEXT':
      return msg.message.slice(0, 100);
    case 'ENQUIRY':
      return 'New enquiry';
    case 'OFFER':
      return 'New offer';
    default:
      return `Sent a ${msg.type.toLowerCase()}`;
  }
}

export class FcmPushSender implements PushSender {
  constructor(
    private readonly redis: Redis,
    private readonly messaging: import('firebase-admin/messaging').Messaging,
    private readonly log: Logger,
  ) {}

  async notify(userId: string, msg: ChatMessage): Promise<void> {
    const tokens = await this.redis.smembers(tokensKey(userId));
    if (tokens.length === 0) return;

    const res = await this.messaging.sendEachForMulticast({
      tokens,
      notification: { body: previewOf(msg) },
      data: { conversation_id: msg.conversation_id, message_id: msg.message_id },
    });

    // Drop tokens FCM says are gone so we stop paying for them on every message.
    const dead = res.responses
      .map((r, i) => (r.error?.code === 'messaging/registration-token-not-registered' ? tokens[i] : undefined))
      .filter((t): t is string => Boolean(t));
    if (dead.length) {
      await this.redis.srem(tokensKey(userId), ...dead);
      this.log.debug({ userId, removed: dead.length }, 'pruned push tokens');
    }
  }
}

export class LogPushSender implements PushSender {
  constructor(private readonly log: Logger) {}

  async notify(userId: string, msg: ChatMessage): Promise<void> {
    this.log.info({ userId, messageId: msg.message_id, body: previewOf(msg) }, 'push (not sent: FCM not configured)');
  }
}
