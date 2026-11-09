import {
  GetCommand,
  QueryCommand,
  TransactWriteCommand,
  UpdateCommand,
  type DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import type { Redis } from 'ioredis';
import { conversationIdFor } from './ids.js';
import {
  decodeCursor,
  encodeCursor,
  isConditionFailure,
  isTransactionConditionFailure,
} from './dynamo.js';
import type { ChatMessage, ConversationRow, Page } from './types.js';

const KNOWN_TTL_S = 3000;
const PREVIEW_LENGTH = 120;

export interface InboxUpdater {
  applyMessage(msg: ChatMessage): Promise<void>;
}

export class ConversationStore implements InboxUpdater {
  constructor(
    private readonly db: DynamoDBDocumentClient,
    private readonly redis: Redis,
    private readonly table: string,
  ) {}

  // Idempotent: returns the same conversation however many times, and from
  // however many concurrent callers, it is opened.
  //
  // The id is derived from the pair, and both rows are written in one
  // transaction conditioned on not existing. Concurrent callers all compute the
  // same id; one transaction succeeds and the rest fail the condition, which
  // just means "already there". The Redis key is only a shortcut that lets
  // repeat opens skip the write; correctness never depends on it.
  async open(userId: string, otherUserId: string): Promise<{ conversationId: string; created: boolean }> {
    const conversationId = conversationIdFor(userId, otherUserId);
    const knownKey = `conv:known:${conversationId}`;

    if (await this.redis.exists(knownKey).catch(() => 0)) {
      return { conversationId, created: false };
    }

    const now = new Date().toISOString();
    const row = (user: string, other: string): ConversationRow => ({
      user_id: user,
      conversation_id: conversationId,
      other_user_id: other,
      unread_count: 0,
      created_at: now,
    });

    let created = true;
    try {
      await this.db.send(
        new TransactWriteCommand({
          TransactItems: [userId, otherUserId].map((u) => ({
            Put: {
              TableName: this.table,
              Item: row(u, u === userId ? otherUserId : userId),
              ConditionExpression: 'attribute_not_exists(conversation_id)',
            },
          })),
        }),
      );
    } catch (err) {
      if (!isTransactionConditionFailure(err)) throw err;
      created = false;
    }

    await this.redis.set(knownKey, '1', 'EX', KNOWN_TTL_S).catch(() => undefined);
    return { conversationId, created };
  }

  async isParticipant(userId: string, conversationId: string): Promise<boolean> {
    const res = await this.db.send(
      new GetCommand({
        TableName: this.table,
        Key: { user_id: userId, conversation_id: conversationId },
        ProjectionExpression: 'conversation_id',
      }),
    );
    return Boolean(res.Item);
  }

  async inbox(userId: string, limit = 20, cursor?: string): Promise<Page<ConversationRow>> {
    const res = await this.db.send(
      new QueryCommand({
        TableName: this.table,
        IndexName: 'by_last_message',
        KeyConditionExpression: 'user_id = :u',
        ExpressionAttributeValues: { ':u': userId },
        ScanIndexForward: false,
        Limit: Math.min(limit, 50),
        ExclusiveStartKey: decodeCursor(cursor),
      }),
    );
    return { items: (res.Items ?? []) as ConversationRow[], cursor: encodeCursor(res.LastEvaluatedKey) };
  }

  // Applies a persisted message to both inbox rows. Safe to replay and safe to
  // receive out of order: an update only lands if its message id sorts after
  // the one already recorded, and ULIDs sort by creation time. A redelivered
  // message fails the condition on both rows, so unread_count is never bumped
  // twice.
  async applyMessage(msg: ChatMessage): Promise<void> {
    const preview = msg.message ? msg.message.slice(0, PREVIEW_LENGTH) : `[${msg.type.toLowerCase()}]`;

    const update = (userId: string, isRecipient: boolean) =>
      this.db
        .send(
          new UpdateCommand({
            TableName: this.table,
            Key: { user_id: userId, conversation_id: msg.conversation_id },
            UpdateExpression:
              'SET last_message = :preview, last_message_id = :id, last_message_at = :at, last_sender_id = :sender' +
              (isRecipient ? ' ADD unread_count :one' : ''),
            ConditionExpression:
              'attribute_exists(conversation_id) AND (attribute_not_exists(last_message_id) OR last_message_id < :id)',
            ExpressionAttributeValues: {
              ':preview': preview,
              ':id': msg.message_id,
              ':at': msg.created_at,
              ':sender': msg.sender_id,
              ...(isRecipient && { ':one': 1 }),
            },
          }),
        )
        .catch((err) => {
          if (!isConditionFailure(err)) throw err;
        });

    await Promise.all([update(msg.sender_id, false), update(msg.recipient_id, true)]);
  }

  // Read state lives on the reader's conversation row as a high-water mark
  // rather than as a status on every message, so marking a long thread read is
  // one write instead of N.
  //
  // The count is only zeroed when nothing newer than the read mark has
  // arrived; otherwise a message landing between the client rendering and the
  // receipt arriving would be silently marked read.
  async markRead(userId: string, conversationId: string, upToMessageId: string): Promise<boolean> {
    const key = { user_id: userId, conversation_id: conversationId };
    try {
      await this.db.send(
        new UpdateCommand({
          TableName: this.table,
          Key: key,
          UpdateExpression: 'SET last_read_message_id = :id',
          ConditionExpression:
            'attribute_exists(conversation_id) AND (attribute_not_exists(last_read_message_id) OR last_read_message_id < :id)',
          ExpressionAttributeValues: { ':id': upToMessageId },
        }),
      );
    } catch (err) {
      if (isConditionFailure(err)) return false;
      throw err;
    }

    await this.db
      .send(
        new UpdateCommand({
          TableName: this.table,
          Key: key,
          UpdateExpression: 'SET unread_count = :zero',
          ConditionExpression: 'attribute_not_exists(last_message_id) OR last_message_id <= :id',
          ExpressionAttributeValues: { ':id': upToMessageId, ':zero': 0 },
        }),
      )
      .catch((err) => {
        if (!isConditionFailure(err)) throw err;
      });
    return true;
  }
}
