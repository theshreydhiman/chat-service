import { PutCommand, QueryCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { isConditionFailure } from './dynamo.js';
import type { ChatMessage } from './types.js';

export interface MessageWriter {
  /** Returns false if the message was already stored (a redelivery). */
  save(msg: ChatMessage): Promise<boolean>;
}

export interface HistoryQuery {
  before?: string;
  after?: string;
  limit?: number;
}

export class MessageStore implements MessageWriter {
  constructor(
    private readonly db: DynamoDBDocumentClient,
    private readonly table: string,
  ) {}

  async save(msg: ChatMessage): Promise<boolean> {
    try {
      await this.db.send(
        new PutCommand({
          TableName: this.table,
          Item: msg,
          ConditionExpression: 'attribute_not_exists(message_id)',
        }),
      );
      return true;
    } catch (err) {
      if (isConditionFailure(err)) return false;
      throw err;
    }
  }

  // Message ids are ULIDs, so the sort key is also the timeline. `before` pages
  // back through history, `after` catches up after a reconnect. Results are
  // always returned oldest first.
  async history(conversationId: string, q: HistoryQuery = {}): Promise<ChatMessage[]> {
    const limit = Math.min(q.limit ?? 30, 100);
    const values: Record<string, string> = { ':c': conversationId };
    let range = '';
    if (q.before) {
      range = ' AND message_id < :b';
      values[':b'] = q.before;
    } else if (q.after) {
      range = ' AND message_id > :a';
      values[':a'] = q.after;
    }

    const res = await this.db.send(
      new QueryCommand({
        TableName: this.table,
        KeyConditionExpression: `conversation_id = :c${range}`,
        ExpressionAttributeValues: values,
        ScanIndexForward: Boolean(q.after),
        Limit: limit,
      }),
    );
    const items = (res.Items ?? []) as ChatMessage[];
    return q.after ? items : items.reverse();
  }

  // A window centred on one message, for jumping to a search hit or a quoted
  // reply.
  async around(conversationId: string, messageId: string, radius = 15): Promise<ChatMessage[]> {
    const [older, newer] = await Promise.all([
      this.history(conversationId, { before: messageId, limit: radius }),
      this.history(conversationId, { after: messageId, limit: radius }),
    ]);
    const anchor = await this.db.send(
      new QueryCommand({
        TableName: this.table,
        KeyConditionExpression: 'conversation_id = :c AND message_id = :m',
        ExpressionAttributeValues: { ':c': conversationId, ':m': messageId },
      }),
    );
    return [...older, ...((anchor.Items ?? []) as ChatMessage[]), ...newer];
  }
}
