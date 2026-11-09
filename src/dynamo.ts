import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { Config } from './config.js';

export function createDocumentClient(config: Config): DynamoDBDocumentClient {
  const client = new DynamoDBClient({
    region: config.AWS_REGION,
    ...(config.DYNAMO_ENDPOINT && {
      endpoint: config.DYNAMO_ENDPOINT,
      credentials: { accessKeyId: 'local', secretAccessKey: 'local' },
    }),
  });
  return DynamoDBDocumentClient.from(client, {
    marshallOptions: { removeUndefinedValues: true },
  });
}

export const isConditionFailure = (err: unknown): boolean =>
  err instanceof Error && err.name === 'ConditionalCheckFailedException';

// TransactWrite reports per-item reasons; we only care whether any of them was
// a failed condition (as opposed to throttling or a conflict with another tx).
export function isTransactionConditionFailure(err: unknown): boolean {
  if (!(err instanceof Error) || err.name !== 'TransactionCanceledException') return false;
  const reasons = (err as { CancellationReasons?: { Code?: string }[] }).CancellationReasons ?? [];
  return reasons.some((r) => r.Code === 'ConditionalCheckFailed');
}

export const encodeCursor = (key: Record<string, unknown> | undefined) =>
  key ? Buffer.from(JSON.stringify(key)).toString('base64url') : undefined;

export function decodeCursor(cursor: string | undefined): Record<string, unknown> | undefined {
  if (!cursor) return undefined;
  try {
    return JSON.parse(Buffer.from(cursor, 'base64url').toString());
  } catch {
    throw new Error('invalid cursor');
  }
}
