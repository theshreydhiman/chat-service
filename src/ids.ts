import { monotonicFactory } from 'ulid';

// Monotonic so two messages created in the same millisecond still sort in
// creation order. Message ids double as the sort key for history and as the
// ordering guard on inbox updates, so this matters.
const nextUlid = monotonicFactory();

export const newMessageId = (): string => nextUlid();

// One conversation per pair, by construction. Two requests racing to open the
// same chat compute the same id, so the conditional write in
// ConversationStore.open decides the winner and DynamoDB enforces uniqueness.
const USER_ID = /^[A-Za-z0-9-]+$/;

export function conversationIdFor(a: string, b: string): string {
  if (!USER_ID.test(a) || !USER_ID.test(b)) throw new Error('user ids must be alphanumeric');
  if (a === b) throw new Error('cannot open a conversation with yourself');
  const [lo, hi] = a < b ? [a, b] : [b, a];
  return `conv_${lo}_${hi}`;
}

export function participantsOf(conversationId: string): [string, string] {
  const parts = conversationId.split('_');
  if (parts.length !== 3 || parts[0] !== 'conv') {
    throw new Error(`malformed conversation id: ${conversationId}`);
  }
  return [parts[1]!, parts[2]!];
}

export function peerOf(conversationId: string, userId: string): string {
  const [a, b] = participantsOf(conversationId);
  if (userId === a) return b;
  if (userId === b) return a;
  throw new Error(`${userId} is not in ${conversationId}`);
}

export const userRoom = (userId: string) => `user_${userId}`;
