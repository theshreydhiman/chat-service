import type { Logger } from 'pino';
import type { InboxUpdater } from './conversations.js';
import { userRoom } from './ids.js';
import type { MessageWriter } from './messages.js';
import type { PushSender } from './push.js';
import type { ChatMessage } from './types.js';

export interface RoomEmitter {
  to(room: string): { emit(event: string, payload: unknown): unknown };
}

export interface DeliveryDeps {
  messages: MessageWriter;
  inbox: InboxUpdater;
  presence: { isOnline(userId: string): Promise<boolean> };
  push: PushSender;
  io: RoomEmitter;
  log: Logger;
}

// Consumes chat.messages. Every step is safe to repeat, because Kafka delivers
// at least once and a handler can die halfway through:
//
//   1. save        conditional put; a redelivery is detected, not duplicated
//   2. inbox       guarded by message id; a replay can't double-count unread
//   3. deliver     socket emit or push; clients dedupe on message_id
//
// A redelivery still runs steps 2 and 3. If the first attempt died after the
// save, skipping them would leave the message stored but never shown.
export function createDeliveryHandler(deps: DeliveryDeps) {
  return async (value: Buffer): Promise<void> => {
    const msg = JSON.parse(value.toString()) as ChatMessage;

    const fresh = await deps.messages.save(msg);
    if (!fresh) deps.log.debug({ messageId: msg.message_id }, 'redelivered message, resuming');

    await deps.inbox.applyMessage(msg);

    if (await deps.presence.isOnline(msg.recipient_id)) {
      deps.io.to(userRoom(msg.recipient_id)).emit('receive_message', msg);
      deps.io.to(userRoom(msg.sender_id)).emit('message_status', {
        conversation_id: msg.conversation_id,
        message_id: msg.message_id,
        status: 'DELIVERED',
      });
    } else {
      await deps.push.notify(msg.recipient_id, msg);
    }

    // Other devices of the sender also need the message in their timeline.
    deps.io.to(userRoom(msg.sender_id)).emit('message_synced', msg);
  };
}
