import type { Redis } from 'ioredis';
import type { Producer } from 'kafkajs';
import type { Logger } from 'pino';
import type { Server, Socket } from 'socket.io';
import { z } from 'zod';
import { verifyToken } from './auth.js';
import type { ConversationStore } from './conversations.js';
import { newMessageId, peerOf, userRoom } from './ids.js';
import { Presence } from './presence.js';
import { sendMessageSchema, type ChatMessage } from './types.js';

type Ack = (res: { ok: true; [k: string]: unknown } | { ok: false; error: string }) => void;

const readReceiptSchema = z.object({ conversation_id: z.string(), up_to: z.string() });
const typingSchema = z.object({ conversation_id: z.string(), typing: z.boolean() });

const CLIENT_REF_TTL_S = 600;

export interface SocketDeps {
  io: Server;
  redis: Redis;
  producer: Producer;
  topic: string;
  conversations: ConversationStore;
  presence: Presence;
  jwtSecret: string;
  log: Logger;
}

export function attachSocketHandlers(deps: SocketDeps): void {
  const { io, log } = deps;

  io.use((socket, next) => {
    try {
      socket.data.userId = verifyToken(socket.handshake.auth?.token, deps.jwtSecret);
      next();
    } catch {
      next(new Error('unauthorized'));
    }
  });

  io.on('connection', async (socket: Socket) => {
    const userId: string = socket.data.userId;
    // Membership never changes once a conversation exists, so it's safe to
    // remember for the life of the socket instead of reading it per message.
    const memberOf = new Set<string>();

    await socket.join(userRoom(userId));
    await deps.presence.connect(userId, socket.id);
    const heartbeat = setInterval(
      () => deps.presence.touch(userId, socket.id).catch((err) => log.warn({ err }, 'heartbeat failed')),
      Presence.heartbeatIntervalMs,
    );

    const assertMember = async (conversationId: string) => {
      if (memberOf.has(conversationId)) return;
      if (!(await deps.conversations.isParticipant(userId, conversationId))) {
        throw new Error('not a participant');
      }
      memberOf.add(conversationId);
    };

    socket.on('send_message', async (raw: unknown, ack: Ack = () => {}) => {
      const parsed = sendMessageSchema.safeParse(raw);
      if (!parsed.success) return ack({ ok: false, error: 'invalid payload' });
      const input = parsed.data;

      try {
        await assertMember(input.conversation_id);

        // Claim the client's ref before producing. A retried send finds the
        // id from the first attempt and re-produces that same message, which
        // the consumer then treats as a redelivery.
        const candidateId = newMessageId();
        const refKey = `send:${userId}:${input.client_ref}`;
        const claimed = await deps.redis.set(refKey, candidateId, 'EX', CLIENT_REF_TTL_S, 'NX');
        const messageId = claimed ? candidateId : ((await deps.redis.get(refKey)) ?? candidateId);

        const msg: ChatMessage = {
          conversation_id: input.conversation_id,
          message_id: messageId,
          sender_id: userId,
          recipient_id: peerOf(input.conversation_id, userId),
          type: input.type,
          message: input.message,
          attachments: input.attachments,
          created_at: new Date().toISOString(),
        };

        await deps.producer.send({
          topic: deps.topic,
          messages: [{ key: msg.conversation_id, value: JSON.stringify(msg) }],
        });
        ack({ ok: true, message_id: messageId, status: 'SENT' });
      } catch (err) {
        log.warn({ err, userId }, 'send_message failed');
        ack({ ok: false, error: err instanceof Error ? err.message : 'send failed' });
      }
    });

    socket.on('read_receipt', async (raw: unknown, ack: Ack = () => {}) => {
      const parsed = readReceiptSchema.safeParse(raw);
      if (!parsed.success) return ack({ ok: false, error: 'invalid payload' });
      const { conversation_id, up_to } = parsed.data;
      try {
        await assertMember(conversation_id);
        if (await deps.conversations.markRead(userId, conversation_id, up_to)) {
          io.to(userRoom(peerOf(conversation_id, userId))).emit('messages_read', {
            conversation_id,
            reader_id: userId,
            up_to,
          });
        }
        ack({ ok: true });
      } catch (err) {
        ack({ ok: false, error: err instanceof Error ? err.message : 'failed' });
      }
    });

    // Typing is fire-and-forget and never persisted.
    socket.on('typing', async (raw: unknown) => {
      const parsed = typingSchema.safeParse(raw);
      if (!parsed.success) return;
      try {
        await assertMember(parsed.data.conversation_id);
        io.to(userRoom(peerOf(parsed.data.conversation_id, userId))).emit('typing', {
          ...parsed.data,
          user_id: userId,
        });
      } catch {
        // not a member; ignore
      }
    });

    socket.on('disconnect', async () => {
      clearInterval(heartbeat);
      try {
        await deps.presence.disconnect(userId, socket.id);
      } catch (err) {
        log.warn({ err, userId }, 'presence disconnect failed');
      }
    });
  });
}
