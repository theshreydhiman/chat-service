import { createServer } from 'node:http';
import { S3Client } from '@aws-sdk/client-s3';
import { createAdapter } from '@socket.io/redis-adapter';
import { Redis } from 'ioredis';
import { pino } from 'pino';
import { Server } from 'socket.io';
import { loadConfig } from './config.js';
import { ConversationStore } from './conversations.js';
import { createDeliveryHandler } from './delivery.js';
import { createDocumentClient } from './dynamo.js';
import { createApp } from './http.js';
import { createKafka, runConsumer, withRetryAndDlq } from './kafka.js';
import { MessageStore } from './messages.js';
import { Presence } from './presence.js';
import { FcmPushSender, LogPushSender, type PushSender } from './push.js';
import { attachSocketHandlers } from './socket.js';

async function main() {
  const config = loadConfig();
  const log = pino({ level: config.LOG_LEVEL });

  const db = createDocumentClient(config);
  const redis = new Redis(config.REDIS_URL);
  const s3 = new S3Client({
    region: config.AWS_REGION,
    ...(config.S3_ENDPOINT && { endpoint: config.S3_ENDPOINT, forcePathStyle: true }),
  });

  const conversations = new ConversationStore(db, redis, config.CONVERSATIONS_TABLE);
  const messages = new MessageStore(db, config.MESSAGES_TABLE);
  const presence = new Presence(redis);

  let push: PushSender = new LogPushSender(log);
  if (config.FIREBASE_CREDENTIALS) {
    const { initializeApp, cert } = await import('firebase-admin/app');
    const { getMessaging } = await import('firebase-admin/messaging');
    const app = initializeApp({ credential: cert(JSON.parse(config.FIREBASE_CREDENTIALS)) });
    push = new FcmPushSender(redis, getMessaging(app), log);
  }

  const http = createServer(createApp({ conversations, messages, presence, redis, s3, bucket: config.MEDIA_BUCKET, jwtSecret: config.JWT_SECRET }));

  // The consumer that delivers a message is usually not on the pod holding the
  // recipient's socket. The Redis adapter fans room emits out to every pod.
  const io = new Server(http, { cors: { origin: true } });
  io.adapter(createAdapter(redis.duplicate(), redis.duplicate()));

  const kafka = createKafka(config);
  const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
  const consumer = kafka.consumer({ groupId: `${config.KAFKA_CLIENT_ID}.delivery` });
  await Promise.all([producer.connect(), consumer.connect()]);

  attachSocketHandlers({
    io,
    redis,
    producer,
    topic: config.MESSAGES_TOPIC,
    conversations,
    presence,
    jwtSecret: config.JWT_SECRET,
    log,
  });

  await runConsumer({
    consumer,
    topic: config.MESSAGES_TOPIC,
    handler: withRetryAndDlq({
      topic: config.MESSAGES_TOPIC,
      producer,
      log,
      handler: createDeliveryHandler({ messages, inbox: conversations, presence, push, io, log }),
    }),
  });

  http.listen(config.PORT, () => log.info({ port: config.PORT }, 'listening'));

  // Stop taking new sockets first, then let the consumer finish its in-flight
  // message and commit before the connections go away.
  const shutdown = async (signal: string) => {
    log.info({ signal }, 'shutting down');
    io.close();
    await consumer.disconnect();
    await producer.disconnect();
    redis.disconnect();
    process.exit(0);
  };
  process.once('SIGTERM', () => void shutdown('SIGTERM'));
  process.once('SIGINT', () => void shutdown('SIGINT'));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
