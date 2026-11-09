import { z } from 'zod';

const schema = z.object({
  PORT: z.coerce.number().default(4000),
  JWT_SECRET: z.string().min(16),
  AWS_REGION: z.string().default('ap-south-1'),
  DYNAMO_ENDPOINT: z.string().url().optional(),
  CONVERSATIONS_TABLE: z.string().default('chat_conversations'),
  MESSAGES_TABLE: z.string().default('chat_messages'),
  REDIS_URL: z.string().default('redis://localhost:6379'),
  KAFKA_BROKERS: z.string().default('localhost:9092'),
  KAFKA_CLIENT_ID: z.string().default('chat-service'),
  MESSAGES_TOPIC: z.string().default('chat.messages'),
  MEDIA_BUCKET: z.string().default('chat-media-local'),
  S3_ENDPOINT: z.string().url().optional(),
  FIREBASE_CREDENTIALS: z.string().optional(),
  LOG_LEVEL: z.enum(['debug', 'info', 'warn', 'error']).default('info'),
});

export type Config = z.infer<typeof schema>;

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const parsed = schema.safeParse(env);
  if (!parsed.success) {
    const fields = parsed.error.issues.map((i) => i.path.join('.')).join(', ');
    throw new Error(`invalid environment: ${fields}`);
  }
  return parsed.data;
}
