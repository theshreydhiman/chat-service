import { PutObjectCommand, GetObjectCommand, type S3Client } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import express, { type Response } from 'express';
import type { Redis } from 'ioredis';
import { z } from 'zod';
import { requireUser, type AuthedRequest } from './auth.js';
import type { ConversationStore } from './conversations.js';
import { newMessageId } from './ids.js';
import type { MessageStore } from './messages.js';
import type { Presence } from './presence.js';
import { registerDevice } from './push.js';
import type { ChatMessage } from './types.js';

const MAX_UPLOAD_BYTES = 25 * 1024 * 1024;
const ALLOWED_MIME = /^(image\/(jpeg|png|webp|heic)|video\/(mp4|quicktime)|application\/pdf)$/;

export interface HttpDeps {
  conversations: ConversationStore;
  messages: MessageStore;
  presence: Presence;
  redis: Redis;
  s3: S3Client;
  bucket: string;
  jwtSecret: string;
}

export function createApp(deps: HttpDeps) {
  const app = express();
  app.use(express.json({ limit: '64kb' }));

  app.get('/healthz', (_req, res) => {
    res.json({ ok: true });
  });

  const api = express.Router();
  api.use(requireUser(deps.jwtSecret));

  const member = async (req: AuthedRequest, res: Response, conversationId: string) => {
    if (await deps.conversations.isParticipant(req.userId!, conversationId)) return true;
    res.status(404).json({ error: 'conversation not found' });
    return false;
  };

  // Attachment keys are stored on messages; URLs are signed on the way out so
  // they never expire inside the database.
  const withMediaUrls = (msgs: ChatMessage[]) =>
    Promise.all(
      msgs.map(async (m) =>
        m.attachments.length === 0
          ? m
          : {
              ...m,
              attachments: await Promise.all(
                m.attachments.map(async (a) => ({
                  ...a,
                  url: await getSignedUrl(deps.s3, new GetObjectCommand({ Bucket: deps.bucket, Key: a.key }), {
                    expiresIn: 3600,
                  }),
                })),
              ),
            },
      ),
    );

  api.post('/conversations', async (req: AuthedRequest, res) => {
    const body = z.object({ other_user_id: z.string() }).safeParse(req.body);
    if (!body.success) return res.status(400).json({ error: 'other_user_id required' });
    const result = await deps.conversations.open(req.userId!, body.data.other_user_id);
    res.status(result.created ? 201 : 200).json(result);
  });

  api.get('/conversations', async (req: AuthedRequest, res) => {
    const limit = Number(req.query.limit ?? 20);
    const page = await deps.conversations.inbox(req.userId!, limit, req.query.cursor as string | undefined);
    res.json(page);
  });

  api.get('/conversations/:id/messages', async (req: AuthedRequest, res) => {
    const id = req.params.id as string;
    if (!(await member(req, res, id))) return;
    const q = req.query as Record<string, string | undefined>;
    const items = q.around
      ? await deps.messages.around(id, q.around)
      : await deps.messages.history(id, { before: q.before, after: q.after, limit: Number(q.limit ?? 30) });
    res.json({ items: await withMediaUrls(items) });
  });

  api.post('/uploads', async (req: AuthedRequest, res) => {
    const body = z
      .object({
        conversation_id: z.string(),
        mime: z.string().regex(ALLOWED_MIME),
        size: z.number().int().positive().max(MAX_UPLOAD_BYTES),
      })
      .safeParse(req.body);
    if (!body.success) return res.status(400).json({ error: 'unsupported file' });
    if (!(await member(req, res, body.data.conversation_id))) return;

    const key = `${body.data.conversation_id}/${newMessageId()}`;
    const url = await getSignedUrl(
      deps.s3,
      new PutObjectCommand({
        Bucket: deps.bucket,
        Key: key,
        ContentType: body.data.mime,
        ContentLength: body.data.size,
      }),
      { expiresIn: 300 },
    );
    res.status(201).json({ key, url });
  });

  api.get('/presence/:userId', async (req, res) => {
    res.json({ online: await deps.presence.isOnline(req.params.userId as string) });
  });

  api.post('/devices', async (req: AuthedRequest, res) => {
    const body = z.object({ token: z.string().min(10) }).safeParse(req.body);
    if (!body.success) return res.status(400).json({ error: 'token required' });
    await registerDevice(deps.redis, req.userId!, body.data.token);
    res.status(204).end();
  });

  app.use('/v1', api);

  app.use((err: Error, _req: express.Request, res: Response, _next: express.NextFunction) => {
    const status = /invalid cursor|yourself|alphanumeric/.test(err.message) ? 400 : 500;
    res.status(status).json({ error: status === 400 ? err.message : 'internal error' });
  });

  return app;
}
