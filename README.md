# chat-service

One-to-one buyer–seller chat for a marketplace: realtime over Socket.IO, history over REST, every write through Kafka, state in DynamoDB and Redis, FCM for users who aren't connected.

I designed and built the production version of this service at work. That code belongs to the client, so this is a rebuild of the same architecture from scratch. It also fixes the problems I listed against the original (see [What changed from the first version](#what-changed-from-the-first-version)).

## How a message moves

```
 client ──send_message──▶ socket pod ──produce (key = conversation_id)──▶ chat.messages
   ▲                         │                                                 │
   │◀──── ack: SENT ─────────┘                                                 ▼
   │                                                                   delivery consumer
   │                                                     1. save        (conditional put)
   │                                                     2. inbox rows  (guarded by message id)
   │◀──── receive_message ── Redis adapter ◀─── online ─ 3. deliver ─── offline ──▶ FCM
```

The socket layer never writes to DynamoDB. It validates, assigns a ULID, produces, and acks. That keeps socket pods thin and lets a burst queue in Kafka instead of landing on the tables. Keying by `conversation_id` keeps each conversation's messages in order within a partition.

Kafka delivers at least once, so every consumer step is safe to repeat:

- **Save** uses `attribute_not_exists(message_id)`, so a redelivered message is detected, not duplicated.
- **Inbox update** only lands if the message id sorts after the one already on the row. ULIDs sort by time, so this one condition handles replays (no double `unread_count`) and out-of-order arrival.
- **Delivery** can repeat; clients dedupe on `message_id`.

A redelivery still runs the inbox and delivery steps. If the first attempt died right after saving, stopping at "already saved" would leave a stored message nobody was told about.

A handler that keeps failing is retried in place three times with backoff, then parked on `chat.messages.dlq` with the error in the headers. Retrying in place, rather than through a retry topic, keeps ordering: the next message in that conversation waits.

## Data model

| Table | Key | Notes |
| --- | --- | --- |
| `chat_conversations` | `user_id`, `conversation_id` | Two rows per conversation, one per participant, so each user lists and sorts their own inbox. Holds the preview, `unread_count`, `last_message_id` and `last_read_message_id`. |
| ↳ `by_last_message` | `user_id`, `last_message_at` | Inbox, newest first. Sparse: a conversation appears once it has a message. |
| `chat_messages` | `conversation_id`, `message_id` | ULID sort key, so the key is also the timeline. |

Read state is a high-water mark on the reader's conversation row, not a status on each message. Marking a long thread read is one write instead of N.

Redis holds presence, the `conv:known:*` shortcut, client send refs, push tokens, and the Socket.IO adapter channel. Nothing in it is needed for correctness.

## Decisions worth explaining

**One conversation per pair, enforced by DynamoDB.** The conversation id is derived from the two user ids (`conv_{min}_{max}`), and both rows go in one `TransactWriteItems` conditioned on not existing. Two people opening the same chat at once compute the same id, one transaction wins, and the other gets a condition failure that just means "already exists".

**Presence is per socket.** Each socket is a member of `presence:{user}`, a sorted set scored by its last heartbeat. A user with a phone and a laptop stays online until both disconnect. Sockets on a pod that dies without running disconnect handlers stop heartbeating and age out after 75 seconds.

**Sends are idempotent from the client.** Each message carries a client-generated `client_ref`. The server claims it in Redis before producing, so a client that resends after a timeout or `send_error` reuses the first message id, and the consumer treats the resend as a redelivery.

**Cross-pod delivery.** The consumer that handles a message is usually not on the pod holding the recipient's socket. Emits go to `user_{id}` rooms through the Redis adapter, which fans out to every pod.

**Media.** Clients upload straight to S3 with a short-lived presigned PUT. Messages store only the object key, and download URLs are signed when messages are read, so nothing expired is ever persisted.

## What changed from the first version

The production service started with check-then-create on conversations, with a random id per request. Overlapping requests (a double-tap, a retry, both users opening the chat at once) each found nothing and each created a conversation, and users ended up with two threads with the same person. The first fix was a Redis lock per pair; the race, that fix and a bug in its error path are reproduced in [conversation-race-repro](https://github.com/theshreydhiman/conversation-race-repro).

Writing that fix up surfaced more problems, and this version addresses them:

| First version | Here |
| --- | --- |
| Uniqueness depended on a Redis lock and was lost while Redis was down | Deterministic id plus a conditional transaction. Redis is only a shortcut. |
| The lock's error path could delete another request's lock | No lock to release |
| The two conversation rows were written with `Promise.all`, so a partial failure left an orphan | One transaction |
| Presence stored one socket id, so a second device disconnecting marked the user offline | Per-socket sorted set with heartbeats |
| A failed Kafka produce surfaced as `send_error`, with no safe way for the client to retry | `client_ref` makes resends safe |

## Running it

```
docker compose up -d
cp .env.example .env
npm install
npm run tables
npm run dev
```

Create the `chat-media-local` bucket in MinIO (console on :9000) before trying uploads.

Clients authenticate with a JWT whose `sub` is the user id: `Authorization: Bearer …` for REST, `auth: { token }` in the Socket.IO handshake.

### API

| | |
| --- | --- |
| `POST /v1/conversations` `{ other_user_id }` | Open or fetch the conversation. 201 if created, 200 if it existed. |
| `GET /v1/conversations?cursor=` | Inbox |
| `GET /v1/conversations/:id/messages?before=&after=&around=` | History, catch-up after reconnect, or a window around one message |
| `POST /v1/uploads` `{ conversation_id, mime, size }` | Presigned upload URL and key |
| `POST /v1/devices` `{ token }` | Register an FCM token |
| `GET /v1/presence/:userId` | |

Socket events: `send_message`, `read_receipt` and `typing` from the client; `receive_message`, `message_status`, `message_synced`, `messages_read` and `typing` from the server.

## Tests

```
npm test
```

The presence and conversation tests need Redis (`REDIS_URL`, default `redis://localhost:6390`) and are skipped without it. The conversation test fires 20 concurrent opens from both sides and checks that exactly one conversation is created, with and without Redis.

## Not done

- Enquiry and offer events from other marketplace services. In production these arrive on their own topics and become a conversation plus a typed first message; the message types are here but the consumers aren't.
- Blocking and reporting.
- Integration tests against DynamoDB Local and Redpanda in CI.
