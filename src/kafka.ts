import { Kafka, logLevel, type Consumer, type Producer } from 'kafkajs';
import type { Logger } from 'pino';
import type { Config } from './config.js';

export function createKafka(config: Config): Kafka {
  return new Kafka({
    clientId: config.KAFKA_CLIENT_ID,
    brokers: config.KAFKA_BROKERS.split(','),
    logLevel: logLevel.WARN,
  });
}

export interface RetryPolicy {
  attempts: number;
  backoffMs: (attempt: number) => number;
}

export const defaultRetry: RetryPolicy = {
  attempts: 3,
  backoffMs: (attempt) => 200 * 2 ** (attempt - 1),
};

export type Handler = (value: Buffer, key: string | undefined) => Promise<void>;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// Retries a handler in place, then parks the record on <topic>.dlq so one bad
// message can't block its partition. Retrying in place (rather than through a
// retry topic) keeps per-conversation ordering: the next message for this
// conversation waits until this one has succeeded or been parked.
export function withRetryAndDlq(opts: {
  topic: string;
  handler: Handler;
  producer: Pick<Producer, 'send'>;
  log: Logger;
  retry?: RetryPolicy;
}): Handler {
  const retry = opts.retry ?? defaultRetry;
  const dlq = `${opts.topic}.dlq`;

  return async (value, key) => {
    let lastError: unknown;
    for (let attempt = 1; attempt <= retry.attempts; attempt++) {
      try {
        await opts.handler(value, key);
        return;
      } catch (err) {
        lastError = err;
        opts.log.warn({ err, attempt, key }, 'handler failed');
        if (attempt < retry.attempts) await sleep(retry.backoffMs(attempt));
      }
    }

    await opts.producer.send({
      topic: dlq,
      messages: [
        {
          key,
          value,
          headers: {
            'x-error': String(lastError instanceof Error ? lastError.message : lastError),
            'x-source-topic': opts.topic,
            'x-failed-at': new Date().toISOString(),
          },
        },
      ],
    });
    opts.log.error({ key, dlq }, 'moved to DLQ');
  };
}

export async function runConsumer(opts: {
  consumer: Consumer;
  topic: string;
  handler: Handler;
}): Promise<void> {
  await opts.consumer.subscribe({ topic: opts.topic, fromBeginning: false });
  await opts.consumer.run({
    // Offsets are committed only after the handler (including any DLQ write)
    // returns, so a crash mid-message means redelivery, never loss. The
    // handlers are written to make redelivery harmless.
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      await opts.handler(message.value, message.key?.toString());
    },
  });
}
