import { describe, expect, it, vi } from 'vitest';
import { withRetryAndDlq } from '../src/kafka.js';
import { silentLog } from './helpers.js';

const noBackoff = { attempts: 3, backoffMs: () => 0 };

describe('withRetryAndDlq', () => {
  it('retries a transient failure without touching the DLQ', async () => {
    const producer = { send: vi.fn() };
    const handler = vi.fn().mockRejectedValueOnce(new Error('throttled')).mockResolvedValueOnce(undefined);

    await withRetryAndDlq({ topic: 'chat.messages', handler, producer, log: silentLog, retry: noBackoff })(
      Buffer.from('{}'),
      'conv_a_b',
    );

    expect(handler).toHaveBeenCalledTimes(2);
    expect(producer.send).not.toHaveBeenCalled();
  });

  it('parks the record on the DLQ after the last attempt, keeping key and value', async () => {
    const producer = { send: vi.fn() };
    const handler = vi.fn().mockRejectedValue(new Error('bad payload'));
    const value = Buffer.from('{"message_id":"x"}');

    await withRetryAndDlq({ topic: 'chat.messages', handler, producer, log: silentLog, retry: noBackoff })(
      value,
      'conv_a_b',
    );

    expect(handler).toHaveBeenCalledTimes(3);
    const sent = producer.send.mock.calls[0]![0];
    expect(sent.topic).toBe('chat.messages.dlq');
    expect(sent.messages[0]).toMatchObject({ key: 'conv_a_b', value });
    expect(sent.messages[0].headers['x-error']).toBe('bad payload');
  });
});
