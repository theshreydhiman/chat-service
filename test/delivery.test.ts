import { describe, expect, it, vi } from 'vitest';
import { createDeliveryHandler } from '../src/delivery.js';
import type { ChatMessage } from '../src/types.js';
import { silentLog } from './helpers.js';

const msg: ChatMessage = {
  conversation_id: 'conv_buyer_seller',
  message_id: '01JC0000000000000000000000',
  sender_id: 'buyer',
  recipient_id: 'seller',
  type: 'TEXT',
  message: 'is this still available?',
  attachments: [],
  created_at: '2026-11-10T10:00:00.000Z',
};

function setup({ online, alreadySaved = false }: { online: boolean; alreadySaved?: boolean }) {
  const emitted: { room: string; event: string }[] = [];
  const deps = {
    messages: { save: vi.fn().mockResolvedValue(!alreadySaved) },
    inbox: { applyMessage: vi.fn().mockResolvedValue(undefined) },
    presence: { isOnline: vi.fn().mockResolvedValue(online) },
    push: { notify: vi.fn().mockResolvedValue(undefined) },
    io: { to: (room: string) => ({ emit: (event: string) => emitted.push({ room, event }) }) },
    log: silentLog,
  };
  return { deps, emitted, handle: createDeliveryHandler(deps) };
}

describe('delivery handler', () => {
  it('emits to an online recipient and tells the sender it was delivered', async () => {
    const { deps, emitted, handle } = setup({ online: true });
    await handle(Buffer.from(JSON.stringify(msg)));

    expect(deps.inbox.applyMessage).toHaveBeenCalledWith(msg);
    expect(emitted).toContainEqual({ room: 'user_seller', event: 'receive_message' });
    expect(emitted).toContainEqual({ room: 'user_buyer', event: 'message_status' });
    expect(deps.push.notify).not.toHaveBeenCalled();
  });

  it('pushes to an offline recipient', async () => {
    const { deps, emitted, handle } = setup({ online: false });
    await handle(Buffer.from(JSON.stringify(msg)));

    expect(deps.push.notify).toHaveBeenCalledWith('seller', msg);
    expect(emitted.find((e) => e.event === 'receive_message')).toBeUndefined();
  });

  // The first attempt may have died after saving. A redelivery has to finish
  // the job rather than stop at "already saved".
  it('still updates the inbox and delivers when the message was already saved', async () => {
    const { deps, emitted, handle } = setup({ online: true, alreadySaved: true });
    await handle(Buffer.from(JSON.stringify(msg)));

    expect(deps.inbox.applyMessage).toHaveBeenCalledWith(msg);
    expect(emitted).toContainEqual({ room: 'user_seller', event: 'receive_message' });
  });
});
