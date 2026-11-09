import { describe, expect, it } from 'vitest';
import { conversationIdFor, newMessageId, participantsOf, peerOf } from '../src/ids.js';

describe('conversationIdFor', () => {
  it('is the same whichever side opens the chat', () => {
    expect(conversationIdFor('u-42', 'u-7')).toBe(conversationIdFor('u-7', 'u-42'));
  });

  it('round-trips to its participants', () => {
    const id = conversationIdFor('buyer-1', 'seller-9');
    expect(participantsOf(id)).toEqual(['buyer-1', 'seller-9']);
    expect(peerOf(id, 'seller-9')).toBe('buyer-1');
  });

  it('rejects a chat with yourself and ids that would break parsing', () => {
    expect(() => conversationIdFor('a', 'a')).toThrow();
    expect(() => conversationIdFor('a_b', 'c')).toThrow();
  });

  it('refuses to name a peer for an outsider', () => {
    expect(() => peerOf(conversationIdFor('a', 'b'), 'c')).toThrow();
  });
});

describe('newMessageId', () => {
  it('sorts in creation order even within one millisecond', () => {
    const ids = Array.from({ length: 1000 }, newMessageId);
    expect([...ids].sort()).toEqual(ids);
  });
});
