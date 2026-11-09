import jwt from 'jsonwebtoken';
import type { NextFunction, Request, Response } from 'express';

export interface AuthedRequest extends Request {
  userId?: string;
}

export function verifyToken(token: string | undefined, secret: string): string {
  if (!token) throw new Error('missing token');
  const payload = jwt.verify(token.replace(/^Bearer\s+/i, ''), secret);
  if (typeof payload === 'string' || typeof payload.sub !== 'string') {
    throw new Error('token has no subject');
  }
  return payload.sub;
}

export function requireUser(secret: string) {
  return (req: AuthedRequest, res: Response, next: NextFunction) => {
    try {
      req.userId = verifyToken(req.headers.authorization, secret);
      next();
    } catch {
      res.status(401).json({ error: 'unauthorized' });
    }
  };
}
