import { existsSync } from 'node:fs';
import { join } from 'node:path';
import express, { type NextFunction, type Request, type Response } from 'express';
import { GradeRequest, allQuestions, type Course, type HealthResponse, type OpenQ } from '@trainer/shared';
import type { Grader } from './grader';

export interface AppOptions {
  course: Course;
  /** Undefined when no API credentials are configured; /api/grade then answers 503. */
  grader?: Grader;
  /** Built web app to serve in production. */
  staticDir?: string;
  /** Max grading calls per client per minute. */
  rateLimitPerMinute?: number;
}

/** Tiny fixed-window limiter; enough to stop a runaway client burning API credits. */
function rateLimiter(limit: number) {
  const hits = new Map<string, { count: number; resetAt: number }>();
  return (req: Request, res: Response, next: NextFunction) => {
    const now = Date.now();
    const key = req.ip ?? 'unknown';
    const entry = hits.get(key);
    if (!entry || entry.resetAt <= now) {
      hits.set(key, { count: 1, resetAt: now + 60_000 });
      return next();
    }
    if (++entry.count > limit) {
      res.setHeader('Retry-After', String(Math.ceil((entry.resetAt - now) / 1000)));
      return res.status(429).json({ error: 'Too many grading requests, slow down a little.' });
    }
    next();
  };
}

export function createApp({ course, grader, staticDir, rateLimitPerMinute = 30 }: AppOptions) {
  const openQuestions = new Map<string, OpenQ>();
  for (const q of allQuestions(course)) if (q.type === 'open') openQuestions.set(q.id, q);

  const app = express();
  app.disable('x-powered-by');
  app.use(express.json({ limit: '32kb' }));

  app.get('/api/health', (_req, res) => {
    const body: HealthResponse = { ok: true, ai: Boolean(grader), model: grader?.model };
    res.json(body);
  });

  app.post('/api/grade', rateLimiter(rateLimitPerMinute), async (req, res) => {
    if (!grader) return res.status(503).json({ error: 'AI grading is not configured on this server.' });

    const parsed = GradeRequest.safeParse(req.body);
    if (!parsed.success) return res.status(400).json({ error: 'Invalid request body.' });
    const { questionId, answer } = parsed.data;

    const question = openQuestions.get(questionId);
    if (!question) return res.status(404).json({ error: 'Unknown open question.' });
    if (answer.trim().length < 3) return res.status(400).json({ error: 'Answer is empty.' });
    if (answer.length > question.maxChars * 2) return res.status(413).json({ error: 'Answer is too long.' });

    try {
      res.json(await grader.grade(question, answer));
    } catch (err) {
      console.error('[grade] failed:', err instanceof Error ? err.message : err);
      // The client falls back to self-assessment on any non-2xx, so keep the detail server-side.
      res.status(502).json({ error: 'AI grading failed.' });
    }
  });

  if (staticDir && existsSync(staticDir)) {
    app.use(express.static(staticDir));
    app.get(/^(?!\/api\/).*/, (_req, res) => res.sendFile(join(staticDir, 'index.html')));
  }

  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    const status = typeof err === 'object' && err && 'status' in err ? Number((err as { status: number }).status) : 500;
    res.status(status >= 400 && status < 600 ? status : 500).json({ error: 'Request failed.' });
  });

  return app;
}
