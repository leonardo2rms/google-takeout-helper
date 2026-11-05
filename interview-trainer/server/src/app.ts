import { existsSync } from 'node:fs';
import express, { type NextFunction, type Request, type Response } from 'express';
import {
  ExplainRequest, GradeRequest, allQuestions, explainableQuestions, openParts,
  type Course, type ExplainResponse, type HealthResponse, type OpenQ, type Question,
} from '@trainer/shared';
import type { Grader } from './grader';
import type { Tutor } from './tutor';

export interface AppOptions {
  course: Course;
  /** Undefined when no API credentials are configured; /api/grade then answers 503. */
  grader?: Grader;
  /** Undefined when no API credentials are configured; /api/explain then answers 503. */
  tutor?: Tutor;
  /** Built web app to serve in production. */
  staticDir?: string;
  /** Max grading calls per client per minute. */
  rateLimitPerMinute?: number;
  /** Max "Ask why" calls per client per minute. */
  explainRateLimitPerMinute?: number;
  /**
   * Express `trust proxy` setting (e.g. 1 behind one reverse proxy). Off by default: `X-Forwarded-For` is then ignored,
   * which is right on a directly exposed server and wrong behind a proxy, where every user would share one rate-limit bucket.
   */
  trustProxy?: boolean | number | string;
}

/** Content-Security-Policy of the app shell. `'unsafe-eval'` is needed by the code-question runner (`new Function` in a blob worker). */
export const CSP = [
  "default-src 'self'",
  "script-src 'self' 'unsafe-eval'",
  "style-src 'self' 'unsafe-inline'",
  "img-src 'self' data: blob:",
  "font-src 'self' data:",
  "connect-src 'self'",
  "worker-src 'self' blob:",
  "manifest-src 'self'",
  "object-src 'none'",
  "base-uri 'none'",
  "form-action 'self'",
  "frame-ancestors 'none'",
].join('; ');

function securityHeaders(_req: Request, res: Response, next: NextFunction) {
  res.setHeader('Content-Security-Policy', CSP);
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('X-Frame-Options', 'DENY');
  next();
}

/** A request for a file (an extension, a dot-path like /.git/config) or anything under /assets/: it must exist or be a real 404, never the HTML shell. */
const isAssetPath = (path: string): boolean => path.startsWith('/assets/') || /\.[A-Za-z0-9]{1,12}$/.test(path) || /(^|\/)\.[^/]/.test(path);

/** Tiny fixed-window limiter; enough to stop a runaway client burning API credits. */
function rateLimiter(limit: number, message = 'Too many grading requests, slow down a little.') {
  const hits = new Map<string, { count: number; resetAt: number }>();
  return (req: Request, res: Response, next: NextFunction) => {
    const now = Date.now();
    // Expired windows are dropped as the table grows, so a stream of distinct addresses cannot grow it without bound.
    if (hits.size > 1000) for (const [k, v] of hits) if (v.resetAt <= now) hits.delete(k);
    const key = req.ip ?? 'unknown';
    const entry = hits.get(key);
    if (!entry || entry.resetAt <= now) {
      hits.set(key, { count: 1, resetAt: now + 60_000 });
      return next();
    }
    if (++entry.count > limit) {
      res.setHeader('Retry-After', String(Math.ceil((entry.resetAt - now) / 1000)));
      return res.status(429).json({ error: message });
    }
    next();
  };
}

export function createApp({ course, grader, tutor, staticDir, rateLimitPerMinute = 30, explainRateLimitPerMinute = 20, trustProxy }: AppOptions) {
  const openQuestions = new Map<string, OpenQ>();
  // Follow-ups are graded like standalone open questions, under their own ids (`<id>~f1`, ...).
  for (const q of allQuestions(course)) if (q.type === 'open') for (const part of openParts(q)) openQuestions.set(part.id, part);

  const explainable: Map<string, Question> = explainableQuestions(allQuestions(course));

  const app = express();
  app.disable('x-powered-by');
  if (trustProxy !== undefined) app.set('trust proxy', trustProxy);
  app.use(securityHeaders);
  app.use(express.json({ limit: '32kb' }));

  app.get('/api/health', (_req, res) => {
    const body: HealthResponse = { ok: true, ai: Boolean(grader), model: grader?.model, explain: Boolean(tutor) };
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

  app.post('/api/explain', rateLimiter(explainRateLimitPerMinute, 'Too many questions, give it a minute.'), async (req, res) => {
    if (!tutor) return res.status(503).json({ error: 'The AI tutor is not configured on this server.' });

    const parsed = ExplainRequest.safeParse(req.body);
    if (!parsed.success) return res.status(400).json({ error: 'Invalid request body.' });
    const { questionId, answer, correct, messages } = parsed.data;

    // The question, the correct answer and the explanation come from the course, never from the client.
    const question = explainable.get(questionId);
    if (!question) return res.status(404).json({ error: 'Unknown question.' });

    try {
      const body: ExplainResponse = { reply: await tutor.explain(question, answer, correct, messages) };
      res.json(body);
    } catch (err) {
      console.error('[explain] failed:', err instanceof Error ? err.message : err);
      res.status(502).json({ error: 'The AI tutor failed.' });
    }
  });

  // A known path with the wrong method says so (405 + Allow) instead of looking like a missing page.
  const only = (method: 'GET' | 'POST') => (_req: Request, res: Response) => res.setHeader('Allow', method).status(405).json({ error: 'Method not allowed.' });
  app.all('/api/health', only('GET'));
  app.all('/api/grade', only('POST'));
  app.all('/api/explain', only('POST'));
  // Everything else under /api (with or without a trailing slash) is a JSON 404, never the HTML shell.
  app.use((req, res, next) => (req.path === '/api' || req.path.startsWith('/api/') ? res.status(404).json({ error: 'Not found.' }) : next()));

  if (staticDir && existsSync(staticDir)) {
    app.use(
      express.static(staticDir, {
        setHeaders(res, path) {
          // Hashed bundle files never change; everything else (index.html, sw.js, manifest) must be revalidated so a deploy is seen.
          res.setHeader('Cache-Control', /[\\/]assets[\\/]/.test(path) ? 'public, max-age=31536000, immutable' : 'no-cache');
        },
      }),
    );
    // Only page navigations fall back to the shell. A missing script, stylesheet, source map or dotfile is a real 404:
    // answering it with index.html (status 200) once poisoned the service worker's cache with HTML-as-JavaScript.
    // `root` (not a joined absolute path): sendFile refuses paths with a dot-directory in them (e.g. /srv/.apps/trainer/...).
    app.get(/^(?!\/api(\/|$)).*/, (req, res) => {
      if (isAssetPath(req.path)) return res.status(404).type('text/plain').send('Not found');
      res.setHeader('Cache-Control', 'no-cache');
      res.sendFile('index.html', { root: staticDir });
    });
  }

  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    const status = typeof err === 'object' && err && 'status' in err ? Number((err as { status: number }).status) : 500;
    res.status(status >= 400 && status < 600 ? status : 500).json({ error: 'Request failed.' });
  });

  return app;
}
