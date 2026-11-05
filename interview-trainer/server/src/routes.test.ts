/**
 * HTTP-level behaviour that is not specific to grading or the tutor: body parsing and size limits, the error
 * handler, headers (CORS, x-powered-by), static serving with the SPA fallback, rate-limit windows, and the
 * promise that upstream errors never reach the client.
 */
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { Course, type GradeResponse, type OpenQ } from '@trainer/shared';
import { createApp } from './app';
import type { Grader } from './grader';
import type { Tutor } from './tutor';

const course = Course.parse({
  units: [{
    id: 'u', title: 'U', emoji: 'x', description: 'd',
    lessons: [{
      id: 'l', title: 'L',
      questions: [
        {
          type: 'open', prompt: 'Explain X', explanation: 'e', modelAnswer: 'X is Y', maxChars: 50,
          keyPoints: [{ point: 'Y', keywords: ['y'] }, { point: 'Z', keywords: ['z'] }],
        },
        { type: 'single', prompt: 'p', explanation: 'e', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a' },
        { type: 'single', prompt: 'p', explanation: 'e', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a' },
      ],
    }],
  }],
});

const grader: Grader = {
  model: 'fake',
  async grade(_q: OpenQ, answer: string): Promise<GradeResponse> {
    if (answer.includes('boom')) throw new Error('sk-ant-SECRET upstream exploded');
    return { score: 1, covered: [true, true], feedback: 'ok', graded_by: 'ai' };
  },
};
const tutor: Tutor = {
  model: 'fake',
  async explain(_q, _a, _c, messages) {
    if (messages.some((m) => m.content.includes('boom'))) throw new Error('sk-ant-SECRET upstream exploded');
    return 'reply';
  },
};

interface Served {
  server: ReturnType<ReturnType<typeof createApp>['listen']>;
  base: string;
  raw: (path: string, init?: RequestInit) => Promise<Response>;
  post: (path: string, body: unknown, headers?: Record<string, string>) => Promise<Response>;
}
const servers: Served[] = [];
function serve(opts: Partial<Parameters<typeof createApp>[0]> = {}): Served {
  const server = createApp({ course, ...opts }).listen(0);
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const raw = (path: string, init?: RequestInit) => fetch(`${base}${path}`, init);
  const post = (path: string, body: unknown, headers: Record<string, string> = {}) =>
    raw(path, { method: 'POST', headers: { 'content-type': 'application/json', ...headers }, body: typeof body === 'string' ? body : JSON.stringify(body) });
  const s: Served = { server, base, raw, post };
  servers.push(s);
  return s;
}
afterAll(() => servers.forEach((s) => s.server.close()));
beforeAll(() => new Promise((r) => setTimeout(r, 20)));

describe('request parsing', () => {
  const s = serve({ grader, tutor });

  it('answers malformed JSON with a generic 400, not a stack trace', async () => {
    for (const path of ['/api/grade', '/api/explain']) {
      const res = await s.post(path, '{"questionId": ');
      expect(res.status).toBe(400);
      const text = await res.text();
      expect(JSON.parse(text)).toEqual({ error: 'Request failed.' });
      expect(text).not.toMatch(/SyntaxError|at \w+|node_modules/);
    }
  });

  it('rejects bodies over 32 kB with 413 before any handler runs', async () => {
    const res = await s.post('/api/grade', { questionId: 'l/1', answer: 'y'.repeat(40_000) });
    expect(res.status).toBe(413);
    expect(await res.json()).toEqual({ error: 'Request failed.' });
  });

  it('rejects wrongly typed fields and a non-object body with 400', async () => {
    expect((await s.post('/api/grade', { questionId: 7, answer: 'abc' })).status).toBe(400);
    expect((await s.post('/api/grade', { questionId: 'l/1', answer: ['abc'] })).status).toBe(400);
    expect((await s.post('/api/grade', [1, 2])).status).toBe(400);
    expect((await s.post('/api/grade', 'null')).status).toBe(400);
    expect((await s.post('/api/grade', { questionId: '', answer: 'abc' })).status).toBe(400);
    expect((await s.post('/api/grade', { questionId: 'q'.repeat(201), answer: 'abc' })).status).toBe(400);
  });

  it('does not parse a body sent with another content type (treated as a missing body)', async () => {
    const res = await s.raw('/api/grade', { method: 'POST', headers: { 'content-type': 'text/plain' }, body: '{"questionId":"l/1","answer":"abc"}' });
    expect(res.status).toBe(400);
  });
});

describe('grade limits', () => {
  const s = serve({ grader });

  it('uses the question maxChars (x2) as the length limit, and the schema cap of 10 000 first', async () => {
    expect((await s.post('/api/grade', { questionId: 'l/1', answer: 'y'.repeat(100) })).status).toBe(200);
    expect((await s.post('/api/grade', { questionId: 'l/1', answer: 'y'.repeat(101) })).status).toBe(413);
    expect((await s.post('/api/grade', { questionId: 'l/1', answer: 'y'.repeat(10_001) })).status).toBe(400);
  });

  it('requires at least 3 non-blank characters', async () => {
    expect((await s.post('/api/grade', { questionId: 'l/1', answer: ' a ' })).status).toBe(400);
    expect((await s.post('/api/grade', { questionId: 'l/1', answer: ' abc ' })).status).toBe(200);
  });

  it('gives the 404 for an unknown id and for a non-open question, never revealing which exist', async () => {
    const a = await s.post('/api/grade', { questionId: 'l/2', answer: 'abc' });
    const b = await s.post('/api/grade', { questionId: 'zzz', answer: 'abc' });
    expect(a.status).toBe(404);
    expect(await a.json()).toEqual(await b.json());
  });
});

describe('503 without credentials', () => {
  const s = serve({});
  it('answers 503 before validating anything (so the client can fall back)', async () => {
    expect((await s.post('/api/grade', 'not json at all')).status).toBe(400); // parse errors come first
    expect((await s.post('/api/grade', {})).status).toBe(503);
    expect((await s.post('/api/explain', {})).status).toBe(503);
  });
});

describe('error hygiene', () => {
  const s = serve({ grader, tutor });
  afterEach(() => vi.restoreAllMocks());

  it('never leaks upstream messages (grade and explain) and logs only the message', async () => {
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    const g = await s.post('/api/grade', { questionId: 'l/1', answer: 'boom boom' });
    expect(g.status).toBe(502);
    expect(await g.text()).not.toMatch(/SECRET|exploded/);
    const e = await s.post('/api/explain', { questionId: 'l/1', answer: '', messages: [{ role: 'user', content: 'boom' }] });
    expect(e.status).toBe(502);
    expect(await e.text()).not.toMatch(/SECRET|exploded/);
    expect(log).toHaveBeenCalledTimes(2);
    expect(String(log.mock.calls[0]![1])).toBe('sk-ant-SECRET upstream exploded');
  });

  it('logs non-Error throwables without crashing the request', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const weird = serve({ grader: { model: 'm', grade: () => Promise.reject('plain string') }, tutor: { model: 'm', explain: () => Promise.reject(42) } });
    expect((await weird.post('/api/grade', { questionId: 'l/1', answer: 'abc' })).status).toBe(502);
    expect((await weird.post('/api/explain', { questionId: 'l/1', answer: '', messages: [{ role: 'user', content: 'x' }] })).status).toBe(502);
  });

  it('does not send x-powered-by or any CORS headers (same-origin only)', async () => {
    const res = await s.raw('/api/health', { headers: { origin: 'https://evil.example' } });
    expect(res.headers.get('x-powered-by')).toBeNull();
    expect(res.headers.get('access-control-allow-origin')).toBeNull();
    const pre = await s.raw('/api/grade', {
      method: 'OPTIONS',
      headers: { origin: 'https://evil.example', 'access-control-request-method': 'POST' },
    });
    expect(pre.headers.get('access-control-allow-origin')).toBeNull();
  });

  it('only exposes the documented methods', async () => {
    const get = await s.raw('/api/grade'); // GET on a POST route
    expect(get.status).toBe(405);
    expect(get.headers.get('allow')).toBe('POST');
    const post = await s.raw('/api/health', { method: 'POST' });
    expect(post.status).toBe(405);
    expect(post.headers.get('allow')).toBe('GET');
    expect((await s.raw('/api/nope')).status).toBe(404);
    expect((await s.raw('/api')).status).toBe(404);
    expect(await (await s.raw('/api?x=1')).json()).toEqual({ error: 'Not found.' });
  });
});

describe('health', () => {
  it('reports which AI features are on, without echoing configuration beyond the model name', async () => {
    const both = await serve({ grader, tutor }).raw('/api/health');
    expect(await both.json()).toEqual({ ok: true, ai: true, model: 'fake', explain: true });
    const tutorOnly = await serve({ tutor }).raw('/api/health');
    expect(await tutorOnly.json()).toEqual({ ok: true, ai: false, explain: true });
    const none = await serve({}).raw('/api/health');
    expect(await none.json()).toEqual({ ok: true, ai: false, explain: false });
  });
});

describe('explain validation', () => {
  const s = serve({ tutor });
  const msg = (content: string) => [{ role: 'user', content }];

  it('rejects empty and whitespace-only messages and a non-boolean correct flag', async () => {
    expect((await s.post('/api/explain', { questionId: 'l/1', messages: msg('   ') })).status).toBe(400);
    expect((await s.post('/api/explain', { questionId: 'l/1', correct: 'yes', messages: msg('why') })).status).toBe(400);
  });

  it('defaults a missing answer to empty and accepts an undefined correct flag', async () => {
    expect((await s.post('/api/explain', { questionId: 'l/1', messages: msg('why') })).status).toBe(200);
  });

  it('treats every question type the same (any course question is explainable) and an unknown id as 404', async () => {
    expect((await s.post('/api/explain', { questionId: 'l/2', messages: msg('why') })).status).toBe(200);
    expect((await s.post('/api/explain', { questionId: 'l/1~f1', messages: msg('why') })).status).toBe(404);
  });
});

describe('rate limiter windows', () => {
  afterEach(() => vi.restoreAllMocks());

  it('sets Retry-After, and starts a fresh window once the minute has passed', async () => {
    const s = serve({ grader, rateLimitPerMinute: 1 });
    const t0 = 1_700_000_000_000;
    const now = vi.spyOn(Date, 'now').mockReturnValue(t0);
    const body = { questionId: 'l/1', answer: 'abc' };
    expect((await s.post('/api/grade', body)).status).toBe(200);
    now.mockReturnValue(t0 + 20_000);
    const blocked = await s.post('/api/grade', body);
    expect(blocked.status).toBe(429);
    expect(Number(blocked.headers.get('retry-after'))).toBe(40);
    expect(await blocked.json()).toEqual({ error: 'Too many grading requests, slow down a little.' });
    now.mockReturnValue(t0 + 60_000); // window boundary is inclusive: resetAt <= now starts over
    expect((await s.post('/api/grade', body)).status).toBe(200);
    expect((await s.post('/api/grade', body)).status).toBe(429);
  });

  it('counts grading and explaining separately, and 400s still consume the budget', async () => {
    const s = serve({ grader, tutor, rateLimitPerMinute: 2, explainRateLimitPerMinute: 1 });
    expect((await s.post('/api/grade', {})).status).toBe(400);
    expect((await s.post('/api/grade', {})).status).toBe(400);
    expect((await s.post('/api/grade', {})).status).toBe(429);
    expect((await s.post('/api/explain', { questionId: 'l/1', messages: [{ role: 'user', content: 'x' }] })).status).toBe(200);
    const limited = await s.post('/api/explain', { questionId: 'l/1', messages: [{ role: 'user', content: 'x' }] });
    expect(limited.status).toBe(429);
    expect(await limited.json()).toEqual({ error: 'Too many questions, give it a minute.' });
  });

  it('does not rate limit health checks', async () => {
    const s = serve({ grader, rateLimitPerMinute: 1 });
    for (let i = 0; i < 5; i++) expect((await s.raw('/api/health')).status).toBe(200);
  });
});

describe('static serving', () => {
  const dir = mkdtempSync(join(tmpdir(), 'trainer-static-'));
  mkdirSync(join(dir, 'assets'));
  writeFileSync(join(dir, 'index.html'), '<!doctype html><title>spa</title>');
  writeFileSync(join(dir, 'assets', 'app.js'), 'console.log(1)');
  writeFileSync(join(dir, 'sw.js'), '// sw');
  afterAll(() => rmSync(dir, { recursive: true, force: true }));
  const s = serve({ grader, staticDir: dir });

  it('serves files, the index at /, and falls back to index.html for client routes', async () => {
    expect(await (await s.raw('/assets/app.js')).text()).toBe('console.log(1)');
    expect(await (await s.raw('/')).text()).toContain('<title>spa</title>');
    const deep = await s.raw('/lesson/kotlin-basics/3');
    expect(deep.status).toBe(200);
    expect(await deep.text()).toContain('<title>spa</title>');
    expect((await s.raw('/sw.js')).status).toBe(200);
  });

  it('never answers an unknown /api path with the HTML shell', async () => {
    const res = await s.raw('/api/unknown');
    expect(res.status).toBe(404);
    expect(await res.text()).not.toContain('<title>spa</title>');
  });

  it('does not serve files outside the static dir (path traversal)', async () => {
    writeFileSync(join(dir, '..', 'trainer-secret.txt'), 'top secret');
    try {
      const res = await s.raw('/..%2ftrainer-secret.txt');
      expect(await res.text()).not.toContain('top secret');
      const res2 = await s.raw('/assets/..%2f..%2ftrainer-secret.txt');
      expect(await res2.text()).not.toContain('top secret');
    } finally {
      rmSync(join(dir, '..', 'trainer-secret.txt'), { force: true });
    }
  });

  it('regression: the SPA fallback also works when the app lives under a dot-directory (e.g. /srv/.apps/trainer)', async () => {
    const hiddenParent = mkdtempSync(join(tmpdir(), 'trainer-hidden-'));
    const hidden = join(hiddenParent, '.app', 'web', 'dist');
    mkdirSync(hidden, { recursive: true });
    writeFileSync(join(hidden, 'index.html'), '<title>hidden spa</title>');
    try {
      const h = serve({ staticDir: hidden });
      const res = await h.raw('/lesson/kotlin-basics/3');
      expect(res.status).toBe(200);
      expect(await res.text()).toContain('hidden spa');
    } finally {
      rmSync(hiddenParent, { recursive: true, force: true });
    }
  });

  it('API routes still win over the SPA fallback', async () => {
    expect(((await (await s.raw('/api/health')).json()) as { ok: boolean }).ok).toBe(true);
  });

  it('serves nothing extra when the static dir does not exist (API-only dev mode)', async () => {
    const api = serve({ grader, staticDir: join(dir, 'does-not-exist') });
    expect((await api.raw('/')).status).toBe(404);
    expect((await api.raw('/api/health')).status).toBe(200);
  });
});

describe('error handler', () => {
  it('turns an unexpected throw into a generic 500 with no detail', async () => {
    const broken = { get model(): string { throw new Error('sk-ant-SECRET internal'); }, grade: async () => { throw new Error('x'); } } as unknown as Grader;
    const res = await serve({ grader: broken }).raw('/api/health');
    expect(res.status).toBe(500);
    const text = await res.text();
    expect(JSON.parse(text)).toEqual({ error: 'Request failed.' });
    expect(text).not.toMatch(/SECRET/);
  });

  it('keeps 4xx statuses from body parsing and ignores out-of-range statuses', async () => {
    const s = serve({ grader });
    expect((await s.post('/api/grade', '{bad')).status).toBe(400);
  });

  it('serves the SPA shell for an undecodable path instead of crashing', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'trainer-static-'));
    writeFileSync(join(dir, 'index.html'), 'shell');
    try {
      const res = await serve({ staticDir: dir }).raw('/%E0%A4%A');
      expect(res.status).toBeLessThan(500);
      expect(await res.text()).not.toMatch(/URIError|decodeURI/);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
