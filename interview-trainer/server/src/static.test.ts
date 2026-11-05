/**
 * Static serving rules that keep a deploy safe (QA findings S1-2, S2-1, S2-2, S2-3): a missing asset is a real 404 and
 * never the HTML shell, the service worker and shell are always revalidated while hashed assets are immutable,
 * security headers are present, and the rate limiter does not grow without bound or share a bucket behind a proxy.
 */
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, afterEach, describe, expect, it, vi } from 'vitest';
import { Course, type GradeResponse } from '@trainer/shared';
import { CSP, createApp } from './app';
import type { Grader } from './grader';

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
        { type: 'single', prompt: 'p2', explanation: 'e', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a' },
      ],
    }],
  }],
});
const grader: Grader = { model: 'fake', grade: async (): Promise<GradeResponse> => ({ score: 1, covered: [true, true], feedback: 'ok', graded_by: 'ai' }) };

const closers: Array<() => void> = [];
function serve(opts: Partial<Parameters<typeof createApp>[0]> = {}) {
  const server = createApp({ course, ...opts }).listen(0);
  closers.push(() => server.close());
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  return (path: string, init?: RequestInit) => fetch(`${base}${path}`, init);
}
afterAll(() => closers.forEach((c) => c()));
afterEach(() => vi.restoreAllMocks());

const dir = mkdtempSync(join(tmpdir(), 'trainer-deploy-'));
mkdirSync(join(dir, 'assets'));
writeFileSync(join(dir, 'index.html'), '<!doctype html><title>spa</title>');
writeFileSync(join(dir, 'assets', 'app-abc123.js'), 'console.log(1)');
writeFileSync(join(dir, 'assets', 'big-def456.js'), `const course = ${JSON.stringify(Array.from({ length: 400 }, (_, i) => ({ id: `q/${i}`, prompt: 'What is the time complexity?' })))};`);
writeFileSync(join(dir, 'sw.js'), '// sw');
writeFileSync(join(dir, 'manifest.webmanifest'), '{}');
afterAll(() => rmSync(dir, { recursive: true, force: true }));

describe('a missing static file is a 404, not the app shell (S1-2)', () => {
  const get = serve({ staticDir: dir });

  it.each(['/assets/app-OLDHASH.js', '/assets/missing.css', '/assets/', '/assets/deep/er/x', '/favicon.ico', '/.git/config', '/app.js.map', '/robots.txt'])(
    '%s',
    async (path) => {
      const res = await get(path);
      expect(res.status).toBe(404);
      expect(res.headers.get('content-type')).not.toContain('text/html');
      expect(await res.text()).not.toContain('<title>spa</title>');
    },
  );

  it('HEAD for a missing asset is a 404 too', async () => {
    expect((await get('/assets/gone.js', { method: 'HEAD' })).status).toBe(404);
  });

  it('page navigations still get the shell, with or without a query, and extension-less client routes too', async () => {
    for (const path of ['/', '/lesson/kotlin-basics/3', '/?utm=x', '/progress', '/some.thing/in/the/middle']) {
      const res = await get(path);
      expect(res.status).toBe(200);
      expect(await res.text()).toContain('<title>spa</title>');
    }
  });

  it('existing files are served with their own content type', async () => {
    const js = await get('/assets/app-abc123.js');
    expect(js.status).toBe(200);
    expect(js.headers.get('content-type')).toMatch(/javascript/);
  });
});

describe('caching headers', () => {
  const get = serve({ staticDir: dir });

  it('hashed assets are immutable; the shell, service worker and manifest are always revalidated', async () => {
    expect((await get('/assets/app-abc123.js')).headers.get('cache-control')).toBe('public, max-age=31536000, immutable');
    for (const path of ['/sw.js', '/manifest.webmanifest', '/', '/lesson/x']) expect((await get(path)).headers.get('cache-control')).toBe('no-cache');
  });
});

describe('compression (S2-2)', () => {
  const get = serve({ staticDir: dir });

  it('compresses large text responses for clients that accept gzip, and says so', async () => {
    const res = await get('/assets/big-def456.js', { headers: { 'accept-encoding': 'gzip' } });
    expect(res.headers.get('content-encoding')).toBe('gzip');
    expect(res.headers.get('vary')).toMatch(/Accept-Encoding/i);
    expect(await res.text()).toContain('What is the time complexity?'); // transparently decoded by fetch
  });

  it('sends identity to clients that do not accept it, and leaves tiny files alone', async () => {
    expect((await get('/assets/big-def456.js', { headers: { 'accept-encoding': 'identity' } })).headers.get('content-encoding')).toBeNull();
    expect((await get('/assets/app-abc123.js', { headers: { 'accept-encoding': 'gzip' } })).headers.get('content-encoding')).toBeNull();
  });
});

describe('security headers (S2-1)', () => {
  const get = serve({ staticDir: dir, grader });

  it('are on pages, assets, API answers and errors', async () => {
    for (const path of ['/', '/assets/app-abc123.js', '/api/health', '/assets/nope.js', '/api/nope']) {
      const res = await get(path);
      expect(res.headers.get('x-content-type-options'), path).toBe('nosniff');
      expect(res.headers.get('referrer-policy'), path).toBe('no-referrer');
      expect(res.headers.get('x-frame-options'), path).toBe('DENY');
      expect(res.headers.get('content-security-policy'), path).toBe(CSP);
    }
  });

  it('the policy allows what the app needs (blob workers for the code runner, inline styles) and nothing remote', () => {
    expect(CSP).toContain("worker-src 'self' blob:");
    expect(CSP).toContain("style-src 'self' 'unsafe-inline'");
    expect(CSP).toContain("frame-ancestors 'none'");
    expect(CSP).toContain("default-src 'self'");
    expect(CSP).not.toMatch(/https?:/);
  });
});

describe('rate limiter behind a proxy and under churn (S2-3)', () => {
  const body = JSON.stringify({ questionId: 'l/1', answer: 'abc' });
  const post = (get: ReturnType<typeof serve>, headers: Record<string, string> = {}) =>
    get('/api/grade', { method: 'POST', headers: { 'content-type': 'application/json', ...headers }, body });

  it('ignores X-Forwarded-For by default, honours it when trustProxy is set', async () => {
    const direct = serve({ grader, rateLimitPerMinute: 1 });
    expect((await post(direct, { 'x-forwarded-for': '1.1.1.1' })).status).toBe(200);
    expect((await post(direct, { 'x-forwarded-for': '2.2.2.2' })).status).toBe(429);

    const proxied = serve({ grader, rateLimitPerMinute: 1, trustProxy: 1 });
    expect((await post(proxied, { 'x-forwarded-for': '1.1.1.1' })).status).toBe(200);
    expect((await post(proxied, { 'x-forwarded-for': '2.2.2.2' })).status).toBe(200); // a different client has its own budget
    expect((await post(proxied, { 'x-forwarded-for': '1.1.1.1' })).status).toBe(429);
  });

  it('drops expired windows when the table grows, so the same client starts fresh and memory stays bounded', async () => {
    const proxied = serve({ grader, rateLimitPerMinute: 1, trustProxy: 1 });
    const t0 = 1_700_000_000_000;
    const now = vi.spyOn(Date, 'now').mockReturnValue(t0);
    for (let i = 0; i < 1005; i++) await post(proxied, { 'x-forwarded-for': `10.0.${i >> 8}.${i & 255}` });
    now.mockReturnValue(t0 + 61_000);
    // the sweep runs on this request; every earlier client has been forgotten and is allowed again
    expect((await post(proxied, { 'x-forwarded-for': '10.0.0.1' })).status).toBe(200);
    expect((await post(proxied, { 'x-forwarded-for': '10.0.0.1' })).status).toBe(429);
  });
});
