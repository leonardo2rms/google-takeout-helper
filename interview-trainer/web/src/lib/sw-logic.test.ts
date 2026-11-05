/**
 * The service worker's decisions (QA finding S1-2): never cache an HTML page served for a script URL, install
 * all-or-nothing under a version-keyed cache name, drop old versions, network-first pages, cache-first assets.
 */
import { describe, expect, it, vi } from 'vitest';
import { CACHE_PREFIX, activate, cacheName, handleFetch, install, isNavigation, isStorable, shouldHandle, type SwEnv } from './sw-logic';

const ORIGIN = 'https://app.test';

class FakeCache {
  entries = new Map<string, Response>();
  failPut = false;
  key = (r: Request | string) => new URL(typeof r === 'string' ? r : r.url, ORIGIN).pathname;
  async match(r: Request | string) {
    return this.entries.get(this.key(r))?.clone();
  }
  async put(r: Request | string, res: Response) {
    if (this.failPut) throw new DOMException('full', 'QuotaExceededError');
    this.entries.set(this.key(r), res.clone());
  }
}
class FakeCaches {
  stores = new Map<string, FakeCache>();
  async open(name: string) {
    if (!this.stores.has(name)) this.stores.set(name, new FakeCache());
    return this.stores.get(name)!;
  }
  async keys() {
    return [...this.stores.keys()];
  }
  async delete(name: string) {
    return this.stores.delete(name);
  }
}

const js = (body = 'console.log(1)') => new Response(body, { status: 200, headers: { 'content-type': 'text/javascript' } });
const html = (body = '<title>app</title>', status = 200) => new Response(body, { status, headers: { 'content-type': 'text/html; charset=utf-8' } });

interface Rig {
  env: SwEnv;
  caches: FakeCaches;
  net: Map<string, () => Response | Promise<Response>>;
  offline: boolean;
  calls: string[];
}
function rig(version = 'v1', precache = ['/', '/assets/app-1.js']): Rig {
  const caches = new FakeCaches();
  const r: Rig = {
    caches,
    net: new Map<string, () => Response | Promise<Response>>([['/', () => html()], ['/assets/app-1.js', () => js()]]),
    offline: false,
    calls: [],
    env: undefined as never,
  };
  r.env = {
    caches: caches as unknown as CacheStorage,
    version,
    precache,
    origin: ORIGIN,
    fetch: async (input) => {
      const url = new URL(typeof input === 'string' ? input : input.url, ORIGIN);
      r.calls.push(url.pathname);
      if (r.offline) throw new TypeError('Failed to fetch');
      const make = r.net.get(url.pathname);
      return make ? make() : html('<title>app</title>'); // the bug under test: an SPA server answering anything with the shell
    },
  };
  return r;
}
const nav = (path = '/lesson/x') => new Request(ORIGIN + path, { headers: { accept: 'text/html' } });
const asset = (path: string) => new Request(ORIGIN + path);
const stored = (r: Rig, v = 'v1') => r.caches.stores.get(cacheName(v))!;

describe('what may be cached', () => {
  const u = (p: string) => new URL(p, ORIGIN);
  it('accepts a 200 of the type the URL promises', () => {
    expect(isStorable(u('/assets/a.js'), js())).toBe(true);
    expect(isStorable(u('/assets/a.css'), new Response('', { headers: { 'content-type': 'text/css' } }))).toBe(true);
    expect(isStorable(u('/'), html())).toBe(true);
    expect(isStorable(u('/icon.svg'), new Response('', { headers: { 'content-type': 'image/svg+xml' } }))).toBe(true);
    expect(isStorable(u('/f.woff2'), new Response('', { headers: { 'content-type': 'font/woff2' } }))).toBe(true);
  });
  it('refuses HTML served for a script or stylesheet (the SPA fallback), and a script served for a page', () => {
    expect(isStorable(u('/assets/gone.js'), html())).toBe(false);
    expect(isStorable(u('/assets/gone.css'), html())).toBe(false);
    expect(isStorable(u('/lesson/x'), js())).toBe(false);
  });
  it('refuses errors, partial content, opaque responses, missing content types and unknown extensions', () => {
    expect(isStorable(u('/assets/a.js'), new Response('x', { status: 404, headers: { 'content-type': 'text/javascript' } }))).toBe(false);
    expect(isStorable(u('/assets/a.js'), new Response('x', { status: 206, headers: { 'content-type': 'text/javascript' } }))).toBe(false);
    expect(isStorable(u('/assets/a.js'), new Response('x'))).toBe(false);
    expect(isStorable(u('/a.exe'), new Response('x', { headers: { 'content-type': 'application/octet-stream' } }))).toBe(false);
    const opaque = { status: 200, type: 'opaque', headers: new Headers() } as Response;
    expect(isStorable(u('/assets/a.js'), opaque)).toBe(false);
  });
});

describe('request filtering', () => {
  it('handles same-origin GETs outside /api/ only', () => {
    expect(shouldHandle(asset('/assets/a.js'), ORIGIN)).toBe(true);
    expect(shouldHandle(asset('/api/health'), ORIGIN)).toBe(false);
    expect(shouldHandle(new Request(ORIGIN + '/api/grade', { method: 'POST', body: '{}' }), ORIGIN)).toBe(false);
    expect(shouldHandle(new Request('https://other.test/a.js'), ORIGIN)).toBe(false);
  });
  it('recognises navigations by mode or by Accept', () => {
    expect(isNavigation(nav())).toBe(true);
    expect(isNavigation(asset('/assets/a.js'))).toBe(false);
    expect(isNavigation({ mode: 'navigate', headers: new Headers() } as Request)).toBe(true);
  });
});

describe('install is all-or-nothing and version-keyed', () => {
  it('stores every file under the versioned cache name', async () => {
    const r = rig('v7');
    await install(r.env);
    expect([...stored(r, 'v7').entries.keys()].sort()).toEqual(['/', '/assets/app-1.js']);
    expect(cacheName('v7')).toBe(`${CACHE_PREFIX}v7`);
  });

  it('fails, and leaves no half-filled cache, when a file comes back as the HTML shell', async () => {
    const r = rig('v2', ['/', '/assets/app-2.js']); // /assets/app-2.js is not on the "server": the SPA fallback answers with HTML
    await expect(install(r.env)).rejects.toThrow(/unusable response for \/assets\/app-2\.js/);
    expect(r.caches.stores.has(cacheName('v2'))).toBe(false);
  });

  it('fails when the network is down', async () => {
    const r = rig('v3');
    r.offline = true;
    await expect(install(r.env)).rejects.toThrow();
    expect(r.caches.stores.size).toBe(0);
  });

  it('activation deletes the caches of other versions and keeps unrelated ones', async () => {
    const r = rig('v2');
    await r.caches.open(cacheName('v1'));
    await r.caches.open(cacheName('v0'));
    await r.caches.open('someone-else');
    await install(r.env);
    await activate(r.env);
    expect(await r.caches.keys()).toEqual(['someone-else', cacheName('v2')].sort((a, b) => (a === cacheName('v2') ? 1 : b === cacheName('v2') ? -1 : 0)));
  });
});

describe('fetching', () => {
  it('serves a precached asset from the cache without touching the network', async () => {
    const r = rig();
    await install(r.env);
    r.calls.length = 0;
    const res = await handleFetch(r.env, asset('/assets/app-1.js'));
    expect(await res.text()).toBe('console.log(1)');
    expect(r.calls).toEqual([]);
  });

  it('REGRESSION: a script URL that the server answers with the HTML shell is returned but never cached', async () => {
    const r = rig();
    await install(r.env);
    const pending: Promise<unknown>[] = [];
    const res = await handleFetch(r.env, asset('/assets/index-OLDHASH.js'), (p) => pending.push(p));
    await Promise.all(pending);
    expect(res.headers.get('content-type')).toContain('text/html');
    expect(stored(r).entries.has('/assets/index-OLDHASH.js')).toBe(false);
  });

  it('caches a good asset fetched later, and serves it from the cache the second time', async () => {
    const r = rig();
    r.net.set('/assets/late.js', () => js('late'));
    const pending: Promise<unknown>[] = [];
    await handleFetch(r.env, asset('/assets/late.js'), (p) => pending.push(p));
    await Promise.all(pending);
    r.calls.length = 0;
    expect(await (await handleFetch(r.env, asset('/assets/late.js'))).text()).toBe('late');
    expect(r.calls).toEqual([]);
  });

  it('a failing cache write does not break the response', async () => {
    const r = rig();
    const cache = await r.caches.open(cacheName('v1'));
    cache.failPut = true;
    r.net.set('/assets/late.js', () => js('late'));
    const pending: Promise<unknown>[] = [];
    const res = await handleFetch(r.env, asset('/assets/late.js'), (p) => pending.push(p));
    await Promise.all(pending);
    expect(await res.text()).toBe('late');
  });

  it('an offline asset request that is not cached fails cleanly', async () => {
    const r = rig();
    r.offline = true;
    const res = await handleFetch(r.env, asset('/assets/never.js'));
    expect(res.type).toBe('error');
  });

  it('navigations are network-first: a new deploy shows up on the next load, and the shell copy is refreshed', async () => {
    const r = rig();
    await install(r.env);
    r.net.set('/lesson/x', () => html('<title>NEW shell</title>'));
    const pending: Promise<unknown>[] = [];
    const res = await handleFetch(r.env, nav('/lesson/x'), (p) => pending.push(p));
    await Promise.all(pending);
    expect(await res.text()).toBe('<title>NEW shell</title>');
    expect(await (await stored(r).match('/'))!.text()).toBe('<title>NEW shell</title>');
  });

  it('offline navigations get the cached shell', async () => {
    const r = rig();
    await install(r.env);
    r.offline = true;
    const res = await handleFetch(r.env, nav('/progress'));
    expect(await res.text()).toBe('<title>app</title>');
  });

  it('a server error on a navigation falls back to the cached shell; with no cache the error is passed through', async () => {
    const r = rig();
    await install(r.env);
    r.net.set('/lesson/x', () => html('oops', 503));
    expect(await (await handleFetch(r.env, nav('/lesson/x'))).text()).toBe('<title>app</title>');

    const empty = rig('fresh');
    empty.net.set('/lesson/x', () => html('oops', 503));
    expect((await handleFetch(empty.env, nav('/lesson/x'))).status).toBe(503);
  });

  it('offline with nothing cached is a network error, and a 404 page is returned without being stored as the shell', async () => {
    const r = rig();
    r.offline = true;
    expect((await handleFetch(r.env, nav())).type).toBe('error');

    const r2 = rig();
    r2.net.set('/lesson/y', () => html('not found', 404));
    const pending: Promise<unknown>[] = [];
    expect((await handleFetch(r2.env, nav('/lesson/y'), (p) => pending.push(p))).status).toBe(404);
    await Promise.all(pending);
    expect(stored(r2).entries.has('/')).toBe(false);
  });

  it('default waitUntil is optional', async () => {
    const r = rig();
    const spy = vi.fn();
    await handleFetch(r.env, nav('/'), spy);
    expect(spy).toHaveBeenCalled();
    await expect(handleFetch(r.env, asset('/assets/app-1.js'))).resolves.toBeInstanceOf(Response);
  });
});
