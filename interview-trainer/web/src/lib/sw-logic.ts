/**
 * The decisions of the service worker, as plain functions over injected `caches` and `fetch`, so they can be unit tested
 * (the worker file in src/sw/sw.ts only wires them to events).
 *
 * Rules that exist because of real failures (QA finding S1-2):
 *  - Only a successful (200), same-origin response of the content type the URL promises is ever cached. A server that
 *    answers a missing `/assets/x.js` with index.html must not poison the cache with HTML-as-JavaScript.
 *  - The cache name carries the build version and the whole shell is pre-cached at install, so an update is
 *    all-or-nothing: if one file cannot be fetched the new worker does not activate and the old version keeps working.
 *  - Navigations are network-first (a deploy shows up on the next load); hashed assets are cache-first (they never change).
 *  - Old versions' caches are deleted on activate, so storage does not grow with every deploy.
 *  - A 200 text/html is not enough to be the offline shell: a captive portal or proxy error page answers that way too. Only a
 *    document that carries the app's own markers (its root element and a script from /assets/) replaces the stored shell, so
 *    a good shell is never swapped for a worse one.
 */

export const CACHE_PREFIX = 'interview-trainer-';
export const cacheName = (version: string): string => `${CACHE_PREFIX}${version}`;
export const SHELL_URL = '/';

export interface SwEnv {
  caches: CacheStorage;
  fetch: (input: Request | string, init?: RequestInit) => Promise<Response>;
  /** Build identifier; changes whenever any shipped file changes. */
  version: string;
  /** Same-origin paths that make up the app, e.g. ['/', '/assets/index-abc.js', ...]. */
  precache: readonly string[];
  origin: string;
}

const TYPES: Record<string, RegExp> = {
  js: /javascript|ecmascript/i,
  mjs: /javascript|ecmascript/i,
  css: /^text\/css/i,
  svg: /^image\/svg\+xml/i,
  webmanifest: /json/i,
  json: /json/i,
  png: /^image\//i,
  jpg: /^image\//i,
  jpeg: /^image\//i,
  ico: /^image\//i,
  webp: /^image\//i,
  woff2: /font|octet-stream/i,
  woff: /font|octet-stream/i,
  ttf: /font|octet-stream/i,
};

const extensionOf = (pathname: string): string => /\.([a-z0-9]+)$/i.exec(pathname)?.[1]?.toLowerCase() ?? '';

/** True for an HTML document request (the app's own navigations). */
export const isNavigation = (request: Request): boolean => request.mode === 'navigate' || (request.headers.get('accept') ?? '').includes('text/html');

/** Whether the worker should handle this request at all: same-origin GETs outside /api/. */
export function shouldHandle(request: Request, origin: string): boolean {
  if (request.method !== 'GET') return false;
  const url = new URL(request.url);
  return url.origin === origin && !url.pathname.startsWith('/api/');
}

/** Whether `response` is something that may be stored for `url`: a complete 200 of the type the URL promises. */
export function isStorable(url: URL, response: Response): boolean {
  if (response.status !== 200 || response.type === 'opaque' || response.type === 'error') return false;
  const type = response.headers.get('content-type') ?? '';
  const ext = extensionOf(url.pathname);
  if (ext === '') return /^text\/html/i.test(type); // the shell (or any extension-less route) is HTML
  const expected = TYPES[ext];
  return expected !== undefined && expected.test(type); // unknown extensions are never cached
}

const ROOT_MARKER = /<div\s[^>]*\bid\s*=\s*["']?root["']?[\s>/]/i;
const SCRIPT_MARKER = /<script\s[^>]*\bsrc\s*=\s*["']\/assets\//i;

/** Whether `response` really is the app's HTML shell: the right type and status, and the app's root element plus its bundle. */
export async function isAppShell(url: URL, response: Response): Promise<boolean> {
  if (!isStorable(url, response)) return false;
  if (response.redirected && new URL(response.url, url).origin !== url.origin) return false;
  try {
    const body = await response.clone().text();
    return ROOT_MARKER.test(body) && SCRIPT_MARKER.test(body);
  } catch {
    return false;
  }
}

async function openCurrent(env: SwEnv): Promise<Cache> {
  return env.caches.open(cacheName(env.version));
}

/** Fetches and stores the whole shell. Throws (so installation fails and the old worker stays) if any file is bad. */
export async function install(env: SwEnv): Promise<void> {
  const name = cacheName(env.version);
  const cache = await env.caches.open(name);
  try {
    await Promise.all(
      env.precache.map(async (path) => {
        const url = new URL(path, env.origin);
        const res = await env.fetch(new Request(url, { cache: 'reload' }));
        const usable = path === SHELL_URL ? await isAppShell(url, res) : isStorable(url, res);
        if (!usable) throw new Error(`precache: unusable response for ${path} (${res.status} ${res.headers.get('content-type')})`);
        await cache.put(path, res);
      }),
    );
  } catch (err) {
    await env.caches.delete(name); // never leave a half-filled cache that a later install could mistake for complete
    throw err;
  }
}

/** Deletes the caches of every other version. */
export async function activate(env: SwEnv): Promise<void> {
  const keep = cacheName(env.version);
  const keys = await env.caches.keys();
  await Promise.all(keys.filter((k) => k.startsWith(CACHE_PREFIX) && k !== keep).map((k) => env.caches.delete(k)));
}

async function store(env: SwEnv, key: string, url: URL, response: Response): Promise<void> {
  if (key === SHELL_URL ? !(await isAppShell(url, response)) : !isStorable(url, response)) return;
  try {
    await (await openCurrent(env)).put(key, response);
  } catch {
    /* quota: serving the response matters more than caching it */
  }
}

/** Network-first for pages with the cached shell as the offline fallback; cache-first for assets. */
export async function handleFetch(env: SwEnv, request: Request, waitUntil: (p: Promise<unknown>) => void = () => {}): Promise<Response> {
  const url = new URL(request.url);
  const cache = await openCurrent(env);

  if (isNavigation(request)) {
    let failed: Response | undefined;
    try {
      const res = await env.fetch(request);
      if (res.status < 500) {
        // Any app route is the same shell document.
        waitUntil(store(env, SHELL_URL, new URL(SHELL_URL, env.origin), res.clone()));
        return res;
      }
      failed = res;
    } catch {
      /* offline: fall through to the cached shell */
    }
    return (await cache.match(SHELL_URL)) ?? failed ?? Response.error();
  }

  const key = url.pathname;
  const cached = await cache.match(key);
  if (cached) return cached;
  try {
    const res = await env.fetch(request);
    waitUntil(store(env, key, url, res.clone()));
    return res;
  } catch {
    return Response.error();
  }
}
