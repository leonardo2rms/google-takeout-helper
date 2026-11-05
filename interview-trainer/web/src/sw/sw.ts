/// <reference lib="webworker" />
/**
 * Service worker entry. Built separately by the `serviceWorker` plugin in vite.config.ts, which injects the list of files
 * of this build (`__PRECACHE__`) and a version derived from them (`__VERSION__`). All decisions live in lib/sw-logic.ts.
 */
import { activate, handleFetch, install, shouldHandle, type SwEnv } from '../lib/sw-logic';

declare const __PRECACHE__: string[];
declare const __VERSION__: string;

const sw = self as unknown as ServiceWorkerGlobalScope;
const env: SwEnv = {
  caches: sw.caches,
  fetch: (input, init) => sw.fetch(input, init),
  version: __VERSION__,
  precache: __PRECACHE__,
  origin: sw.location.origin,
};

sw.addEventListener('install', (event) => {
  // skipWaiting only after the whole shell is stored: a failed install leaves the working version in charge.
  event.waitUntil(install(env).then(() => sw.skipWaiting()));
});

sw.addEventListener('activate', (event) => {
  event.waitUntil(activate(env).then(() => sw.clients.claim()));
});

sw.addEventListener('fetch', (event) => {
  if (!shouldHandle(event.request, env.origin)) return;
  event.respondWith(handleFetch(env, event.request, (p) => event.waitUntil(p)));
});
