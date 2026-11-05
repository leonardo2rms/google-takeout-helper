import { useSyncExternalStore } from 'react';

/**
 * Page side of the service worker: registers it, keeps checking for a new deploy, and tells the UI once a new version
 * has taken over ("Update ready, reload"). The worker activates immediately (skipWaiting + clients.claim), so the page
 * that is open keeps running its old code until the learner reloads; nothing reloads behind their back mid-lesson.
 */

let ready = false;
const listeners = new Set<() => void>();
const subscribe = (l: () => void) => {
  listeners.add(l);
  return () => void listeners.delete(l);
};
const markReady = () => {
  if (ready) return;
  ready = true;
  listeners.forEach((l) => l());
};

export const isUpdateReady = (): boolean => ready;
/** True once a newer version of the app is installed and a reload will show it. */
export const useUpdateReady = (): boolean => useSyncExternalStore(subscribe, isUpdateReady, () => false);
/** Test hook: forget any pending update. */
export const resetUpdateState = (): void => {
  ready = false;
};

export const UPDATE_CHECK_MS = 60 * 60 * 1000;

interface Doc {
  visibilityState?: string;
  addEventListener(type: 'visibilitychange', fn: () => void): void;
}

/**
 * Registers `/sw.js`. `updateViaCache: 'none'` makes the browser fetch the worker file itself fresh, so a deploy is
 * noticed on the next check instead of after the HTTP cache expires.
 */
export async function registerServiceWorker(container: ServiceWorkerContainer, doc: Doc, setTimer: (fn: () => void, ms: number) => unknown = setInterval): Promise<void> {
  // A hard reload (Ctrl+Shift+R) loads the page without a controller although a worker is installed. Looking at the
  // registration tells that apart from a first visit, so the first takeover after a hard reload still raises the notice.
  const installed = await container.getRegistration?.().then((r) => Boolean(r?.active), () => false);
  let hadController = container.controller !== null || installed === true;
  container.addEventListener('controllerchange', () => {
    if (hadController) markReady();
    else hadController = true; // the first visit: the worker just took control, there is nothing newer to reload for
  });
  return container
    .register('/sw.js', { updateViaCache: 'none' })
    .then((reg) => {
      const check = () => void reg.update().catch(() => {});
      doc.addEventListener('visibilitychange', () => {
        if (doc.visibilityState === 'visible') check();
      });
      setTimer(check, UPDATE_CHECK_MS);
    })
    .catch(() => {});
}
