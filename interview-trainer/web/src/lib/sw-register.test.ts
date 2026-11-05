/** Page side of the service worker: registration, periodic update checks, and the "update ready" signal (QA S1-2). */
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('react', () => ({
  useSyncExternalStore: (subscribe: (l: () => void) => () => void, get: () => unknown) => {
    (globalThis as { __subscribe?: unknown }).__subscribe = subscribe;
    return get();
  },
}));

import { UPDATE_CHECK_MS, isUpdateReady, registerServiceWorker, resetUpdateState, useUpdateReady } from './sw-register';

class FakeContainer extends EventTarget {
  controller: object | null;
  registerArgs: unknown[] = [];
  update = vi.fn().mockResolvedValue(undefined);
  failRegister = false;
  constructor(controller: object | null) {
    super();
    this.controller = controller;
  }
  async register(...args: unknown[]) {
    this.registerArgs = args;
    if (this.failRegister) throw new Error('blocked');
    return { update: this.update };
  }
}
const asContainer = (c: FakeContainer) => c as unknown as ServiceWorkerContainer;

function fakeDoc() {
  const handlers: Array<() => void> = [];
  return {
    visibilityState: 'visible',
    addEventListener: (_t: 'visibilitychange', fn: () => void) => void handlers.push(fn),
    fire: () => handlers.forEach((h) => h()),
  };
}

beforeEach(() => resetUpdateState());

describe('update notice', () => {
  it('registers /sw.js without trusting the HTTP cache for the worker file', async () => {
    const c = new FakeContainer(null);
    await registerServiceWorker(asContainer(c), fakeDoc(), () => 0);
    expect(c.registerArgs).toEqual(['/sw.js', { updateViaCache: 'none' }]);
  });

  it('the first visit taking control is not an update; a later takeover is', async () => {
    const c = new FakeContainer(null);
    await registerServiceWorker(asContainer(c), fakeDoc(), () => 0);
    c.dispatchEvent(new Event('controllerchange')); // first install claims the page
    expect(isUpdateReady()).toBe(false);
    c.dispatchEvent(new Event('controllerchange')); // a new deploy takes over
    expect(isUpdateReady()).toBe(true);
    expect(useUpdateReady()).toBe(true);
  });

  it('a page that already had a controller reports the first takeover as an update', async () => {
    const c = new FakeContainer({});
    await registerServiceWorker(asContainer(c), fakeDoc(), () => 0);
    c.dispatchEvent(new Event('controllerchange'));
    expect(isUpdateReady()).toBe(true);
    c.dispatchEvent(new Event('controllerchange')); // idempotent
    expect(isUpdateReady()).toBe(true);
  });

  it('notifies subscribers exactly once, and not after they unsubscribe', async () => {
    const c = new FakeContainer({});
    useUpdateReady();
    const subscribe = (globalThis as unknown as { __subscribe: (l: () => void) => () => void }).__subscribe;
    const listener = vi.fn();
    const gone = vi.fn();
    subscribe(gone)();
    subscribe(listener);
    await registerServiceWorker(asContainer(c), fakeDoc(), () => 0);
    c.dispatchEvent(new Event('controllerchange'));
    c.dispatchEvent(new Event('controllerchange'));
    expect(listener).toHaveBeenCalledTimes(1);
    expect(gone).not.toHaveBeenCalled();
  });
})

describe('looking for new deploys', () => {
  it('checks when the tab becomes visible and on a timer, but not while hidden', async () => {
    const c = new FakeContainer({});
    const doc = fakeDoc();
    let tick: () => void = () => {};
    let every = 0;
    await registerServiceWorker(asContainer(c), doc, (fn, ms) => {
      tick = fn;
      every = ms;
    });
    expect(every).toBe(UPDATE_CHECK_MS);
    doc.fire();
    expect(c.update).toHaveBeenCalledTimes(1);
    doc.visibilityState = 'hidden';
    doc.fire();
    expect(c.update).toHaveBeenCalledTimes(1);
    tick();
    expect(c.update).toHaveBeenCalledTimes(2);
  });

  it('uses setInterval for the periodic check unless told otherwise', async () => {
    vi.useFakeTimers();
    const c = new FakeContainer({});
    await registerServiceWorker(asContainer(c), fakeDoc());
    await vi.advanceTimersByTimeAsync(UPDATE_CHECK_MS * 2);
    expect(c.update).toHaveBeenCalledTimes(2);
    vi.useRealTimers();
  });

  it('swallows a failed update check and a failed registration', async () => {
    const c = new FakeContainer({});
    c.update.mockRejectedValue(new Error('offline'));
    const doc = fakeDoc();
    await registerServiceWorker(asContainer(c), doc, () => 0);
    expect(() => doc.fire()).not.toThrow();
    const bad = new FakeContainer(null);
    bad.failRegister = true;
    await expect(registerServiceWorker(asContainer(bad), fakeDoc(), () => 0)).resolves.toBeUndefined();
  });
});
