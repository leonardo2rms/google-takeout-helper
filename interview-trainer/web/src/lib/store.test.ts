/**
 * The progress store with a fake `window.localStorage`: quota errors, a storage area that throws, writes from another
 * tab (both noticed on the next update and via the `storage` event), corrupt data and badge toasts. React is replaced by
 * a two-hook stub so `useProgress` can be exercised without a DOM.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { KEY_V1, KEY_V2, backupKey, dayKey, emptyProgress, type Progress } from '@trainer/shared';

vi.mock('react', () => ({
  useCallback: <T,>(fn: T) => fn,
  useSyncExternalStore: (subscribe: (l: () => void) => () => void, get: () => unknown) => {
    (globalThis as { __subscribe?: unknown }).__subscribe = subscribe;
    return get();
  },
}));

class FakeStorage {
  data = new Map<string, string>();
  failWrites: 'quota' | null = null;
  failReads = false;
  writes: string[] = [];
  getItem(k: string) {
    if (this.failReads) throw new Error('SecurityError');
    return this.data.get(k) ?? null;
  }
  setItem(k: string, v: string) {
    if (this.failWrites) throw new DOMException('quota', 'QuotaExceededError');
    this.writes.push(k);
    this.data.set(k, v);
  }
}

type Listener = (e: { key: string | null }) => void;
let storage: FakeStorage;
let windowListeners: Listener[];
let storageGetterThrows = false;

function installWindow() {
  windowListeners = [];
  vi.stubGlobal('window', {
    get localStorage() {
      if (storageGetterThrows) throw new DOMException('denied', 'SecurityError');
      return storage;
    },
    addEventListener: (type: string, fn: Listener) => {
      if (type === 'storage') windowListeners.push(fn);
    },
    matchMedia: () => ({ matches: false }),
  });
  vi.stubGlobal('localStorage', storage);
}

async function load() {
  vi.resetModules();
  installWindow();
  const store = await import('./store');
  const toasts = await import('./toasts');
  return { store, toasts };
}
const stored = (): Progress => JSON.parse(storage.data.get(KEY_V2)!);
const withXp = (xp: number) => (p: Progress): Progress => ({ ...p, xp });

beforeEach(() => {
  storage = new FakeStorage();
  storageGetterThrows = false;
  vi.useFakeTimers();
  vi.setSystemTime(new Date(2026, 5, 15, 12, 0, 0));
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('reading and writing', () => {
  it('starts empty without writing anything, then persists each update', async () => {
    const { store } = await load();
    expect(store.getProgress()).toEqual(emptyProgress());
    expect(storage.writes).toEqual([]);
    store.updateProgress(withXp(10));
    expect(stored().xp).toBe(10);
    expect(store.getProgress().xp).toBe(10);
  });

  it('does not write when the updater returns the same object', async () => {
    const { store } = await load();
    store.updateProgress((p) => p);
    expect(storage.writes).toEqual([]);
  });

  it('migrates a v1 profile on first read and leaves the v1 key alone', async () => {
    const v1 = JSON.stringify({ version: 1, xp: 120, streak: 3, lastDay: '2026-06-14', lessons: {}, missed: [] });
    storage.data.set(KEY_V1, v1);
    const { store } = await load();
    expect(store.getProgress()).toMatchObject({ version: 2, xp: 120 });
    expect(storage.data.get(KEY_V1)).toBe(v1);
    expect(stored().version).toBe(2);
  });

  it('exports text that round-trips through replaceProgress', async () => {
    const { store } = await load();
    store.updateProgress(withXp(77));
    const text = store.exportText();
    expect(JSON.parse(text)).toMatchObject({ app: expect.any(String), progress: { xp: 77 } });
    store.replaceProgress({ ...emptyProgress(), xp: 5 });
    expect(stored().xp).toBe(5);
    store.mergeIntoProgress({ ...emptyProgress(), xp: 500 });
    expect(stored().xp).toBe(500);
  });
});

describe('failing storage', () => {
  it('keeps working in memory when the quota is exceeded', async () => {
    const { store } = await load();
    store.updateProgress(withXp(1));
    storage.failWrites = 'quota';
    expect(() => store.updateProgress(withXp(2))).not.toThrow();
    expect(store.getProgress().xp).toBe(2);
    expect(stored().xp).toBe(1); // what is on disk is the last good write
    storage.failWrites = null;
    store.updateProgress(withXp(3));
    expect(stored().xp).toBe(3);
  });

  it('falls back to a per-tab memory store when localStorage itself throws', async () => {
    storageGetterThrows = true;
    const { store } = await load();
    store.updateProgress(withXp(9));
    expect(store.getProgress().xp).toBe(9);
    expect(storage.writes).toEqual([]);
  });

  it('survives reads that throw (treats them as nothing stored)', async () => {
    storage.failReads = true;
    const { store } = await load();
    expect(store.getProgress().xp).toBe(0);
    expect(() => store.updateProgress(withXp(4))).not.toThrow();
    expect(store.getProgress().xp).toBe(4);
  });

  it('keeps its in-memory state when a later read throws (storage revoked mid-session)', async () => {
    const { store } = await load();
    store.updateProgress(withXp(3));
    storage.failReads = true;
    expect(() => store.updateProgress(withXp(4))).not.toThrow();
    expect(store.getProgress().xp).toBe(4);
  });

  it('keeps unreadable data safe: starts empty, and backs it up before the first overwrite', async () => {
    storage.data.set(KEY_V2, '{this is not json');
    const { store } = await load();
    expect(store.getProgress().xp).toBe(0);
    expect(storage.data.get(KEY_V2)).toBe('{this is not json');
    store.updateProgress(withXp(1));
    expect(storage.data.get(backupKey(dayKey(new Date())))).toBe('{this is not json');
    expect(stored().xp).toBe(1);
  });

  it('does not destroy data written by a newer app version', async () => {
    const future = JSON.stringify({ version: 99, xp: 1000 });
    storage.data.set(KEY_V2, future);
    const { store } = await load();
    expect(store.getProgress().xp).toBe(0);
    store.updateProgress(withXp(2));
    expect(storage.data.get(backupKey(dayKey(new Date())))).toBe(future);
  });
});

describe('several tabs', () => {
  it('applies updates on top of another tab\'s newer write instead of clobbering it', async () => {
    const { store } = await load();
    store.updateProgress(withXp(10));
    // "Tab B" wrote while we were idle.
    storage.data.set(KEY_V2, JSON.stringify({ ...stored(), xp: 50, streak: 4 }));
    const seen: number[] = [];
    store.updateProgress((p) => {
      seen.push(p.xp);
      return { ...p, xp: p.xp + 5 };
    });
    expect(seen).toEqual([50]);
    expect(stored()).toMatchObject({ xp: 55, streak: 4 });
  });

  it('notifies subscribers on the storage event for our key, on clear (key null) and ignores other keys', async () => {
    const { store } = await load();
    store.getProgress();
    const { useProgress } = store;
    useProgress();
    const subscribe = (globalThis as unknown as { __subscribe: (l: () => void) => () => void }).__subscribe;
    const listener = vi.fn();
    const off = subscribe(listener);

    storage.data.set(KEY_V2, JSON.stringify({ ...emptyProgress(), xp: 30 }));
    windowListeners.forEach((l) => l({ key: 'something-else' }));
    expect(listener).not.toHaveBeenCalled();
    windowListeners.forEach((l) => l({ key: KEY_V2 }));
    expect(listener).toHaveBeenCalledTimes(1);
    expect(store.getProgress().xp).toBe(30);

    // Same value again: nothing to adopt, so no re-render.
    windowListeners.forEach((l) => l({ key: KEY_V2 }));
    expect(listener).toHaveBeenCalledTimes(1);

    storage.data.set(KEY_V2, JSON.stringify({ ...emptyProgress(), xp: 31 }));
    windowListeners.forEach((l) => l({ key: null }));
    expect(listener).toHaveBeenCalledTimes(2);

    off();
    store.updateProgress(withXp(99));
    expect(listener).toHaveBeenCalledTimes(2);
  });

  it('ignores a storage event before anything was loaded', async () => {
    const { store } = await load();
    storage.data.set(KEY_V2, JSON.stringify({ ...emptyProgress(), xp: 8 }));
    expect(() => windowListeners.forEach((l) => l({ key: KEY_V2 }))).not.toThrow();
    expect(store.getProgress().xp).toBe(8);
  });

  it('emits to subscribers when only another tab changed state and our updater was a no-op', async () => {
    const { store } = await load();
    store.useProgress();
    const subscribe = (globalThis as unknown as { __subscribe: (l: () => void) => () => void }).__subscribe;
    store.getProgress();
    const listener = vi.fn();
    subscribe(listener);
    storage.data.set(KEY_V2, JSON.stringify({ ...emptyProgress(), xp: 12 }));
    store.updateProgress((p) => p);
    expect(listener).toHaveBeenCalledTimes(1);
    expect(store.getProgress().xp).toBe(12);
  });

  it('exposes update and reset through useProgress', async () => {
    const { store } = await load();
    const hook = store.useProgress();
    hook.update(withXp(20));
    expect(store.getProgress().xp).toBe(20);
    hook.reset();
    expect(store.getProgress().xp).toBe(0);
    expect(stored().xp).toBe(0);
  });
});

describe('badges', () => {
  const finishedLesson = (p: Progress): Progress => ({ ...p, lessons: { x: { bestAccuracy: 1, completions: 1, lastDay: '2026-06-15' } } });

  it('records a newly earned badge and raises exactly one toast', async () => {
    const { store, toasts } = await load();
    store.updateProgress(finishedLesson);
    expect(stored().achievements['first-lesson']).toBe('2026-06-15');
    // The toast queue is observable through the hook's server snapshot.
    expect(readToast(toasts)).toMatchObject({ badges: [{ id: 'first-lesson' }] });
    // A second write that earns nothing new raises no further toast.
    toasts.dismissBadgeToast(readToast(toasts)!.id);
    store.updateProgress(withXp(5));
    expect(readToast(toasts)).toBeUndefined();
  });

  it('records badges silently for imports and replacements', async () => {
    const { store, toasts } = await load();
    store.replaceProgress(finishedLesson(emptyProgress()));
    expect(stored().achievements['first-lesson']).toBeDefined();
    expect(readToast(toasts)).toBeUndefined();
  });

  it('backfills once, silently, for a profile with history, and never for an empty one', async () => {
    const { store, toasts } = await load();
    store.backfillBadges();
    expect(storage.writes).toEqual([]); // nothing to backfill on an empty profile
    storage.data.set(KEY_V2, JSON.stringify(finishedLesson({ ...emptyProgress(), xp: 5 })));
    store.updateProgress((p) => p); // adopt the other tab's write
    store.backfillBadges();
    expect(stored().achievements['first-lesson']).toBeDefined();
    expect(stored().milestones['ach:init']).toBe('2026-06-15');
    expect(readToast(toasts)).toBeUndefined();
    const writes = storage.writes.length;
    store.backfillBadges();
    expect(storage.writes.length).toBe(writes); // idempotent
  });
});

function readToast(toasts: typeof import('./toasts')) {
  // useBadgeToast reads the queue with useSyncExternalStore, which the stub resolves to the snapshot.
  return toasts.useBadgeToast();
}

describe('persistence and the iOS tip', () => {
  it('asks the browser for persistent storage only once, and tolerates missing or throwing APIs', async () => {
    const { store } = await load();
    const persist = vi.fn().mockResolvedValue(true);
    vi.stubGlobal('navigator', { storage: { persist } });
    store.requestPersistence();
    store.requestPersistence();
    expect(persist).toHaveBeenCalledTimes(1);

    const second = await load();
    vi.stubGlobal('navigator', {});
    expect(() => second.store.requestPersistence()).not.toThrow();
    const third = await load();
    vi.stubGlobal('navigator', { storage: { persist: () => { throw new Error('nope'); } } });
    expect(() => third.store.requestPersistence()).not.toThrow();
  });

  const nav = (ua: string, extra: Record<string, unknown> = {}) => vi.stubGlobal('navigator', { userAgent: ua, maxTouchPoints: 0, ...extra });
  const IPHONE = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Safari/604.1';

  it('shows the tip on iPhone Safari that is not installed, and not elsewhere', async () => {
    const { store } = await load();
    nav(IPHONE);
    expect(store.shouldShowIosTip()).toBe(true);
    nav('Mozilla/5.0 (X11; Linux x86_64) Chrome/120');
    expect(store.shouldShowIosTip()).toBe(false);
    nav('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)', { maxTouchPoints: 5 }); // iPadOS reporting as a Mac
    expect(store.shouldShowIosTip()).toBe(true);
    nav('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)', { maxTouchPoints: 0 }); // a real Mac
    expect(store.shouldShowIosTip()).toBe(false);
  });

  it('hides the tip once installed (standalone) or dismissed, and survives a throwing environment', async () => {
    const { store } = await load();
    nav(IPHONE, { standalone: true });
    expect(store.shouldShowIosTip()).toBe(false);
    nav(IPHONE);
    vi.stubGlobal('window', { matchMedia: () => ({ matches: true }) });
    expect(store.shouldShowIosTip()).toBe(false);
    installWindow();
    nav(IPHONE);
    expect(store.shouldShowIosTip()).toBe(true);
    store.dismissIosTip();
    expect(store.shouldShowIosTip()).toBe(false);
    vi.stubGlobal('navigator', undefined);
    expect(store.shouldShowIosTip()).toBe(false);
  });

  it('dismissIosTip never throws when storage is unavailable', async () => {
    const { store } = await load();
    vi.stubGlobal('localStorage', { setItem: () => { throw new Error('full'); }, getItem: () => null });
    expect(() => store.dismissIosTip()).not.toThrow();
  });
});
