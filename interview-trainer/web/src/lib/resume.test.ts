/**
 * The saved-session store with a fake `window.localStorage`: round trip, one session at a time, expiry, a changed
 * course, listeners and the `storage` event from another tab, and the in-memory fallback when storage is unavailable
 * or refuses a write. React is replaced by a stub so `useResume` can be called without a DOM.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { RESUME_KEY, RESUME_MAX_AGE_MS, allQuestions, createSession, submit, type Question, type Session } from '@trainer/shared';
import course from 'virtual:course';

vi.mock('react', () => ({
  useSyncExternalStore: (subscribe: (l: () => void) => () => void, get: () => unknown) => {
    (globalThis as { __subscribe?: unknown }).__subscribe = subscribe;
    return get();
  },
}));

class FakeStorage {
  data = new Map<string, string>();
  failWrites = false;
  failReads = false;
  getItem(k: string) {
    if (this.failReads) throw new DOMException('denied', 'SecurityError');
    return this.data.get(k) ?? null;
  }
  setItem(k: string, v: string) {
    if (this.failWrites) throw new DOMException('quota', 'QuotaExceededError');
    this.data.set(k, v);
  }
  removeItem(k: string) {
    if (this.failWrites) throw new DOMException('quota', 'QuotaExceededError');
    this.data.delete(k);
  }
}

type StorageListener = (e: { key: string | null }) => void;
/** The `subscribe` function `useResume` handed to React (captured by the stub above). */
const subscribeToResume = (listener: () => void): (() => void) => (globalThis as unknown as { __subscribe: (l: () => void) => () => void }).__subscribe(listener);
let storage: FakeStorage;
let windowListeners: StorageListener[];
let removedListeners: StorageListener[];

async function load() {
  vi.resetModules();
  windowListeners = [];
  removedListeners = [];
  vi.stubGlobal('window', {
    localStorage: storage,
    addEventListener: (type: string, fn: StorageListener) => type === 'storage' && windowListeners.push(fn),
    removeEventListener: (type: string, fn: StorageListener) => type === 'storage' && removedListeners.push(fn),
  });
  return import('./resume');
}

const lesson = course.units[0]!.lessons[0]!;
const other = course.units[1]!.lessons[0]!;
const meta = (route = `/lesson/${lesson.id}`) => ({ route, title: 'Null safety', kind: 'lesson' as const, lessonId: lesson.id, servedOn: '2026-06-15' });
const played = (n: number, from: readonly Question[] = lesson.questions): Session => {
  let s = createSession(from);
  for (let i = 0; i < n; i++) s = submit(s, true);
  return s;
};

beforeEach(() => {
  storage = new FakeStorage();
  vi.useFakeTimers();
  vi.setSystemTime(new Date(2026, 5, 15, 12, 0, 0));
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('saving and loading', () => {
  it('has nothing at first', async () => {
    const r = await load();
    expect(r.loadResume()).toBeNull();
    expect(r.resumeFor(`/lesson/${lesson.id}`)).toBeNull();
  });

  it('keeps a session after an answer and gives back the same session, with the current questions', async () => {
    const r = await load();
    const s = played(2);
    r.saveResume(s, meta());
    expect(storage.data.has(RESUME_KEY)).toBe(true);
    const back = r.loadResume()!;
    expect(back.session).toEqual(s);
    expect(back.record).toMatchObject({ title: 'Null safety', kind: 'lesson', route: `/lesson/${lesson.id}` });
    expect(r.loadResume()).toBe(back); // stable between reads: useSyncExternalStore needs that
  });

  it('stores nothing for a session with nothing answered, and nothing once it is finished', async () => {
    const r = await load();
    r.saveResume(createSession(lesson.questions), meta());
    expect(storage.data.size).toBe(0);
    r.saveResume(played(lesson.questions.length), meta());
    expect(storage.data.size).toBe(0);
  });

  it('resumeFor answers only for the route the session belongs to', async () => {
    const r = await load();
    r.saveResume(played(1), meta());
    expect(r.resumeFor(`/lesson/${lesson.id}`)).not.toBeNull();
    expect(r.resumeFor(`/lesson/${other.id}`)).toBeNull();
    expect(r.resumeFor('/daily')).toBeNull();
  });

  it('holds one session: saving another replaces it', async () => {
    const r = await load();
    r.saveResume(played(1), meta());
    r.saveResume(played(1, other.questions), { ...meta(`/lesson/${other.id}`), lessonId: other.id, title: 'Other' });
    expect(r.loadResume()!.record.title).toBe('Other');
    expect(r.resumeFor(`/lesson/${lesson.id}`)).toBeNull();
  });

  it('forgets a session that is too old, or that no longer matches the course', async () => {
    const r = await load();
    r.saveResume(played(1), meta());
    expect(r.loadResume()).not.toBeNull();
    vi.setSystemTime(Date.now() + RESUME_MAX_AGE_MS + 1000);
    expect(r.loadResume()).toBeNull();

    vi.setSystemTime(new Date(2026, 5, 15, 12, 0, 0));
    const r2 = await load();
    const id = allQuestions(course).find((q) => q.id === lesson.questions[1]!.id)!.id;
    storage.data.set(RESUME_KEY, JSON.stringify({ ...JSON.parse(storage.data.get(RESUME_KEY)!), queue: [{ id, h: 'changed' }] }));
    expect(r2.loadResume()).toBeNull();
  });

  it('treats unreadable text as nothing, and clearing removes it', async () => {
    const r = await load();
    storage.data.set(RESUME_KEY, '{not json');
    expect(r.loadResume()).toBeNull();
    r.clearResume();
    expect(storage.data.has(RESUME_KEY)).toBe(false);
  });
});

describe('clearing', () => {
  it('removes the session, and tells listeners', async () => {
    const r = await load();
    r.saveResume(played(1), meta());
    r.useResume();
    const seen = vi.fn();
    subscribeToResume(seen);
    r.clearResume();
    expect(r.loadResume()).toBeNull();
    expect(storage.data.has(RESUME_KEY)).toBe(false);
    expect(seen).toHaveBeenCalledTimes(1);
  });

  it('with a route only clears that session', async () => {
    const r = await load();
    r.saveResume(played(1), meta());
    r.clearResume('/daily');
    expect(r.loadResume()).not.toBeNull();
    r.clearResume(`/lesson/${lesson.id}`);
    expect(r.loadResume()).toBeNull();
  });

  it('does nothing, and says nothing, when there is nothing to clear', async () => {
    const r = await load();
    const seen = vi.fn();
    r.useResume();
    subscribeToResume(seen);
    r.clearResume();
    r.clearResume('/daily');
    expect(seen).not.toHaveBeenCalled();
  });
});

describe('listeners and other tabs', () => {
  it('a save notifies listeners; unsubscribing stops it and removes the storage listener', async () => {
    const r = await load();
    r.useResume();
    const subscribe = subscribeToResume;
    const seen = vi.fn();
    const stop = subscribe(seen);
    r.saveResume(played(1), meta());
    expect(seen).toHaveBeenCalledTimes(1);
    stop();
    expect(removedListeners).toHaveLength(1);
    r.saveResume(played(2), meta());
    expect(seen).toHaveBeenCalledTimes(1);
  });

  it('a storage event for this key (or a cleared storage) notifies; other keys do not', async () => {
    const r = await load();
    r.useResume();
    const seen = vi.fn();
    subscribeToResume(seen);
    for (const fire of windowListeners) {
      fire({ key: 'something-else' });
      expect(seen).not.toHaveBeenCalled();
      fire({ key: RESUME_KEY });
      fire({ key: null });
    }
    expect(seen).toHaveBeenCalledTimes(2);
  });

  it('useResume returns the saved session', async () => {
    const r = await load();
    expect(r.useResume()).toBeNull();
    r.saveResume(played(1), meta());
    expect(r.useResume()?.record.route).toBe(`/lesson/${lesson.id}`);
  });
});

describe('when storage does not work', () => {
  it('a storage area that throws on read: the session is kept in memory for this tab', async () => {
    storage.failReads = true;
    const r = await load();
    expect(r.loadResume()).toBeNull();
    r.saveResume(played(2), meta());
    expect(r.loadResume()!.session.resolved).toBe(2);
    r.clearResume();
    expect(r.loadResume()).toBeNull();
  });

  it('a write that fails (quota): this tab keeps the newest session and never reads back the older stored one', async () => {
    const r = await load();
    r.saveResume(played(1), meta());
    storage.failWrites = true;
    r.saveResume(played(3), meta());
    expect(r.loadResume()!.session.resolved).toBe(3);
    r.clearResume();
    expect(r.loadResume()).toBeNull();
  });

  it('a window without localStorage at all behaves the same', async () => {
    vi.resetModules();
    vi.stubGlobal('window', {
      get localStorage(): never {
        throw new DOMException('denied', 'SecurityError');
      },
      addEventListener: () => undefined,
      removeEventListener: () => undefined,
    });
    const r = await import('./resume');
    expect(r.loadResume()).toBeNull();
    r.saveResume(played(1), meta());
    expect(r.loadResume()).not.toBeNull();
  });
});
