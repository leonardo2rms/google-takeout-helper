/**
 * Data safety of the progress store: a failed write never loses unsaved sessions (S0-2), unreadable data is parked and
 * surfaced rather than overwritten (S0-1, S1-6), blocked storage is detected (S1-5), replace/reset keep a safety copy (S2-18).
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { KEY_V2, emptyProgress, type Progress } from '@trainer/shared';

vi.mock('react', () => ({
  useCallback: <T,>(fn: T) => fn,
  useSyncExternalStore: (_subscribe: unknown, get: () => unknown) => get(),
}));

class FakeStorage {
  data = new Map<string, string>();
  failWrites = false;
  /** Fail only writes to keys that match (e.g. the parked-copy keys). */
  failKey: ((k: string) => boolean) | null = null;
  writes: string[] = [];
  get length() {
    return this.data.size;
  }
  key(i: number) {
    return [...this.data.keys()][i] ?? null;
  }
  removeItem(k: string) {
    this.data.delete(k);
  }
  getItem(k: string) {
    return this.data.get(k) ?? null;
  }
  setItem(k: string, v: string) {
    if (this.failWrites || this.failKey?.(k)) throw new DOMException('quota', 'QuotaExceededError');
    this.writes.push(k);
    this.data.set(k, v);
  }
}

let storage: FakeStorage;
let storageGetterThrows = false;
let storageListeners: Array<(e: { key: string | null }) => void>;
let pageListeners: Record<string, Array<() => void>>;

async function load() {
  vi.resetModules();
  storageListeners = [];
  pageListeners = {};
  vi.stubGlobal('window', {
    get localStorage() {
      if (storageGetterThrows) throw new DOMException('denied', 'SecurityError');
      return storage;
    },
    addEventListener: (type: string, fn: (e: { key: string | null }) => void) => {
      if (type === 'storage') storageListeners.push(fn);
      else (pageListeners[type] ??= []).push(fn as () => void);
    },
    matchMedia: () => ({ matches: false }),
  });
  const store = await import('./store');
  const fire = [...storageListeners]; // this tab's `storage` listeners (load() resets the shared array for the next tab)
  return { store, otherTabWrote: () => fire.forEach((l) => l({ key: KEY_V2 })) };
}

const stored = (): Progress => JSON.parse(storage.data.get(KEY_V2)!);
const addXp = (n: number) => (p: Progress): Progress => ({ ...p, xp: p.xp + n, stats: { ...p.stats, sessions: p.stats.sessions + 1 } });
const parked = () => [...storage.data.keys()].filter((k) => k.includes('.backup-') || k.includes('.before-'));

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

describe('a failed write never loses unsaved sessions (S0-2)', () => {
  it('the write after a failed one still contains the unsaved session', async () => {
    const { store } = await load();
    store.updateProgress(addXp(10)); // q1 saved
    storage.failWrites = true;
    store.updateProgress(addXp(10)); // q2: in memory only
    expect(store.getProgress().xp).toBe(20);
    storage.failWrites = false;
    store.updateProgress(addXp(10)); // q3
    expect(stored()).toMatchObject({ xp: 30, stats: { sessions: 3 } });
    expect(store.getProgress().xp).toBe(30);
  });

  it('reports the risk while writes fail and clears it once they work', async () => {
    const { store } = await load();
    expect(store.getSafetyStatus()).toMatchObject({ persistence: 'ok', atRisk: false });
    store.updateProgress(addXp(10));
    expect(store.getSafetyStatus()).toMatchObject({ persistence: 'ok', atRisk: false });
    storage.failWrites = true;
    store.updateProgress(addXp(10));
    expect(store.getSafetyStatus()).toMatchObject({ persistence: 'failing', atRisk: true });
    storage.failWrites = false;
    store.retrySave();
    expect(store.getSafetyStatus()).toMatchObject({ persistence: 'ok', atRisk: false });
    expect(stored().xp).toBe(20);
  });

  it('retries on its own with backoff, and stops retrying after success', async () => {
    const { store } = await load();
    storage.failWrites = true;
    store.updateProgress(addXp(10));
    await vi.advanceTimersByTimeAsync(4000);
    expect(store.getSafetyStatus().atRisk).toBe(true); // still failing: scheduled the next, longer, retry
    storage.failWrites = false;
    await vi.advanceTimersByTimeAsync(8000);
    expect(stored().xp).toBe(10);
    expect(store.getSafetyStatus().atRisk).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('the back-off doubles from 4 s and stops growing at 60 s (retries at +4, +8, +16, +32, then every 60 s)', async () => {
    const { store } = await load();
    const attempts = vi.spyOn(storage, 'setItem');
    storage.failWrites = true;
    store.updateProgress(addXp(10));
    const tries = () => attempts.mock.calls.length;
    expect(tries()).toBe(1);
    // times of the retries: 4, 12, 28, 60, then 120 and 180 (a missing cap would give 124 and 252)
    const checkpoints: Array<[number, number]> = [[3999, 1], [4000, 2], [11_999, 2], [12_000, 3], [27_999, 3], [28_000, 4], [59_999, 4], [60_000, 5], [119_999, 5], [120_000, 6], [179_999, 6], [180_000, 7]];
    let now = 0;
    for (const [at, expected] of checkpoints) {
      await vi.advanceTimersByTimeAsync(at - now);
      now = at;
      expect(tries(), `${at} ms`).toBe(expected);
    }
  });

  it('saves when the page is hidden, closed or back online; retrySave is a no-op when nothing is pending', async () => {
    const { store } = await load();
    store.retrySave(); // before anything is loaded
    store.updateProgress(addXp(5));
    const writes = storage.writes.length;
    store.retrySave();
    expect(storage.writes.length).toBe(writes);
    storage.failWrites = true;
    store.updateProgress(addXp(5));
    storage.failWrites = false;
    for (const type of ['pagehide', 'visibilitychange', 'online']) expect(pageListeners[type]).toHaveLength(1);
    pageListeners.pagehide![0]!();
    expect(stored().xp).toBe(10);
  });

  it('merges unsaved changes on top of what another tab wrote meanwhile', async () => {
    const { store } = await load();
    store.updateProgress(addXp(10));
    storage.failWrites = true;
    store.updateProgress(addXp(10)); // unsaved: 20
    storage.failWrites = false;
    storage.data.set(KEY_V2, JSON.stringify({ ...stored(), xp: 50, stats: { ...stored().stats, sessions: 1 } })); // another tab saved 50
    store.updateProgress((p) => p);
    expect(store.getProgress().xp).toBe(50); // the merge keeps the larger XP and the larger counters
    expect(store.getProgress().stats.sessions).toBe(2);
    store.retrySave();
    expect(stored().xp).toBe(50);
  });

  it('keeps the same status object while nothing changed (no needless re-renders)', async () => {
    const { store } = await load();
    const a = store.getSafetyStatus();
    store.updateProgress(addXp(1));
    expect(store.getSafetyStatus()).toBe(a);
  });
});

describe('blocked storage is detected (S1-5)', () => {
  it('reports "memory" once there is progress that would vanish on reload', async () => {
    storageGetterThrows = true;
    const { store } = await load();
    expect(store.getSafetyStatus()).toMatchObject({ persistence: 'memory', atRisk: false }); // nothing to lose yet
    store.updateProgress(addXp(10));
    expect(store.getSafetyStatus()).toMatchObject({ persistence: 'memory', atRisk: true });
    expect(store.exportText()).toContain('"xp": 10'); // an export is still possible
  });

  it('swallows a rejected persist() promise', async () => {
    const { store } = await load();
    vi.stubGlobal('navigator', { storage: { persist: () => Promise.reject(new DOMException('no', 'SecurityError')) } });
    store.requestPersistence();
    await vi.advanceTimersByTimeAsync(10);
    // vitest fails the run on an unhandled rejection, so reaching this line is the assertion
    expect(store.getSafetyStatus().persistence).toBe('ok');
  });
});

describe('safety copies in a tab whose storage is blocked', () => {
  it('are kept in the tab, rotated, and listed', async () => {
    storageGetterThrows = true;
    const { store } = await load();
    const { reset } = store.useProgress();
    for (let i = 1; i <= 5; i++) {
      store.updateProgress(addXp(i)); // a different profile each time, so each reset makes a new copy
      reset();
    }
    const copies = store.listParkedCopies();
    expect(copies).toHaveLength(3); // the newest three
    expect(copies.map((c) => JSON.parse(c.raw).xp)).toEqual([5, 4, 3]);
  });

  it('useSafetyStatus returns the same snapshot as getSafetyStatus', async () => {
    const { store } = await load();
    expect(store.useSafetyStatus()).toBe(store.getSafetyStatus());
  });
});

describe('storage that fails after it was working', () => {
  it('keeps the in-memory profile when a later read throws, and does not claim another tab wrote', async () => {
    const { store } = await load();
    store.updateProgress(addXp(5));
    let reads = 0;
    const real = storage.getItem.bind(storage);
    storage.getItem = (k: string) => {
      // the availability probe passes; the read that looks for another tab's write throws
      if (++reads % 2 === 0) throw new Error('revoked');
      return real(k);
    };
    expect(() => store.updateProgress(addXp(5))).not.toThrow();
    expect(store.getProgress().xp).toBe(10);
  });
});

describe('unreadable data is parked, kept and shown (S0-1, S1-6)', () => {
  const future = JSON.stringify({ version: 99, xp: 1000 });

  it('does not overwrite newer-version data while its copy cannot be written, and keeps playing in memory', async () => {
    storage.data.set(KEY_V2, future);
    storage.failKey = (k) => k.includes('.backup-');
    const { store } = await load();
    store.updateProgress(addXp(10));
    expect(storage.data.get(KEY_V2)).toBe(future);
    expect(store.getProgress().xp).toBe(10);
    expect(store.getSafetyStatus()).toMatchObject({ unreadablePending: true, atRisk: true });
    storage.failKey = null;
    store.updateProgress(addXp(10)); // the backup works now: parked, then written
    expect(stored().xp).toBe(20);
    expect(parked().map((k) => storage.data.get(k))).toEqual([future]);
    expect(store.getSafetyStatus().unreadablePending).toBe(false);
  });

  it('a second corruption the same day does not replace the first copy', async () => {
    storage.data.set(KEY_V2, '{first');
    let { store } = await load();
    store.updateProgress(addXp(1));
    storage.data.set(KEY_V2, '{second');
    ({ store } = await load());
    store.updateProgress(addXp(1));
    expect(parked().map((k) => storage.data.get(k)).sort()).toEqual(['{first', '{second']);
  });

  it('raises a notice for parked data until it is acknowledged, then stays quiet', async () => {
    storage.data.set(KEY_V2, future);
    const { store } = await load();
    expect(store.getSafetyStatus()).toMatchObject({ unreadablePending: true, recovered: [] });
    store.updateProgress(addXp(1));
    expect(store.getSafetyStatus().recovered).toHaveLength(1);
    expect(store.listParkedCopies().map((c) => c.raw)).toEqual([future]);
    store.acknowledgeRecovered();
    expect(store.getSafetyStatus().recovered).toEqual([]);
    // a reload remembers the acknowledgement; a new corruption raises a new notice
    const again = await load();
    expect(again.store.getSafetyStatus().recovered).toEqual([]);
    storage.data.set(KEY_V2, '{later corruption');
    const third = await load();
    third.store.updateProgress(addXp(1));
    expect(third.store.getSafetyStatus().recovered).toHaveLength(1);
  });

  it('acknowledging works when storage refuses the write (kept for this tab)', async () => {
    storage.data.set(KEY_V2, future);
    const { store } = await load();
    store.updateProgress(addXp(1));
    storage.failWrites = true;
    store.acknowledgeRecovered();
    expect(store.getSafetyStatus().recovered).toEqual([]);
  });
});

describe('replace and reset keep a safety copy (S2-18)', () => {
  it("replace keeps a copy of the current profile and this device's reading preferences", async () => {
    const { store } = await load();
    store.updateProgress((p) => ({ ...addXp(40)(p), prefs: { ...p.prefs, reading: { ...p.prefs.reading, size: 'large' as never } } }));
    const prefs = store.getProgress().prefs;
    store.replaceProgress({ ...emptyProgress(), xp: 5 });
    expect(stored().xp).toBe(5);
    expect(stored().prefs).toEqual(prefs);
    const copies = store.listParkedCopies();
    expect(copies).toHaveLength(1);
    expect(copies[0]).toMatchObject({ kind: 'safety' });
    expect(JSON.parse(copies[0]!.raw).xp).toBe(40);
  });

  it('reset keeps a copy too, but not of an empty profile', async () => {
    const { store } = await load();
    store.useProgress().reset();
    expect(store.listParkedCopies()).toEqual([]);
    store.updateProgress(addXp(7));
    store.useProgress().reset();
    expect(stored().xp).toBe(0);
    expect(store.listParkedCopies().map((c) => JSON.parse(c.raw).xp)).toEqual([7]);
  });
});

describe('a foreign value in the main key never replaces a tab\'s own profile (N-S1-1)', () => {
  const newer = JSON.stringify({ version: 3, xp: 5000, something: 'only a newer build understands' });
  const parkedRaws = () => parked().map((k) => storage.data.get(k));

  async function tabWithTwentyXp() {
    const tab = await load();
    tab.store.updateProgress(addXp(10));
    tab.store.updateProgress(addXp(10));
    expect(stored().xp).toBe(20);
    return tab;
  }

  it('a newer-schema value: keeps the 20 XP in memory, parks the value, and the next answer saves 30 (not 10)', async () => {
    const { store, otherTabWrote } = await tabWithTwentyXp();
    storage.data.set(KEY_V2, newer);
    otherTabWrote();
    expect(store.getProgress().xp).toBe(20);
    expect(parkedRaws()).toEqual([newer]);
    expect(store.getSafetyStatus().recovered).toHaveLength(1); // the recovery banner
    expect(storage.data.get(KEY_V2)).toBe(newer); // the newer tab's value is left alone until this tab changes something
    store.updateProgress(addXp(10));
    expect(stored().xp).toBe(30);
    expect(parkedRaws()).toEqual([newer]);
  });

  it('a corrupt value: parked, and the profile is written back at once (this tab may hold the only good copy)', async () => {
    const { store, otherTabWrote } = await tabWithTwentyXp();
    storage.data.set(KEY_V2, '{"version":2,"xp":2');
    otherTabWrote();
    expect(parkedRaws()).toEqual(['{"version":2,"xp":2']);
    expect(stored().xp).toBe(20);
    expect(store.getProgress().xp).toBe(20);
    expect(store.getSafetyStatus().recovered).toHaveLength(1);
  });

  it('a value found while updating (no storage event) is handled the same way', async () => {
    const { store } = await tabWithTwentyXp();
    storage.data.set(KEY_V2, newer);
    store.updateProgress(addXp(10));
    expect(stored().xp).toBe(30);
    expect(parkedRaws()).toEqual([newer]);
  });

  it('a cleared key is restored from memory', async () => {
    const { store, otherTabWrote } = await tabWithTwentyXp();
    storage.data.delete(KEY_V2);
    otherTabWrote();
    expect(stored().xp).toBe(20);
    storage.data.delete(KEY_V2);
    store.updateProgress(addXp(10));
    expect(stored().xp).toBe(30);
  });

  it('an empty tab simply adopts whatever the other tab wrote (nothing to protect)', async () => {
    const { store, otherTabWrote } = await load();
    expect(store.getProgress().xp).toBe(0);
    storage.data.set(KEY_V2, '{broken');
    otherTabWrote();
    expect(store.getProgress().xp).toBe(0);
    expect(store.getSafetyStatus().unreadablePending).toBe(true); // handled like a fresh load: parked at the first write
    store.updateProgress(addXp(10));
    expect(stored().xp).toBe(10);
    expect(parkedRaws()).toEqual(['{broken']);
  });

  it('writes nothing while the copy cannot be made, then writes once it can', async () => {
    const { store, otherTabWrote } = await tabWithTwentyXp();
    storage.failKey = (k) => k.includes('.backup-');
    storage.data.set(KEY_V2, newer);
    otherTabWrote();
    store.updateProgress(addXp(10));
    expect(storage.data.get(KEY_V2)).toBe(newer);
    expect(store.getProgress().xp).toBe(30);
    expect(store.getSafetyStatus()).toMatchObject({ unreadablePending: true, atRisk: true });
    storage.failKey = null;
    store.retrySave();
    expect(stored().xp).toBe(30);
    expect(parkedRaws()).toEqual([newer]);
  });

  it('two tabs that both see the foreign value park it once and end up agreeing', async () => {
    const a = await tabWithTwentyXp();
    const b = await load();
    expect(b.store.getProgress().xp).toBe(20);
    storage.data.set(KEY_V2, newer);
    a.otherTabWrote();
    b.otherTabWrote();
    expect(parkedRaws()).toEqual([newer]);
    a.store.updateProgress(addXp(10)); // A writes 30
    b.otherTabWrote();
    b.store.updateProgress(addXp(5)); // B adopts A's 30 and adds 5
    expect(stored().xp).toBe(35);
    a.otherTabWrote();
    expect(a.store.getProgress().xp).toBe(35);
    expect(parkedRaws()).toEqual([newer]);
  });
});

describe('parked copies keep working when the clock is wrong (N-S2-3)', () => {
  it('the newest-version data is kept as a copy even when copies from a later date already exist', async () => {
    for (let i = 1; i <= 8; i++) storage.data.set(`interview-trainer.progress.backup-20270101-0${i}`, `later-${i}`);
    const future = JSON.stringify({ version: 3, xp: 1 });
    storage.data.set(KEY_V2, future);
    const { store } = await load();
    store.updateProgress(addXp(10));
    const key = parked().find((k) => storage.data.get(k) === future)!;
    expect(key).toBeDefined();
    store.acknowledgeRecovered(); // the learner saw the banner for the old copies...
    storage.data.set(KEY_V2, '{corrupt again');
    store.updateProgress(addXp(1)); // ...a new copy must raise a new one even though its date is "older"
    expect(store.getSafetyStatus().recovered.length).toBe(1);
  });
});
