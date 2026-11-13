/**
 * The backup-note store with a fake `window.localStorage` and stubbed file helpers: what a real backup records, what a blocked one
 * does not, "Later", listeners and the `storage` event from another tab, and the in-memory fallback when storage is unavailable.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { BACKUP_KEY, BACKUP_SNOOZE_DAYS, addDays, dayKey } from '@trainer/shared';

vi.mock('react', () => ({
  useSyncExternalStore: (subscribe: (l: () => void) => () => void, get: () => unknown) => {
    (globalThis as { __subscribe?: unknown }).__subscribe = subscribe;
    return get();
  },
}));

const files = { saveFile: vi.fn(), copyText: vi.fn() };
vi.mock('./files', () => ({ saveFile: (...a: unknown[]) => files.saveFile(...a), copyText: (...a: unknown[]) => files.copyText(...a) }));
vi.mock('./store', () => ({ exportText: () => '{"backup":true}' }));

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
}

type StorageListener = (e: { key: string | null }) => void;
const subscribeToNote = (listener: () => void): (() => void) => (globalThis as unknown as { __subscribe: (l: () => void) => () => void }).__subscribe(listener);
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
  return import('./backup-note');
}

const today = '2026-11-04';
const stored = () => JSON.parse(storage.data.get(BACKUP_KEY) ?? 'null');

beforeEach(() => {
  storage = new FakeStorage();
  vi.useFakeTimers();
  vi.setSystemTime(new Date(2026, 10, 4, 10, 0, 0));
  files.saveFile.mockReset();
  files.copyText.mockReset();
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('the backup note', () => {
  it('starts empty, and an unreadable stored value is the same as empty', async () => {
    const m = await load();
    expect(m.loadBackupNote()).toEqual({ lastBackup: null, snoozedUntil: null });
    storage.data.set(BACKUP_KEY, 'garbage');
    const again = await load();
    expect(again.loadBackupNote()).toEqual({ lastBackup: null, snoozedUntil: null });
  });

  it('a saved download is recorded under today, with the day in the file name', async () => {
    files.saveFile.mockResolvedValue('saved');
    const m = await load();
    expect(await m.downloadBackup()).toBe('saved');
    expect(files.saveFile).toHaveBeenCalledWith(`interview-trainer-progress-${dayKey(new Date())}.json`, '{"backup":true}', 'application/json');
    expect(stored()).toMatchObject({ lastBackup: today, snoozedUntil: null });
    expect(m.loadBackupNote().lastBackup).toBe(today);
  });

  it('a declined or unsupported download records nothing', async () => {
    const m = await load();
    for (const result of ['declined', 'unsupported']) {
      files.saveFile.mockResolvedValue(result);
      expect(await m.downloadBackup()).toBe(result);
    }
    expect(storage.data.has(BACKUP_KEY)).toBe(false);
  });

  it('copying records only when the clipboard took the text', async () => {
    const m = await load();
    files.copyText.mockResolvedValue(false);
    expect(await m.copyBackup()).toBe(false);
    expect(storage.data.has(BACKUP_KEY)).toBe(false);
    files.copyText.mockResolvedValue(true);
    expect(await m.copyBackup()).toBe(true);
    expect(files.copyText).toHaveBeenLastCalledWith('{"backup":true}');
    expect(stored().lastBackup).toBe(today);
  });

  it('"Later" keeps the last backup and waits a week; a backup afterwards clears the wait', async () => {
    const m = await load();
    m.recordBackup();
    vi.setSystemTime(new Date(2026, 10, 20, 10, 0, 0));
    m.snoozeBackupNudge();
    expect(stored()).toMatchObject({ lastBackup: today, snoozedUntil: addDays('2026-11-20', BACKUP_SNOOZE_DAYS) });
    m.recordBackup();
    expect(stored()).toMatchObject({ lastBackup: '2026-11-20', snoozedUntil: null });
  });

  it('tells listeners, caches per stored text, and follows the storage event of another tab', async () => {
    const m = await load();
    const a = m.loadBackupNote();
    expect(m.loadBackupNote()).toBe(a);
    m.useBackupNote(); // hands React's subscribe function to the stub
    const seen = vi.fn();
    const off = subscribeToNote(seen);
    m.recordBackup();
    expect(seen).toHaveBeenCalledTimes(1);
    for (const fn of windowListeners) {
      fn({ key: 'something-else' });
      fn({ key: BACKUP_KEY });
      fn({ key: null });
    }
    expect(seen).toHaveBeenCalledTimes(3);
    off();
    expect(removedListeners.length).toBe(1);
    expect(m.useBackupNote().lastBackup).toBe(today);
  });

  it('keeps it in memory for this tab when storage refuses writes or reads', async () => {
    storage.failWrites = true;
    const m = await load();
    m.recordBackup();
    expect(storage.data.has(BACKUP_KEY)).toBe(false);
    expect(m.loadBackupNote().lastBackup).toBe(today);
    storage.failReads = true;
    const n = await load();
    expect(n.loadBackupNote()).toEqual({ lastBackup: null, snoozedUntil: null });
    n.recordBackup();
    expect(n.loadBackupNote().lastBackup).toBe(today);
  });
});
