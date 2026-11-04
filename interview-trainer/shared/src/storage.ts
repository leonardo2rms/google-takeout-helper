import type { Day } from './day';
import { migrateProgress, type MigrationContext } from './migrate';
import { emptyProgress, type Progress } from './progress';

export const KEY_V1 = 'interview-trainer.progress.v1';
export const KEY_V2 = 'interview-trainer.progress.v2';
export const backupKey = (today: Day): string => `interview-trainer.progress.backup-${today.replaceAll('-', '')}`;

/** The subset of the Web Storage API we use, so the load/save rules can be tested without a browser. */
export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

export interface Loaded {
  progress: Progress;
  /** The exact string last read from / written to KEY_V2 (null if none), used to notice other tabs' writes. */
  raw: string | null;
  /**
   * Set when KEY_V2 holds something we could not understand (corrupt, or written by a newer version).
   * It must not be overwritten before it has been copied to a backup key: `saveProgress` does that.
   */
  protectedRaw: string | null;
  from: 0 | 1 | 2;
}

function safeGet(s: StorageLike, key: string): string | null {
  try {
    return s.getItem(key);
  } catch {
    return null;
  }
}

function safeSet(s: StorageLike, key: string, value: string): boolean {
  try {
    s.setItem(key, value);
    return true;
  } catch {
    return false; // private mode / quota: progress just won't persist
  }
}

/**
 * Load order: v2 (normalised and written back if needed) -> else migrate v1 and write v2 -> else empty.
 * The v1 key is never deleted, so rolling back to the previous app version keeps working.
 */
export function loadProgress(storage: StorageLike, ctx: MigrationContext): Loaded {
  const raw2 = safeGet(storage, KEY_V2);
  if (raw2 !== null) {
    const res = migrateProgress(raw2, ctx);
    if (res.problem) return { progress: res.progress, raw: raw2, protectedRaw: raw2, from: 0 };
    let raw = raw2;
    if (res.changed) {
      raw = JSON.stringify(res.progress);
      safeSet(storage, KEY_V2, raw);
    }
    return { progress: res.progress, raw, protectedRaw: null, from: res.from };
  }
  const raw1 = safeGet(storage, KEY_V1);
  if (raw1 !== null) {
    const res = migrateProgress(raw1, ctx);
    if (!res.problem && res.from === 1) {
      const raw = JSON.stringify(res.progress);
      safeSet(storage, KEY_V2, raw);
      return { progress: res.progress, raw, protectedRaw: null, from: 1 };
    }
  }
  return { progress: emptyProgress(), raw: null, protectedRaw: null, from: 0 };
}

/** Writes `p`. If an unreadable value is being replaced, it is first copied to `backupKey(today)`. */
export function saveProgress(storage: StorageLike, p: Progress, protectedRaw: string | null, today: Day): { raw: string; backedUp: boolean } {
  let backedUp = false;
  if (protectedRaw !== null) backedUp = safeSet(storage, backupKey(today), protectedRaw);
  const raw = JSON.stringify(p);
  safeSet(storage, KEY_V2, raw);
  return { raw, backedUp };
}
