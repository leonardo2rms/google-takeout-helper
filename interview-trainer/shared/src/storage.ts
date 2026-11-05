import type { Day } from './day';
import { migrateProgress, type MigrationContext } from './migrate';
import { emptyProgress, type Progress } from './progress';

export const KEY_V1 = 'interview-trainer.progress.v1';
export const KEY_V2 = 'interview-trainer.progress.v2';
/** Prefix of copies of data this version could not read (corrupt, or written by a newer version). */
export const BACKUP_PREFIX = 'interview-trainer.progress.backup-';
/** Prefix of safety copies of a readable profile, taken just before it is replaced or reset. */
export const SAFETY_PREFIX = 'interview-trainer.progress.before-';
/** The first (oldest-format) backup key of a day. Further copies of the same day get a `-NN` suffix, see `parkedKey`. */
export const backupKey = (today: Day): string => `${BACKUP_PREFIX}${today.replaceAll('-', '')}`;

/** The subset of the Web Storage API we use, so the load/save rules can be tested without a browser. */
export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  /** Optional: only needed to find, rotate and (in the UI) delete parked copies. */
  removeItem?(key: string): void;
  readonly length?: number;
  key?(index: number): string | null;
}

export type ParkedKind = 'unreadable' | 'safety';

/** A copy of progress set aside in storage, listed so the learner can get it back. */
export interface ParkedCopy {
  key: string;
  kind: ParkedKind;
  /** `YYYY-MM-DD` the copy was made, or null if the key does not say. */
  day: Day | null;
  raw: string;
}

/** How many copies of each kind are kept. The oldest goes only after a newer one was written successfully. */
export const MAX_PARKED: Record<ParkedKind, number> = { unreadable: 8, safety: 3 };
const MAX_PER_DAY = 30;

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

const dayOfStamp = (stamp: string): Day | null => (/^\d{8}$/.test(stamp) ? `${stamp.slice(0, 4)}-${stamp.slice(4, 6)}-${stamp.slice(6, 8)}` : null);
const pad2 = (n: number) => String(n).padStart(2, '0');

/**
 * The storage key of the `n`th copy of `kind` made on `today`. Keys sort in creation order (lexicographically) up to 99 a day.
 * The first unreadable copy of a day keeps the historic key `backupKey(today)`.
 */
export function parkedKey(kind: ParkedKind, today: Day, n: number): string {
  const stamp = today.replaceAll('-', '');
  if (kind === 'unreadable') return n <= 1 ? `${BACKUP_PREFIX}${stamp}` : `${BACKUP_PREFIX}${stamp}-${pad2(n)}`;
  return `${SAFETY_PREFIX}${stamp}-${pad2(n)}`;
}

function parseParkedKey(key: string): { kind: ParkedKind; day: Day | null } | null {
  for (const [prefix, kind] of [[BACKUP_PREFIX, 'unreadable'], [SAFETY_PREFIX, 'safety']] as const) {
    if (key.startsWith(prefix)) return { kind, day: dayOfStamp(key.slice(prefix.length).slice(0, 8)) };
  }
  return null;
}

/** Every parked copy in storage, newest first. Returns [] when the storage cannot enumerate its keys. */
export function listParked(storage: StorageLike): ParkedCopy[] {
  const out: ParkedCopy[] = [];
  try {
    const n = storage.length ?? 0;
    for (let i = 0; i < n; i++) {
      const key = storage.key?.(i);
      const meta = key ? parseParkedKey(key) : null;
      if (!key || !meta) continue;
      const raw = safeGet(storage, key);
      if (raw !== null) out.push({ key, ...meta, raw });
    }
  } catch {
    /* an unreadable storage lists nothing */
  }
  return out.sort((a, b) => (a.key < b.key ? 1 : a.key > b.key ? -1 : 0));
}

/**
 * Sets `raw` aside under a key that is never reused: an existing copy is never replaced, so a second corruption cannot
 * erase the first. An identical copy that is already there counts as success. Returns the key holding the copy,
 * or null if it could not be written (quota, blocked storage): the caller must then keep the original where it is.
 * After a successful write the oldest copies beyond `MAX_PARKED` are dropped.
 */
export function parkRaw(storage: StorageLike, raw: string, today: Day, kind: ParkedKind): string | null {
  const existing = listParked(storage).filter((c) => c.kind === kind);
  const same = existing.find((c) => c.raw === raw);
  if (same) return same.key;
  // Always one past the highest index used today, so keys keep sorting in creation order even after rotation freed a lower slot.
  const prefix = (kind === 'unreadable' ? BACKUP_PREFIX : SAFETY_PREFIX) + today.replaceAll('-', '');
  let highest = 0;
  for (const c of existing) {
    if (!c.key.startsWith(prefix)) continue;
    const m = /^-(\d{2})$/.exec(c.key.slice(prefix.length));
    highest = Math.max(highest, m ? Number(m[1]) : c.key === prefix ? 1 : 0);
  }
  const key = highest < MAX_PER_DAY ? parkedKey(kind, today, highest + 1) : null;
  if (key === null || !safeSet(storage, key, raw)) return null;
  const all = listParked(storage).filter((c) => c.kind === kind); // newest first
  for (const old of all.slice(MAX_PARKED[kind])) {
    try {
      storage.removeItem?.(old.key);
    } catch {
      /* keep it */
    }
  }
  return key;
}

export interface Saved {
  /** The serialised profile (what was, or would have been, written). */
  raw: string;
  /** True when an unreadable value was in the way and a copy of it now exists. */
  backedUp: boolean;
  /** True when `raw` really is in storage now. */
  written: boolean;
  /** True when nothing was written because the unreadable value could not be copied first: it is still the only copy. */
  skipped: boolean;
}

/**
 * Writes `p`. If an unreadable value is being replaced, it is first copied to a parked key; when that copy cannot be
 * made, nothing is written at all, so the original survives and the caller keeps `p` in memory and retries later.
 */
export function saveProgress(storage: StorageLike, p: Progress, protectedRaw: string | null, today: Day): Saved {
  const raw = JSON.stringify(p);
  let backedUp = false;
  if (protectedRaw !== null) {
    backedUp = parkRaw(storage, protectedRaw, today, 'unreadable') !== null;
    if (!backedUp) return { raw, backedUp: false, written: false, skipped: true };
  }
  return { raw, backedUp, written: safeSet(storage, KEY_V2, raw), skipped: false };
}
