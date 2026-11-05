import { useCallback, useSyncExternalStore } from 'react';
import course from 'virtual:course';
import {
  KEY_V2, allQuestions, backfillAchievements, dayKey, emptyProgress, exportProgress, listParked, loadProgress, mergeProgress, parkRaw, saveProgress,
  silentUnlock, unlockAchievements, type ParkedCopy, type Progress, type StorageLike,
} from '@trainer/shared';
import { pushBadgeToast } from './toasts';

/**
 * The single source of truth for progress, shared by every component and kept in sync across tabs.
 * Reads and writes go through the rules in shared/storage.ts (v2 key, v1 migration, backup-before-overwrite).
 */

const questions = allQuestions(course);
const migrationCtx = () => ({ today: dayKey(new Date()), questions });

const memory = new Map<string, string>();
const memoryStorage: StorageLike = {
  getItem: (k) => memory.get(k) ?? null,
  setItem: (k, v) => void memory.set(k, v),
  removeItem: (k) => void memory.delete(k),
  get length() {
    return memory.size;
  },
  key: (i) => [...memory.keys()][i] ?? null,
};

let usingMemory = false;
/** localStorage when it works (it can throw in private windows / sandboxed frames), otherwise memory for this tab only. */
function getStorage(): StorageLike {
  try {
    const s = window.localStorage;
    s.getItem(KEY_V2);
    usingMemory = false;
    return s;
  } catch {
    usingMemory = true;
    return memoryStorage;
  }
}

let current: Progress | null = null;
/** The value KEY_V2 held when we last read it or successfully wrote it (never a value we failed to write). */
let lastRaw: string | null = null;
/** An unreadable value sitting in KEY_V2: it is not overwritten until a copy of it has been written. */
let protectedRaw: string | null = null;
/** True while `current` has changes that are not in storage (a failed write). They stay in memory and are retried. */
let dirty = false;
/** True once this tab changed anything: before that there is nothing to lose, so no warning is due. */
let committed = false;
const listeners = new Set<() => void>();
const emit = () => listeners.forEach((l) => l());

function adopt(l: { progress: Progress; raw: string | null; protectedRaw: string | null }) {
  current = l.progress;
  lastRaw = l.raw;
  protectedRaw = l.protectedRaw;
}

// ---------- safety status (what the UI may tell the learner) ----------

export interface SafetyStatus {
  /** 'ok'; 'memory' = the browser blocks storage here, progress lives in this tab only; 'failing' = writes are being refused (quota). */
  persistence: 'ok' | 'memory' | 'failing';
  /** Progress that exists only in memory right now and would be lost on reload. False until the learner has progress to lose. */
  atRisk: boolean;
  /** Unreadable data (corrupt / newer version) that has been set aside and not yet acknowledged. Keys of the parked copies. */
  recovered: string[];
  /** Unreadable data is still sitting in the main key because its copy could not be written. */
  unreadablePending: boolean;
}

const ACK_KEY = 'interview-trainer.ui.recovered-ack';
let parkedKeys: string[] = [];
let status: SafetyStatus = { persistence: 'ok', atRisk: false, recovered: [], unreadablePending: false };

function readAck(): string {
  try {
    return window.localStorage.getItem(ACK_KEY) ?? '';
  } catch {
    return '';
  }
}
let ackedMemory = '';

function refreshParked() {
  parkedKeys = listParked(getStorage()).filter((c) => c.kind === 'unreadable').map((c) => c.key);
}

function refreshStatus() {
  const persistence: SafetyStatus['persistence'] = usingMemory ? 'memory' : dirty ? 'failing' : 'ok';
  const ack = ackedMemory > readAck() ? ackedMemory : readAck();
  const next: SafetyStatus = {
    persistence,
    atRisk: committed && (usingMemory || dirty),
    recovered: parkedKeys.filter((k) => k > ack),
    unreadablePending: protectedRaw !== null,
  };
  if (next.persistence !== status.persistence || next.atRisk !== status.atRisk || next.unreadablePending !== status.unreadablePending || next.recovered.join() !== status.recovered.join()) status = next;
}

/** The learner has seen the notice about parked data: stop showing the banner for the copies that exist now. */
export function acknowledgeRecovered(): void {
  ackedMemory = parkedKeys.reduce((a, k) => (k > a ? k : a), ackedMemory);
  try {
    window.localStorage.setItem(ACK_KEY, ackedMemory);
  } catch {
    /* kept in memory for this tab */
  }
  refreshStatus();
  emit();
}

export const getSafetyStatus = (): SafetyStatus => {
  ensure();
  return status;
};

let retryTimer: ReturnType<typeof setTimeout> | undefined;
let retryDelay = 4000;

function scheduleRetry() {
  if (retryTimer !== undefined || typeof window === 'undefined') return;
  retryTimer = setTimeout(() => {
    retryTimer = undefined;
    retrySave();
  }, retryDelay);
  retryDelay = Math.min(retryDelay * 2, 60_000);
}

/** Tries again to write whatever is only in memory. Safe to call at any time (the page calls it when it is hidden). */
export function retrySave(): void {
  if (current === null || !dirty) return;
  commit(current);
}

function ensure(): Progress {
  if (current === null) {
    adopt(loadProgress(getStorage(), migrationCtx()));
    refreshParked();
    refreshStatus();
  }
  return current!;
}

/** Re-reads storage if another tab wrote since we last looked. Returns true if our copy changed. */
function syncFromStorage(): boolean {
  let raw: string | null;
  try {
    raw = getStorage().getItem(KEY_V2);
  } catch {
    return false;
  }
  if (current !== null && raw === lastRaw) return false;
  const unsaved = dirty && current !== null ? current : null;
  adopt(loadProgress(getStorage(), migrationCtx()));
  // Changes this tab could not save are not thrown away because another tab wrote: they are merged on top.
  if (unsaved) current = mergeProgress(current!, unsaved, migrationCtx());
  refreshStatus();
  return true;
}

function commit(next: Progress) {
  current = next;
  committed = true;
  const saved = saveProgress(getStorage(), next, protectedRaw, dayKey(new Date()));
  if (saved.backedUp) {
    protectedRaw = null;
    refreshParked();
  }
  if (saved.written) {
    lastRaw = saved.raw;
    dirty = false;
    retryDelay = 4000;
    if (retryTimer !== undefined) clearTimeout(retryTimer);
    retryTimer = undefined;
  } else {
    // Keep `lastRaw` as what storage really holds, so the next read does not mistake our own unsaved state for stale data.
    dirty = true;
    scheduleRetry();
  }
  refreshStatus();
  emit();
}

const badgeCtx = () => ({ course, today: dayKey(new Date()) });

/**
 * Applies `fn` to the latest progress (including writes from other tabs) and saves the result. `fn` runs synchronously.
 * Badges are evaluated after every write, so no screen has to remember to; a new one raises a quiet toast, unless the
 * write is `silent` (an import or the first-time backfill: what the data already earned is not "just" achieved).
 */
export function updateProgress(fn: (p: Progress) => Progress, opts: { silent?: boolean } = {}): void {
  ensure();
  const changed = syncFromStorage();
  let next = fn(current!);
  if (next === current) {
    if (changed) emit();
    return;
  }
  if (opts.silent) next = silentUnlock(next, badgeCtx());
  else {
    const res = unlockAchievements(next, badgeCtx());
    next = res.progress;
    pushBadgeToast(res.unlocked);
  }
  commit(next);
}

/** Once per profile: records the badges an older profile has already earned, without any celebration. */
export const backfillBadges = (): void => updateProgress((p) => backfillAchievements(p, badgeCtx()), { silent: true });

export const getProgress = (): Progress => ensure();

if (typeof window !== 'undefined') {
  window.addEventListener('storage', (e) => {
    if ((e.key === KEY_V2 || e.key === null) && current !== null && syncFromStorage()) emit();
  });
  // Last chance to save what only lives in memory: when the page is hidden or closed, and when the browser comes back online.
  for (const type of ['pagehide', 'visibilitychange', 'online']) window.addEventListener(type, () => retrySave());
}

const subscribe = (l: () => void) => {
  listeners.add(l);
  return () => void listeners.delete(l);
};

/** Keeps a copy of the profile as it is now, before an action replaces it. Skipped for a profile with nothing in it. */
function keepSafetyCopy(): void {
  const p = ensure();
  if (p.xp === 0 && p.stats.sessions === 0 && Object.keys(p.cards).length === 0) return;
  parkRaw(getStorage(), JSON.stringify(p), dayKey(new Date()), 'safety');
}

function resetProgress(): void {
  keepSafetyCopy();
  updateProgress(() => emptyProgress());
}

export function useProgress() {
  const progress = useSyncExternalStore(subscribe, ensure, ensure);
  const update = useCallback((fn: (p: Progress) => Progress) => updateProgress(fn), []);
  return { progress, update, reset: resetProgress };
}

/** What the UI may tell the learner about whether their progress is being kept (see `SafetyStatus`). */
export function useSafetyStatus(): SafetyStatus {
  return useSyncExternalStore(subscribe, getSafetyStatus, getSafetyStatus);
}

/** Copies of data set aside (unreadable data, and the profile as it was before a replace or reset), newest first. */
export const listParkedCopies = (): ParkedCopy[] => listParked(getStorage());

// ---------- backup ----------

export const exportText = (): string => exportProgress(ensure(), new Date());
/** Replaces the profile with `p` after keeping a safety copy of the current one. Reading preferences and reminders stay those of this device. */
export function replaceProgress(p: Progress): void {
  keepSafetyCopy();
  updateProgress((cur) => ({ ...p, prefs: cur.prefs }), { silent: true });
}
export const mergeIntoProgress = (incoming: Progress): void => updateProgress((cur) => mergeProgress(cur, incoming, migrationCtx()), { silent: true });
export const importContext = migrationCtx;

// ---------- keeping the data safe ----------

let persistAsked = false;
/** Asks the browser not to evict our storage (Safari/ITP otherwise clears script-written data after ~7 idle days). Best effort, once. */
export function requestPersistence(): void {
  if (persistAsked) return;
  persistAsked = true;
  try {
    // In a sandboxed frame persist() returns a rejected promise; that must not surface as an unhandled rejection.
    void Promise.resolve(navigator.storage?.persist?.()).catch(() => {});
  } catch {
    /* unsupported */
  }
}

const IOS_TIP_KEY = 'interview-trainer.ui.ios-tip-dismissed';

/** True on iPhone/iPad Safari when the app has not been added to the Home Screen (where storage can be evicted). */
export function shouldShowIosTip(): boolean {
  try {
    const ua = navigator.userAgent;
    const ios = /iPhone|iPad|iPod/.test(ua) || (ua.includes('Mac') && navigator.maxTouchPoints > 1);
    const standalone = (navigator as Navigator & { standalone?: boolean }).standalone === true || window.matchMedia('(display-mode: standalone)').matches;
    return ios && !standalone && localStorage.getItem(IOS_TIP_KEY) !== '1';
  } catch {
    return false;
  }
}

export function dismissIosTip(): void {
  try {
    localStorage.setItem(IOS_TIP_KEY, '1');
  } catch {
    /* ignore */
  }
}
