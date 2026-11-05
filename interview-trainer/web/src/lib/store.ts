import { useCallback, useSyncExternalStore } from 'react';
import course from 'virtual:course';
import {
  KEY_V2, allQuestions, backfillAchievements, dayKey, emptyProgress, exportProgress, loadProgress, mergeProgress, saveProgress, silentUnlock,
  unlockAchievements, type Progress, type StorageLike,
} from '@trainer/shared';
import { pushBadgeToast } from './toasts';

/**
 * The single source of truth for progress, shared by every component and kept in sync across tabs.
 * Reads and writes go through the rules in shared/storage.ts (v2 key, v1 migration, backup-before-overwrite).
 */

const questions = allQuestions(course);
const migrationCtx = () => ({ today: dayKey(new Date()), questions });

const memory = new Map<string, string>();
const memoryStorage: StorageLike = { getItem: (k) => memory.get(k) ?? null, setItem: (k, v) => void memory.set(k, v) };

/** localStorage when it works (it can throw in private windows / sandboxed frames), otherwise memory for this tab only. */
function getStorage(): StorageLike {
  try {
    const s = window.localStorage;
    s.getItem(KEY_V2);
    return s;
  } catch {
    return memoryStorage;
  }
}

let current: Progress | null = null;
let lastRaw: string | null = null;
let protectedRaw: string | null = null;
const listeners = new Set<() => void>();
const emit = () => listeners.forEach((l) => l());

function adopt(l: { progress: Progress; raw: string | null; protectedRaw: string | null }) {
  current = l.progress;
  lastRaw = l.raw;
  protectedRaw = l.protectedRaw;
}

function ensure(): Progress {
  if (current === null) adopt(loadProgress(getStorage(), migrationCtx()));
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
  adopt(loadProgress(getStorage(), migrationCtx()));
  return true;
}

function commit(next: Progress) {
  current = next;
  const saved = saveProgress(getStorage(), next, protectedRaw, dayKey(new Date()));
  lastRaw = saved.raw;
  protectedRaw = null;
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
}

const subscribe = (l: () => void) => {
  listeners.add(l);
  return () => void listeners.delete(l);
};

export function useProgress() {
  const progress = useSyncExternalStore(subscribe, ensure, ensure);
  const update = useCallback((fn: (p: Progress) => Progress) => updateProgress(fn), []);
  const reset = useCallback(() => updateProgress(() => emptyProgress()), []);
  return { progress, update, reset };
}

// ---------- backup ----------

export const exportText = (): string => exportProgress(ensure(), new Date());
export const replaceProgress = (p: Progress): void => updateProgress(() => p, { silent: true });
export const mergeIntoProgress = (incoming: Progress): void => updateProgress((cur) => mergeProgress(cur, incoming, migrationCtx()), { silent: true });
export const importContext = migrationCtx;

// ---------- keeping the data safe ----------

let persistAsked = false;
/** Asks the browser not to evict our storage (Safari/ITP otherwise clears script-written data after ~7 idle days). Best effort, once. */
export function requestPersistence(): void {
  if (persistAsked) return;
  persistAsked = true;
  try {
    void navigator.storage?.persist?.();
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
