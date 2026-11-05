import type { Day } from './day';
import { fnv1a } from './hash';
import { mergeExtras } from './extras';
import { migrateProgress, normalizeProgress, pruneProgress, type MigrationContext } from './migrate';
import type { Card, DayLog, LessonRecord, Progress } from './progress';

export const BACKUP_APP = 'interview-trainer';
/** Refuse absurdly large files before parsing them. */
export const MAX_BACKUP_BYTES = 5_000_000;

export interface BackupFile {
  app: typeof BACKUP_APP;
  /** Version of the Progress format inside. */
  format: 2;
  exportedAt: string;
  /** fnv1a of JSON.stringify(progress): catches truncated or hand-edited files. Not a security feature. */
  checksum: string;
  progress: Progress;
}

const checksumOf = (p: unknown): string => fnv1a(JSON.stringify(p)).toString(16);

/** The text of a backup file. Pure: pass `now` in. */
export function exportProgress(p: Progress, now: Date): string {
  const file: BackupFile = { app: BACKUP_APP, format: 2, exportedAt: now.toISOString(), checksum: checksumOf(p), progress: p };
  return JSON.stringify(file, null, 2);
}

export const backupFileName = (day: Day): string => `interview-trainer-progress-${day}.json`;

export type ImportResult =
  | { ok: true; progress: Progress; exportedAt: string | null; from: 1 | 2 }
  | { ok: false; error: string };

/**
 * Parses a backup (our envelope, or a bare Progress blob of v1 or v2), validates and migrates it.
 * Never throws; never touches storage. The caller decides whether to replace or merge.
 */
export function importProgress(text: string, ctx: MigrationContext): ImportResult {
  if (typeof text !== 'string' || text.trim() === '') return { ok: false, error: 'The file is empty.' };
  if (text.length > MAX_BACKUP_BYTES) return { ok: false, error: 'The file is too large to be a progress backup.' };
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { ok: false, error: 'That is not a valid backup file (it is not JSON).' };
  }
  let payload: unknown = parsed;
  let exportedAt: string | null = null;
  if (typeof parsed === 'object' && parsed !== null && (parsed as { app?: unknown }).app === BACKUP_APP) {
    const env = parsed as Partial<BackupFile>;
    if (env.progress === undefined) return { ok: false, error: 'The backup has no progress in it.' };
    if (typeof env.checksum === 'string' && env.checksum !== checksumOf(env.progress)) {
      return { ok: false, error: 'The backup looks damaged or was edited (checksum mismatch). Nothing was changed.' };
    }
    payload = env.progress;
    exportedAt = typeof env.exportedAt === 'string' ? env.exportedAt : null;
  }
  const res = migrateProgress(payload, ctx);
  if (res.problem === 'future-version') return { ok: false, error: 'This backup was made by a newer version of the app. Update the app first.' };
  if (res.problem || res.from === 0) return { ok: false, error: 'That does not look like Interview Trainer progress.' };
  return { ok: true, progress: res.progress, exportedAt, from: res.from };
}

const later = (a: string | null, b: string | null): string | null => (a === null ? b : b === null ? a : a >= b ? a : b);
const maxRecord = <T>(a: Record<string, T>, b: Record<string, T>, pick: (x: T, y: T) => T): Record<string, T> => {
  const out: Record<string, T> = { ...a };
  for (const [k, v] of Object.entries(b)) out[k] = Object.hasOwn(out, k) ? pick(out[k]!, v) : v;
  return out;
};

/**
 * Merge for "I practised on two devices": nothing is lost. XP, counters and best accuracy take the larger
 * value, the streak comes from whichever side practised most recently, each card comes from the side that saw it
 * last. Settings and the daily goal stay as they are on the device you are importing into.
 */
export function mergeProgress(local: Progress, incoming: Progress, ctx: MigrationContext): Progress {
  const newer = (incoming.lastDay ?? '') > (local.lastDay ?? '') || ((incoming.lastDay ?? '') === (local.lastDay ?? '') && incoming.streak > local.streak) ? incoming : local;
  const merged: Progress = {
    ...local,
    xp: Math.max(local.xp, incoming.xp),
    streak: newer.streak,
    lastDay: later(local.lastDay, incoming.lastDay),
    freezes: newer.freezes,
    longestStreak: Math.max(local.longestStreak, incoming.longestStreak),
    lessons: maxRecord<LessonRecord>(local.lessons, incoming.lessons, (x, y) => ({
      bestAccuracy: Math.max(x.bestAccuracy, y.bestAccuracy),
      completions: Math.max(x.completions, y.completions),
      lastDay: later(x.lastDay, y.lastDay)!,
    })),
    missed: [...new Set([...local.missed, ...incoming.missed])],
    cards: maxRecord<Card>(local.cards, incoming.cards, (x, y) => (y.last > x.last || (y.last === x.last && y.seen > x.seen) ? y : x)),
    days: maxRecord<DayLog>(local.days, incoming.days, (x, y) => {
      const big = y.answered > x.answered ? y : x;
      return { ...big, xp: Math.max(x.xp, y.xp), answered: Math.max(x.answered, y.answered), firstTry: Math.max(x.firstTry, y.firstTry) };
    }),
    profile: {
      ...local.profile,
      onboarded: local.profile.onboarded || incoming.profile.onboarded,
      role: local.profile.role ?? incoming.profile.role,
      interviewDate: local.profile.interviewDate ?? incoming.profile.interviewDate,
      restDays: local.profile.restDays.length ? local.profile.restDays : incoming.profile.restDays,
      practiceTime: local.profile.practiceTime ?? incoming.profile.practiceTime,
    },
    stats: {
      sessions: Math.max(local.stats.sessions, incoming.stats.sessions),
      answered: Math.max(local.stats.answered, incoming.stats.answered),
      firstTry: Math.max(local.stats.firstTry, incoming.stats.firstTry),
      perfect: Math.max(local.stats.perfect, incoming.stats.perfect),
      practiceDays: Math.max(local.stats.practiceDays, incoming.stats.practiceDays),
    },
    milestones: maxRecord<Day>(local.milestones, incoming.milestones, (x, y) => (x <= y ? x : y)),
    ...mergeExtras(local, incoming),
  };
  return pruneProgress(normalizeProgress(merged, ctx), ctx);
}

export interface BackupSummary {
  xp: number;
  streak: number;
  lessons: number;
  cards: number;
  lastDay: Day | null;
}

export const summarizeProgress = (p: Progress): BackupSummary => ({
  xp: p.xp,
  streak: p.streak,
  lessons: Object.keys(p.lessons).length,
  cards: Object.keys(p.cards).length,
  lastDay: p.lastDay,
});
