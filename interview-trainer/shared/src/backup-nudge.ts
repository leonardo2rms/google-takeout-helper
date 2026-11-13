import { addDays, daysBetween, isDay, type Day } from './day';
import type { Progress } from './progress';

/**
 * When to remind the learner that their progress lives on this device only. Progress is local (no accounts), so a cleared
 * browser or a lost phone loses everything; a backup file is the only protection, and people do not think of it until it is too late.
 *
 * Kept apart from `Progress` (its own storage key, like the saved session): it is a note about this device's last backup, not part
 * of the profile, so it is not exported, merged or migrated, and a restored backup cannot overwrite it with an older date.
 * The nudge is one quiet line that can be put off, never a gate, and it only appears for someone with something worth protecting.
 */

export const BACKUP_KEY = 'interview-trainer.backup.v1';
/** A backup older than this (or none at all) is worth a reminder. */
export const BACKUP_EVERY_DAYS = 14;
/** "Later" puts the reminder off for this long. */
export const BACKUP_SNOOZE_DAYS = 7;
/** Days practised before there is enough to lose that a reminder is fair: a newcomer is never nagged. */
export const BACKUP_MIN_PRACTICE_DAYS = 5;

export interface BackupNote {
  /** The day a backup was last downloaded or copied from this device. */
  lastBackup: Day | null;
  /** No reminder before this day ("Later"). */
  snoozedUntil: Day | null;
}

export const EMPTY_BACKUP_NOTE: BackupNote = { lastBackup: null, snoozedUntil: null };

/** Reads the stored note; anything unreadable is "no backup yet, not snoozed", never an error. */
export function readBackupNote(raw: string | null): BackupNote {
  if (raw === null) return EMPTY_BACKUP_NOTE;
  try {
    const o: unknown = JSON.parse(raw);
    if (typeof o !== 'object' || o === null) return EMPTY_BACKUP_NOTE;
    const r = o as Record<string, unknown>;
    return { lastBackup: isDay(r.lastBackup) ? r.lastBackup : null, snoozedUntil: isDay(r.snoozedUntil) ? r.snoozedUntil : null };
  } catch {
    return EMPTY_BACKUP_NOTE;
  }
}

export const serializeBackupNote = (n: BackupNote): string => JSON.stringify({ v: 1, ...n });

export const noteBackupMade = (today: Day): BackupNote => ({ lastBackup: today, snoozedUntil: null });
export const noteBackupSnoozed = (n: BackupNote, today: Day): BackupNote => ({ ...n, snoozedUntil: addDays(today, BACKUP_SNOOZE_DAYS) });

/** Whole days since the last backup, or null if there is none (a date in the future, from a wrong clock, counts as today). */
export function daysSinceBackup(n: BackupNote, today: Day): number | null {
  return n.lastBackup === null ? null : Math.max(0, daysBetween(n.lastBackup, today));
}

/** Whether to show the reminder today. */
export function shouldNudgeBackup(p: Pick<Progress, 'stats'>, n: BackupNote, today: Day): boolean {
  if (p.stats.practiceDays < BACKUP_MIN_PRACTICE_DAYS) return false;
  if (n.snoozedUntil !== null && today < n.snoozedUntil) return false;
  const since = daysSinceBackup(n, today);
  return since === null || since >= BACKUP_EVERY_DAYS;
}
