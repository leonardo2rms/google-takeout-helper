import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import {
  BACKUP_EVERY_DAYS, BACKUP_MIN_PRACTICE_DAYS, BACKUP_SNOOZE_DAYS, EMPTY_BACKUP_NOTE, addDays, daysSinceBackup, emptyProgress, noteBackupMade, noteBackupSnoozed, readBackupNote,
  serializeBackupNote, shouldNudgeBackup, type BackupNote,
} from './index';

const today = '2026-11-04';
const practised = (practiceDays: number) => ({ stats: { ...emptyProgress().stats, practiceDays } });
const note = (over: Partial<BackupNote> = {}): BackupNote => ({ ...EMPTY_BACKUP_NOTE, ...over });

describe('reading the note', () => {
  it('round-trips', () => {
    const n = note({ lastBackup: '2026-10-01', snoozedUntil: '2026-11-08' });
    expect(readBackupNote(serializeBackupNote(n))).toEqual(n);
  });
  it('treats anything unreadable as "no backup, not snoozed"', () => {
    for (const raw of [null, '', 'not json', '42', 'null', '[]', '{"lastBackup":"yesterday","snoozedUntil":7}', '{"lastBackup":"2026-02-30"}']) {
      expect(readBackupNote(raw)).toEqual(EMPTY_BACKUP_NOTE);
    }
  });
  it('keeps a valid field when the other is junk', () => {
    expect(readBackupNote('{"lastBackup":"2026-10-01","snoozedUntil":"soon"}')).toEqual({ lastBackup: '2026-10-01', snoozedUntil: null });
  });
});

describe('updating the note', () => {
  it('a backup clears a snooze and stamps the day', () => {
    expect(noteBackupMade(today)).toEqual({ lastBackup: today, snoozedUntil: null });
  });
  it('"Later" keeps the last backup and waits a week', () => {
    expect(noteBackupSnoozed(note({ lastBackup: '2026-10-01' }), today)).toEqual({ lastBackup: '2026-10-01', snoozedUntil: addDays(today, BACKUP_SNOOZE_DAYS) });
  });
  it('counts whole days since the backup; none is null; a future date (wrong clock) is today', () => {
    expect(daysSinceBackup(note(), today)).toBeNull();
    expect(daysSinceBackup(note({ lastBackup: today }), today)).toBe(0);
    expect(daysSinceBackup(note({ lastBackup: addDays(today, -9) }), today)).toBe(9);
    expect(daysSinceBackup(note({ lastBackup: addDays(today, 5) }), today)).toBe(0);
  });
});

describe('when to nudge', () => {
  it('never for a newcomer, however long ago', () => {
    expect(shouldNudgeBackup(practised(0), note(), today)).toBe(false);
    expect(shouldNudgeBackup(practised(BACKUP_MIN_PRACTICE_DAYS - 1), note(), today)).toBe(false);
  });
  it('once there is something to lose and no backup was ever made', () => {
    expect(shouldNudgeBackup(practised(BACKUP_MIN_PRACTICE_DAYS), note(), today)).toBe(true);
  });
  it('not while the last backup is recent; again once it is BACKUP_EVERY_DAYS old', () => {
    const p = practised(30);
    expect(shouldNudgeBackup(p, note({ lastBackup: addDays(today, -(BACKUP_EVERY_DAYS - 1)) }), today)).toBe(false);
    expect(shouldNudgeBackup(p, note({ lastBackup: addDays(today, -BACKUP_EVERY_DAYS) }), today)).toBe(true);
    expect(shouldNudgeBackup(p, note({ lastBackup: today }), today)).toBe(false);
  });
  it('"Later" holds it back until the snooze day, then it returns', () => {
    const p = practised(30);
    const snoozed = noteBackupSnoozed(note(), today);
    expect(shouldNudgeBackup(p, snoozed, today)).toBe(false);
    expect(shouldNudgeBackup(p, snoozed, addDays(today, BACKUP_SNOOZE_DAYS - 1))).toBe(false);
    expect(shouldNudgeBackup(p, snoozed, addDays(today, BACKUP_SNOOZE_DAYS))).toBe(true);
  });
  it('property: a backup made today always silences it, whatever the snooze or history', () => {
    fc.assert(
      fc.property(fc.integer({ min: 0, max: 500 }), fc.integer({ min: -60, max: 60 }), (days, offset) => {
        const n = { ...noteBackupMade(today), snoozedUntil: addDays(today, offset) };
        expect(shouldNudgeBackup(practised(days), n, today)).toBe(false);
      }),
    );
  });
});
