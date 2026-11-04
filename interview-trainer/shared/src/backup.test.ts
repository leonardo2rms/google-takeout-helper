import { describe, expect, it } from 'vitest';
import {
  allQuestions, backupFileName, backupKey, BACKUP_APP, emptyProgress, exportProgress, importProgress, KEY_V1, KEY_V2, loadProgress, mergeProgress,
  saveProgress, summarizeProgress, type MigrationContext, type Progress, type StorageLike,
} from './index';
import { card, mkCourse } from './fixtures';

const today = '2026-11-04';
const course = mkCourse({ k: { 'k-1': 4 } });
const questions = allQuestions(course);
const ctx: MigrationContext = { today, questions };
const now = new Date('2026-11-04T10:00:00Z');

const base: Progress = {
  ...emptyProgress(), xp: 200, streak: 3, longestStreak: 5, lastDay: today, freezes: 1,
  lessons: { 'k-1': { bestAccuracy: 0.8, completions: 2, lastDay: today } },
  cards: { 'k-1/1': card(questions[0]!, { last: '2026-11-01', seen: 2 }) },
  days: { [today]: { xp: 50, answered: 5, firstTry: 4, target: 8 } },
  stats: { sessions: 3, answered: 12, firstTry: 9, perfect: 1, practiceDays: 3 },
  milestones: { 'rank:0': '2026-10-01' },
};

describe('export / import', () => {
  it('round-trips exactly', () => {
    const text = exportProgress(base, now);
    const file = JSON.parse(text);
    expect(file).toMatchObject({ app: BACKUP_APP, format: 2, exportedAt: now.toISOString() });
    const r = importProgress(text, ctx);
    expect(r).toMatchObject({ ok: true, exportedAt: now.toISOString(), from: 2 });
    if (r.ok) expect(r.progress).toEqual(base);
  });

  it('accepts a bare v2 or v1 blob', () => {
    expect(importProgress(JSON.stringify(base), ctx)).toMatchObject({ ok: true, from: 2, exportedAt: null });
    const v1 = importProgress(JSON.stringify({ version: 1, xp: 5, streak: 1, lastDay: today, lessons: {}, missed: [] }), ctx);
    expect(v1).toMatchObject({ ok: true, from: 1 });
  });

  it('rejects garbage with a clear error and never throws', () => {
    for (const bad of ['', '   ', 'not json', '[]', '{}', '42', 'null', '{"app":"interview-trainer"}', '{"version":2,"xp":"x"', JSON.stringify({ app: 'other', version: 9 })]) {
      const r = importProgress(bad, ctx);
      expect(r.ok, bad).toBe(false);
      if (!r.ok) expect(r.error.length).toBeGreaterThan(10);
    }
    expect(importProgress(JSON.stringify({ version: 3 }), ctx)).toMatchObject({ ok: false, error: expect.stringContaining('newer version') });
    expect(importProgress('x'.repeat(5_000_001), ctx)).toMatchObject({ ok: false });
    expect(importProgress(undefined as never, ctx)).toMatchObject({ ok: false });
  });

  it('detects a damaged or edited file by checksum', () => {
    const text = exportProgress(base, now).replace('"xp": 200', '"xp": 999999');
    expect(importProgress(text, ctx)).toMatchObject({ ok: false, error: expect.stringContaining('checksum') });
    const noChecksum = JSON.parse(exportProgress(base, now));
    delete noChecksum.checksum;
    expect(importProgress(JSON.stringify(noChecksum), ctx).ok).toBe(true);
  });

  it('names the file by day and summarises what is inside', () => {
    expect(backupFileName(today)).toBe('interview-trainer-progress-2026-11-04.json');
    expect(summarizeProgress(base)).toEqual({ xp: 200, streak: 3, lessons: 1, cards: 1, lastDay: today });
  });
});

describe('mergeProgress', () => {
  const other: Progress = {
    ...emptyProgress(), xp: 350, streak: 9, longestStreak: 9, lastDay: '2026-11-02', freezes: 2,
    lessons: { 'k-1': { bestAccuracy: 1, completions: 1, lastDay: '2026-11-02' }, 'j-1': { bestAccuracy: 0.5, completions: 1, lastDay: '2026-11-02' } },
    missed: ['k-1/2'],
    cards: { 'k-1/1': card(questions[0]!, { last: '2026-11-03', seen: 5, interval: 9 }), 'k-1/2': card(questions[1]!, { last: '2026-11-02' }) },
    days: { [today]: { xp: 80, answered: 3, firstTry: 3, target: 8 }, '2026-11-02': { xp: 10, answered: 1, firstTry: 1, target: 8 } },
    stats: { sessions: 9, answered: 4, firstTry: 4, perfect: 0, practiceDays: 8 },
    milestones: { 'rank:0': '2026-09-01', 'streak:7': '2026-11-02' },
    profile: { ...emptyProgress().profile, role: 'generalist', interviewDate: '2027-01-01' },
  };
  const m = mergeProgress(base, other, ctx);

  it('keeps the larger counters and best accuracy', () => {
    expect(m.xp).toBe(350);
    expect(m.longestStreak).toBe(9);
    expect(m.lessons['k-1']).toEqual({ bestAccuracy: 1, completions: 2, lastDay: today });
    expect(m.lessons['j-1']).toBeDefined();
    expect(m.stats).toEqual({ sessions: 9, answered: 12, firstTry: 9, perfect: 1, practiceDays: 8 });
  });
  it('takes the streak from whichever side practised most recently', () => {
    expect(m).toMatchObject({ streak: 3, freezes: 1, lastDay: today });
    expect(mergeProgress(other, base, ctx)).toMatchObject({ streak: 3, freezes: 1, lastDay: today });
  });
  it('each card comes from the side that saw it last; unions the rest', () => {
    expect(m.cards['k-1/1']).toMatchObject({ seen: 5, interval: 9 });
    expect(m.cards['k-1/2']).toBeDefined();
    expect(m.missed).toEqual(['k-1/2']);
    expect(m.days[today]).toEqual({ xp: 80, answered: 5, firstTry: 4, target: 8 });
    expect(m.days['2026-11-02']).toBeDefined();
    expect(m.milestones).toEqual({ 'rank:0': '2026-09-01', 'streak:7': '2026-11-02' });
  });
  it('keeps this device\'s settings but fills blanks from the backup', () => {
    expect(m.profile.role).toBe('generalist');
    expect(m.profile.interviewDate).toBe('2027-01-01');
    expect(m.goal).toEqual(base.goal);
  });
  it('is commutative on the numbers that matter, and merging with itself changes nothing', () => {
    expect(mergeProgress(other, base, ctx).xp).toBe(m.xp);
    expect(mergeProgress(base, base, ctx)).toEqual(base);
  });
  it('when both practised the same day the longer streak wins', () => {
    const a: Progress = { ...base, streak: 2, lastDay: today };
    const b: Progress = { ...base, streak: 6, lastDay: today };
    expect(mergeProgress(a, b, ctx).streak).toBe(6);
  });
});

function memory(initial: Record<string, string> = {}): StorageLike & { data: Record<string, string> } {
  const data = { ...initial };
  return { data, getItem: (k) => data[k] ?? null, setItem: (k, v) => void (data[k] = v) };
}
const throwing: StorageLike = { getItem: () => { throw new Error('denied'); }, setItem: () => { throw new Error('quota'); } };

describe('storage load/save', () => {
  it('empty storage -> empty progress, nothing written yet', () => {
    const s = memory();
    const l = loadProgress(s, ctx);
    expect(l).toMatchObject({ from: 0, raw: null, protectedRaw: null });
    expect(l.progress).toEqual(emptyProgress());
    expect(s.data).toEqual({});
  });

  it('migrates v1 -> v2, writes v2 and never deletes or alters v1', () => {
    const v1 = JSON.stringify({ version: 1, xp: 90, streak: 2, lastDay: '2026-11-03', lessons: {}, missed: [] });
    const s = memory({ [KEY_V1]: v1 });
    const l = loadProgress(s, ctx);
    expect(l.from).toBe(1);
    expect(l.progress.xp).toBe(90);
    expect(JSON.parse(s.data[KEY_V2]!)).toMatchObject({ version: 2, xp: 90 });
    expect(s.data[KEY_V1]).toBe(v1);
    // second load reads v2 and does not rewrite
    const again = loadProgress(s, ctx);
    expect(again).toMatchObject({ from: 2, raw: s.data[KEY_V2] });
  });

  it('v2 wins over v1 when both exist', () => {
    const s = memory({ [KEY_V1]: JSON.stringify({ version: 1, xp: 1, streak: 0, lastDay: null, lessons: {}, missed: [] }), [KEY_V2]: JSON.stringify({ ...base }) });
    expect(loadProgress(s, ctx).progress.xp).toBe(200);
  });

  it('completes an older v2 blob and writes it back', () => {
    const s = memory({ [KEY_V2]: JSON.stringify({ version: 2, xp: 3, streak: 0, lastDay: null, lessons: {}, missed: [] }) });
    const l = loadProgress(s, ctx);
    expect(l.progress.settings.tips).toBe(true);
    expect(JSON.parse(s.data[KEY_V2]!).settings).toBeDefined();
  });

  it('a corrupt or newer v2 value is protected: not overwritten until it has been copied to a backup', () => {
    const future = JSON.stringify({ version: 3, xp: 999 });
    const s = memory({ [KEY_V2]: future });
    const l = loadProgress(s, ctx);
    expect(l.progress).toEqual(emptyProgress());
    expect(l.protectedRaw).toBe(future);
    expect(s.data[KEY_V2]).toBe(future); // untouched by load
    const saved = saveProgress(s, { ...emptyProgress(), xp: 5 }, l.protectedRaw, today);
    expect(saved.backedUp).toBe(true);
    expect(s.data[backupKey(today)]).toBe(future);
    expect(backupKey(today)).toBe('interview-trainer.progress.backup-20261104');
    expect(JSON.parse(s.data[KEY_V2]!).xp).toBe(5);
  });

  it('a corrupt v1 blob alone does not block a fresh start', () => {
    const l = loadProgress(memory({ [KEY_V1]: '{oops' }), ctx);
    expect(l).toMatchObject({ from: 0, protectedRaw: null });
  });

  it('unavailable storage (private mode, quota) never throws', () => {
    expect(loadProgress(throwing, ctx).progress).toEqual(emptyProgress());
    expect(saveProgress(throwing, base, 'something', today)).toMatchObject({ backedUp: false });
  });

  it('saveProgress without protection just writes', () => {
    const s = memory();
    const r = saveProgress(s, base, null, today);
    expect(r.backedUp).toBe(false);
    expect(s.data[KEY_V2]).toBe(r.raw);
    expect(Object.keys(s.data)).toEqual([KEY_V2]);
  });
});
