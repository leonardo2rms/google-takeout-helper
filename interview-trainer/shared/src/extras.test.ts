import { describe, expect, it } from 'vitest';
import {
  CALIBRATION_MIN, CHALLENGE_KEPT, MAX_BOOKMARKS, MAX_MOCKS, MAX_NOTES, MAX_NOTE_CHARS, addDays, allQuestions, applySession, calibrationRows, calibrationSentence,
  emptyExtras, emptyProgress, exportProgress, importProgress, mergeExtras, mergeProgress, migrateProgress, normalizeExtras, type MockRecord,
} from './index';
import { mkCourse } from './fixtures';

const today = '2026-11-04';
const course = mkCourse({ k: { 'k-1': 4 } });
const ctx = { today, questions: allQuestions(course) };

const mock = (over: Partial<MockRecord> = {}): MockRecord => ({
  day: today, total: 10, answered: 8, score: 60, limitMin: 15, usedSec: 600, timedOut: false, byUnit: { k: { n: 10, score: 6 } }, ...over,
});

describe('normalizeExtras', () => {
  it('gives defaults for anything missing, and is idempotent', () => {
    expect(normalizeExtras({}, today)).toEqual(emptyExtras());
    const messy = {
      calibration: { n: [3, 2, 1], right: [9, 1, 1] },
      bookmarks: { 'k-1/1': { h: 'abc', day: today } },
      notes: { 'k-1/2': { h: 'abc', text: '  remember this  ', day: today } },
      challenge: { [today]: 'pass' },
      achievements: { 'first-lesson': today },
      mocks: [mock()],
      passedByType: { single: 3 },
    };
    const once = normalizeExtras(messy, today);
    expect(once.calibration).toEqual({ n: [3, 2, 1], right: [3, 1, 1] }); // right never exceeds n
    expect(once.notes['k-1/2']!.text).toBe('remember this');
    expect(normalizeExtras(once as unknown as Record<string, unknown>, today)).toEqual(once);
  });

  it('drops garbage without throwing', () => {
    const e = normalizeExtras(
      {
        calibration: 'x',
        bookmarks: { a: 1, b: { h: '' }, c: { h: 'ok', day: 'not-a-day' }, [`${'x'.repeat(90)}`]: { h: 'ok' } },
        notes: { a: { h: 'h', text: '   ' }, b: { h: 'h', text: 5 }, c: { h: 'h', text: 'y'.repeat(2000) } },
        challenge: { 'bad-day': 'pass', [today]: 'maybe', [addDays(today, -1)]: 'fail', [addDays(today, -CHALLENGE_KEPT - 3)]: 'pass' },
        achievements: { '': today, ok: 'nope' },
        mocks: [null, { day: 'x' }, { day: today, total: 0 }, mock({ answered: 99, score: 500 }), { ...mock(), byUnit: { u: { n: 0, score: 1 }, v: { n: 4, score: 99 }, w: 'x' } }],
        passedByType: { single: -4, multi: 2.9, [`${'t'.repeat(30)}`]: 3 },
      },
      today,
    );
    expect(Object.keys(e.bookmarks)).toEqual(['c']);
    expect(e.bookmarks.c!.day).toBe(today);
    expect(Object.keys(e.notes)).toEqual(['c']);
    expect(e.notes.c!.text).toHaveLength(MAX_NOTE_CHARS);
    expect(e.challenge).toEqual({ [addDays(today, -1)]: 'fail' });
    expect(e.achievements).toEqual({ ok: today });
    expect(e.mocks).toHaveLength(2);
    expect(e.mocks[0]).toMatchObject({ answered: 10, score: 100 });
    expect(e.mocks[1]!.byUnit).toEqual({ v: { n: 4, score: 4 } });
    expect(e.passedByType).toEqual({ multi: 2 });
  });

  it('caps the sizes of the collections', () => {
    const bookmarks = Object.fromEntries(Array.from({ length: MAX_BOOKMARKS + 20 }, (_, i) => [`k-1/${i}`, { h: 'h', day: today }]));
    const notes = Object.fromEntries(Array.from({ length: MAX_NOTES + 20 }, (_, i) => [`k-1/${i}`, { h: 'h', text: 'n', day: today }]));
    const mocks = Array.from({ length: MAX_MOCKS + 5 }, () => mock());
    const e = normalizeExtras({ bookmarks, notes, mocks }, today);
    expect(Object.keys(e.bookmarks)).toHaveLength(MAX_BOOKMARKS);
    expect(Object.keys(e.notes)).toHaveLength(MAX_NOTES);
    expect(e.mocks).toHaveLength(MAX_MOCKS);
  });
});

describe('mergeExtras', () => {
  const base = emptyExtras();
  it('unions bookmarks, takes the larger counters and the better challenge result', () => {
    const a = { ...base, bookmarks: { x: { h: '1', day: today } }, challenge: { [today]: 'fail' as const }, passedByType: { single: 2 }, calibration: { n: [1, 0, 0] as [number, number, number], right: [1, 0, 0] as [number, number, number] } };
    const b = { ...base, bookmarks: { y: { h: '2', day: today } }, challenge: { [today]: 'pass' as const }, passedByType: { single: 5, open: 1 }, calibration: { n: [0, 4, 0] as [number, number, number], right: [0, 2, 0] as [number, number, number] } };
    const m = mergeExtras(a, b);
    expect(Object.keys(m.bookmarks).sort()).toEqual(['x', 'y']);
    expect(m.challenge[today]).toBe('pass');
    expect(m.passedByType).toEqual({ single: 5, open: 1 });
    expect(m.calibration).toEqual({ n: [1, 4, 0], right: [1, 2, 0] });
  });

  it('keeps the newer note, the earliest unlock day and dedupes mocks, newest first', () => {
    const a = { ...base, notes: { q: { h: 'h', text: 'old', day: '2026-11-01' } }, achievements: { first: '2026-11-03' }, mocks: [mock({ day: '2026-11-02' })] };
    const b = { ...base, notes: { q: { h: 'h', text: 'new', day: '2026-11-03' }, z: { h: 'h', text: 'only here', day: today } }, achievements: { first: '2026-11-01', other: today }, mocks: [mock({ day: '2026-11-02' }), mock({ day: today, score: 70 })] };
    const m = mergeExtras(a, b);
    expect(m.notes.q!.text).toBe('new');
    expect(m.notes.z!.text).toBe('only here');
    expect(m.achievements).toEqual({ first: '2026-11-01', other: today });
    expect(m.mocks.map((x) => x.day)).toEqual([today, '2026-11-02']);
  });

  it('keeps the local note when it is as new but the incoming one is not longer', () => {
    const a = { ...base, notes: { q: { h: 'h', text: 'local text', day: today } } };
    const b = { ...base, notes: { q: { h: 'other', text: 'x', day: today } } };
    expect(mergeExtras(a, b).notes.q!.text).toBe('local text');
    const c = { ...base, notes: { q: { h: 'other', text: 'a much longer incoming note', day: today } } };
    expect(mergeExtras(a, c).notes.q!.text).toBe('a much longer incoming note');
  });
});

describe('export, import and merge carry the extras', () => {
  const withExtras = {
    ...emptyProgress(),
    bookmarks: { 'k-1/1': { h: 'h', day: today } },
    notes: { 'k-1/1': { h: 'h', text: 'my note', day: today } },
    challenge: { [today]: 'pass' as const },
    achievements: { 'first-lesson': today },
    mocks: [mock()],
    passedByType: { single: 4 },
    calibration: { n: [1, 2, 3] as [number, number, number], right: [0, 1, 3] as [number, number, number] },
  };

  it('a backup round-trips bookmarks, notes, challenge, badges, mocks and counters', () => {
    const res = importProgress(exportProgress(withExtras, new Date(2026, 10, 4)), ctx);
    expect(res.ok).toBe(true);
    if (res.ok) {
      expect(res.progress.bookmarks).toEqual(withExtras.bookmarks);
      expect(res.progress.notes).toEqual(withExtras.notes);
      expect(res.progress.challenge).toEqual(withExtras.challenge);
      expect(res.progress.achievements).toEqual(withExtras.achievements);
      expect(res.progress.mocks).toEqual(withExtras.mocks);
      expect(res.progress.calibration).toEqual(withExtras.calibration);
    }
  });

  it('an older v2 blob without any of them loads with defaults and migrates idempotently', () => {
    const { bookmarks: _b, notes: _n, challenge: _c, achievements: _a, mocks: _m, passedByType: _p, calibration: _k, ...old } = withExtras;
    const res = migrateProgress(old, ctx);
    expect(res.progress).toMatchObject(emptyExtras());
    expect(migrateProgress(res.progress, ctx).changed).toBe(false);
  });

  it('merging two devices keeps both sides', () => {
    const other = { ...emptyProgress(), bookmarks: { 'k-1/2': { h: 'h', day: today } }, mocks: [mock({ day: '2026-11-01' })] };
    const merged = mergeProgress(withExtras, other, ctx);
    expect(Object.keys(merged.bookmarks).sort()).toEqual(['k-1/1', 'k-1/2']);
    expect(merged.mocks).toHaveLength(2);
    expect(merged.notes['k-1/1']!.text).toBe('my note');
  });
});

describe('calibration', () => {
  const p = (n: [number, number, number], right: [number, number, number]) => ({ calibration: { n, right } });
  it('hides a rate until there are enough ratings', () => {
    expect(calibrationRows(p([5, 9, 9], [1, 1, 1])).map((r) => r.rate)).toEqual([null, null, null]);
    expect(calibrationSentence(p([0, 0, CALIBRATION_MIN - 1], [0, 0, 5]))).toBeNull();
  });
  it('states the sure rate once there is enough data', () => {
    expect(calibrationSentence(p([0, 0, 44], [0, 0, 38]))).toBe('When you said Sure you were right 86% of the time (44 ratings).');
    expect(calibrationRows(p([10, 0, 0], [5, 0, 0]))[0]).toMatchObject({ label: 'Guess', rate: 0.5 });
  });
});

describe('applySession tallies', () => {
  const run = (r: Parameters<typeof applySession>[1]) => applySession(emptyProgress(), r, { today, course }).progress;
  it('counts ratings by outcome and passes by question type', () => {
    const p = run({
      lessonId: 'k-1', accuracy: 0.5, xp: 20, missed: ['k-1/2'], recovered: ['k-1/2'], cleared: ['k-1/1', 'k-1/2', 'k-1/3'],
      confidence: { 'k-1/1': 3, 'k-1/2': 3, 'k-1/3': 1, 'k-1/4': 2 },
    });
    // k-1/4 was not part of the session outcomes, so its rating is ignored
    expect(p.calibration).toEqual({ n: [1, 0, 2], right: [1, 0, 1] });
    expect(p.passedByType).toEqual({ single: 3 });
  });
  it('a single or mock session is never a perfect lesson', () => {
    const r = { lessonId: undefined, accuracy: 1, xp: 20, missed: [], recovered: [], cleared: ['k-1/1'] };
    expect(run({ ...r, kind: 'single' }).stats.perfect).toBe(0);
    expect(run({ ...r, kind: 'mock' }).stats.perfect).toBe(0);
    expect(run(r).stats.perfect).toBe(1);
  });
});
