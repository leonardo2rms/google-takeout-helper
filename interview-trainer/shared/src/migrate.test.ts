import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import {
  addDays, allQuestions, DAYS_KEPT, emptyProgress, fnv1a, MAX_ID_LENGTH, MAX_INTERVAL, MAX_RECORDS, migrateProgress, normalizeProgress, pruneProgress, questionHash,
  rankFor, sameJson, type MigrationContext, type Progress,
} from './index';
import { loadCourse } from './load-content';
import { card } from './fixtures';

const here = dirname(fileURLToPath(import.meta.url));
const course = loadCourse(resolve(here, '../../content'));
const questions = allQuestions(course);
const today = '2026-11-04';
const ctx: MigrationContext = { today, questions };
const v1Text = readFileSync(resolve(here, '__fixtures__/progress-v1.json'), 'utf8');
const v1 = JSON.parse(v1Text) as Record<string, unknown> & { xp: number; streak: number; lastDay: string; lessons: Record<string, { completions: number }>; missed: string[] };

describe('migrateProgress: v1 -> v2', () => {
  const res = migrateProgress(v1, ctx);
  const p = res.progress;

  it('is lossless for the v1 fields', () => {
    expect(res).toMatchObject({ from: 1, changed: true });
    expect(p.version).toBe(2);
    expect(p.xp).toBe(v1.xp);
    expect(p.streak).toBe(v1.streak);
    expect(p.lastDay).toBe(v1.lastDay);
    expect(p.lessons).toEqual(v1.lessons);
    expect(p.missed).toEqual(v1.missed); // even the id that no longer exists in the course
  });

  it('accepts a JSON string as found in localStorage', () => {
    expect(migrateProgress(v1Text, ctx).progress).toEqual(p);
  });

  it('seeds the new fields honestly', () => {
    expect(p.longestStreak).toBe(4);
    expect(p.freezes).toBe(1);
    expect(p.milestones['gift:freeze']).toBe(today);
    expect(p.stats).toEqual({ sessions: 4, answered: 0, firstTry: 0, perfect: 0, practiceDays: 4 });
    expect(p.goal.target).toBe(8);
    expect(p.profile.onboarded).toBe(false);
    expect(p.days).toEqual({});
  });

  it('never celebrates retroactively: ranks up to the current one are pre-marked', () => {
    const idx = rankFor(v1.xp).index;
    for (let i = 0; i <= idx; i++) expect(p.milestones[`rank:${i}`], `rank:${i}`).toBe(today);
    expect(p.milestones[`rank:${idx + 1}`]).toBeUndefined();
  });

  it('seeds cards for completed lessons and missed questions only, spread over the next week', () => {
    const ids = Object.keys(p.cards);
    const doneLessons = Object.keys(v1.lessons);
    const expected = questions.filter((q) => doneLessons.includes(q.id.split('/')[0]!) || v1.missed.includes(q.id));
    expect(ids.sort()).toEqual(expected.map((q) => q.id).sort());
    for (const q of expected) expect(p.cards[q.id]!.h).toBe(questionHash(q));

    for (const id of v1.missed.filter((m) => p.cards[m])) {
      expect(p.cards[id]).toMatchObject({ interval: 1, due: today, lapses: 1, reps: 0, ease: 2.1 });
    }
    const spread = new Set<string>();
    for (const [id, c] of Object.entries(p.cards)) {
      if (v1.missed.includes(id)) continue;
      expect(c).toMatchObject({ interval: 3, reps: 1, ease: 2.3 });
      const offset = 1 + (fnv1a(id) % 7);
      expect(c.due).toBe(addDays(today, offset));
      spread.add(c.due);
    }
    expect(spread.size).toBe(7);
    // not an avalanche: only the missed ones are due today
    expect(Object.values(p.cards).filter((c) => c.due <= today).length).toBe(v1.missed.filter((m) => p.cards[m]).length);
  });

  it('is idempotent: migrating the result changes nothing', () => {
    const again = migrateProgress(p, ctx);
    expect(again).toMatchObject({ from: 2, changed: false });
    expect(again.progress).toEqual(p);
    expect(migrateProgress(JSON.parse(JSON.stringify(p)), ctx).changed).toBe(false);
  });

  it('does not mutate its input', () => {
    const before = JSON.stringify(v1);
    migrateProgress(v1, ctx);
    expect(JSON.stringify(v1)).toBe(before);
  });

  it('an empty v1 profile gets no gift and no cards', () => {
    const r = migrateProgress({ version: 1, xp: 0, streak: 0, lastDay: null, lessons: {}, missed: [] }, ctx).progress;
    expect(r).toMatchObject({ freezes: 0, cards: {}, stats: { practiceDays: 0, sessions: 0 } });
    expect(r.milestones['gift:freeze']).toBeUndefined();
  });

  it('repairs a damaged v1 blob field by field instead of discarding it', () => {
    const r = migrateProgress({ version: 1, xp: 120.9, streak: -3, lastDay: '2026-02-30', lessons: { a: { bestAccuracy: 7, completions: 'x' }, b: 'nope' }, missed: ['ok', 3, null, 'ok'] }, ctx).progress;
    expect(r).toMatchObject({ xp: 120, streak: 0, lastDay: null, missed: ['ok'] });
    expect(r.lessons).toEqual({ a: { bestAccuracy: 1, completions: 0, lastDay: today } });
  });
});

describe('migrateProgress: unusable storage falls back safely', () => {
  it.each([
    ['null', null, 0, undefined],
    ['undefined', undefined, 0, undefined],
    ['unparsable string', '{not json', 0, 'corrupt'],
    ['a number', 42, 0, 'corrupt'],
    ['an array', [1, 2], 0, 'corrupt'],
    ['an object without version', { xp: 5 }, 0, 'corrupt'],
    ['a string that is JSON null', 'null', 0, undefined],
    ['version 3 (from the future)', { version: 3, xp: 5 }, 0, 'future-version'],
    ['version 99 as text', '{"version":99}', 0, 'future-version'],
    ['version as a string', { version: '2' }, 0, 'corrupt'],
  ])('%s -> empty progress, never throws', (_name, raw, from, problem) => {
    const r = migrateProgress(raw, ctx);
    expect(r.progress).toEqual(emptyProgress());
    expect(r.from).toBe(from);
    expect(r.changed).toBe(false);
    expect(r.problem).toBe(problem);
  });
});

describe('normalizeProgress / v2 repair', () => {
  const good: Progress = { ...emptyProgress(), xp: 10, streak: 2, longestStreak: 2, lastDay: today };

  it('keeps a valid v2 untouched', () => {
    expect(migrateProgress(good, ctx)).toMatchObject({ from: 2, changed: false });
    expect(normalizeProgress(good, ctx)).toEqual(good);
  });

  it('clamps and validates every field', () => {
    const q = questions[0]!;
    const dirty = {
      ...good,
      xp: -50,
      streak: 3.7,
      longestStreak: 1,
      lastDay: 'tomorrow',
      freezes: 9,
      goal: { target: 5000 },
      profile: { onboarded: 'yes', role: 'ceo', interviewDate: '2026-02-30', restDays: [6, 0, 6, 9, -1, 'x', 3, 4, 1], practiceTime: '25:99' },
      settings: { haptics: 'no', confetti: false },
      stats: { sessions: -4, answered: 'x' },
      cards: {
        [q.id]: { h: questionHash(q), ease: 99, interval: 5000, due: addDays(today, 9999), reps: -1, lapses: 2.5, seen: 3, firstTry: 1, last: 'bad' },
        broken: { h: 'x' },
        alsoBroken: 'text',
      },
      days: { '2026-02-30': { xp: 1 }, [today]: { xp: 5, answered: 'many', firstTry: 2, target: 0 }, [addDays(today, -DAYS_KEPT)]: { xp: 1, answered: 1, firstTry: 1, target: 8 } },
      milestones: { 'rank:1': 'junk', ok: today, '': today },
      extra: 'dropped',
    };
    const p = normalizeProgress(dirty, ctx);
    expect(p).toMatchObject({ xp: 0, streak: 3, longestStreak: 3, lastDay: null, freezes: 2 });
    expect(p.goal.target).toBe(100);
    expect(p.profile).toEqual({ onboarded: false, role: null, interviewDate: null, restDays: [0, 1, 3], practiceTime: null });
    expect(p.settings).toEqual({ haptics: true, confetti: false, tips: true, confidence: false });
    expect(p.stats).toMatchObject({ sessions: 0, answered: 0 });
    expect(Object.keys(p.cards)).toEqual([q.id]);
    expect(p.cards[q.id]).toMatchObject({ ease: 2.8, interval: MAX_INTERVAL, due: addDays(today, MAX_INTERVAL), reps: 0, lapses: 2 });
    expect(p.cards[q.id]!.last).toBe(p.cards[q.id]!.due);
    expect(Object.keys(p.days)).toEqual([today]);
    expect(p.days[today]).toEqual({ xp: 5, answered: 0, firstTry: 2, target: 1 });
    expect(p.milestones).toEqual({ 'rank:1': today, ok: today });
    expect('extra' in p).toBe(false);
    // and the repaired result is stable
    expect(normalizeProgress(p, ctx)).toEqual(p);
    expect(sameJson(p, normalizeProgress(p, ctx))).toBe(true);
  });

  it('normalising non-objects gives defaults', () => {
    expect(normalizeProgress(null, ctx)).toEqual(emptyProgress());
    expect(normalizeProgress('x', ctx)).toEqual(emptyProgress());
  });

  it('a v2 blob missing newer additive fields is completed and flagged as changed', () => {
    const partial = { version: 2, xp: 7, streak: 1, lastDay: today, lessons: {}, missed: [] };
    const r = migrateProgress(partial, ctx);
    expect(r).toMatchObject({ from: 2, changed: true });
    expect(r.progress.settings.tips).toBe(true);
    expect(migrateProgress(r.progress, ctx).changed).toBe(false);
  });

  it('keeps valid profile data', () => {
    const p = normalizeProgress({ ...good, profile: { onboarded: true, role: 'staff-architect', interviewDate: '2026-12-01', restDays: [0, 6], practiceTime: '07:30' } }, ctx);
    expect(p.profile).toEqual({ onboarded: true, role: 'staff-architect', interviewDate: '2026-12-01', restDays: [0, 6], practiceTime: '07:30' });
  });
});

describe('pruneProgress', () => {
  it('drops cards of removed or rewritten questions but never touches missed', () => {
    const [a, b] = questions;
    const p: Progress = { ...emptyProgress(), missed: ['ghost/1'], cards: { [a!.id]: card(a!), [b!.id]: card(b!, { h: 'stale' }), 'ghost/1': card(a!) } };
    const out = pruneProgress(p, ctx);
    expect(Object.keys(out.cards)).toEqual([a!.id]);
    expect(out.missed).toEqual(['ghost/1']);
  });
  it('does not wipe cards when the content failed to load (empty question list)', () => {
    const [a] = questions;
    const p: Progress = { ...emptyProgress(), cards: { [a!.id]: card(a!) } };
    expect(pruneProgress(p, { today, questions: [] }).cards).toEqual(p.cards);
  });
  it('trims days beyond DAYS_KEPT', () => {
    const old = addDays(today, -DAYS_KEPT - 5);
    const p: Progress = { ...emptyProgress(), days: { [old]: { xp: 1, answered: 1, firstTry: 1, target: 8 }, [today]: { xp: 1, answered: 1, firstTry: 1, target: 8 } } };
    expect(Object.keys(pruneProgress(p, ctx).days)).toEqual([today]);
  });
});

describe('normalizeProgress: bounded size (QA S2-19)', () => {
  const many = (n: number, f: (i: number) => unknown) => Object.fromEntries(Array.from({ length: n }, (_, i) => [`id-${i}`, f(i)]));

  it('keeps at most MAX_RECORDS missed ids, cards and lessons, and drops absurdly long ids', () => {
    const r = normalizeProgress(
      {
        version: 2,
        missed: [...Array.from({ length: MAX_RECORDS * 3 }, (_, i) => `q/${i}`), 'x'.repeat(MAX_ID_LENGTH + 1)],
        cards: many(MAX_RECORDS + 50, () => ({ h: 'abc', due: today })),
        lessons: many(MAX_RECORDS + 50, () => ({ bestAccuracy: 1, completions: 1, lastDay: today })),
      },
      ctx,
    );
    expect(r.missed).toHaveLength(MAX_RECORDS);
    expect(Object.keys(r.cards)).toHaveLength(MAX_RECORDS);
    expect(Object.keys(r.lessons)).toHaveLength(MAX_RECORDS);
    const long = normalizeProgress({ version: 2, missed: ['x'.repeat(MAX_ID_LENGTH + 1), 'ok'], cards: { ['y'.repeat(MAX_ID_LENGTH + 1)]: { h: 'a', due: today } }, lessons: { ['z'.repeat(MAX_ID_LENGTH + 1)]: {} } }, ctx);
    expect(long.missed).toEqual(['ok']);
    expect(long.cards).toEqual({});
    expect(long.lessons).toEqual({});
  });

  it('a profile of realistic size is untouched', () => {
    const r = migrateProgress({ version: 2, missed: ['a/1', 'a/2'], lessons: { a: { bestAccuracy: 0.5, completions: 2, lastDay: today } } }, ctx).progress;
    expect(r.missed).toEqual(['a/1', 'a/2']);
    expect(r.lessons.a).toMatchObject({ completions: 2 });
  });
});

describe('the retired sound setting', () => {
  it('is dropped from saved progress and from backups that still carry it, without disturbing the other settings', () => {
    const old = { ...emptyProgress(), settings: { haptics: false, sound: true, confetti: true, tips: false, confidence: true } };
    const p = normalizeProgress(old, ctx);
    expect(p.settings).toEqual({ haptics: false, confetti: true, tips: false, confidence: true });
    expect('sound' in p.settings).toBe(false);
  });
});
