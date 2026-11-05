import { describe, expect, it } from 'vitest';
import {
  ACHIEVEMENTS, ACHIEVEMENT_GROUPS, addDays, achievementStatus, allQuestions, backfillAchievements, emptyProgress, newlyEarned, silentUnlock, unlockAchievements,
  type AchievementCtx, type MockRecord, type Progress, type Question,
} from './index';
import { card, mkCourse, withCards } from './fixtures';

const today = '2026-11-04';
const course = mkCourse({ a: { 'a-1': 3 }, b: { 'b-1': 3 } });
const ctx: AchievementCtx = { course, today };
const qs = allQuestions(course);
const mock = (score: number): MockRecord => ({ day: today, total: 10, answered: 10, score, limitMin: 15, usedSec: 500, timedOut: false, byUnit: {} });

const p0 = (): Progress => emptyProgress();
const ids = (p: Progress) => newlyEarned(p, ctx).map((d) => d.id);

/** For each badge: a progress that earns it, built from the empty profile. */
const earns: Record<string, (p: Progress) => Progress> = {
  'first-lesson': (p) => ({ ...p, lessons: { 'a-1': { bestAccuracy: 1, completions: 1, lastDay: today } } }),
  'perfect-lesson': (p) => ({ ...p, stats: { ...p.stats, perfect: 1 } }),
  'perfect-5': (p) => ({ ...p, stats: { ...p.stats, perfect: 5 } }),
  'rank-senior': (p) => ({ ...p, xp: 500 }),
  'streak-7': (p) => ({ ...p, longestStreak: 7 }),
  'streak-30': (p) => ({ ...p, longestStreak: 30 }),
  'practice-days-10': (p) => ({ ...p, stats: { ...p.stats, practiceDays: 10 } }),
  'practice-days-30': (p) => ({ ...p, stats: { ...p.stats, practiceDays: 30 } }),
  'goal-getter': (p) => ({ ...p, days: Object.fromEntries(Array.from({ length: 7 }, (_, i) => [addDays(today, -i), { xp: 50, answered: 8, firstTry: 8, target: 8 }])) }),
  comeback: (p) => ({ ...p, days: { [addDays(today, -12)]: { xp: 1, answered: 1, firstTry: 1, target: 8 }, [addDays(today, -2)]: { xp: 1, answered: 1, firstTry: 1, target: 8 } } }),
  'first-code': (p) => ({ ...p, passedByType: { code: 1 } }),
  'first-spot': (p) => ({ ...p, passedByType: { spot: 1 } }),
  'first-estimate': (p) => ({ ...p, passedByType: { estimate: 2 } }),
  'first-open': (p) => ({ ...p, passedByType: { open: 1 } }),
  'all-formats': (p) => ({ ...p, passedByType: Object.fromEntries(['single', 'multi', 'fill', 'order', 'spot', 'estimate', 'match', 'code', 'open'].map((t) => [t, 1])) }),
  'topic-mastery': (p) => withCards(p, qs.slice(0, 3).map((q): [Question, object] => [q, { interval: 30, due: addDays(today, 20) }])),
  'reviews-50': (p) => withCards(p, [[qs[0]!, { seen: 50 }]]),
  'reviews-250': (p) => withCards(p, [[qs[0]!, { seen: 100 }], [qs[1]!, { seen: 150 }]]),
  'questions-100': (p) => ({ ...p, stats: { ...p.stats, answered: 100 } }),
  calibrated: (p) => ({ ...p, calibration: { n: [10, 10, 10], right: [5, 5, 5] } }),
  'topic-explorer': (p) => withCards(p, [[qs[0]!, {}], [qs[3]!, {}]]),
  'mock-first': (p) => ({ ...p, mocks: [mock(10)] }),
  'mock-strong': (p) => ({ ...p, mocks: [mock(80)] }),
  curator: (p) => ({ ...p, bookmarks: Object.fromEntries(Array.from({ length: 5 }, (_, i) => [`x/${i}`, { h: 'h', day: today }])) }),
  'note-taker': (p) => ({ ...p, notes: { 'x/1': { h: 'h', text: 'n', day: today } } }),
  'challenge-first': (p) => ({ ...p, challenge: { [today]: 'fail' } }),
  'challenge-7': (p) => ({ ...p, challenge: Object.fromEntries(Array.from({ length: 7 }, (_, i) => [addDays(today, -i - 10), 'pass'])) as Progress['challenge'] }),
};

describe('badge catalogue', () => {
  it('has at least 18 badges with unique ids, titles and groups', () => {
    expect(ACHIEVEMENTS.length).toBeGreaterThanOrEqual(18);
    expect(new Set(ACHIEVEMENTS.map((a) => a.id)).size).toBe(ACHIEVEMENTS.length);
    expect(new Set(ACHIEVEMENTS.map((a) => a.title)).size).toBe(ACHIEVEMENTS.length);
    for (const a of ACHIEVEMENTS) {
      expect(a.description.length, a.id).toBeGreaterThan(8);
      expect(ACHIEVEMENT_GROUPS[a.group], a.id).toBeTruthy();
    }
  });

  it('a fresh profile earns nothing', () => {
    expect(ids(p0())).toEqual([]);
  });

  it('every badge has an "earns" fixture here (so none is untested)', () => {
    expect(Object.keys(earns).sort()).toEqual(ACHIEVEMENTS.map((a) => a.id).sort());
  });

  it.each(ACHIEVEMENTS.map((a) => a.id))('%s: earned by its condition, not by a nearby one', (id) => {
    expect(ids(earns[id]!(p0()))).toContain(id);
  });

  it('thresholds are exact: one short earns nothing', () => {
    expect(ids({ ...p0(), longestStreak: 6 })).not.toContain('streak-7');
    expect(ids({ ...p0(), xp: 499 })).not.toContain('rank-senior');
    expect(ids({ ...p0(), stats: { ...p0().stats, answered: 99 } })).not.toContain('questions-100');
    expect(ids(withCards(p0(), [[qs[0]!, { seen: 49 }]]))).not.toContain('reviews-50');
    expect(ids({ ...p0(), mocks: [mock(79)] })).not.toContain('mock-strong');
    expect(ids({ ...p0(), passedByType: { single: 1, multi: 1 } })).not.toContain('all-formats');
    expect(ids(withCards(p0(), [[qs[0]!, {}]]))).not.toContain('topic-explorer'); // only one of two topics seen
    expect(ids({ ...p0(), days: { [addDays(today, -6)]: { xp: 1, answered: 1, firstTry: 1, target: 8 }, [today]: { xp: 1, answered: 1, firstTry: 1, target: 8 } } })).not.toContain('comeback');
    expect(ids({ ...p0(), calibration: { n: [10, 10, 9], right: [0, 0, 0] } })).not.toContain('calibrated');
    expect(ids(withCards(p0(), qs.slice(0, 3).map((q): [Question, object] => [q, { interval: 5, due: addDays(today, 4) }])))).not.toContain('topic-mastery');
  });

  it('the challenge streak badge needs consecutive days', () => {
    const gaps = Object.fromEntries([0, 1, 2, 4, 5, 6].map((i) => [addDays(today, -i), 'pass'])) as Progress['challenge'];
    expect(ids({ ...p0(), challenge: gaps })).not.toContain('challenge-7');
  });
});

describe('unlockAchievements', () => {
  it('records new badges with today, once (idempotent) and never removes them', () => {
    const start = { ...p0(), stats: { ...p0().stats, perfect: 1 }, lessons: { 'a-1': { bestAccuracy: 1, completions: 1, lastDay: today } } };
    const first = unlockAchievements(start, ctx);
    expect(first.unlocked.map((d) => d.id).sort()).toEqual(['first-lesson', 'perfect-lesson']);
    expect(first.progress.achievements).toEqual({ 'first-lesson': today, 'perfect-lesson': today });
    const second = unlockAchievements(first.progress, { course, today: addDays(today, 3) });
    expect(second.unlocked).toEqual([]);
    expect(second.progress).toBe(first.progress);
    // the condition stops holding (e.g. a reset of stats): the badge stays
    const reverted = unlockAchievements({ ...first.progress, stats: p0().stats }, ctx);
    expect(reverted.progress.achievements['perfect-lesson']).toBe(today);
  });

  it('achievementStatus lists every badge with its unlock day', () => {
    const p = { ...p0(), achievements: { 'first-lesson': '2026-10-01' } };
    const status = achievementStatus(p);
    expect(status).toHaveLength(ACHIEVEMENTS.length);
    expect(status.find((s) => s.def.id === 'first-lesson')!.on).toBe('2026-10-01');
    expect(status.filter((s) => s.on === null)).toHaveLength(ACHIEVEMENTS.length - 1);
  });
});

describe('backfill and silent unlock', () => {
  const veteran: Progress = {
    ...p0(), xp: 600, longestStreak: 8, stats: { sessions: 12, answered: 120, firstTry: 90, perfect: 2, practiceDays: 11 },
    lessons: { 'a-1': { bestAccuracy: 1, completions: 2, lastDay: today } },
  };

  it('grants what is already earned, silently, once', () => {
    const b = backfillAchievements(veteran, ctx);
    expect(Object.keys(b.achievements).sort()).toEqual(['first-lesson', 'perfect-lesson', 'practice-days-10', 'questions-100', 'rank-senior', 'streak-7']);
    expect(b.milestones['ach:init']).toBe(today);
    expect(backfillAchievements(b, ctx)).toBe(b);
    // anything earned later is a normal, loud unlock
    const later = { ...b, longestStreak: 30 };
    expect(unlockAchievements(later, ctx).unlocked.map((d) => d.id)).toEqual(['streak-30']);
  });

  it('leaves a brand new profile untouched (nothing to backfill, nothing to write)', () => {
    const fresh = p0();
    expect(backfillAchievements(fresh, ctx)).toBe(fresh);
    expect(fresh.milestones['ach:init']).toBeUndefined();
  });

  it('silentUnlock (used for imports) grants without touching the flag', () => {
    const s = silentUnlock(veteran, ctx);
    expect(Object.keys(s.achievements)).toHaveLength(6);
    expect(s.milestones['ach:init']).toBeUndefined();
  });

  it('card-based profiles count as history', () => {
    const p = withCards(p0(), [[qs[0]!, { seen: 60 }]]);
    expect(Object.keys(backfillAchievements(p, ctx).achievements)).toContain('reviews-50');
  });
});

describe('copy', () => {
  it('has no guilt or urgency wording anywhere', () => {
    const text = ACHIEVEMENTS.map((a) => `${a.title} ${a.description}`).join(' ').toLowerCase();
    for (const bad of ['lose', 'lost', 'miss', 'hurry', 'last chance', 'only', 'don\'t break', 'expire']) expect(text, bad).not.toContain(bad);
  });
  it('a card helper exists for fixtures', () => {
    expect(card(qs[0]!).h).toBeTruthy();
  });
});
