import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { addDays, allQuestions, buildDailySession, emptyProgress, interleave, unitOfFor, type Progress } from './index';
import { loadCourse } from './load-content';
import { mkCourse, withCards } from './fixtures';

const today = '2026-11-04';
const adjacentRepeats = (keys: string[]) => keys.filter((k, i) => i > 0 && k === keys[i - 1]).length;

describe('interleave', () => {
  const key = (s: string) => s[0]!;
  it('never drops or duplicates items', () => {
    const items = ['a1', 'a2', 'a3', 'b1', 'b2', 'c1'];
    expect([...interleave(items, key)].sort()).toEqual([...items].sort());
    expect(interleave([], key)).toEqual([]);
    expect(interleave(['a1'], key)).toEqual(['a1']);
  });
  it('minimises adjacent repeats (the optimum is max(0, biggest - rest - 1))', () => {
    const cases = [
      ['a1', 'a2', 'a3', 'a4', 'b1', 'c1'],
      ['a1', 'a2', 'a3', 'a4', 'a5', 'b1'],
      ['a1', 'b1', 'c1', 'd1'],
      ['a1', 'a2', 'b1', 'b2', 'c1', 'c2', 'c3'],
      ['a1', 'a2', 'a3'],
    ];
    for (const items of cases) {
      const counts = new Map<string, number>();
      for (const i of items) counts.set(key(i), (counts.get(key(i)) ?? 0) + 1);
      const biggest = Math.max(...counts.values());
      const optimum = Math.max(0, biggest - (items.length - biggest) - 1);
      for (const seed of [undefined, 1, 2, 3]) expect(adjacentRepeats(interleave(items, key, seed).map(key)), `${items} seed ${seed}`).toBe(optimum);
    }
  });
  it('is deterministic for equal input and seed, and varies with the seed', () => {
    const items = Array.from({ length: 12 }, (_, i) => `${'abcd'[i % 4]}${i}`);
    expect(interleave(items, key, 5)).toEqual(interleave(items, key, 5));
    expect(new Set([1, 2, 3, 4, 5, 6].map((s) => interleave(items, key, s).join(',')))).not.toEqual(new Set([interleave(items, key, 1).join(',')]));
  });
  it('without a seed ties keep original order', () => {
    expect(interleave(['a1', 'b1', 'c1'], key)).toEqual(['a1', 'b1', 'c1']);
  });
});

describe('buildDailySession (synthetic course)', () => {
  const course = mkCourse({ u1: { 'u1-a': 6 }, u2: { 'u2-a': 6 }, u3: { 'u3-a': 6 }, u4: { 'u4-a': 6 } });
  const all = allQuestions(course);

  function withDue(n: number): Progress {
    return withCards(emptyProgress(), all.slice(0, n).map((q, i) => [q, { interval: 2, due: addDays(today, -(i % 4) - 1) }] as [typeof q, object]));
  }

  it('a new learner gets a first-lesson session of fresh questions, with no fake "due" items', () => {
    const plan = buildDailySession(course, emptyProgress(), today);
    expect(plan.kind).toBe('first');
    expect(plan.mix).toEqual({ due: 0, weak: 0, fresh: 8, filler: 0 });
    expect(plan.questions).toHaveLength(8);
    expect(plan.minutes).toBe(5);
  });

  it('3 due: all 3 are included, the rest is weak spots then fresh', () => {
    const plan = buildDailySession(course, withDue(3), today);
    expect(plan.kind).toBe('daily');
    expect(plan.mix.due).toBe(3);
    expect(plan.mix.due + plan.mix.weak + plan.mix.fresh + plan.mix.filler).toBe(8);
    expect(plan.questions).toHaveLength(8);
  });

  it('50 due (a long break): at most 5 due plus 2 weak spots and 1 new — never an overwhelming list', () => {
    const many = withCards(emptyProgress(), all.slice(0, 20).map((q) => [q, { interval: 2, due: addDays(today, -30) }] as [typeof q, object]));
    const plan = buildDailySession(course, many, today);
    expect(plan.mix).toEqual({ due: 5, weak: 2, fresh: 1, filler: 0 });
    expect(plan.questions).toHaveLength(8);
  });

  it('never duplicates, respects the size and shrinks for a tiny course', () => {
    const plan = buildDailySession(course, withDue(12), today, { size: 5 });
    expect(plan.questions).toHaveLength(5);
    expect(new Set(plan.questions.map((q) => q.id)).size).toBe(5);
    const tiny = mkCourse({ t: { 't-a': 3 } });
    expect(buildDailySession(tiny, emptyProgress(), today).questions).toHaveLength(3);
  });

  it('when fresh runs out it backfills with random seen questions', () => {
    const tiny = mkCourse({ t: { 't-a': 6 }, v: { 'v-a': 6 } });
    const tq = allQuestions(tiny);
    const p = withCards(emptyProgress(), tq.map((q) => [q, { interval: 10, due: addDays(today, 5) }] as [typeof q, object]));
    const plan = buildDailySession(tiny, p, today);
    expect(plan.questions).toHaveLength(8);
    expect(plan.mix.fresh).toBe(0);
    expect(plan.mix.weak + plan.mix.filler).toBe(8);
  });

  it('is deterministic for the same day and differs only through the day seed', () => {
    const p = withDue(10);
    expect(buildDailySession(course, p, today).questions.map((q) => q.id)).toEqual(buildDailySession(course, p, today).questions.map((q) => q.id));
  });

  it('mixes units: no two neighbours share a unit when avoidable', () => {
    const unitOf = unitOfFor(course);
    const plan = buildDailySession(course, withDue(16), today);
    expect(adjacentRepeats(plan.questions.map(unitOf))).toBe(0);
    expect(new Set(plan.questions.map(unitOf)).size).toBeGreaterThanOrEqual(3);
  });

  it('becomes a bonus round only when nothing is due and today\'s goal is already met', () => {
    const learned = withCards(emptyProgress(), all.map((q) => [q, { interval: 10, due: addDays(today, 5) }] as [typeof q, object]));
    expect(buildDailySession(course, learned, today).kind).toBe('daily');
    const met: Progress = { ...learned, days: { [today]: { xp: 80, answered: 8, firstTry: 8, target: 8 } } };
    expect(buildDailySession(course, met, today).kind).toBe('bonus');
  });
});

describe('buildDailySession (real course)', () => {
  const course = loadCourse(resolve(dirname(fileURLToPath(import.meta.url)), '../../content'));
  const all = allQuestions(course);

  it('has no duplicates and mixes units for a learner with a backlog across every unit', () => {
    const unitOf = unitOfFor(course);
    const p = withCards(emptyProgress(), all.filter((_, i) => i % 3 === 0).map((q) => [q, { interval: 3, due: addDays(today, -4) }] as [typeof q, object]));
    const plan = buildDailySession(course, p, today);
    expect(plan.questions).toHaveLength(8);
    expect(new Set(plan.questions.map((q) => q.id)).size).toBe(8);
    expect(new Set(plan.questions.map(unitOf)).size).toBeGreaterThanOrEqual(3);
  });

  it('every unit is reachable through the fresh-question flow', () => {
    // Walking the whole course, one daily session at a time, must touch every unit.
    let p = emptyProgress();
    const seenUnits = new Set<string>();
    const unitOf = unitOfFor(course);
    for (let day = 0; day < 40 && Object.keys(p.cards).length < all.length; day++) {
      const d = addDays(today, day);
      const plan = buildDailySession(course, p, d);
      for (const q of plan.questions) {
        seenUnits.add(unitOf(q));
        p = withCards(p, [[q, { interval: 30, due: addDays(d, 30) }]]);
      }
    }
    expect([...seenUnits].sort()).toEqual(course.units.map((u) => u.id).sort());
  });
});
