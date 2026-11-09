/**
 * First reviews are spread (audit N-4): at most FIRST_REVIEW_DAY_CAP new cards become due on one day, the rest slide to the next
 * day or two, so three lessons on day 1 no longer show "25 due" on day 2. Later reviews, misses and early reviews are untouched.
 */
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { addDays, daysBetween, type Day } from './day';
import { loadCourse } from './load-content';
import { emptyProgress, type Card, type Progress } from './progress';
import { allQuestions } from './schema';
import {
  FIRST_REVIEW_DAY_CAP, FIRST_REVIEW_MAX_SLIDE, applyToCards, isFirstReview, scheduleCard, spreadFirstReview, type Outcome,
} from './srs';
import { questionHash } from './hash';

const course = loadCourse(resolve(dirname(fileURLToPath(import.meta.url)), '../../content'));
const all = allQuestions(course);
const D: Day = '2026-11-04';
const first = (qs: typeof all): Record<string, Outcome> => Object.fromEntries(qs.map((q) => [q.id, 'first' as const]));
const loadOf = (cards: Progress['cards']) => {
  const m = new Map<Day, number>();
  for (const c of Object.values(cards)) m.set(c.due, (m.get(c.due) ?? 0) + 1);
  return m;
};
const dayCounts = (cards: Progress['cards']) => Object.fromEntries([...loadOf(cards)].sort());

describe('spreadFirstReview: the pure choice of a day', () => {
  const load = (...counts: number[]) => new Map<Day, number>(counts.map((n, i) => [addDays(D, i), n]));

  it('keeps the scheduled day while it has room', () => {
    expect(spreadFirstReview(D, load())).toBe(0);
    expect(spreadFirstReview(D, load(FIRST_REVIEW_DAY_CAP - 1))).toBe(0);
  });
  it('slides to the first later day with room', () => {
    expect(spreadFirstReview(D, load(FIRST_REVIEW_DAY_CAP))).toBe(1);
    expect(spreadFirstReview(D, load(FIRST_REVIEW_DAY_CAP, FIRST_REVIEW_DAY_CAP, 3))).toBe(2);
  });
  it('when every candidate is full, takes the least loaded, the earliest on a tie', () => {
    expect(spreadFirstReview(D, load(12, 10, 11))).toBe(1);
    expect(spreadFirstReview(D, load(10, 10, 10))).toBe(0);
    expect(spreadFirstReview(D, load(14, 13, 13))).toBe(1);
  });
  it('never slides further than allowed', () => {
    expect(spreadFirstReview(D, load(10, 10, 0), 1)).toBe(0); // both allowed days are full: the earlier one
    expect(spreadFirstReview(D, load(10, 0, 0), 0)).toBe(0);
    expect(FIRST_REVIEW_MAX_SLIDE).toBe(2);
  });
});

describe('applyToCards: new cards', () => {
  it('one lesson of new cards is untouched: all due the next day, as before', () => {
    const lesson = all.slice(0, 9);
    const cards = applyToCards({}, first(lesson), all, D);
    expect(dayCounts(cards)).toEqual({ [addDays(D, 1)]: 9 });
    for (const c of Object.values(cards)) expect(c.interval).toBe(1);
  });

  it('25 new cards spread over three days: 10, 10, 5', () => {
    const cards = applyToCards({}, first(all.slice(0, 25)), all, D);
    expect(dayCounts(cards)).toEqual({ [addDays(D, 1)]: 10, [addDays(D, 2)]: 10, [addDays(D, 3)]: 5 });
    for (const c of Object.values(cards)) expect(c.interval).toBe(daysBetween(D, c.due)); // the interval says how far away it is
  });

  it('three lessons in one day (separate sessions) also stay under the cap', () => {
    let cards: Progress['cards'] = {};
    for (const [a, b] of [[0, 9], [9, 18], [18, 25]] as const) cards = applyToCards(cards, first(all.slice(a, b)), all, D);
    expect(Math.max(...loadOf(cards).values())).toBeLessThanOrEqual(FIRST_REVIEW_DAY_CAP);
    expect(Object.keys(cards)).toHaveLength(25);
    expect(dayCounts(cards)[addDays(D, 1)]).toBe(10);
  });

  it('a first review can only slide later, never earlier, and never past the slide limit', () => {
    const cards = applyToCards({}, first(all.slice(0, 40)), all, D);
    for (const c of Object.values(cards)) {
      expect(c.due >= addDays(D, 1)).toBe(true);
      expect(c.due <= addDays(D, 1 + FIRST_REVIEW_MAX_SLIDE)).toBe(true);
    }
  });

  it('is deterministic and does not touch its input', () => {
    const before: Progress['cards'] = applyToCards({}, first(all.slice(0, 5)), all, D);
    const snapshot = JSON.stringify(before);
    const once = applyToCards(before, first(all.slice(5, 30)), all, D);
    expect(JSON.stringify(before)).toBe(snapshot);
    expect(applyToCards(before, first(all.slice(5, 30)), all, D)).toEqual(once);
  });

  it('property: any number of new cards stays within three days, under the cap while it can, and none is lost', () => {
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 60 }), (n) => {
        const cards = applyToCards({}, first(all.slice(0, n)), all, D);
        expect(Object.keys(cards)).toHaveLength(n);
        const counts = [...loadOf(cards).values()];
        expect(counts.reduce((a, b) => a + b, 0)).toBe(n);
        if (n <= 3 * FIRST_REVIEW_DAY_CAP) expect(Math.max(...counts)).toBeLessThanOrEqual(FIRST_REVIEW_DAY_CAP);
        else expect(Math.max(...counts)).toBeLessThanOrEqual(Math.ceil(n / 3));
        for (const c of Object.values(cards)) expect(daysBetween(D, c.due)).toBeGreaterThanOrEqual(1);
      }),
      { numRuns: 60 },
    );
  });
});

describe('applyToCards: what is not spread', () => {
  const q = all[0]!;
  const card = (over: Partial<Card>): Card => ({ h: questionHash(q), ease: 2.3, interval: 1, due: addDays(D, 1), reps: 0, lapses: 0, seen: 1, firstTry: 0, last: D, ...over });
  /** 12 other cards already due on `due`, so that day is over the cap. */
  const crowded = (due: Day): Progress['cards'] => Object.fromEntries(all.slice(50, 62).map((o) => [o.id, { ...card({ h: questionHash(o), due, reps: 2, interval: 3, last: addDays(due, -3) }) }]));

  it('a miss is due tomorrow whatever the load', () => {
    const cards = applyToCards(crowded(addDays(D, 1)), { [q.id]: 'failed' }, all, D);
    expect(cards[q.id]!.due).toBe(addDays(D, 1));
    expect(cards[q.id]!.interval).toBe(1);
  });

  it('a later review keeps its schedule: the second correct answer is 3 days out, on a crowded day too', () => {
    const start = { ...crowded(addDays(D, 3)), [q.id]: card({ reps: 1, interval: 1, due: D, last: addDays(D, -1) }) };
    const cards = applyToCards(start, { [q.id]: 'first' }, all, D);
    expect(cards[q.id]).toMatchObject({ reps: 2, interval: 3, due: addDays(D, 3) });
  });

  it('an early review is bookkeeping only', () => {
    const start = { ...crowded(addDays(D, 5)), [q.id]: card({ reps: 2, interval: 5, due: addDays(D, 5), last: D }) };
    const cards = applyToCards(start, { [q.id]: 'first' }, all, D);
    expect(cards[q.id]).toMatchObject({ reps: 2, interval: 5, due: addDays(D, 5) });
  });

  it('an early review of a card that already has one success is not a first review, so it does not slide', () => {
    const start = { ...crowded(addDays(D, 1)), [q.id]: card({ reps: 1, interval: 1, due: addDays(D, 1), last: D }) };
    const cards = applyToCards(start, { [q.id]: 'first' }, all, D);
    expect(cards[q.id]).toMatchObject({ reps: 1, interval: 1, due: addDays(D, 1) });
  });

  it('the first review of a card that was only ever missed counts as a first review (and may slide)', () => {
    const start = { ...crowded(addDays(D, 1)), [q.id]: card({ reps: 0, interval: 1, due: D, last: addDays(D, -1), lapses: 1 }) };
    const cards = applyToCards(start, { [q.id]: 'first' }, all, D);
    expect(cards[q.id]!.reps).toBe(1);
    expect(cards[q.id]!.due).toBe(addDays(D, 2));
  });

  it('an interview date that leaves no room does not let a card slide past its cap', () => {
    const deadline = addDays(D, 2); // cap = max(1, floor(2 / 2)) = 1 day
    const cards = applyToCards(crowded(addDays(D, 1)), { [q.id]: 'first' }, all, D, { deadline });
    expect(cards[q.id]!.due).toBe(addDays(D, 1));
    expect(cards[q.id]!.interval).toBe(1);
  });

  it('a slide is limited by the interview date, not removed: a longer runway still lets it slide', () => {
    const cards = applyToCards(crowded(addDays(D, 1)), { [q.id]: 'first' }, all, D, { deadline: addDays(D, 40) });
    expect(cards[q.id]!.due).toBe(addDays(D, 2));
  });

  it('an easy first answer (scheduled 2 days out) slides from there', () => {
    const start = crowded(addDays(D, 2));
    const cards = applyToCards(start, { [q.id]: 'first' }, all, D, { confidence: { [q.id]: 3 } });
    expect(cards[q.id]!.due).toBe(addDays(D, 3));
  });
});

describe('isFirstReview', () => {
  const q = all[0]!;
  it('is true for a new card, a reworded one, and one that was only ever missed; false once it has been right', () => {
    expect(isFirstReview(emptyProgress(), q)).toBe(true);
    const base = { h: questionHash(q), ease: 2.3, interval: 1, due: D, lapses: 0, seen: 1, firstTry: 0, last: D };
    expect(isFirstReview({ cards: { [q.id]: { ...base, reps: 0 } } }, q)).toBe(true);
    expect(isFirstReview({ cards: { [q.id]: { ...base, reps: 1 } } }, q)).toBe(false);
    expect(isFirstReview({ cards: { [q.id]: { ...base, reps: 3, h: 'older-wording' } } }, q)).toBe(true);
  });
  it('agrees with scheduleCard: the card after a first success has reps 1', () => {
    expect(scheduleCard(undefined, 'good', D, { hash: questionHash(q) }).reps).toBe(1);
  });
});
