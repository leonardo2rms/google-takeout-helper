import { describe, expect, it } from 'vitest';
import { addDays, allQuestions, dueOverview, dueQuestions, emptyProgress, minutesFor, reviewQueue, topicStats, unitOfFor, REVIEW_SESSION_SIZE } from './index';
import { mkCourse, withCards } from './fixtures';

const today = '2026-11-04';
const course = mkCourse({ k: { 'k-1': 12, 'k-2': 12 }, s: { 's-1': 12 }, d: { 'd-1': 12 } });
const qs = allQuestions(course);

/** A profile with the given question indexes due (today or earlier), the later ones scheduled ahead, and some missed. */
function profile(dueIdx: number[], laterIdx: number[], missedIdx: number[] = []) {
  const rows = [
    ...dueIdx.map((i) => [qs[i]!, { due: addDays(today, -(i % 4)) }] as [(typeof qs)[number], object]),
    ...laterIdx.map((i) => [qs[i]!, { due: addDays(today, 1 + (i % 9)) }] as [(typeof qs)[number], object]),
  ];
  const p = withCards(emptyProgress(), rows);
  return { ...p, missed: missedIdx.map((i) => qs[i]!.id) };
}
const range = (a: number, b: number) => Array.from({ length: b - a }, (_, i) => a + i);

describe('dueOverview: one meaning of "due" (a card whose due date has arrived)', () => {
  const shapes: Record<string, ReturnType<typeof profile>> = {
    'brand new': emptyProgress(),
    'one lesson, nothing due yet': profile([], range(0, 12)),
    'a few due': profile([0, 1, 13], range(2, 8)),
    'a heavy backlog': profile(range(0, 40), range(40, 48)),
    'misses that are not due yet': profile([3], range(0, 3), [5, 6, 7]),
    'misses that are also due': profile(range(0, 6), [], [0, 1, 2]),
  };
  for (const [name, p] of Object.entries(shapes)) {
    it(`every surface agrees: ${name}`, () => {
      const o = dueOverview(course, p, today);
      const unitOf = unitOfFor(course);
      // Today (daily.ts), Review and the reminder all count dueQuestions...
      expect(o.due).toBe(dueQuestions(p, qs, today, unitOf).length);
      expect(o.due).toBe(reviewQueue(p, qs, today, unitOf).due.length);
      // ...and the Progress topic rows add up to the same number.
      expect(topicStats(course, p, today).reduce((n, t) => n + t.due, 0)).toBe(o.due);
      // A Review session draws on due + retry; retry is never counted as due.
      expect(o.reviewable).toBe(o.due + o.retry);
      expect(o.reviewable).toBe(reviewQueue(p, qs, today, unitOf).all.length);
    });
  }
  it('cards due earlier or today count; scheduled-ahead cards do not', () => {
    const p = profile([0], []);
    expect(dueOverview(course, p, today).due).toBe(1);
    expect(dueOverview(course, p, addDays(today, -10)).due).toBe(0);
  });
  it('knows the session size and a rough duration', () => {
    expect(REVIEW_SESSION_SIZE).toBe(10);
    expect(minutesFor(0)).toBe(1);
    expect(minutesFor(7)).toBe(4);
  });
});
