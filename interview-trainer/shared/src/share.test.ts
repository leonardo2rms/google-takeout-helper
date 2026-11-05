import { describe, expect, it } from 'vitest';
import { questionHash } from './hash';
import { emptyProgress, type Card, type Progress } from './progress';
import { Course, allQuestions } from './schema';
import { BEST_TOPIC_MIN_PCT, buildShareCard, hasShareableProgress } from './share';

const mk = (id: string, title: string) => ({
  id, title, emoji: 'x', description: 'd',
  lessons: [{
    id: `${id}-l`, title: 'L',
    questions: [1, 2, 3].map((n) => ({ type: 'single', prompt: `${id} ${n}`, explanation: 'e', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a' })),
  }],
});
const course = Course.parse({ units: [mk('kt', 'Kotlin'), mk('sp', 'Spring')] });
const qs = allQuestions(course);
const TODAY = '2026-11-04';

const card = (q: (typeof qs)[number], interval: number): Card => ({
  h: questionHash(q), ease: 2.5, interval, due: '2026-12-31', reps: 3, lapses: 0, seen: 3, firstTry: 3, last: TODAY,
});

describe('buildShareCard', () => {
  it('a new learner: zero everything, no readiness, no topic, still well-formed', () => {
    const c = buildShareCard(course, emptyProgress(), TODAY);
    expect(c).toMatchObject({ streak: 0, rank: 'Associate', lessonsDone: 0, lessonsTotal: 2, daysPracticed: 0, readiness: null, bestTopic: null });
    expect(c.text).not.toMatch(/undefined|null/);
    expect(hasShareableProgress(emptyProgress())).toBe(false);
  });

  it('names the best topic when it is strong, and shares lessons and days practised', () => {
    const p: Progress = {
      ...emptyProgress(), xp: 200, streak: 5, lastDay: TODAY, stats: { ...emptyProgress().stats, answered: 6, practiceDays: 4 },
      lessons: { 'kt-l': { bestAccuracy: 1, completions: 1, last: TODAY } } as never,
      cards: { ...Object.fromEntries(qs.slice(0, 3).map((q) => [q.id, card(q, 30)])), ...Object.fromEntries(qs.slice(3, 4).map((q) => [q.id, card(q, 2)])) },
    };
    const c = buildShareCard(course, p, TODAY);
    expect(c).toMatchObject({ streak: 5, rank: 'Engineer', lessonsDone: 1, lessonsTotal: 2, daysPracticed: 4 });
    expect(c.bestTopic?.title).toBe('Kotlin');
    expect(c.bestTopic!.pct).toBeGreaterThanOrEqual(BEST_TOPIC_MIN_PCT);
    expect(c.text).toContain('Streak: 5 days');
    expect(c.text).toContain('Lessons: 1 of 2\nDays practised: 4');
    expect(c.text).toContain('Best topic: Kotlin');
    expect(c.alt).toMatch(/^Interview Trainer progress card: 5-day streak, rank Engineer, 1 of 2 lessons done, 4 days practised/);
    expect(hasShareableProgress(p)).toBe(true);
  });

  it('never shares the weakest topic or a weak topic: a topic under the bar is left out', () => {
    const p: Progress = { ...emptyProgress(), cards: { [qs[0]!.id]: card(qs[0]!, 1), ...Object.fromEntries(qs.slice(3, 6).map((q) => [q.id, card(q, 40)])) }, stats: { ...emptyProgress().stats, answered: 2 } };
    const c = buildShareCard(course, p, TODAY);
    expect(c.bestTopic?.title).toBe('Spring');
    expect(c.text).not.toMatch(/Kotlin|weak/i);
    const weakOnly = buildShareCard(course, { ...emptyProgress(), cards: { [qs[0]!.id]: card(qs[0]!, 1) }, stats: { ...emptyProgress().stats, answered: 1 } }, TODAY);
    expect(weakOnly.bestTopic).toBeNull();
    expect(weakOnly.text).not.toMatch(/topic/i);
  });

  it('shows readiness only once enough of the course has been seen', () => {
    const all: Progress = { ...emptyProgress(), cards: Object.fromEntries(qs.map((q) => [q.id, card(q, 30)])), stats: { ...emptyProgress().stats, answered: 6 } };
    const c = buildShareCard(course, all, TODAY);
    expect(c.readiness?.score).toBeGreaterThan(0);
    expect(c.text).toContain('Readiness:');
    const few = buildShareCard(course, { ...emptyProgress(), cards: { [qs[0]!.id]: card(qs[0]!, 30) }, stats: { ...emptyProgress().stats, answered: 1 } }, TODAY);
    expect(few.readiness).toBeNull();
    expect(few.text).not.toContain('Readiness');
  });

  it('shares only what the app already shows: no dates, role, interview date or ids', () => {
    const p: Progress = {
      ...emptyProgress(), xp: 10, streak: 1, lastDay: TODAY, stats: { ...emptyProgress().stats, answered: 1 },
      profile: { ...emptyProgress().profile, role: 'staff-architect', interviewDate: '2027-01-15', practiceTime: '07:30' },
      cards: { [qs[0]!.id]: card(qs[0]!, 10) },
    };
    const c = buildShareCard(course, p, TODAY);
    const blob = JSON.stringify(c);
    expect(blob).not.toMatch(/2027|2026|07:30|staff|architect|kt-l|sp-l|\/1/);
  });

  it('singular day', () => {
    expect(buildShareCard(course, { ...emptyProgress(), streak: 1, lastDay: TODAY }, TODAY).text).toContain('Streak: 1 day\n');
  });
});
