import { describe, expect, it } from 'vitest';
import { questionHash } from './hash';
import { emptyProgress, type Card, type Progress } from './progress';
import { Course, allQuestions } from './schema';
import { buildShareCard, hasShareableProgress } from './share';

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
  it('a new learner: no topics to compare, zero readiness, still well-formed', () => {
    const c = buildShareCard(course, emptyProgress(), TODAY);
    expect(c).toMatchObject({ streak: 0, rank: 'Associate', readinessScore: 0, strongest: null, weakest: null });
    expect(c.text).not.toMatch(/undefined|null/);
    expect(hasShareableProgress(emptyProgress())).toBe(false);
  });

  it('names the strongest and weakest started topic with percentages', () => {
    const p: Progress = {
      ...emptyProgress(), xp: 200, streak: 5, lastDay: TODAY, stats: { ...emptyProgress().stats, answered: 6 },
      cards: { ...Object.fromEntries(qs.slice(0, 3).map((q) => [q.id, card(q, 30)])), ...Object.fromEntries(qs.slice(3, 4).map((q) => [q.id, card(q, 2)])) },
    };
    const c = buildShareCard(course, p, TODAY);
    expect(c.streak).toBe(5);
    expect(c.rank).toBe('Engineer');
    expect(c.strongest?.title).toBe('Kotlin');
    expect(c.weakest?.title).toBe('Spring');
    expect(c.strongest!.pct).toBeGreaterThan(c.weakest!.pct);
    expect(c.text).toContain('Streak: 5 days');
    expect(c.text).toContain('Strongest topic: Kotlin');
    expect(c.text).toContain('Weakest: Spring');
    expect(c.alt).toMatch(/^Interview Trainer progress card: 5-day streak, rank Engineer, readiness \d+/);
    expect(hasShareableProgress(p)).toBe(true);
  });

  it('with a single started topic only one topic line appears', () => {
    const p: Progress = { ...emptyProgress(), cards: { [qs[0]!.id]: card(qs[0]!, 10) }, stats: { ...emptyProgress().stats, answered: 1 } };
    const c = buildShareCard(course, p, TODAY);
    expect(c.strongest).toBeNull();
    expect(c.weakest?.title).toBe('Kotlin');
    expect(c.text).toContain('Topic: Kotlin');
    expect(c.text).not.toContain('Strongest');
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
