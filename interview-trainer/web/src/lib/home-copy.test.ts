import { describe, expect, it } from 'vitest';
import { emptyProgress } from '@trainer/shared';
import { FULL_HOME_AFTER_LESSONS, READINESS_NEEDS, REVIEW_SIZE, dueInToday, dueLine, dueText, goalMetHero, homeTier, readinessLine, reinforceText } from './home-copy';

const lessons = (n: number) => Object.fromEntries(Array.from({ length: n }, (_, i) => [`l${i}`, { bestAccuracy: 1, completions: 1, lastDay: '2026-11-04' }]));

describe('homeTier', () => {
  it('a brand-new learner sees the single next step', () => {
    expect(homeTier(emptyProgress())).toBe(0);
  });
  it('after the first session the daily practice modes appear', () => {
    const p = emptyProgress();
    expect(homeTier({ ...p, stats: { ...p.stats, sessions: 1 } })).toBe(1);
    expect(homeTier({ ...p, lessons: lessons(1) })).toBe(1);
  });
  it('a profile that already has scheduled questions is not new', () => {
    const p = emptyProgress();
    expect(homeTier({ ...p, cards: { 'a/1': { h: 'x', ease: 2.3, interval: 1, due: '2026-11-05', reps: 1, lapses: 0, seen: 1, firstTry: 1, last: '2026-11-04' } } })).toBe(1);
  });
  it('after three lessons everything is on the page', () => {
    const p = emptyProgress();
    expect(homeTier({ ...p, lessons: lessons(FULL_HOME_AFTER_LESSONS - 1) })).toBe(1);
    expect(homeTier({ ...p, lessons: lessons(FULL_HOME_AFTER_LESSONS) })).toBe(2);
  });
});

describe('dueLine', () => {
  it('says "N due" with the same phrase and the real number, however large', () => {
    expect(dueLine(0)).toBe('All caught up');
    expect(dueLine(1)).toBe('1 due');
    expect(dueLine(REVIEW_SIZE)).toBe(`${REVIEW_SIZE} due`);
    expect(dueLine(24)).toBe('24 due · 10 per session');
    expect(dueLine(73)).toBe('73 due · 10 per session'); // never capped to "30+"
  });
  it('names earlier misses separately: they are not "due"', () => {
    expect(dueLine(0, 3)).toBe('3 missed to retry');
    expect(dueLine(4, 3)).toBe('4 due · 3 missed to retry');
  });
});

describe('every surface uses the one phrase for one count', () => {
  it('Today, Review and Progress topic rows all say "<n> due" for the same n', () => {
    expect(dueText(73)).toBe('73 due');
    expect(dueInToday(6, 73)).toContain(dueText(73).replace(' due', ''));
    expect(dueInToday(6, 73)).toBe('6 of 73 due');
    expect(dueLine(73)).toContain(dueText(73));
    expect(dueInToday(5, 5)).toBe(dueText(5));
  });
});

describe('dueInToday', () => {
  it('names the total when Today only has room for some of the due cards', () => {
    expect(dueInToday(5, 24)).toBe('5 of 24 due');
    expect(dueInToday(5, 5)).toBe('5 due');
    expect(dueInToday(3, 3)).toBe('3 due');
  });
});

describe('goalMetHero', () => {
  const next = { id: '1.2', title: 'Coroutines' };
  it('is "Done for today" with one optional step: due reviews first', () => {
    const h = goalMetHero({ done: 8, target: 8, due: 4, nextLesson: next });
    expect(h).toMatchObject({ title: 'Done for today', next: 'review', href: '#/review', cta: 'Review' });
    expect(h.sub).toBe('Goal met: 8 questions. Optional: review 4 due.');
  });
  it('then the next lesson, then a bonus round', () => {
    expect(goalMetHero({ done: 8, target: 8, due: 0, nextLesson: next })).toMatchObject({ next: 'lesson', href: '#/lesson/1.2', cta: 'Next lesson' });
    expect(goalMetHero({ done: 8, target: 8, due: 0, nextLesson: null })).toMatchObject({ next: 'more', href: '#/daily', cta: 'Keep going' });
  });
  it('names what went beyond the goal as a bonus instead of a second number', () => {
    expect(goalMetHero({ done: 19, target: 8, due: 0 }).sub).toBe('Goal met, plus 11 bonus questions. Optional: a bonus round.');
    expect(goalMetHero({ done: 9, target: 8, due: 0 }).sub).toContain('plus 1 bonus question.');
  });
  it('never presents "Today" as the call to action', () => {
    expect(JSON.stringify(goalMetHero({ done: 8, target: 8, due: 9, nextLesson: next }))).not.toMatch(/"Today"|Start/);
  });
});

describe('reinforceText', () => {
  it('is neutral wording, not "weak spots"', () => {
    expect(reinforceText(2)).toBe('2 to reinforce');
  });
});

describe('readinessLine', () => {
  it('hides the score until enough of the course has been seen', () => {
    expect(readinessLine({ score: 1, label: 'early', enough: false, coverage: 0.123 })).toBe('Readiness: not enough data yet (12% of the course seen)');
  });
  it('shows the score and its label once there is enough data', () => {
    expect(readinessLine({ score: 31, label: 'building', enough: true, coverage: 0.4 })).toBe('Readiness 31 · Building');
  });
  it('names the threshold', () => {
    expect(READINESS_NEEDS).toBe('25%');
  });
});
