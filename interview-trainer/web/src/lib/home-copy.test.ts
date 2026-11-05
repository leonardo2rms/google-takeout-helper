import { describe, expect, it } from 'vitest';
import { emptyProgress } from '@trainer/shared';
import { FULL_HOME_AFTER_LESSONS, READINESS_NEEDS, REVIEW_SIZE, dueInToday, dueLine, homeTier, readinessLine } from './home-copy';

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
  it('says what is ready and that a round takes a handful', () => {
    expect(dueLine(0)).toBe('All caught up');
    expect(dueLine(1)).toBe('1 ready to review');
    expect(dueLine(REVIEW_SIZE)).toBe(`${REVIEW_SIZE} ready to review`);
    expect(dueLine(24)).toBe('24 ready · 10 at a time');
    expect(dueLine(80)).toBe('30+ ready · 10 at a time');
  });
});

describe('dueInToday', () => {
  it('names the total when Today only has room for some of the due cards', () => {
    expect(dueInToday(5, 24)).toBe('5 of 24 due');
    expect(dueInToday(5, 5)).toBe('5 due');
    expect(dueInToday(3, 3)).toBe('3 due');
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
