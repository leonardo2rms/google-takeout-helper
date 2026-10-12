import { describe, expect, it } from 'vitest';
import {
  createSession, currentQuestion, emptyProgress, gradeFill, gradeMulti, gradeOrder, gradeSingle,
  heuristicCoverage, isFinished, liveStreak, MAX_RETRIES, nextStreak, recordResult, scoreFromCoverage,
  shuffleDifferent, submit, summarize, verdictFor, validateCourse, Course, blankIndexes,
  type OpenQ, type Question,
} from './index';

const single = { id: 'l/1', type: 'single', prompt: 'p', explanation: 'e', difficulty: 'easy', tags: [], options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a' } as Question;
const mk = (id: string) => ({ ...single, id }) as Question;

describe('grading', () => {
  it('single / multi are exact-match', () => {
    expect(gradeSingle(single as never, 'a')).toBe(true);
    expect(gradeSingle(single as never, 'b')).toBe(false);
    const multi = { answers: ['a', 'c'] } as never;
    expect(gradeMulti(multi, ['c', 'a'])).toBe(true);
    expect(gradeMulti(multi, ['a'])).toBe(false);
    expect(gradeMulti(multi, ['a', 'c', 'b'])).toBe(false);
  });
  it('fill and order require the exact arrangement', () => {
    expect(gradeFill({ answers: ['x', 'y'] } as never, ['x', 'y'])).toBe(true);
    expect(gradeFill({ answers: ['x', 'y'] } as never, ['x', null])).toBe(false);
    expect(gradeOrder({ lines: ['1', '2', '3'] } as never, ['1', '3', '2'])).toBe(false);
  });
  it('shuffleDifferent never returns the original order and is deterministic', () => {
    const lines = ['a', 'b', 'c', 'd'];
    for (let seed = 0; seed < 50; seed++) {
      const out = shuffleDifferent(lines, seed);
      expect(out).not.toEqual(lines);
      expect(out.slice().sort()).toEqual(lines);
      expect(shuffleDifferent(lines, seed)).toEqual(out);
    }
  });
  it('heuristic open grading checks any-of keywords case-insensitively', () => {
    const q = { keyPoints: [{ point: 'p1', keywords: ['structured concurrency', 'parent scope'] }, { point: 'p2', keywords: ['cancel'] }] } as unknown as OpenQ;
    const cov = heuristicCoverage(q, 'Children live in the PARENT scope');
    expect(cov).toEqual([true, false]);
    expect(scoreFromCoverage(cov)).toBe(0.5);
    expect(verdictFor(0.5)).toBe('partial');
    expect(verdictFor(0.75)).toBe('correct');
    expect(verdictFor(0.1)).toBe('incorrect');
  });
});

describe('session', () => {
  it('awards full XP for first-try answers and a bonus for a perfect lesson', () => {
    let s = createSession([mk('l/1'), mk('l/2')]);
    s = submit(submit(s, true), true);
    expect(isFinished(s)).toBe(true);
    expect(summarize(s)).toMatchObject({ accuracy: 1, perfect: true, xp: 30 });
  });
  it('re-queues a missed question at the end, with reduced XP when recovered', () => {
    let s = createSession([mk('l/1'), mk('l/2')]);
    s = submit(s, false);
    expect(s.queue.map((q) => q.id)).toEqual(['l/2', 'l/1']);
    s = submit(s, true);
    s = submit(s, true);
    const sum = summarize(s);
    expect(isFinished(s)).toBe(true);
    expect(sum).toMatchObject({ accuracy: 0.5, perfect: false, xp: 14, recovered: ['l/1'], correct: ['l/2', 'l/1'] });
  });
  it('stops re-queueing after MAX_RETRIES so a lesson always ends', () => {
    let s = createSession([mk('l/1')]);
    let guard = 0;
    while (!isFinished(s) && guard++ < 20) s = submit(s, false);
    expect(s.attempts['l/1']).toBe(MAX_RETRIES + 1);
    expect(currentQuestion(s)).toBeUndefined();
  });
});

describe('streaks & progress', () => {
  it('increments on consecutive days, holds within a day, resets after a gap', () => {
    expect(nextStreak(0, null, '2026-01-01')).toBe(1);
    expect(nextStreak(3, '2026-01-01', '2026-01-02')).toBe(4);
    expect(nextStreak(3, '2026-01-02', '2026-01-02')).toBe(3);
    expect(nextStreak(3, '2026-01-01', '2026-01-04')).toBe(1);
    expect(nextStreak(5, '2026-02-28', '2026-03-01')).toBe(6);
  });
  it('shows a lapsed streak as 0', () => {
    const p = { ...emptyProgress(), streak: 4, lastDay: '2026-01-01' };
    expect(liveStreak(p, '2026-01-02')).toBe(4);
    expect(liveStreak(p, '2026-01-03')).toBe(0);
  });
  it('records lessons, keeps the best accuracy, tracks and clears missed questions', () => {
    let p = recordResult(emptyProgress(), { lessonId: 'l', accuracy: 0.5, xp: 20, missed: ['l/1', 'l/2'], recovered: ['l/2'] }, '2026-01-01');
    expect(p.missed).toEqual(['l/1']);
    p = recordResult(p, { lessonId: 'l', accuracy: 0.25, xp: 10, missed: [], recovered: [], cleared: ['l/1'] }, '2026-01-02');
    expect(p).toMatchObject({ xp: 30, streak: 2, missed: [] });
    expect(p.lessons['l']).toMatchObject({ bestAccuracy: 0.5, completions: 2 });
  });
});

describe('course validation', () => {
  const course = (q: object) =>
    Course.parse({ units: [{ id: 'u', title: 'U', emoji: '1', description: 'd', lessons: [{ id: 'l', title: 'L', questions: [q, q, q] }] }] });
  const base = { prompt: 'p', explanation: 'e' };

  it('accepts a valid single question and derives ids', () => {
    const c = course({ ...base, type: 'single', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a' });
    expect(c.units[0]!.lessons[0]!.questions.map((q) => q.id)).toEqual(['l/1', 'l/2', 'l/3']);
    expect(validateCourse(c)).toEqual([]);
  });
  it('flags answers that are not options, and unbalanced blanks', () => {
    expect(validateCourse(course({ ...base, type: 'single', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'z' })).join()).toMatch(/not an option/);
    const fill = course({ ...base, type: 'fill', template: 'a [[1]] b [[3]]', bank: ['x', 'y', 'z'], answers: ['x', 'y'] });
    expect(validateCourse(fill).join()).toMatch(/blanks/);
    expect(blankIndexes('[[1]] and [[2]]')).toEqual([1, 2]);
  });
  it('requires a distractor in fill banks', () => {
    const fill = course({ ...base, type: 'fill', template: '[[1]] [[2]]', bank: ['x', 'y'], answers: ['x', 'y'] });
    expect(validateCourse(fill).join()).toMatch(/distractors/);
  });
});
