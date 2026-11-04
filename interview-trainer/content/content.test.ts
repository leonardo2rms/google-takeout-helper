import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { allQuestions, heuristicCoverage, scoreFromCoverage, type OpenQ } from '../shared/src';
import { loadCourse } from '../shared/src/load-content';

const course = loadCourse(resolve(dirname(fileURLToPath(import.meta.url))));
const questions = allQuestions(course);

describe('course content', () => {
  it('loads and has unique question ids', () => {
    expect(questions.length).toBeGreaterThan(100);
    expect(new Set(questions.map((q) => q.id)).size).toBe(questions.length);
  });

  it('every lesson mixes at least three question types', () => {
    for (const u of course.units)
      for (const l of u.lessons)
        expect(new Set(l.questions.map((q) => q.type)).size, l.id).toBeGreaterThanOrEqual(3);
  });

  it('every lesson has at least one open question for AI grading', () => {
    for (const u of course.units)
      for (const l of u.lessons) expect(l.questions.some((q) => q.type === 'open'), l.id).toBe(true);
  });

  // Guards the offline fallback: a perfect answer must tick every rubric point, otherwise a
  // learner with no network would be told that the model answer itself is incomplete.
  it.each(questions.filter((q): q is OpenQ => q.type === 'open').map((q) => [q.id, q] as const))(
    'open %s: its model answer satisfies its own offline rubric',
    (_id, q) => {
      const covered = heuristicCoverage(q, q.modelAnswer);
      const missing = q.keyPoints.filter((_, i) => !covered[i]).map((k) => k.point);
      expect(missing).toEqual([]);
      expect(scoreFromCoverage(covered)).toBe(1);
    },
  );

  it('open rubrics do not trivially pass on an empty-ish answer', () => {
    for (const q of questions) {
      if (q.type !== 'open') continue;
      expect(scoreFromCoverage(heuristicCoverage(q, 'I do not know'))).toBeLessThan(0.6);
    }
  });
});
