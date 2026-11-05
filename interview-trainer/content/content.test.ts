import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { allQuestions, heuristicCoverage, openParts, runCodeTests, scoreFromCoverage, type CodeQ, type OpenQ } from '../shared/src';
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
  it.each(questions.filter((q): q is OpenQ => q.type === 'open').flatMap(openParts).map((q) => [q.id, q] as const))(
    'open %s: its model answer satisfies its own offline rubric',
    (_id, q) => {
      const covered = heuristicCoverage(q, q.modelAnswer);
      const missing = q.keyPoints.filter((_, i) => !covered[i]).map((k) => k.point);
      expect(missing).toEqual([]);
      expect(scoreFromCoverage(covered)).toBe(1);
    },
  );

  it('open rubrics (follow-ups included) do not trivially pass on an empty-ish answer', () => {
    for (const q of questions.filter((x): x is OpenQ => x.type === 'open').flatMap(openParts)) {
      expect(scoreFromCoverage(heuristicCoverage(q, 'I do not know'))).toBeLessThan(0.6);
    }
  });

  const codeQuestions = questions.filter((q): q is CodeQ => q.type === 'code');

  it.each(codeQuestions.map((q) => [q.id, q] as const))('code %s: the reference solution passes every test, starter does not', (_id, q) => {
    const solved = runCodeTests(q.solution, q.entry, q.tests);
    expect(solved.fatal).toBeUndefined();
    expect(solved.results.filter((r) => !r.passed)).toEqual([]);
    expect(solved.ok).toBe(true);

    // The starter must not be pre-solved, and must fail visibly rather than crash the harness.
    const starter = runCodeTests(q.starter, q.entry, q.tests);
    expect(starter.ok).toBe(false);
    expect(starter.fatal).toBeUndefined();
  });

  it('every code question carries a Kotlin equivalent and at least one hidden test', () => {
    for (const q of codeQuestions) {
      expect(q.kotlin, q.id).toBeTruthy();
      expect(q.tests.some((t) => t.hidden), q.id).toBe(true);
    }
  });

  it('prose is in one spelling: British, like the interface (behaviour, not behavior)', () => {
    const american = /\b(behavior\w*|favorite\w*|customiz\w*|organiz\w*|recogniz\w*|minimiz\w*|summariz\w*)\b/i;
    for (const q of questions) {
      const prose = q as unknown as Record<string, unknown>;
      for (const field of ['prompt', 'explanation', 'hint', 'modelAnswer'])
        if (typeof prose[field] === 'string') expect(prose[field], `${q.id} ${field}`).not.toMatch(american);
    }
  });

  it('every follow-up applies to at least one outcome of its main question (no dead branches)', () => {
    for (const q of questions.filter((x): x is OpenQ => x.type === 'open')) {
      for (const f of q.followUps) expect(f.point, `${q.id} -> "${f.prompt}"`).toBeLessThanOrEqual(q.keyPoints.length);
      if (q.followUps.length) {
        expect(new Set(q.followUps.map((f) => f.when)).size, `${q.id} should cover both a gap and a strength`).toBe(2);
      }
    }
  });
});
