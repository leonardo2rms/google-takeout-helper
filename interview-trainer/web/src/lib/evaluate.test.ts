import { describe, expect, it, vi } from 'vitest';
import { Course, allQuestions, type OpenQ, type Question } from '@trainer/shared';
import { evaluate, isPass, skipOutcome } from './evaluate';

const course = Course.parse({
  units: [{
    id: 'u', title: 'U', emoji: 'x', description: 'd',
    lessons: [{
      id: 'l', title: 'L',
      questions: [
        { type: 'single', prompt: 'p', explanation: 'e', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a' },
        { type: 'multi', prompt: 'p', explanation: 'e', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }, { id: 'c', text: 'C' }], answers: ['a', 'b'] },
        { type: 'open', prompt: 'p', modelAnswer: 'm', keyPoints: [{ point: 'one', keywords: ['alpha'] }, { point: 'two', keywords: ['beta'] }] },
      ],
    }],
  }],
});
const [single, multi, open] = allQuestions(course) as [Question, Question, OpenQ];

describe('evaluate', () => {
  it('grades choices locally', async () => {
    expect((await evaluate(single, { type: 'single', selected: 'a' })).correct).toBe(true);
    expect((await evaluate(single, { type: 'single', selected: 'b' })).correct).toBe(false);
    expect((await evaluate(multi, { type: 'multi', selected: ['b', 'a'] })).correct).toBe(true);
    expect((await evaluate(multi, { type: 'multi', selected: ['a'] })).correct).toBe(false);
  });

  it('uses AI grading for open questions when available', async () => {
    const grade = vi.fn().mockResolvedValue({ score: 0.5, covered: [true, false], feedback: 'good start', graded_by: 'ai' });
    const out = await evaluate(open, { type: 'open', text: 'alpha and beta' }, grade);
    expect(grade).toHaveBeenCalledWith(open.id, 'alpha and beta');
    expect(out.open).toMatchObject({ gradedBy: 'ai', needsSelfAssessment: false, covered: [true, false], feedback: 'good start' });
    expect(isPass(out)).toBe(false); // 0.5 < PASS_SCORE
  });

  it('falls back to a keyword-prefilled self-assessment when grading fails or is absent', async () => {
    const failing = vi.fn().mockRejectedValue(new Error('503'));
    for (const grade of [failing, undefined]) {
      const out = await evaluate(open, { type: 'open', text: 'ALPHA only' }, grade);
      expect(out.open).toMatchObject({ gradedBy: 'self', needsSelfAssessment: true, covered: [true, false] });
    }
  });

  it('ignores an AI response whose rubric length does not match', async () => {
    const grade = vi.fn().mockResolvedValue({ score: 1, covered: [true], feedback: '', graded_by: 'ai' });
    const out = await evaluate(open, { type: 'open', text: 'x' }, grade);
    expect(out.open?.gradedBy).toBe('self');
  });

  it('lets self-assessment edits decide the pass/fail result', async () => {
    const out = await evaluate(open, { type: 'open', text: 'nothing relevant' });
    expect(isPass(out)).toBe(false);
    out.open!.covered = [true, true];
    expect(isPass(out)).toBe(true);
  });

  it('skipping fails the question and covers nothing', () => {
    expect(isPass(skipOutcome(single))).toBe(false);
    expect(skipOutcome(open).open?.covered).toEqual([false, false]);
  });
});
