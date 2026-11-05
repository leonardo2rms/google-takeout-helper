import { describe, expect, it } from 'vitest';
import type { Course, OpenQ, Question } from '@trainer/shared';
import { isAnswered, markMock, mockResults } from './mock-marking';
import type { Answer, GradeFn } from './evaluate';

const single = { id: 'u-1/1', type: 'single', prompt: 's', explanation: 'x', tags: [], difficulty: 'easy', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a' } as unknown as Question;
const code = { id: 'u-1/2', type: 'code', prompt: 'c', explanation: 'x', tags: [], starter: 'function f() {}\n', entry: 'f', tests: [], solution: '', difficulty: 'easy' } as unknown as Question;
const open = {
  id: 'u-1/3', type: 'open', prompt: 'o', tags: [], difficulty: 'easy', format: 'text', modelAnswer: 'retry with backoff', followUps: [], maxChars: 100,
  keyPoints: [{ point: 'backoff', keywords: ['backoff'] }, { point: 'jitter', keywords: ['jitter'] }],
} as unknown as OpenQ;
const questions: Question[] = [single, code, open];
const course = { units: [{ id: 'u', title: 'U', emoji: 'u', color: '#000', description: '', lessons: [{ id: 'u-1', title: 'L', questions }] }] } as unknown as Course;

const ans = (a: Answer | null, id: string) => ({ [id]: a });

describe('isAnswered', () => {
  it('blank, null and an untouched code starter are not answers', () => {
    expect(isAnswered(single, null)).toBe(false);
    expect(isAnswered(single, undefined)).toBe(false);
    expect(isAnswered(single, { type: 'single', selected: 'a' })).toBe(true);
    expect(isAnswered(code, { type: 'code', source: 'function f() {}  ' })).toBe(false);
    expect(isAnswered(code, { type: 'code', source: 'function f() { return 1 }' })).toBe(true);
  });
});

describe('markMock', () => {
  it('marks answered questions, leaves blank ones as skipped, and reports progress', async () => {
    const seen: number[] = [];
    const out = await markMock(questions, { ...ans({ type: 'single', selected: 'a' }, single.id), ...ans({ type: 'code', source: 'function f() {}' }, code.id) }, undefined, undefined, (d) => seen.push(d));
    expect(out[single.id]).toEqual({ correct: true });
    expect(out[code.id]).toMatchObject({ skipped: true });
    expect(out[open.id]).toMatchObject({ skipped: true, open: { needsSelfAssessment: false } });
    expect(seen).toEqual([1, 2, 3]);
  });

  it('uses the AI grader for open answers when there is one', async () => {
    const grade: GradeFn = async () => ({ score: 1, covered: [true, true], feedback: 'good', verdict: 'correct' }) as never;
    const out = await markMock([open], ans({ type: 'open', text: 'backoff and jitter' }, open.id), grade, undefined);
    expect(out[open.id]!.open).toMatchObject({ gradedBy: 'ai', covered: [true, true], needsSelfAssessment: false });
  });

  it('falls back to the offline checklist when the grader throws, and never loses the mock', async () => {
    const grade: GradeFn = async () => {
      throw new Error('down');
    };
    const out = await markMock([open], ans({ type: 'open', text: 'backoff' }, open.id), grade, undefined);
    expect(out[open.id]!.open).toMatchObject({ gradedBy: 'self', needsSelfAssessment: true, covered: [true, false] });
  });
});

describe('mockResults', () => {
  it('scores answered questions by outcome, blanks as zero, and carries flags and topics', async () => {
    const answers = { ...ans({ type: 'single', selected: 'a' }, single.id), ...ans({ type: 'open', text: 'backoff' }, open.id) };
    const outcomes = await markMock(questions, answers, undefined, undefined);
    const r = mockResults(course, questions, answers, outcomes, new Set([open.id]));
    expect(r.map((x) => [x.id, x.answered, x.score, x.flagged, x.unitId])).toEqual([
      [single.id, true, 1, false, 'u'],
      [code.id, false, 0, false, 'u'],
      [open.id, true, 0.5, true, 'u'],
    ]);
  });
});

describe('markMock: a failing runner or grader never loses the mock', () => {
  it('regression: a code runner that throws marks that question as failed and the rest are still marked', async () => {
    const run = async () => { throw new Error('worker exploded'); };
    const out = await markMock(questions, { ...ans({ type: 'single', selected: 'a' }, single.id), ...ans({ type: 'code', source: 'function f() { return 1 }' }, code.id) }, undefined, run);
    expect(out[single.id]!.correct).toBe(true);
    expect(out[code.id]).toEqual({ correct: false });
    expect(out[open.id]!.skipped).toBe(true);
  });

  it('a grader that throws synchronously falls back to the keyword checklist', async () => {
    const grade: GradeFn = () => { throw new Error('sync boom'); };
    const out = await markMock(questions, ans({ type: 'open', text: 'backoff and jitter' }, open.id), grade, undefined);
    expect(out[open.id]!.open).toMatchObject({ gradedBy: 'self', covered: [true, true] });
  });
});
