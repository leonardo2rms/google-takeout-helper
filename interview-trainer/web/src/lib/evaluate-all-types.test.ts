/**
 * `evaluate` for every question type, using real questions from the course so the test also proves each type's
 * correct answer, read straight from the content, passes the whole web-side path.
 */
import { describe, expect, it, vi } from 'vitest';
import { PASS_SCORE, allQuestions, runCodeTests, spotLines, type CodeQ, type Course, type OpenQ, type Question } from '@trainer/shared';
import course from 'virtual:course';
import { evaluate, isPass, outcomeScore, skipOutcome, type Answer, type Outcome, type RunCodeFn } from './evaluate';

const all = allQuestions(course as Course);
const first = <T extends Question['type']>(type: T) => all.find((q) => q.type === type) as Extract<Question, { type: T }>;

/** The correct answer for any non-open question, from the content. */
function correctAnswer(q: Exclude<Question, OpenQ>): Answer {
  switch (q.type) {
    case 'single': return { type: 'single', selected: q.answer };
    case 'multi': return { type: 'multi', selected: [...q.answers].reverse() };
    case 'fill': return { type: 'fill', filled: [...q.answers] };
    case 'order': return { type: 'order', arranged: [...q.lines] };
    case 'spot': return { type: 'spot', selected: [...q.answers] };
    case 'estimate': return { type: 'estimate', value: q.answer };
    case 'match': return { type: 'match', chosen: q.pairs.map((p) => p.right) };
    case 'code': return { type: 'code', source: q.solution };
  }
}

const localRunner: RunCodeFn = async (q: CodeQ, source: string) => runCodeTests(source, q.entry, q.tests);

describe('every question type, correct answer from the content', () => {
  it.each(['single', 'multi', 'fill', 'order', 'spot', 'estimate', 'match', 'code'] as const)('%s passes with its key', async (type) => {
    const q = first(type) as Exclude<Question, OpenQ>;
    const out = await evaluate(q, correctAnswer(q), undefined, localRunner);
    expect(out.correct).toBe(true);
    expect(isPass(out)).toBe(true);
    expect(outcomeScore(out)).toBe(1);
  });

  it('the first question of every type in the course passes, not just one example', async () => {
    let checked = 0;
    for (const q of all) {
      if (q.type === 'open') continue;
      const out = await evaluate(q, correctAnswer(q), undefined, localRunner);
      expect(out.correct, q.id).toBe(true);
      checked++;
    }
    expect(checked).toBeGreaterThan(100);
  });
});

describe('wrong or mismatched answers', () => {
  it('rejects an answer of the wrong shape for the question (never throws)', async () => {
    const wrong: Answer = { type: 'estimate', value: 1 };
    for (const type of ['single', 'multi', 'fill', 'order', 'spot', 'match', 'code'] as const) {
      const out = await evaluate(first(type), wrong, undefined, localRunner);
      expect(out.correct, type).toBe(false);
    }
    expect((await evaluate(first('estimate'), { type: 'single', selected: 'a' })).correct).toBe(false);
    expect((await evaluate(first('open'), { type: 'single', selected: 'a' })).correct).toBe(false);
  });

  it('grades obviously wrong answers of the right shape as wrong', async () => {
    const spot = first('spot');
    const wrongLine = spotLines(spot.source).findIndex((_, i) => !spot.answers.includes(i + 1)) + 1;
    expect((await evaluate(spot, { type: 'spot', selected: [wrongLine] })).correct).toBe(false);
    expect((await evaluate(spot, { type: 'spot', selected: [] })).correct).toBe(false);
    const est = first('estimate');
    expect((await evaluate(est, { type: 'estimate', value: est.answer * 10 + 7 })).correct).toBe(false);
    expect((await evaluate(est, { type: 'estimate', value: Number.NaN })).correct).toBe(false);
    const match = first('match');
    expect((await evaluate(match, { type: 'match', chosen: match.pairs.map(() => null) })).correct).toBe(false);
  });
});

describe('code questions', () => {
  const code = first('code');

  it('fails without a runner instead of pretending to run', async () => {
    expect(await evaluate(code, { type: 'code', source: code.solution })).toEqual({ correct: false });
  });

  it('keeps the run result for display and passes the learner source through untouched', async () => {
    const run = vi.fn<RunCodeFn>().mockResolvedValue({ ok: false, results: [{ hidden: false, passed: false, call: 'f(1)' }] });
    const out = await evaluate(code, { type: 'code', source: 'broken(' }, undefined, run);
    expect(run).toHaveBeenCalledWith(code, 'broken(');
    expect(out).toEqual({ correct: false, code: { ok: false, results: [{ hidden: false, passed: false, call: 'f(1)' }] } });
  });

  it('a starter with no body fails with a readable fatal, not an exception', async () => {
    const out = await evaluate(code, { type: 'code', source: code.starter }, undefined, localRunner);
    expect(out.correct).toBe(false);
  });
});

describe('outcomeScore and isPass', () => {
  const open = first('open');
  const mk = (covered: boolean[]): Outcome => ({ correct: false, open: { covered, gradedBy: 'self', needsSelfAssessment: true } });

  it('scores open outcomes by coverage and others all-or-nothing', () => {
    expect(outcomeScore(mk([true, false, false, false]))).toBe(0.25);
    expect(outcomeScore({ correct: true })).toBe(1);
    expect(outcomeScore({ correct: false })).toBe(0);
    expect(outcomeScore(mk([]))).toBe(0);
  });

  it('passes an open outcome exactly at the pass threshold, using the possibly edited checklist', () => {
    const n = open.keyPoints.length;
    const needed = Math.ceil(PASS_SCORE * n);
    const cover = (k: number) => mk(Array.from({ length: n }, (_, i) => i < k));
    expect(isPass(cover(needed))).toBe(true);
    if (needed > 0) expect(isPass(cover(needed - 1))).toBe(false);
    // The stored `correct` flag does not matter for open questions: the checklist decides.
    expect(isPass({ ...cover(n), correct: false })).toBe(true);
    expect(isPass({ ...cover(0), correct: true })).toBe(false);
  });

  it('skipOutcome never passes, for any type, and gives open questions an all-false checklist of the right length', () => {
    for (const q of all.slice(0, 40)) {
      const s = skipOutcome(q);
      expect(isPass(s)).toBe(false);
      expect(s.skipped).toBe(true);
      if (q.type === 'open') expect(s.open!.covered).toHaveLength(q.keyPoints.length);
    }
  });
});

describe('open questions: AI grading edge cases', () => {
  const open = first('open');
  const answer: Answer = { type: 'open', text: 'some text' };

  it('passes on the AI score at the threshold and records the AI feedback', async () => {
    const n = open.keyPoints.length;
    const covered = Array.from({ length: n }, () => true);
    const grade = vi.fn().mockResolvedValue({ score: 1, covered, feedback: 'great', graded_by: 'ai' });
    const out = await evaluate(open, answer, grade);
    expect(out).toMatchObject({ correct: true, open: { gradedBy: 'ai', feedback: 'great', needsSelfAssessment: false } });
  });

  it('falls back to self-assessment on a thrown synchronous-looking rejection and on an empty rubric answer', async () => {
    const grade = vi.fn().mockResolvedValue({ score: 1, covered: [], feedback: '', graded_by: 'ai' });
    expect((await evaluate(open, answer, grade)).open?.gradedBy).toBe('self');
    const throwing = vi.fn(() => Promise.reject(new TypeError('Failed to fetch')));
    expect((await evaluate(open, answer, throwing)).open?.needsSelfAssessment).toBe(true);
  });

  it('prefills the self-assessment from keywords, case-insensitively', async () => {
    const keyword = open.keyPoints[0]!.keywords[0]!;
    const out = await evaluate(open, { type: 'open', text: keyword.toUpperCase() }, undefined);
    expect(out.open!.covered[0]).toBe(true);
  });
});
