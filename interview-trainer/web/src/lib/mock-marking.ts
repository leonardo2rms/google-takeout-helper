import type { Course, MockResult, Question } from '@trainer/shared';
import { unitOfFor } from '@trainer/shared';
import { evaluate, outcomeScore, skipOutcome, type Answer, type GradeFn, type Outcome, type RunCodeFn } from './evaluate';

/** A code starter left untouched is not an answer; every other non-empty answer is. */
export function isAnswered(q: Question, a: Answer | null | undefined): boolean {
  if (!a) return false;
  if (a.type === 'code' && q.type === 'code') return a.source.trim() !== q.starter.trim();
  return true;
}

/**
 * Marks a finished mock, one question at a time. Answered questions go through the same grader as lessons (AI for open
 * questions when available, otherwise the self-assessment checklist); blank ones become "not answered". A grader that
 * throws never loses the mock: that question falls back to the offline checklist.
 */
export async function markMock(
  questions: readonly Question[],
  answers: Readonly<Record<string, Answer | null>>,
  grade: GradeFn | undefined,
  run: RunCodeFn | undefined,
  onProgress?: (done: number, total: number) => void,
): Promise<Record<string, Outcome>> {
  const out: Record<string, Outcome> = {};
  let done = 0;
  for (const q of questions) {
    const a = answers[q.id];
    if (isAnswered(q, a)) {
      try {
        out[q.id] = await evaluate(q, a!, grade, run);
      } catch {
        // Neither the AI grader nor the code runner may lose the mock: fall back to the offline checklist / a failed run.
        out[q.id] = await evaluate(q, a!, undefined, undefined);
      }
    } else {
      out[q.id] = skipOutcome(q);
    }
    onProgress?.(++done, questions.length);
  }
  return out;
}

/** The pure results the shared report is built from. `outcomes` may be edited later (self-assessment ticks). */
export function mockResults(
  course: Course,
  questions: readonly Question[],
  answers: Readonly<Record<string, Answer | null>>,
  outcomes: Readonly<Record<string, Outcome>>,
  flagged: ReadonlySet<string>,
): MockResult[] {
  const unitOf = unitOfFor(course);
  return questions.map((q) => {
    const answered = isAnswered(q, answers[q.id]);
    const o = outcomes[q.id];
    return { id: q.id, unitId: unitOf(q), answered, score: answered && o ? outcomeScore(o) : 0, flagged: flagged.has(q.id) };
  });
}
