import {
  PASS_SCORE,
  gradeFill,
  gradeMulti,
  gradeOrder,
  gradeSingle,
  heuristicCoverage,
  scoreFromCoverage,
  type GradeResponse,
  type OpenQ,
  type Question,
} from '@trainer/shared';

export type Answer =
  | { type: 'single'; selected: string }
  | { type: 'multi'; selected: string[] }
  | { type: 'fill'; filled: (string | null)[] }
  | { type: 'order'; arranged: string[] }
  | { type: 'open'; text: string };

export interface OpenOutcome {
  /** One flag per rubric key point. In self-assessment mode the learner can toggle these. */
  covered: boolean[];
  feedback?: string;
  gradedBy: 'ai' | 'self';
  /** True when AI grading was unavailable and the learner confirms the checklist themselves. */
  needsSelfAssessment: boolean;
}

export interface Outcome {
  correct: boolean;
  skipped?: boolean;
  open?: OpenOutcome;
}

export type GradeFn = (questionId: string, answer: string) => Promise<GradeResponse>;

/** Whether the outcome counts as a pass, reading the (possibly edited) checklist for open questions. */
export function isPass(outcome: Outcome): boolean {
  return outcome.open ? scoreFromCoverage(outcome.open.covered) >= PASS_SCORE : outcome.correct;
}

export const skipOutcome = (q: Question): Outcome =>
  q.type === 'open'
    ? { correct: false, skipped: true, open: { covered: q.keyPoints.map(() => false), gradedBy: 'self', needsSelfAssessment: false } }
    : { correct: false, skipped: true };

/**
 * Checks an answer. Choice/code challenges are graded locally and synchronously; open questions use
 * `grade` (the AI endpoint) when provided, and fall back to a keyword-prefilled self-assessment
 * checklist if it is missing or fails, so the app stays useful offline.
 */
export async function evaluate(q: Question, a: Answer, grade?: GradeFn): Promise<Outcome> {
  switch (q.type) {
    case 'single':
      return { correct: a.type === 'single' && gradeSingle(q, a.selected) };
    case 'multi':
      return { correct: a.type === 'multi' && gradeMulti(q, a.selected) };
    case 'fill':
      return { correct: a.type === 'fill' && gradeFill(q, a.filled) };
    case 'order':
      return { correct: a.type === 'order' && gradeOrder(q, a.arranged) };
    case 'open':
      return a.type === 'open' ? evaluateOpen(q, a.text, grade) : { correct: false };
  }
}

async function evaluateOpen(q: OpenQ, text: string, grade?: GradeFn): Promise<Outcome> {
  if (grade) {
    try {
      const res = await grade(q.id, text);
      if (res.covered.length === q.keyPoints.length) {
        return {
          correct: res.score >= PASS_SCORE,
          open: { covered: res.covered, feedback: res.feedback, gradedBy: 'ai', needsSelfAssessment: false },
        };
      }
    } catch {
      /* fall through to self-assessment */
    }
  }
  const covered = heuristicCoverage(q, text);
  return {
    correct: scoreFromCoverage(covered) >= PASS_SCORE,
    open: { covered, gradedBy: 'self', needsSelfAssessment: true },
  };
}
