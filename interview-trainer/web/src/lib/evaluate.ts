import {
  PASS_SCORE,
  gradeFill,
  gradeMulti,
  gradeEstimate,
  gradeMatch,
  gradeOrder,
  gradeSingle,
  gradeSpot,
  heuristicCoverage,
  scoreFromCoverage,
  type CodeQ,
  type CodeRunResult,
  type GradeResponse,
  type OpenQ,
  type Question,
} from '@trainer/shared';

export type Answer =
  | { type: 'single'; selected: string }
  | { type: 'multi'; selected: string[] }
  | { type: 'fill'; filled: (string | null)[] }
  | { type: 'order'; arranged: string[] }
  | { type: 'spot'; selected: number[] }
  | { type: 'estimate'; value: number }
  | { type: 'match'; chosen: (string | null)[] }
  | { type: 'code'; source: string }
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
  /** Test results for `code` questions. */
  code?: CodeRunResult;
}

export type RunCodeFn = (q: CodeQ, source: string) => Promise<CodeRunResult>;

/** 0..1: share of rubric points covered (open) or all-or-nothing for everything else. */
export function outcomeScore(outcome: Outcome): number {
  return outcome.open ? scoreFromCoverage(outcome.open.covered) : outcome.correct ? 1 : 0;
}

export type GradeFn = (questionId: string, answer: string) => Promise<GradeResponse>;

/** Whether the outcome counts as a pass, reading the (possibly edited) checklist for open questions. */
export function isPass(outcome: Outcome): boolean {
  return outcome.open ? outcomeScore(outcome) >= PASS_SCORE : outcome.correct;
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
export async function evaluate(q: Question, a: Answer, grade?: GradeFn, runCode?: RunCodeFn): Promise<Outcome> {
  switch (q.type) {
    case 'single':
      return { correct: a.type === 'single' && gradeSingle(q, a.selected) };
    case 'multi':
      return { correct: a.type === 'multi' && gradeMulti(q, a.selected) };
    case 'fill':
      return { correct: a.type === 'fill' && gradeFill(q, a.filled) };
    case 'order':
      return { correct: a.type === 'order' && gradeOrder(q, a.arranged) };
    case 'spot':
      return { correct: a.type === 'spot' && gradeSpot(q, a.selected) };
    case 'estimate':
      return { correct: a.type === 'estimate' && gradeEstimate(q, a.value) };
    case 'match':
      return { correct: a.type === 'match' && gradeMatch(q, a.chosen) };
    case 'code': {
      if (a.type !== 'code' || !runCode) return { correct: false };
      const code = await runCode(q, a.source);
      return { correct: code.ok, code };
    }
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
