import { cleanFeedback, tagPattern } from './grading-prompt';
import type { OpenQ } from './schema';

/**
 * Plausibility of an AI verdict. A model (or a hosted `sample` call) can return something that parses and has the right
 * shape and is still not a grade: all points false with empty feedback, `covered: "yes"`, a score of 7, text that echoes an
 * injected instruction. Showing that as a confident "INCORRECT 0/3" is worse than saying the AI could not grade: the
 * graders run every verdict through `checkVerdict`, and the app falls back to self-check when it is not plausible.
 */

/** Thrown by a grader when the verdict it received cannot be trusted. The app then asks the learner to check themselves. */
export class ImplausibleVerdictError extends Error {
  constructor(reason: string) {
    super(reason);
    this.name = 'ImplausibleVerdictError';
  }
}

export type VerdictCheck = { ok: true; covered: boolean[]; feedback: string } | { ok: false; reason: string };

/** Shortest feedback that is more than a token. The prompt asks for 1-3 sentences. */
export const MIN_FEEDBACK_CHARS = 10;

const PLACEHOLDER = /^(null|undefined|n\/a|none|todo|tbd|string|feedback|\[object object\]|lorem ipsum)\W*$/i;

/** Text that talks to the grader or the pipeline instead of the learner. Narrow on purpose: a false alarm only costs a self-check. */
const INJECTION: RegExp[] = [
  /\bignore\s+(?:all\s+|any\s+|the\s+|your\s+)?(?:previous|prior|above|earlier|preceding)\s+(?:instructions?|prompts?|rules?|messages?)/i,
  /\b(?:mark|grade|score|rate)\s+(?:this|it|me|my\s+answer|the\s+answer|everything|all)\s+(?:as\s+)?(?:correct|right|perfect|full)/i,
  /\b(?:give|award)\s+(?:me\s+)?(?:full\s+marks?|a\s+perfect\s+score|100\s*%)/i,
  /\ball\s+(?:rubric\s+|key\s+)?points\s+(?:are\s+|were\s+)?covered\b/i,
  tagPattern('candidate_answer'),
];

const injectionLike = (text: string): boolean => INJECTION.some((p) => new RegExp(p.source, p.flags.replace('g', '')).test(text.normalize('NFKC')));

const wordCount = (s: string): number => s.trim().split(/\s+/).filter(Boolean).length;

/**
 * Validates a raw verdict `{ points: [{ covered: boolean }], feedback: string, score? }` against the rubric of `q` and the
 * answer that was graded. On success returns the cleaned flags and feedback; otherwise says why it is not to be trusted.
 */
export function checkVerdict(q: Pick<OpenQ, 'keyPoints'>, answer: string, raw: unknown): VerdictCheck {
  const bad = (reason: string): VerdictCheck => ({ ok: false, reason });
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) return bad('the verdict is not an object');
  const v = raw as { points?: unknown; feedback?: unknown; score?: unknown };
  const n = q.keyPoints.length;

  if (!Array.isArray(v.points) || v.points.length !== n) {
    return bad(`Expected ${n} rubric verdicts, got ${Array.isArray(v.points) ? v.points.length : 'none'}`);
  }
  const covered: boolean[] = [];
  for (const p of v.points) {
    const flag = typeof p === 'object' && p !== null ? (p as { covered?: unknown }).covered : undefined;
    if (typeof flag !== 'boolean') return bad('a rubric verdict has no true/false "covered"');
    covered.push(flag);
  }
  const hits = covered.filter(Boolean).length;

  if (v.score !== undefined) {
    if (typeof v.score !== 'number' || !Number.isFinite(v.score) || v.score < 0 || v.score > 1) return bad('the score is outside 0..1');
    if (Math.abs(v.score - hits / n) > 0.01) return bad('the score disagrees with the rubric verdicts');
  }

  if (typeof v.feedback !== 'string') return bad('the feedback is not text');
  const feedback = cleanFeedback(v.feedback);
  if (feedback.length < MIN_FEEDBACK_CHARS || !/\p{L}{3,}/u.test(feedback) || PLACEHOLDER.test(feedback)) return bad('the feedback is empty or a placeholder');
  if (injectionLike(feedback)) return bad('the feedback repeats instructions instead of addressing the answer');

  if (hits > 0 && answer.trim().length < 3) return bad('points were credited to an empty answer');
  if (hits >= 2 && wordCount(answer) < 3) return bad('several points were credited to an answer of a couple of words');
  if (hits > 0 && injectionLike(answer)) return bad('the answer contains instructions to the grader yet was credited');

  return { ok: true, covered, feedback };
}
