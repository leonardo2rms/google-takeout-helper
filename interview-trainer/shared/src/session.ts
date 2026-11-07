import type { Confidence } from './progress';
import type { Question } from './schema';

/** A missed question comes back at the end of the lesson, at most this many extra times. */
export const MAX_RETRIES = 2;
export const XP_FIRST_TRY = 10;
export const XP_RETRY = 4;
export const XP_PERFECT_BONUS = 10;
/**
 * A question already answered earlier today pays this much for a correct answer, whatever the attempt: practising again
 * is welcome, but the full reward is for the first time each day (it also does not count again for the daily goal).
 */
export const XP_REPEAT = 2;
/** Revealing a hint costs this much of the question's XP (never below XP_MIN). */
export const HINT_COST = 3;
export const XP_MIN = 1;

export interface Session {
  queue: Question[];
  total: number;
  /** Distinct questions resolved (answered correctly, or retries exhausted). */
  resolved: number;
  firstTryCorrect: number;
  xp: number;
  attempts: Record<string, number>;
  /** Questions answered wrong at least once. */
  missed: string[];
  /** Questions that were missed earlier but eventually answered correctly. */
  recovered: string[];
  /** Every question answered correctly at some point (first try or after a retry). */
  correct: string[];
  /** Questions whose hint was revealed. */
  hinted: string[];
  /** The learner's rating given before the first check of a question (only when they chose to give one). */
  confidence: Record<string, Confidence>;
  /** Ids of this session's questions that were already answered earlier today: they pay `XP_REPEAT` and do not scale the perfect bonus. */
  repeats: string[];
}

/** `opts.repeats`: ids already answered today (see `alreadyDoneToday`); only those that are in `questions` are kept. */
export function createSession(questions: readonly Question[], opts: { repeats?: Iterable<string> } = {}): Session {
  const again = new Set(opts.repeats);
  return {
    queue: [...questions],
    total: questions.length,
    resolved: 0,
    firstTryCorrect: 0,
    xp: 0,
    attempts: {},
    missed: [],
    recovered: [],
    correct: [],
    hinted: [],
    confidence: {},
    repeats: questions.filter((q) => again.has(q.id)).map((q) => q.id),
  };
}

export const currentQuestion = (s: Session): Question | undefined => s.queue[0];
export const isFinished = (s: Session): boolean => s.queue.length === 0;
export const progress = (s: Session): number => (s.total === 0 ? 1 : s.resolved / s.total);

export interface SubmitOptions {
  /** The hint was revealed for this question (now or on an earlier attempt). */
  hinted?: boolean;
  /** "How sure are you?" given before checking. Only the first attempt's rating is kept. */
  confidence?: Confidence;
}

/**
 * XP for a correct answer: 10 on the first try, 4 after a retry, and at most `XP_REPEAT` for a question already answered
 * today; a hint takes HINT_COST off, never below XP_MIN.
 */
export const xpFor = (attempt: number, hinted: boolean, repeat = false): number => {
  const base = Math.min(attempt === 1 ? XP_FIRST_TRY : XP_RETRY, repeat ? XP_REPEAT : Infinity);
  return hinted ? Math.max(XP_MIN, base - HINT_COST) : base;
};

/** Records the outcome for the current question and moves on. Pure: returns a new Session. */
export function submit(s: Session, correct: boolean, opts: SubmitOptions = {}): Session {
  const q = s.queue[0];
  if (!q) return s;
  const attempt = (s.attempts[q.id] ?? 0) + 1;
  const queue = s.queue.slice(1);
  const hinted = Boolean(opts.hinted);
  const next: Session = {
    ...s,
    queue,
    attempts: { ...s.attempts, [q.id]: attempt },
    hinted: hinted && !s.hinted.includes(q.id) ? [...s.hinted, q.id] : s.hinted,
    confidence: opts.confidence && !(q.id in s.confidence) ? { ...s.confidence, [q.id]: opts.confidence } : s.confidence,
  };

  if (correct) {
    next.resolved = s.resolved + 1;
    next.correct = [...s.correct, q.id];
    next.xp = s.xp + xpFor(attempt, hinted, s.repeats.includes(q.id));
    if (attempt === 1) next.firstTryCorrect = s.firstTryCorrect + 1;
    else next.recovered = [...s.recovered, q.id];
    return next;
  }

  if (!s.missed.includes(q.id)) next.missed = [...s.missed, q.id];
  if (attempt <= MAX_RETRIES) next.queue = [...queue, q];
  else next.resolved = s.resolved + 1;
  return next;
}

export interface SessionSummary {
  accuracy: number;
  xp: number;
  perfect: boolean;
  missed: string[];
  recovered: string[];
  /** Ids answered correctly this session; lets review sessions clear them from the missed pile. */
  correct: string[];
  /** Ids whose hint was revealed: their review quality is capped at "hard". */
  hinted: string[];
  /** Ratings given before checking. */
  confidence: Record<string, Confidence>;
  /** The perfect-lesson bonus included in `xp` (0 unless perfect; smaller when some questions were already done today). */
  bonus: number;
  /** How many of the session's questions had already been answered earlier today. */
  repeats: number;
}

export function summarize(s: Session): SessionSummary {
  const accuracy = s.total === 0 ? 1 : s.firstTryCorrect / s.total;
  const perfect = s.total > 0 && s.firstTryCorrect === s.total;
  // The bonus is for a lesson done well, not for doing the same one again: it scales with the share of new questions.
  const bonus = perfect ? Math.round((XP_PERFECT_BONUS * (s.total - s.repeats.length)) / s.total) : 0;
  return {
    accuracy,
    perfect,
    bonus,
    repeats: s.repeats.length,
    xp: s.xp + bonus,
    missed: s.missed,
    recovered: s.recovered,
    correct: s.correct,
    hinted: s.hinted,
    confidence: s.confidence,
  };
}
