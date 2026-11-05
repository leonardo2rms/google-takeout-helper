import type { Confidence } from './progress';
import type { Question } from './schema';

/** A missed question comes back at the end of the lesson, at most this many extra times. */
export const MAX_RETRIES = 2;
export const XP_FIRST_TRY = 10;
export const XP_RETRY = 4;
export const XP_PERFECT_BONUS = 10;
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
}

export function createSession(questions: readonly Question[]): Session {
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

/** XP for a correct answer: 10 on the first try, 4 after a retry; a hint takes HINT_COST off, never below XP_MIN. */
export const xpFor = (attempt: number, hinted: boolean): number => {
  const base = attempt === 1 ? XP_FIRST_TRY : XP_RETRY;
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
    next.xp = s.xp + xpFor(attempt, hinted);
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
}

export function summarize(s: Session): SessionSummary {
  const accuracy = s.total === 0 ? 1 : s.firstTryCorrect / s.total;
  const perfect = s.total > 0 && s.firstTryCorrect === s.total;
  return {
    accuracy,
    perfect,
    xp: s.xp + (perfect ? XP_PERFECT_BONUS : 0),
    missed: s.missed,
    recovered: s.recovered,
    correct: s.correct,
    hinted: s.hinted,
    confidence: s.confidence,
  };
}
