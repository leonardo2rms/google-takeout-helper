import type { Question } from './schema';

/** A missed question comes back at the end of the lesson, at most this many extra times. */
export const MAX_RETRIES = 2;
export const XP_FIRST_TRY = 10;
export const XP_RETRY = 4;
export const XP_PERFECT_BONUS = 10;

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
  };
}

export const currentQuestion = (s: Session): Question | undefined => s.queue[0];
export const isFinished = (s: Session): boolean => s.queue.length === 0;
export const progress = (s: Session): number => (s.total === 0 ? 1 : s.resolved / s.total);

/** Records the outcome for the current question and moves on. Pure: returns a new Session. */
export function submit(s: Session, correct: boolean): Session {
  const q = s.queue[0];
  if (!q) return s;
  const attempt = (s.attempts[q.id] ?? 0) + 1;
  const queue = s.queue.slice(1);
  const next: Session = { ...s, queue, attempts: { ...s.attempts, [q.id]: attempt } };

  if (correct) {
    next.resolved = s.resolved + 1;
    next.correct = [...s.correct, q.id];
    if (attempt === 1) {
      next.firstTryCorrect = s.firstTryCorrect + 1;
      next.xp = s.xp + XP_FIRST_TRY;
    } else {
      next.xp = s.xp + XP_RETRY;
      next.recovered = [...s.recovered, q.id];
    }
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
  };
}
