import { addDays, daysBetween, dayNumber, type Day } from './day';
import { CHALLENGE_KEPT } from './extras';
import { fnv1a } from './hash';
import type { Progress } from './progress';
import { allQuestions, type Course, type Question } from './schema';

/** Extra XP for passing the daily challenge (once a day, additive, never gating). */
export const CHALLENGE_XP = 15;

/**
 * How often each type is drawn, relative to the others. Senior-level depth (spotting a bug, back-of-the-envelope
 * numbers) is favoured; `match` is left out because it is a poor fit for a single featured question.
 */
export const CHALLENGE_WEIGHTS: Record<Question['type'], number> = {
  single: 1, multi: 1, fill: 1, order: 1, spot: 2, estimate: 2, match: 0, code: 1, open: 1,
};

function pool(course: Course): Question[] {
  const out: Question[] = [];
  for (const q of allQuestions(course)) for (let i = 0; i < CHALLENGE_WEIGHTS[q.type]; i++) out.push(q);
  return out;
}

const rawIndex = (day: Day, size: number): number => fnv1a(`challenge|${day}`) % size;
/** The chain of picks starts here, so every day's question is a pure function of the date (and never repeats the day before). */
const EPOCH: Day = '2024-01-01';

/**
 * The featured question of a local calendar day. A pure function of the date and the course, so it is the same for
 * everybody with the same content, needs no backend and never changes while the day lasts. It never repeats yesterday's:
 * the pick is walked forward day by day from a fixed start, nudging past a repeat.
 */
const cache = new WeakMap<Course, Map<Day, Question | null>>();

export function challengeQuestion(course: Course, day: Day): Question | null {
  let byDay = cache.get(course);
  if (!byDay) cache.set(course, (byDay = new Map()));
  if (!byDay.has(day)) byDay.set(day, pick(course, day));
  return byDay.get(day) ?? null;
}

function pick(course: Course, day: Day): Question | null {
  const qs = pool(course);
  if (qs.length === 0) return null;
  const distinct = new Set(qs.map((q) => q.id)).size > 1;
  const span = Math.max(0, daysBetween(EPOCH, day));
  let i = rawIndex(addDays(day, -span), qs.length);
  for (let n = span - 1; n >= 0; n--) {
    const next = rawIndex(addDays(day, -n), qs.length);
    i = distinct && qs[next]!.id === qs[i]!.id ? nextDifferent(qs, next) : next;
  }
  return qs[i]!;
}

function nextDifferent(qs: readonly Question[], from: number): number {
  let i = from;
  while (qs[i]!.id === qs[from]!.id) i = (i + 1) % qs.length;
  return i;
}

export const challengeResult = (p: Pick<Progress, 'challenge'>, day: Day): 'pass' | 'fail' | null => p.challenge[day] ?? null;

/** Records the day's result. Only the first result of a day counts (a replay earns nothing and changes nothing). */
export function recordChallenge(p: Progress, day: Day, pass: boolean): Progress {
  if (p.challenge[day]) return p;
  const challenge: Progress['challenge'] = { ...p.challenge, [day]: pass ? 'pass' : 'fail' };
  for (const d of Object.keys(challenge)) if (daysBetween(d, day) >= CHALLENGE_KEPT) delete challenge[d];
  return { ...p, challenge };
}

export interface ChallengeStreak {
  /** Consecutive days with a result, ending today (or yesterday while today is still open: it is not broken until the day passes). */
  current: number;
  best: number;
  /** Days with a result, and how many were passed. */
  done: number;
  passed: number;
}

/** The challenge streak counts days the challenge was taken, pass or fail, so a hard question never costs it. */
export function challengeStreak(p: Pick<Progress, 'challenge'>, today: Day): ChallengeStreak {
  const days = Object.keys(p.challenge).sort();
  let best = 0;
  let run = 0;
  let prev: number | null = null;
  for (const d of days) {
    const n = dayNumber(d);
    run = prev !== null && n === prev + 1 ? run + 1 : 1;
    prev = n;
    if (run > best) best = run;
  }
  let current = 0;
  let d = p.challenge[today] ? today : addDays(today, -1);
  while (p.challenge[d]) {
    current++;
    d = addDays(d, -1);
  }
  return { current, best, done: days.length, passed: days.filter((x) => p.challenge[x] === 'pass').length };
}
