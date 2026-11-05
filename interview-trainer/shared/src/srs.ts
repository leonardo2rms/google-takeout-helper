import { addDays, daysBetween, type Day } from './day';
import { questionHash } from './hash';
import { MAX_INTERVAL, MASTERED_INTERVAL, type Card, type Confidence, type Progress, type QuestionId } from './progress';
import type { Question } from './schema';

export type Grade = 'again' | 'hard' | 'good' | 'easy';
/** first-try right | right after a retry | retries exhausted. */
export type Outcome = 'first' | 'retry' | 'failed';

export const NEW_EASE = 2.3;
export const MIN_EASE = 1.3;
export const MAX_EASE = 2.8;

export interface OutcomeSource {
  missed: readonly string[];
  recovered: readonly string[];
  /** Answered correctly at some point (first try or after a retry). */
  correct: readonly string[];
}

/** Splits a finished session into per-question outcomes. Seeing the feedback and then answering is not recall, so only first-try answers count as `first`. */
export function outcomesOf(s: OutcomeSource): Record<QuestionId, Outcome> {
  const out: Record<QuestionId, Outcome> = {};
  const missed = new Set(s.missed);
  const recovered = new Set(s.recovered);
  for (const id of s.correct) if (!missed.has(id)) out[id] = 'first';
  for (const id of s.missed) out[id] = recovered.has(id) ? 'retry' : 'failed';
  return out;
}

/**
 * retry|failed -> again. A first-try answer is `good`; "sure" makes it `easy`, a "guess" makes it `hard`.
 * Needing a hint caps the quality at `hard`: it was not recalled unaided, whatever the rating.
 */
export function gradeFor(o: Outcome, c?: Confidence, hinted = false): Grade {
  if (o !== 'first') return 'again';
  if (hinted || c === 1) return 'hard';
  if (c === 3) return 'easy';
  return 'good';
}

/** Wrong while "sure" (confidence 3): the surprise is what makes a correction stick, so the card comes back sooner and harder. */
export const isHypercorrection = (o: Outcome, c?: Confidence): boolean => o !== 'first' && c === 3;

const round2 = (n: number) => Math.round(n * 100) / 100;
const clampEase = (e: number) => Math.min(MAX_EASE, Math.max(MIN_EASE, round2(e)));

/** Longest interval allowed: global cap, and with an interview date, half the time left so each card gets two more looks. */
export function intervalCap(today: Day, deadline?: Day | null): number {
  if (!deadline) return MAX_INTERVAL;
  const left = daysBetween(today, deadline);
  if (left < 0) return MAX_INTERVAL; // date already passed: treated as unset
  return Math.min(MAX_INTERVAL, Math.max(1, Math.floor(left / 2)));
}

function freshCard(hash: string, today: Day): Card {
  return { h: hash, ease: NEW_EASE, interval: 0, due: today, reps: 0, lapses: 0, seen: 0, firstTry: 0, last: today };
}

/**
 * SM-2-lite, deterministic. Failures always reschedule to tomorrow; successes grow the interval
 * (1d, 3d, then interval x ease). A review that happens before the card is due does not grow the interval,
 * so replaying a lesson ten times cannot game the schedule.
 */
export function scheduleCard(card: Card | undefined, grade: Grade, today: Day, opts: { hash: string; deadline?: Day | null; hypercorrect?: boolean }): Card {
  const base = card && card.h === opts.hash ? card : undefined;
  // Clock moved backwards: behave as if it were still the day we last saw the card.
  const now = base && base.last > today ? base.last : today;
  const cur = base ?? freshCard(opts.hash, now);
  const seen = cur.seen + 1;
  const firstTry = cur.firstTry + (grade === 'again' ? 0 : 1);

  const { hc: _hc, ...plain } = cur; // any new outcome clears the hypercorrection flag; a fresh miss may set it again
  if (grade === 'again') {
    // Wrong while sure: due again today (a second look hours later), and a bigger ease penalty.
    if (opts.hypercorrect) return { ...plain, ease: clampEase(cur.ease - 0.35), interval: 1, due: now, reps: 0, lapses: cur.lapses + 1, seen, firstTry, last: now, hc: true };
    return { ...plain, ease: clampEase(cur.ease - 0.2), interval: 1, due: addDays(now, 1), reps: 0, lapses: cur.lapses + 1, seen, firstTry, last: now };
  }
  if (base && base.interval > 0 && base.due > now) {
    return { ...base, seen, firstTry, last: now }; // early review: bookkeeping only
  }

  let interval = cur.reps === 0 ? 1 : cur.reps === 1 ? 3 : Math.round(cur.interval * cur.ease);
  let ease = cur.ease;
  if (grade === 'hard') {
    interval = cur.reps === 0 ? 1 : Math.max(1, Math.round(cur.interval * 1.2));
    ease = cur.ease - 0.15;
  } else if (grade === 'easy') {
    interval = Math.max(interval + 1, Math.round(interval * 1.3));
    ease = cur.ease + 0.1;
  }
  interval = Math.max(1, Math.min(interval, intervalCap(now, opts.deadline)));
  return { ...plain, ease: clampEase(ease), interval, due: addDays(now, interval), reps: cur.reps + 1, seen, firstTry, last: now };
}

/** How well the card is remembered today, 0.1..1: full until due, then halves every `interval` days overdue. */
export function retention(card: Card, today: Day): number {
  if (card.due >= today) return 1;
  const overdue = daysBetween(card.due, today);
  return Math.max(0.1, 0.5 ** (overdue / Math.max(1, card.interval)));
}

/** 0..1; an unseen question is 0, a card reaches 1 at MASTERED_INTERVAL days. */
export function strength(card: Card | undefined, today: Day): number {
  if (!card || card.interval <= 0) return 0;
  return Math.min(1, card.interval / MASTERED_INTERVAL) * retention(card, today);
}

export const isDue = (card: Card, today: Day): boolean => card.due <= today;

/** The card for `q` if it still describes the same question, otherwise undefined. */
export function cardFor(p: Pick<Progress, 'cards'>, q: Question): Card | undefined {
  const c = p.cards[q.id];
  return c && c.h === questionHash(q) ? c : undefined;
}

export const lessonIdOf = (id: QuestionId): string => id.split('/')[0] ?? id;
export type UnitOf = (q: Question) => string;
export const unitByLesson: UnitOf = (q) => lessonIdOf(q.id);

const overdueRatio = (c: Card, today: Day) => Math.max(0, daysBetween(c.due, today)) / Math.max(1, c.interval);

/** Round-robin over groups, in the order the groups first appear. Keeps each group's internal order. */
function roundRobin<T>(items: readonly T[], keyOf: (t: T) => string): T[] {
  const groups = new Map<string, T[]>();
  for (const it of items) {
    const k = keyOf(it);
    const g = groups.get(k);
    if (g) g.push(it);
    else groups.set(k, [it]);
  }
  const lists = [...groups.values()];
  const out: T[] = [];
  for (let i = 0; out.length < items.length; i++) for (const l of lists) if (i < l.length) out.push(l[i]!);
  return out;
}

/** Questions whose card is due, most overdue first, spread across units (so the first few are never all one topic). */
export function dueQuestions(p: Progress, questions: readonly Question[], today: Day, unitOf: UnitOf = unitByLesson): Question[] {
  const due: { q: Question; c: Card }[] = [];
  for (const q of questions) {
    const c = cardFor(p, q);
    if (c && isDue(c, today)) due.push({ q, c });
  }
  due.sort((a, b) => Number(Boolean(b.c.hc)) - Number(Boolean(a.c.hc)) || overdueRatio(b.c, today) - overdueRatio(a.c, today) || (a.c.due < b.c.due ? -1 : a.c.due > b.c.due ? 1 : 0) || (a.q.id < b.q.id ? -1 : 1));
  return roundRobin(due.map((d) => d.q), unitOf);
}

export interface ReviewQueue {
  /** SRS-due cards, best first. */
  due: Question[];
  /** Missed questions that are not due yet (kept so "Review mistakes" never loses one). */
  missedOnly: Question[];
  /** due followed by missedOnly. */
  all: Question[];
}

/** The review queue: due cards first, then any missed question that is not already in it. De-duplicated. */
export function reviewQueue(p: Progress, questions: readonly Question[], today: Day, unitOf: UnitOf = unitByLesson): ReviewQueue {
  const due = dueQuestions(p, questions, today, unitOf);
  const inDue = new Set(due.map((q) => q.id));
  const missed = new Set(p.missed);
  const missedOnly = questions.filter((q) => missed.has(q.id) && !inDue.has(q.id));
  return { due, missedOnly, all: [...due, ...missedOnly] };
}

/**
 * Applies a finished session to the cards. Questions that no longer exist are ignored.
 * Returns a new object; the input is not modified.
 */
export function applyToCards(
  cards: Progress['cards'],
  outcomes: Record<QuestionId, Outcome>,
  questions: readonly Question[],
  today: Day,
  opts: { deadline?: Day | null; confidence?: Record<QuestionId, Confidence>; hinted?: readonly QuestionId[] } = {},
): Progress['cards'] {
  const byId = new Map(questions.map((q) => [q.id, q] as const));
  const next = { ...cards };
  for (const [id, outcome] of Object.entries(outcomes)) {
    const q = byId.get(id);
    if (!q) continue;
    const conf = opts.confidence?.[id];
    next[id] = scheduleCard(cards[id], gradeFor(outcome, conf, opts.hinted?.includes(id)), today, {
      hash: questionHash(q),
      deadline: opts.deadline,
      hypercorrect: isHypercorrection(outcome, conf),
    });
  }
  return next;
}

/** "In how many days will I see this again" if answered with `outcome` today. Used for the lesson feedback chip. */
export function previewInterval(p: Progress, q: Question, outcome: Outcome, today: Day): number {
  const card = scheduleCard(cardFor(p, q), gradeFor(outcome), today, { hash: questionHash(q), deadline: p.profile.interviewDate });
  return daysBetween(today, card.due);
}

export interface ForecastBucket {
  /** Days from today until these cards are due (1 = tomorrow). */
  inDays: number;
  count: number;
}

/** Groups the given questions' cards by how many days until they are due again. Cards already due are skipped. */
export function reviewForecast(cards: Progress['cards'], ids: readonly QuestionId[], today: Day): ForecastBucket[] {
  const counts = new Map<number, number>();
  for (const id of ids) {
    const c = cards[id];
    if (!c) continue;
    const d = daysBetween(today, c.due);
    if (d >= 1) counts.set(d, (counts.get(d) ?? 0) + 1);
  }
  return [...counts.entries()].sort((a, b) => a[0] - b[0]).map(([inDays, count]) => ({ inDays, count }));
}
