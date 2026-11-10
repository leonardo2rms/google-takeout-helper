import { dayNumber, type Day } from './day';
import { goalToday } from './goal';
import { unitOfFor, weakSpots } from './mastery';
import type { Progress } from './progress';
import { mulberry32 } from './rng';
import { allQuestions, type Course, type Question } from './schema';
import { cardFor, dueQuestions } from './srs';
import { trackOrder } from './track';

export const DAILY_SIZE = 8;
export const DAILY_MAX_DUE = 5;
export const DAILY_WEAK = 2;

/**
 * Reorders `items` so that, where possible, no two neighbours share a key (greedy: always take from the
 * biggest remaining group that differs from the previous pick, which minimises the number of adjacent repeats).
 * Without a seed ties keep the original order; with a seed they are broken pseudo-randomly. Never drops or duplicates items.
 */
export function interleave<T>(items: readonly T[], keyOf: (t: T) => string, seed?: number): T[] {
  const groups = new Map<string, T[]>();
  for (const it of items) {
    const k = keyOf(it);
    const g = groups.get(k);
    if (g) g.push(it);
    else groups.set(k, [it]);
  }
  const rand = seed === undefined ? null : mulberry32(seed);
  const lists = [...groups.entries()].map(([key, list], i) => ({ key, list, pos: 0, rank: rand ? rand() : i }));
  const out: T[] = [];
  let last: string | null = null;
  while (out.length < items.length) {
    const open = lists.filter((l) => l.pos < l.list.length);
    const candidates = open.filter((l) => l.key !== last);
    const pool = candidates.length ? candidates : open;
    let best = pool[0]!;
    for (const l of pool) {
      const left = l.list.length - l.pos;
      const bestLeft = best.list.length - best.pos;
      if (left > bestLeft || (left === bestLeft && l.rank < best.rank)) best = l;
    }
    out.push(best.list[best.pos++]!);
    last = best.key;
  }
  return out;
}

export interface DailyPlan {
  /** first: nothing learned yet. daily: the normal session. bonus: nothing is due and today's goal is already met. */
  kind: 'first' | 'daily' | 'bonus';
  questions: Question[];
  mix: { due: number; weak: number; fresh: number; filler: number };
  minutes: number;
}

/**
 * Today's session: up to 5 due cards, 2 weak spots, the rest new material; backfilled due -> weak -> new -> random seen.
 * Deterministic for a given day and progress (seeded by the day number), interleaved so topics alternate.
 */
export function buildDailySession(course: Course, p: Progress, today: Day, opts: { size?: number } = {}): DailyPlan {
  const size = Math.max(1, opts.size ?? DAILY_SIZE);
  const all = allQuestions(course);
  const unitOf = unitOfFor(course);
  const seed = dayNumber(today);

  const dueList = dueQuestions(p, all, today, unitOf);
  const weakList = weakSpots(course, p, today, all.length, seed);
  const seenIds = new Set(all.filter((q) => cardFor(p, q)).map((q) => q.id));
  // New material follows the learner's track: the role's focus units first, each in course order (no role: course order).
  const freshList = trackOrder(course.units, p.profile.role).flatMap((u) => u.lessons.flatMap((l) => l.questions)).filter((q) => !seenIds.has(q.id));

  const chosen: Question[] = [];
  const ids = new Set<string>();
  const mix = { due: 0, weak: 0, fresh: 0, filler: 0 };
  const take = (list: readonly Question[], max: number, bucket: keyof typeof mix) => {
    for (const q of list) {
      if (max <= 0 || chosen.length >= size) return;
      if (ids.has(q.id)) continue;
      ids.add(q.id);
      chosen.push(q);
      mix[bucket]++;
      max--;
    }
  };

  take(dueList, DAILY_MAX_DUE, 'due');
  take(weakList, DAILY_WEAK, 'weak');
  take(freshList, size - chosen.length, 'fresh');
  // Backfill when a pool ran short.
  take(dueList, size, 'due');
  take(weakList, size, 'weak');
  const rand = mulberry32(seed + 1);
  take(
    all.filter((q) => seenIds.has(q.id)).map((q) => ({ q, k: rand() })).sort((a, b) => a.k - b.k).map((x) => x.q),
    size,
    'filler',
  );

  const questions = interleave(chosen, unitOf, seed);
  const kind: DailyPlan['kind'] = seenIds.size === 0 ? 'first' : dueList.length === 0 && goalToday(p, today).met ? 'bonus' : 'daily';
  return { kind, questions, mix, minutes: Math.max(1, Math.round(questions.length * 0.6)) };
}
