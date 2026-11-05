import type { Day } from './day';
import { MASTERED_INTERVAL, type Progress, type QuestionId, type RoleId } from './progress';
import { mulberry32 } from './rng';
import type { Course, Question } from './schema';
import { allQuestions } from './schema';
import { cardFor, isDue, lessonIdOf, strength, type UnitOf } from './srs';

/** Unit weights per target role (default 1). Keyed by unit id. */
export const ROLE_WEIGHTS: Record<RoleId, Record<string, number>> = {
  'backend-kotlin': { kotlin: 1.5, coroutines: 1.5, spring: 1.5, jvm: 1.5 },
  'staff-architect': { ddd: 1.5, scale: 1.5, ai: 1.5 },
  generalist: {},
};

const unitIndexCache = new WeakMap<Course, Map<QuestionId, string>>();

/** question id -> unit id. */
export function unitIndex(course: Course): Map<QuestionId, string> {
  let idx = unitIndexCache.get(course);
  if (!idx) {
    idx = new Map();
    for (const u of course.units) for (const l of u.lessons) for (const q of l.questions) idx.set(q.id, u.id);
    unitIndexCache.set(course, idx);
  }
  return idx;
}

export function unitOfFor(course: Course): UnitOf {
  const idx = unitIndex(course);
  return (q) => idx.get(q.id) ?? lessonIdOf(q.id);
}

export interface TopicStat {
  unitId: string;
  title: string;
  /** Mean strength over ALL of the unit's questions; unseen ones count as 0. */
  strength: number;
  seen: number;
  total: number;
  due: number;
  mastered: number;
}

export function topicStats(course: Course, p: Progress, today: Day): TopicStat[] {
  return course.units.map((u) => {
    const qs = u.lessons.flatMap((l) => l.questions);
    let sum = 0;
    let seen = 0;
    let due = 0;
    let mastered = 0;
    for (const q of qs) {
      const c = cardFor(p, q);
      if (!c) continue;
      seen++;
      sum += strength(c, today);
      if (isDue(c, today)) due++;
      if (c.interval >= MASTERED_INTERVAL) mastered++;
    }
    return { unitId: u.id, title: u.title, strength: qs.length === 0 ? 0 : sum / qs.length, seen, total: qs.length, due, mastered };
  });
}

export type ReadinessLabel = 'early' | 'building' | 'solid' | 'ready';
export const READINESS_TEXT: Record<ReadinessLabel, string> = {
  early: 'Early estimate',
  building: 'Building',
  solid: 'Solid',
  ready: 'Interview-ready',
};
const LABELS: ReadinessLabel[] = ['early', 'building', 'solid', 'ready'];

export interface Readiness {
  /** 0-100: an estimate of how much of THIS COURSE you would recall today. Not a prediction of any outcome. */
  score: number;
  /** Share of the course's questions you have seen, 0..1. */
  coverage: number;
  label: ReadinessLabel;
  weakestUnitId: string | null;
}

/** Both coverage and score have to support a label, so "Interview-ready" cannot be reached by seeing everything once. */
export function labelFor(coverage: number, score: number): ReadinessLabel {
  const byCoverage = coverage < 0.3 ? 0 : coverage < 0.6 ? 1 : coverage < 0.8 ? 2 : 3;
  const byScore = score < 20 ? 0 : score < 45 ? 1 : score < 70 ? 2 : 3;
  return LABELS[Math.min(byCoverage, byScore)]!;
}

export function readiness(course: Course, p: Progress, today: Day, weights?: Record<string, number>): Readiness {
  const w = weights ?? (p.profile.role ? ROLE_WEIGHTS[p.profile.role] : {});
  const stats = topicStats(course, p, today).filter((s) => s.total > 0);
  let num = 0;
  let den = 0;
  let seen = 0;
  let total = 0;
  for (const s of stats) {
    const weight = w[s.unitId] ?? 1;
    num += weight * s.strength;
    den += weight;
    seen += s.seen;
    total += s.total;
  }
  const score = den === 0 ? 0 : Math.round((num / den) * 100);
  const coverage = total === 0 ? 0 : seen / total;
  const started = stats.filter((s) => s.seen > 0);
  const weakest = started.length ? started.reduce((a, b) => (b.strength < a.strength ? b : a)) : undefined;
  return { score, coverage, label: labelFor(coverage, score), weakestUnitId: weakest?.unitId ?? null };
}

function questionsOf(course: Course, unitId: string): Question[] {
  return course.units.find((u) => u.id === unitId)?.lessons.flatMap((l) => l.questions) ?? [];
}

/** How weak a seen question is: low strength, repeated lapses, or sitting in the missed pile. */
export function weakness(p: Progress, q: Question, today: Day): number {
  const c = cardFor(p, q);
  if (!c) return 0;
  return 1 - strength(c, today) + (0.3 * Math.min(c.lapses, 3)) / 3 + (p.missed.includes(q.id) ? 0.3 : 0) + (c.hc ? 0.2 : 0);
}

/**
 * The weakest seen questions: of the weakest unit first (or only of `unitId`), spilling over to the next-weakest
 * unit when it has too few. Ties are broken by `seed`. Empty when nothing has been seen.
 */
export function weakSpots(course: Course, p: Progress, today: Day, n: number, seed = 0, unitId?: string): Question[] {
  const rand = mulberry32(seed);
  const units = unitId
    ? [unitId]
    : topicStats(course, p, today)
        .filter((t) => t.seen > 0)
        .sort((a, b) => a.strength - b.strength)
        .map((t) => t.unitId);
  const out: Question[] = [];
  for (const id of units) {
    const scored = questionsOf(course, id)
      .filter((q) => cardFor(p, q))
      .map((q) => ({ q, w: weakness(p, q, today), tie: rand() }));
    scored.sort((a, b) => b.w - a.w || a.tie - b.tie);
    out.push(...scored.map((s) => s.q));
    if (out.length >= n) break;
  }
  return out.slice(0, n);
}

/** Mixed practice: a seeded draw without replacement, weighted by `1 + 2 * (1 - strength)` so weak material shows up more. */
export function pickMix(course: Course, p: Progress, today: Day, n: number, seed: number): Question[] {
  const rand = mulberry32(seed);
  const keyed = allQuestions(course).map((q) => {
    const weight = 1 + 2 * (1 - strength(cardFor(p, q), today));
    return { q, key: -Math.log(1 - rand()) / weight };
  });
  keyed.sort((a, b) => a.key - b.key);
  return keyed.slice(0, n).map((k) => k.q);
}
