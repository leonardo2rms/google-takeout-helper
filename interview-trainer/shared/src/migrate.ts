import { addDays, daysBetween, isDay, type Day } from './day';
import { clampGoal } from './goal';
import { fnv1a, questionHash } from './hash';
import {
  DAYS_KEPT, DEFAULT_GOAL, MAX_FREEZES, MAX_INTERVAL, MAX_REST_DAYS, ROLE_IDS, emptyProgress,
  type Card, type DayLog, type LessonRecord, type Progress,
} from './progress';
import { normalizeExtras } from './extras';
import { rankFor } from './rank';
import type { Question } from './schema';
import { lessonIdOf, MAX_EASE, MIN_EASE, NEW_EASE } from './srs';

export interface MigrationContext {
  today: Day;
  /** allQuestions(course): used to seed cards and to drop cards whose question changed. */
  questions: readonly Question[];
}

export interface MigrationResult {
  progress: Progress;
  /** Which format was found: 0 = nothing usable. */
  from: 0 | 1 | 2;
  /** True when `progress` differs from what was stored, so the caller should write it back. */
  changed: boolean;
  /**
   * Set when the stored value could not be understood. The caller must NOT overwrite it
   * (a newer app version may have written it) without taking a backup first.
   */
  problem?: 'future-version' | 'corrupt';
}

type Obj = Record<string, unknown>;
const isObj = (x: unknown): x is Obj => typeof x === 'object' && x !== null && !Array.isArray(x);
const int = (x: unknown, min: number, max: number, dflt: number): number =>
  typeof x === 'number' && Number.isFinite(x) ? Math.min(max, Math.max(min, Math.floor(x))) : dflt;
const bool = (x: unknown, dflt: boolean): boolean => (typeof x === 'boolean' ? x : dflt);
const BIG = Number.MAX_SAFE_INTEGER;

export function sameJson(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== typeof b || a === null || b === null || typeof a !== 'object') return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  const ka = Object.keys(a as Obj);
  const kb = Object.keys(b as Obj);
  if (ka.length !== kb.length) return false;
  return ka.every((k) => k in (b as Obj) && sameJson((a as Obj)[k], (b as Obj)[k]));
}

function normalizeCards(raw: unknown, today: Day): Progress['cards'] {
  const out: Progress['cards'] = {};
  if (!isObj(raw)) return out;
  for (const [id, c] of Object.entries(raw)) {
    if (!isObj(c) || typeof c.h !== 'string' || c.h === '' || !isDay(c.due)) continue;
    // A due date far in the future can only come from clock skew: pull it back inside the interval cap.
    const due = daysBetween(today, c.due) > MAX_INTERVAL ? addDays(today, MAX_INTERVAL) : c.due;
    const card: Card = {
      h: c.h,
      ease: Math.round(Math.min(MAX_EASE, Math.max(MIN_EASE, typeof c.ease === 'number' && Number.isFinite(c.ease) ? c.ease : NEW_EASE)) * 100) / 100,
      interval: int(c.interval, 0, MAX_INTERVAL, 0),
      due,
      reps: int(c.reps, 0, BIG, 0),
      lapses: int(c.lapses, 0, BIG, 0),
      seen: int(c.seen, 0, BIG, 0),
      firstTry: int(c.firstTry, 0, BIG, 0),
      last: isDay(c.last) ? c.last : due,
    };
    if (c.hc === true) card.hc = true;
    out[id] = card;
  }
  return out;
}

function normalizeDays(raw: unknown, today: Day, target: number): Progress['days'] {
  const out: Progress['days'] = {};
  if (!isObj(raw)) return out;
  for (const [day, l] of Object.entries(raw)) {
    if (!isDay(day) || !isObj(l) || daysBetween(day, today) >= DAYS_KEPT) continue;
    const log: DayLog = {
      xp: int(l.xp, 0, BIG, 0),
      answered: int(l.answered, 0, BIG, 0),
      firstTry: int(l.firstTry, 0, BIG, 0),
      target: int(l.target, 1, 100, target),
    };
    if (l.comeback === true) log.comeback = true;
    out[day] = log;
  }
  return out;
}

function normalizeLessons(raw: unknown, today: Day): Progress['lessons'] {
  const out: Progress['lessons'] = {};
  if (!isObj(raw)) return out;
  for (const [id, r] of Object.entries(raw)) {
    if (!isObj(r)) continue;
    const rec: LessonRecord = {
      bestAccuracy: typeof r.bestAccuracy === 'number' && Number.isFinite(r.bestAccuracy) ? Math.min(1, Math.max(0, r.bestAccuracy)) : 0,
      completions: int(r.completions, 0, BIG, 0),
      lastDay: isDay(r.lastDay) ? r.lastDay : today,
    };
    out[id] = rec;
  }
  return out;
}

/**
 * Fills in anything missing and clamps/validates everything present (bad day strings, freezes above the cap,
 * negative xp, intervals above 120 ...). Never throws; unknown keys are dropped. Idempotent.
 */
export function normalizeProgress(input: unknown, ctx: MigrationContext): Progress {
  const d = emptyProgress();
  const p: Obj = isObj(input) ? input : {};
  const profile: Obj = isObj(p.profile) ? p.profile : {};
  const settings: Obj = isObj(p.settings) ? p.settings : {};
  const stats: Obj = isObj(p.stats) ? p.stats : {};
  const goal: Obj = isObj(p.goal) ? p.goal : {};
  const target = typeof goal.target === 'number' ? clampGoal(goal.target) : DEFAULT_GOAL;
  const streak = int(p.streak, 0, BIG, 0);
  const rest = Array.isArray(profile.restDays) ? [...new Set(profile.restDays.filter((x): x is number => Number.isInteger(x) && x >= 0 && x <= 6))].sort() : [];
  const milestones: Progress['milestones'] = {};
  if (isObj(p.milestones)) for (const [k, v] of Object.entries(p.milestones)) if (k.length > 0 && k.length <= 80) milestones[k] = isDay(v) ? v : ctx.today;

  return {
    version: 2,
    xp: int(p.xp, 0, BIG, 0),
    streak,
    lastDay: isDay(p.lastDay) ? p.lastDay : null,
    lessons: normalizeLessons(p.lessons, ctx.today),
    missed: Array.isArray(p.missed) ? [...new Set(p.missed.filter((x): x is string => typeof x === 'string' && x !== ''))] : [],
    longestStreak: Math.max(streak, int(p.longestStreak, 0, BIG, 0)),
    freezes: int(p.freezes, 0, MAX_FREEZES, 0),
    cards: normalizeCards(p.cards, ctx.today),
    days: normalizeDays(p.days, ctx.today, target),
    goal: { target },
    profile: {
      onboarded: bool(profile.onboarded, d.profile.onboarded),
      role: ROLE_IDS.find((r) => r === profile.role) ?? null,
      interviewDate: isDay(profile.interviewDate) ? profile.interviewDate : null,
      restDays: rest.slice(0, MAX_REST_DAYS),
      practiceTime: typeof profile.practiceTime === 'string' && /^([01]\d|2[0-3]):[0-5]\d$/.test(profile.practiceTime) ? profile.practiceTime : null,
    },
    settings: {
      haptics: bool(settings.haptics, d.settings.haptics),
      sound: bool(settings.sound, d.settings.sound),
      confetti: bool(settings.confetti, d.settings.confetti),
      tips: bool(settings.tips, d.settings.tips),
      confidence: bool(settings.confidence, d.settings.confidence),
    },
    stats: {
      sessions: int(stats.sessions, 0, BIG, 0),
      answered: int(stats.answered, 0, BIG, 0),
      firstTry: int(stats.firstTry, 0, BIG, 0),
      perfect: int(stats.perfect, 0, BIG, 0),
      practiceDays: int(stats.practiceDays, 0, BIG, 0),
    },
    milestones,
    ...normalizeExtras(p, ctx.today),
  };
}

/** Drops cards whose question is gone or was rewritten (hash mismatch) and days older than DAYS_KEPT. Never touches `missed`. */
export function pruneProgress(p: Progress, ctx: MigrationContext): Progress {
  let cards = p.cards;
  if (ctx.questions.length > 0) {
    // An empty question list would mean the content failed to load: pruning then would wipe every card.
    const hashes = new Map(ctx.questions.map((q) => [q.id, questionHash(q)] as const));
    cards = {};
    for (const [id, c] of Object.entries(p.cards)) if (hashes.get(id) === c.h) cards[id] = c;
  }
  const days: Progress['days'] = {};
  for (const [day, log] of Object.entries(p.days)) if (daysBetween(day, ctx.today) < DAYS_KEPT) days[day] = log;
  return { ...p, cards, days };
}

/** Cards for a v1 profile, so the review queue is useful on day one without an avalanche: due dates spread over a week. */
function seedCards(p: Progress, ctx: MigrationContext): Progress['cards'] {
  const cards: Progress['cards'] = {};
  const missed = new Set(p.missed);
  for (const q of ctx.questions) {
    const wasMissed = missed.has(q.id);
    if (!wasMissed && !((p.lessons[lessonIdOf(q.id)]?.completions ?? 0) > 0)) continue;
    cards[q.id] = wasMissed
      ? { h: questionHash(q), ease: 2.1, interval: 1, due: ctx.today, reps: 0, lapses: 1, seen: 1, firstTry: 0, last: ctx.today }
      : { h: questionHash(q), ease: NEW_EASE, interval: 3, due: addDays(ctx.today, 1 + (fnv1a(q.id) % 7)), reps: 1, lapses: 0, seen: 1, firstTry: 1, last: ctx.today };
  }
  return cards;
}

function fromV1(v1: Obj, ctx: MigrationContext): Progress {
  const base = normalizeProgress(
    { xp: v1.xp, streak: v1.streak, lastDay: v1.lastDay, lessons: v1.lessons, missed: v1.missed },
    ctx,
  );
  const hasHistory = base.lastDay !== null || base.xp > 0 || Object.keys(base.lessons).length > 0;
  const milestones = { ...base.milestones };
  for (let i = 0; i <= rankFor(base.xp).index; i++) milestones[`rank:${i}`] = ctx.today; // never celebrate retroactively
  if (hasHistory) milestones['gift:freeze'] = ctx.today;
  return {
    ...base,
    longestStreak: base.streak,
    freezes: hasHistory ? 1 : 0, // one-time welcome gift, announced once in the next summary
    cards: seedCards(base, ctx),
    stats: {
      ...base.stats,
      sessions: Object.values(base.lessons).reduce((n, l) => n + l.completions, 0),
      practiceDays: base.lastDay ? Math.max(1, base.streak) : 0, // a lower bound: honest rather than invented
    },
    milestones,
  };
}

const failed = (problem: MigrationResult['problem'], from: 0 | 1 | 2 = 0): MigrationResult => ({ progress: emptyProgress(), from, changed: false, problem });

/**
 * Accepts anything found in storage (a v1 or v2 object, a JSON string, garbage, null). Never throws; pure.
 * v1 -> v2 is lossless for the v1 fields and idempotent (migrating the result changes nothing).
 */
export function migrateProgress(raw: unknown, ctx: MigrationContext): MigrationResult {
  let value = raw;
  if (typeof value === 'string') {
    try {
      value = JSON.parse(value) as unknown;
    } catch {
      return failed('corrupt');
    }
  }
  if (value === null || value === undefined) return { progress: emptyProgress(), from: 0, changed: false };
  if (!isObj(value)) return failed('corrupt');

  if (value.version === 1) {
    return { progress: pruneProgress(fromV1(value, ctx), ctx), from: 1, changed: true };
  }
  if (value.version === 2) {
    const progress = pruneProgress(normalizeProgress(value, ctx), ctx);
    return { progress, from: 2, changed: !sameJson(progress, value) };
  }
  if (typeof value.version === 'number' && value.version > 2) return failed('future-version');
  return failed('corrupt');
}
