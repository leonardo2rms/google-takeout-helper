import { daysBetween, isDay, type Day } from './day';
import type { QuestionId } from './progress';

/**
 * Additive Progress fields for confidence ratings, the library, the daily challenge, achievements and mock interviews.
 * They live apart from the v2 core so they can be normalised, merged and exported with a handful of one-line hooks
 * (`emptyExtras`, `normalizeExtras`, `mergeExtras`). They are optional in stored data and get defaults on load, so adding
 * them needs no version bump.
 */

export const MAX_NOTE_CHARS = 500;
export const MAX_NOTES = 300;
export const MAX_BOOKMARKS = 500;
export const MAX_MOCKS = 10;
/** The challenge log keeps this many days (a streak and badges never need more). */
export const CHALLENGE_KEPT = 400;

/** A star. `h` is the question hash: if the prompt is rewritten the star no longer applies (it is kept, in case the text is restored). */
export interface BookmarkRec {
  h: string;
  day: Day;
}

export interface NoteRec {
  h: string;
  text: string;
  day: Day;
}

/** One finished mock interview. */
export interface MockRecord {
  day: Day;
  /** Questions in the mock. */
  total: number;
  /** Questions the learner gave an answer to (not skipped, not left blank). */
  answered: number;
  /** 0..100: the mean score over ALL questions, so unanswered ones count as zero. */
  score: number;
  limitMin: number;
  usedSec: number;
  timedOut: boolean;
  /** Per topic (unit id): questions asked and the summed score (0..n). */
  byUnit: Record<string, { n: number; score: number }>;
}

export interface Calibration {
  /** Ratings given per level: [guess, think so, sure]. */
  n: [number, number, number];
  /** Of those, the first-try right ones. */
  right: [number, number, number];
}

export interface ProgressExtras {
  calibration: Calibration;
  bookmarks: Record<QuestionId, BookmarkRec>;
  notes: Record<QuestionId, NoteRec>;
  /** Daily challenge result per local day. */
  challenge: Record<Day, 'pass' | 'fail'>;
  /** Badge id -> the day it was unlocked. */
  achievements: Record<string, Day>;
  /** Newest first, at most MAX_MOCKS. */
  mocks: MockRecord[];
  /** Question type -> distinct questions answered correctly in a session (any attempt). */
  passedByType: Record<string, number>;
}

export const emptyExtras = (): ProgressExtras => ({
  calibration: { n: [0, 0, 0], right: [0, 0, 0] },
  bookmarks: {},
  notes: {},
  challenge: {},
  achievements: {},
  mocks: [],
  passedByType: {},
});

type Obj = Record<string, unknown>;
/** Keys that must never be copied into a plain object: assigning `__proto__` would swap the object's prototype. */
export const safeKey = (k: string): boolean => k !== '__proto__';
const isObj = (x: unknown): x is Obj => typeof x === 'object' && x !== null && !Array.isArray(x);
const nat = (x: unknown, max = Number.MAX_SAFE_INTEGER): number => (typeof x === 'number' && Number.isFinite(x) ? Math.min(max, Math.max(0, Math.floor(x))) : 0);
const triple = (x: unknown): [number, number, number] => {
  const a = Array.isArray(x) ? x : [];
  return [nat(a[0]), nat(a[1]), nat(a[2])];
};
const KEY_MAX = 80;

function normalizeMock(raw: unknown): MockRecord | null {
  if (!isObj(raw) || !isDay(raw.day)) return null;
  const total = nat(raw.total, 200);
  if (total === 0) return null;
  const byUnit: MockRecord['byUnit'] = {};
  if (isObj(raw.byUnit)) {
    for (const [id, v] of Object.entries(raw.byUnit)) {
      if (id.length > KEY_MAX || !safeKey(id) || !isObj(v)) continue;
      const n = nat(v.n, 200);
      const score = typeof v.score === 'number' && Number.isFinite(v.score) ? Math.min(n, Math.max(0, v.score)) : 0;
      if (n > 0) byUnit[id] = { n, score: Math.round(score * 100) / 100 };
    }
  }
  return {
    day: raw.day,
    total,
    answered: Math.min(total, nat(raw.answered)),
    score: nat(raw.score, 100),
    limitMin: nat(raw.limitMin, 600),
    usedSec: nat(raw.usedSec, 24 * 3600),
    timedOut: raw.timedOut === true,
    byUnit,
  };
}

/** Clamps and validates the extras of a stored value. Never throws; unknown keys are dropped. Idempotent. */
export function normalizeExtras(p: Obj, today: Day): ProgressExtras {
  const cal = isObj(p.calibration) ? p.calibration : {};
  const n = triple(cal.n);
  const right = triple(cal.right).map((r, i) => Math.min(r, n[i]!)) as [number, number, number];

  const bookmarks: ProgressExtras['bookmarks'] = {};
  if (isObj(p.bookmarks)) {
    for (const [id, b] of Object.entries(p.bookmarks)) {
      if (id.length > KEY_MAX || !safeKey(id) || !isObj(b) || typeof b.h !== 'string' || b.h === '') continue;
      if (Object.keys(bookmarks).length >= MAX_BOOKMARKS) break;
      bookmarks[id] = { h: b.h, day: isDay(b.day) ? b.day : today };
    }
  }

  const notes: ProgressExtras['notes'] = {};
  if (isObj(p.notes)) {
    for (const [id, v] of Object.entries(p.notes)) {
      if (id.length > KEY_MAX || !safeKey(id) || !isObj(v) || typeof v.h !== 'string' || v.h === '' || typeof v.text !== 'string') continue;
      const text = v.text.trim().slice(0, MAX_NOTE_CHARS);
      if (text === '') continue;
      if (Object.keys(notes).length >= MAX_NOTES) break;
      notes[id] = { h: v.h, text, day: isDay(v.day) ? v.day : today };
    }
  }

  const challenge: ProgressExtras['challenge'] = {};
  if (isObj(p.challenge)) {
    for (const [day, r] of Object.entries(p.challenge)) {
      if (isDay(day) && (r === 'pass' || r === 'fail') && daysBetween(day, today) < CHALLENGE_KEPT) challenge[day] = r;
    }
  }

  const achievements: ProgressExtras['achievements'] = {};
  if (isObj(p.achievements)) {
    for (const [id, d] of Object.entries(p.achievements)) if (id.length <= KEY_MAX && id !== '' && safeKey(id)) achievements[id] = isDay(d) ? d : today;
  }

  const mocks = (Array.isArray(p.mocks) ? p.mocks : [])
    .map(normalizeMock)
    .filter((m): m is MockRecord => m !== null)
    .slice(0, MAX_MOCKS);

  const passedByType: ProgressExtras['passedByType'] = {};
  if (isObj(p.passedByType)) for (const [t, v] of Object.entries(p.passedByType)) if (t.length <= 20 && safeKey(t) && nat(v) > 0) passedByType[t] = nat(v);

  return { calibration: { n, right }, bookmarks, notes, challenge, achievements, mocks, passedByType };
}

const maxRec = (a: Record<string, number>, b: Record<string, number>): Record<string, number> => {
  const out = { ...a };
  for (const [k, v] of Object.entries(b)) out[k] = Math.max(Object.hasOwn(out, k) ? out[k]! : 0, v);
  return out;
};

const mockKey = (m: MockRecord) => `${m.day}|${m.total}|${m.score}|${m.usedSec}|${m.answered}`;

/** Merge for "I practised on two devices": nothing is lost. */
export function mergeExtras(local: ProgressExtras, incoming: ProgressExtras): ProgressExtras {
  const pair = (a: [number, number, number], b: [number, number, number]): [number, number, number] => [Math.max(a[0], b[0]), Math.max(a[1], b[1]), Math.max(a[2], b[2])];

  const notes = { ...local.notes };
  for (const [id, n] of Object.entries(incoming.notes)) {
    const mine = notes[id];
    if (!mine || n.day > mine.day || (n.day === mine.day && n.h !== mine.h && n.text.length > mine.text.length)) notes[id] = n;
  }

  const challenge = { ...local.challenge };
  for (const [day, r] of Object.entries(incoming.challenge)) challenge[day] = challenge[day] === 'pass' || r === 'pass' ? 'pass' : 'fail';

  const achievements = { ...local.achievements };
  for (const [id, d] of Object.entries(incoming.achievements)) achievements[id] = Object.hasOwn(achievements, id) && achievements[id]! <= d ? achievements[id]! : d;

  const seen = new Set<string>();
  const mocks = [...local.mocks, ...incoming.mocks]
    .filter((m) => (seen.has(mockKey(m)) ? false : (seen.add(mockKey(m)), true)))
    .sort((a, b) => (a.day < b.day ? 1 : a.day > b.day ? -1 : 0))
    .slice(0, MAX_MOCKS);

  return {
    calibration: { n: pair(local.calibration.n, incoming.calibration.n), right: pair(local.calibration.right, incoming.calibration.right) },
    bookmarks: { ...incoming.bookmarks, ...local.bookmarks },
    notes,
    challenge,
    achievements,
    mocks,
    passedByType: maxRec(local.passedByType, incoming.passedByType),
  };
}
