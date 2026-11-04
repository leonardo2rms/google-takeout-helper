import type { Day } from './day';
import { defaultPrefs, type Prefs } from './prefs';

export type QuestionId = string; // '<lessonId>/<n>'
/** guess | think so | certain. Reserved for the confidence-rating feature; accepted (and ignored) by the scheduler today. */
export type Confidence = 1 | 2 | 3;
export type RoleId = 'backend-kotlin' | 'staff-architect' | 'generalist';

export const ROLE_IDS: readonly RoleId[] = ['backend-kotlin', 'staff-architect', 'generalist'];

export interface LessonRecord {
  bestAccuracy: number;
  completions: number;
  lastDay: Day;
}

/** Spaced-repetition state of one question. */
export interface Card {
  /** fnv1a-32 hex of `${type}|${prompt}`; a mismatch means the question was rewritten, so the card is dropped. */
  h: string;
  /** 1.3 .. 2.8 */
  ease: number;
  /** Days until the next review; 0 = never scheduled. */
  interval: number;
  due: Day;
  /** Consecutive successful reviews. */
  reps: number;
  lapses: number;
  seen: number;
  firstTry: number;
  last: Day;
}

export interface DayLog {
  xp: number;
  /** Distinct questions resolved that day. */
  answered: number;
  firstTry: number;
  /** Goal snapshot, so changing the goal never rewrites history. */
  target: number;
  /** True when the (halved) comeback goal applied that day. */
  comeback?: boolean;
}

export interface Profile {
  onboarded: boolean;
  role: RoleId | null;
  interviewDate: Day | null;
  /** 0 = Sunday .. 6 = Saturday. */
  restDays: number[];
  /** 'HH:mm', the learner's own "when will I practise" cue. */
  practiceTime: string | null;
}

export interface Settings {
  haptics: boolean;
  sound: boolean;
  confetti: boolean;
  tips: boolean;
  confidence: boolean;
}

export interface Stats {
  sessions: number;
  answered: number;
  firstTry: number;
  perfect: number;
  practiceDays: number;
}

export interface Progress {
  version: 2;
  // ---- v1 fields, same meaning ----
  xp: number;
  /** Current run of kept days (practised, plus rest days / freezes that covered a gap). */
  streak: number;
  /** Last practice day. */
  lastDay: Day | null;
  lessons: Record<string, LessonRecord>;
  /** Question ids missed and not yet recovered. Kept alongside the cards: "Review mistakes" semantics are unchanged. */
  missed: QuestionId[];
  // ---- v2 ----
  longestStreak: number;
  /** 0..MAX_FREEZES. Earned, never bought. */
  freezes: number;
  cards: Record<QuestionId, Card>;
  /** Pruned to the last DAYS_KEPT days. */
  days: Record<Day, DayLog>;
  /** Questions per day. */
  goal: { target: number };
  profile: Profile;
  settings: Settings;
  stats: Stats;
  /** Dedupes celebrations: 'rank:3', 'streak:7', 'unit:ddd:80', 'goal:first', 'gift:freeze'. */
  milestones: Record<string, Day>;
  /** Device preferences (reminders, reading). Namespaced here so other features' fields never collide. Kept local on import/merge. */
  prefs: Prefs;
}

export const MAX_FREEZES = 2;
/** A freeze is earned each time the streak reaches a multiple of this. */
export const FREEZE_EVERY = 7;
export const MAX_INTERVAL = 120;
export const MASTERED_INTERVAL = 21;
export const DAYS_KEPT = 400;
export const MAX_REST_DAYS = 3;
export const DEFAULT_GOAL = 8;
export const MAX_GOAL = 100;

export const emptyProgress = (): Progress => ({
  version: 2,
  xp: 0,
  streak: 0,
  lastDay: null,
  lessons: {},
  missed: [],
  longestStreak: 0,
  freezes: 0,
  cards: {},
  days: {},
  goal: { target: DEFAULT_GOAL },
  profile: { onboarded: false, role: null, interviewDate: null, restDays: [], practiceTime: null },
  settings: { haptics: true, sound: false, confetti: true, tips: true, confidence: false },
  stats: { sessions: 0, answered: 0, firstTry: 0, perfect: 0, practiceDays: 0 },
  milestones: {},
  prefs: defaultPrefs(),
});
