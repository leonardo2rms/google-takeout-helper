import { addDays, daysBetween, weekday, type Day } from './day';
import { FREEZE_EVERY, MAX_FREEZES, type Progress } from './progress';

/** Legacy v1 rule (no forgiveness). Kept because it is still the baseline the forgiving rules are tested against. */
export function nextStreak(streak: number, lastDay: Day | null, today: Day): number {
  if (lastDay === null) return 1;
  const gap = daysBetween(lastDay, today);
  if (gap <= 0) return Math.max(streak, 1);
  return gap === 1 ? streak + 1 : 1;
}

export type StreakState =
  | 'new' // never practised
  | 'done-today' // practised today (or the clock moved backwards: hold)
  | 'open' // practised yesterday, or every missed day was a chosen rest day
  | 'saved-by-freeze' // the missed days will be covered by freezes the next time you practise
  | 'lapsed'; // too many missed days to cover: the next practice starts a fresh streak

export interface StreakStatus {
  /** The streak to show right now (0 when lapsed or new). */
  streak: number;
  state: StreakState;
  /** Freezes that the next practice will use. */
  freezesNeeded: number;
  /** Missed days covered by the learner's chosen rest days. */
  restDaysCovered: number;
  /** Days between the last practice day and today, minus one (0 when practised yesterday or today). */
  missedDays: number;
}

interface GapPlan {
  missed: number;
  restCovered: number;
  freezesNeeded: number;
  covered: boolean;
}

/** Walks the days strictly between `lastDay` and `today`: rest days are free, other days need one freeze each. */
function planGap(p: Progress, lastDay: Day, today: Day): GapPlan {
  const missed = Math.max(0, daysBetween(lastDay, today) - 1);
  const rest = new Set(p.profile.restDays);
  let restCovered = 0;
  let freezesNeeded = 0;
  for (let i = 1; i <= missed; i++) {
    if (rest.has(weekday(addDays(lastDay, i)))) restCovered++;
    else freezesNeeded++;
    if (freezesNeeded > p.freezes) return { missed, restCovered, freezesNeeded, covered: false };
  }
  return { missed, restCovered, freezesNeeded, covered: true };
}

/**
 * Pure prediction of what the streak looks like at `today`. Nothing is written on app open: the
 * settlement happens in `advanceStreak` on the next practice, so reloading can never spend a freeze twice.
 */
export function streakStatus(p: Progress, today: Day): StreakStatus {
  if (p.lastDay === null) return { streak: 0, state: 'new', freezesNeeded: 0, restDaysCovered: 0, missedDays: 0 };
  const gap = daysBetween(p.lastDay, today);
  if (gap <= 0) return { streak: p.streak, state: 'done-today', freezesNeeded: 0, restDaysCovered: 0, missedDays: 0 };
  if (gap === 1) return { streak: p.streak, state: 'open', freezesNeeded: 0, restDaysCovered: 0, missedDays: 0 };
  const plan = planGap(p, p.lastDay, today);
  if (!plan.covered) {
    return { streak: 0, state: 'lapsed', freezesNeeded: 0, restDaysCovered: plan.restCovered, missedDays: plan.missed };
  }
  return {
    streak: p.streak,
    state: plan.freezesNeeded > 0 ? 'saved-by-freeze' : 'open',
    freezesNeeded: plan.freezesNeeded,
    restDaysCovered: plan.restCovered,
    missedDays: plan.missed,
  };
}

/** What the streak chip shows. */
export const liveStreak = (p: Progress, today: Day): number => streakStatus(p, today).streak;

export type StreakResult = Pick<Progress, 'streak' | 'longestStreak' | 'freezes' | 'lastDay'> & {
  freezeUsed: number;
  freezeEarned: boolean;
  /** True when this practice moved the streak up (first practice of a new day). */
  incremented: boolean;
  /** True when a lapsed streak was restarted. */
  restarted: boolean;
};

/**
 * Applies "I practised today". Idempotent within a day. Freezes are spent only when they fully cover the gap;
 * otherwise the streak restarts at 1 and no freeze is lost. A freeze is earned each time the streak reaches a multiple of 7.
 */
export function advanceStreak(p: Progress, today: Day): StreakResult {
  const base = { freezeUsed: 0, freezeEarned: false, incremented: false, restarted: false };
  if (p.lastDay !== null) {
    const gap = daysBetween(p.lastDay, today);
    if (gap <= 0) {
      // Same day, or the clock moved backwards: hold, and re-anchor `lastDay` so the next day continues normally.
      const streak = Math.max(p.streak, 1);
      return { ...base, streak, longestStreak: Math.max(p.longestStreak, streak), freezes: p.freezes, lastDay: today };
    }
  }

  const status = streakStatus(p, today);
  let streak: number;
  let freezeUsed = 0;
  let restarted = false;
  if (p.lastDay === null) streak = 1;
  else if (status.state === 'lapsed') {
    streak = 1;
    restarted = true;
  } else {
    streak = p.streak + 1;
    freezeUsed = status.freezesNeeded;
  }

  let freezes = p.freezes - freezeUsed;
  let freezeEarned = false;
  if (!restarted && streak % FREEZE_EVERY === 0 && freezes < MAX_FREEZES) {
    freezes++;
    freezeEarned = true;
  }
  return { streak, longestStreak: Math.max(p.longestStreak, streak), freezes, lastDay: today, freezeUsed, freezeEarned, incremented: true, restarted };
}
