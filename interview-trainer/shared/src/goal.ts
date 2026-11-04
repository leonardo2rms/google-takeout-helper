import { addDays, daysBetween, type Day } from './day';
import { DAYS_KEPT, DEFAULT_GOAL, MAX_GOAL, type DayLog, type Progress } from './progress';

/** Questions per day. A default Daily Practice is 8, which is "light" and takes about five minutes. */
export const GOAL_PRESETS = { light: 8, regular: 16, serious: 24 } as const;
/** After this many days away the next day's goal is halved, once, with neutral copy. */
export const COMEBACK_AFTER_DAYS = 7;

export const clampGoal = (n: number): number => (Number.isFinite(n) ? Math.min(MAX_GOAL, Math.max(1, Math.round(n))) : DEFAULT_GOAL);

export interface GoalToday {
  target: number;
  done: number;
  /** done / target, clamped to 1. */
  fraction: number;
  met: boolean;
  /** An easy-restart day (half goal) after a week or more away. */
  comeback: boolean;
}

/** The target that applies on `today` and whether it is the halved comeback goal. */
export function targetFor(p: Progress, today: Day): { target: number; comeback: boolean } {
  const log = p.days[today];
  const comeback = log ? Boolean(log.comeback) : p.lastDay !== null && daysBetween(p.lastDay, today) >= COMEBACK_AFTER_DAYS;
  return { target: comeback ? Math.max(1, Math.ceil(p.goal.target / 2)) : p.goal.target, comeback };
}

export function goalToday(p: Progress, today: Day): GoalToday {
  const { target, comeback } = targetFor(p, today);
  const done = p.days[today]?.answered ?? 0;
  return { target, done, fraction: Math.min(1, done / target), met: done >= target, comeback };
}

export interface DayBar {
  day: Day;
  xp: number;
  answered: number;
  target: number;
  met: boolean;
}

/** The last `n` days ending today, oldest first. Days without practice are zero-filled. */
export function lastDays(p: Progress, today: Day, n = 14): DayBar[] {
  const out: DayBar[] = [];
  for (let i = n - 1; i >= 0; i--) {
    const day = addDays(today, -i);
    const log = day === today ? undefined : p.days[day];
    if (day === today) {
      const g = goalToday(p, today);
      out.push({ day, xp: p.days[day]?.xp ?? 0, answered: g.done, target: g.target, met: g.met });
    } else {
      out.push({ day, xp: log?.xp ?? 0, answered: log?.answered ?? 0, target: log?.target ?? p.goal.target, met: log ? log.answered >= log.target : false });
    }
  }
  return out;
}

/** Sum of questions answered in the `n` days ending today. */
export const answeredInLast = (p: Progress, today: Day, n: number): number => lastDays(p, today, n).reduce((a, d) => a + d.answered, 0);

/** Adds a finished session to `days`, snapshotting the target, and prunes anything older than DAYS_KEPT. */
export function recordDay(p: Progress, today: Day, add: { xp: number; answered: number; firstTry: number }): Progress['days'] {
  const { target, comeback } = targetFor(p, today);
  const prev: DayLog = p.days[today] ?? { xp: 0, answered: 0, firstTry: 0, target, ...(comeback ? { comeback } : {}) };
  const days: Progress['days'] = {};
  for (const [day, log] of Object.entries(p.days)) if (daysBetween(day, today) < DAYS_KEPT) days[day] = log;
  days[today] = { ...prev, xp: prev.xp + add.xp, answered: prev.answered + add.answered, firstTry: prev.firstTry + add.firstTry, target };
  return days;
}

/** Changes the daily goal. It applies to today and the future; earlier days keep their snapshot. */
export function setGoalTarget(p: Progress, target: number, today: Day): Progress {
  const goal = { target: clampGoal(target) };
  const next: Progress = { ...p, goal };
  const log = p.days[today];
  if (log) next.days = { ...p.days, [today]: { ...log, target: targetFor(next, today).target } };
  return next;
}
