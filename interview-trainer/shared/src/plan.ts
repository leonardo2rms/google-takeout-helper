import { daysBetween, type Day } from './day';
import type { Progress } from './progress';
import { allQuestions, type Course } from './schema';
import { cardFor, isDue } from './srs';

export interface Plan {
  /** Days until the interview (0 = today), or null when no valid future date is set. */
  daysLeft: number | null;
  /** Unseen questions plus the ones due now. */
  remaining: number;
  /** Questions per day needed to get through `remaining` with two spare days; null without a date. */
  perDay: number | null;
  /** Does the learner's daily goal cover `perDay`? null without a date. */
  onTrack: boolean | null;
  /** Whole days left over if the learner keeps to their daily goal (can be negative). null without a date. */
  spareDays: number | null;
  /** none: no date. steady: normal plan. final-review: the interview is today or tomorrow, so only review and weak spots. */
  mode: 'none' | 'steady' | 'final-review';
  /** A date was entered but it is in the past; the UI may offer "Set a new date?". */
  datePassed: boolean;
}

/** Calm, deterministic maths from the interview date the learner entered. Recomputed on every render; never raises alarms. */
export function planFor(course: Course, p: Progress, today: Day): Plan {
  const all = allQuestions(course);
  let remaining = 0;
  for (const q of all) {
    const c = cardFor(p, q);
    if (!c || isDue(c, today)) remaining++;
  }
  const date = p.profile.interviewDate;
  if (!date) return { daysLeft: null, remaining, perDay: null, onTrack: null, spareDays: null, mode: 'none', datePassed: false };
  const daysLeft = daysBetween(today, date);
  if (daysLeft < 0) return { daysLeft: null, remaining, perDay: null, onTrack: null, spareDays: null, mode: 'none', datePassed: true };
  const perDay = Math.ceil(remaining / Math.max(1, daysLeft - 2));
  const goal = p.goal.target;
  return {
    daysLeft,
    remaining,
    perDay,
    onTrack: goal >= perDay,
    spareDays: daysLeft - Math.ceil(remaining / goal),
    mode: daysLeft <= 1 ? 'final-review' : 'steady',
    datePassed: false,
  };
}
