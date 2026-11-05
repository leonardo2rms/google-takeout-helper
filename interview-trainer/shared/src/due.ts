import type { Day } from './day';
import type { Progress } from './progress';
import { allQuestions, type Course } from './schema';
import { unitOfFor } from './mastery';
import { reviewQueue } from './srs';

/** How many cards one Review session takes (the rest wait for the next one). */
export const REVIEW_SESSION_SIZE = 10;

/**
 * The ONE meaning of "due": a card is due when its due date has arrived. Every number the app shows for it (Today, the
 * Review card, Progress topics, reminders) comes from here, so they can never disagree.
 */
export interface DueOverview {
  /** Cards whose due date has arrived. */
  due: number;
  /** Questions missed earlier whose cards are not due yet: Review offers them as a retry, they are not "due". */
  retry: number;
  /** What a Review session can draw on: due + retry. */
  reviewable: number;
}

export function dueOverview(course: Course, p: Progress, today: Day): DueOverview {
  const q = reviewQueue(p, allQuestions(course), today, unitOfFor(course));
  return { due: q.due.length, retry: q.missedOnly.length, reviewable: q.all.length };
}

/** Rough minutes for `n` questions (the same 0.6 min each Today uses). */
export const minutesFor = (n: number): number => Math.max(1, Math.round(n * 0.6));
