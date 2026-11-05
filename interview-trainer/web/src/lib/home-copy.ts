import { READINESS_MIN_COVERAGE, READINESS_TEXT, type Progress, type Readiness } from '@trainer/shared';
import { cappedCount } from './format';

/** How many cards one Review session takes (the rest wait for the next one). */
export const REVIEW_SIZE = 10;

/**
 * How much of Home to show. A brand-new learner gets one next step; the practice modes join as they become useful.
 *  0  nothing done yet: only "Start your first lesson"; every other mode sits under "More ways to practise"
 *  1  started: daily challenge, review and progress are on the page; mixed practice, mock and library are under "More"
 *  2  three lessons in: everything is on the page
 * Nothing is ever unreachable: whatever is not on the page is one tap away under "More ways to practise".
 */
export type HomeTier = 0 | 1 | 2;
export const FULL_HOME_AFTER_LESSONS = 3;

export function homeTier(p: Pick<Progress, 'stats' | 'lessons' | 'cards'>): HomeTier {
  const lessons = Object.keys(p.lessons).length;
  if (p.stats.sessions === 0 && lessons === 0 && Object.keys(p.cards).length === 0) return 0;
  return lessons >= FULL_HOME_AFTER_LESSONS ? 2 : 1;
}

/** The Review card's second line: how many cards are ready, and that one round takes a handful (so "30+" never reads as a debt). */
export function dueLine(due: number): string {
  if (due === 0) return 'All caught up';
  if (due <= REVIEW_SIZE) return `${due} ready to review`;
  return `${cappedCount(due)} ready · ${REVIEW_SIZE} at a time`;
}

/** "5 due" in Today's card, and "5 of 24 due" when there are more due cards than Today has room for. */
export function dueInToday(inToday: number, totalDue: number): string {
  return totalDue > inToday ? `${inToday} of ${totalDue} due` : `${inToday} due`;
}

/** The Progress card on Home: the score only once there is enough of the course behind it to mean something. */
export function readinessLine(r: Pick<Readiness, 'score' | 'label' | 'enough' | 'coverage'>): string {
  if (!r.enough) return `Readiness: not enough data yet (${Math.round(r.coverage * 100)}% of the course seen)`;
  return `Readiness ${r.score} · ${READINESS_TEXT[r.label]}`;
}

/** What the number needs, for the Progress screen. */
export const READINESS_NEEDS = `${Math.round(READINESS_MIN_COVERAGE * 100)}%`;
