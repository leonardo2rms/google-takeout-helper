import { READINESS_MIN_COVERAGE, READINESS_TEXT, REVIEW_SESSION_SIZE, type Progress, type Readiness } from '@trainer/shared';
import { plural } from './format';

/** How many cards one Review session takes (the rest wait for the next one). */
export const REVIEW_SIZE = REVIEW_SESSION_SIZE;

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

/**
 * The unit Home keeps open: the first one with a lesson not yet done (where the learner is working). Null when every lesson
 * is done. Every other unit is a one-line row; all of their lessons stay one tap away.
 */
export function currentUnitId(units: readonly { id: string; lessons: readonly { id: string }[] }[], done: Readonly<Record<string, unknown>>): string | null {
  return units.find((u) => u.lessons.some((l) => !done[l.id]))?.id ?? null;
}

/** The one phrase for the one meaning of "due" (a card whose due date has arrived): "5 due". */
export const dueText = (n: number): string => `${n} due`;

/**
 * The Review card's second line. `due` is the number of cards whose date has arrived (the same figure as Today and Progress);
 * `retry` are earlier misses that are not due yet, named separately so they never inflate "due".
 */
export function dueLine(due: number, retry = 0): string {
  if (due === 0 && retry === 0) return 'All caught up';
  const parts: string[] = [];
  if (due > 0) parts.push(due > REVIEW_SIZE ? `${dueText(due)} · ${REVIEW_SIZE} per session` : dueText(due));
  if (retry > 0) parts.push(`${retry} missed to retry`);
  return parts.join(' · ');
}

/** "5 due" in Today's card, and "5 of 24 due" when there are more due cards than Today has room for. */
export function dueInToday(inToday: number, totalDue: number): string {
  return totalDue > inToday ? `${inToday} of ${totalDue} due` : dueText(inToday);
}

/** "3 to reinforce" (never "weak spots": a flawless lesson has none, and it reads as a reproach). */
export const reinforceText = (n: number): string => `${n} to reinforce`;

export interface GoalMetHero {
  href: string;
  title: string;
  sub: string;
  cta: string;
  /** What the single suggestion is. */
  next: 'review' | 'lesson' | 'more';
}

/**
 * Home's hero once today's goal is met: no longer "Today" (that session is done), but a calm "done" with the one best optional
 * next step: reviewing what is due, else the next lesson, else more practice.
 */
export function goalMetHero(i: { done: number; target: number; due: number; nextLesson?: { id: string; title: string } | null }): GoalMetHero {
  const bonus = Math.max(0, i.done - i.target);
  const said = bonus > 0 ? `Goal met, plus ${plural(bonus, 'bonus question')}.` : `Goal met: ${plural(i.target, 'question')}.`;
  if (i.due > 0) return { href: '#/review', title: 'Done for today', sub: `${said} Optional: review ${dueText(i.due)}.`, cta: 'Review', next: 'review' };
  if (i.nextLesson) return { href: `#/lesson/${i.nextLesson.id}`, title: 'Done for today', sub: `${said} Optional: ${i.nextLesson.title}.`, cta: 'Next lesson', next: 'lesson' };
  return { href: '#/daily', title: 'Done for today', sub: `${said} Optional: a bonus round.`, cta: 'Keep going', next: 'more' };
}

/** The Progress card on Home: the score only once there is enough of the course behind it to mean something. */
export function readinessLine(r: Pick<Readiness, 'score' | 'label' | 'enough' | 'coverage'>): string {
  if (!r.enough) return `Readiness: not enough data yet (${Math.round(r.coverage * 100)}% of the course seen)`;
  return `Readiness ${r.score} · ${READINESS_TEXT[r.label]}`;
}

/** What the number needs, for the Progress screen. */
export const READINESS_NEEDS = `${Math.round(READINESS_MIN_COVERAGE * 100)}%`;
