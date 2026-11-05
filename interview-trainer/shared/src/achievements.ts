import { daysBetween, type Day } from './day';
import { challengeStreak } from './challenge';
import { topicStats } from './mastery';
import type { Progress } from './progress';
import { rankFor } from './rank';
import type { Course, Question } from './schema';

/**
 * Badges reward learning behaviours (finishing, mastering, reviewing, trying every format), never time spent or
 * a streak held under pressure: there is no badge for "not missing a day", and nothing here is ever taken away.
 * They are computed from Progress, so adding one later needs no migration.
 */

export interface AchievementCtx {
  course: Course;
  today: Day;
}

export type AchievementGroup = 'start' | 'habit' | 'skills' | 'review' | 'explore';

export interface AchievementDef {
  id: string;
  title: string;
  emoji: string;
  /** What it takes, in plain words (shown on locked badges too). */
  description: string;
  group: AchievementGroup;
  test: (f: Facts) => boolean;
}

const TYPES: Question['type'][] = ['single', 'multi', 'fill', 'order', 'spot', 'estimate', 'match', 'code', 'open'];

/** Everything the badge tests need, derived lazily and at most once per evaluation. */
export class Facts {
  constructor(
    readonly p: Progress,
    readonly ctx: AchievementCtx,
  ) {}
  private memo = new Map<string, unknown>();
  private once<T>(key: string, make: () => T): T {
    if (!this.memo.has(key)) this.memo.set(key, make());
    return this.memo.get(key) as T;
  }
  get topics() {
    return this.once('topics', () => topicStats(this.ctx.course, this.p, this.ctx.today));
  }
  /** Total card reviews (every time a question was answered and scheduled). */
  get reviews(): number {
    return this.once('reviews', () => Object.values(this.p.cards).reduce((n, c) => n + c.seen, 0));
  }
  get lessonsDone(): number {
    return Object.keys(this.p.lessons).length;
  }
  passed(type: Question['type']): number {
    return this.p.passedByType[type] ?? 0;
  }
  get challenge() {
    return this.once('challenge', () => challengeStreak(this.p, this.ctx.today));
  }
  /** A gap of a week or more between two practice days, followed by practice again. */
  get cameBack(): boolean {
    return this.once('cameBack', () => {
      const days = Object.keys(this.p.days).sort();
      return days.some((d, i) => i > 0 && daysBetween(days[i - 1]!, d) >= 7);
    });
  }
  get goalDays(): number {
    return this.once('goalDays', () => Object.values(this.p.days).filter((d) => d.answered >= d.target).length);
  }
  get ratings(): number {
    return this.p.calibration.n.reduce((a, b) => a + b, 0);
  }
}

const def = (id: string, group: AchievementGroup, emoji: string, title: string, description: string, test: (f: Facts) => boolean): AchievementDef => ({
  id, group, emoji, title, description, test,
});

export const ACHIEVEMENTS: readonly AchievementDef[] = [
  // first steps
  def('first-lesson', 'start', '🌱', 'First lesson', 'Finish any lesson.', (f) => f.lessonsDone >= 1),
  def('perfect-lesson', 'start', '🏆', 'Perfect lesson', 'Answer every question of a lesson right on the first try.', (f) => f.p.stats.perfect >= 1),
  def('perfect-5', 'skills', '🎯', 'Sharpshooter', 'Five perfect lessons.', (f) => f.p.stats.perfect >= 5),
  def('rank-senior', 'skills', '🎖️', 'Senior', 'Reach the Senior rank.', (f) => rankFor(f.p.xp).index >= 2),

  // habit (days practised, never "days in a row without a miss")
  def('streak-7', 'habit', '🔥', 'A week in a row', 'Practise 7 days in a row (rest days and freezes count).', (f) => f.p.longestStreak >= 7),
  def('streak-30', 'habit', '🌋', 'A month in a row', 'Practise 30 days in a row (rest days and freezes count).', (f) => f.p.longestStreak >= 30),
  def('practice-days-10', 'habit', '📅', 'Ten days practised', 'Practise on 10 different days.', (f) => f.p.stats.practiceDays >= 10),
  def('practice-days-30', 'habit', '🗓️', 'Thirty days practised', 'Practise on 30 different days, in any order.', (f) => f.p.stats.practiceDays >= 30),
  def('goal-getter', 'habit', '⭕', 'Goal getter', 'Reach your daily goal on 7 different days.', (f) => f.goalDays >= 7),
  def('comeback', 'habit', '👋', 'Welcome back', 'Practise again after a week or more away.', (f) => f.cameBack),

  // skills: one badge per format that is hard to fake
  def('first-code', 'skills', '💻', 'Code challenge passed', 'Pass a code challenge.', (f) => f.passed('code') >= 1),
  def('first-spot', 'skills', '🐞', 'Bug spotter', 'Find your first bug in a spot-the-bug question.', (f) => f.passed('spot') >= 1),
  def('first-estimate', 'skills', '🧮', 'Back of the envelope', 'Get a numeric estimate right.', (f) => f.passed('estimate') >= 1),
  def('first-open', 'skills', '🎤', 'In your own words', 'Pass an open question.', (f) => f.passed('open') >= 1),
  def('all-formats', 'skills', '🧩', 'Every format', 'Pass at least one question of each of the nine formats.', (f) => TYPES.every((t) => f.passed(t) >= 1)),
  def('topic-mastery', 'skills', '⭐', 'Topic mastered', 'Bring any topic to 80% recall strength.', (f) => f.topics.some((t) => t.strength >= 0.8)),

  // review
  def('reviews-50', 'review', '🔁', '50 reviews', 'Answer and schedule 50 reviews in total.', (f) => f.reviews >= 50),
  def('reviews-250', 'review', '♻️', '250 reviews', 'Answer and schedule 250 reviews in total.', (f) => f.reviews >= 250),
  def('questions-100', 'review', '💯', '100 questions', 'Answer 100 questions.', (f) => f.p.stats.answered >= 100),
  def('calibrated', 'review', '🧭', 'Self-aware', 'Rate your confidence on 30 questions.', (f) => f.ratings >= 30),

  // explore
  def('topic-explorer', 'explore', '🗺️', 'Explorer', 'Try at least one question in every topic.', (f) => f.topics.length > 0 && f.topics.every((t) => t.seen > 0)),
  def('mock-first', 'explore', '⏱️', 'Mock interview done', 'Finish a timed mock interview.', (f) => f.p.mocks.length >= 1),
  def('mock-strong', 'explore', '🥇', 'Strong mock', 'Score 80% or more in a mock interview.', (f) => f.p.mocks.some((m) => m.score >= 80)),
  def('curator', 'explore', '🔖', 'Curator', 'Save 5 questions to your library.', (f) => Object.keys(f.p.bookmarks).length >= 5),
  def('note-taker', 'explore', '📝', 'Note taker', 'Write your first note on a question.', (f) => Object.keys(f.p.notes).length >= 1),
  def('challenge-first', 'explore', '🧩', 'Daily challenger', 'Take a daily challenge.', (f) => f.challenge.done >= 1),
  def('challenge-7', 'explore', '📆', 'Challenge week', 'Take the daily challenge 7 days in a row.', (f) => f.challenge.best >= 7),
];

export const ACHIEVEMENT_GROUPS: Record<AchievementGroup, string> = {
  start: 'First steps',
  habit: 'Habit',
  skills: 'Skills',
  review: 'Review',
  explore: 'Explore',
};

export interface AchievementStatus {
  def: AchievementDef;
  /** The day it was unlocked, or null while locked. */
  on: Day | null;
}

export const achievementStatus = (p: Progress): AchievementStatus[] => ACHIEVEMENTS.map((d) => ({ def: d, on: p.achievements[d.id] ?? null }));

/** Badges whose condition holds now and that have not been recorded yet. */
export function newlyEarned(p: Progress, ctx: AchievementCtx): AchievementDef[] {
  const facts = new Facts(p, ctx);
  return ACHIEVEMENTS.filter((d) => !(d.id in p.achievements) && d.test(facts));
}

/**
 * Records every badge that has just been earned. Idempotent: a second call with the same progress returns the same
 * object and an empty list. Badges are never removed, even if the condition later stops holding.
 */
export function unlockAchievements(p: Progress, ctx: AchievementCtx): { progress: Progress; unlocked: AchievementDef[] } {
  const unlocked = newlyEarned(p, ctx);
  if (unlocked.length === 0) return { progress: p, unlocked };
  const achievements = { ...p.achievements };
  for (const d of unlocked) achievements[d.id] = ctx.today;
  return { progress: { ...p, achievements }, unlocked };
}

/** Whether the profile has any history worth backfilling (a brand new profile has nothing to unlock silently). */
const hasHistory = (p: Progress): boolean => p.stats.sessions > 0 || p.xp > 0 || Object.keys(p.cards).length > 0 || Object.keys(p.lessons).length > 0;

/**
 * For profiles from before badges existed, and for imports: unlocks everything already earned WITHOUT a celebration
 * (twenty toasts at once would be noise, and the learner did not "just" do any of it). Runs once per profile.
 */
export function backfillAchievements(p: Progress, ctx: AchievementCtx): Progress {
  if (p.milestones['ach:init'] || !hasHistory(p)) return p;
  const { progress } = unlockAchievements(p, ctx);
  return { ...progress, milestones: { ...progress.milestones, 'ach:init': ctx.today } };
}

/** Silent version for imports: records what the data already earns, whatever the flag says. */
export const silentUnlock = (p: Progress, ctx: AchievementCtx): Progress => unlockAchievements(p, ctx).progress;
