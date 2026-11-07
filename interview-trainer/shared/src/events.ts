import type { Day } from './day';
import { alreadyDoneToday, goalToday, recordDay } from './goal';
import { fnv1a } from './hash';
import { readiness, topicStats, type ReadinessLabel } from './mastery';
import type { Confidence, Progress, QuestionId } from './progress';
import { rankFor } from './rank';
import { allQuestions, type Course, type Question } from './schema';
import { applyToCards, outcomesOf, type Outcome } from './srs';
import { advanceStreak } from './streak';

export interface LessonResult {
  /** Omit for review sessions that aren't tied to one lesson. */
  lessonId?: string;
  accuracy: number;
  xp: number;
  missed: string[];
  recovered: string[];
  /** Question ids that were answered correctly in this session (clears them from `missed`). */
  cleared?: string[];
  /** Per-question outcome; derived from missed / recovered / cleared when omitted. */
  outcomes?: Record<QuestionId, Outcome>;
  confidence?: Record<QuestionId, Confidence>;
  /** Question ids whose hint was revealed: their review quality is capped. */
  hinted?: string[];
  /**
   * `lesson` (default) is a normal session. `single` (one question: library practice, the daily challenge) and `mock`
   * never count as a "perfect lesson" and never show a tip.
   */
  kind?: 'lesson' | 'single' | 'mock';
  /** Distinct questions resolved; defaults to the number of outcomes. */
  answered?: number;
}

export const TIP_BONUS_XP = 5;
/** One session in this many shows an "Interviewer's tip". Printed in Settings. */
export const TIP_ONE_IN = 6;
export const STREAK_MILESTONES: readonly number[] = [7, 30, 100, 365];
export const TIP_MIN_INTERVAL = 7;

export type SessionEvent =
  | { type: 'goal-met'; first: boolean }
  | { type: 'streak'; value: number; milestone: boolean }
  | { type: 'freeze-earned' }
  | { type: 'freeze-used'; count: number }
  | { type: 'freeze-gift' }
  | { type: 'rank-up'; title: string }
  | { type: 'unit-mastered'; unitId: string; title: string }
  | { type: 'perfect' }
  | { type: 'readiness'; label: ReadinessLabel }
  | { type: 'tip'; questionId: QuestionId; bonusXp: number };

/** Lessons / xp / missed: the v1-visible part of a result. Pure. */
function applyBasics(p: Progress, r: LessonResult, today: Day, xp: number): Pick<Progress, 'lessons' | 'missed' | 'xp'> {
  const lessons = { ...p.lessons };
  if (r.lessonId) {
    const prev = lessons[r.lessonId];
    lessons[r.lessonId] = {
      bestAccuracy: Math.max(prev?.bestAccuracy ?? 0, r.accuracy),
      completions: (prev?.completions ?? 0) + 1,
      lastDay: today,
    };
  }
  const cleared = new Set([...(r.cleared ?? []), ...r.recovered]);
  const missed = [...new Set([...p.missed.filter((id) => !cleared.has(id)), ...r.missed.filter((id) => !r.recovered.includes(id))])];
  return { lessons, missed, xp: p.xp + xp };
}

/** Deterministic dice: roughly one session in six shows a tip, and reloading never re-rolls it. */
export function tipFor(p: Progress, today: Day): { show: boolean } {
  return { show: p.settings.tips && fnv1a(`${today}|${p.stats.sessions}`) % TIP_ONE_IN === 0 };
}

/** The question a tip reminds the learner of: one they already know well, picked deterministically. */
export function tipQuestion(p: Progress, questions: readonly Question[], today: Day): Question | null {
  const known = questions.filter((q) => (p.cards[q.id]?.interval ?? 0) >= TIP_MIN_INTERVAL).sort((a, b) => (a.id < b.id ? -1 : 1));
  if (known.length === 0) return null;
  return known[fnv1a(`${today}|${p.stats.sessions}|q`) % known.length]!;
}

export const explanationOf = (q: Question): string => q.explanation ?? (q.type === 'open' ? q.modelAnswer : '');

/** Calibration counts (how often "sure" was right) and the per-type pass counters behind some badges. */
function tally(
  p: Progress,
  r: LessonResult,
  outcomes: Record<QuestionId, Outcome>,
  questions: readonly Question[],
): Pick<Progress, 'calibration' | 'passedByType'> {
  const n: [number, number, number] = [...p.calibration.n];
  const right: [number, number, number] = [...p.calibration.right];
  for (const [id, c] of Object.entries(r.confidence ?? {})) {
    const o = outcomes[id];
    if (!o) continue;
    n[c - 1]!++;
    if (o === 'first') right[c - 1]!++;
  }
  const typeOf = new Map(questions.map((q) => [q.id, q.type] as const));
  const passedByType = { ...p.passedByType };
  for (const [id, o] of Object.entries(outcomes)) {
    const t = typeOf.get(id);
    if (t && (o === 'first' || o === 'retry')) passedByType[t] = (passedByType[t] ?? 0) + 1;
  }
  return { calibration: { n, right }, passedByType };
}

/**
 * The single write path for a finished session. Pure: returns the next progress plus what happened,
 * so the UI can celebrate without re-deriving anything.
 */
export function applySession(
  p: Progress,
  r: LessonResult,
  ctx: { today: Day; course: Course },
): { progress: Progress; events: SessionEvent[] } {
  const { today, course } = ctx;
  const questions = allQuestions(course);
  const outcomes = r.outcomes ?? outcomesOf({ missed: r.missed, recovered: r.recovered, correct: r.cleared ?? [] });
  const answered = r.answered ?? Object.keys(outcomes).length;
  // `answered` can be lower than the number of outcomes (a mock credits the goal only partly): first tries never exceed it.
  const firstTry = Math.min(answered, Object.values(outcomes).filter((o) => o === 'first').length);
  // The daily goal counts distinct questions: one already answered earlier today does not count again (it still counts as
  // practice in the lifetime stats). A mock has its own capped credit and is left as it is.
  const repeated = new Set(r.kind === 'mock' ? [] : alreadyDoneToday(p, questions.filter((q) => q.id in outcomes), today));
  const fresh = Object.keys(outcomes).filter((id) => !repeated.has(id));
  const credited = Math.min(answered, fresh.length);
  const firstTryCredited = Math.min(credited, fresh.filter((id) => outcomes[id] === 'first').length);

  const beforeRank = rankFor(p.xp);
  const beforeGoal = goalToday(p, today);
  const beforeReady = readiness(course, p, today);
  const beforeTopics = topicStats(course, p, today);

  const regular = (r.kind ?? 'lesson') === 'lesson';
  const tipQ = regular && tipFor(p, today).show ? tipQuestion(p, questions, today) : null;
  const bonus = tipQ ? TIP_BONUS_XP : 0;

  const basics = applyBasics(p, r, today, r.xp + bonus);
  const streak = advanceStreak(p, today);
  const newDay = p.lastDay !== today && !p.days[today];
  const perfect = regular && r.accuracy >= 1 && answered > 0;

  const next: Progress = {
    ...p,
    ...basics,
    streak: streak.streak,
    longestStreak: streak.longestStreak,
    freezes: streak.freezes,
    lastDay: streak.lastDay,
    cards: applyToCards(p.cards, outcomes, questions, today, { deadline: p.profile.interviewDate, confidence: r.confidence, hinted: r.hinted }),
    days: recordDay(p, today, { xp: r.xp + bonus, answered: credited, firstTry: firstTryCredited }),
    stats: {
      sessions: p.stats.sessions + 1,
      answered: p.stats.answered + answered,
      firstTry: p.stats.firstTry + firstTry,
      perfect: p.stats.perfect + (perfect ? 1 : 0),
      practiceDays: p.stats.practiceDays + (newDay ? 1 : 0),
    },
    milestones: { ...p.milestones },
    ...tally(p, r, outcomes, questions),
  };

  const events: SessionEvent[] = [];
  const mark = (key: string) => {
    next.milestones[key] = today;
  };

  if (p.milestones['gift:freeze'] && !p.milestones['gift:freeze:seen']) {
    mark('gift:freeze:seen');
    events.push({ type: 'freeze-gift' });
  }
  if (streak.freezeUsed > 0) events.push({ type: 'freeze-used', count: streak.freezeUsed });
  if (streak.freezeEarned) events.push({ type: 'freeze-earned' });
  if (streak.incremented && !streak.restarted) {
    const milestone = STREAK_MILESTONES.includes(streak.streak) && !p.milestones[`streak:${streak.streak}`];
    if (milestone) mark(`streak:${streak.streak}`);
    events.push({ type: 'streak', value: streak.streak, milestone });
  }

  const afterRank = rankFor(next.xp);
  if (afterRank.index > beforeRank.index) {
    const fresh = !p.milestones[`rank:${afterRank.index}`];
    for (let i = 0; i <= afterRank.index; i++) if (!next.milestones[`rank:${i}`]) mark(`rank:${i}`);
    if (fresh) events.push({ type: 'rank-up', title: afterRank.title });
  }

  for (const t of topicStats(course, next, today)) {
    const was = beforeTopics.find((b) => b.unitId === t.unitId)?.strength ?? 0;
    const key = `unit:${t.unitId}:80`;
    if (was < 0.8 && t.strength >= 0.8 && !p.milestones[key]) {
      mark(key);
      events.push({ type: 'unit-mastered', unitId: t.unitId, title: t.title });
    }
  }

  const afterGoal = goalToday(next, today);
  if (afterGoal.met && !beforeGoal.met) {
    const first = !p.milestones['goal:first'];
    if (first) mark('goal:first');
    events.push({ type: 'goal-met', first });
  }
  if (perfect) events.push({ type: 'perfect' });

  const afterReady = readiness(course, next, today);
  const order: ReadinessLabel[] = ['early', 'building', 'solid', 'ready'];
  if (order.indexOf(afterReady.label) > order.indexOf(beforeReady.label) && !p.milestones[`readiness:${afterReady.label}`]) {
    mark(`readiness:${afterReady.label}`);
    events.push({ type: 'readiness', label: afterReady.label });
  }

  if (tipQ) events.push({ type: 'tip', questionId: tipQ.id, bonusXp: bonus });
  return { progress: next, events };
}

/**
 * The legacy entry point (no course): lessons, xp, missed, streak, day log and stats, but no cards or events.
 * Kept so v1 callers and tests keep their exact behaviour.
 */
export function recordResult(p: Progress, r: LessonResult, today: Day): Progress {
  const outcomes = r.outcomes ?? outcomesOf({ missed: r.missed, recovered: r.recovered, correct: r.cleared ?? [] });
  const answered = r.answered ?? Object.keys(outcomes).length;
  const firstTry = Object.values(outcomes).filter((o) => o === 'first').length;
  const streak = advanceStreak(p, today);
  const newDay = p.lastDay !== today && !p.days[today];
  return {
    ...p,
    ...applyBasics(p, r, today, r.xp),
    streak: streak.streak,
    longestStreak: streak.longestStreak,
    freezes: streak.freezes,
    lastDay: streak.lastDay,
    days: recordDay(p, today, { xp: r.xp, answered, firstTry }),
    stats: {
      sessions: p.stats.sessions + 1,
      answered: p.stats.answered + answered,
      firstTry: p.stats.firstTry + firstTry,
      perfect: p.stats.perfect + (r.accuracy >= 1 && answered > 0 ? 1 : 0),
      practiceDays: p.stats.practiceDays + (newDay ? 1 : 0),
    },
  };
}

const PRIORITY: SessionEvent['type'][] = ['rank-up', 'streak', 'unit-mastered', 'goal-met', 'perfect', 'readiness', 'tip'];

/** At most one headline moment per session, by priority. Non-milestone streak ticks and freeze bookkeeping never headline. */
export function pickCelebration(events: readonly SessionEvent[]): SessionEvent | null {
  const eligible = events.filter((e) => (e.type === 'streak' ? e.milestone : PRIORITY.includes(e.type)));
  let best: SessionEvent | null = null;
  for (const e of eligible) if (best === null || PRIORITY.indexOf(e.type) < PRIORITY.indexOf(best.type)) best = e;
  return best;
}

/** Whether the headline deserves the big treatment (confetti / haptics). A routine daily goal does not. */
export const isLargeCelebration = (e: SessionEvent | null): boolean => e !== null && e.type !== 'tip' && e.type !== 'readiness' && !(e.type === 'goal-met' && !e.first);
