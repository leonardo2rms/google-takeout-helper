import type { Day } from './day';
import { READINESS_TEXT, readiness, topicStats } from './mastery';
import type { Progress } from './progress';
import { rankFor } from './rank';
import type { Course } from './schema';
import { streakStatus } from './streak';

/**
 * What a shared progress card says. Only numbers the learner already sees in the app, and only the ones worth posting:
 * streak, rank, lessons done, days practised, the readiness score once there is enough data behind it, and the best topic
 * once it is genuinely strong. Never the weakest topic, never a low score. No dates, no interview date, no role,
 * no question content, no identifiers.
 */
export interface ShareCard {
  streak: number;
  rank: string;
  lessonsDone: number;
  lessonsTotal: number;
  daysPracticed: number;
  /** The readiness score, only once enough of the course has been seen for it to mean something. */
  readiness: { score: number; label: string } | null;
  /** The strongest started topic, only when it is at least `BEST_TOPIC_MIN_PCT`. */
  bestTopic: { title: string; pct: number } | null;
  /** Spoken description for the image (alt text). */
  alt: string;
  /** Plain-text version: the share text and the "copy" fallback. */
  text: string;
}

export const BEST_TOPIC_MIN_PCT = 50;

const pct = (x: number) => Math.round(x * 100);

/** The strongest started topic, shown only when it is strong enough to be worth saying (at least `BEST_TOPIC_MIN_PCT` percent). */
export function bestTopicOf(topics: readonly { title: string; strength: number }[]): { title: string; pct: number } | null {
  const top = [...topics].sort((a, b) => b.strength - a.strength || a.title.localeCompare(b.title))[0];
  return top && pct(top.strength) >= BEST_TOPIC_MIN_PCT ? { title: top.title, pct: pct(top.strength) } : null;
}

export function buildShareCard(course: Course, p: Progress, today: Day): ShareCard {
  const streak = streakStatus(p, today).streak;
  const rank = rankFor(p.xp).title;
  const ready = readiness(course, p, today);
  const lessonsTotal = course.units.reduce((n, u) => n + u.lessons.length, 0);
  const lessonsDone = Object.keys(p.lessons).length;
  const daysPracticed = p.stats.practiceDays;
  const started = topicStats(course, p, today).filter((t) => t.seen > 0 && t.total > 0);
  const bestTopic = bestTopicOf(started);
  const readinessShown = ready.enough ? { score: ready.score, label: READINESS_TEXT[ready.label] } : null;
  const days = (n: number) => `${n} ${n === 1 ? 'day' : 'days'}`;

  const facts = [
    `${streak}-day streak`,
    `rank ${rank}`,
    `${lessonsDone} of ${lessonsTotal} lessons done`,
    `${days(daysPracticed)} practised`,
    readinessShown && `readiness ${readinessShown.score} (${readinessShown.label.toLowerCase()})`,
    bestTopic && `best topic ${bestTopic.title} ${bestTopic.pct}%`,
  ].filter((x): x is string => Boolean(x));

  const text = [
    'My Interview Trainer progress',
    `Streak: ${days(streak)}`,
    `Rank: ${rank}`,
    `Lessons: ${lessonsDone} of ${lessonsTotal}`,
    `Days practised: ${daysPracticed}`,
    readinessShown && `Readiness: ${readinessShown.score} (${readinessShown.label}). An estimate of recall, not a prediction.`,
    bestTopic && `Best topic: ${bestTopic.title} (${bestTopic.pct}%)`,
  ]
    .filter((x): x is string => Boolean(x))
    .join('\n');

  return { streak, rank, lessonsDone, lessonsTotal, daysPracticed, readiness: readinessShown, bestTopic, alt: `Interview Trainer progress card: ${facts.join(', ')}.`, text };
}

/** The learner has done something worth sharing. */
export const hasShareableProgress = (p: Progress): boolean => p.stats.answered > 0;
