import type { Day } from './day';
import { READINESS_TEXT, readiness, topicStats } from './mastery';
import type { Progress } from './progress';
import { rankFor } from './rank';
import type { Course } from './schema';
import { streakStatus } from './streak';

/**
 * What a shared progress card says. Only numbers the learner already sees in the app (streak, rank, readiness, topic bars):
 * no dates, no interview date, no role, no question content, no identifiers.
 */
export interface ShareCard {
  streak: number;
  rank: string;
  readinessScore: number;
  readinessLabel: string;
  /** Percent of the course seen. */
  coveragePct: number;
  /** Topics with at least one question seen; `null` when there is nothing to compare. */
  strongest: { title: string; pct: number } | null;
  weakest: { title: string; pct: number } | null;
  /** Spoken description for the image (alt text). */
  alt: string;
  /** Plain-text version: the share text and the "copy" fallback. */
  text: string;
}

const pct = (x: number) => Math.round(x * 100);

export function buildShareCard(course: Course, p: Progress, today: Day): ShareCard {
  const streak = streakStatus(p, today).streak;
  const rank = rankFor(p.xp).title;
  const ready = readiness(course, p, today);
  const started = topicStats(course, p, today).filter((t) => t.seen > 0 && t.total > 0);
  const byStrength = [...started].sort((a, b) => b.strength - a.strength || a.title.localeCompare(b.title));
  // With a single topic started, "strongest" and "weakest" would be the same bar: show only one.
  const strongestStat = byStrength.length >= 2 ? byStrength[0]! : null;
  const weakestStat = byStrength.length >= 1 ? byStrength[byStrength.length - 1]! : null;
  const strongest = strongestStat ? { title: strongestStat.title, pct: pct(strongestStat.strength) } : null;
  const weakest = weakestStat ? { title: weakestStat.title, pct: pct(weakestStat.strength) } : null;
  const label = READINESS_TEXT[ready.label];

  const facts = [
    `${streak}-day streak`,
    `rank ${rank}`,
    `readiness ${ready.score} (${label.toLowerCase()}, ${pct(ready.coverage)}% of the course seen)`,
    strongest && `strongest topic ${strongest.title} ${strongest.pct}%`,
    weakest && `${strongest ? 'weakest' : 'topic'} ${weakest.title} ${weakest.pct}%`,
  ].filter((x): x is string => Boolean(x));

  const text = [
    'My Interview Trainer progress',
    `Streak: ${streak} ${streak === 1 ? 'day' : 'days'}`,
    `Rank: ${rank}`,
    `Readiness: ${ready.score} (${label}) · ${pct(ready.coverage)}% of the course seen. An estimate of recall, not a prediction.`,
    strongest && `Strongest topic: ${strongest.title} (${strongest.pct}%)`,
    weakest && `${strongest ? 'Weakest' : 'Topic'}: ${weakest.title} (${weakest.pct}%)`,
  ]
    .filter((x): x is string => Boolean(x))
    .join('\n');

  return {
    streak,
    rank,
    readinessScore: ready.score,
    readinessLabel: label,
    coveragePct: pct(ready.coverage),
    strongest,
    weakest,
    alt: `Interview Trainer progress card: ${facts.join(', ')}.`,
    text,
  };
}

/** The learner has done something worth sharing. */
export const hasShareableProgress = (p: Progress): boolean => p.stats.answered > 0;
