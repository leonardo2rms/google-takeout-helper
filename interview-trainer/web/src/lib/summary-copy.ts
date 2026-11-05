import { minutesFor, type ForecastBucket, type SessionSummary } from '@trainer/shared';
import { forecastText } from './format';

/** What kind of session just ended. Lessons are the curriculum; the rest are practice built from what you have seen. */
export type SessionKind = 'lesson' | 'review' | 'daily' | 'mix' | 'warmup' | 'topic' | 'one' | 'challenge';

type Outcomes = Pick<SessionSummary, 'perfect' | 'correct' | 'missed' | 'recovered'>;

/** Questions never answered right, even after the retries (a miss that came back and was answered right does not count). */
export function failedCount(s: Pick<SessionSummary, 'missed' | 'recovered'>): number {
  return s.missed.filter((id) => !s.recovered.includes(id)).length;
}

/**
 * The headline of the result screen, by outcome rather than by first-try accuracy alone: a lesson in which every question was
 * eventually answered right is complete, whatever the first-try score was. "Another pass needed" is for sessions that ended
 * with at least a third of their questions still wrong.
 */
export function resultHeading(kind: SessionKind, s: Outcomes): string {
  if (s.perfect) return 'Clean run';
  const failed = failedCount(s);
  const total = s.correct.length + failed;
  if (failed > 0 && failed / total >= 1 / 3) return 'Another pass needed';
  if (kind === 'lesson') return 'Lesson complete';
  return kind === 'review' ? 'Review done' : 'Practice complete';
}

/** The first few sessions get the one-time "make tomorrow easy" offer; after that the learner knows the app. */
export const BRIDGE_SESSIONS = 3;

/** Questions due tomorrow, from the forecast of the session that just ended. */
export const dueTomorrow = (forecast: readonly ForecastBucket[]): number => forecast.find((b) => b.inDays <= 1)?.count ?? 0;

/**
 * "Next review: 7 questions tomorrow · about 4 min": what is next and when, concretely. The minutes appear only for
 * tomorrow, where they help a learner decide to come back; later buckets just say when.
 */
export function nextReviewLine(forecast: readonly ForecastBucket[]): string {
  const tomorrow = dueTomorrow(forecast);
  return `Next review: ${forecastText(forecast)}${tomorrow > 0 ? ` · about ${minutesFor(tomorrow)} min` : ''}`;
}

/**
 * Whether to offer a calendar entry for tomorrow, ONCE: only in the first sessions, never after it was shown, never when a
 * reminder time already exists, and only when something is scheduled and the platform can hand over a file.
 */
export function shouldOfferBridge(i: { sessions: number; nudged: boolean; hasReminder: boolean; scheduled: boolean; canDownload: boolean }): boolean {
  return i.sessions >= 1 && i.sessions <= BRIDGE_SESSIONS && !i.nudged && !i.hasReminder && i.scheduled && i.canDownload;
}

/** One line on how the session really ended, only when first-try accuracy alone would mislead ("57%" but everything was learned by the end). */
export function endingLine(s: Outcomes): string | null {
  if (s.perfect) return null;
  const failed = failedCount(s);
  const total = s.correct.length + failed;
  if (total === 0) return null;
  if (failed === 0) return `All ${total} answered right by the end; ${s.recovered.length} needed another try.`;
  return `${s.correct.length} of ${total} answered right by the end; ${failed} still to revisit.`;
}
