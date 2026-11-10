import type { ForecastBucket } from '@trainer/shared';

/** 1 -> "tomorrow", 3 -> "in 3 days". */
export const inDays = (n: number): string => (n <= 1 ? 'tomorrow' : `in ${n} days`);

export const plural = (n: number, one: string, many = `${one}s`): string => `${n} ${n === 1 ? one : many}`;

/** Days of the forecast that are spelled out; a learner with a long tail of reviews hears about the next few. */
export const FORECAST_DAYS_SHOWN = 3;

/** "5 questions tomorrow, 4 in 3 days": what the count belongs to is said out loud. At most the next FORECAST_DAYS_SHOWN days with reviews. */
export const forecastText = (buckets: readonly ForecastBucket[]): string =>
  buckets.slice(0, FORECAST_DAYS_SHOWN).map((b, i) => `${i === 0 ? plural(b.count, 'question') : b.count} ${inDays(b.inDays)}`).join(', ');

/** "A", "A and B", "A, B and C". */
export const listNames = (names: readonly string[]): string =>
  names.length <= 1 ? (names[0] ?? '') : `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}`;

/** Counts are capped so a long break never shows a scary number. */
export const cappedCount = (n: number, cap = 30): string => (n > cap ? `${cap}+` : String(n));

export const prefersReducedMotion = (): boolean => {
  try {
    return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  } catch {
    return false;
  }
};

export const WEEKDAYS = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'] as const;

/** A day key ("2026-11-04") for people, in their own locale and without a timezone shift ("Nov 4, 2026"). Falls back to the key itself. */
export function formatDay(key: string): string {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(key);
  if (!m) return key;
  const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  try {
    return new Intl.DateTimeFormat(undefined, { day: 'numeric', month: 'short', year: 'numeric' }).format(d);
  } catch {
    return key;
  }
}

/** A number with the reader's own digit grouping ("1,260" or "1.260"). */
export const formatCount = (n: number): string => {
  try {
    return n.toLocaleString();
  } catch {
    return String(n);
  }
};
