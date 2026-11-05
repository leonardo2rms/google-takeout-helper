import type { ForecastBucket } from '@trainer/shared';

/** 1 -> "tomorrow", 3 -> "in 3 days". */
export const inDays = (n: number): string => (n <= 1 ? 'tomorrow' : `in ${n} days`);

export const plural = (n: number, one: string, many = `${one}s`): string => `${n} ${n === 1 ? one : many}`;

/** "5 questions tomorrow, 4 in 3 days": what the count belongs to is said out loud. */
export const forecastText = (buckets: readonly ForecastBucket[]): string =>
  buckets.map((b, i) => `${i === 0 ? plural(b.count, 'question') : b.count} ${inDays(b.inDays)}`).join(', ');

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
