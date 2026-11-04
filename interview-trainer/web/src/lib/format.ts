import type { ForecastBucket } from '@trainer/shared';

/** 1 -> "tomorrow", 3 -> "in 3 days". */
export const inDays = (n: number): string => (n <= 1 ? 'tomorrow' : `in ${n} days`);

export const plural = (n: number, one: string, many = `${one}s`): string => `${n} ${n === 1 ? one : many}`;

/** "tomorrow (5), in 3 days (4)" */
export const forecastText = (buckets: readonly ForecastBucket[]): string => buckets.map((b) => `${inDays(b.inDays)} (${b.count})`).join(', ');

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
