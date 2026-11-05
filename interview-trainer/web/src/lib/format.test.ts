import { afterEach, describe, expect, it, vi } from 'vitest';
import { WEEKDAYS, cappedCount, forecastText, inDays, plural, prefersReducedMotion } from './format';

afterEach(() => vi.unstubAllGlobals());

describe('format edge cases', () => {
  it('inDays treats negative values as tomorrow and larger ones as a plural', () => {
    expect(inDays(-4)).toBe('tomorrow');
    expect(inDays(2)).toBe('in 2 days');
  });
  it('cappedCount honours a custom cap and the exact boundary', () => {
    expect(cappedCount(10, 10)).toBe('10');
    expect(cappedCount(11, 10)).toBe('10+');
    expect(cappedCount(0)).toBe('0');
  });
  it('prefersReducedMotion reads the media query and is false when matchMedia is missing or throws', () => {
    vi.stubGlobal('window', { matchMedia: (q: string) => ({ matches: q.includes('reduce') }) });
    expect(prefersReducedMotion()).toBe(true);
    vi.stubGlobal('window', { matchMedia: () => ({ matches: false }) });
    expect(prefersReducedMotion()).toBe(false);
    vi.stubGlobal('window', {});
    expect(prefersReducedMotion()).toBe(false);
    vi.unstubAllGlobals();
    expect(prefersReducedMotion()).toBe(false);
  });
  it('WEEKDAYS is indexed like Date#getDay (Sunday first)', () => {
    expect(WEEKDAYS).toHaveLength(7);
    expect(WEEKDAYS[new Date(2026, 5, 14).getDay()]).toBe('Sunday');
    expect(WEEKDAYS[new Date(2026, 5, 15).getDay()]).toBe('Monday');
  });
});

describe('format helpers', () => {
  it('inDays', () => {
    expect(inDays(1)).toBe('tomorrow');
    expect(inDays(0)).toBe('tomorrow');
    expect(inDays(3)).toBe('in 3 days');
  });
  it('plural', () => {
    expect(plural(1, 'day')).toBe('1 day');
    expect(plural(2, 'day')).toBe('2 days');
    expect(plural(0, 'weak spot')).toBe('0 weak spots');
    expect(plural(2, 'person', 'people')).toBe('2 people');
  });
  it('forecastText lists the buckets', () => {
    expect(forecastText([{ inDays: 1, count: 5 }, { inDays: 3, count: 4 }])).toBe('tomorrow (5), in 3 days (4)');
    expect(forecastText([])).toBe('');
  });
  it('counts are capped so a long break never shows a scary number', () => {
    expect(cappedCount(12)).toBe('12');
    expect(cappedCount(30)).toBe('30');
    expect(cappedCount(47)).toBe('30+');
  });
});
