import { describe, expect, it } from 'vitest';
import { cappedCount, forecastText, inDays, plural } from './format';

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
