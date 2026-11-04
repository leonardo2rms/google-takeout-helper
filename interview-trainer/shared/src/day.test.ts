import { describe, expect, it } from 'vitest';
import { addDays, dayKey, dayNumber, daysBetween, fnv1a, fromDayNumber, isDay, mulberry32, questionHash, rankFor, RANKS, weekday } from './index';
import { mkQuestion } from './fixtures';

describe('day helpers', () => {
  it('isDay accepts real dates only', () => {
    for (const ok of ['2026-02-28', '2024-02-29', '2026-12-31', '1970-01-01']) expect(isDay(ok), ok).toBe(true);
    for (const bad of ['2026-02-30', '2025-02-29', '2026-13-01', '2026-00-10', '2026-1-1', '26-01-01', '', ' 2026-01-01', '2026-01-01T00:00', null, undefined, 20260101, {}, '1969-12-31', '3000-01-01']) {
      expect(isDay(bad), String(bad)).toBe(false);
    }
  });

  it('dayKey is the local calendar day', () => {
    expect(dayKey(new Date(2026, 0, 5, 23, 59))).toBe('2026-01-05');
    expect(dayKey(new Date(2026, 11, 31, 0, 0))).toBe('2026-12-31');
    expect(dayKey(new Date(2026, 2, 8, 12))).toBe('2026-03-08');
  });

  it('dayKey follows the machine timezone across a DST change', () => {
    const before = process.env.TZ;
    process.env.TZ = 'America/New_York';
    try {
      // 2026-03-08 is the 23-hour spring-forward day; 2026-11-01 is the 25-hour fall-back day.
      expect(dayKey(new Date(2026, 2, 8, 23, 59))).toBe('2026-03-08');
      expect(dayKey(new Date(2026, 2, 9, 0, 1))).toBe('2026-03-09');
      expect(dayKey(new Date(2026, 10, 1, 23, 59))).toBe('2026-11-01');
      expect(dayKey(new Date(2026, 10, 2, 0, 1))).toBe('2026-11-02');
    } finally {
      if (before === undefined) delete process.env.TZ;
      else process.env.TZ = before;
    }
  });

  it('day arithmetic ignores DST and handles leap days and year ends', () => {
    expect(addDays('2026-03-07', 1)).toBe('2026-03-08');
    expect(addDays('2026-03-08', 1)).toBe('2026-03-09');
    expect(addDays('2026-11-01', 1)).toBe('2026-11-02');
    expect(addDays('2028-02-28', 1)).toBe('2028-02-29');
    expect(addDays('2028-02-29', 1)).toBe('2028-03-01');
    expect(addDays('2026-02-28', 1)).toBe('2026-03-01');
    expect(addDays('2026-12-31', 1)).toBe('2027-01-01');
    expect(addDays('2027-01-01', -1)).toBe('2026-12-31');
    expect(daysBetween('2026-01-01', '2026-01-01')).toBe(0);
    expect(daysBetween('2026-03-01', '2026-03-08')).toBe(7);
    expect(daysBetween('2026-03-08', '2026-03-01')).toBe(-7);
    expect(daysBetween('2024-12-31', '2026-12-31')).toBe(730);
    expect(fromDayNumber(dayNumber('2026-11-04'))).toBe('2026-11-04');
  });

  it('weekday is 0 = Sunday', () => {
    expect(weekday('2026-11-01')).toBe(0);
    expect(weekday('2026-11-04')).toBe(3);
    expect(weekday('2026-11-07')).toBe(6);
    expect(weekday('1970-01-01')).toBe(4);
    expect(weekday('1969-12-31' as never)).toBe(3); // before the epoch still maps correctly
  });
});

describe('hash and rng', () => {
  it('fnv1a matches known vectors and is unsigned', () => {
    expect(fnv1a('')).toBe(0x811c9dc5);
    expect(fnv1a('a')).toBe(0xe40c292c);
    expect(fnv1a('foobar')).toBe(0xbf9cf968);
    expect(fnv1a('anything')).toBeGreaterThanOrEqual(0);
  });
  it('questionHash depends on type and prompt only, and is cached', () => {
    const a = mkQuestion('l/1', 'same prompt');
    const b = mkQuestion('l/9', 'same prompt');
    expect(questionHash(a)).toBe(questionHash(b));
    expect(questionHash(a)).toBe(questionHash(a));
    expect(questionHash(mkQuestion('l/1', 'other'))).not.toBe(questionHash(a));
    expect(questionHash({ ...a, type: 'multi' } as never)).not.toBe(questionHash(a));
  });
  it('mulberry32 is deterministic and in [0, 1)', () => {
    const r1 = mulberry32(42);
    const r2 = mulberry32(42);
    const xs = Array.from({ length: 100 }, () => r1());
    expect(xs).toEqual(Array.from({ length: 100 }, () => r2()));
    expect(xs.every((x) => x >= 0 && x < 1)).toBe(true);
    expect(mulberry32(43)()).not.toBe(xs[0]);
  });
});

describe('ranks', () => {
  it('changes exactly at each floor', () => {
    RANKS.forEach((r, i) => {
      expect(rankFor(r.xp).index).toBe(i);
      if (r.xp > 0) expect(rankFor(r.xp - 1).index).toBe(i - 1);
      expect(rankFor(r.xp + 1).index).toBe(i);
    });
  });
  it('reports the next rank and the fraction towards it', () => {
    const r = rankFor(325);
    expect(r).toMatchObject({ title: 'Engineer', floor: 150, next: 500, nextTitle: 'Senior' });
    expect(r.fraction).toBeCloseTo(0.5);
    expect(rankFor(0).fraction).toBe(0);
  });
  it('the top rank has no next and a full bar; bad input is rank 0', () => {
    expect(rankFor(99999)).toMatchObject({ title: 'Fellow', next: null, nextTitle: null, fraction: 1 });
    expect(rankFor(-5).index).toBe(0);
    expect(rankFor(Number.NaN).index).toBe(0);
  });
});
