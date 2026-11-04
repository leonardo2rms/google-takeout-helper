import { describe, expect, it } from 'vitest';
import { addDays, advanceStreak, emptyProgress, liveStreak, MAX_FREEZES, nextStreak, streakStatus, type Progress } from './index';

// 2026-11-01 is a Sunday.
const mk = (streak: number, lastDay: string | null, freezes = 0, restDays: number[] = []): Progress => ({
  ...emptyProgress(),
  streak,
  longestStreak: streak,
  lastDay,
  freezes,
  profile: { ...emptyProgress().profile, restDays },
});

describe('legacy nextStreak', () => {
  it('increments, holds, resets', () => {
    expect(nextStreak(0, null, '2026-01-01')).toBe(1);
    expect(nextStreak(3, '2026-01-01', '2026-01-02')).toBe(4);
    expect(nextStreak(3, '2026-01-02', '2026-01-02')).toBe(3);
    expect(nextStreak(3, '2026-01-04', '2026-01-02')).toBe(3);
    expect(nextStreak(3, '2026-01-01', '2026-01-04')).toBe(1);
  });
});

describe('streakStatus (pure prediction)', () => {
  it('new, done today, open', () => {
    expect(streakStatus(mk(0, null), '2026-11-04')).toMatchObject({ streak: 0, state: 'new' });
    expect(streakStatus(mk(5, '2026-11-04'), '2026-11-04')).toMatchObject({ streak: 5, state: 'done-today' });
    expect(streakStatus(mk(5, '2026-11-03'), '2026-11-04')).toMatchObject({ streak: 5, state: 'open', freezesNeeded: 0 });
  });

  // gap = days between lastDay and today. Missed days = gap - 1.
  const table: [string, number, number, 'open' | 'saved-by-freeze' | 'lapsed', number][] = [
    // label, gap, freezes, expected state, freezes needed
    ['gap 2, no freeze', 2, 0, 'lapsed', 0],
    ['gap 2, 1 freeze', 2, 1, 'saved-by-freeze', 1],
    ['gap 2, 2 freezes', 2, 2, 'saved-by-freeze', 1],
    ['gap 3, 1 freeze', 3, 1, 'lapsed', 0],
    ['gap 3, 2 freezes', 3, 2, 'saved-by-freeze', 2],
    ['gap 4, 2 freezes', 4, 2, 'lapsed', 0],
    ['gap 30, 2 freezes', 30, 2, 'lapsed', 0],
  ];
  it.each(table)('%s', (_label, gap, freezes, state, needed) => {
    const s = streakStatus(mk(6, '2026-10-01', freezes), addDays('2026-10-01', gap));
    expect(s.state).toBe(state);
    expect(s.freezesNeeded).toBe(needed);
    expect(s.streak).toBe(state === 'lapsed' ? 0 : 6);
  });

  it('rest days are covered first and free', () => {
    // Fri 2026-10-30 practised; Sat/Sun rest; today Mon 2026-11-02 -> both missed days are rest days.
    const p = mk(6, '2026-10-30', 0, [6, 0]);
    expect(streakStatus(p, '2026-11-02')).toMatchObject({ state: 'open', streak: 6, freezesNeeded: 0, restDaysCovered: 2, missedDays: 2 });
    // Tuesday: Monday is not a rest day -> needs one freeze.
    expect(streakStatus(p, '2026-11-03')).toMatchObject({ state: 'lapsed', restDaysCovered: 2 });
    expect(streakStatus({ ...p, freezes: 1 }, '2026-11-03')).toMatchObject({ state: 'saved-by-freeze', freezesNeeded: 1, restDaysCovered: 2, streak: 6 });
  });

  it('is idempotent and never mutates', () => {
    const p = mk(6, '2026-10-30', 1);
    const copy = JSON.stringify(p);
    expect(streakStatus(p, '2026-11-01')).toEqual(streakStatus(p, '2026-11-01'));
    expect(JSON.stringify(p)).toBe(copy);
  });

  it('a clock that moved backwards holds the streak', () => {
    expect(streakStatus(mk(5, '2026-11-10'), '2026-11-04')).toMatchObject({ streak: 5, state: 'done-today' });
  });

  it('liveStreak is the displayed streak', () => {
    expect(liveStreak(mk(4, '2026-01-01'), '2026-01-02')).toBe(4);
    expect(liveStreak(mk(4, '2026-01-01'), '2026-01-03')).toBe(0);
    expect(liveStreak(mk(4, '2026-01-01', 1), '2026-01-03')).toBe(4);
  });
});

describe('advanceStreak', () => {
  it('first practice, consecutive days, same day', () => {
    expect(advanceStreak(mk(0, null), '2026-11-04')).toMatchObject({ streak: 1, longestStreak: 1, lastDay: '2026-11-04', freezeUsed: 0, incremented: true });
    expect(advanceStreak(mk(3, '2026-11-03'), '2026-11-04')).toMatchObject({ streak: 4, incremented: true });
    expect(advanceStreak(mk(3, '2026-11-04'), '2026-11-04')).toMatchObject({ streak: 3, incremented: false, freezeEarned: false });
  });

  it('spends freezes only when they fully cover the gap', () => {
    expect(advanceStreak(mk(6, '2026-10-30', 1), '2026-11-01')).toMatchObject({ streak: 7, freezeUsed: 1 });
    // gap of 3 with one freeze: the streak restarts and the freeze is kept
    const r = advanceStreak(mk(6, '2026-10-29', 1), '2026-11-01');
    expect(r).toMatchObject({ streak: 1, freezes: 1, freezeUsed: 0, restarted: true });
    expect(r.longestStreak).toBe(6);
  });

  it('uses rest days before freezes', () => {
    const r = advanceStreak(mk(6, '2026-10-30', 1, [6, 0]), '2026-11-02');
    expect(r).toMatchObject({ streak: 7, freezeUsed: 0, freezes: 2 }); // rest days free; 7 earns a freeze (1 -> 2)
  });

  it('earns a freeze at each multiple of 7 and caps at MAX_FREEZES', () => {
    expect(advanceStreak(mk(6, '2026-11-03', 0), '2026-11-04')).toMatchObject({ streak: 7, freezes: 1, freezeEarned: true });
    expect(advanceStreak(mk(13, '2026-11-03', 1), '2026-11-04')).toMatchObject({ streak: 14, freezes: 2, freezeEarned: true });
    const capped = advanceStreak(mk(20, '2026-11-03', MAX_FREEZES), '2026-11-04');
    expect(capped).toMatchObject({ streak: 21, freezes: MAX_FREEZES, freezeEarned: false });
    expect(advanceStreak(mk(7, '2026-11-03', 0), '2026-11-04').freezeEarned).toBe(false); // 8 is not a multiple
  });

  it('spends and earns in one practice (the e2e scenario)', () => {
    expect(advanceStreak(mk(6, '2026-11-02', 1), '2026-11-04')).toMatchObject({ streak: 7, freezeUsed: 1, freezeEarned: true, freezes: 1 });
  });

  it('a restart never earns a freeze, even if the restarted value is a multiple of 7', () => {
    expect(advanceStreak(mk(40, '2026-10-01', 0), '2026-11-04')).toMatchObject({ streak: 1, freezeEarned: false });
  });

  it('self-heals when the clock moved backwards', () => {
    const r = advanceStreak(mk(5, '2026-11-10'), '2026-11-04');
    expect(r).toMatchObject({ streak: 5, lastDay: '2026-11-04', incremented: false });
    expect(advanceStreak({ ...mk(5, '2026-11-10'), lastDay: r.lastDay }, '2026-11-05').streak).toBe(6);
  });

  it('crosses leap days and year ends', () => {
    expect(advanceStreak(mk(3, '2028-02-28', 0), '2028-02-29').streak).toBe(4);
    expect(advanceStreak(mk(3, '2028-02-29', 0), '2028-03-01').streak).toBe(4);
    expect(advanceStreak(mk(3, '2026-12-31', 0), '2027-01-01').streak).toBe(4);
    expect(advanceStreak(mk(3, '2026-12-30', 1), '2027-01-01')).toMatchObject({ streak: 4, freezeUsed: 1 });
  });

  it('is idempotent for the same day', () => {
    const once = advanceStreak(mk(6, '2026-11-02', 1), '2026-11-04');
    const p2: Progress = { ...mk(6, '2026-11-02', 1), streak: once.streak, longestStreak: once.longestStreak, freezes: once.freezes, lastDay: once.lastDay };
    const twice = advanceStreak(p2, '2026-11-04');
    expect(twice).toMatchObject({ streak: once.streak, freezes: once.freezes, freezeUsed: 0, freezeEarned: false, incremented: false });
  });
});
