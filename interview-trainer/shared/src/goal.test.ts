import { describe, expect, it } from 'vitest';
import { addDays, answeredInLast, clampGoal, DAYS_KEPT, emptyProgress, goalDisplay, goalToday, GOAL_PRESETS, lastDays, recordDay, setGoalTarget, targetFor, type Progress } from './index';

const today = '2026-11-04';
const log = (answered: number, target = 8, extra = {}) => ({ xp: answered * 10, answered, firstTry: answered, target, ...extra });

describe('goalToday', () => {
  it('starts empty: 0 of 8', () => {
    expect(goalToday(emptyProgress(), today)).toEqual({ target: 8, done: 0, fraction: 0, met: false, comeback: false });
  });
  it('fraction clamps to 1 and met flips exactly at the target', () => {
    const p = (n: number): Progress => ({ ...emptyProgress(), days: { [today]: log(n) } });
    expect(goalToday(p(7), today)).toMatchObject({ done: 7, met: false, fraction: 7 / 8 });
    expect(goalToday(p(8), today)).toMatchObject({ met: true, fraction: 1 });
    expect(goalToday(p(30), today)).toMatchObject({ met: true, fraction: 1 });
  });
  it('presets are 8 / 16 / 24 and goals are clamped to 1..100', () => {
    expect(GOAL_PRESETS).toEqual({ light: 8, regular: 16, serious: 24 });
    expect(clampGoal(0)).toBe(1);
    expect(clampGoal(500)).toBe(100);
    expect(clampGoal(12.6)).toBe(13);
    expect(clampGoal(Number.NaN)).toBe(8);
  });
});

describe('comeback goal', () => {
  const away = (days: number): Progress => ({ ...emptyProgress(), lastDay: addDays(today, -days), goal: { target: 8 } });
  it('halves the goal after 7 or more days away, and only then', () => {
    expect(targetFor(away(6), today)).toEqual({ target: 8, comeback: false });
    expect(targetFor(away(7), today)).toEqual({ target: 4, comeback: true });
    expect(targetFor(away(60), today)).toEqual({ target: 4, comeback: true });
    expect(targetFor({ ...away(30), goal: { target: 1 } }, today)).toEqual({ target: 1, comeback: true });
    expect(targetFor(emptyProgress(), today)).toEqual({ target: 8, comeback: false });
  });
  it('stays halved for the rest of that day once practice has started', () => {
    const p = away(10);
    const days = recordDay(p, today, { xp: 10, answered: 3, firstTry: 3 });
    const after: Progress = { ...p, days, lastDay: today };
    expect(days[today]).toMatchObject({ target: 4, comeback: true, answered: 3 });
    expect(goalToday(after, today)).toMatchObject({ target: 4, comeback: true, done: 3, met: false });
    const more: Progress = { ...after, days: recordDay(after, today, { xp: 10, answered: 2, firstTry: 2 }) };
    expect(goalToday(more, today)).toMatchObject({ done: 5, met: true });
  });
});

describe('recordDay', () => {
  it('accumulates across two sessions in one day', () => {
    let p = emptyProgress();
    p = { ...p, days: recordDay(p, today, { xp: 80, answered: 8, firstTry: 6 }) };
    p = { ...p, days: recordDay(p, today, { xp: 40, answered: 4, firstTry: 4 }) };
    expect(p.days[today]).toEqual({ xp: 120, answered: 12, firstTry: 10, target: 8 });
  });
  it('snapshots the target so changing the goal does not rewrite history', () => {
    let p: Progress = { ...emptyProgress(), goal: { target: 16 } };
    p = { ...p, days: recordDay(p, '2026-11-01', { xp: 1, answered: 10, firstTry: 1 }) };
    p = { ...p, goal: { target: 8 } };
    expect(p.days['2026-11-01']!.target).toBe(16);
    expect(lastDays(p, today, 5).find((d) => d.day === '2026-11-01')).toMatchObject({ target: 16, met: false });
  });
  it('prunes days older than DAYS_KEPT but keeps recent ones', () => {
    const old = addDays(today, -DAYS_KEPT);
    const recent = addDays(today, -(DAYS_KEPT - 1));
    const p: Progress = { ...emptyProgress(), days: { [old]: log(5), [recent]: log(5) } };
    const days = recordDay(p, today, { xp: 1, answered: 1, firstTry: 1 });
    expect(Object.keys(days).sort()).toEqual([recent, today]);
  });
  it('setGoalTarget applies to today but not to the past', () => {
    const p: Progress = { ...emptyProgress(), days: { [today]: log(5, 8), '2026-11-03': log(5, 8) } };
    const next = setGoalTarget(p, 16, today);
    expect(next.goal.target).toBe(16);
    expect(next.days[today]!.target).toBe(16);
    expect(next.days['2026-11-03']!.target).toBe(8);
    expect(setGoalTarget(emptyProgress(), 9999, today).goal.target).toBe(100);
  });
});

describe('lastDays', () => {
  it('is oldest first, zero-filled, and ends today', () => {
    const p: Progress = { ...emptyProgress(), days: { [today]: log(3), [addDays(today, -2)]: log(9) } };
    const bars = lastDays(p, today, 14);
    expect(bars).toHaveLength(14);
    expect(bars[13]).toMatchObject({ day: today, answered: 3, met: false });
    expect(bars[0]!.day).toBe(addDays(today, -13));
    expect(bars[11]).toMatchObject({ answered: 9, met: true });
    expect(bars[12]).toMatchObject({ answered: 0, xp: 0, met: false });
  });
  it('defaults to 14 days and sums a window', () => {
    const p: Progress = { ...emptyProgress(), days: { [today]: log(3), [addDays(today, -6)]: log(4), [addDays(today, -7)]: log(100) } };
    expect(lastDays(p, today)).toHaveLength(14);
    expect(answeredInLast(p, today, 7)).toBe(7);
  });
});

describe('goalDisplay: one rule for exceeded goals', () => {
  it('never shows more than the target; the extra is a named bonus', () => {
    expect(goalDisplay({ done: 3, target: 8 })).toMatchObject({ shown: 3, bonus: 0, ratio: '3/8', sentence: '3 of 8 questions today' });
    expect(goalDisplay({ done: 8, target: 8 })).toMatchObject({ shown: 8, bonus: 0, ratio: '8/8' });
    expect(goalDisplay({ done: 19, target: 8 })).toMatchObject({ shown: 8, bonus: 11, ratio: '8/8', sentence: '8 of 8 questions today' });
  });
});
