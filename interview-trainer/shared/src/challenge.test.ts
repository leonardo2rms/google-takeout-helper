import { describe, expect, it } from 'vitest';
import {
  CHALLENGE_KEPT, CHALLENGE_WEIGHTS, addDays, challengeQuestion, challengeResult, challengeStreak, emptyProgress, recordChallenge, type Course, type Question,
} from './index';
import { mkCourse } from './fixtures';

const today = '2026-11-04';
const types = Object.keys(CHALLENGE_WEIGHTS) as Question['type'][];

/** One question of every type, several of each, so the weights are visible. */
function wideCourse(perType = 6): Course {
  const questions = types.flatMap((t) => Array.from({ length: perType }, (_, i) => ({ id: `w-1/${t}${i}`, type: t, prompt: `${t} ${i}`, explanation: 'x', tags: [], difficulty: 'easy' })));
  return { units: [{ id: 'w', title: 'W', emoji: 'W', color: '#000', description: '', lessons: [{ id: 'w-1', title: 'W1', questions }] }] } as unknown as Course;
}

describe('challengeQuestion', () => {
  const course = wideCourse();
  it('is deterministic: the same date always gives the same question', () => {
    expect(challengeQuestion(course, today)!.id).toBe(challengeQuestion(course, today)!.id);
    expect(challengeQuestion(wideCourse(), today)!.id).toBe(challengeQuestion(course, today)!.id);
  });

  it('never repeats the previous day, and different days differ over a year', () => {
    const ids: string[] = [];
    for (let i = 0; i < 365; i++) ids.push(challengeQuestion(course, addDays('2026-01-01', i))!.id);
    for (let i = 1; i < ids.length; i++) expect(ids[i], `day ${i}`).not.toBe(ids[i - 1]);
    expect(new Set(ids).size).toBeGreaterThan(30);
  });

  it('covers every type except match over a year, and favours spot and estimate', () => {
    const count: Record<string, number> = {};
    for (let i = 0; i < 730; i++) {
      const q = challengeQuestion(course, addDays('2026-01-01', i))!;
      count[q.type] = (count[q.type] ?? 0) + 1;
    }
    expect(count.match).toBeUndefined();
    for (const t of types.filter((x) => x !== 'match')) expect(count[t], t).toBeGreaterThan(20);
    expect(count.spot!).toBeGreaterThan(count.single! * 1.3);
    expect(count.estimate!).toBeGreaterThan(count.open! * 1.3);
  });

  it('handles tiny and empty courses', () => {
    expect(challengeQuestion(mkCourse({}), today)).toBeNull();
    const one = mkCourse({ a: { 'a-1': 1 } });
    expect(challengeQuestion(one, today)!.id).toBe('a-1/1'); // a single question cannot avoid repeating itself
    const onlyMatch = { units: [{ id: 'm', title: 'M', emoji: 'M', color: '#000', description: '', lessons: [{ id: 'm-1', title: 'M', questions: [{ id: 'm-1/1', type: 'match', prompt: 'p' }] }] }] } as unknown as Course;
    expect(challengeQuestion(onlyMatch, today)).toBeNull();
  });
});

describe('recordChallenge / challengeResult', () => {
  it('records the first result of the day only', () => {
    const p1 = recordChallenge(emptyProgress(), today, false);
    expect(challengeResult(p1, today)).toBe('fail');
    expect(recordChallenge(p1, today, true)).toBe(p1);
    expect(challengeResult(recordChallenge(emptyProgress(), today, true), today)).toBe('pass');
    expect(challengeResult(emptyProgress(), today)).toBeNull();
  });

  it('prunes entries older than the retention window', () => {
    const old = addDays(today, -CHALLENGE_KEPT - 1);
    const p = recordChallenge({ ...emptyProgress(), challenge: { [old]: 'pass', [addDays(today, -3)]: 'fail' } }, today, true);
    expect(Object.keys(p.challenge).sort()).toEqual([addDays(today, -3), today]);
  });
});

describe('challengeStreak', () => {
  const ch = (...offsets: number[]) => ({ challenge: Object.fromEntries(offsets.map((o) => [addDays(today, o), o % 2 === 0 ? 'pass' : 'fail'])) as Record<string, 'pass' | 'fail'> });
  it('is 0 with no results', () => {
    expect(challengeStreak(emptyProgress(), today)).toEqual({ current: 0, best: 0, done: 0, passed: 0 });
  });
  it('counts consecutive days ending today, pass or fail', () => {
    expect(challengeStreak(ch(0, -1, -2), today)).toMatchObject({ current: 3, best: 3, done: 3, passed: 2 });
  });
  it('is not broken while today is still open', () => {
    expect(challengeStreak(ch(-1, -2), today).current).toBe(2);
  });
  it('resets after a missed day, but keeps the best', () => {
    expect(challengeStreak(ch(0, -2, -3, -4, -5), today)).toMatchObject({ current: 1, best: 4 });
    expect(challengeStreak(ch(-2, -3), today).current).toBe(0);
  });
  it('handles month and year boundaries', () => {
    const p = { challenge: { '2026-12-31': 'pass', '2027-01-01': 'pass', '2027-01-02': 'fail' } as Record<string, 'pass' | 'fail'> };
    expect(challengeStreak(p, '2027-01-02')).toMatchObject({ current: 3, best: 3 });
  });
});
