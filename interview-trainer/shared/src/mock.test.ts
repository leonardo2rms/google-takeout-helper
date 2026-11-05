import { describe, expect, it } from 'vitest';
import {
  MAX_MOCKS, MOCK_MINUTES, MOCK_REWARDED_PER_DAY, MOCK_XP_CAP, MOCK_XP_PER_ANSWER, MOCK_WEAK_BELOW, addDays, allQuestions, applyMock, buildMock, buildReport,
  clockText, defaultMinutes, elapsedMs, emptyProgress, goalToday, isTimeUp, mockCredit, mockSeed, pauseTimer, remainingMs, resumeTimer, startTimer, timeAnnouncement,
  unitOfFor, type Course, type MockConfig, type MockResult, type Progress, type Question,
} from './index';
import { withCards } from './fixtures';

const today = '2026-11-04';
const TYPES: Question['type'][] = ['single', 'multi', 'fill', 'order', 'spot', 'estimate', 'match', 'code', 'open'];

/** Two topics x 12 questions, formats cycling through all nine types. */
function course(): Course {
  const mk = (unit: string) =>
    Array.from({ length: 12 }, (_, i) => ({ id: `${unit}-1/${i + 1}`, type: TYPES[i % TYPES.length], prompt: `${unit} q${i + 1}`, explanation: 'x', difficulty: 'easy', tags: [] }));
  return {
    units: ['a', 'b'].map((u) => ({ id: u, title: `Unit ${u}`, emoji: u, color: '#000', description: '', lessons: [{ id: `${u}-1`, title: `${u}1`, questions: mk(u) }] })),
  } as unknown as Course;
}
const C = course();
const qs = allQuestions(C);
const unitOf = unitOfFor(C);
const cfg = (over: Partial<MockConfig> = {}): MockConfig => ({ length: 10, minutes: 15, unitId: null, ...over });
const ids = (l: Question[]) => l.map((q) => q.id);

describe('buildMock', () => {
  it('picks the requested number of distinct questions, deterministically for a seed', () => {
    const a = buildMock(C, emptyProgress(), today, cfg(), 7);
    expect(a).toHaveLength(10);
    expect(new Set(ids(a)).size).toBe(10);
    expect(ids(buildMock(C, emptyProgress(), today, cfg(), 7))).toEqual(ids(a));
    expect(ids(buildMock(C, emptyProgress(), today, cfg(), 8))).not.toEqual(ids(a));
    expect(buildMock(C, emptyProgress(), today, cfg({ length: 20 }), 1)).toHaveLength(20);
  });

  it('mixes formats and caps the slow ones (code, open)', () => {
    for (let seed = 0; seed < 60; seed++) {
      const m = buildMock(C, emptyProgress(), today, cfg(), seed);
      const count = (t: string) => m.filter((q) => q.type === t).length;
      expect(new Set(m.map((q) => q.type)).size, `seed ${seed}`).toBeGreaterThanOrEqual(5);
      expect(count('code'), `seed ${seed}`).toBeLessThanOrEqual(1);
      expect(count('open'), `seed ${seed}`).toBeLessThanOrEqual(2);
      const m20 = buildMock(C, emptyProgress(), today, cfg({ length: 20 }), seed);
      expect(m20.filter((q) => q.type === 'code').length).toBeLessThanOrEqual(2);
      expect(m20.filter((q) => q.type === 'open').length).toBeLessThanOrEqual(4);
    }
  });

  it('leans towards weak topics: a topic you have mastered is drawn less than one you have not touched', () => {
    const strong = withCards(emptyProgress(), qs.filter((q) => unitOf(q) === 'a').map((q): [Question, object] => [q, { interval: 60, due: addDays(today, 40) }]));
    let a = 0;
    let b = 0;
    for (let seed = 0; seed < 200; seed++) for (const q of buildMock(C, strong, today, cfg(), seed)) unitOf(q) === 'a' ? a++ : b++;
    expect(b).toBeGreaterThan(a * 1.5);
  });

  it('can be limited to one topic, and shrinks when the pool is smaller than the length', () => {
    const only = buildMock(C, emptyProgress(), today, cfg({ unitId: 'b' }), 3);
    expect(only.every((q) => unitOf(q) === 'b')).toBe(true);
    const tiny = { units: [{ ...C.units[0]!, lessons: [{ ...C.units[0]!.lessons[0]!, questions: C.units[0]!.lessons[0]!.questions.slice(0, 4) }] }] } as unknown as Course;
    expect(buildMock(tiny, emptyProgress(), today, cfg(), 1)).toHaveLength(4);
    expect(buildMock(C, emptyProgress(), today, cfg({ unitId: 'nope' }), 1)).toEqual([]);
  });

  it('takes capped formats when nothing else is left (an all-open topic)', () => {
    const opens = { units: [{ id: 'o', title: 'O', emoji: 'o', color: '#000', description: '', lessons: [{ id: 'o-1', title: 'O', questions: Array.from({ length: 10 }, (_, i) => ({ id: `o-1/${i + 1}`, type: 'open', prompt: `p${i}`, tags: [] })) }] }] } as unknown as Course;
    expect(buildMock(opens, emptyProgress(), today, cfg(), 1)).toHaveLength(10);
  });

  it('alternates topics where it can', () => {
    const m = buildMock(C, emptyProgress(), today, cfg(), 5);
    const topics = m.map(unitOf);
    expect(topics.filter((t, i) => i > 0 && t === topics[i - 1]).length).toBeLessThanOrEqual(1);
  });

  it('mockSeed changes with the day, sessions and past mocks', () => {
    const p = emptyProgress();
    expect(mockSeed(p, today)).not.toBe(mockSeed(p, addDays(today, 1)));
    expect(mockSeed({ ...p, stats: { ...p.stats, sessions: 3 } }, today)).not.toBe(mockSeed(p, today));
  });

  it('offers sensible time limits', () => {
    expect(defaultMinutes(10)).toBe(15);
    expect(defaultMinutes(20)).toBe(30);
    for (const l of [10, 20] as const) expect(MOCK_MINUTES[l]).toContain(defaultMinutes(l));
  });
});

describe('timer', () => {
  const t0 = startTimer(1_000_000, 15);
  it('counts down and expires', () => {
    expect(remainingMs(t0, 1_000_000)).toBe(15 * 60_000);
    expect(remainingMs(t0, 1_000_000 + 60_000)).toBe(14 * 60_000);
    expect(isTimeUp(t0, 1_000_000 + 15 * 60_000 - 1)).toBe(false);
    expect(isTimeUp(t0, 1_000_000 + 15 * 60_000)).toBe(true);
    expect(remainingMs(t0, 1_000_000 + 99 * 60_000)).toBe(0);
  });
  it('stops while paused and continues after resume', () => {
    const paused = pauseTimer(t0, 1_000_000 + 60_000);
    expect(elapsedMs(paused, 1_000_000 + 600_000)).toBe(60_000);
    expect(pauseTimer(paused, 1_000_000 + 700_000)).toBe(paused); // a second pause changes nothing
    const back = resumeTimer(paused, 1_000_000 + 600_000);
    expect(elapsedMs(back, 1_000_000 + 600_000)).toBe(60_000);
    expect(elapsedMs(back, 1_000_000 + 660_000)).toBe(120_000);
    expect(resumeTimer(back, 1_000_000 + 700_000)).toBe(back);
  });
  it('never goes negative when the clock moves backwards', () => {
    expect(elapsedMs(t0, 500_000)).toBe(0);
    expect(remainingMs(t0, 500_000)).toBe(15 * 60_000);
    const odd = resumeTimer(pauseTimer(t0, 2_000_000), 1_900_000);
    expect(odd.pausedMs).toBe(0);
  });
  it('formats the clock, rounding up', () => {
    expect(clockText(15 * 60_000)).toBe('15:00');
    expect(clockText(61_000)).toBe('1:01');
    expect(clockText(59_001)).toBe('1:00');
    expect(clockText(1)).toBe('0:01');
    expect(clockText(0)).toBe('0:00');
    expect(clockText(-5)).toBe('0:00');
  });
  it('announces five minutes, one minute and time up, once each', () => {
    expect(timeAnnouncement(6 * 60_000, 4.9 * 60_000)).toBe('5 minutes left.');
    expect(timeAnnouncement(5 * 60_000, 4.9 * 60_000)).toBeNull();
    expect(timeAnnouncement(2 * 60_000, 59_000)).toBe('One minute left.');
    expect(timeAnnouncement(20_000, 0)).toBe('Time is up. Your answers are being submitted.');
    expect(timeAnnouncement(0, 0)).toBeNull();
    expect(timeAnnouncement(10 * 60_000, 9 * 60_000)).toBeNull();
    expect(timeAnnouncement(10 * 60_000, 0)).toBe('Time is up. Your answers are being submitted.');
  });
});

const res = (id: string, unitId: string, score: number, answered = true, flagged = false): MockResult => ({ id, unitId, score, answered, flagged });

describe('buildReport', () => {
  it('scores per topic, flags weak ones weakest first, and lists wrong answers', () => {
    const r = buildReport([res('a/1', 'a', 1), res('a/2', 'a', 1), res('b/1', 'b', 0), res('b/2', 'b', 0.5), res('c/1', 'c', 0, false)]);
    expect(r).toMatchObject({ total: 5, answered: 4, correct: 2, score: 50, wrong: ['b/1', 'b/2'] });
    expect(r.byTopic.map((t) => [t.unitId, t.pct, t.weak])).toEqual([['c', 0, true], ['b', 25, true], ['a', 100, false]]);
    expect(r.weakSpots).toEqual(['c', 'b']);
  });
  it('unanswered questions count as zero and are never "wrong answers"', () => {
    const r = buildReport([res('a/1', 'a', 1), res('a/2', 'a', 0, false)]);
    expect(r.score).toBe(50);
    expect(r.wrong).toEqual([]);
  });
  it('the weak-spot line is exactly MOCK_WEAK_BELOW', () => {
    const at = buildReport([...Array.from({ length: 6 }, (_, i) => res(`a/${i}`, 'a', 1)), ...Array.from({ length: 4 }, (_, i) => res(`a/x${i}`, 'a', 0))]);
    expect(at.byTopic[0]!.pct).toBe(MOCK_WEAK_BELOW);
    expect(at.byTopic[0]!.weak).toBe(false);
  });
  it('an empty mock reports zeros', () => {
    expect(buildReport([])).toMatchObject({ total: 0, score: 0, byTopic: [], weakSpots: [] });
  });
});

describe('mockCredit', () => {
  const p = emptyProgress();
  it('is capped in XP and in goal credit (at most half the goal)', () => {
    expect(mockCredit(p, today, 3)).toEqual({ xp: 3 * MOCK_XP_PER_ANSWER, goalCredit: 3, rewarded: true });
    expect(mockCredit(p, today, 20)).toEqual({ xp: MOCK_XP_CAP, goalCredit: 4, rewarded: true }); // default goal 8 -> 4
    expect(mockCredit({ ...p, goal: { target: 24 } }, today, 20).goalCredit).toBe(12);
    expect(mockCredit({ ...p, goal: { target: 1 } }, today, 20).goalCredit).toBe(1);
  });
  it('only the first mocks of a day are rewarded; none for an empty one', () => {
    const rec = (day: string) => ({ day, total: 10, answered: 5, score: 50, limitMin: 15, usedSec: 100, timedOut: false, byUnit: {} });
    const many = { ...p, mocks: [rec(today), rec(today), rec(addDays(today, -1))] };
    expect(MOCK_REWARDED_PER_DAY).toBe(2);
    expect(mockCredit(many, today, 10)).toEqual({ xp: 0, goalCredit: 0, rewarded: false });
    expect(mockCredit({ ...p, mocks: [rec(today)] }, today, 10).rewarded).toBe(true);
    expect(mockCredit({ ...p, mocks: [rec(addDays(today, -1)), rec(addDays(today, -1))] }, today, 10).rewarded).toBe(true);
    expect(mockCredit(p, today, 0).rewarded).toBe(false);
  });
});

describe('applyMock', () => {
  const q = (i: number) => qs[i]!;
  const outcome = (results: MockResult[], over = {}) => ({ results, limitMin: 15, usedSec: 400, timedOut: false, ...over });
  const run = (p: Progress, results: MockResult[], over = {}) => applyMock(p, outcome(results, over), { today, course: C });

  it('wrong answers become due cards tomorrow, right ones are scheduled, unanswered ones are left alone', () => {
    const { progress } = run(emptyProgress(), [res(q(0).id, 'a', 1), res(q(1).id, 'a', 0), res(q(2).id, 'a', 0, false)]);
    expect(progress.cards[q(0).id]).toMatchObject({ interval: 1, lapses: 0 });
    expect(progress.cards[q(1).id]).toMatchObject({ lapses: 1, due: addDays(today, 1) });
    expect(progress.cards[q(2).id]).toBeUndefined();
    expect(progress.missed).toEqual([q(1).id]);
  });

  it('records the mock (newest first, last ten) and a scorecard by topic', () => {
    let p = emptyProgress();
    for (let i = 0; i < MAX_MOCKS + 3; i++) p = run(p, [res(q(0).id, 'a', 1), res(q(12).id, 'b', 0)], { usedSec: 100 + i }).progress;
    expect(p.mocks).toHaveLength(MAX_MOCKS);
    expect(p.mocks[0]).toMatchObject({ day: today, total: 2, answered: 2, score: 50, limitMin: 15, usedSec: 100 + MAX_MOCKS + 2, timedOut: false, byUnit: { a: { n: 1, score: 1 }, b: { n: 1, score: 0 } } });
  });

  it('XP and goal credit are capped; later mocks the same day earn nothing; still counts as practice', () => {
    const results = qs.slice(0, 20).map((x) => res(x.id, unitOf(x), 1));
    const first = run(emptyProgress(), results);
    expect(first.credit).toEqual({ xp: MOCK_XP_CAP, goalCredit: 4, rewarded: true });
    expect(first.progress.xp).toBe(MOCK_XP_CAP);
    expect(goalToday(first.progress, today)).toMatchObject({ done: 4, met: false });
    expect(first.progress.streak).toBe(1);
    const second = run(first.progress, results);
    const third = run(second.progress, results);
    expect(third.credit).toEqual({ xp: 0, goalCredit: 0, rewarded: false });
    expect(third.progress.xp).toBe(MOCK_XP_CAP * 2);
    expect(goalToday(third.progress, today).done).toBe(8); // two rewarded mocks: 4 + 4, the third adds nothing
    expect(third.progress.cards[results[0]!.id]).toBeDefined(); // but it is still scheduled
  });

  it('is never a perfect lesson, never shows a tip, and keeps first tries within the credited answers', () => {
    const results = qs.slice(0, 10).map((x) => res(x.id, unitOf(x), 1));
    const { progress, events } = run(emptyProgress(), results);
    expect(progress.stats.perfect).toBe(0);
    expect(events.map((e) => e.type)).not.toContain('perfect');
    expect(events.map((e) => e.type)).not.toContain('tip');
    expect(progress.days[today]!.firstTry).toBeLessThanOrEqual(progress.days[today]!.answered);
  });

  it('saves nothing when nothing was answered', () => {
    const p = emptyProgress();
    const out = run(p, [res(q(0).id, 'a', 0, false)]);
    expect(out.progress).toBe(p);
    expect(out.report.answered).toBe(0);
  });

  it('records a timed-out mock as such', () => {
    const { progress } = run(emptyProgress(), [res(q(0).id, 'a', 1)], { timedOut: true, usedSec: 900 });
    expect(progress.mocks[0]).toMatchObject({ timedOut: true, usedSec: 900 });
  });
});
