import { describe, expect, it } from 'vitest';
import {
  addDays, allQuestions, applySession, emptyProgress, explanationOf, isLargeCelebration, MAX_FREEZES, MAX_INTERVAL, mulberry32, pickCelebration,
  rankFor, recordResult, TIP_BONUS_XP, tipFor, tipQuestion, type LessonResult, type Progress, type SessionEvent,
} from './index';
import { card, fresh, mkCourse, mkQuestion, withCards } from './fixtures';

const today = '2026-11-04';
const course = mkCourse({ k: { 'k-1': 4 }, s: { 's-1': 4 } });
const qs = allQuestions(course);
const q = (id: string) => qs.find((x) => x.id === id)!;

const perfect = (ids: string[], over: Partial<LessonResult> = {}): LessonResult => ({
  lessonId: 'k-1', accuracy: 1, xp: ids.length * 10 + 10, missed: [], recovered: [], cleared: ids, ...over,
});
const run = (p: Progress, r: LessonResult, day = today) => applySession(p, r, { today: day, course });
const types = (events: SessionEvent[]) => events.map((e) => e.type);

describe('applySession', () => {
  it('records xp, lesson, streak, day log, stats and cards', () => {
    const { progress: p, events } = run(emptyProgress(), perfect(['k-1/1', 'k-1/2', 'k-1/3', 'k-1/4']));
    expect(p).toMatchObject({ xp: 50, streak: 1, longestStreak: 1, lastDay: today, version: 2 });
    expect(p.lessons['k-1']).toMatchObject({ completions: 1, bestAccuracy: 1 });
    expect(p.days[today]).toEqual({ xp: 50, answered: 4, firstTry: 4, target: 8 });
    expect(p.stats).toEqual({ sessions: 1, answered: 4, firstTry: 4, perfect: 1, practiceDays: 1 });
    expect(Object.keys(p.cards)).toHaveLength(4);
    expect(p.cards['k-1/1']).toMatchObject({ interval: 1, due: addDays(today, 1) });
    expect(types(events)).toContain('perfect');
  });

  it('misses are scheduled for tomorrow and stay in the missed pile until recovered in a review', () => {
    const r: LessonResult = { lessonId: 'k-1', accuracy: 0.5, xp: 20, missed: ['k-1/1', 'k-1/2'], recovered: ['k-1/2'], cleared: ['k-1/2', 'k-1/3', 'k-1/4'] };
    const { progress: p } = run(emptyProgress(), r);
    expect(p.missed).toEqual(['k-1/1']);
    expect(p.cards['k-1/1']).toMatchObject({ lapses: 1, interval: 1, firstTry: 0 });
    expect(p.cards['k-1/2']).toMatchObject({ lapses: 1 }); // right only after feedback: still "again"
    expect(p.cards['k-1/3']).toMatchObject({ lapses: 0, firstTry: 1 });
    expect(p.stats.perfect).toBe(0);
  });

  it('keeps the v1-visible fields identical to recordResult', () => {
    const base: Progress = { ...emptyProgress(), xp: 100, streak: 2, longestStreak: 2, lastDay: addDays(today, -1), missed: ['k-1/4'] };
    const r: LessonResult = { lessonId: 'k-1', accuracy: 0.75, xp: 30, missed: ['k-1/1'], recovered: [], cleared: ['k-1/2', 'k-1/3', 'k-1/4'] };
    const a = run(base, r).progress;
    const b = recordResult(base, r, today);
    for (const key of ['xp', 'streak', 'lastDay', 'lessons', 'missed', 'longestStreak', 'freezes', 'days', 'stats'] as const) expect(a[key], key).toEqual(b[key]);
  });

  it('a review session (no lessonId) updates cards and missed but no lesson record', () => {
    const base = { ...emptyProgress(), missed: ['k-1/1'] };
    const { progress: p } = run(base, { accuracy: 1, xp: 10, missed: [], recovered: [], cleared: ['k-1/1'] });
    expect(p.lessons).toEqual({});
    expect(p.missed).toEqual([]);
  });

  it('replaying a lesson the same day cannot inflate the schedule or the practice-day count', () => {
    let p = run(emptyProgress(), perfect(['k-1/1', 'k-1/2', 'k-1/3', 'k-1/4'])).progress;
    const dueBefore = p.cards['k-1/1']!.due;
    for (let i = 0; i < 5; i++) p = run(p, perfect(['k-1/1', 'k-1/2', 'k-1/3', 'k-1/4'])).progress;
    expect(p.cards['k-1/1']).toMatchObject({ due: dueBefore, interval: 1, seen: 6 });
    expect(p.stats).toMatchObject({ practiceDays: 1, sessions: 6 });
    expect(p.streak).toBe(1);
  });

  it('uses outcomes, answered and the deadline when given', () => {
    const r: LessonResult = { accuracy: 1, xp: 5, missed: [], recovered: [], outcomes: { 'k-1/1': 'first' }, answered: 1 };
    const withDate = { ...emptyProgress(), profile: { ...emptyProgress().profile, interviewDate: addDays(today, 0) } };
    const { progress: p } = run(withDate, r);
    expect(p.days[today]!.answered).toBe(1);
    expect(p.cards['k-1/1']!.interval).toBe(1);
  });

  it('records a goal-met moment once, flagging the first time ever', () => {
    const ids = ['k-1/1', 'k-1/2', 'k-1/3', 'k-1/4'];
    let { progress: p, events } = run({ ...emptyProgress(), goal: { target: 4 } }, perfect(ids));
    expect(events).toContainEqual({ type: 'goal-met', first: true });
    expect(p.milestones['goal:first']).toBe(today);
    ({ progress: p, events } = run(p, perfect(ids)));
    expect(types(events)).not.toContain('goal-met'); // already met today
    ({ events } = run(p, perfect(ids), addDays(today, 1)));
    expect(events).toContainEqual({ type: 'goal-met', first: false });
  });
});

describe('streak events', () => {
  it('freeze used and earned in one session (the e2e scenario)', () => {
    const base: Progress = { ...emptyProgress(), streak: 6, longestStreak: 6, lastDay: addDays(today, -2), freezes: 1 };
    const { progress: p, events } = run(base, perfect(['k-1/1']));
    expect(p).toMatchObject({ streak: 7, freezes: 1 });
    expect(events).toContainEqual({ type: 'freeze-used', count: 1 });
    expect(events).toContainEqual({ type: 'freeze-earned' });
    expect(events).toContainEqual({ type: 'streak', value: 7, milestone: true });
    expect(p.milestones['streak:7']).toBe(today);
  });
  it('a streak milestone is announced once; non-milestones are silent ticks', () => {
    const base: Progress = { ...emptyProgress(), streak: 3, longestStreak: 3, lastDay: addDays(today, -1) };
    expect(run(base, perfect(['k-1/1'])).events).toContainEqual({ type: 'streak', value: 4, milestone: false });
    const again: Progress = { ...emptyProgress(), streak: 6, longestStreak: 6, lastDay: addDays(today, -1), milestones: { 'streak:7': addDays(today, -30) } };
    expect(run(again, perfect(['k-1/1'])).events).toContainEqual({ type: 'streak', value: 7, milestone: false });
  });
  it('a lapse restarts quietly without spending the freeze', () => {
    const base: Progress = { ...emptyProgress(), streak: 9, longestStreak: 9, lastDay: addDays(today, -5), freezes: 1 };
    const { progress: p, events } = run(base, perfect(['k-1/1']));
    expect(p).toMatchObject({ streak: 1, freezes: 1, longestStreak: 9 });
    expect(types(events)).not.toContain('freeze-used');
    expect(types(events)).not.toContain('streak');
  });
  it('announces the migration welcome-gift freeze exactly once', () => {
    const base: Progress = { ...emptyProgress(), freezes: 1, milestones: { 'gift:freeze': addDays(today, -1) } };
    const first = run(base, perfect(['k-1/1']));
    expect(types(first.events)).toContain('freeze-gift');
    expect(types(run(first.progress, perfect(['k-1/1'])).events)).not.toContain('freeze-gift');
  });
});

describe('rank-up', () => {
  it('fires when XP crosses a floor and never twice', () => {
    const base: Progress = { ...emptyProgress(), xp: 499, milestones: { 'rank:0': today, 'rank:1': today } };
    const first = run(base, perfect(['k-1/1']));
    expect(rankFor(first.progress.xp).title).toBe('Senior');
    expect(first.events).toContainEqual({ type: 'rank-up', title: 'Senior' });
    expect(first.progress.milestones['rank:2']).toBe(today);
    expect(types(run(first.progress, perfect(['k-1/1'])).events)).not.toContain('rank-up');
  });
  it('a pre-seeded rank does not celebrate', () => {
    const base: Progress = { ...emptyProgress(), xp: 499, milestones: { 'rank:0': today, 'rank:1': today, 'rank:2': today } };
    expect(types(run(base, perfect(['k-1/1'])).events)).not.toContain('rank-up');
  });
});

describe('mastery events', () => {
  it('announces a unit reaching 80%, once', () => {
    const two = mkCourse({ u: { 'u-1': 2 } });
    const [a, b] = allQuestions(two);
    const base = withCards(emptyProgress(), [[a!, { interval: 21, due: addDays(today, 9) }], [b!, { interval: 12, due: today, reps: 3 }]]);
    const r: LessonResult = { lessonId: 'u-1', accuracy: 1, xp: 10, missed: [], recovered: [], cleared: [b!.id] };
    const out = applySession(base, r, { today, course: two });
    expect(out.events).toContainEqual({ type: 'unit-mastered', unitId: 'u', title: 'Unit u' });
    expect(out.progress.milestones['unit:u:80']).toBe(today);
    const again = applySession({ ...out.progress, cards: { ...out.progress.cards, [b!.id]: card(b!, { interval: 21, due: today, reps: 3 }) } }, r, { today, course: two });
    expect(types(again.events)).not.toContain('unit-mastered');
  });
});

describe('readiness events', () => {
  it('announces a label change once and never for "early"', () => {
    const two = mkCourse({ u: { 'u-1': 2 } });
    const [a, b] = allQuestions(two);
    const base = withCards(emptyProgress(), [[a!, { interval: 21, due: addDays(today, 9) }], [b!, { interval: 5, due: today, reps: 3 }]]);
    const r: LessonResult = { lessonId: 'u-1', accuracy: 1, xp: 10, missed: [], recovered: [], cleared: [b!.id] };
    const out = applySession(base, r, { today, course: two });
    expect(out.events).toContainEqual({ type: 'readiness', label: 'ready' });
    expect(out.progress.milestones['readiness:ready']).toBe(today);
    const again = applySession({ ...out.progress, cards: base.cards }, r, { today, course: two });
    expect(types(again.events)).not.toContain('readiness');
    const first = run(emptyProgress(), perfect(['k-1/1']));
    expect(types(first.events)).not.toContain('readiness');
  });
});

describe('tips', () => {
  it('are deterministic per day and session count, and roughly one in six', () => {
    let shown = 0;
    for (let n = 0; n < 6000; n++) {
      const p: Progress = { ...emptyProgress(), stats: { ...emptyProgress().stats, sessions: n } };
      expect(tipFor(p, today)).toEqual(tipFor(p, today));
      if (tipFor(p, today).show) shown++;
    }
    expect(shown).toBeGreaterThan(900);
    expect(shown).toBeLessThan(1100);
  });
  it('can be switched off', () => {
    for (let n = 0; n < 50; n++) {
      const p: Progress = { ...emptyProgress(), settings: { ...emptyProgress().settings, tips: false }, stats: { ...emptyProgress().stats, sessions: n } };
      expect(tipFor(p, today).show).toBe(false);
    }
  });
  it('need something already learned, and then add a small bonus', () => {
    const empty = emptyProgress();
    expect(tipQuestion(empty, qs, today)).toBeNull();
    const known = withCards(empty, [[q('k-1/1'), { interval: 14, due: addDays(today, 3) }], [q('s-1/2'), { interval: 30, due: addDays(today, 3) }], [q('k-1/2'), { interval: 2 }]]);
    const picked = tipQuestion(known, qs, today)!;
    expect(['k-1/1', 's-1/2']).toContain(picked.id);
    expect(tipQuestion(known, qs, today)).toBe(picked);
    // find a session count where the dice show
    let n = 0;
    while (!tipFor({ ...known, stats: { ...known.stats, sessions: n } }, today).show) n++;
    const base: Progress = { ...known, stats: { ...known.stats, sessions: n } };
    const out = run(base, perfect(['k-1/3']));
    const tip = out.events.find((e) => e.type === 'tip') as Extract<SessionEvent, { type: 'tip' }>;
    expect(tip.bonusXp).toBe(TIP_BONUS_XP);
    expect(out.progress.xp).toBe(base.xp + 20 + TIP_BONUS_XP);
  });
  it('explanationOf falls back to the model answer for open questions', () => {
    expect(explanationOf(mkQuestion('x/1'))).toBe('Because x/1');
    expect(explanationOf({ type: 'open', modelAnswer: 'model' } as never)).toBe('model');
  });
});

describe('pickCelebration', () => {
  const E: Record<string, SessionEvent> = {
    rank: { type: 'rank-up', title: 'Senior' },
    streak: { type: 'streak', value: 7, milestone: true },
    tick: { type: 'streak', value: 4, milestone: false },
    unit: { type: 'unit-mastered', unitId: 'k', title: 'K' },
    goal: { type: 'goal-met', first: false },
    perfect: { type: 'perfect' },
    ready: { type: 'readiness', label: 'building' },
    tip: { type: 'tip', questionId: 'a', bonusXp: 5 },
  };
  it('chooses at most one event by priority', () => {
    expect(pickCelebration([E.tip!, E.goal!, E.rank!, E.streak!])).toBe(E.rank);
    expect(pickCelebration([E.tip!, E.perfect!, E.goal!, E.unit!, E.streak!])).toBe(E.streak);
    expect(pickCelebration([E.tip!, E.perfect!, E.goal!, E.unit!])).toBe(E.unit);
    expect(pickCelebration([E.tip!, E.ready!, E.perfect!, E.goal!])).toBe(E.goal);
    expect(pickCelebration([E.tip!, E.ready!, E.perfect!])).toBe(E.perfect);
    expect(pickCelebration([E.tip!, E.ready!])).toBe(E.ready);
    expect(pickCelebration([E.tip!])).toBe(E.tip);
  });
  it('ignores plain streak ticks and freeze bookkeeping; empty gives null', () => {
    expect(pickCelebration([])).toBeNull();
    expect(pickCelebration([E.tick!, { type: 'freeze-used', count: 1 }, { type: 'freeze-earned' }, { type: 'freeze-gift' }])).toBeNull();
  });
  it('only milestones get the big treatment', () => {
    expect(isLargeCelebration(null)).toBe(false);
    expect(isLargeCelebration(E.rank!)).toBe(true);
    expect(isLargeCelebration(E.perfect!)).toBe(true);
    expect(isLargeCelebration({ type: 'goal-met', first: true })).toBe(true);
    expect(isLargeCelebration(E.goal!)).toBe(false);
    expect(isLargeCelebration(E.tip!)).toBe(false);
    expect(isLargeCelebration(E.ready!)).toBe(false);
  });
});

describe('invariants over 500 random sessions', () => {
  it('xp is monotonic; freezes, streaks, intervals, ease and due dates stay in range', () => {
    const rand = mulberry32(2026);
    const big = mkCourse({ a: { 'a-1': 12 }, b: { 'b-1': 12 }, c: { 'c-1': 12 } });
    const all = allQuestions(big);
    let p = emptyProgress();
    let day = '2026-01-01';
    for (let i = 0; i < 500; i++) {
      const gap = rand() < 0.5 ? 0 : rand() < 0.7 ? 1 : Math.floor(rand() * 6);
      day = addDays(day, gap);
      if (rand() < 0.02) day = addDays(day, -Math.floor(rand() * 4)); // the clock sometimes goes backwards
      const picks = all.filter(() => rand() < 0.3).slice(0, 8);
      if (picks.length === 0) continue;
      const missed = picks.filter(() => rand() < 0.3).map((x) => x.id);
      const recovered = missed.filter(() => rand() < 0.5);
      const cleared = picks.filter((x) => !missed.includes(x.id) || recovered.includes(x.id)).map((x) => x.id);
      const r: LessonResult = { accuracy: 1 - missed.length / picks.length, xp: picks.length * 10, missed, recovered, cleared };
      const before = p;
      p = applySession(p, r, { today: day, course: big }).progress;
      expect(p.xp).toBeGreaterThanOrEqual(before.xp);
      expect(p.freezes).toBeGreaterThanOrEqual(0);
      expect(p.freezes).toBeLessThanOrEqual(MAX_FREEZES);
      expect(p.longestStreak).toBeGreaterThanOrEqual(p.streak);
      expect(p.streak).toBeGreaterThanOrEqual(1);
      for (const c of Object.values(p.cards)) {
        expect(c.interval).toBeLessThanOrEqual(MAX_INTERVAL);
        expect(c.interval).toBeGreaterThanOrEqual(1);
        expect(c.ease).toBeGreaterThanOrEqual(1.3);
        expect(c.ease).toBeLessThanOrEqual(2.8);
        expect(c.due >= c.last).toBe(true);
      }
    }
    expect(Object.keys(p.cards).length).toBeGreaterThan(20);
  });
});

describe('fixtures', () => {
  it('fresh() builds a progress with overrides', () => {
    expect(fresh({ xp: 5 }).xp).toBe(5);
  });
});
