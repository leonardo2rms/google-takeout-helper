/**
 * The rule against farming (S2-11): a question already answered today pays reduced XP and does not count for the daily
 * goal again; the perfect-lesson bonus scales with how much of the session was new. Covered here from the XP of one
 * answer up to replaying one question many times through the real session and credit path.
 */
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { applySession } from './events';
import { alreadyDoneToday, goalToday } from './goal';
import { questionHash } from './hash';
import { loadCourse } from './load-content';
import { emptyProgress, type Progress } from './progress';
import { allQuestions, type Question } from './schema';
import {
  HINT_COST, XP_FIRST_TRY, XP_MIN, XP_PERFECT_BONUS, XP_REPEAT, XP_RETRY, createSession, submit, summarize, xpFor, type Session,
} from './session';

const course = loadCourse(resolve(dirname(fileURLToPath(import.meta.url)), '../../content'));
const all = allQuestions(course);
const lesson = course.units[0]!.lessons[0]!;
const TODAY = '2026-11-05';
const TOMORROW = '2026-11-06';

/** The questions that are neither open nor code (answered with one tap in the real app), so a session is plain right/wrong. */
const [a, b, c, d] = lesson.questions.slice(0, 4) as [Question, Question, Question, Question];

describe('xpFor', () => {
  it('is unchanged for a question not answered today', () => {
    expect(xpFor(1, false)).toBe(XP_FIRST_TRY);
    expect(xpFor(2, false)).toBe(XP_RETRY);
    expect(xpFor(1, true)).toBe(XP_FIRST_TRY - HINT_COST);
    expect(xpFor(1, false, false)).toBe(XP_FIRST_TRY);
  });

  it('pays at most XP_REPEAT for a question already answered today, whichever attempt it is', () => {
    expect(XP_REPEAT).toBeLessThan(XP_RETRY);
    expect(xpFor(1, false, true)).toBe(XP_REPEAT);
    expect(xpFor(2, false, true)).toBe(XP_REPEAT);
    expect(xpFor(3, false, true)).toBe(XP_REPEAT);
  });

  it('a hint still costs, and never takes it below the minimum', () => {
    expect(xpFor(1, true, true)).toBe(Math.max(XP_MIN, XP_REPEAT - HINT_COST));
    expect(xpFor(1, true, true)).toBeGreaterThanOrEqual(XP_MIN);
  });
});

describe('a session that knows which questions were already done today', () => {
  it('keeps only the repeats that are in the session, and none by default', () => {
    expect(createSession([a, b]).repeats).toEqual([]);
    expect(createSession([a, b], { repeats: [b.id, 'not/in-session'] }).repeats).toEqual([b.id]);
    expect(createSession([a, b], { repeats: new Set([a.id, b.id]) }).repeats).toEqual([a.id, b.id]);
  });

  it('pays XP_REPEAT for a repeated question and the full reward for the others in the same session', () => {
    let s = createSession([a, b], { repeats: [a.id] });
    s = submit(s, true); // a: a repeat
    expect(s.xp).toBe(XP_REPEAT);
    s = submit(s, true); // b: new today
    expect(s.xp).toBe(XP_REPEAT + XP_FIRST_TRY);
  });

  it('a repeat answered right only on the retry still pays no more than XP_REPEAT', () => {
    let s = createSession([a], { repeats: [a.id] });
    s = submit(s, false);
    expect(s.xp).toBe(0);
    s = submit(s, true);
    expect(s.xp).toBe(XP_REPEAT);
  });

  it('a hint on a repeat takes it down to the minimum', () => {
    const s = submit(createSession([a], { repeats: [a.id] }), true, { hinted: true });
    expect(s.xp).toBe(XP_MIN);
  });
});

describe('the perfect-lesson bonus scales with what was new', () => {
  const perfect = (s: Session) => {
    let x = s;
    while (x.queue.length) x = submit(x, true);
    return summarize(x);
  };
  const qs = [a, b, c, d];

  it('is the full bonus when every question is new', () => {
    const sum = perfect(createSession(qs));
    expect(sum).toMatchObject({ perfect: true, bonus: XP_PERFECT_BONUS, repeats: 0, xp: 4 * XP_FIRST_TRY + XP_PERFECT_BONUS });
  });

  it('is proportional when some questions are repeats, and zero when all are', () => {
    expect(perfect(createSession(qs, { repeats: [a.id, b.id] }))).toMatchObject({ bonus: XP_PERFECT_BONUS / 2, repeats: 2 });
    expect(perfect(createSession(qs, { repeats: [a.id] })).bonus).toBe(Math.round(XP_PERFECT_BONUS * 0.75));
    const all4 = perfect(createSession(qs, { repeats: qs.map((q) => q.id) }));
    expect(all4).toMatchObject({ bonus: 0, repeats: 4, xp: 4 * XP_REPEAT });
  });

  it('is zero when the lesson was not perfect, and the summary xp always includes exactly the bonus', () => {
    let s = createSession(qs);
    s = submit(s, false);
    while (s.queue.length) s = submit(s, true);
    const sum = summarize(s);
    expect(sum.perfect).toBe(false);
    expect(sum.bonus).toBe(0);
    expect(sum.xp).toBe(s.xp);
    expect(perfect(createSession(qs)).xp).toBe(createSession(qs).xp + 4 * XP_FIRST_TRY + XP_PERFECT_BONUS);
  });

  it('an empty session has no bonus and does not divide by zero', () => {
    expect(summarize(createSession([]))).toMatchObject({ perfect: false, bonus: 0, repeats: 0 });
  });
});

describe('alreadyDoneToday', () => {
  const card = (q: Question, over: Partial<Progress['cards'][string]> = {}) => ({ h: questionHash(q), ease: 2.5, interval: 1, due: TOMORROW, reps: 1, lapses: 0, seen: 1, firstTry: 1, last: TODAY, ...over });
  const withCards = (cards: Progress['cards']): Progress => ({ ...emptyProgress(), cards });

  it('is a question whose card was updated today and still matches its wording', () => {
    const p = withCards({ [a.id]: card(a), [b.id]: card(b, { last: '2026-11-04' }), [c.id]: card(c, { h: 'older-wording' }) });
    expect(alreadyDoneToday(p, [a, b, c, d], TODAY)).toEqual([a.id]);
  });

  it('is empty for a new profile, and for another day', () => {
    expect(alreadyDoneToday(emptyProgress(), [a, b], TODAY)).toEqual([]);
    expect(alreadyDoneToday(withCards({ [a.id]: card(a) }), [a], TOMORROW)).toEqual([]);
  });
});

/** One session the way the app plays it: repeats from the profile, every answer right, XP minus the bonus for one-question sessions. */
function play(p: Progress, questions: Question[], today: string, kind: 'lesson' | 'single' = 'lesson'): Progress {
  let s = createSession(questions, { repeats: alreadyDoneToday(p, questions, today) });
  while (s.queue.length) s = submit(s, true);
  const sum = summarize(s);
  return applySession(
    p,
    { lessonId: kind === 'lesson' ? 'x' : undefined, kind: kind === 'single' ? 'single' : undefined, accuracy: sum.accuracy, xp: kind === 'single' ? sum.xp - sum.bonus : sum.xp, missed: sum.missed, recovered: sum.recovered, cleared: sum.correct },
    { today, course },
  ).progress;
}

describe('the daily goal counts distinct questions', () => {
  const four = [a, b, c, d];

  it('credits every new question once, and nothing for the same questions again the same day', () => {
    const once = play(emptyProgress(), four, TODAY);
    expect(once.days[TODAY]).toMatchObject({ answered: 4, firstTry: 4 });
    const twice = play(once, four, TODAY);
    expect(twice.days[TODAY]).toMatchObject({ answered: 4, firstTry: 4 }); // unchanged
    expect(goalToday(twice, TODAY).done).toBe(4);
    // ... but it is still practice: the lifetime counters and the cards move on
    expect(twice.stats.answered).toBe(8);
    expect(twice.stats.sessions).toBe(2);
    expect(twice.cards[a.id]!.seen).toBe(2);
  });

  it('credits only the new questions of a mixed session', () => {
    const first = play(emptyProgress(), [a, b], TODAY);
    const mixed = play(first, [a, b, c, d], TODAY);
    expect(mixed.days[TODAY]!.answered).toBe(4); // 2 + the 2 new ones
  });

  it('pays the XP it was given: reduced by the session, never re-derived here', () => {
    const once = play(emptyProgress(), four, TODAY);
    const twice = play(once, four, TODAY);
    expect(twice.xp - once.xp).toBe(4 * XP_REPEAT); // no perfect bonus for a lesson that was all repeats
    expect(twice.days[TODAY]!.xp).toBe(once.days[TODAY]!.xp + 4 * XP_REPEAT);
  });

  it('counts the same questions again on the next day', () => {
    const once = play(emptyProgress(), four, TODAY);
    const next = play(once, four, TOMORROW);
    expect(next.days[TOMORROW]).toMatchObject({ answered: 4 });
    expect(next.xp - once.xp).toBe(4 * XP_FIRST_TRY + XP_PERFECT_BONUS);
  });

  it('the day\'s first-try count only includes the questions that were credited', () => {
    const first = play(emptyProgress(), [a, b], TODAY); // a, b done today
    const outcomes = { [a.id]: 'first', [b.id]: 'first', [c.id]: 'retry', [d.id]: 'failed' } as const;
    const next = applySession(
      first,
      { lessonId: 'x', accuracy: 0.5, xp: 20, missed: [d.id], recovered: [c.id], cleared: [a.id, b.id, c.id], outcomes },
      { today: TODAY, course },
    ).progress;
    expect(next.days[TODAY]).toMatchObject({ answered: 4, firstTry: 2 }); // not 4: the two repeats' first tries are not new credit
    expect(next.stats.firstTry).toBe(2 + 2); // the lifetime counter still counts them
  });

  it('does not fire "goal met" a second time for credit that was not given', () => {
    const p = { ...emptyProgress(), goal: { target: 4 } };
    const first = applySession(p, { lessonId: 'x', accuracy: 1, xp: 40, missed: [], recovered: [], cleared: four.map((q) => q.id) }, { today: TODAY, course });
    expect(first.events.some((e) => e.type === 'goal-met')).toBe(true);
    const again = applySession(first.progress, { lessonId: 'x', accuracy: 1, xp: 8, missed: [], recovered: [], cleared: four.map((q) => q.id) }, { today: TODAY, course });
    expect(again.events.some((e) => e.type === 'goal-met')).toBe(false);
    expect(goalToday(again.progress, TODAY).done).toBe(4);
  });

  it('a mock interview keeps its own credit: questions done today still count for it', () => {
    const first = play(emptyProgress(), four, TODAY);
    const mock = applySession(
      first,
      { kind: 'mock', accuracy: 1, xp: 5, missed: [], recovered: [], cleared: four.map((q) => q.id), outcomes: Object.fromEntries(four.map((q) => [q.id, 'first' as const])), answered: 3 },
      { today: TODAY, course },
    ).progress;
    expect(mock.days[TODAY]!.answered).toBe(4 + 3);
  });
});

describe('replaying one question cannot farm the goal or XP (S2-11)', () => {
  it('8 repeats of a Library question: 10 + 7 x 2 XP and a goal credit of 1, not 80 XP and 8/8', () => {
    let p = emptyProgress();
    for (let i = 0; i < 8; i++) p = play(p, [a], TODAY, 'single');
    expect(p.xp).toBe(XP_FIRST_TRY + 7 * XP_REPEAT);
    expect(goalToday(p, TODAY)).toMatchObject({ done: 1, met: false });
  });

  it('for any number of repeats: goal credit stays 1 and XP grows by XP_REPEAT each', () => {
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 40 }), (n) => {
        let p = emptyProgress();
        for (let i = 0; i < n; i++) p = play(p, [a], TODAY, 'single');
        expect(goalToday(p, TODAY).done).toBe(1);
        expect(p.xp).toBe(XP_FIRST_TRY + (n - 1) * XP_REPEAT);
      }),
      { numRuns: 40 },
    );
  });

  it('replaying a whole lesson all day earns far less than doing new ones', () => {
    const lessonQs = lesson.questions.filter((q) => q.type !== 'open' && q.type !== 'code');
    let p = emptyProgress();
    for (let i = 0; i < 10; i++) p = play(p, lessonQs, TODAY);
    const firstRound = lessonQs.length * XP_FIRST_TRY + XP_PERFECT_BONUS;
    expect(p.xp).toBe(firstRound + 9 * lessonQs.length * XP_REPEAT);
    expect(goalToday(p, TODAY).done).toBe(lessonQs.length);
  });
});

describe('questions that exist in the course', () => {
  it('the four test questions are real and distinct', () => {
    expect(new Set([a, b, c, d].map((q) => q.id)).size).toBe(4);
    expect(all.length).toBeGreaterThan(100);
  });
});
