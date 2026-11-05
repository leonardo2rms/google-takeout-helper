import { describe, expect, it } from 'vitest';
import {
  addDays, applyToCards, cardFor, dueQuestions, gradeFor, intervalCap, isDue, MAX_INTERVAL, outcomesOf, previewInterval, questionHash,
  retention, reviewForecast, reviewQueue, scheduleCard, strength, unitByLesson, emptyProgress, type Card, type Grade, type Progress,
} from './index';
import { card, mkCourse, mkQuestion, withCards } from './fixtures';

const today = '2026-11-04';
const q = mkQuestion('a/1');
const hash = questionHash(q);
const sched = (c: Card | undefined, g: Grade, deadline?: string | null) => scheduleCard(c, g, today, { hash, deadline });

describe('outcomesOf / gradeFor', () => {
  it('splits a session into first-try, retry and failed', () => {
    const o = outcomesOf({ correct: ['a/1', 'a/2', 'a/3'], missed: ['a/2', 'a/4'], recovered: ['a/2'] });
    expect(o).toEqual({ 'a/1': 'first', 'a/3': 'first', 'a/2': 'retry', 'a/4': 'failed' });
  });
  it('maps outcome and confidence to a grade', () => {
    expect(gradeFor('retry')).toBe('again');
    expect(gradeFor('failed', 3)).toBe('again');
    expect(gradeFor('first')).toBe('good');
    expect(gradeFor('first', 2)).toBe('good');
    expect(gradeFor('first', 3)).toBe('easy');
    expect(gradeFor('first', 1)).toBe('hard');
  });
});

describe('scheduleCard', () => {
  it('good: 1 day, then 3 days, then interval x ease', () => {
    const c1 = sched(undefined, 'good');
    expect(c1).toMatchObject({ interval: 1, due: '2026-11-05', reps: 1, seen: 1, firstTry: 1, lapses: 0, ease: 2.3, h: hash });
    const c2 = scheduleCard(c1, 'good', '2026-11-05', { hash });
    expect(c2).toMatchObject({ interval: 3, reps: 2, due: '2026-11-08' });
    const c3 = scheduleCard(c2, 'good', '2026-11-08', { hash });
    expect(c3).toMatchObject({ interval: 7, reps: 3, due: '2026-11-15' }); // round(3 * 2.3)
    const c4 = scheduleCard(c3, 'good', '2026-11-15', { hash });
    expect(c4.interval).toBe(16);
  });

  it('again: tomorrow, ease -0.2, reps reset, lapse counted, even for a brand new card', () => {
    const c = card(q, { ease: 2.3, interval: 10, due: today, reps: 4 });
    expect(sched(c, 'again')).toMatchObject({ interval: 1, due: '2026-11-05', ease: 2.1, reps: 0, lapses: 1, firstTry: 1, seen: 2 });
    expect(sched(undefined, 'again')).toMatchObject({ interval: 1, due: '2026-11-05', lapses: 1, firstTry: 0, seen: 1 });
  });

  it('hard and easy adjust interval and ease', () => {
    const c = card(q, { ease: 2.3, interval: 10, due: today, reps: 3 });
    expect(sched(c, 'hard')).toMatchObject({ interval: 12, ease: 2.15, reps: 4 });
    expect(sched(c, 'easy')).toMatchObject({ interval: 30, ease: 2.4 }); // good = 23, x1.3 = 30
    expect(sched(undefined, 'hard').interval).toBe(1);
    expect(sched(undefined, 'easy').interval).toBe(2);
  });

  it('early reviews do not inflate the interval; failures still apply', () => {
    const c = card(q, { interval: 3, due: addDays(today, 2), reps: 1, seen: 1, last: addDays(today, -1) });
    const again = sched(c, 'good');
    expect(again).toMatchObject({ interval: 3, due: c.due, reps: 1, seen: 2, last: today });
    expect(sched(c, 'easy').interval).toBe(3);
    expect(sched(c, 'again')).toMatchObject({ interval: 1, due: addDays(today, 1), lapses: 1 });
    // replaying ten times changes nothing but the counters
    let x = c;
    for (let i = 0; i < 10; i++) x = sched(x, 'good');
    expect(x).toMatchObject({ interval: 3, due: c.due, reps: 1, seen: 11 });
  });

  it('ease stays within 1.3 .. 2.8 and the interval within 1 .. 120', () => {
    let c: Card | undefined;
    for (let i = 0; i < 30; i++) c = sched(c, 'again');
    expect(c!.ease).toBe(1.3);
    let e: Card = card(q, { ease: 2.8, interval: 100, reps: 5, due: today });
    for (let i = 0; i < 5; i++) e = scheduleCard(e, 'easy', e.due, { hash });
    expect(e.ease).toBe(2.8);
    expect(e.interval).toBe(MAX_INTERVAL);
  });

  it('caps intervals at half the time left before the interview, minimum 1 day', () => {
    const c = card(q, { interval: 20, due: today, reps: 3 });
    expect(sched(c, 'good', addDays(today, 10)).interval).toBe(5);
    expect(sched(c, 'good', addDays(today, 1)).interval).toBe(1);
    expect(sched(c, 'good', today).interval).toBe(1);
    expect(sched(c, 'good', addDays(today, -3)).interval).toBe(46); // date passed: ignored
    expect(intervalCap(today, null)).toBe(MAX_INTERVAL);
    expect(intervalCap(today, addDays(today, 1000))).toBe(MAX_INTERVAL);
  });

  it('never goes negative when the clock moved backwards', () => {
    const c = card(q, { interval: 3, due: '2026-11-08', last: '2026-11-10', reps: 2 });
    const r = sched(c, 'again');
    expect(r.last).toBe('2026-11-10');
    expect(r.due).toBe('2026-11-11');
    expect(sched(c, 'good').due >= '2026-11-10').toBe(true);
  });

  it('starts over when the question behind the card changed', () => {
    const c = card(q, { h: 'stale', interval: 50, reps: 9, lapses: 3 });
    expect(sched(c, 'good')).toMatchObject({ interval: 1, reps: 1, lapses: 0, seen: 1, h: hash });
  });

  it('is deterministic', () => {
    const c = card(q, { interval: 7, due: today, reps: 3 });
    expect(sched(c, 'good')).toEqual(sched(c, 'good'));
  });
});

describe('strength and retention', () => {
  const c = (over: Partial<Card>) => card(q, over);
  it('unseen is 0, and a card is fully strong at 21 days', () => {
    expect(strength(undefined, today)).toBe(0);
    expect(strength(c({ interval: 0 }), today)).toBe(0);
    expect(strength(c({ interval: 21, due: addDays(today, 5) }), today)).toBe(1);
    expect(strength(c({ interval: 42, due: addDays(today, 5) }), today)).toBe(1);
    expect(strength(c({ interval: 7, due: addDays(today, 5) }), today)).toBeCloseTo(7 / 21);
  });
  it('retention halves per interval overdue and is floored at 0.1', () => {
    expect(retention(c({ interval: 4, due: today }), today)).toBe(1);
    expect(retention(c({ interval: 4, due: addDays(today, -4) }), today)).toBeCloseTo(0.5);
    expect(retention(c({ interval: 4, due: addDays(today, -400) }), today)).toBe(0.1);
    expect(retention(c({ interval: 0, due: addDays(today, -1) }), today)).toBe(0.5);
  });
  it('isDue is due <= today', () => {
    expect(isDue(c({ due: today }), today)).toBe(true);
    expect(isDue(c({ due: addDays(today, 1) }), today)).toBe(false);
    expect(isDue(c({ due: addDays(today, -9) }), today)).toBe(true);
  });
});

describe('queues', () => {
  const course = mkCourse({ k: { 'k-1': 4 }, s: { 's-1': 4 }, j: { 'j-1': 2 } });
  const all = course.units.flatMap((u) => u.lessons.flatMap((l) => l.questions));
  const byId = (id: string) => all.find((x) => x.id === id)!;
  const unit = (x: { id: string }) => x.id.split('-')[0]!;

  it('orders due cards by how overdue they are and spreads units', () => {
    const p = withCards(emptyProgress(), [
      [byId('k-1/1'), { interval: 2, due: addDays(today, -10) }], // ratio 5
      [byId('k-1/2'), { interval: 2, due: addDays(today, -8) }], // ratio 4
      [byId('s-1/1'), { interval: 10, due: addDays(today, -10) }], // ratio 1
      [byId('j-1/1'), { interval: 2, due: addDays(today, -2) }], // ratio 1
      [byId('j-1/2'), { interval: 2, due: addDays(today, 3) }], // not due
    ]);
    const due = dueQuestions(p, all, today);
    // k1 (ratio 5), k2 (4), then the two ties by earliest due date; round-robin over units
    expect(due.map((x) => x.id)).toEqual(['k-1/1', 's-1/1', 'j-1/1', 'k-1/2']);
    expect(unit({ id: due[0]!.id })).toBe('k');
    expect(due.map((x) => x.id)).not.toContain('j-1/2');
  });

  it('ignores cards of unknown questions and of questions whose prompt changed', () => {
    const stale = card(byId('k-1/1'), { h: 'different', due: addDays(today, -1) });
    const p: Progress = { ...emptyProgress(), cards: { 'k-1/1': stale, 'gone/1': card(byId('k-1/2'), { due: addDays(today, -1) }) } };
    expect(dueQuestions(p, all, today)).toEqual([]);
    expect(cardFor(p, byId('k-1/1'))).toBeUndefined();
  });

  it('review queue = due followed by missed-not-due, de-duplicated', () => {
    let p = withCards(emptyProgress(), [
      [byId('k-1/1'), { due: addDays(today, -1) }],
      [byId('s-1/1'), { due: addDays(today, 4) }],
    ]);
    p = { ...p, missed: ['k-1/1', 's-1/1', 'j-1/2', 'nope/9'] };
    const rq = reviewQueue(p, all, today);
    expect(rq.due.map((x) => x.id)).toEqual(['k-1/1']);
    expect(rq.missedOnly.map((x) => x.id)).toEqual(['s-1/1', 'j-1/2']);
    expect(rq.all.map((x) => x.id)).toEqual(['k-1/1', 's-1/1', 'j-1/2']);
    expect(unitByLesson(byId('k-1/1'))).toBe('k-1');
  });

  it('applyToCards updates cards from outcomes and ignores unknown ids', () => {
    const before = withCards(emptyProgress(), [[byId('k-1/1'), { interval: 3, due: today, reps: 1 }]]);
    const cards = applyToCards(before.cards, { 'k-1/1': 'first', 's-1/1': 'failed', 'ghost/1': 'first' }, all, today, { deadline: null });
    expect(cards['k-1/1']).toMatchObject({ interval: 3, reps: 2 });
    expect(cards['s-1/1']).toMatchObject({ lapses: 1, interval: 1 });
    expect(cards['ghost/1']).toBeUndefined();
    expect(before.cards['k-1/1']!.reps).toBe(1); // input untouched
    const easy = applyToCards({}, { 'k-1/1': 'first' }, all, today, { confidence: { 'k-1/1': 3 } });
    expect(easy['k-1/1']!.ease).toBe(2.4);
  });

  it('previewInterval says when a question would come back', () => {
    const p = emptyProgress();
    expect(previewInterval(p, byId('k-1/1'), 'first', today)).toBe(1);
    expect(previewInterval(p, byId('k-1/1'), 'retry', today)).toBe(1);
    const learnt = withCards(p, [[byId('k-1/1'), { interval: 7, due: today, reps: 3 }]]);
    expect(previewInterval(learnt, byId('k-1/1'), 'first', today)).toBe(16);
  });

  it('reviewForecast groups upcoming reviews by days', () => {
    const cards = {
      a: card(q, { due: addDays(today, 1) }),
      b: card(q, { due: addDays(today, 1) }),
      c: card(q, { due: addDays(today, 3) }),
      d: card(q, { due: today }),
    };
    expect(reviewForecast(cards, ['a', 'b', 'c', 'd', 'missing'], today)).toEqual([{ inDays: 1, count: 2 }, { inDays: 3, count: 1 }]);
    expect(reviewForecast({}, [], today)).toEqual([]);
  });
});
