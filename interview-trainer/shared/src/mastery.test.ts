import { describe, expect, it } from 'vitest';
import {
  addDays, allQuestions, emptyProgress, labelFor, pickMix, planFor, readiness, ROLE_WEIGHTS, strength, topicStats, unitIndex, unitOfFor, weakSpots,
  type Progress,
} from './index';
import { card, mkCourse, withCards } from './fixtures';

const today = '2026-11-04';
const course = mkCourse({ k: { 'k-1': 4 }, s: { 's-1': 4 }, empty: {} });
const qs = allQuestions(course);
const q = (id: string) => qs.find((x) => x.id === id)!;
const known = (ids: string[], interval = 21) => withCards(emptyProgress(), ids.map((id) => [q(id), { interval, due: addDays(today, 3) }] as [ReturnType<typeof q>, object]));

describe('topicStats', () => {
  it('unseen questions count as 0 so the score cannot be inflated', () => {
    const t = topicStats(course, known(['k-1/1', 'k-1/2']), today);
    expect(t[0]).toMatchObject({ unitId: 'k', seen: 2, total: 4, strength: 0.5, mastered: 2, due: 0 });
    expect(t[1]).toMatchObject({ unitId: 's', seen: 0, strength: 0 });
    expect(t[2]).toMatchObject({ total: 0, strength: 0 });
  });
  it('counts due cards', () => {
    const p = withCards(emptyProgress(), [[q('k-1/1'), { due: today }], [q('k-1/2'), { due: addDays(today, 2) }]]);
    expect(topicStats(course, p, today)[0]!.due).toBe(1);
  });
  it('ignores a card whose question changed', () => {
    const p: Progress = { ...emptyProgress(), cards: { 'k-1/1': card(q('k-1/1'), { h: 'old' }) } };
    expect(topicStats(course, p, today)[0]!.seen).toBe(0);
  });
  it('unit index and unitOf map ids to units, falling back to the lesson id', () => {
    expect(unitIndex(course).get('s-1/2')).toBe('s');
    expect(unitIndex(course)).toBe(unitIndex(course));
    expect(unitOfFor(course)(q('s-1/2'))).toBe('s');
    expect(unitOfFor(course)({ id: 'zzz/1' } as never)).toBe('zzz');
  });
});

describe('readiness', () => {
  it('empty: score 0, early estimate, no weakest unit', () => {
    expect(readiness(course, emptyProgress(), today)).toEqual({ score: 0, coverage: 0, label: 'early', weakestUnitId: null });
  });
  it('is the mean unit strength x 100 with coverage next to it', () => {
    const r = readiness(course, known(['k-1/1', 'k-1/2', 'k-1/3', 'k-1/4']), today);
    expect(r).toMatchObject({ score: 50, coverage: 0.5, weakestUnitId: 'k' });
  });
  it('weights units by role and by explicit weights', () => {
    const p = known(['k-1/1', 'k-1/2', 'k-1/3', 'k-1/4']);
    expect(readiness(course, p, today, { k: 3, s: 1 }).score).toBe(75);
    expect(readiness(course, p, today, { k: 1, s: 3 }).score).toBe(25);
    expect(readiness(course, { ...p, profile: { ...p.profile, role: 'generalist' } }, today).score).toBe(50);
    expect(ROLE_WEIGHTS['staff-architect']).toMatchObject({ ddd: 1.5 });
  });
  it('weakest unit is the lowest strength among units that have been started', () => {
    const p = withCards(known(['k-1/1', 'k-1/2', 'k-1/3', 'k-1/4']), [[q('s-1/1'), { interval: 3, due: addDays(today, 1) }]]);
    expect(readiness(course, p, today).weakestUnitId).toBe('s');
  });
  it('label needs both coverage and score', () => {
    expect(labelFor(0.1, 90)).toBe('early');
    expect(labelFor(0.5, 90)).toBe('building');
    expect(labelFor(0.7, 90)).toBe('solid');
    expect(labelFor(0.95, 90)).toBe('ready');
    expect(labelFor(0.95, 10)).toBe('early'); // saw everything once: not "interview-ready"
    expect(labelFor(0.95, 30)).toBe('building');
    expect(labelFor(0.95, 50)).toBe('solid');
  });
  it('answering well never lowers strength on the same day', () => {
    const before = withCards(emptyProgress(), [[q('k-1/1'), { interval: 3, due: addDays(today, -1) }]]);
    const s0 = strength(before.cards['k-1/1'], today);
    const after = { ...before.cards['k-1/1']!, interval: 7, due: addDays(today, 7) };
    expect(strength(after, today)).toBeGreaterThan(s0);
  });
});

describe('weakSpots', () => {
  it('nothing seen -> nothing', () => {
    expect(weakSpots(course, emptyProgress(), today, 3)).toEqual([]);
  });
  it('picks the weakest seen questions of the weakest unit; missed and lapsed rank higher', () => {
    let p = withCards(emptyProgress(), [
      [q('k-1/1'), { interval: 21, due: addDays(today, 5) }],
      [q('k-1/2'), { interval: 21, due: addDays(today, 5) }],
      [q('s-1/1'), { interval: 7, due: addDays(today, 5) }],
      [q('s-1/2'), { interval: 7, due: addDays(today, 5), lapses: 3 }],
      [q('s-1/3'), { interval: 7, due: addDays(today, 5) }],
    ]);
    p = { ...p, missed: ['s-1/3'] };
    const out = weakSpots(course, p, today, 2, 1);
    expect(out.map((x) => x.id).sort()).toEqual(['s-1/2', 's-1/3']);
    expect(weakSpots(course, p, today, 10, 1).map((x) => x.id)).not.toContain('s-1/4'); // unseen is not a weak spot
  });
  it('spills over to the next weakest unit when the weakest has too few seen questions', () => {
    const p = withCards(emptyProgress(), [
      [q('k-1/1'), { interval: 21, due: addDays(today, 5) }],
      [q('s-1/1'), { interval: 3, due: addDays(today, 5) }],
    ]);
    expect(weakSpots(course, p, today, 5, 1).map((x) => x.id)).toEqual(['s-1/1', 'k-1/1']);
  });
  it('can be limited to one unit and is deterministic per seed', () => {
    const p = known(['k-1/1', 'k-1/2', 's-1/1']);
    expect(weakSpots(course, p, today, 5, 3, 'k').every((x) => x.id.startsWith('k'))).toBe(true);
    expect(weakSpots(course, p, today, 5, 3, 'k')).toEqual(weakSpots(course, p, today, 5, 3, 'k'));
    expect(weakSpots(course, p, today, 5, 3, 'missing-unit')).toEqual([]);
  });
});

describe('pickMix', () => {
  it('is deterministic, unique and the right size; falls back to uniform when nothing is seen', () => {
    const a = pickMix(course, emptyProgress(), today, 5, 7);
    expect(a).toEqual(pickMix(course, emptyProgress(), today, 5, 7));
    expect(new Set(a.map((x) => x.id)).size).toBe(5);
    expect(pickMix(course, emptyProgress(), today, 100, 7)).toHaveLength(8);
  });
  it('favours weak material over strong material across many seeded draws', () => {
    const strong = ['k-1/1', 'k-1/2', 'k-1/3', 'k-1/4'];
    const p = known(strong);
    let strongPicks = 0;
    let weakPicks = 0;
    for (let seed = 0; seed < 1000; seed++) {
      for (const x of pickMix(course, p, today, 2, seed)) {
        if (strong.includes(x.id)) strongPicks++;
        else weakPicks++;
      }
    }
    // weights: strong 1, unseen 3 -> unseen should be picked about 3x as often per question
    expect(weakPicks).toBeGreaterThan(strongPicks * 1.8);
    expect(strongPicks).toBeGreaterThan(0);
  });
  it('roughly uniform when nothing is seen', () => {
    const counts: Record<string, number> = {};
    for (let seed = 0; seed < 2000; seed++) for (const x of pickMix(course, emptyProgress(), today, 1, seed)) counts[x.id] = (counts[x.id] ?? 0) + 1;
    for (const n of Object.values(counts)) expect(Math.abs(n - 250)).toBeLessThan(90);
  });
});

describe('planFor', () => {
  const planned = (date: string | null, extra: Partial<Progress> = {}) => ({ ...emptyProgress(), profile: { ...emptyProgress().profile, interviewDate: date }, ...extra });
  const big = mkCourse({ a: { 'a-1': 60 }, b: { 'b-1': 40 } }); // 100 questions

  it('no date: no countdown at all', () => {
    expect(planFor(big, planned(null), today)).toMatchObject({ daysLeft: null, perDay: null, onTrack: null, mode: 'none', datePassed: false, remaining: 100 });
  });
  it('a date in the past is treated as unset, and flagged', () => {
    expect(planFor(big, planned(addDays(today, -1)), today)).toMatchObject({ daysLeft: null, mode: 'none', datePassed: true });
  });
  it('90 days out: easy', () => {
    const p = planFor(big, planned(addDays(today, 90)), today);
    expect(p).toMatchObject({ daysLeft: 90, perDay: 2, onTrack: true, mode: 'steady' });
    expect(p.spareDays).toBe(90 - 13);
  });
  it('10 days out: 100 questions over 8 working days = 13/day, not on track at the default 8', () => {
    const p = planFor(big, planned(addDays(today, 10)), today);
    expect(p).toMatchObject({ perDay: 13, onTrack: false, mode: 'steady' });
    expect(planFor(big, planned(addDays(today, 10), { goal: { target: 16 } }), today).onTrack).toBe(true);
    expect(p.spareDays).toBeLessThan(0);
  });
  it('today or tomorrow switches to final review', () => {
    expect(planFor(big, planned(today), today)).toMatchObject({ daysLeft: 0, mode: 'final-review', perDay: 100 });
    expect(planFor(big, planned(addDays(today, 1)), today)).toMatchObject({ daysLeft: 1, mode: 'final-review' });
    expect(planFor(big, planned(addDays(today, 2)), today).mode).toBe('steady');
  });
  it('remaining = unseen + due now; everything learned and not due means nothing remaining', () => {
    const all = allQuestions(big);
    const learned = withCards(emptyProgress(), all.map((x) => [x, { interval: 30, due: addDays(today, 10) }] as [typeof x, object]));
    const p = { ...learned, profile: { ...learned.profile, interviewDate: addDays(today, 10) } };
    expect(planFor(big, p, today)).toMatchObject({ remaining: 0, perDay: 0, onTrack: true });
    const dueNow = { ...p, cards: { ...p.cards, [all[0]!.id]: { ...p.cards[all[0]!.id]!, due: today } } };
    expect(planFor(big, dueNow, today).remaining).toBe(1);
  });
});
