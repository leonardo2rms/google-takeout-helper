import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import {
  FOCUS_WEIGHT, ROLE_FOCUS, ROLE_IDS, ROLE_WEIGHTS, addDays, allQuestions, buildDailySession, buildMock, emptyProgress, hasTrack, isFocusUnit, nextLessonFor, pickMix, readiness,
  roleWeights, trackOrder, unitOfFor, type Progress, type RoleId,
} from './index';
import { loadCourse } from './load-content';
import { mkCourse, withCards } from './fixtures';

const today = '2026-11-04';
const realCourse = () => loadCourse(resolve(dirname(fileURLToPath(import.meta.url)), '../../content'));
// Real unit ids so the role tracks apply: kotlin and data are on the backend track, ddd and scale on the architect track.
const course = mkCourse({ kotlin: { 'kt-1': 3, 'kt-2': 3 }, ddd: { 'dd-1': 5 }, data: { 'da-1': 5 }, scale: { 'sc-1': 5 } });
const withRole = (role: RoleId | null): Progress => ({ ...emptyProgress(), profile: { ...emptyProgress().profile, role } });
const ids = (units: readonly { id: string }[]) => units.map((u) => u.id);

describe('roles and tracks', () => {
  it('generalist and no role have no track and keep the course order', () => {
    expect(hasTrack(null)).toBe(false);
    expect(hasTrack('generalist')).toBe(false);
    expect(ids(trackOrder(course.units, null))).toEqual(['kotlin', 'ddd', 'data', 'scale']);
    expect(ids(trackOrder(course.units, 'generalist'))).toEqual(['kotlin', 'ddd', 'data', 'scale']);
    expect(roleWeights(null)).toEqual({});
  });
  it('a role puts its focus units first, each group in course order', () => {
    expect(ids(trackOrder(course.units, 'backend-kotlin'))).toEqual(['kotlin', 'data', 'ddd', 'scale']);
    expect(ids(trackOrder(course.units, 'staff-architect'))).toEqual(['ddd', 'scale', 'kotlin', 'data']);
  });
  it('never drops, duplicates or mutates', () => {
    const before = ids(course.units);
    for (const role of [null, ...ROLE_IDS]) expect(ids(trackOrder(course.units, role)).sort()).toEqual([...before].sort());
    expect(ids(course.units)).toEqual(before);
  });
  it('weights are derived from the track: focus units weigh FOCUS_WEIGHT, everything else 1', () => {
    expect(ROLE_WEIGHTS['backend-kotlin']).toMatchObject({ kotlin: FOCUS_WEIGHT, data: FOCUS_WEIGHT });
    expect(Object.keys(ROLE_WEIGHTS['staff-architect']).sort()).toEqual([...ROLE_FOCUS['staff-architect']].sort());
    expect(ROLE_WEIGHTS.generalist).toEqual({});
    expect(isFocusUnit('staff-architect', 'ddd')).toBe(true);
    expect(isFocusUnit('staff-architect', 'kotlin')).toBe(false);
    expect(isFocusUnit(null, 'kotlin')).toBe(false);
  });
  it('the two tracks do not share a unit, and every unit of the real course is on at most one', () => {
    const real = realCourse();
    const unitIds = new Set(ids(real.units));
    for (const role of ROLE_IDS) for (const id of ROLE_FOCUS[role]) expect(unitIds.has(id), `${role}: ${id}`).toBe(true);
    const both = ROLE_FOCUS['backend-kotlin'].filter((id) => ROLE_FOCUS['staff-architect'].includes(id));
    expect(both).toEqual([]);
    for (const role of ROLE_IDS.filter((r) => r !== 'generalist')) {
      expect(ROLE_FOCUS[role].length).toBeGreaterThan(0);
      expect(ROLE_FOCUS[role].length).toBeLessThan(real.units.length);
    }
  });
  it('property: a track is a permutation with the focus group first', () => {
    const real = realCourse();
    fc.assert(
      fc.property(fc.constantFrom<RoleId | null>(null, ...ROLE_IDS), (role) => {
        const out = trackOrder(real.units, role);
        expect(ids(out).sort()).toEqual(ids(real.units).sort());
        const flags = out.map((u) => isFocusUnit(role, u.id));
        expect(flags).toEqual([...flags].sort((a, b) => Number(b) - Number(a)));
      }),
    );
  });
});

describe('nextLessonFor', () => {
  it('follows the track and skips what is done (or the lesson just finished)', () => {
    expect(nextLessonFor(course, null, {})?.id).toBe('kt-1');
    expect(nextLessonFor(course, 'staff-architect', {})?.id).toBe('dd-1');
    expect(nextLessonFor(course, 'staff-architect', { 'dd-1': 1 })?.id).toBe('sc-1');
    expect(nextLessonFor(course, 'staff-architect', { 'dd-1': 1, 'sc-1': 1 })?.id).toBe('kt-1');
    expect(nextLessonFor(course, 'backend-kotlin', { 'kt-1': 1 }, 'kt-2')?.id).toBe('da-1');
  });
  it('is undefined when everything is done', () => {
    const done = Object.fromEntries(['kt-1', 'kt-2', 'dd-1', 'da-1', 'sc-1'].map((id) => [id, 1]));
    expect(nextLessonFor(course, 'backend-kotlin', done)).toBeUndefined();
  });
});

describe('Today follows the track', () => {
  const unitsOf = (qs: { id: string }[]) => qs.map((q) => unitOfFor(course)(q as never));
  it('draws new material from the focus units first', () => {
    const arch = buildDailySession(course, withRole('staff-architect'), today);
    expect(new Set(unitsOf(arch.questions))).toEqual(new Set(['ddd', 'scale']));
    const be = buildDailySession(course, withRole('backend-kotlin'), today);
    expect(new Set(unitsOf(be.questions))).toEqual(new Set(['kotlin', 'data']));
  });
  it('with no role it is the course order, as before', () => {
    const plain = buildDailySession(course, withRole(null), today);
    expect(new Set(unitsOf(plain.questions))).toEqual(new Set(['kotlin', 'ddd']));
  });
  it('spills past the track once it is all seen, and due cards still come first', () => {
    const all = allQuestions(course);
    const trackQs = all.filter((q) => ['ddd', 'scale'].includes(unitOfFor(course)(q)));
    const p = withCards(withRole('staff-architect'), trackQs.map((q) => [q, { due: addDays(today, 5), interval: 5 }] as [typeof q, object]));
    const day = buildDailySession(course, p, today);
    expect(day.mix.fresh).toBe(day.questions.length - day.mix.weak);
    expect(day.questions.every((q) => !['ddd', 'scale'].includes(unitOfFor(course)(q)) || day.mix.weak > 0)).toBe(true);
    const due = withCards(withRole('staff-architect'), [[trackQs[0]!, { due: addDays(today, -1), interval: 2 }]]);
    expect(buildDailySession(course, due, today).mix.due).toBe(1);
  });
});

describe('Mixed practice and mocks lean towards the track', () => {
  const real = realCourse();
  const share = (role: RoleId | null, pick: (p: Progress, seed: number) => { id: string }[]) => {
    let focus = 0;
    let total = 0;
    const unitOf = unitOfFor(real);
    for (let seed = 1; seed <= 60; seed++) {
      for (const q of pick(withRole(role), seed)) {
        total++;
        if (isFocusUnit('staff-architect', unitOf(q as never))) focus++;
      }
    }
    return focus / total;
  };
  it('mixed practice draws more from the focus units for that role than for a generalist', () => {
    const mix = (p: Progress, seed: number) => pickMix(real, p, today, 10, seed);
    expect(share('staff-architect', mix)).toBeGreaterThan(share('generalist', mix) + 0.03);
    expect(share('backend-kotlin', mix)).toBeLessThan(share('generalist', mix));
  });
  it('a mock does the same', () => {
    const mock = (p: Progress, seed: number) => buildMock(real, p, today, { length: 10, minutes: 15, unitId: null }, seed);
    expect(share('staff-architect', mock)).toBeGreaterThan(share('generalist', mock) + 0.03);
  });
  it('a topic-restricted mock stays inside that topic whatever the role', () => {
    const unitOf = unitOfFor(real);
    const qs = buildMock(real, withRole('staff-architect'), today, { length: 10, minutes: 15, unitId: 'kotlin' }, 3);
    expect(qs.length).toBe(10);
    expect(qs.every((q) => unitOf(q) === 'kotlin')).toBe(true);
  });
  it('is unchanged for no role and for generalist (same seed, same questions)', () => {
    const a = pickMix(real, withRole(null), today, 10, 5).map((q) => q.id);
    const b = pickMix(real, withRole('generalist'), today, 10, 5).map((q) => q.id);
    expect(a).toEqual(b);
  });
});

describe('readiness weights follow the same track', () => {
  it('a role weighs its focus units more', () => {
    const real = realCourse();
    const qs = allQuestions(real);
    const unitOf = unitOfFor(real);
    // Know everything in the architect track, nothing else.
    const known = qs.filter((q) => isFocusUnit('staff-architect', unitOf(q)));
    const base = withCards(emptyProgress(), known.map((q) => [q, { interval: 30, due: addDays(today, 10) }] as [typeof q, object]));
    const asArchitect = readiness(real, { ...base, profile: { ...base.profile, role: 'staff-architect' } }, today).score;
    const asBackend = readiness(real, { ...base, profile: { ...base.profile, role: 'backend-kotlin' } }, today).score;
    expect(asArchitect).toBeGreaterThan(asBackend);
  });
});
