/**
 * Property-based tests (fast-check) for the progress data: normalisation and migration must be total, idempotent and
 * lossless for valid data; merging must never lose anything and must not depend on argument order where it claims not to.
 */
import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { exportProgress, importProgress, mergeProgress } from './backup';
import { daysBetween } from './day';
import { migrateProgress, normalizeProgress, sameJson } from './migrate';
import { emptyProgress, type Progress } from './progress';
import { CTX, TODAY, progressArb, rawProgressArb, recentDayArb } from './arbitraries';
import { mkCourse } from './fixtures';
import { allQuestions } from './schema';

const RUNS = { numRuns: 150 };

/** Everything the merge promises to treat symmetrically (cards and streak ties are resolved in favour of "local"). */
function symmetric(p: Progress) {
  return {
    xp: p.xp,
    longestStreak: p.longestStreak,
    lastDay: p.lastDay,
    stats: p.stats,
    lessons: p.lessons,
    days: p.days,
    milestones: p.milestones,
    missed: [...p.missed].sort(),
    passedByType: p.passedByType,
    calibration: p.calibration,
    achievements: p.achievements,
    cardIds: Object.keys(p.cards).sort(),
    challengeDays: Object.keys(p.challenge).sort(),
  };
}

describe('normalizeProgress', () => {
  it('never throws, whatever it is given', () => {
    fc.assert(fc.property(fc.anything(), (x) => void normalizeProgress(x, CTX)), RUNS);
    fc.assert(fc.property(rawProgressArb, (x) => void normalizeProgress(x, CTX)), RUNS);
  });

  it('is idempotent', () => {
    fc.assert(
      fc.property(rawProgressArb, (raw) => {
        const once = normalizeProgress(raw, CTX);
        expect(normalizeProgress(once, CTX)).toEqual(once);
      }),
      RUNS,
    );
  });

  it('always produces a self-consistent profile', () => {
    fc.assert(
      fc.property(progressArb, (p) => {
        expect(p.version).toBe(2);
        expect(p.xp).toBeGreaterThanOrEqual(0);
        expect(p.longestStreak).toBeGreaterThanOrEqual(p.streak);
        expect(p.freezes).toBeGreaterThanOrEqual(0);
        expect(p.freezes).toBeLessThanOrEqual(2);
        expect(p.profile.restDays.length).toBeLessThanOrEqual(3);
        expect(p.goal.target).toBeGreaterThanOrEqual(1);
        expect(p.goal.target).toBeLessThanOrEqual(100);
        for (const c of Object.values(p.cards)) {
          expect(c.ease).toBeGreaterThanOrEqual(1.3);
          expect(c.ease).toBeLessThanOrEqual(2.8);
          expect(Number.isInteger(c.interval) && c.interval >= 0 && c.interval <= 120).toBe(true);
          expect(daysBetween(TODAY, c.due)).toBeLessThanOrEqual(120);
        }
        for (const [id, r] of Object.entries(p.calibration.right)) expect(r).toBeLessThanOrEqual(p.calibration.n[Number(id)]!);
        // The result survives a JSON round trip unchanged (no undefined, NaN or prototype tricks).
        expect(JSON.parse(JSON.stringify(p))).toEqual(p);
      }),
      RUNS,
    );
  });

  it('keeps valid data exactly as it is (lossless for valid input)', () => {
    fc.assert(
      fc.property(progressArb, (p) => {
        const res = migrateProgress(p, CTX);
        expect(res.problem).toBeUndefined();
        expect(res.from).toBe(2);
        expect(res.changed).toBe(false);
        expect(res.progress).toEqual(p);
      }),
      RUNS,
    );
  });

  it('is not fooled by __proto__ / constructor keys and does not pollute Object.prototype', () => {
    const evil = JSON.parse(
      '{"version":2,"xp":5,"cards":{"__proto__":{"h":"x","due":"2026-06-16"},"constructor":{"h":"y","due":"2026-06-16"}},' +
        '"lessons":{"__proto__":{"bestAccuracy":1}},"milestones":{"__proto__":"2026-06-15"},"bookmarks":{"__proto__":{"h":"z"}},' +
        '"notes":{"__proto__":{"h":"z","text":"t"}},"achievements":{"__proto__":"2026-06-15"},"passedByType":{"__proto__":3}}',
    );
    const p = normalizeProgress(evil, CTX);
    expect(p.xp).toBe(5);
    expect(({} as Record<string, unknown>).h).toBeUndefined();
    expect(({} as Record<string, unknown>).bestAccuracy).toBeUndefined();
    expect(Object.getPrototypeOf(p.cards)).toBe(Object.prototype);
    expect(normalizeProgress(p, CTX)).toEqual(p);
  });
});

describe('migrateProgress', () => {
  it('is total: any value, any string, any JSON text gives a result and never throws', () => {
    fc.assert(
      fc.property(fc.oneof(fc.anything(), fc.string(), fc.json(), fc.jsonValue().map((v) => JSON.stringify(v))), (x) => {
        const res = migrateProgress(x, CTX);
        expect([0, 1, 2]).toContain(res.from);
        expect(res.progress.version).toBe(2);
      }),
      { numRuns: 300 },
    );
  });

  it('flags a problem (and returns a safe empty profile) for anything that is not a v1/v2 object', () => {
    fc.assert(
      fc.property(fc.oneof(fc.string({ minLength: 1 }), fc.integer(), fc.boolean(), fc.array(fc.anything())), (x) => {
        const res = migrateProgress(x, CTX);
        if (typeof x === 'string') {
          let parsed: unknown;
          try {
            parsed = JSON.parse(x);
          } catch {
            expect(res.problem).toBe('corrupt');
            expect(res.progress).toEqual(emptyProgress());
            return;
          }
          if (parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed)) return; // an object in a string: covered elsewhere
          if (parsed === null) return;
        }
        expect(res.problem).toBe('corrupt');
        expect(res.progress).toEqual(emptyProgress());
      }),
      RUNS,
    );
  });

  it('versions newer than 2 are reported, never interpreted', () => {
    fc.assert(
      fc.property(fc.integer({ min: 3, max: 1000 }), fc.dictionary(fc.string(), fc.anything()), (version, rest) => {
        const res = migrateProgress({ ...rest, version }, CTX);
        expect(res.problem).toBe('future-version');
        expect(res.changed).toBe(false);
      }),
      RUNS,
    );
  });

  const v1Arb = fc.record({
    version: fc.constant(1),
    xp: fc.nat(100_000),
    streak: fc.nat(400),
    lastDay: fc.oneof(recentDayArb, fc.constant(null)),
    lessons: fc.dictionary(
      fc.stringMatching(/^[a-z][a-z-]{0,12}$/).filter((k) => k !== 'constructor'),
      fc.record({ bestAccuracy: fc.double({ min: 0, max: 1, noNaN: true }), completions: fc.integer({ min: 1, max: 20 }), lastDay: recentDayArb }),
      { maxKeys: 6 },
    ),
    missed: fc.uniqueArray(fc.stringMatching(/^[a-z]{1,6}\/[0-9]{1,2}$/), { maxLength: 8 }),
  });
  const questions = allQuestions(mkCourse({ u: { a: 4, b: 4, c: 4 } }));
  const qctx = { today: TODAY, questions };

  it('v1 -> v2 keeps every v1 field, and migrating the result again changes nothing (idempotent)', () => {
    fc.assert(
      fc.property(v1Arb, (v1) => {
        const frozen = JSON.stringify(v1);
        const first = migrateProgress(v1, qctx);
        expect(JSON.stringify(v1)).toBe(frozen); // input untouched
        expect(first.from).toBe(1);
        const p = first.progress;
        expect(p.xp).toBe(v1.xp);
        expect(p.streak).toBe(v1.streak);
        expect(p.lastDay).toBe(v1.lastDay);
        expect(p.missed).toEqual(v1.missed);
        for (const [id, rec] of Object.entries(v1.lessons)) expect(p.lessons[id]).toEqual(rec);
        expect(p.longestStreak).toBe(v1.streak);
        const second = migrateProgress(JSON.stringify(p), qctx);
        expect(second.changed).toBe(false);
        expect(second.progress).toEqual(p);
      }),
      RUNS,
    );
  });

  it('v1 -> v2 never celebrates retroactively and never hands out more than the one welcome freeze', () => {
    fc.assert(
      fc.property(v1Arb, (v1) => {
        const p = migrateProgress(v1, qctx).progress;
        expect(p.freezes).toBeLessThanOrEqual(1);
        expect(p.milestones['rank:0']).toBeDefined();
      }),
      RUNS,
    );
  });
});

describe('mergeProgress', () => {
  it('merging with itself changes nothing', () => {
    fc.assert(
      fc.property(progressArb, (a) => {
        expect(mergeProgress(a, a, CTX)).toEqual(a);
      }),
      RUNS,
    );
  });

  it('is commutative for everything it promises to treat symmetrically', () => {
    fc.assert(
      fc.property(progressArb, progressArb, (a, b) => {
        expect(symmetric(mergeProgress(a, b, CTX))).toEqual(symmetric(mergeProgress(b, a, CTX)));
      }),
      RUNS,
    );
  });

  it('is associative for the symmetric fields', () => {
    fc.assert(
      fc.property(progressArb, progressArb, progressArb, (a, b, c) => {
        const left = mergeProgress(mergeProgress(a, b, CTX), c, CTX);
        const right = mergeProgress(a, mergeProgress(b, c, CTX), CTX);
        expect(symmetric(left)).toEqual(symmetric(right));
      }),
      RUNS,
    );
  });

  it('never loses anything: counters, lessons, missed ids, cards, days and badges only grow', () => {
    fc.assert(
      fc.property(progressArb, progressArb, (a, b) => {
        const m = mergeProgress(a, b, CTX);
        expect(m.xp).toBeGreaterThanOrEqual(Math.max(a.xp, b.xp));
        expect(m.longestStreak).toBeGreaterThanOrEqual(Math.max(a.longestStreak, b.longestStreak));
        for (const src of [a, b]) {
          for (const id of Object.keys(src.lessons)) expect(m.lessons[id]).toBeDefined();
          for (const id of src.missed) expect(m.missed).toContain(id);
          for (const id of Object.keys(src.cards)) expect(m.cards[id]).toBeDefined();
          for (const d of Object.keys(src.days)) expect(m.days[d]).toBeDefined();
          for (const id of Object.keys(src.achievements)) expect(m.achievements[id]).toBeDefined();
          for (const id of Object.keys(src.bookmarks)) expect(m.bookmarks[id]).toBeDefined();
        }
      }),
      RUNS,
    );
  });

  it('the result is itself valid (normalising it changes nothing)', () => {
    fc.assert(
      fc.property(progressArb, progressArb, (a, b) => {
        const m = mergeProgress(a, b, CTX);
        expect(sameJson(normalizeProgress(m, CTX), m)).toBe(true);
      }),
      RUNS,
    );
  });

  it('keeps the local device preferences and goal', () => {
    fc.assert(
      fc.property(progressArb, progressArb, (a, b) => {
        const m = mergeProgress(a, b, CTX);
        expect(m.settings).toEqual(a.settings);
        expect(m.prefs).toEqual(a.prefs);
        expect(m.goal).toEqual(a.goal);
      }),
      RUNS,
    );
  });
});

describe('backup export / import', () => {
  it('round-trips any valid progress exactly', () => {
    fc.assert(
      fc.property(progressArb, fc.date({ min: new Date('2020-01-01'), max: new Date('2030-01-01'), noInvalidDate: true }), (p, now) => {
        const res = importProgress(exportProgress(p, now), CTX);
        expect(res.ok).toBe(true);
        if (res.ok) {
          expect(res.progress).toEqual(p);
          expect(res.exportedAt).toBe(now.toISOString());
        }
      }),
      RUNS,
    );
  });

  it('never throws on arbitrary text, and anything it accepts is valid progress', () => {
    fc.assert(
      fc.property(fc.oneof(fc.string(), fc.json(), fc.jsonValue().map((v) => JSON.stringify(v))), (text) => {
        const res = importProgress(text, CTX);
        if (res.ok) expect(sameJson(normalizeProgress(res.progress, CTX), res.progress)).toBe(true);
        else expect(res.error.length).toBeGreaterThan(0);
      }),
      { numRuns: 300 },
    );
  });

  it('a backup edited anywhere inside the progress is rejected by the checksum (or still equals the original)', () => {
    fc.assert(
      fc.property(progressArb, fc.nat(), fc.constantFrom('0', '1', '7', 'x'), (p, at, ch) => {
        const text = exportProgress(p, new Date(0));
        const start = text.indexOf('"progress"');
        const i = start + (at % (text.length - start));
        if (text[i] === ch) return;
        const res = importProgress(text.slice(0, i) + ch + text.slice(i + 1), CTX);
        if (res.ok) expect(res.progress).toEqual(p); // e.g. whitespace changed: same data
      }),
      RUNS,
    );
  });
});

describe('regression: keys that exist on Object.prototype', () => {
  const rec = { bestAccuracy: 0.5, completions: 3, lastDay: '2026-06-15' };

  it('merging lessons / passedByType / achievements keyed like inherited properties keeps the real values', () => {
    const a = normalizeProgress({ version: 2, lessons: { toString: rec }, passedByType: { constructor: 4 }, achievements: { valueOf: '2026-06-10' } }, CTX);
    const b = normalizeProgress({ version: 2, lessons: { toString: { ...rec, completions: 7 } }, passedByType: { constructor: 2 }, achievements: { valueOf: '2026-06-12' } }, CTX);
    const m = mergeProgress(a, b, CTX);
    expect(m.lessons['toString']).toEqual({ ...rec, completions: 7 });
    expect(m.passedByType['constructor']).toBe(4);
    expect(m.achievements['valueOf']).toBe('2026-06-10');
  });

  it('a __proto__ key in stored data is dropped instead of replacing the prototype of its container', () => {
    const p = normalizeProgress(JSON.parse('{"version":2,"cards":{"__proto__":{"h":"x","due":"2026-06-16"}},"lessons":{"__proto__":{"completions":9}}}'), CTX);
    expect(Object.keys(p.cards)).toEqual([]);
    expect(Object.getPrototypeOf(p.lessons)).toBe(Object.prototype);
    expect(Object.keys(p.lessons)).toEqual([]);
  });
});
