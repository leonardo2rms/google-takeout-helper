/**
 * Property-based tests (fast-check) for the pure learning logic: day arithmetic, streaks, spaced repetition,
 * number parsing, the code-test harness and the deterministic selection of daily / mock / challenge questions.
 */
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import fc from 'fast-check';
import { afterAll, describe, expect, it } from 'vitest';
import { deepEqual, runCodeTests, show } from './code-runner';
import { challengeQuestion, CHALLENGE_WEIGHTS } from './challenge';
import { DAILY_MAX_DUE, DAILY_SIZE, buildDailySession, interleave } from './daily';
import { addDays, dayKey, dayNumber, daysBetween, fromDayNumber, isDay, weekday } from './day';
import { parseNumber, seededShuffle, shuffleDifferent } from './grade';
import { MAX_FREEZES, emptyProgress, type Card, type Progress } from './progress';
import { MAX_EASE, MIN_EASE, intervalCap, retention, scheduleCard, strength, type Grade } from './srs';
import { advanceStreak, liveStreak, streakStatus } from './streak';
import { dayArb } from './arbitraries';
import { loadCourse } from './load-content';
import { buildMock } from './mock';
import { allQuestions } from './schema';
import { card, withCards } from './fixtures';

const RUNS = { numRuns: 200 };
const here = dirname(fileURLToPath(import.meta.url));
const course = loadCourse(resolve(here, '../../content'));
const questions = allQuestions(course);

describe('day arithmetic', () => {
  it('addDays / daysBetween / dayNumber agree with each other', () => {
    fc.assert(
      fc.property(dayArb, fc.integer({ min: -4000, max: 4000 }), (d, n) => {
        const e = addDays(d, n);
        expect(isDay(e) || dayNumber(e) < dayNumber('1970-01-01') + 1).toBe(true);
        expect(daysBetween(d, e)).toBe(n);
        expect(daysBetween(e, d) === -n).toBe(true); // (=== so that -0 equals 0)
        expect(addDays(e, -n)).toBe(d);
        expect(fromDayNumber(dayNumber(d))).toBe(d);
        expect(addDays(addDays(d, 1), n)).toBe(addDays(d, n + 1));
      }),
      RUNS,
    );
  });

  it('weekday cycles every 7 days and advances by one per day', () => {
    fc.assert(
      fc.property(dayArb, (d) => {
        expect(weekday(addDays(d, 7))).toBe(weekday(d));
        expect(weekday(addDays(d, 1))).toBe((weekday(d) + 1) % 7);
        expect(weekday(d)).toBeGreaterThanOrEqual(0);
        expect(weekday(d)).toBeLessThanOrEqual(6);
      }),
      RUNS,
    );
  });

  it('day keys sort like time: later instants never get an earlier key (DST and odd offsets included)', () => {
    const zones = ['UTC', 'America/New_York', 'Europe/London', 'Australia/Lord_Howe', 'Asia/Kolkata', 'Pacific/Auckland', 'America/St_Johns'];
    const before = process.env.TZ;
    afterAll(() => {
      if (before === undefined) delete process.env.TZ;
      else process.env.TZ = before;
    });
    fc.assert(
      fc.property(
        fc.constantFrom(...zones),
        fc.integer({ min: Date.UTC(2024, 0, 1), max: Date.UTC(2030, 0, 1) }),
        fc.integer({ min: 0, max: 3 * 86_400_000 }),
        (tz, t, dt) => {
          process.env.TZ = tz;
          const a = dayKey(new Date(t));
          const b = dayKey(new Date(t + dt));
          expect(isDay(a) && isDay(b)).toBe(true);
          expect(a <= b).toBe(true);
          expect(daysBetween(a, b)).toBeLessThanOrEqual(Math.ceil(dt / 86_400_000) + 1);
          expect(daysBetween(a, b)).toBeGreaterThanOrEqual(Math.floor(dt / 86_400_000) - 1);
        },
      ),
      { numRuns: 300 },
    );
  });

  it('dayKey of a local date built from components is exactly those components, in any zone', () => {
    const before = process.env.TZ;
    afterAll(() => {
      if (before === undefined) delete process.env.TZ;
      else process.env.TZ = before;
    });
    fc.assert(
      fc.property(
        fc.constantFrom('UTC', 'America/New_York', 'Europe/Berlin', 'Australia/Sydney'),
        fc.integer({ min: 2024, max: 2030 }),
        fc.integer({ min: 0, max: 11 }),
        fc.integer({ min: 1, max: 28 }),
        fc.integer({ min: 0, max: 23 }),
        (tz, y, m, d, h) => {
          process.env.TZ = tz;
          expect(dayKey(new Date(y, m, d, h, 30))).toBe(`${y}-${String(m + 1).padStart(2, '0')}-${String(d).padStart(2, '0')}`);
        },
      ),
      RUNS,
    );
  });
});

// ---------- streaks ----------

const stepArb = fc.record({ delta: fc.oneof({ weight: 6, arbitrary: fc.integer({ min: 1, max: 3 }) }, { weight: 2, arbitrary: fc.integer({ min: 4, max: 12 }) }, { weight: 1, arbitrary: fc.integer({ min: -5, max: 0 }) }) });

describe('streaks', () => {
  it('advanceStreak keeps every invariant over arbitrary practice histories, including a clock that goes backwards', () => {
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: MAX_FREEZES }),
        fc.uniqueArray(fc.integer({ min: 0, max: 6 }), { maxLength: 3 }),
        dayArb,
        fc.array(stepArb, { minLength: 1, maxLength: 40 }),
        (freezes, restDays, start, steps) => {
          let p: Progress = { ...emptyProgress(), freezes, profile: { ...emptyProgress().profile, restDays } };
          let today = start;
          for (const { delta } of steps) {
            today = addDays(today, delta);
            const before = p;
            const status = streakStatus(before, today);
            const r = advanceStreak(before, today);
            // prediction and settlement agree
            if (before.lastDay !== null && daysBetween(before.lastDay, today) > 0) {
              expect(r.restarted).toBe(status.state === 'lapsed');
              expect(r.freezeUsed).toBe(status.state === 'lapsed' ? 0 : status.freezesNeeded);
            }
            // invariants
            expect(r.lastDay).toBe(today);
            expect(r.streak).toBeGreaterThanOrEqual(1);
            expect(r.longestStreak).toBeGreaterThanOrEqual(Math.max(r.streak, before.longestStreak));
            expect(r.freezes).toBeGreaterThanOrEqual(0);
            expect(r.freezes).toBeLessThanOrEqual(MAX_FREEZES);
            expect(r.freezeUsed).toBeLessThanOrEqual(before.freezes);
            expect(r.freezes).toBe(before.freezes - r.freezeUsed + (r.freezeEarned ? 1 : 0));
            if (before.lastDay !== null) {
              const gap = daysBetween(before.lastDay, today);
              if (gap === 1) expect(r.streak).toBe(before.streak + 1);
              if (gap <= 0) expect(r.streak).toBe(Math.max(before.streak, 1));
              if (gap > 1 && !r.restarted) expect(r.streak).toBe(before.streak + 1);
            }
            if (r.restarted) expect([r.streak, r.freezes, r.freezeEarned]).toEqual([1, before.freezes, false]);
            if (r.freezeEarned) expect(r.streak % 7).toBe(0);
            // after practising, what the chip shows is the settled streak
            p = { ...before, streak: r.streak, longestStreak: r.longestStreak, freezes: r.freezes, lastDay: r.lastDay };
            expect(liveStreak(p, today)).toBe(r.streak);
            // idempotent within the day
            expect(advanceStreak(p, today)).toMatchObject({ streak: r.streak, freezes: r.freezes, incremented: false });
          }
        },
      ),
      { numRuns: 150 },
    );
  });

  it('missed days that are all chosen rest days cost nothing', () => {
    fc.assert(
      fc.property(dayArb, fc.integer({ min: 2, max: 3 }), (last, gap) => {
        const missedDays = Array.from({ length: gap - 1 }, (_, i) => weekday(addDays(last, i + 1)));
        const p: Progress = { ...emptyProgress(), streak: 5, longestStreak: 5, lastDay: last, freezes: 0, profile: { ...emptyProgress().profile, restDays: [...new Set(missedDays)] } };
        fc.pre(p.profile.restDays.length <= 3);
        const r = advanceStreak(p, addDays(last, gap));
        expect(r).toMatchObject({ streak: 6, freezeUsed: 0, restarted: false });
      }),
      RUNS,
    );
  });
});

// ---------- spaced repetition ----------

const gradeArb = fc.constantFrom<Grade>('again', 'hard', 'good', 'easy');
const q0 = questions[0]!;
const hash = card(q0).h;

describe('scheduleCard', () => {
  it('keeps the card valid over arbitrary review histories (on time, early, or with the clock going backwards)', () => {
    fc.assert(
      fc.property(
        dayArb,
        fc.option(fc.integer({ min: 2, max: 400 }), { nil: undefined }),
        fc.array(fc.record({ grade: gradeArb, wait: fc.oneof(fc.constant('due' as const), fc.integer({ min: -3, max: 4 })), hc: fc.boolean() }), { minLength: 1, maxLength: 30 }),
        (start, deadlineIn, steps) => {
          let c: Card | undefined;
          let today = start;
          const deadline = deadlineIn === undefined ? null : addDays(start, deadlineIn);
          for (const s of steps) {
            const prev = c;
            today = s.wait === 'due' ? (prev && prev.due > today ? prev.due : today) : addDays(today, s.wait);
            const next = scheduleCard(prev, s.grade, today, { hash, deadline, hypercorrect: s.hc && s.grade === 'again' });
            expect(next.h).toBe(hash);
            expect(next.ease).toBeGreaterThanOrEqual(MIN_EASE);
            expect(next.ease).toBeLessThanOrEqual(MAX_EASE);
            expect(Number.isInteger(next.interval) && next.interval >= 0 && next.interval <= 120).toBe(true);
            expect(next.due >= next.last).toBe(true);
            expect(next.seen).toBe((prev?.seen ?? 0) + 1);
            expect(next.firstTry).toBeLessThanOrEqual(next.seen);
            expect(next.lapses).toBeLessThanOrEqual(next.seen);
            if (prev) expect(next.last >= prev.last).toBe(true); // `last` never moves backwards with the clock
            if (s.grade === 'again') {
              expect(next.reps).toBe(0);
              expect(next.interval).toBe(1);
              expect(daysBetween(next.last, next.due)).toBe(s.hc ? 0 : 1);
              expect(next.lapses).toBe((prev?.lapses ?? 0) + 1);
            } else if (!prev || prev.due <= next.last || prev.interval === 0) {
              expect(next.interval).toBeGreaterThanOrEqual(1);
              expect(next.interval).toBeLessThanOrEqual(intervalCap(next.last, deadline));
              expect(next.due).toBe(addDays(next.last, next.interval));
              expect(next.reps).toBe((prev?.reps ?? 0) + 1);
            } else {
              // an early review is bookkeeping only
              expect({ interval: next.interval, due: next.due, ease: next.ease }).toEqual({ interval: prev.interval, due: prev.due, ease: prev.ease });
            }
            c = next;
          }
        },
      ),
      RUNS,
    );
  });

  it('answering "good" on the due day never shortens the interval, and grows it until the cap', () => {
    fc.assert(
      fc.property(dayArb, fc.integer({ min: 2, max: 25 }), (start, n) => {
        let c: Card | undefined;
        let today = start;
        let last = 0;
        for (let i = 0; i < n; i++) {
          c = scheduleCard(c, 'good', today, { hash });
          expect(c.interval).toBeGreaterThanOrEqual(last);
          if (last > 0 && last < 120) expect(c.interval).toBeGreaterThan(last);
          last = c.interval;
          today = c.due;
        }
      }),
      RUNS,
    );
  });

  it('for any reachable card, hard <= good <= easy (before the cap)', () => {
    fc.assert(
      fc.property(dayArb, fc.array(gradeArb, { maxLength: 15 }), (start, history) => {
        let c: Card | undefined;
        let today = start;
        for (const g of history) {
          c = scheduleCard(c, g, today, { hash });
          today = c.due;
        }
        const next = (g: Grade) => scheduleCard(c, g, today, { hash }).interval;
        const [hard, good, easy] = [next('hard'), next('good'), next('easy')];
        if (good < 120) expect(hard).toBeLessThanOrEqual(good);
        expect(good).toBeLessThanOrEqual(easy);
      }),
      RUNS,
    );
  });

  it('is a pure function: same inputs, same card, and the input card is not modified', () => {
    fc.assert(
      fc.property(dayArb, gradeArb, fc.boolean(), (day, g, hc) => {
        const base = scheduleCard(undefined, 'good', day, { hash });
        const frozen = JSON.stringify(base);
        const a = scheduleCard(base, g, addDays(day, 1), { hash, hypercorrect: hc });
        const b = scheduleCard(base, g, addDays(day, 1), { hash, hypercorrect: hc });
        expect(a).toEqual(b);
        expect(JSON.stringify(base)).toBe(frozen);
      }),
      RUNS,
    );
  });

  it('intervalCap is within [1, 120] and never shrinks when the deadline moves later', () => {
    fc.assert(
      fc.property(dayArb, fc.integer({ min: 0, max: 400 }), fc.integer({ min: 0, max: 400 }), (today, a, b) => {
        const [lo, hi] = a <= b ? [a, b] : [b, a];
        const capLo = intervalCap(today, addDays(today, lo));
        const capHi = intervalCap(today, addDays(today, hi));
        expect(capLo).toBeGreaterThanOrEqual(1);
        expect(capHi).toBeLessThanOrEqual(120);
        expect(capLo).toBeLessThanOrEqual(capHi);
        expect(intervalCap(today, null)).toBe(120);
        expect(intervalCap(today, addDays(today, -1 - lo))).toBe(120); // a passed deadline is ignored
      }),
      RUNS,
    );
  });

  it('retention and strength stay in [0, 1] and only fall as a card gets more overdue', () => {
    fc.assert(
      fc.property(dayArb, fc.array(gradeArb, { minLength: 1, maxLength: 8 }), fc.integer({ min: 0, max: 200 }), fc.integer({ min: 0, max: 200 }), (start, history, x, y) => {
        let c: Card | undefined;
        let today = start;
        for (const g of history) {
          c = scheduleCard(c, g, today, { hash });
          today = c.due;
        }
        const [near, far] = x <= y ? [x, y] : [y, x];
        const r1 = retention(c!, addDays(c!.due, near));
        const r2 = retention(c!, addDays(c!.due, far));
        const s1 = strength(c, addDays(c!.due, near));
        const s2 = strength(c, addDays(c!.due, far));
        for (const v of [r1, r2, s1, s2]) {
          expect(v).toBeGreaterThanOrEqual(0);
          expect(v).toBeLessThanOrEqual(1);
        }
        expect(r2).toBeLessThanOrEqual(r1);
        expect(s2).toBeLessThanOrEqual(s1);
        expect(strength(undefined, today)).toBe(0);
      }),
      RUNS,
    );
  });
});

// ---------- parsing and comparing ----------

describe('parseNumber', () => {
  it('never throws, and returns null or a finite number', () => {
    fc.assert(
      fc.property(fc.string(), (s) => {
        const n = parseNumber(s);
        expect(n === null || Number.isFinite(n)).toBe(true);
      }),
      { numRuns: 500 },
    );
  });

  it('reads back any plain decimal it is shown', () => {
    fc.assert(
      fc.property(fc.double({ min: -1e9, max: 1e9, noNaN: true }).map((x) => Math.round(x * 1000) / 1000), (x) => {
        fc.pre(!String(x).includes('e'));
        expect(parseNumber(String(x)) === x).toBe(true); // (=== so that -0 equals 0)
        expect(parseNumber(`  ${x}  `) === x).toBe(true);
      }),
      RUNS,
    );
  });

  it('ignores spaces and underscores anywhere in a number', () => {
    fc.assert(
      fc.property(fc.integer({ min: 0, max: 99_999_999 }), fc.array(fc.constantFrom(' ', '_', '\t'), { maxLength: 4 }), fc.nat(), (n, junk, at) => {
        const s = String(n);
        const i = at % (s.length + 1);
        expect(parseNumber(s.slice(0, i) + junk.join('') + s.slice(i))).toBe(n);
      }),
      RUNS,
    );
  });

  it('understands thousands separators in US and European style, and a "k" suffix', () => {
    const group = (n: number, sep: string) => String(n).replace(/\B(?=(\d{3})+(?!\d))/g, sep);
    fc.assert(
      fc.property(fc.integer({ min: 1000, max: 999_999_999 }), fc.integer({ min: 1, max: 99 }), (n, cents) => {
        const frac = String(cents).padStart(2, '0');
        expect(parseNumber(group(n, ','))).toBe(n);
        expect(parseNumber(`${group(n, ',')}.${frac}`)).toBeCloseTo(n + cents / 100, 6);
        expect(parseNumber(`${group(n, '.')},${frac}`)).toBeCloseTo(n + cents / 100, 6);
        expect(parseNumber(`${n}k`)).toBe(n * 1000);
        expect(parseNumber(`${n}%`)).toBe(n);
      }),
      RUNS,
    );
  });

  it('regression: European "1.234,5" is 1234.5, not 1.2345 (the last separator is the decimal point)', () => {
    expect(parseNumber('1.234,5')).toBe(1234.5);
    expect(parseNumber('1.234.567,89')).toBe(1234567.89);
    expect(parseNumber('1,234.5')).toBe(1234.5);
    expect(parseNumber('1,234,567.89')).toBe(1234567.89);
    expect(parseNumber('2,5k')).toBe(2500);
    expect(parseNumber('1.2,3,4')).toBeNull();
  });

  it('rejects text that is not a number', () => {
    for (const bad of ['', ' ', 'abc', '1e3', '--1', '1.2.3', '0x10', 'NaN', 'Infinity', '12abc', 'k', '%', '1,2,3,4x']) expect(parseNumber(bad), bad).toBeNull();
  });
});

const canon = (v: unknown): string => JSON.stringify(v, (_k, x) => (x && typeof x === 'object' && !Array.isArray(x) ? Object.fromEntries(Object.entries(x).sort(([a], [b]) => (a < b ? -1 : 1))) : x));

describe('deepEqual / show', () => {
  it('is reflexive, symmetric, and agrees with comparing key-order-independent JSON', () => {
    fc.assert(
      fc.property(fc.jsonValue(), fc.jsonValue(), (a, b) => {
        expect(deepEqual(a, a)).toBe(true);
        expect(deepEqual(a, structuredClone(a))).toBe(true);
        expect(deepEqual(a, b)).toBe(deepEqual(b, a));
        expect(deepEqual(a, b)).toBe(canon(a) === canon(b));
      }),
      { numRuns: 400 },
    );
  });

  it('notices a change to any single leaf', () => {
    fc.assert(
      fc.property(fc.array(fc.integer(), { minLength: 1, maxLength: 8 }), fc.nat(), (xs, at) => {
        const ys = [...xs];
        const i = at % ys.length;
        ys[i] = ys[i]! + 1;
        expect(deepEqual({ xs }, { xs: ys })).toBe(false);
        expect(deepEqual(xs, [...xs, 0])).toBe(false);
      }),
      RUNS,
    );
  });

  it('does not confuse arrays, objects and null', () => {
    expect(deepEqual([], {})).toBe(false);
    expect(deepEqual({ 0: 1 }, [1])).toBe(false);
    expect(deepEqual(null, {})).toBe(false);
    expect(deepEqual({ a: undefined }, { b: undefined })).toBe(false);
    expect(deepEqual(Number.NaN, Number.NaN)).toBe(true);
    expect(deepEqual(0, -0)).toBe(true);
  });

  it('show never throws and stays short', () => {
    fc.assert(
      fc.property(fc.anything(), (v) => {
        const t = show(v);
        expect(typeof t).toBe('string');
        expect(t.length).toBeLessThanOrEqual(140);
      }),
      RUNS,
    );
    const cyclic: Record<string, unknown> = {};
    cyclic['self'] = cyclic;
    expect(() => show(cyclic)).not.toThrow();
    expect(show(undefined)).toBe('undefined');
    expect(show(Number.POSITIVE_INFINITY)).toBe('Infinity');
  });
});

describe('runCodeTests', () => {
  it('a correct function passes any JSON arguments it is checked against, wrong ones fail', () => {
    fc.assert(
      fc.property(fc.array(fc.integer({ min: -1000, max: 1000 }), { maxLength: 12 }), (xs) => {
        const sum = xs.reduce((a, b) => a + b, 0);
        const ok = runCodeTests('function total(a) { return a.reduce((x, y) => x + y, 0); }', 'total', [{ args: [xs], expect: sum }]);
        const bad = runCodeTests('function total(a) { return a.reduce((x, y) => x + y, 0) + 1; }', 'total', [{ args: [xs], expect: sum }]);
        expect(ok.ok).toBe(true);
        expect(bad.ok).toBe(false);
      }),
      RUNS,
    );
  });

  it('does not let one run change the arguments of the next (arguments are cloned per test)', () => {
    const r = runCodeTests('function f(a) { a.push(1); return a.length; }', 'f', [{ args: [[]], expect: 1 }, { args: [[]], expect: 1 }]);
    expect(r.ok).toBe(true);
  });

  it('never throws for arbitrary source text: it reports a fatal error or a result', () => {
    fc.assert(
      fc.property(fc.stringMatching(/^[a-z(){}=;+*\-,. 0-9\n]{0,60}$/), (src) => {
        const r = runCodeTests(src, 'f', [{ args: [1], expect: 1 }]);
        expect(typeof r.ok).toBe('boolean');
        if (r.fatal !== undefined) expect(r.ok).toBe(false);
      }),
      { numRuns: 300 },
    );
  });
});

// ---------- deterministic selection ----------

describe('seededShuffle / interleave', () => {
  it('shuffles are deterministic permutations; shuffleDifferent really differs', () => {
    fc.assert(
      fc.property(fc.array(fc.integer({ min: 0, max: 5 }), { maxLength: 15 }), fc.integer(), (xs, seed) => {
        const a = seededShuffle(xs, seed);
        expect(a).toEqual(seededShuffle(xs, seed));
        expect([...a].sort()).toEqual([...xs].sort());
        const d = shuffleDifferent(xs, seed);
        expect([...d].sort()).toEqual([...xs].sort());
        if (new Set(xs).size > 1) expect(d).not.toEqual(xs);
      }),
      RUNS,
    );
  });

  it('interleave keeps every item exactly once, is deterministic, and separates same-topic neighbours whenever that is possible', () => {
    fc.assert(
      fc.property(fc.array(fc.constantFrom('a', 'b', 'c', 'd'), { maxLength: 24 }), fc.option(fc.integer(), { nil: undefined }), (keys, seed) => {
        const items = keys.map((k, i) => ({ k, i }));
        const out = interleave(items, (x) => x.k, seed);
        expect(out).toHaveLength(items.length);
        expect(new Set(out.map((x) => x.i)).size).toBe(items.length);
        expect(interleave(items, (x) => x.k, seed)).toEqual(out);
        const counts = new Map<string, number>();
        for (const k of keys) counts.set(k, (counts.get(k) ?? 0) + 1);
        if (Math.max(0, ...counts.values()) <= Math.ceil(keys.length / 2)) {
          for (let i = 1; i < out.length; i++) expect(out[i]!.k).not.toBe(out[i - 1]!.k);
        }
        // within one topic the original order is kept
        for (const k of counts.keys()) {
          const idx = out.filter((x) => x.k === k).map((x) => x.i);
          expect(idx).toEqual([...idx].sort((a, b) => a - b));
        }
      }),
      RUNS,
    );
  });
});

describe('daily session, mock and challenge selection', () => {
  const pAt = (today: string, offsets: [number, number][]): Progress =>
    withCards(emptyProgress(), offsets.map(([qi, due]) => [questions[qi % questions.length]!, { due: addDays(today, due), interval: 3, last: addDays(today, due - 3) }] as const).map(([q, over]) => [q, over]));

  it('buildDailySession is deterministic, duplicate-free, within size, and describes itself truthfully', () => {
    fc.assert(
      fc.property(
        dayArb,
        fc.array(fc.tuple(fc.nat(questions.length - 1), fc.integer({ min: -20, max: 20 })), { maxLength: 40 }),
        fc.integer({ min: 1, max: 12 }),
        (today, offsets, size) => {
          const p = pAt(today, offsets);
          const plan = buildDailySession(course, p, today, { size });
          expect(buildDailySession(course, p, today, { size })).toEqual(plan);
          const ids = plan.questions.map((q) => q.id);
          expect(new Set(ids).size).toBe(ids.length);
          expect(ids.length).toBeLessThanOrEqual(size);
          expect(plan.mix.due + plan.mix.weak + plan.mix.fresh + plan.mix.filler).toBe(ids.length);
          expect(plan.kind === 'first').toBe(Object.keys(p.cards).length === 0);
          expect(plan.minutes).toBeGreaterThanOrEqual(1);
          const dueCount = Object.values(p.cards).filter((c) => c.due <= today).length;
          expect(plan.mix.due).toBeGreaterThanOrEqual(Math.min(DAILY_MAX_DUE, dueCount, size));
        },
      ),
      { numRuns: 60 },
    );
    expect(DAILY_SIZE).toBe(8);
  });

  it('buildMock is a seeded draw: deterministic, unique, sized, topic-restricted and capped on slow formats', () => {
    const units = course.units.map((u) => u.id);
    fc.assert(
      fc.property(fc.constantFrom(10 as const, 20 as const), fc.option(fc.constantFrom(...units), { nil: null }), fc.integer({ min: 0, max: 1_000_000 }), dayArb, (length, unitId, seed, today) => {
        const cfg = { length, minutes: 15, unitId };
        const a = buildMock(course, emptyProgress(), today, cfg, seed);
        expect(buildMock(course, emptyProgress(), today, cfg, seed).map((q) => q.id)).toEqual(a.map((q) => q.id));
        const ids = a.map((q) => q.id);
        expect(new Set(ids).size).toBe(ids.length);
        const pool = unitId ? course.units.find((u) => u.id === unitId)!.lessons.flatMap((l) => l.questions) : questions;
        expect(a).toHaveLength(Math.min(length, pool.length));
        if (unitId) for (const q of a) expect(pool.some((x) => x.id === q.id)).toBe(true);
        const n = a.length;
        expect(a.filter((q) => q.type === 'code').length).toBeLessThanOrEqual(Math.max(1, Math.round(n / 10)));
        expect(a.filter((q) => q.type === 'open').length).toBeLessThanOrEqual(Math.max(1, Math.round(n / 5)));
      }),
      { numRuns: 60 },
    );
  });

  it('the daily challenge is a pure function of the date: stable, clone-proof, never a repeat of yesterday, never a zero-weight format', () => {
    const clone = structuredClone(course);
    fc.assert(
      fc.property(dayArb, (day) => {
        const q = challengeQuestion(course, day)!;
        expect(q).toBeTruthy();
        expect(challengeQuestion(course, day)!.id).toBe(q.id);
        expect(challengeQuestion(clone, day)!.id).toBe(q.id);
        expect(CHALLENGE_WEIGHTS[q.type]).toBeGreaterThan(0);
        if (day > '2024-01-01') expect(challengeQuestion(course, addDays(day, -1))!.id).not.toBe(q.id);
      }),
      { numRuns: 80 },
    );
  });
});
