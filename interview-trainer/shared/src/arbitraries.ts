/** Test helpers: fast-check generators for days and progress. Not exported from the package index. */
import fc from 'fast-check';
import { dayNumber, fromDayNumber, type Day } from './day';
import { normalizeProgress, type MigrationContext } from './migrate';
import type { Progress } from './progress';

export const TODAY: Day = '2026-06-15';
/** Questions are empty on purpose: pruning then keeps every card, so generated cards survive normalisation. */
export const CTX: MigrationContext = { today: TODAY, questions: [] };

const FIRST = dayNumber('2024-01-01');
const LAST = dayNumber('2028-12-31');

export const dayArb: fc.Arbitrary<Day> = fc.integer({ min: FIRST, max: LAST }).map(fromDayNumber);
/** A day within a year before `TODAY` (the window progress data actually lives in). */
export const recentDayArb: fc.Arbitrary<Day> = fc.integer({ min: dayNumber(TODAY) - 360, max: dayNumber(TODAY) }).map(fromDayNumber);

const keyArb = fc.oneof(
  fc.string({ minLength: 1, maxLength: 12 }),
  fc.constantFrom('__proto__', 'constructor', 'toString', 'a/1', 'b/2', 'c/3', 'kotlin/1', 'kotlin/2'),
);
const junk = fc.anything();
const maybeJunk = <T>(good: fc.Arbitrary<T>): fc.Arbitrary<unknown> => fc.oneof({ weight: 8, arbitrary: good }, { weight: 1, arbitrary: junk });
const int = (max = 5000) => fc.integer({ min: -5, max });

const cardLike = fc.record(
  {
    h: fc.oneof(fc.stringMatching(/^[0-9a-f]{1,8}$/), fc.constant('')),
    ease: fc.oneof(fc.double({ min: 0, max: 5, noNaN: true }), fc.constant(Number.NaN)),
    interval: int(300),
    due: fc.oneof(dayArb, fc.string()),
    reps: int(),
    lapses: int(),
    seen: int(),
    firstTry: int(),
    last: fc.oneof(dayArb, fc.string()),
    hc: fc.oneof(fc.constant(true), fc.constant(false)),
  },
  { requiredKeys: ['h', 'due'] },
);

/** A loosely shaped, partly invalid "stored progress" object: what a damaged or hand-edited profile might look like. */
export const rawProgressArb: fc.Arbitrary<Record<string, unknown>> = fc.record(
  {
    version: fc.constant(2),
    xp: maybeJunk(int(100_000)),
    streak: maybeJunk(int(500)),
    lastDay: maybeJunk(fc.oneof(recentDayArb, fc.constant(null))),
    lessons: maybeJunk(
      fc.dictionary(
        keyArb,
        fc.record({ bestAccuracy: fc.double({ min: -1, max: 2, noNaN: true }), completions: int(50), lastDay: fc.oneof(recentDayArb, fc.string()) }, { requiredKeys: [] }),
        { maxKeys: 5 },
      ),
    ),
    missed: maybeJunk(fc.array(fc.oneof(fc.string(), keyArb), { maxLength: 6 })),
    longestStreak: maybeJunk(int(500)),
    freezes: maybeJunk(int(5)),
    cards: maybeJunk(fc.dictionary(keyArb, maybeJunk(cardLike), { maxKeys: 6 })),
    days: maybeJunk(
      fc.dictionary(
        fc.oneof(recentDayArb, fc.string()),
        fc.record({ xp: int(), answered: int(60), firstTry: int(60), target: int(120), comeback: fc.boolean() }, { requiredKeys: [] }),
        { maxKeys: 6 },
      ),
    ),
    goal: maybeJunk(fc.record({ target: fc.oneof(int(300), fc.double()) }, { requiredKeys: [] })),
    profile: maybeJunk(
      fc.record(
        {
          onboarded: fc.boolean(),
          role: fc.constantFrom('backend-kotlin', 'staff-architect', 'generalist', 'wizard', null),
          interviewDate: fc.oneof(dayArb, fc.string(), fc.constant(null)),
          restDays: fc.array(fc.integer({ min: -2, max: 9 }), { maxLength: 6 }),
          practiceTime: fc.oneof(fc.constantFrom('09:00', '23:59', '24:00', '9:00', 'x'), fc.constant(null)),
        },
        { requiredKeys: [] },
      ),
    ),
    settings: maybeJunk(fc.record({ haptics: fc.boolean(), sound: fc.boolean(), confetti: fc.boolean(), tips: fc.boolean(), confidence: fc.boolean() }, { requiredKeys: [] })),
    stats: maybeJunk(fc.record({ sessions: int(), answered: int(), firstTry: int(), perfect: int(), practiceDays: int() }, { requiredKeys: [] })),
    milestones: maybeJunk(fc.dictionary(fc.oneof(keyArb, fc.string({ minLength: 70, maxLength: 90 })), fc.oneof(recentDayArb, fc.string()), { maxKeys: 5 })),
    calibration: maybeJunk(fc.record({ n: fc.array(int(50), { maxLength: 4 }), right: fc.array(int(50), { maxLength: 4 }) }, { requiredKeys: [] })),
    bookmarks: maybeJunk(fc.dictionary(keyArb, fc.record({ h: fc.string(), day: fc.oneof(dayArb, fc.string()) }, { requiredKeys: [] }), { maxKeys: 4 })),
    notes: maybeJunk(fc.dictionary(keyArb, fc.record({ h: fc.string(), text: fc.string({ maxLength: 700 }), day: fc.oneof(dayArb, fc.string()) }, { requiredKeys: [] }), { maxKeys: 4 })),
    challenge: maybeJunk(fc.dictionary(fc.oneof(recentDayArb, fc.string()), fc.constantFrom('pass', 'fail', 'maybe'), { maxKeys: 5 })),
    achievements: maybeJunk(fc.dictionary(keyArb, fc.oneof(recentDayArb, fc.string()), { maxKeys: 5 })),
    mocks: maybeJunk(
      fc.array(
        fc.record(
          {
            day: fc.oneof(recentDayArb, fc.string()),
            total: int(250),
            answered: int(300),
            score: fc.double({ min: -10, max: 150, noNaN: true }),
            limitMin: int(700),
            usedSec: int(100_000),
            timedOut: fc.boolean(),
            byUnit: fc.dictionary(keyArb, fc.record({ n: int(250), score: fc.double({ min: -5, max: 300, noNaN: true }) }, { requiredKeys: [] }), { maxKeys: 3 }),
          },
          { requiredKeys: [] },
        ),
        { maxLength: 14 },
      ),
    ),
    passedByType: maybeJunk(fc.dictionary(fc.constantFrom('single', 'open', 'code', 'x'.repeat(30)), int(50), { maxKeys: 4 })),
    prefs: maybeJunk(
      fc.record(
        {
          reminders: fc.record({ time: fc.constantFrom('09:00', '7:5', null), notify: fc.boolean(), lastNotified: fc.oneof(recentDayArb, fc.constant(null)) }, { requiredKeys: [] }),
          reading: fc.record({ textSize: fc.constantFrom('S', 'M', 'L', 'XL', 'XXL'), dyslexiaFont: fc.boolean() }, { requiredKeys: [] }),
        },
        { requiredKeys: [] },
      ),
    ),
  },
  { requiredKeys: [] },
);

/** A fully valid Progress: any raw value, passed through the normaliser (which is what makes data valid). */
export const progressArb: fc.Arbitrary<Progress> = rawProgressArb.map((raw) => {
  const p = normalizeProgress(raw, CTX);
  // Merging deliberately collapses mock records identical in day, size, score, answers and duration (one mock imported
  // twice), so valid test data does not contain such twins.
  const seen = new Set<string>();
  const mocks = p.mocks.filter((m) => {
    const key = `${m.day}|${m.total}|${m.score}|${m.usedSec}|${m.answered}`;
    return seen.has(key) ? false : (seen.add(key), true);
  });
  // Documented order: newest first.
  mocks.sort((a, b) => (a.day < b.day ? 1 : a.day > b.day ? -1 : 0));
  return { ...p, mocks };
});
