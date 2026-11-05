import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { loadCourse } from '../shared/src/load-content';
import { describeViolations, lint, LIMITS, measure, violations, type Choice } from '../scripts/content-lint';

/**
 * "Balanced options": a learner must not be able to pass single/multi questions by picking the longest
 * (or most detailed) option. The rule and its rationale are in docs/content-guide.md. When this file fails, the
 * message lists the questions to fix; do NOT raise a limit, rewrite the options instead.
 */
const course = loadCourse(resolve(dirname(fileURLToPath(import.meta.url))));

describe('balanced options: the real course', () => {
  const result = lint(course);

  it('the correct option is not systematically the longest (per unit, overall, top-2, correlation, multi)', () => {
    const v = violations(result);
    expect(v.length, `\n${describeViolations(v)}\n`).toBe(0);
  });

  it('measures every single and multi question', () => {
    const singles = course.units.flatMap((u) => u.lessons.flatMap((l) => l.questions)).filter((q) => q.type === 'single').length;
    expect(result.overall.singles).toBe(singles);
    expect(result.units.reduce((n, u) => n + u.singles, 0)).toBe(singles);
  });
});

const q = (id: string, unit: string, options: string[], correct: number[], kind: 'single' | 'multi' = 'single'): Choice => ({
  id, unit, prompt: 'What does the compiler check?', kind,
  options: options.map((text, i) => ({ id: String.fromCharCode(97 + i), text })),
  correct: new Set(correct.map((i) => String.fromCharCode(97 + i))),
});
const L = (n: number) => 'x'.repeat(n);
const report = (all: Choice[], units: string[]) => ({ overall: measure(all, 'ALL'), units: units.map((u) => measure(all, u)) });

describe('balanced options: the lint itself', () => {
  it('counts a strictly longest answer, not a tie, and lists it as an offender', () => {
    const s = measure([q('l/1', 'u', [L(50), L(10), L(12), L(11)], [0]), q('l/2', 'u', [L(30), L(30), L(10), L(10)], [0]), q('l/3', 'u', [L(10), L(40), L(12), L(11)], [0])], 'u');
    expect(s.singles).toBe(3);
    expect(s.longest).toBe(1);
    expect(s.offenders).toEqual(['l/1']);
    expect(s.top2).toBe(2); // the tie is broken against the answer
  });

  it('reports a clearly positive correlation when answers are longer, and a negative one when they are shorter', () => {
    const longer = measure([q('l/1', 'u', [L(50), L(10), L(12), L(11)], [0]), q('l/2', 'u', [L(10), L(40), L(12), L(11)], [1])], 'u');
    const shorter = measure([q('l/1', 'u', [L(10), L(50), L(52), L(51)], [0])], 'u');
    expect(longer.corr).toBeGreaterThan(0.9);
    expect(shorter.corr).toBeLessThan(-0.9);
  });

  it('ignores absolute words inside code spans, and counts them in prose', () => {
    const s = measure([q('l/1', 'u', ['`Any` is fine here', 'Do it always', 'Use a lock', 'Wait'], [0])], 'u');
    expect(s.absAnswers).toBe(0);
    expect(s.absDistractors).toBe(1);
    expect(s.absolutesOnlyInDistractors).toBe(1);
  });

  it('flags a multi question whose correct set is exactly the longest options', () => {
    const s = measure([q('m/1', 'u', [L(40), L(41), L(42), L(10), L(11)], [0, 1, 2], 'multi'), q('m/2', 'u', [L(40), L(10), L(42), L(50), L(11)], [0, 1, 2], 'multi')], 'u');
    expect(s.multis).toBe(2);
    expect(s.multiTopK).toBe(1);
  });

  it('passes a balanced unit and fails a biased one with the offenders named', () => {
    const balanced = [
      q('b/1', 'u', [L(50), L(10), L(12), L(11)], [0]),
      q('b/2', 'u', [L(10), L(40), L(12), L(11)], [0]),
      q('b/3', 'u', [L(30), L(40), L(12), L(11)], [0]),
      q('b/4', 'u', [L(20), L(40), L(42), L(11)], [0]),
    ];
    expect(violations(report(balanced, ['u'])).filter((v) => v.scope === 'unit u')).toEqual([]);

    const biased = Array.from({ length: 6 }, (_, i) => q(`x/${i + 1}`, 'v', [L(50), L(10), L(12), L(11)], [0]));
    const v = violations(report(biased, ['v']));
    expect(v.map((x) => x.scope)).toContain('unit v');
    expect(describeViolations(v)).toContain('x/6');
    expect(describeViolations(v)).toContain('strictly longest');
  });

  it('allows one question of rounding slack in a unit, but never more than the hard ceiling', () => {
    // 3 of 8 is 37.5%: above 35% but within the 45% ceiling and a whole question of slack.
    const eight = (n: number) => Array.from({ length: 8 }, (_, i) => q(`s/${i + 1}`, 'u', i < n ? [L(50), L(10), L(12), L(11)] : [L(10), L(50), L(12), L(11)], [0]));
    const unitOk = (n: number) => violations(report(eight(n), ['u'])).filter((v) => v.scope === 'unit u').length === 0;
    expect(unitOk(3)).toBe(true);
    expect(unitOk(4)).toBe(false);
    expect(LIMITS.unitLongestHard).toBeGreaterThan(LIMITS.unitLongest);
  });

  it('keeps the overall limit stricter than the per-unit one', () => {
    expect(LIMITS.overallLongest).toBeLessThan(LIMITS.unitLongest);
    expect(LIMITS.overallLongest).toBeGreaterThan(0.25); // chance for four options
  });
});
