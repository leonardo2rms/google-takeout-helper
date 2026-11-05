import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { loadCourse } from '../shared/src/load-content';
import {
  bagOfWords, combinedScore, describeViolations, has, lint, LIMITS, measure, measureStyle, STYLE_LIMITS, styleViolations, tokens, violations, type Choice,
} from '../scripts/content-lint';

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

describe('style tells: the lint itself', () => {
  const style = (all: Choice[]) => measureStyle(all, 'ALL', bagOfWords(all.filter((c) => c.kind === 'single')));
  const feat = (all: Choice[], f: string) => style(all).single.find((s) => s.feature === f)!;
  const opt = (text: string) => ({ id: 'x', text });
  /** A word no other option uses (the tokeniser drops digits, so build it from letters). */
  const uniq = (i: number, j: number) => `${'abcdefghijklmnopqrstuvwxyz'[i % 26]}${'abcdefghijklmnopqrstuvwxyz'[Math.floor(i / 26)]}${'wxyz'[j]}word`;

  it('reads features from prose only, never from code spans', () => {
    expect(has('marker', opt('`fun f(): Int`'))).toBe(false);
    expect(has('marker', opt('Use a lock (a mutex)'))).toBe(true);
    expect(has('or', opt('`a or b`'))).toBe(false);
    expect(has('or', opt('Retry or fail'))).toBe(true);
    expect(has('or', opt('Use an orchestrator'))).toBe(false);
    expect(has('causal', opt('Fails because the lock is held'))).toBe(true);
    expect(has('absolute', opt('It never blocks'))).toBe(true);
  });

  it('measures how much a feature points at the answer: lift, P(correct), prefer and avoid rules', () => {
    // "or" is only ever in the answer: the prefer rule always wins, the avoid rule never does.
    const leaky = Array.from({ length: 16 }, (_, i) => q(`o/${i}`, 'u', ['Retry or fail', 'Retry now', 'Fail now', 'Wait now'].map((t, j) => `${t} ${i}${j}`), [0]));
    const s = feat(leaky, 'or');
    expect(s.answers).toBe(16);
    expect(s.distractors).toBe(0);
    expect(s.lift).toBe(Infinity);
    expect(s.pCorrect).toBe(1);
    expect(s.prefer).toBe(1);
    expect(s.avoid).toBe(0);
    // A feature on every option tells nothing: both rules are at chance.
    const flat = Array.from({ length: 16 }, (_, i) => q(`f/${i}`, 'u', ['A or B', 'C or D', 'E or F', 'G or H'].map((t) => `${t} ${i}`), [i % 4]));
    expect(feat(flat, 'or').prefer).toBeCloseTo(0.25);
    expect(feat(flat, 'or').avoid).toBeCloseTo(0.25);
    expect(feat(flat, 'or').lift).toBeCloseTo(1);
  });

  it('does not judge a feature that is too rare to measure', () => {
    const rare = [q('r/1', 'u', ['Retry or fail', 'Retry', 'Fail', 'Wait'], [0])];
    expect(Number.isNaN(feat(rare, 'or').lift)).toBe(true);
    expect(styleViolations({ overall: style(rare) }).filter((v) => v.what.includes('lift'))).toEqual([]);
  });

  it('scores options with the fixed combined rule (markers and "or" up; causal, absolute and shortest down)', () => {
    const c = q('c/1', 'u', ['Retry (with backoff) or fail', 'It always fails because of locks', 'Fail', 'Wait for it'], [0]);
    expect(combinedScore(c, c.options[0]!)).toBe(4);
    expect(combinedScore(c, c.options[1]!)).toBe(-2.5);
    expect(combinedScore(c, c.options[2]!)).toBe(-1);
    expect(combinedScore(c, c.options[3]!)).toBe(0);
  });

  it('tokenises words, stop words included, and ( ; : marks', () => {
    expect([...tokens('Retry (once); or fail: because')]).toEqual(['retry', '(', 'once', ';', 'or', 'fail', ':', 'because']);
  });

  it('the bag-of-words attacker learns a word that marks answers, and is at chance without one', () => {
    const marked = Array.from({ length: 10 }, (_, i) => q(`b/${i}`, 'u', [`zeta ${uniq(i, 0)}`, uniq(i, 1), uniq(i, 2), uniq(i, 3)], [0]));
    const acc = bagOfWords(marked);
    expect([...acc.values()].every((x) => x === 1)).toBe(true);
    // Leave-one-out: a word seen only in the question being scored teaches nothing.
    const unique = Array.from({ length: 10 }, (_, i) => q(`u/${i}`, 'u', [0, 1, 2, 3].map((j) => uniq(i, j)), [0]));
    expect([...bagOfWords(unique).values()].every((x) => Math.abs(x - 0.25) < 1e-9)).toBe(true);
  });

  it('the bag-of-words attacker is not fooled into preferring options with more unseen words', () => {
    // The answer is always the option with one word; distractors have many words nobody else uses. A classifier that
    // rewarded unseen words would pick a distractor every time; a neutral one ties at chance or learns nothing worse.
    const wordy = Array.from({ length: 12 }, (_, i) => q(`w/${i}`, 'u', [uniq(i, 0), ...[1, 2, 3].map((j) => `${uniq(i, j)} ${uniq(i + 100, j)} ${uniq(i + 200, j)}`)], [0]));
    expect([...bagOfWords(wordy).values()].every((x) => Math.abs(x - 0.25) < 1e-9)).toBe(true);
  });

  it('fails a course whose answers are styled differently, and names the fix', () => {
    const biased = Array.from({ length: 20 }, (_, i) =>
      q(`s/${i}`, 'u', [`Retry (with backoff) or fail ${i}`, `It fails because of locks ${i}`, `It fails since it waits ${i}`, `It fails which is slow ${i}`], [0]),
    );
    const v = styleViolations({ overall: style(biased) });
    const whats = v.map((x) => x.what).join('\n');
    expect(whats).toContain('options with ( ; or : are more often correct');
    expect(whats).toContain('options with the word "or" are more often correct');
    expect(whats).toContain('options with a causal clause (because, since, so that, which) are more often wrong');
    expect(whats).toContain('combined style rule');
    expect(whats).toContain('bag-of-words');
    expect(describeViolations(v)).toContain('--offenders');
  });

  it('passes a course where every option is written in the same style', () => {
    // Answer position and answer style both rotate, so each style is the answer a quarter of the time.
    const styles = ['(aside) or', 'because', 'plain', 'plain too'];
    const even = Array.from({ length: 32 }, (_, i) =>
      q(`e/${i}`, 'u', [0, 1, 2, 3].map((j) => `${uniq(i, j)} ${styles[(j + Math.floor(i / 4)) % 4]}`), [i % 4]),
    );
    expect(styleViolations({ overall: style(even) })).toEqual([]);
  });

  it('keeps the style limits near chance', () => {
    expect(STYLE_LIMITS.rule).toBeLessThanOrEqual(0.35);
    expect(STYLE_LIMITS.combined).toBeLessThanOrEqual(0.35);
    expect(STYLE_LIMITS.bow).toBeLessThanOrEqual(0.4);
    expect(STYLE_LIMITS.liftLow).toBeGreaterThanOrEqual(0.5);
    expect(STYLE_LIMITS.liftHigh).toBeLessThanOrEqual(1.65);
  });
});
