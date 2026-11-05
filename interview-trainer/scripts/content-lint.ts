/**
 * Test-wiseness lint for `single` and `multi` questions.
 *
 * A learner who can pass by "pick the longest / most detailed / least absolute option" is being tested on
 * test-taking, not on the material, and the readiness numbers become dishonest. This script measures the
 * tells, per unit and overall. `content/content.test.ts` turns the headline numbers into a ratchet.
 *
 *   npm run content:lint            report only (exit 0)
 *   npm run content:lint -- --list  also list every question where the answer is the strictly longest option
 */
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadCourse } from '../shared/src/load-content';
import type { Course } from '../shared/src';

export type Opt = { id: string; text: string };
export type Choice = { id: string; unit: string; prompt: string; code?: string; kind: 'single' | 'multi'; options: Opt[]; correct: Set<string> };

/** Ratchet limits (see docs/content-guide.md, "Balanced options"). Chance for 4 options is 25%. */
export const LIMITS = { unitLongest: 0.35, unitLongestHard: 0.45, overallLongest: 0.3, overallTop2: 0.7, overallCorr: 0.3, multiTopK: 0.25 } as const;

const ABSOLUTES = /\b(always|never|only|all|none|must|cannot|can't|every|entirely|guarantees?|guaranteed|impossible|no longer|any)\b/i;
const HEDGES = /\b(usually|typically|generally|often|mostly|may|might|can|could|depends|unless|except|roughly|approximately|by default|in most|tends? to|at least|up to|rather than|but)\b/i;
const STOP = new Set(
  'a an the and or of to in on at is are was be it its as by for with that this these those from not no do does what which when how why you your can will than then into if are use used using'.split(' '),
);

/** Prose of an option: code spans removed, so identifiers such as `Any` or `all` are not mistaken for absolute claims. */
const prose = (s: string) => s.replace(/`[^`]*`/g, ' ');

const words = (s: string): string[] =>
  (s.toLowerCase().match(/[a-z][a-z0-9_]{2,}/g) ?? []).filter((w) => !STOP.has(w));

/** Rough "detail" score: numbers, code spans, identifiers, commas/semicolons. */
function specificity(t: string): number {
  return (t.match(/\d+/g)?.length ?? 0) + (t.match(/`[^`]+`/g)?.length ?? 0) + (t.match(/[,;:(]/g)?.length ?? 0);
}

export function choices(course: Course): Choice[] {
  const out: Choice[] = [];
  for (const u of course.units)
    for (const l of u.lessons)
      l.questions.forEach((q, i) => {
        if (q.type === 'single') out.push({ id: `${l.id}/${i + 1}`, unit: u.id, prompt: q.prompt, code: q.code?.source, kind: 'single', options: q.options, correct: new Set([q.answer]) });
        if (q.type === 'multi') out.push({ id: `${l.id}/${i + 1}`, unit: u.id, prompt: q.prompt, code: q.code?.source, kind: 'multi', options: q.options, correct: new Set(q.answers) });
      });
  return out;
}

const len = (o: Opt) => o.text.length;
const uniqueMax = (opts: Opt[], score: (o: Opt) => number): Opt | undefined => {
  const best = Math.max(...opts.map(score));
  const top = opts.filter((o) => score(o) === best);
  return top.length === 1 ? top[0] : undefined;
};

function pointBiserial(c: Choice): number | null {
  const xs = c.options.map(len);
  const ys = c.options.map((o) => (c.correct.has(o.id) ? 1 : 0));
  const n = xs.length;
  const mx = xs.reduce((a, b) => a + b, 0) / n;
  const my = ys.reduce((a, b) => a + b, 0) / n;
  const sx = Math.sqrt(xs.reduce((a, x) => a + (x - mx) ** 2, 0) / n);
  const sy = Math.sqrt(ys.reduce((a, y) => a + (y - my) ** 2, 0) / n);
  if (sx === 0 || sy === 0) return null;
  return xs.reduce((a, x, i) => a + (x - mx) * (ys[i]! - my), 0) / n / (sx * sy);
}

export type Stats = {
  unit: string;
  singles: number;
  /** correct option is strictly the longest */
  longest: number;
  /** correct option is among the 2 longest (ties broken against the answer) */
  top2: number;
  /** mean within-question point-biserial correlation between option length and correctness */
  corr: number;
  /** singles whose answer has no absolute word while at least one distractor does */
  absolutesOnlyInDistractors: number;
  /** answers / distractors containing an absolute word ("always", "never", "only", "all", ...) */
  absAnswers: number;
  absDistractors: number;
  /** singles whose answer is the only option with a hedging word */
  onlyHedged: number;
  /** singles whose answer is uniquely the most specific (numbers, code spans, punctuation) option */
  mostSpecific: number;
  /** options (answers / distractors) that share no content word with the prompt or its code */
  noOverlapAnswers: number;
  noOverlapDistractors: number;
  distractors: number;
  /** singles where the answer is the only option sharing any vocabulary with the prompt */
  onlyOverlap: number;
  /** singles where the longest option is more than 2x the shortest */
  ratioOver2: number;
  multis: number;
  /** multis whose correct set equals the k longest options */
  multiTopK: number;
  /** offenders: single question ids where the answer is the strictly longest option */
  offenders: string[];
};

export function measure(all: Choice[], unit: string): Stats {
  const cs = all.filter((c) => unit === 'ALL' || c.unit === unit);
  const singles = cs.filter((c) => c.kind === 'single');
  const multis = cs.filter((c) => c.kind === 'multi');
  const s: Stats = {
    unit, singles: singles.length, longest: 0, top2: 0, corr: 0, absolutesOnlyInDistractors: 0, absAnswers: 0, absDistractors: 0, onlyHedged: 0, mostSpecific: 0, onlyOverlap: 0, noOverlapAnswers: 0, noOverlapDistractors: 0, distractors: 0, ratioOver2: 0,
    multis: multis.length, multiTopK: 0, offenders: [],
  };
  let corrSum = 0;
  let corrN = 0;
  for (const c of singles) {
    const ans = c.options.find((o) => c.correct.has(o.id))!;
    const wrong = c.options.filter((o) => o !== ans);
    if (uniqueMax(c.options, len) === ans) {
      s.longest++;
      s.offenders.push(c.id);
    }
    const rank = [...c.options].sort((a, b) => len(b) - len(a) || (a === ans ? 1 : -1));
    if (rank.indexOf(ans) < 2) s.top2++;
    const r = pointBiserial(c);
    if (r !== null) {
      corrSum += r;
      corrN++;
    }
    if (!ABSOLUTES.test(prose(ans.text)) && wrong.some((o) => ABSOLUTES.test(prose(o.text)))) s.absolutesOnlyInDistractors++;
    if (ABSOLUTES.test(prose(ans.text))) s.absAnswers++;
    s.absDistractors += wrong.filter((o) => ABSOLUTES.test(prose(o.text))).length;
    if (HEDGES.test(ans.text) && !wrong.some((o) => HEDGES.test(o.text))) s.onlyHedged++;
    if (uniqueMax(c.options, (o) => specificity(o.text)) === ans) s.mostSpecific++;
    const pw = new Set(words(`${c.prompt} ${c.code ?? ''}`));
    const overlap = (o: Opt) => words(o.text).filter((w) => pw.has(w)).length;
    if (overlap(ans) > 0 && wrong.every((o) => overlap(o) === 0)) s.onlyOverlap++;
    if (overlap(ans) === 0) s.noOverlapAnswers++;
    s.noOverlapDistractors += wrong.filter((o) => overlap(o) === 0).length;
    s.distractors += wrong.length;
    if (Math.max(...c.options.map(len)) > 2 * Math.min(...c.options.map(len))) s.ratioOver2++;
  }
  for (const c of multis) {
    const k = c.correct.size;
    const top = [...c.options].sort((a, b) => len(b) - len(a)).slice(0, k);
    const longestWrong = Math.max(...c.options.filter((x) => !c.correct.has(x.id)).map(len));
    if (top.every((o) => c.correct.has(o.id) && len(o) > longestWrong)) s.multiTopK++;
  }
  s.corr = corrN ? corrSum / corrN : 0;
  return s;
}

export function lint(course: Course): { overall: Stats; units: Stats[] } {
  const all = choices(course);
  return { overall: measure(all, 'ALL'), units: course.units.map((u) => measure(all, u.id)) };
}

export type Violation = { scope: string; what: string; value: number; limit: number; offenders: string[] };

/** Whole questions allowed at `rate` of `n`, rounded up so a tiny unit is not punished for one question. */
const allowedCount = (n: number, rate: number) => Math.ceil(n * rate - 1e-9);

/** The ratchet: what `content.test.ts` asserts. Returns the violations (empty = pass). */
export function violations(r: { overall: Stats; units: Stats[] }): Violation[] {
  const out: Violation[] = [];
  const rate = (n: number, d: number) => (d ? n / d : 0);
  for (const u of r.units) {
    const v = rate(u.longest, u.singles);
    const limit = v > LIMITS.unitLongestHard ? LIMITS.unitLongestHard : LIMITS.unitLongest;
    if (u.longest > allowedCount(u.singles, LIMITS.unitLongest) || v > LIMITS.unitLongestHard)
      out.push({ scope: `unit ${u.unit}`, what: 'the correct option is the strictly longest', value: v, limit, offenders: u.offenders });
  }
  const o = r.overall;
  if (rate(o.longest, o.singles) > LIMITS.overallLongest)
    out.push({ scope: 'all units', what: 'the correct option is the strictly longest', value: rate(o.longest, o.singles), limit: LIMITS.overallLongest, offenders: o.offenders });
  if (rate(o.top2, o.singles) > LIMITS.overallTop2)
    out.push({ scope: 'all units', what: 'the correct option is among the two longest', value: rate(o.top2, o.singles), limit: LIMITS.overallTop2, offenders: [] });
  if (o.corr > LIMITS.overallCorr)
    out.push({ scope: 'all units', what: 'option length correlates with correctness (mean point-biserial r)', value: o.corr, limit: LIMITS.overallCorr, offenders: [] });
  if (rate(o.multiTopK, o.multis) > LIMITS.multiTopK)
    out.push({ scope: 'multi questions', what: 'the correct set is exactly the longest options', value: rate(o.multiTopK, o.multis), limit: LIMITS.multiTopK, offenders: [] });
  return out;
}

export function describeViolations(v: Violation[]): string {
  const fmt = (x: Violation, n: number) => (x.what.includes('correlates') ? n.toFixed(2) : `${(n * 100).toFixed(0)}%`);
  return v
    .map(
      (x) =>
        `${x.scope}: ${x.what} at ${fmt(x, x.value)} (limit ${fmt(x, x.limit)}).` +
        (x.offenders.length
          ? ` Shorten the correct option (move detail into the explanation) or make the distractors as long and as specific as the answer, in:\n    ${x.offenders.join('\n    ')}`
          : ' Make wrong options as detailed as the right one; see docs/content-guide.md, "Balanced options".'),
    )
    .join('\n');
}

const pct = (n: number, d: number) => (d ? `${((n / d) * 100).toFixed(0)}%`.padStart(4) : '   -');

export function report(r: { overall: Stats; units: Stats[] }): string {
  const head = 'unit'.padEnd(20) + ['single', 'longest', 'top2', 'corr', 'abs A/D', 'only-hedged', 'most-specific', 'only-overlap', 'no-overlap A/D', 'len>2x', 'multi top-k'].map((h) => h.padStart(15)).join('');
  const row = (s: Stats) =>
    s.unit.padEnd(20) +
    [
      String(s.singles),
      pct(s.longest, s.singles),
      pct(s.top2, s.singles),
      s.corr.toFixed(2),
      `${pct(s.absAnswers, s.singles).trim()}/${pct(s.absDistractors, s.distractors).trim()}`,
      pct(s.onlyHedged, s.singles),
      pct(s.mostSpecific, s.singles),
      pct(s.onlyOverlap, s.singles),
      `${pct(s.noOverlapAnswers, s.singles).trim()}/${pct(s.noOverlapDistractors, s.distractors).trim()}`,
      pct(s.ratioOver2, s.singles),
      `${s.multiTopK}/${s.multis}`,
    ]
      .map((c) => c.padStart(15))
      .join('');
  return [head, ...r.units.map(row), row(r.overall)].join('\n');
}

const isMain = process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const course = loadCourse(resolve(dirname(fileURLToPath(import.meta.url)), '../content'));
  const r = lint(course);
  console.log(report(r));
  console.log(
    `\nchance for 4 options: longest 25%, top2 50%, corr 0. limits: unit longest <= ${LIMITS.unitLongest * 100}% (rounded up to a whole question, never above ${LIMITS.unitLongestHard * 100}%), overall longest <= ${LIMITS.overallLongest * 100}%, top2 <= ${LIMITS.overallTop2 * 100}%, corr <= ${LIMITS.overallCorr}, multi top-k <= ${LIMITS.multiTopK * 100}%.`,
  );
  const v = violations(r);
  if (process.argv.includes('--list')) console.log(`\nAnswer is the strictly longest option:\n  ${r.overall.offenders.join('\n  ')}`);
  console.log(v.length ? `\nRATCHET FAILS\n${describeViolations(v)}` : '\nratchet OK');
  if (v.length) process.exitCode = 1;
}
