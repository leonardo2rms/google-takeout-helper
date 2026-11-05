/**
 * Test-wiseness lint for `single` and `multi` questions.
 *
 * A learner who can pass by "pick the longest / most detailed / least absolute option" is being tested on
 * test-taking, not on the material, and the readiness numbers become dishonest. This script measures the
 * tells, per unit and overall: length, and style (asides, "or", causal clauses, absolute words, plus two fixed
 * attackers). `content/balance.test.ts` turns the headline numbers into a ratchet.
 *
 *   npm run content:lint            report only (exit 0)
 *   npm run content:lint -- --list  also list every question where the answer is the strictly longest option
 *   npm run content:lint -- --offenders  list questions where one option's style differs from the rest
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

/* ------------------------------------------------------------------------------------------------------------ *
 * Style tells: surface features that are not length. Each feature is tested on the prose of an option (code spans
 * removed). The attackers below are fixed in advance and never tuned on the content: a learner who knows nothing
 * should not beat chance by much with any of them.
 * ------------------------------------------------------------------------------------------------------------ */

/** The features the round-2 audit found (N-S1-2), plus absolute words. */
export const FEATURES = {
  marker: { label: '( ; or :', re: /[(;:]/ },
  or: { label: 'the word "or"', re: /\bor\b/i },
  causal: { label: 'a causal clause (because, since, so that, which)', re: /\b(because|since|so that|which)\b/i },
  absolute: { label: 'an absolute word (always, never, only, all, ...)', re: ABSOLUTES },
} as const;
export type Feature = keyof typeof FEATURES;
export const FEATURE_NAMES = Object.keys(FEATURES) as Feature[];

export const has = (f: Feature, o: Opt) => FEATURES[f].re.test(prose(o.text));

/** Style limits. Chance is 25% for a four-option single. See docs/content-guide.md, "Balanced options". */
export const STYLE_LIMITS = {
  /** a feature is only judged once it appears on at least this many options of that kind */
  minSupport: 15,
  /** rate among correct options / rate among distractors; [0.55, 1.6] is P(correct | feature) within 25% +- 10 points */
  liftLow: 0.55,
  liftHigh: 1.6,
  /** expected accuracy of "prefer options with the feature" / "avoid them", random among the rest */
  rule: 0.35,
  /** the audit's fixed combined rule */
  combined: 0.35,
  /** leave-one-question-out bag-of-words classifier */
  bow: 0.4,
} as const;

export type FeatureStat = {
  feature: Feature;
  /** options carrying the feature, among correct options and distractors */
  answers: number;
  answerTotal: number;
  distractors: number;
  distractorTotal: number;
  /** rate among correct / rate among distractors (NaN when unsupported) */
  lift: number;
  /** singles only: P(correct | option has the feature) */
  pCorrect: number;
  /** singles only: expected accuracy of "pick among options with the feature (else any)" and "pick among options without it" */
  prefer: number;
  avoid: number;
};

export type StyleStats = {
  unit: string;
  singles: number;
  multis: number;
  /** per feature, singles and multis measured separately (different base rates) */
  single: FeatureStat[];
  multi: FeatureStat[];
  /** expected accuracy of the fixed combined rule on singles */
  combined: number;
  /** expected accuracy of the leave-one-question-out bag-of-words classifier on singles (computed over the whole course) */
  bow: number;
  /** singles whose answer is the strictly shortest option (chance 25%) */
  shortest: number;
};

/** Expected accuracy of choosing uniformly among `candidates` (all options when empty). */
function pickAmong(c: Choice, candidates: Opt[]): number {
  const pool = candidates.length ? candidates : c.options;
  return pool.some((o) => c.correct.has(o.id)) ? 1 / pool.length : 0;
}

/** Expected accuracy of choosing uniformly among the options with the highest score. */
function argmaxAccuracy(c: Choice, score: (o: Opt) => number): number {
  const scores = c.options.map(score);
  const best = Math.max(...scores);
  return pickAmong(c, c.options.filter((_, i) => scores[i]! >= best - 1e-9));
}

function featureStat(cs: Choice[], f: Feature): FeatureStat {
  let answers = 0, answerTotal = 0, distractors = 0, distractorTotal = 0, prefer = 0, avoid = 0;
  for (const c of cs) {
    for (const o of c.options) {
      if (c.correct.has(o.id)) { answerTotal++; if (has(f, o)) answers++; }
      else { distractorTotal++; if (has(f, o)) distractors++; }
    }
    prefer += pickAmong(c, c.options.filter((o) => has(f, o)));
    avoid += pickAmong(c, c.options.filter((o) => !has(f, o)));
  }
  const ra = answerTotal ? answers / answerTotal : 0;
  const rd = distractorTotal ? distractors / distractorTotal : 0;
  const lift = answers + distractors < STYLE_LIMITS.minSupport ? NaN : rd === 0 ? Infinity : ra / rd;
  return { feature: f, answers, answerTotal, distractors, distractorTotal, lift, pCorrect: answers + distractors ? answers / (answers + distractors) : NaN, prefer: cs.length ? prefer / cs.length : 0, avoid: cs.length ? avoid / cs.length : 0 };
}

/** The round-2 audit's rule, weights fixed before measuring: markers and "or" up, causal, absolute and shortest down. */
export function combinedScore(c: Choice, o: Opt): number {
  const shortest = Math.min(...c.options.map(len));
  return 2 * +has('marker', o) + 2 * +has('or', o) - 1.5 * +has('causal', o) - 1 * +has('absolute', o) - 1 * +(len(o) === shortest);
}

/** Tokens a bag-of-words attacker sees: lower-case words (stop words included: "or" and "because" are the tells) and ( ; : marks. */
export const tokens = (t: string): Set<string> => new Set(t.toLowerCase().match(/[a-z]+|[(;:]/g) ?? []);

/**
 * Leave-one-question-out Bernoulli naive Bayes on option tokens: trained on every other single, it picks the option
 * whose tokens look most like correct options. Returns the expected accuracy per question id.
 */
export function bagOfWords(singles: Choice[]): Map<string, number> {
  const toks = singles.map((c) => c.options.map((o) => ({ o, t: tokens(o.text), ok: c.correct.has(o.id) })));
  const inC = new Map<string, number>(), inD = new Map<string, number>();
  let nC = 0, nD = 0;
  for (const q of toks) for (const x of q) {
    const m = x.ok ? inC : inD;
    if (x.ok) nC++; else nD++;
    for (const w of x.t) m.set(w, (m.get(w) ?? 0) + 1);
  }
  const out = new Map<string, number>();
  singles.forEach((c, i) => {
    // remove this question's own options from the counts
    const own = toks[i]!;
    const dC = new Map<string, number>(), dD = new Map<string, number>();
    let oC = 0, oD = 0;
    for (const x of own) { if (x.ok) oC++; else oD++; for (const w of x.t) (x.ok ? dC : dD).set(w, ((x.ok ? dC : dD).get(w) ?? 0) + 1); }
    const NC = nC - oC, ND = nD - oD;
    const score = (o: Opt) => {
      let s = 0;
      for (const w of tokens(o.text)) {
        const cw = (inC.get(w) ?? 0) - (dC.get(w) ?? 0), dw = (inD.get(w) ?? 0) - (dD.get(w) ?? 0);
        s += Math.log((cw + 1) / (NC + 2)) - Math.log((dw + 1) / (ND + 2));
      }
      return s;
    };
    out.set(c.id, argmaxAccuracy(c, score));
  });
  return out;
}

export function measureStyle(all: Choice[], unit: string, bow: Map<string, number>): StyleStats {
  const cs = all.filter((c) => unit === 'ALL' || c.unit === unit);
  const singles = cs.filter((c) => c.kind === 'single');
  const multis = cs.filter((c) => c.kind === 'multi');
  const avg = (xs: number[]) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : 0);
  return {
    unit,
    singles: singles.length,
    multis: multis.length,
    single: FEATURE_NAMES.map((f) => featureStat(singles, f)),
    multi: FEATURE_NAMES.map((f) => featureStat(multis, f)),
    combined: avg(singles.map((c) => argmaxAccuracy(c, (o) => combinedScore(c, o)))),
    bow: avg(singles.map((c) => bow.get(c.id) ?? 0)),
    shortest: singles.filter((c) => uniqueMax(c.options, (o) => -len(o)) === c.options.find((o) => c.correct.has(o.id))).length,
  };
}

export function lintStyle(course: Course): { overall: StyleStats; units: StyleStats[] } {
  const all = choices(course);
  const bow = bagOfWords(all.filter((c) => c.kind === 'single'));
  return { overall: measureStyle(all, 'ALL', bow), units: course.units.map((u) => measureStyle(all, u.id, bow)) };
}

/** Questions where a feature points at the answer (or away from it) uniquely: the ones to rewrite first. */
export function styleOffenders(course: Course, f: Feature): string[] {
  return choices(course)
    .filter((c) => c.kind === 'single')
    .filter((c) => {
      const marked = c.options.filter((o) => has(f, o));
      return marked.length === 1 || marked.length === c.options.length - 1;
    })
    .map((c) => c.id);
}

const STYLE_HINT =
  ' Write correct options and distractors in the same style: the same share of asides in ( ), ";" and ":", of "X or Y", and of "because" clauses. `npm run content:lint -- --offenders` lists the questions where one option differs from the rest; see docs/content-guide.md, "Balanced options".';

/** The style ratchet, on the whole course (units are too small for per-feature rates to mean much). */
export function styleViolations(r: { overall: StyleStats }): Violation[] {
  const out: Violation[] = [];
  const o = r.overall;
  const L = STYLE_LIMITS;
  for (const [kind, stats] of [['single', o.single], ['multi', o.multi]] as const)
    for (const s of stats) {
      if (Number.isNaN(s.lift)) continue;
      if (s.lift > L.liftHigh) out.push({ scope: `${kind} questions`, what: `options with ${FEATURES[s.feature].label} are more often correct (lift)`, value: s.lift, limit: L.liftHigh, offenders: [], hint: STYLE_HINT });
      if (s.lift < L.liftLow) out.push({ scope: `${kind} questions`, what: `options with ${FEATURES[s.feature].label} are more often wrong (lift)`, value: s.lift, limit: L.liftLow, offenders: [], hint: STYLE_HINT });
    }
  for (const s of o.single) {
    if (s.prefer > L.rule) out.push({ scope: 'single questions', what: `"pick the option with ${FEATURES[s.feature].label}" scores`, value: s.prefer, limit: L.rule, offenders: [], hint: STYLE_HINT });
    if (s.avoid > L.rule) out.push({ scope: 'single questions', what: `"avoid the option with ${FEATURES[s.feature].label}" scores`, value: s.avoid, limit: L.rule, offenders: [], hint: STYLE_HINT });
  }
  if (o.combined > L.combined) out.push({ scope: 'single questions', what: 'the combined style rule scores', value: o.combined, limit: L.combined, offenders: [], hint: STYLE_HINT });
  if (o.bow > L.bow) out.push({ scope: 'single questions', what: 'a bag-of-words classifier (leave one question out) scores', value: o.bow, limit: L.bow, offenders: [], hint: STYLE_HINT });
  return out;
}

export function styleReport(r: { overall: StyleStats; units: StyleStats[] }): string {
  const p = (x: number) => (Number.isFinite(x) ? `${(x * 100).toFixed(0)}%` : '-');
  const lift = (x: number) => (Number.isNaN(x) ? '-' : Number.isFinite(x) ? x.toFixed(2) : 'inf');
  const head = 'unit'.padEnd(20) + [...FEATURE_NAMES.map((f) => `${f} A/D`), 'combined', 'bow', 'shortest'].map((h) => h.padStart(14)).join('');
  const row = (s: StyleStats) =>
    s.unit.padEnd(20) +
    [
      ...s.single.map((f) => `${f.answers}/${f.distractors}`),
      p(s.combined),
      p(s.bow),
      p(s.singles ? s.shortest / s.singles : 0),
    ].map((c) => c.padStart(14)).join('');
  const detail = (kind: string, stats: FeatureStat[]) =>
    stats.map((s) => `  ${kind.padEnd(7)}${FEATURES[s.feature].label.padEnd(48)} correct ${String(s.answers).padStart(3)}/${s.answerTotal}  wrong ${String(s.distractors).padStart(3)}/${s.distractorTotal}  lift ${lift(s.lift).padStart(5)}` + (kind === 'single' ? `  P(correct) ${p(s.pCorrect).padStart(4)}  prefer ${p(s.prefer)}  avoid ${p(s.avoid)}` : '')).join('\n');
  return [head, ...r.units.map(row), row(r.overall), '', detail('single', r.overall.single), detail('multi', r.overall.multi)].join('\n');
}

export function lint(course: Course): { overall: Stats; units: Stats[] } {
  const all = choices(course);
  return { overall: measure(all, 'ALL'), units: course.units.map((u) => measure(all, u.id)) };
}

export type Violation = { scope: string; what: string; value: number; limit: number; offenders: string[]; hint?: string };

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
  const fmt = (x: Violation, n: number) => (x.what.includes('correlates') || x.what.includes('(lift)') ? n.toFixed(2) : `${(n * 100).toFixed(0)}%`);
  return v
    .map(
      (x) =>
        `${x.scope}: ${x.what} at ${fmt(x, x.value)} (limit ${fmt(x, x.limit)}).` +
        (x.hint ??
          (x.offenders.length
          ? ` Shorten the correct option (move detail into the explanation) or make the distractors as long and as specific as the answer, in:\n    ${x.offenders.join('\n    ')}`
          : ' Make wrong options as detailed as the right one; see docs/content-guide.md, "Balanced options".')),
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
  const st = lintStyle(course);
  console.log(`\nStyle tells (A/D = correct/wrong single options carrying the feature):\n${styleReport(st)}`);
  console.log(
    `\nstyle limits: lift in [${STYLE_LIMITS.liftLow}, ${STYLE_LIMITS.liftHigh}] once a feature is on ${STYLE_LIMITS.minSupport}+ options, prefer/avoid rule <= ${STYLE_LIMITS.rule * 100}%, combined rule <= ${STYLE_LIMITS.combined * 100}%, bag of words <= ${STYLE_LIMITS.bow * 100}% (chance 25%).`,
  );
  const v = [...violations(r), ...styleViolations(st)];
  if (process.argv.includes('--offenders'))
    for (const f of FEATURE_NAMES) console.log(`\n${FEATURES[f].label}: one option differs from the rest in\n  ${styleOffenders(course, f).join('\n  ')}`);
  if (process.argv.includes('--list')) console.log(`\nAnswer is the strictly longest option:\n  ${r.overall.offenders.join('\n  ')}`);
  console.log(v.length ? `\nRATCHET FAILS\n${describeViolations(v)}` : '\nratchet OK');
  if (v.length) process.exitCode = 1;
}
