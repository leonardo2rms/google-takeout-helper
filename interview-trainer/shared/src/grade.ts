import type { EstimateQ, FillQ, MatchQ, MultiQ, OpenQ, OrderQ, SingleQ, SpotQ } from './schema';

/** An open answer scoring at least this much counts as "correct" for lesson flow. */
export const PASS_SCORE = 0.6;

export type OpenVerdict = 'correct' | 'partial' | 'incorrect';

export function verdictFor(score: number): OpenVerdict {
  if (score >= PASS_SCORE) return 'correct';
  if (score >= 0.34) return 'partial';
  return 'incorrect';
}

export const gradeSingle = (q: SingleQ, selected: string | null): boolean => selected === q.answer;

export function gradeMulti(q: MultiQ, selected: readonly string[]): boolean {
  const a = new Set(q.answers);
  return selected.length === a.size && selected.every((s) => a.has(s));
}

export function gradeFill(q: FillQ, filled: readonly (string | null)[]): boolean {
  return q.answers.length === filled.length && q.answers.every((a, i) => filled[i] === a);
}

export function gradeOrder(q: OrderQ, arranged: readonly string[]): boolean {
  return q.lines.length === arranged.length && q.lines.every((l, i) => arranged[i] === l);
}

export function gradeSpot(q: SpotQ, selected: readonly number[]): boolean {
  const a = new Set(q.answers);
  return selected.length === a.size && selected.every((n) => a.has(n));
}

/**
 * Parses what a person types as a number: "50", "1,234.5", "1.234,5", "1 234", "18,2", "18.2%", "5k", "2M", "−5".
 * A lone comma followed by exactly three digits groups ("1,234" = 1234); any other lone comma is a decimal. Dots that
 * group thousands only count as grouping when there are several ("12.345.678"); a single "1.234" is a decimal.
 * Full-width digits and the typographic minus are read like their ASCII forms.
 */
export function parseNumber(raw: string): number | null {
  let s = raw.normalize('NFKC').trim().toLowerCase().replace(/[\s_]/g, '').replace(/[\u2212\u2013]/g, '-');
  if (s.endsWith('%')) s = s.slice(0, -1);
  let mult = 1;
  if (s.endsWith('k')) mult = 1000;
  else if (s.endsWith('m')) mult = 1_000_000;
  if (mult !== 1) s = s.slice(0, -1);
  if (s.includes(',') && s.includes('.')) {
    // Both separators: the one that comes last is the decimal point ("1,234.5" and "1.234,5" are both 1234.5).
    s = s.lastIndexOf(',') > s.lastIndexOf('.') ? s.replace(/\./g, '').replace(',', '.') : s.replace(/,/g, '');
  } else if (/^\d{1,3}(,\d{3})+$/.test(s)) s = s.replace(/,/g, '');
  else if (/^\d{1,3}(\.\d{3}){2,}$/.test(s)) s = s.replace(/\./g, '');
  else s = s.replace(',', '.');
  if (!/^[-+]?(\d+\.?\d*|\.\d+)$/.test(s)) return null;
  const n = Number(s) * mult;
  return Number.isFinite(n) ? n : null;
}

export function gradeEstimate(q: EstimateQ, value: number | null): boolean {
  return value !== null && Math.abs(value - q.answer) <= Math.abs(q.answer) * q.tolerance + 1e-9;
}

/** `chosen[i]` is the right-hand text the learner connected to `pairs[i].left`. */
export function gradeMatch(q: MatchQ, chosen: readonly (string | null)[]): boolean {
  return q.pairs.length === chosen.length && q.pairs.every((p, i) => chosen[i] === p.right);
}

const norm = (s: string) => s.toLowerCase().replace(/[`"'’]/g, '').replace(/\s+/g, ' ');

/**
 * Offline fallback for open questions: a key point counts as covered when any of its
 * keywords appears in the answer. Deliberately generous; it only *pre-ticks* a
 * self-assessment checklist, the learner has the final say when AI grading isn't available.
 */
export function heuristicCoverage(q: OpenQ, answer: string): boolean[] {
  const text = norm(answer);
  return q.keyPoints.map((kp) => kp.keywords.some((k) => text.includes(norm(k))));
}

export function scoreFromCoverage(covered: readonly boolean[]): number {
  if (covered.length === 0) return 0;
  return covered.filter(Boolean).length / covered.length;
}

/** Deterministic shuffle (mulberry32) so a question's arrangement is stable per attempt. */
export function seededShuffle<T>(items: readonly T[], seed: number): T[] {
  const out = [...items];
  let s = seed >>> 0;
  const rand = () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [out[i], out[j]] = [out[j]!, out[i]!];
  }
  return out;
}

/** Shuffle that is guaranteed to differ from the input order (so "order" isn't pre-solved). */
export function shuffleDifferent<T>(items: readonly T[], seed: number): T[] {
  for (let k = 0; k < 8; k++) {
    const out = seededShuffle(items, seed + k * 7919);
    if (out.some((x, i) => x !== items[i])) return out;
  }
  return [...items].reverse();
}
