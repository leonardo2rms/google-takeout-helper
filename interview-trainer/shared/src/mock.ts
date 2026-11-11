import { dayNumber, type Day } from './day';
import { interleave } from './daily';
import { MAX_MOCKS, type MockRecord } from './extras';
import { PASS_SCORE } from './grade';
import { topicStats, unitOfFor } from './mastery';
import type { Progress, QuestionId } from './progress';
import { mulberry32 } from './rng';
import { allQuestions, type Course, type Question } from './schema';
import { applySession, type SessionEvent } from './events';
import { cardFor, strength, type Outcome } from './srs';
import { roleWeights } from './track';

// ---------- setup ----------

export const MOCK_LENGTHS = [10, 20] as const;
export type MockLength = (typeof MOCK_LENGTHS)[number];
/** Time limits offered per length (minutes). The first is the default. */
export const MOCK_MINUTES: Record<MockLength, readonly number[]> = { 10: [15, 10, 20], 20: [30, 20, 45] };
export const defaultMinutes = (length: MockLength): number => MOCK_MINUTES[length][0]!;

export interface MockConfig {
  length: MockLength;
  minutes: number;
  /** Restrict the mock to one topic (unit id), or null for the whole course. */
  unitId: string | null;
}

/** A mock never changes the streak negatively and rewards a little: a few XP per answer, capped, for the first two mocks of a day. */
export const MOCK_XP_PER_ANSWER = 4;
export const MOCK_XP_CAP = 40;
export const MOCK_REWARDED_PER_DAY = 2;

// ---------- choosing the questions ----------

/** Diversity: every extra question of a type already in the mock counts this much less. */
const TYPE_DECAY = 0.55;

/**
 * Picks the mock's questions: a seeded draw without replacement, weighted towards weak topics and weak questions
 * (like Mixed practice) and slightly towards the role's focus units, with a penalty for formats already chosen so the mix stays varied, capped counts of the slow
 * formats (code, open), and finally interleaved so neighbours rarely share a topic. Deterministic for a given seed.
 */
export function buildMock(course: Course, p: Progress, today: Day, cfg: MockConfig, seed: number): Question[] {
  const rand = mulberry32(seed);
  const unitOf = unitOfFor(course);
  const topics = new Map(topicStats(course, p, today).map((t) => [t.unitId, t.strength] as const));
  const pool = allQuestions(course).filter((q) => !cfg.unitId || unitOf(q) === cfg.unitId);
  const n = Math.min(cfg.length, pool.length);
  const caps: Partial<Record<Question['type'], number>> = { code: Math.max(1, Math.round(n / 10)), open: Math.max(1, Math.round(n / 5)) };

  const byRole = roleWeights(p.profile.role);
  const weight = (q: Question) => (1 + 2 * (1 - strength(cardFor(p, q), today))) * (1 + 1.5 * (1 - (topics.get(unitOf(q)) ?? 0))) * (byRole[unitOf(q)] ?? 1);
  const chosen: Question[] = [];
  const perType: Partial<Record<Question['type'], number>> = {};
  const remaining = [...pool];
  while (chosen.length < n && remaining.length > 0) {
    let total = 0;
    const ws = remaining.map((q) => {
      const have = perType[q.type] ?? 0;
      const cap = caps[q.type];
      const w = cap !== undefined && have >= cap ? 0 : weight(q) * TYPE_DECAY ** have;
      total += w;
      return w;
    });
    let at = 0;
    if (total > 0) {
      let r = rand() * total;
      while (at < ws.length - 1 && r >= ws[at]!) r -= ws[at++]!;
    } // else only capped formats are left: take them in order
    const [q] = remaining.splice(at, 1);
    chosen.push(q!);
    perType[q!.type] = (perType[q!.type] ?? 0) + 1;
  }
  return interleave(chosen, unitOf, seed);
}

/** A seed that changes every day and every time the learner has practised or mocked since. */
export const mockSeed = (p: Progress, today: Day): number => dayNumber(today) * 131 + p.stats.sessions * 7 + p.mocks.length * 17;

// ---------- the timer ----------

/** All times are epoch milliseconds passed in by the caller, so tests and the page clock stay in control. */
export interface MockTimer {
  startedAt: number;
  limitMs: number;
  /** Set while the app is hidden: the clock does not run then. */
  pausedAt: number | null;
  pausedMs: number;
}

export const startTimer = (now: number, minutes: number): MockTimer => ({ startedAt: now, limitMs: minutes * 60_000, pausedAt: null, pausedMs: 0 });
export const pauseTimer = (t: MockTimer, now: number): MockTimer => (t.pausedAt === null ? { ...t, pausedAt: now } : t);
export const resumeTimer = (t: MockTimer, now: number): MockTimer =>
  t.pausedAt === null ? t : { ...t, pausedAt: null, pausedMs: t.pausedMs + Math.max(0, now - t.pausedAt) };

/** Time that has really counted: a clock that jumped backwards never gives negative time. */
export function elapsedMs(t: MockTimer, now: number): number {
  const end = t.pausedAt ?? now;
  return Math.max(0, end - t.startedAt - t.pausedMs);
}
export const remainingMs = (t: MockTimer, now: number): number => Math.max(0, t.limitMs - elapsedMs(t, now));
export const isTimeUp = (t: MockTimer, now: number): boolean => elapsedMs(t, now) >= t.limitMs;

/** "12:05". Rounds up, so the display reads 0:01 until the last moment rather than 0:00 while still running. */
export function clockText(ms: number): string {
  const s = Math.max(0, Math.ceil(ms / 1000));
  const m = Math.floor(s / 60);
  return `${m}:${String(s % 60).padStart(2, '0')}`;
}

/** Whole minutes that count as a spoken milestone for screen readers (announced once each). */
export const ANNOUNCE_AT_MINUTES: readonly number[] = [5, 1];

/** The milestone to announce when the remaining time went from `before` to `after` ms, or null. */
export function timeAnnouncement(before: number, after: number): string | null {
  for (const m of ANNOUNCE_AT_MINUTES) {
    if (before > m * 60_000 && after <= m * 60_000 && after > 0) return m === 1 ? 'One minute left.' : `${m} minutes left.`;
  }
  return after <= 0 && before > 0 ? 'Time is up. Your answers are being submitted.' : null;
}

// ---------- the report ----------

export interface MockResult {
  id: QuestionId;
  unitId: string;
  /** 0..1: all-or-nothing for most types, the share of rubric points for open questions. Unanswered questions score 0. */
  score: number;
  /** An answer was given (blank questions are never recorded for review: no penalty for not reaching them). */
  answered: boolean;
  flagged: boolean;
}

export interface TopicScore {
  unitId: string;
  n: number;
  /** Summed score over the topic's questions (0..n). */
  score: number;
  /** 0..100 */
  pct: number;
  /** Below this share the topic is called out as a weak spot. */
  weak: boolean;
}

export const MOCK_WEAK_BELOW = 60;

export interface MockReport {
  total: number;
  answered: number;
  /** Questions that reached the pass mark. */
  correct: number;
  /** 0..100: the mean score over ALL questions, so unanswered ones count as zero. */
  score: number;
  byTopic: TopicScore[];
  /** Weak topic ids, weakest first. */
  weakSpots: string[];
  /** Answered and below the pass mark. */
  wrong: QuestionId[];
}

export function buildReport(results: readonly MockResult[]): MockReport {
  const topics = new Map<string, { n: number; score: number }>();
  for (const r of results) {
    const t = topics.get(r.unitId) ?? { n: 0, score: 0 };
    t.n++;
    t.score += r.score;
    topics.set(r.unitId, t);
  }
  const byTopic = [...topics.entries()]
    .map(([unitId, t]): TopicScore => {
      const pct = Math.round((t.score / t.n) * 100);
      return { unitId, n: t.n, score: Math.round(t.score * 100) / 100, pct, weak: pct < MOCK_WEAK_BELOW };
    })
    .sort((a, b) => a.pct - b.pct || (a.unitId < b.unitId ? -1 : 1));
  const total = results.length;
  const sum = results.reduce((s, r) => s + r.score, 0);
  return {
    total,
    answered: results.filter((r) => r.answered).length,
    correct: results.filter((r) => r.score >= PASS_SCORE).length,
    score: total === 0 ? 0 : Math.round((sum / total) * 100),
    byTopic,
    weakSpots: byTopic.filter((t) => t.weak).map((t) => t.unitId),
    wrong: results.filter((r) => r.answered && r.score < PASS_SCORE).map((r) => r.id),
  };
}

// ---------- saving ----------

export interface MockCredit {
  xp: number;
  /** Questions that count toward the daily goal: at most half the goal, so a mock cannot meet it alone. */
  goalCredit: number;
  rewarded: boolean;
}

/** What a mock earns. Only the first MOCK_REWARDED_PER_DAY mocks of a day earn anything, and it is capped. */
export function mockCredit(p: Progress, today: Day, answered: number): MockCredit {
  const todays = p.mocks.filter((m) => m.day === today).length;
  if (todays >= MOCK_REWARDED_PER_DAY || answered <= 0) return { xp: 0, goalCredit: 0, rewarded: false };
  return {
    xp: Math.min(MOCK_XP_CAP, MOCK_XP_PER_ANSWER * answered),
    goalCredit: Math.min(answered, Math.ceil(p.goal.target / 2)),
    rewarded: true,
  };
}

export interface MockOutcome {
  results: readonly MockResult[];
  limitMin: number;
  usedSec: number;
  timedOut: boolean;
}

/** The record kept in Progress.mocks. */
export function mockRecord(today: Day, report: MockReport, o: Pick<MockOutcome, 'limitMin' | 'usedSec' | 'timedOut'>): MockRecord {
  const byUnit: MockRecord['byUnit'] = {};
  for (const t of report.byTopic) byUnit[t.unitId] = { n: t.n, score: t.score };
  return { day: today, total: report.total, answered: report.answered, score: report.score, limitMin: o.limitMin, usedSec: o.usedSec, timedOut: o.timedOut, byUnit };
}

/**
 * Saves a finished mock: wrong answers become due cards (right ones are scheduled like any first-try answer), the mock
 * joins the history (last MAX_MOCKS), and a capped amount of XP and goal credit is granted. Questions the learner did not
 * answer are left alone. Nothing is saved for a mock with no answers at all.
 */
export function applyMock(p: Progress, o: MockOutcome, ctx: { today: Day; course: Course }): { progress: Progress; events: SessionEvent[]; report: MockReport; credit: MockCredit } {
  const report = buildReport(o.results);
  const credit = mockCredit(p, ctx.today, report.answered);
  if (report.answered === 0) return { progress: p, events: [], report, credit };

  const outcomes: Record<QuestionId, Outcome> = {};
  for (const r of o.results) if (r.answered) outcomes[r.id] = r.score >= PASS_SCORE ? 'first' : 'failed';
  const passed = Object.keys(outcomes).filter((id) => outcomes[id] === 'first');
  const failed = Object.keys(outcomes).filter((id) => outcomes[id] === 'failed');

  const res = applySession(
    p,
    {
      kind: 'mock',
      accuracy: passed.length / Math.max(1, report.answered),
      xp: credit.xp,
      missed: failed,
      recovered: [],
      cleared: passed,
      outcomes,
      answered: credit.goalCredit,
    },
    ctx,
  );
  const mocks = [mockRecord(ctx.today, report, o), ...res.progress.mocks].slice(0, MAX_MOCKS);
  return { progress: { ...res.progress, mocks }, events: res.events, report, credit };
}
