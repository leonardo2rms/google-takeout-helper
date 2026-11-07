import { questionHash } from './hash';
import type { Confidence } from './progress';
import type { Question } from './schema';
import { MAX_RETRIES, XP_FIRST_TRY, type Session } from './session';

/**
 * A session the learner left half way, kept so they can carry on from the question they were on.
 *
 * It lives apart from `Progress` (its own storage key, see `RESUME_KEY`): it is scratch state about one unfinished
 * session, not part of the profile, so it is not exported, merged or migrated. Everything read back is checked against
 * the current course, because the course can change between visits (a question moved or reworded): a record that no
 * longer matches is dropped and the session simply starts again, it never resumes into the wrong question.
 */

export const RESUME_KEY = 'interview-trainer.resume.v1';
export const RESUME_VERSION = 1;
/** An unfinished session older than this is forgotten: what was due, or what "Today" picked, has moved on. */
export const RESUME_MAX_AGE_MS = 48 * 60 * 60 * 1000;

/** Sessions that can be resumed. One-question sessions (Library practice, the daily challenge) have nothing to resume. */
export type ResumeKind = 'lesson' | 'review' | 'daily' | 'mix' | 'warmup' | 'topic';
const KINDS: readonly ResumeKind[] = ['lesson', 'review', 'daily', 'mix', 'warmup', 'topic'];
export const isResumeKind = (k: string): k is ResumeKind => (KINDS as readonly string[]).includes(k);

/** What identifies and describes a session, apart from its progress. */
export interface ResumeMeta {
  /** The route that opens it, without `#`: `/lesson/kotlin-null-types`, `/daily`, `/topic/spring`. */
  route: string;
  title: string;
  kind: ResumeKind;
  lessonId?: string;
  /** The day the session was opened (the "day" its results count for). */
  servedOn: string;
}

export interface ResumeRecord extends ResumeMeta {
  v: typeof RESUME_VERSION;
  /** Epoch milliseconds of the last save. */
  savedAt: number;
  /** The questions still to ask, in order, each with the hash of what it asks. */
  queue: { id: string; h: string }[];
  /** Everything of the session except the queue. */
  state: Omit<Session, 'queue'>;
}

/** A record read back and matched to the current course. */
export interface Resumable {
  record: ResumeRecord;
  session: Session;
}

/** The record for a session in progress, or null when there is nothing worth resuming (nothing answered, or finished). */
export function toResume(session: Session, meta: ResumeMeta, now: number): ResumeRecord | null {
  if (session.queue.length === 0 || Object.keys(session.attempts).length === 0) return null;
  const { queue, ...state } = session;
  return { v: RESUME_VERSION, ...meta, savedAt: now, queue: queue.map((q) => ({ id: q.id, h: questionHash(q) })), state };
}

export const serializeResume = (record: ResumeRecord): string => JSON.stringify(record);

/** How far along a saved session is, for display: questions resolved out of the lesson's total. */
export const resumeProgress = (r: Pick<ResumeRecord, 'state'>): { done: number; total: number } => ({ done: r.state.resolved, total: r.state.total });

// ---------- reading back ----------

type Obj = Record<string, unknown>;
const isObj = (x: unknown): x is Obj => typeof x === 'object' && x !== null && !Array.isArray(x);
const isInt = (x: unknown, min: number, max: number): x is number => typeof x === 'number' && Number.isInteger(x) && x >= min && x <= max;
const isId = (x: unknown): x is string => typeof x === 'string' && x.length > 0 && x.length <= 120;
const idList = (x: unknown, max: number): string[] | null =>
  Array.isArray(x) && x.length <= max && x.every(isId) ? (x as string[]) : null;

const MAX_QUEUE = 60;
const MAX_TOTAL = 60;

/**
 * Parses a stored record and rebuilds the session against the current course. Returns null for anything that is not
 * a well-formed, recent record whose every question still exists and still asks the same thing.
 * `questions` maps a question id to its current definition.
 */
export function readResume(text: string | null, questions: ReadonlyMap<string, Question>, now: number): Resumable | null {
  if (text === null) return null;
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch {
    return null;
  }
  if (!isObj(raw) || raw.v !== RESUME_VERSION) return null;

  const { route, title, kind, lessonId, servedOn, savedAt } = raw;
  if (typeof route !== 'string' || !route.startsWith('/') || route.length > 200) return null;
  if (typeof title !== 'string' || title.length === 0 || title.length > 200) return null;
  if (typeof kind !== 'string' || !isResumeKind(kind)) return null;
  if (lessonId !== undefined && !isId(lessonId)) return null;
  if (typeof servedOn !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(servedOn)) return null;
  if (typeof savedAt !== 'number' || !Number.isFinite(savedAt)) return null;
  if (now - savedAt > RESUME_MAX_AGE_MS || savedAt - now > 24 * 60 * 60 * 1000) return null; // too old, or from a clock far in the future

  if (!Array.isArray(raw.queue) || raw.queue.length === 0 || raw.queue.length > MAX_QUEUE) return null;
  const queue: Question[] = [];
  for (const item of raw.queue) {
    if (!isObj(item) || !isId(item.id) || typeof item.h !== 'string') return null;
    const q = questions.get(item.id);
    if (!q || questionHash(q) !== item.h) return null; // the course changed under this record
    queue.push(q);
  }

  const st = raw.state;
  if (!isObj(st)) return null;
  const { total, resolved, firstTryCorrect, xp, attempts, confidence } = st;
  if (!isInt(total, 1, MAX_TOTAL) || !isInt(resolved, 0, total) || !isInt(firstTryCorrect, 0, resolved)) return null;
  if (!isInt(xp, 0, total * XP_FIRST_TRY)) return null;
  if (!isObj(attempts) || Object.keys(attempts).length === 0 || Object.keys(attempts).length > MAX_TOTAL) return null;
  const attemptsOk: Record<string, number> = {};
  for (const [id, n] of Object.entries(attempts)) {
    if (!isId(id) || id === '__proto__' || !isInt(n, 1, MAX_RETRIES + 1)) return null;
    attemptsOk[id] = n;
  }
  const missed = idList(st.missed, MAX_TOTAL);
  const recovered = idList(st.recovered, MAX_TOTAL);
  const correct = idList(st.correct, MAX_TOTAL);
  const hinted = idList(st.hinted, MAX_TOTAL);
  const repeats = st.repeats === undefined ? [] : idList(st.repeats, MAX_TOTAL); // saved before repeats existed: none
  if (!missed || !recovered || !correct || !hinted || !repeats) return null;
  if (!isObj(confidence)) return null;
  const confidenceOk: Record<string, Confidence> = {};
  for (const [id, c] of Object.entries(confidence)) {
    if (!isId(id) || id === '__proto__' || (c !== 1 && c !== 2 && c !== 3)) return null;
    confidenceOk[id] = c;
  }

  const state = { total, resolved, firstTryCorrect, xp, attempts: attemptsOk, missed, recovered, correct, hinted, confidence: confidenceOk, repeats };
  const record: ResumeRecord = {
    v: RESUME_VERSION, route, title, kind, ...(lessonId === undefined ? {} : { lessonId }), servedOn, savedAt,
    queue: queue.map((q) => ({ id: q.id, h: questionHash(q) })),
    state,
  };
  return { record, session: { queue, ...state } };
}
