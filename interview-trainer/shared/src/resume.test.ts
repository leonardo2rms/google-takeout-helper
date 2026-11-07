import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { loadCourse } from './load-content';
import {
  RESUME_MAX_AGE_MS, RESUME_VERSION, isResumeKind, readResume, resumeProgress, serializeResume, toResume,
  type ResumeMeta, type ResumeRecord,
} from './resume';
import { allQuestions, type Question } from './schema';
import { createSession, submit } from './session';

const course = loadCourse(resolve(dirname(fileURLToPath(import.meta.url)), '../../content'));
const all = allQuestions(course);
const byId = new Map(all.map((q) => [q.id, q] as const));
const lesson = course.units[0]!.lessons[0]!;
const NOW = Date.UTC(2026, 10, 5, 12, 0, 0);
const meta: ResumeMeta = { route: `/lesson/${lesson.id}`, title: '1.1 Null safety', kind: 'lesson', lessonId: lesson.id, servedOn: '2026-11-05' };

/** A session in which the first `n` questions were answered: the first wrongly, the rest right on the first try. */
function played(n: number, opts: { hint?: boolean; sure?: 1 | 2 | 3 } = {}) {
  let s = createSession(lesson.questions);
  for (let i = 0; i < n; i++) s = submit(s, i !== 0, { hinted: Boolean(opts.hint) && i === 1, confidence: i === 1 ? opts.sure : undefined });
  return s;
}
const roundTrip = (r: ResumeRecord | null, now = NOW) => readResume(r ? serializeResume(r) : null, byId, now);

describe('toResume', () => {
  it('has nothing to keep before the first answer, and after the last', () => {
    expect(toResume(createSession(lesson.questions), meta, NOW)).toBeNull();
    let s = createSession(lesson.questions.slice(0, 3));
    for (let i = 0; i < 3; i++) s = submit(s, true);
    expect(s.queue).toHaveLength(0);
    expect(toResume(s, meta, NOW)).toBeNull();
  });

  it('keeps the question ids with what each asks, the progress, and where the session came from', () => {
    const s = played(3);
    const r = toResume(s, meta, NOW)!;
    expect(r).toMatchObject({ v: RESUME_VERSION, ...meta, savedAt: NOW });
    expect(r.queue.map((q) => q.id)).toEqual(s.queue.map((q) => q.id));
    expect(r.queue.every((q) => /^[0-9a-f]+$/.test(q.h))).toBe(true);
    const { queue: _queue, ...rest } = s;
    expect(r.state).toEqual(rest);
    expect(resumeProgress(r)).toEqual({ done: s.resolved, total: s.total });
  });
});

describe('readResume: a saved session comes back exactly', () => {
  it.each([1, 2, 5])('after %i answered questions (a miss that will be retried included)', (n) => {
    const s = played(n, { hint: true, sure: 3 });
    const back = roundTrip(toResume(s, meta, NOW))!;
    expect(back).not.toBeNull();
    expect(back.session).toEqual(s);
    expect(back.record).toMatchObject(meta);
    // the questions are the current definitions from the course, not copies from storage
    expect(back.session.queue.every((q, i) => q === byId.get(s.queue[i]!.id))).toBe(true);
  });

  it('keeps XP, retries, misses, hints and ratings, so the summary is the one the learner would have had', () => {
    const s = played(4, { hint: true, sure: 2 });
    expect(s.missed.length).toBeGreaterThan(0);
    expect(s.hinted.length).toBeGreaterThan(0);
    expect(Object.keys(s.confidence).length).toBeGreaterThan(0);
    const back = roundTrip(toResume(s, meta, NOW))!.session;
    expect(back.xp).toBe(s.xp);
    expect(back.attempts).toEqual(s.attempts);
    expect(back.missed).toEqual(s.missed);
    expect(back.hinted).toEqual(s.hinted);
    expect(back.confidence).toEqual(s.confidence);
  });

  it('works for every kind of session', () => {
    for (const kind of ['lesson', 'review', 'daily', 'mix', 'warmup', 'topic'] as const) {
      const m: ResumeMeta = { ...meta, kind, route: kind === 'lesson' ? meta.route : `/${kind}`, lessonId: kind === 'lesson' ? lesson.id : undefined };
      const r = roundTrip(toResume(played(2), m, NOW));
      expect(r?.record.kind, kind).toBe(kind);
      expect(r?.record.route).toBe(m.route);
      expect(isResumeKind(kind)).toBe(true);
    }
    expect(isResumeKind('one')).toBe(false);
    expect(isResumeKind('challenge')).toBe(false);
  });
});

describe('readResume: a record that is not valid for this course or this moment is dropped, never half-used', () => {
  const good = () => JSON.parse(serializeResume(toResume(played(3), meta, NOW)!)) as Record<string, any>;
  const read = (r: unknown, now = NOW) => readResume(JSON.stringify(r), byId, now);

  it('nothing stored, text that is not JSON, or JSON that is not a record', () => {
    expect(readResume(null, byId, NOW)).toBeNull();
    for (const t of ['', 'not json', '{', 'null', '[]', '"x"', '42', '{}']) expect(readResume(t, byId, NOW), t).toBeNull();
  });

  it('is the control: the good record reads', () => {
    expect(read(good())).not.toBeNull();
  });

  it('a record written by another version', () => {
    expect(read({ ...good(), v: 2 })).toBeNull();
    expect(read({ ...good(), v: undefined })).toBeNull();
  });

  it('too old, or saved by a clock far in the future', () => {
    expect(read(good(), NOW + RESUME_MAX_AGE_MS)).not.toBeNull();
    expect(read(good(), NOW + RESUME_MAX_AGE_MS + 1)).toBeNull();
    expect(read(good(), NOW - 25 * 60 * 60 * 1000)).toBeNull();
  });

  it('a question that no longer exists, or that now asks something else', () => {
    const gone = good();
    gone.queue[0].id = 'no-such-lesson/1';
    expect(read(gone)).toBeNull();
    const reworded = good();
    reworded.queue[0].h = 'deadbeef';
    expect(read(reworded)).toBeNull();
    // the same position with a different prompt is the real-world case: simulate it with a changed course
    const changed = new Map(byId);
    const q = good().queue[0].id as string;
    changed.set(q, { ...byId.get(q)!, prompt: `${byId.get(q)!.prompt} (rewritten)` } as Question);
    expect(readResume(JSON.stringify(good()), changed, NOW)).toBeNull();
  });

  it.each([
    ['route', { route: 'lesson/x' }], ['route', { route: 42 }], ['title', { title: '' }], ['kind', { kind: 'one' }], ['kind', { kind: 'challenge' }],
    ['lessonId', { lessonId: '' }], ['servedOn', { servedOn: '5 Nov' }], ['savedAt', { savedAt: 'now' }], ['queue', { queue: [] }], ['queue', { queue: 'x' }],
    ['queue item', { queue: [{ id: 5, h: 'a' }] }], ['state', { state: null }], ['state', { state: [] }],
  ])('a malformed %s', (_what, patch) => {
    expect(read({ ...good(), ...patch })).toBeNull();
  });

  it.each([
    ['total 0', { total: 0 }], ['total huge', { total: 9999 }], ['total fractional', { total: 7.5 }],
    ['resolved above total', { resolved: 99 }], ['resolved negative', { resolved: -1 }],
    ['first-try above resolved', { firstTryCorrect: 9 }],
    ['xp above what the lesson can give', { xp: 100000 }], ['xp negative', { xp: -5 }], ['xp NaN as text', { xp: 'NaN' }],
    ['no attempts', { attempts: {} }], ['attempt count 0', { attempts: { 'x/1': 0 } }], ['attempt count beyond the retries', { attempts: { 'x/1': 9 } }],
    ['attempts is a list', { attempts: [] }], ['missed is not ids', { missed: [1] }], ['hinted is a string', { hinted: 'a' }],
    ['correct too long', { correct: Array.from({ length: 61 }, (_, i) => `q/${i}`) }],
    ['bad rating', { confidence: { 'x/1': 7 } }], ['ratings as a list', { confidence: [] }],
  ])('a state with %s', (_what, patch) => {
    const g = good();
    expect(read({ ...g, state: { ...g.state, ...patch } })).toBeNull();
  });

  it('keys that would reach the prototype are refused', () => {
    const g = good();
    const evil = JSON.parse(JSON.stringify(g).replace('"attempts":{', '"attempts":{"__proto__":1,'));
    expect(Object.keys(evil.state.attempts)).toContain('__proto__');
    expect(read(evil)).toBeNull();
    const evil2 = JSON.parse(JSON.stringify({ ...g, state: { ...g.state, confidence: {} } }).replace('"confidence":{', '"confidence":{"__proto__":2'));
    expect(read(evil2)).toBeNull();
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
  });

  it('unknown extra fields are not carried over', () => {
    const g = { ...good(), extra: 'x', state: { ...good().state, sneaky: 1 } };
    const back = read(g)!;
    expect(back).not.toBeNull();
    expect(Object.keys(back.record)).not.toContain('extra');
    expect(Object.keys(back.session)).not.toContain('sneaky');
  });
});

describe('readResume: whatever is in storage, it never throws and never builds an inconsistent session', () => {
  it('survives arbitrary JSON, and arbitrary corruptions of a real record', () => {
    const real = JSON.parse(serializeResume(toResume(played(3), meta, NOW)!));
    const corrupt = fc.tuple(fc.constantFrom('v', 'route', 'title', 'kind', 'servedOn', 'savedAt', 'queue', 'state'), fc.jsonValue()).map(([k, v]) => ({ ...real, [k as string]: v }));
    fc.assert(
      fc.property(fc.oneof(fc.jsonValue(), corrupt), (value) => {
        const back = readResume(JSON.stringify(value), byId, NOW);
        if (back) {
          expect(back.session.queue.length).toBeGreaterThan(0);
          expect(back.session.resolved).toBeLessThanOrEqual(back.session.total);
          expect(back.session.queue.every((q) => byId.get(q.id) === q)).toBe(true);
        }
      }),
      { numRuns: 400 },
    );
  });

  it('survives arbitrary text', () => {
    fc.assert(fc.property(fc.string(), (text) => { readResume(text, byId, NOW); }), { numRuns: 300 });
  });
});
