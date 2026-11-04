import { describe, expect, it } from 'vitest';
import { Course, deepEqual, followUpAsOpen, followUpId, openParts, pickFollowUp, runCodeTests, show, validateCourse, type OpenQ } from './index';

describe('deepEqual', () => {
  it('compares JSON-like values structurally', () => {
    expect(deepEqual([1, [2, { a: 3 }]], [1, [2, { a: 3 }]])).toBe(true);
    expect(deepEqual({ a: 1, b: 2 }, { b: 2, a: 1 })).toBe(true); // key order is irrelevant
    expect(deepEqual([1, 2], [1, 2, 3])).toBe(false);
    expect(deepEqual({ a: 1 }, { a: 1, b: undefined })).toBe(false);
    expect(deepEqual(null, {})).toBe(false);
    expect(deepEqual(NaN, NaN)).toBe(true);
    expect(deepEqual(0, -0)).toBe(true);
    expect(deepEqual([1], { 0: 1 })).toBe(false);
  });
});

describe('runCodeTests', () => {
  const add = 'function add(a, b) { return a + b; }';

  it('passes when every test passes', () => {
    const r = runCodeTests(add, 'add', [{ args: [1, 2], expect: 3 }, { args: [-1, 1], expect: 0 }]);
    expect(r).toMatchObject({ ok: true });
    expect(r.results.map((x) => x.passed)).toEqual([true, true]);
  });

  it('reports expected vs got for visible failures and hides the details of hidden ones', () => {
    const r = runCodeTests('function f(x) { return x + 1; }', 'f', [
      { args: [1], expect: 3 },
      { args: [1], expect: 3, hidden: true },
    ]);
    expect(r.ok).toBe(false);
    expect(r.results[0]).toMatchObject({ passed: false, call: 'f(1)', expected: '3', got: '2' });
    expect(r.results[1]).toEqual({ hidden: true, passed: false });
  });

  it('supports const arrow functions and does not let tests mutate shared inputs', () => {
    const r = runCodeTests('const f = (xs) => { xs.push(9); return xs.length; };', 'f', [
      { args: [[1, 2]], expect: 3 },
      { args: [[1, 2]], expect: 3 },
    ]);
    expect(r.ok).toBe(true);
  });

  it('checks expected errors by message, and fails when nothing is thrown', () => {
    const src = 'function f(x) { if (x < 0) throw new Error("negative not allowed"); return x; }';
    expect(runCodeTests(src, 'f', [{ args: [-1], throws: 'negative' }, { args: [-1], throws: '' }]).ok).toBe(true);
    const wrongMsg = runCodeTests(src, 'f', [{ args: [-1], throws: 'positive' }]);
    expect(wrongMsg.ok).toBe(false);
    expect(wrongMsg.results[0]!.got).toContain('negative not allowed');
    expect(runCodeTests(src, 'f', [{ args: [1], throws: '' }]).results[0]).toMatchObject({ passed: false, note: 'expected an error to be thrown' });
  });

  it('reports unexpected exceptions on the failing test', () => {
    const r = runCodeTests('function f() { return null.x; }', 'f', [{ args: [], expect: 1 }]);
    expect(r.results[0]!.passed).toBe(false);
    expect(r.results[0]!.note).toMatch(/^threw /);
  });

  it('reports syntax errors and a missing function as fatal, without running tests', () => {
    expect(runCodeTests('function f( {', 'f', [{ args: [], expect: 1 }])).toMatchObject({ ok: false, fatal: expect.stringMatching(/^Syntax error/) });
    expect(runCodeTests('function other() {}', 'f', [{ args: [], expect: 1 }])).toMatchObject({ ok: false, fatal: 'Define a function named f.' });
    expect(runCodeTests('var f = 3;', 'f', [{ args: [], expect: 1 }]).fatal).toMatch(/Define a function/);
  });

  it('builds large inputs from a js expression', () => {
    const r = runCodeTests('function len(xs) { return xs.length; }', 'len', [
      { args: { js: '[Array.from({ length: 100000 }, (_, i) => i)]' }, expect: 100000, hidden: true },
    ]);
    expect(r.ok).toBe(true);
  });

  it('runs in strict mode so accidental globals are caught', () => {
    const r = runCodeTests('function f() { leaked = 1; return leaked; }', 'f', [{ args: [], expect: 1 }]);
    expect(r.results[0]!.passed).toBe(false);
  });

  it('formats values for display', () => {
    expect(show(undefined)).toBe('undefined');
    expect(show([1, 'a'])).toBe('[1,"a"]');
    expect(show(Infinity)).toBe('Infinity');
    expect(show('x'.repeat(300)).length).toBeLessThanOrEqual(140);
  });
});

describe('interviewer follow-ups', () => {
  const kp = [{ point: 'a', keywords: ['a'] }, { point: 'b', keywords: ['b'] }];
  const q = {
    id: 'l/1', type: 'open', prompt: 'main', modelAnswer: 'm', format: 'text', maxChars: 1200, difficulty: 'medium', tags: [],
    keyPoints: [...kp, { point: 'c', keywords: ['c'] }],
    followUps: [
      { when: 'covered', point: 1, prompt: 'deeper on 1', modelAnswer: 'x', keyPoints: kp, maxChars: 1000 },
      { when: 'missed', point: 3, prompt: 'probe 3', modelAnswer: 'y', keyPoints: kp, maxChars: 1000 },
      { when: 'missed', point: 2, prompt: 'probe 2', modelAnswer: 'z', keyPoints: kp, maxChars: 1000 },
    ],
  } as unknown as OpenQ;

  it('probes a gap first, in authoring order', () => {
    expect(pickFollowUp(q, [true, false, false])?.followUp.prompt).toBe('probe 3');
    expect(pickFollowUp(q, [true, false, true])?.followUp.prompt).toBe('probe 2');
  });
  it('goes deeper on a covered point when nothing was missed', () => {
    expect(pickFollowUp(q, [true, true, true])?.followUp.prompt).toBe('deeper on 1');
  });
  it('returns nothing when no follow-up applies', () => {
    expect(pickFollowUp({ ...q, followUps: [q.followUps[0]!] } as OpenQ, [false, true, true])).toBeNull();
    expect(pickFollowUp({ ...q, followUps: [] } as OpenQ, [true, true, true])).toBeNull();
  });
  it('turns follow-ups into gradable open questions with their own ids', () => {
    const f = followUpAsOpen(q, 1);
    expect(f).toMatchObject({ id: 'l/1~f2', prompt: 'probe 3', type: 'open', followUps: [] });
    expect(followUpId(q, 0)).toBe('l/1~f1');
    expect(openParts(q).map((p) => p.id)).toEqual(['l/1', 'l/1~f1', 'l/1~f2', 'l/1~f3']);
  });
});

describe('validation of code questions and follow-ups', () => {
  const wrap = (q: object) =>
    Course.parse({ units: [{ id: 'u', title: 'U', emoji: 'x', description: 'd', lessons: [{ id: 'l', title: 'L', questions: [q, q, q] }] }] });
  const base = { prompt: 'p', explanation: 'e' };
  const code = (over: object = {}) => ({
    ...base, type: 'code', entry: 'f', starter: 'function f() {}', solution: 'function f() { return 1; }',
    tests: [{ args: [], expect: 1 }, { args: [], expect: 1, hidden: true }], ...over,
  });

  it('accepts a well-formed code question', () => expect(validateCourse(wrap(code()))).toEqual([]));
  it('requires a visible test and the entry function in starter and solution', () => {
    expect(validateCourse(wrap(code({ tests: [{ args: [], expect: 1, hidden: true }, { args: [], expect: 1, hidden: true }] }))).join()).toMatch(/visible test/);
    expect(validateCourse(wrap(code({ starter: 'function g() {}' }))).join()).toMatch(/must define `f`/);
  });
  it('rejects a test with neither expect nor throws', () => {
    expect(() => wrap(code({ tests: [{ args: [] }, { args: [], expect: 1 }] }))).toThrow();
  });
  it('rejects follow-ups pointing past the rubric', () => {
    const open = {
      ...base, type: 'open', modelAnswer: 'm', keyPoints: [{ point: 'a', keywords: ['a'] }, { point: 'b', keywords: ['b'] }],
      followUps: [{ when: 'missed', point: 5, prompt: 'p', modelAnswer: 'm', keyPoints: [{ point: 'a', keywords: ['a'] }, { point: 'b', keywords: ['b'] }] }],
    };
    expect(validateCourse(wrap(open)).join()).toMatch(/key point 5/);
  });
});
