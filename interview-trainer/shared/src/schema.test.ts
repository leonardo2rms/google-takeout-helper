/**
 * The content schema: what the zod types accept and reject for every question type, the derived ids and defaults,
 * and every cross-field rule in validateCourse (each rule has a case that triggers it, and a valid twin that does not).
 */
import { describe, expect, it } from 'vitest';
import { Course, allQuestions, blankIndexes, findLesson, spotLines, validateCourse, type Course as CourseT } from './schema';

type Q = Record<string, unknown>;

const valid: Record<string, Q> = {
  single: { type: 'single', prompt: 'p', explanation: 'e', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a' },
  multi: { type: 'multi', prompt: 'p', explanation: 'e', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }, { id: 'c', text: 'C' }], answers: ['a', 'b'] },
  fill: { type: 'fill', prompt: 'p', explanation: 'e', template: 'x[[1]]y[[2]]', bank: ['1', '2', '3'], answers: ['1', '2'] },
  order: { type: 'order', prompt: 'p', explanation: 'e', lines: ['one', 'two', 'three'] },
  spot: { type: 'spot', prompt: 'p', explanation: 'e', source: 'a\nb\nc\nd', answers: [2] },
  estimate: { type: 'estimate', prompt: 'p', explanation: 'e', answer: 50 },
  match: { type: 'match', prompt: 'p', explanation: 'e', pairs: [{ left: 'a', right: '1' }, { left: 'b', right: '2' }, { left: 'c', right: '3' }] },
  code: {
    type: 'code', prompt: 'p', explanation: 'e', starter: 'function f(x) {}', entry: 'f', solution: 'function f(x) { return x; }',
    tests: [{ args: [1], expect: 1 }, { args: [2], expect: 2 }],
  },
  open: { type: 'open', prompt: 'p', modelAnswer: 'm', keyPoints: [{ point: 'a', keywords: ['a'] }, { point: 'b', keywords: ['b'] }] },
};
const filler = (n: number): Q[] => Array.from({ length: n }, () => ({ ...valid['single']! }));

function courseWith(questions: Q[], over: { unitId?: string; lessonId?: string } = {}) {
  const pad = filler(Math.max(0, 3 - questions.length));
  return {
    units: [{ id: over.unitId ?? 'u', title: 'U', emoji: 'x', description: 'd', lessons: [{ id: over.lessonId ?? 'l', title: 'L', questions: [...questions, ...pad] }] }],
  };
}
const parse = (questions: Q[]) => Course.safeParse(courseWith(questions));
const problemsOf = (q: Q): string[] => validateCourse(Course.parse(courseWith([q])));
const withQ = (type: keyof typeof valid, over: Q): Q => ({ ...valid[type]!, ...over });

describe('every valid example parses, gets an id, and the defaults', () => {
  it.each(Object.keys(valid))('%s', (type) => {
    const res = parse([valid[type]!]);
    expect(res.success).toBe(true);
    const q = allQuestions(res.data!)[0]!;
    expect(q.id).toBe('l/1');
    expect(q.difficulty).toBe('medium');
    expect(q.tags).toEqual([]);
    expect(validateCourse(res.data!)).toEqual([]);
  });

  it('applies type-specific defaults', () => {
    const [fill, order, spot, estimate, code, open] = ['fill', 'order', 'spot', 'estimate', 'code', 'open'].map((t) => allQuestions(parse([valid[t]!]).data!)[0]!) as any[];
    expect([fill.lang, order.lang, order.kind, spot.lang]).toEqual(['kotlin', 'kotlin', 'code', 'kotlin']);
    expect(estimate.tolerance).toBe(0.05);
    expect([code.lang, code.timeoutMs, code.tests[0].hidden]).toEqual(['javascript', 3000, false]);
    expect([open.format, open.maxChars, open.followUps]).toEqual(['text', 1200, []]);
  });

  it('derives ids from the lesson id and position, across lessons', () => {
    const course = Course.parse({
      units: [{ id: 'u', title: 'U', emoji: 'x', description: 'd', lessons: [
        { id: 'one', title: 'L', questions: filler(3) },
        { id: 'two', title: 'L', questions: filler(4) },
      ] }],
    });
    expect(allQuestions(course).map((q) => q.id)).toEqual(['one/1', 'one/2', 'one/3', 'two/1', 'two/2', 'two/3', 'two/4']);
    expect(findLesson(course, 'two')?.lesson.questions).toHaveLength(4);
    expect(findLesson(course, 'two')?.unit.id).toBe('u');
    expect(findLesson(course, 'nope')).toBeUndefined();
  });
});

describe('structure limits', () => {
  it.each([
    ['uppercase slug', { lessonId: 'Bad' }],
    ['slug with a space', { lessonId: 'a b' }],
    ['slug starting with a dash', { lessonId: '-a' }],
    ['empty slug', { lessonId: '' }],
    ['uppercase unit', { unitId: 'Unit' }],
  ])('rejects %s', (_n, over) => {
    expect(Course.safeParse(courseWith(filler(3), over)).success).toBe(false);
  });

  it('needs at least one unit, one lesson and three questions per lesson', () => {
    expect(Course.safeParse({ units: [] }).success).toBe(false);
    expect(Course.safeParse({ units: [{ id: 'u', title: 'U', emoji: 'x', description: 'd', lessons: [] }] }).success).toBe(false);
    expect(Course.safeParse({ units: [{ id: 'u', title: 'U', emoji: 'x', description: 'd', lessons: [{ id: 'l', title: 'L', questions: filler(2) }] }] }).success).toBe(false);
  });

  it('rejects an unknown question type and a missing type', () => {
    expect(parse([{ ...valid['single']!, type: 'essay' }]).success).toBe(false);
    const { type: _t, ...noType } = valid['single']!;
    expect(parse([noType]).success).toBe(false);
  });

  it('unit colour is a legacy optional field: no default any more (the UI uses design tokens), any string is accepted', () => {
    const c = Course.parse(courseWith(filler(3)));
    expect(c.units[0]!.color).toBeUndefined();
    const withColour = courseWith(filler(3));
    (withColour.units[0] as Record<string, unknown>)['color'] = 'rebeccapurple';
    expect(Course.parse(withColour).units[0]!.color).toBe('rebeccapurple');
  });
});

describe('field limits per question type', () => {
  const cases: [string, Q][] = [
    ['empty prompt', withQ('single', { prompt: '' })],
    ['missing explanation', (() => { const { explanation: _e, ...r } = valid['single']!; return r; })()],
    ['hint over 220 chars', withQ('single', { hint: 'x'.repeat(221) })],
    ['empty hint', withQ('single', { hint: '' })],
    ['bad difficulty', withQ('single', { difficulty: 'impossible' })],
    ['single with 1 option', withQ('single', { options: [{ id: 'a', text: 'A' }] })],
    ['single with 7 options', withQ('single', { options: Array.from({ length: 7 }, (_, i) => ({ id: `o${i}`, text: 'x' })) })],
    ['option with empty text', withQ('single', { options: [{ id: 'a', text: '' }, { id: 'b', text: 'B' }] })],
    ['multi with 2 options', withQ('multi', { options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }] })],
    ['multi with no answers', withQ('multi', { answers: [] })],
    ['fill with 1 bank token', withQ('fill', { bank: ['1'] })],
    ['fill with no answers', withQ('fill', { answers: [] })],
    ['fill with empty template', withQ('fill', { template: '' })],
    ['order with 2 lines', withQ('order', { lines: ['a', 'b'] })],
    ['order with 11 lines', withQ('order', { lines: Array.from({ length: 11 }, (_, i) => `l${i}`) })],
    ['order with a bad kind', withQ('order', { kind: 'poem' })],
    ['spot with no answers', withQ('spot', { answers: [] })],
    ['spot with 5 answers', withQ('spot', { answers: [1, 2, 3, 4, 5] })],
    ['spot with line 0', withQ('spot', { answers: [0] })],
    ['spot with a fractional line', withQ('spot', { answers: [1.5] })],
    ['estimate with a string answer', withQ('estimate', { answer: '50' })],
    ['estimate with infinite answer', withQ('estimate', { answer: Infinity })],
    ['estimate tolerance below 0', withQ('estimate', { tolerance: -0.1 })],
    ['estimate tolerance above 0.5', withQ('estimate', { tolerance: 0.6 })],
    ['match with 2 pairs', withQ('match', { pairs: [{ left: 'a', right: '1' }, { left: 'b', right: '2' }] })],
    ['match with 7 pairs', withQ('match', { pairs: Array.from({ length: 7 }, (_, i) => ({ left: `l${i}`, right: `r${i}` })) })],
    ['code with 1 test', withQ('code', { tests: [{ args: [1], expect: 1 }] })],
    ['code with 13 tests', withQ('code', { tests: Array.from({ length: 13 }, () => ({ args: [1], expect: 1 })) })],
    ['code entry with a space', withQ('code', { entry: 'my fn' })],
    ['code entry starting with a digit', withQ('code', { entry: '1f' })],
    ['code lang other than javascript', withQ('code', { lang: 'kotlin' })],
    ['code timeout below 300', withQ('code', { timeoutMs: 100 })],
    ['code timeout above 10000', withQ('code', { timeoutMs: 20000 })],
    ['code test with neither expect nor throws', withQ('code', { tests: [{ args: [1] }, { args: [2], expect: 2 }] })],
    ['code test with a malformed js arg', withQ('code', { tests: [{ args: { js: '' }, expect: 1 }, { args: [2], expect: 2 }] })],
    ['open with 1 key point', withQ('open', { keyPoints: [{ point: 'a', keywords: ['a'] }] })],
    ['open with 7 key points', withQ('open', { keyPoints: Array.from({ length: 7 }, (_, i) => ({ point: `p${i}`, keywords: ['k'] })) })],
    ['open key point with no keywords', withQ('open', { keyPoints: [{ point: 'a', keywords: [] }, { point: 'b', keywords: ['b'] }] })],
    ['open key point with an empty keyword', withQ('open', { keyPoints: [{ point: 'a', keywords: [''] }, { point: 'b', keywords: ['b'] }] })],
    ['open without a model answer', withQ('open', { modelAnswer: '' })],
    ['open with 5 follow-ups', withQ('open', { followUps: Array.from({ length: 5 }, () => ({ when: 'missed', point: 1, prompt: 'p', modelAnswer: 'm', keyPoints: [{ point: 'a', keywords: ['a'] }, { point: 'b', keywords: ['b'] }] })) })],
    ['open follow-up with 1 key point', withQ('open', { followUps: [{ when: 'missed', point: 1, prompt: 'p', modelAnswer: 'm', keyPoints: [{ point: 'a', keywords: ['a'] }] }] })],
    ['open follow-up with a bad "when"', withQ('open', { followUps: [{ when: 'always', point: 1, prompt: 'p', modelAnswer: 'm', keyPoints: [{ point: 'a', keywords: ['a'] }, { point: 'b', keywords: ['b'] }] }] })],
    ['open follow-up pointing at point 0', withQ('open', { followUps: [{ when: 'missed', point: 0, prompt: 'p', modelAnswer: 'm', keyPoints: [{ point: 'a', keywords: ['a'] }, { point: 'b', keywords: ['b'] }] }] })],
    ['a code block with empty source', withQ('single', { code: { lang: 'kotlin', source: '' } })],
  ];
  it.each(cases)('rejects %s', (_name, q) => {
    expect(parse([q]).success).toBe(false);
  });

  it('error paths point at the offending field', () => {
    const res = parse([withQ('estimate', { tolerance: 0.9 })]);
    expect(res.success).toBe(false);
    expect(res.error!.issues[0]!.path.join('.')).toBe('units.0.lessons.0.questions.0.tolerance');
  });

  it('open questions may omit the explanation (the model answer is shown instead), others may not', () => {
    expect(parse([valid['open']!]).success).toBe(true);
  });

  it('accepts the boundary values', () => {
    for (const q of [
      withQ('estimate', { tolerance: 0 }),
      withQ('estimate', { tolerance: 0.5 }),
      withQ('estimate', { answer: -3.5 }),
      withQ('code', { timeoutMs: 300 }),
      withQ('code', { timeoutMs: 10000 }),
      withQ('single', { hint: 'x'.repeat(220) }),
      withQ('spot', { answers: [1, 2, 3, 4] }),
      withQ('code', { tests: [{ args: { js: '[1]' }, expect: 1 }, { args: [], throws: '' }] }),
    ]) expect(parse([q]).success).toBe(true);
  });
});

describe('helpers', () => {
  it('blankIndexes lists the blank numbers in order of appearance', () => {
    expect(blankIndexes('a[[2]]b[[1]]c[[10]]')).toEqual([2, 1, 10]);
    expect(blankIndexes('no blanks [[x]] [ [1] ]')).toEqual([]);
    expect(blankIndexes('')).toEqual([]);
  });
  it('spotLines drops trailing blank lines but keeps inner ones and indentation', () => {
    expect(spotLines('a\n\n  b\n\n\n')).toEqual(['a', '', '  b']);
    expect(spotLines('')).toEqual(['']);
  });
});

describe('validateCourse', () => {
  it('reports duplicate unit and lesson ids', () => {
    const unit = (id: string, lessonId: string) => ({ id, title: 'U', emoji: 'x', description: 'd', lessons: [{ id: lessonId, title: 'L', questions: filler(3) }] });
    const course = Course.parse({ units: [unit('a', 'l1'), unit('a', 'l2'), unit('b', 'l1')] });
    expect(validateCourse(course)).toEqual(['duplicate unit id "a"', 'duplicate lesson id "l1"']);
  });

  const options = [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }, { id: 'c', text: 'C' }];
  const bad: [string, Q, RegExp][] = [
    ['single: duplicate option ids', withQ('single', { options: [{ id: 'a', text: 'A' }, { id: 'a', text: 'B' }] }), /duplicate option ids/],
    ['single: answer not an option', withQ('single', { answer: 'z' }), /answer "z" is not an option/],
    ['multi: answer not an option', withQ('multi', { options, answers: ['a', 'z'] }), /answer "z" is not an option/],
    ['multi: duplicate answers', withQ('multi', { options, answers: ['a', 'a'] }), /duplicate answers/],
    ['multi: only one correct answer', withQ('multi', { options, answers: ['a'] }), />= 2 correct answers/],
    ['fill: blanks not 1..n', withQ('fill', { template: 'x[[1]]y[[3]]' }), /blanks \[\[n\]\] must be 1\.\.2/],
    ['fill: a blank used twice', withQ('fill', { template: 'x[[1]]y[[1]]' }), /exactly once/],
    ['fill: answer missing from bank', withQ('fill', { bank: ['1', '9', '3'] }), /answer "2" missing from bank/],
    ['fill: bank without distractors', withQ('fill', { bank: ['1', '2'] }), /no distractors/],
    ['fill: a repeated answer needs two tokens in the bank', withQ('fill', { answers: ['1', '1'], bank: ['1', '3'] }), /answer "1" missing from bank/],
    ['order: duplicate lines', withQ('order', { lines: ['x', 'x', 'y'] }), /lines must be unique/],
    ['spot: answer past the end', withQ('spot', { answers: [9] }), /answer line 9 is past the end \(4 lines\)/],
    ['spot: blank answer line', withQ('spot', { source: 'a\n\nc\nd', answers: [2] }), /answer line 2 is blank/],
    ['spot: duplicate answers', withQ('spot', { answers: [2, 2] }), /duplicate answer lines/],
    ['spot: too short', withQ('spot', { source: 'a\nb', answers: [1] }), /need 3-20 lines|3-20 lines/],
    ['spot: too long', withQ('spot', { source: Array.from({ length: 21 }, (_, i) => `l${i}`).join('\n'), answers: [1] }), /3-20 lines/],
    ['estimate: zero answer', withQ('estimate', { answer: 0 }), /non-zero/],
    ['match: duplicate left', withQ('match', { pairs: [{ left: 'a', right: '1' }, { left: 'a', right: '2' }, { left: 'c', right: '3' }] }), /duplicate left items/],
    ['match: duplicate right', withQ('match', { pairs: [{ left: 'a', right: '1' }, { left: 'b', right: '1' }, { left: 'c', right: '3' }] }), /duplicate right items/],
    ['code: only hidden tests', withQ('code', { tests: [{ args: [1], expect: 1, hidden: true }, { args: [2], expect: 2, hidden: true }] }), /at least one visible test/],
    ['code: starter does not define the entry', withQ('code', { starter: 'function g() {}' }), /must define `f`/],
    ['code: solution does not define the entry', withQ('code', { solution: 'const h = 1' }), /must define `f`/],
    ['open: follow-up past the rubric', withQ('open', { followUps: [{ when: 'missed', point: 3, prompt: 'p', modelAnswer: 'm', keyPoints: [{ point: 'a', keywords: ['a'] }, { point: 'b', keywords: ['b'] }] }] }), /refers to key point 3, but there are 2/],
  ];
  it.each(bad)('%s', (_name, q, pattern) => {
    const problems = problemsOf(q);
    expect(problems.length).toBeGreaterThan(0);
    expect(problems.join('\n')).toMatch(pattern);
    expect(problems.every((p) => p.startsWith('l/1'))).toBe(true);
  });

  it('accepts the valid twins of those cases', () => {
    const twins: Q[] = [
      withQ('fill', { answers: ['1', '1'], bank: ['1', '1', '3'], template: '[[1]]-[[2]]' }),
      withQ('fill', { template: '[[2]] and [[1]]' }),
      withQ('code', { starter: 'const f = (x) => x', solution: 'let f = (x) => x' }),
      withQ('code', { entry: '$f', starter: 'function $f() {}', solution: 'var $f = () => 1' }),
      withQ('spot', { source: 'a\nb\nc', answers: [3] }),
      withQ('spot', { source: Array.from({ length: 20 }, (_, i) => `l${i}`).join('\n'), answers: [20] }),
      withQ('estimate', { answer: -5 }),
      withQ('open', { followUps: [{ when: 'covered', point: 2, prompt: 'p', modelAnswer: 'm', keyPoints: [{ point: 'a', keywords: ['a'] }, { point: 'b', keywords: ['b'] }] }] }),
    ];
    for (const q of twins) expect(problemsOf(q), JSON.stringify(q).slice(0, 80)).toEqual([]);
  });

  it('collects several problems at once', () => {
    const course = Course.parse(courseWith([withQ('single', { answer: 'z' }), withQ('estimate', { answer: 0 }), withQ('order', { lines: ['x', 'x', 'y'] })]));
    expect(validateCourse(course as CourseT)).toHaveLength(3);
  });
});
