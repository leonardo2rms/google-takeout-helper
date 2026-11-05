import { afterEach, describe, expect, it, vi } from 'vitest';
import { explainOnline, createSampleExplainer, describeLearnerAnswer } from './explain';
import { fetchHealth, gradeOpen } from './api';
import { Course, allQuestions, type Question } from '@trainer/shared';

const json = (body: unknown, init: ResponseInit = {}) => new Response(JSON.stringify(body), { status: 200, ...init });
afterEach(() => vi.unstubAllGlobals());

describe('fetchHealth', () => {
  it('returns the health body, and null on any failure (offline, 5xx, bad JSON)', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(json({ ok: true, ai: true })));
    expect(await fetchHealth()).toEqual({ ok: true, ai: true });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(json({}, { status: 503 })));
    expect(await fetchHealth()).toBeNull();
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new TypeError('Failed to fetch')));
    expect(await fetchHealth()).toBeNull();
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('<html>', { status: 200 })));
    expect(await fetchHealth()).toBeNull();
  });

  it('uses a short timeout so an offline start is not slow', async () => {
    const fetchMock = vi.fn().mockResolvedValue(json({ ok: true, ai: false }));
    vi.stubGlobal('fetch', fetchMock);
    await fetchHealth();
    expect(fetchMock.mock.calls[0]![0]).toBe('/api/health');
    expect(fetchMock.mock.calls[0]![1].signal).toBeInstanceOf(AbortSignal);
  });
});

describe('gradeOpen', () => {
  it('posts the question id and answer as JSON', async () => {
    const fetchMock = vi.fn().mockResolvedValue(json({ score: 1, covered: [true], feedback: 'ok', graded_by: 'ai' }));
    vi.stubGlobal('fetch', fetchMock);
    expect(await gradeOpen('l/1', 'my answer')).toMatchObject({ score: 1 });
    const [url, init] = fetchMock.mock.calls[0]!;
    expect(url).toBe('/api/grade');
    expect(init.method).toBe('POST');
    expect(init.headers).toEqual({ 'content-type': 'application/json' });
    expect(JSON.parse(init.body)).toEqual({ questionId: 'l/1', answer: 'my answer' });
  });

  it('throws on every non-2xx status so the caller falls back to self-assessment', async () => {
    for (const status of [400, 404, 413, 429, 502, 503]) {
      vi.stubGlobal('fetch', vi.fn().mockResolvedValue(json({ error: 'x' }, { status })));
      await expect(gradeOpen('l/1', 'abc')).rejects.toThrow(`grading failed (${status})`);
    }
  });
});

describe('explainOnline', () => {
  it('posts the conversation and cleans the reply', async () => {
    const fetchMock = vi.fn().mockResolvedValue(json({ reply: '## Heading\nBody  ' }));
    vi.stubGlobal('fetch', fetchMock);
    const messages = [{ role: 'user' as const, content: 'why?' }];
    expect(await explainOnline({ questionId: 'l/1', answer: 'b', correct: false, messages })).toBe('Heading\nBody');
    expect(JSON.parse(fetchMock.mock.calls[0]![1].body)).toEqual({ questionId: 'l/1', answer: 'b', correct: false, messages });
    expect(fetchMock.mock.calls[0]![0]).toBe('/api/explain');
  });

  it('throws when the server refuses', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(json({}, { status: 503 })));
    await expect(explainOnline({ questionId: 'a', answer: '', correct: true, messages: [] })).rejects.toThrow('explain failed (503)');
  });
});

const course = Course.parse({
  units: [{
    id: 'u', title: 'U', emoji: 'x', description: 'd',
    lessons: [{
      id: 'l', title: 'L',
      questions: [
        { type: 'single', prompt: 'Which?', explanation: 'Because A.', options: [{ id: 'a', text: 'Alpha' }, { id: 'b', text: 'Beta' }], answer: 'a' },
        { type: 'match', prompt: 'm', explanation: 'e', pairs: [{ left: 'L1', right: 'R1' }, { left: 'L2', right: 'R2' }, { left: 'L3', right: 'R3' }] },
        {
          type: 'open', prompt: 'o', modelAnswer: 'm',
          keyPoints: [{ point: 'one', keywords: ['a'] }, { point: 'two', keywords: ['b'] }],
          followUps: [{ when: 'missed', point: 1, prompt: 'f', modelAnswer: 'fm', keyPoints: [{ point: 'x', keywords: ['x'] }, { point: 'y', keywords: ['y'] }] }],
        },
      ],
    }],
  }],
});
const [single, match, open] = allQuestions(course) as [Question, Question, Question];
const msgs = [{ role: 'user' as const, content: 'why?' }];

describe('createSampleExplainer (hosted build)', () => {
  it('sends one self-contained prompt without caching and accepts {text} or a bare string', async () => {
    const sample = vi.fn().mockResolvedValue({ text: '  ## Hi there ' });
    const explain = createSampleExplainer(course, sample as never);
    expect(await explain({ questionId: 'l/1', answer: 'b', correct: false, messages: msgs })).toBe('Hi there');
    expect(sample.mock.calls[0]![0]).toContain('<learner_message>why?</learner_message>');
    expect(sample.mock.calls[0]![1]).toEqual({ cache: false });
    const bare = createSampleExplainer(course, vi.fn().mockResolvedValue('plain') as never);
    expect(await bare({ questionId: 'l/1', answer: '', correct: true, messages: msgs })).toBe('plain');
  });

  it('explains follow-up ids too, and rejects unknown ids and empty replies', async () => {
    const explain = createSampleExplainer(course, vi.fn().mockResolvedValue({ text: 'ok' }) as never);
    expect(await explain({ questionId: 'l/3~f1', answer: '', correct: false, messages: msgs })).toBe('ok');
    await expect(explain({ questionId: 'nope', answer: '', correct: false, messages: msgs })).rejects.toThrow('unknown question');
    for (const bad of [{ text: '   ' }, '', null, undefined, { text: 5 }]) {
      const e = createSampleExplainer(course, vi.fn().mockResolvedValue(bad) as never);
      await expect(e({ questionId: 'l/1', answer: '', correct: false, messages: msgs })).rejects.toThrow('empty reply');
    }
  });
});

describe('describeLearnerAnswer', () => {
  it('is empty for skipped or missing answers', () => {
    expect(describeLearnerAnswer(single, { type: 'single', selected: 'a' }, true)).toBe('');
    expect(describeLearnerAnswer(single, null, false)).toBe('');
  });

  it('describes choices with their option text, falling back to the id', () => {
    expect(describeLearnerAnswer(single, { type: 'single', selected: 'b' }, false)).toBe('b) Beta');
    expect(describeLearnerAnswer(single, { type: 'multi', selected: ['a', 'zz'] }, false)).toBe('a) Alpha; zz) zz');
    expect(describeLearnerAnswer(match, { type: 'single', selected: 'a' }, false)).toBe(''); // mismatched question type
  });

  it('describes each other answer shape', () => {
    expect(describeLearnerAnswer(single, { type: 'fill', filled: ['?.', null] }, false)).toBe('Filled blanks: ?., (empty)');
    expect(describeLearnerAnswer(single, { type: 'order', arranged: ['a', 'b'] }, false)).toBe('Order chosen: a / b');
    expect(describeLearnerAnswer(single, { type: 'spot', selected: [3, 1, 2] }, false)).toBe('Lines chosen: 1, 2, 3');
    expect(describeLearnerAnswer(single, { type: 'estimate', value: 42 }, false)).toBe('Estimate: 42');
    expect(describeLearnerAnswer(match, { type: 'match', chosen: ['R2', null, 'R3'] }, false)).toBe('L1 -> R2; L2 -> (none); L3 -> R3');
    expect(describeLearnerAnswer(single, { type: 'match', chosen: [] }, false)).toBe('');
    expect(describeLearnerAnswer(open, { type: 'code', source: 'fun f() {}' }, false)).toBe('fun f() {}');
    expect(describeLearnerAnswer(open, { type: 'open', text: 'hello' }, false)).toBe('hello');
  });

  it('caps long answers at 600 characters with an ellipsis (but not the spot line list)', () => {
    const long = describeLearnerAnswer(open, { type: 'open', text: 'x'.repeat(2000) }, false);
    expect(long).toHaveLength(600);
    expect(long.endsWith('…')).toBe(true);
    expect(describeLearnerAnswer(open, { type: 'open', text: 'x'.repeat(600) }, false)).toHaveLength(600);
    expect(describeLearnerAnswer(open, { type: 'open', text: 'x'.repeat(600) }, false).endsWith('…')).toBe(false);
  });
});
