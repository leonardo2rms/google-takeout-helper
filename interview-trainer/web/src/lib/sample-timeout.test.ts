/**
 * Hosted single-file build: a `sample` call that never settles must not leave the learner stuck on "Grading..."
 * or "Thinking..." (QA S1-3). After the timeout the call rejects, and the callers fall back to self-assessment.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Course, FEEDBACK_MAX_CHARS } from '@trainer/shared';
import { createSampleExplainer } from './explain';
import { SAMPLE_LOAD_TIMEOUT_MS, SAMPLE_TIMEOUT_MS, createSampleGrader, loadSample, withTimeout } from './sample-grader';

const course = Course.parse({
  units: [{
    id: 'u', title: 'U', emoji: 'x', description: 'd',
    lessons: [{
      id: 'l', title: 'L',
      questions: [
        { type: 'open', prompt: 'Explain X', modelAnswer: 'X is Y', keyPoints: [{ point: 'Y', keywords: ['y'] }, { point: 'Z', keywords: ['z'] }] },
        { type: 'single', prompt: 'p', explanation: 'e', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a' },
        { type: 'single', prompt: 'p', explanation: 'e', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a' },
      ],
    }],
  }],
});

const never = () => new Promise<never>(() => {});

beforeEach(() => vi.useFakeTimers());
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('withTimeout', () => {
  it('passes through a result that arrives in time and clears its timer', async () => {
    await expect(withTimeout(Promise.resolve(7), 100)).resolves.toBe(7);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('passes through a rejection that arrives in time', async () => {
    await expect(withTimeout(Promise.reject(new Error('quota')), 100)).rejects.toThrow('quota');
    expect(vi.getTimerCount()).toBe(0);
  });

  it('rejects once the time is up', async () => {
    const p = withTimeout(never(), 100);
    const caught = expect(p).rejects.toThrow('timed out after 100 ms');
    await vi.advanceTimersByTimeAsync(100);
    await caught;
  });

  it('defaults to the sample timeout', async () => {
    const p = withTimeout(never());
    const caught = expect(p).rejects.toThrow(`timed out after ${SAMPLE_TIMEOUT_MS} ms`);
    await vi.advanceTimersByTimeAsync(SAMPLE_TIMEOUT_MS);
    await caught;
  });
});

describe('hosted calls that never settle', () => {
  it('grading rejects (so the lesson falls back to self-assessment)', async () => {
    const sample = Object.assign(vi.fn(), { json: vi.fn(never) }) as never;
    const caught = expect(createSampleGrader(course, sample)('l/1', 'my answer')).rejects.toThrow('timed out');
    await vi.advanceTimersByTimeAsync(SAMPLE_TIMEOUT_MS);
    await caught;
  });

  it('Ask why rejects', async () => {
    const sample = vi.fn(never) as never;
    const caught = expect(createSampleExplainer(course, sample)({ questionId: 'l/2', answer: 'a) A', correct: false, messages: [{ role: 'user', content: 'why?' }] })).rejects.toThrow('timed out');
    await vi.advanceTimersByTimeAsync(SAMPLE_TIMEOUT_MS);
    await caught;
  });

  it('a host that never hands over the capability means "no AI", not a hung start-up', async () => {
    vi.stubGlobal('window', { claude: { use: vi.fn(never) } });
    const loaded = loadSample();
    await vi.advanceTimersByTimeAsync(SAMPLE_LOAD_TIMEOUT_MS);
    expect(await loaded).toBeNull();
  });
});

describe('runaway feedback (S2-5)', () => {
  it('is capped to a few sentences worth of text', async () => {
    const huge = 'x'.repeat(4_000_000);
    const sample = Object.assign(vi.fn(), { json: vi.fn().mockResolvedValue({ points: [{ covered: true }, { covered: false }], feedback: huge }) }) as never;
    const res = await createSampleGrader(course, sample)('l/1', 'my own explanation');
    expect(res.feedback.length).toBe(FEEDBACK_MAX_CHARS + 1);
    expect(res.feedback.endsWith('…')).toBe(true);
  });
});
