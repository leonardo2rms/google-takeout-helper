import { afterEach, describe, expect, it, vi } from 'vitest';
import { Course } from '@trainer/shared';

const course = Course.parse({
  units: [{
    id: 'u', title: 'U', emoji: 'x', description: 'd',
    lessons: [{
      id: 'l', title: 'L',
      questions: [
        {
          type: 'open', prompt: 'Explain X', modelAnswer: 'X is Y',
          keyPoints: [{ point: 'Y', keywords: ['y'] }, { point: 'Z', keywords: ['z'] }],
          followUps: [{ when: 'missed', point: 2, prompt: 'And Z?', modelAnswer: 'W', keyPoints: [{ point: 'W', keywords: ['w'] }, { point: 'V', keywords: ['v'] }] }],
        },
        { type: 'single', prompt: 'p', explanation: 'e', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a' },
        { type: 'single', prompt: 'p', explanation: 'e', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a' },
      ],
    }],
  }],
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
  vi.resetModules();
});

const sampleWith = (verdict: unknown) => Object.assign(vi.fn(), { json: vi.fn().mockResolvedValue(verdict) }) as never;

describe('createSampleGrader', async () => {
  const { createSampleGrader } = await import('./sample-grader');

  it('turns the model verdict into a GradeResponse (score from covered flags, trimmed feedback)', async () => {
    const sample = sampleWith({ points: [{ covered: true }, { covered: false }], feedback: '  good  ' });
    const grade = createSampleGrader(course, sample);
    expect(await grade('l/1', 'my answer')).toEqual({ score: 0.5, covered: [true, false], feedback: 'good', graded_by: 'ai' });
    const [prompt, opts] = (sample as unknown as { json: ReturnType<typeof vi.fn> }).json.mock.calls[0]!;
    expect(prompt).toContain('<candidate_answer>\nmy answer\n</candidate_answer>');
    expect(opts).toEqual({ cache: false });
  });

  it('grades follow-ups under their own ids, with their own rubric', async () => {
    const sample = sampleWith({ points: [{ covered: true }, { covered: true }], feedback: 'f' });
    const grade = createSampleGrader(course, sample);
    expect((await grade('l/1~f1', 'w and v')).score).toBe(1);
    expect((sample as unknown as { json: ReturnType<typeof vi.fn> }).json.mock.calls[0]![0]).toContain('And Z?');
  });

  it('only counts an explicit true as covered (strings, numbers and nulls do not)', async () => {
    const grade = createSampleGrader(course, sampleWith({ points: [{ covered: 'true' }, null], feedback: 7 }));
    expect(await grade('l/1', 'a')).toMatchObject({ covered: [false, false], score: 0, feedback: '' });
  });

  it('rejects unknown ids, non-open questions and verdicts of the wrong shape', async () => {
    const grade = createSampleGrader(course, sampleWith({ points: [{ covered: true }, { covered: true }], feedback: '' }));
    await expect(grade('nope', 'a')).rejects.toThrow('unknown question');
    await expect(grade('l/2', 'a')).rejects.toThrow('unknown question');
    for (const bad of [{ points: [{ covered: true }] }, { points: 'x' }, {}, { points: [] }]) {
      await expect(createSampleGrader(course, sampleWith(bad))('l/1', 'abc')).rejects.toThrow('bad verdict shape');
    }
  });

  it('propagates host errors (the caller then falls back to self-assessment)', async () => {
    const sample = Object.assign(vi.fn(), { json: vi.fn().mockRejectedValue(new Error('quota')) }) as never;
    await expect(createSampleGrader(course, sample)('l/1', 'abc')).rejects.toThrow('quota');
  });
});

describe('loadSample', async () => {
  const { loadSample } = await import('./sample-grader');

  it('is null outside a host that provides window.claude', async () => {
    vi.stubGlobal('window', {});
    expect(await loadSample()).toBeNull();
    vi.stubGlobal('window', { claude: {} });
    expect(await loadSample()).toBeNull();
  });

  it('returns the capability the host hands out, and null when it is refused or throws', async () => {
    const sample = vi.fn();
    vi.stubGlobal('window', { claude: { use: vi.fn().mockResolvedValue(sample) } });
    expect(await loadSample()).toBe(sample);
    vi.stubGlobal('window', { claude: { use: vi.fn().mockResolvedValue(undefined) } });
    expect(await loadSample()).toBeNull();
    vi.stubGlobal('window', { claude: { use: vi.fn().mockRejectedValue(new Error('denied')) } });
    expect(await loadSample()).toBeNull();
  });

  it('asks for the sample capability by name', async () => {
    const use = vi.fn().mockResolvedValue(null);
    vi.stubGlobal('window', { claude: { use } });
    await loadSample();
    expect(use).toHaveBeenCalledWith('sample');
  });
});

describe('isStandalone', () => {
  it('follows VITE_STANDALONE=1 at build time', async () => {
    vi.stubEnv('VITE_STANDALONE', '1');
    vi.resetModules();
    expect((await import('./sample-grader')).isStandalone).toBe(true);
    vi.stubEnv('VITE_STANDALONE', '');
    vi.resetModules();
    expect((await import('./sample-grader')).isStandalone).toBe(false);
  });
});
