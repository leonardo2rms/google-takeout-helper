import { ImplausibleVerdictError, allQuestions, buildPlainPrompt, checkVerdict, openParts, type Course, type GradeResponse, type OpenQ } from '@trainer/shared';
import type { GradeFn } from './evaluate';

/**
 * Hosted single-file build: the page has no server, so grading goes through the viewer's own Claude
 * account via the artifact `sample` capability. `window.claude` only exists inside that viewer.
 */
export interface Sample {
  (input: string, options?: { modelTier?: 'quick' | 'default' | 'complex'; cache?: boolean }): Promise<{ text: string }>;
  json<T>(input: string, options?: { modelTier?: 'quick' | 'default' | 'complex'; cache?: boolean }): Promise<T>;
}
interface ClaudeHost {
  use(name: string): Promise<unknown>;
}

export const isStandalone = import.meta.env.VITE_STANDALONE === '1';

/** How long a hosted `sample` call may take before the app gives up and falls back to self-assessment. */
export const SAMPLE_TIMEOUT_MS = 25_000;
/** How long the host may take to hand over the capability at start-up. */
export const SAMPLE_LOAD_TIMEOUT_MS = 5_000;

/** Rejects if `work` has not settled within `ms`. A host call that never answers must not leave the learner stuck on "Grading...". */
export function withTimeout<T>(work: Promise<T>, ms: number = SAMPLE_TIMEOUT_MS): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`timed out after ${ms} ms`)), ms);
  });
  return Promise.race([work, timeout]).finally(() => clearTimeout(timer));
}

export async function loadSample(): Promise<Sample | null> {
  const host = (window as unknown as { claude?: ClaudeHost }).claude;
  if (!host?.use) return null;
  try {
    return ((await withTimeout(host.use('sample'), SAMPLE_LOAD_TIMEOUT_MS)) as Sample | null) ?? null;
  } catch {
    return null;
  }
}

export function createSampleGrader(course: Course, sample: Sample): GradeFn {
  const open = new Map<string, OpenQ>();
  for (const q of allQuestions(course)) if (q.type === 'open') for (const part of openParts(q)) open.set(part.id, part);

  return async (questionId, answer): Promise<GradeResponse> => {
    const q = open.get(questionId);
    if (!q) throw new Error('unknown question');
    const raw = await withTimeout(sample.json<unknown>(buildPlainPrompt(q, answer), { cache: false }));
    // The host returns whatever the model produced: only a plausible verdict becomes a grade, otherwise the app self-checks.
    const verdict = checkVerdict(q, answer, raw);
    if (!verdict.ok) throw new ImplausibleVerdictError(verdict.reason);
    return {
      score: verdict.covered.filter(Boolean).length / verdict.covered.length,
      covered: verdict.covered,
      feedback: verdict.feedback,
      graded_by: 'ai',
    };
  };
}
