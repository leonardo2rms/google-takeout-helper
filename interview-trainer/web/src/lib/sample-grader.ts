import { allQuestions, buildPlainPrompt, type Course, type GradeResponse, type OpenQ } from '@trainer/shared';
import type { GradeFn } from './evaluate';

/**
 * Hosted single-file build: the page has no server, so grading goes through the viewer's own Claude
 * account via the artifact `sample` capability. `window.claude` only exists inside that viewer.
 */
interface Sample {
  json<T>(input: string, options?: { modelTier?: 'quick' | 'default' | 'complex'; cache?: boolean }): Promise<T>;
}
interface ClaudeHost {
  use(name: string): Promise<unknown>;
}

export const isStandalone = import.meta.env.VITE_STANDALONE === '1';

export async function loadSample(): Promise<Sample | null> {
  const host = (window as unknown as { claude?: ClaudeHost }).claude;
  if (!host?.use) return null;
  try {
    return ((await host.use('sample')) as Sample | null) ?? null;
  } catch {
    return null;
  }
}

interface Verdict {
  points?: { covered?: unknown }[];
  feedback?: unknown;
}

export function createSampleGrader(course: Course, sample: Sample): GradeFn {
  const open = new Map<string, OpenQ>();
  for (const q of allQuestions(course)) if (q.type === 'open') open.set(q.id, q);

  return async (questionId, answer): Promise<GradeResponse> => {
    const q = open.get(questionId);
    if (!q) throw new Error('unknown question');
    const verdict = await sample.json<Verdict>(buildPlainPrompt(q, answer), { cache: false });
    const points = verdict.points;
    if (!Array.isArray(points) || points.length !== q.keyPoints.length) throw new Error('bad verdict shape');
    const covered = points.map((p) => p?.covered === true);
    return {
      score: covered.filter(Boolean).length / covered.length,
      covered,
      feedback: typeof verdict.feedback === 'string' ? verdict.feedback.trim() : '',
      graded_by: 'ai',
    };
  };
}
