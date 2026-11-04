import type { FollowUp, OpenQ } from './schema';

export const followUpId = (q: OpenQ, index: number): string => `${q.id}~f${index + 1}`;

/**
 * Chooses the interviewer's follow-up from what the learner covered: first a gap to probe (`missed`),
 * otherwise a deeper question on something they got right (`covered`). Authoring order breaks ties.
 */
export function pickFollowUp(q: OpenQ, covered: readonly boolean[]): { index: number; followUp: FollowUp } | null {
  const hit = (when: FollowUp['when']) =>
    q.followUps.findIndex((f) => f.when === when && (covered[f.point - 1] ?? false) === (when === 'covered'));
  for (const when of ['missed', 'covered'] as const) {
    const index = hit(when);
    if (index !== -1) return { index, followUp: q.followUps[index]! };
  }
  return null;
}

/** A follow-up as a standalone open question, so it can be graded by the same machinery (and endpoint). */
export function followUpAsOpen(q: OpenQ, index: number): OpenQ {
  const f = q.followUps[index]!;
  return {
    type: 'open',
    id: followUpId(q, index),
    prompt: f.prompt,
    explanation: undefined,
    difficulty: q.difficulty,
    tags: q.tags,
    format: 'text',
    modelAnswer: f.modelAnswer,
    keyPoints: f.keyPoints,
    maxChars: f.maxChars,
    followUps: [],
  };
}

/** The question itself plus each follow-up as an open question: everything a grader may be asked about. */
export const openParts = (q: OpenQ): OpenQ[] => [q, ...q.followUps.map((_, i) => followUpAsOpen(q, i))];
