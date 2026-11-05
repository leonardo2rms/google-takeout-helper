import {
  EXPLAIN_MAX_ANSWER_CHARS, allQuestions, buildExplainPlainPrompt, cleanReply, explainableQuestions,
  type Course, type ExplainMessage, type ExplainResponse, type Question,
} from '@trainer/shared';
import type { Answer } from './evaluate';
import { withTimeout, type Sample } from './sample-grader';

/** Sends the conversation about one question and resolves with the tutor's plain-text reply. */
export type ExplainFn = (req: { questionId: string; answer: string; correct: boolean; messages: ExplainMessage[] }) => Promise<string>;

const BASE = import.meta.env.VITE_API_URL ?? '';

export const explainOnline: ExplainFn = async ({ questionId, answer, correct, messages }) => {
  const res = await fetch(`${BASE}/api/explain`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ questionId, answer, correct, messages }),
    signal: AbortSignal.timeout(60_000),
  });
  if (!res.ok) throw new Error(`explain failed (${res.status})`);
  return cleanReply(((await res.json()) as ExplainResponse).reply);
};

/** Hosted single-file build: same prompt, sent through the viewer's own Claude via the `sample` capability. */
export function createSampleExplainer(course: Course, sample: Sample): ExplainFn {
  const known = explainableQuestions(allQuestions(course));
  return async ({ questionId, answer, correct, messages }) => {
    const q = known.get(questionId);
    if (!q) throw new Error('unknown question');
    const result = await withTimeout(sample(buildExplainPlainPrompt(q, answer, correct, messages), { cache: false }));
    const text = typeof result === 'string' ? result : result?.text;
    if (typeof text !== 'string' || text.trim() === '') throw new Error('empty reply');
    return cleanReply(text);
  };
}

const cut = (s: string) => (s.length > EXPLAIN_MAX_ANSWER_CHARS ? `${s.slice(0, EXPLAIN_MAX_ANSWER_CHARS - 1)}…` : s);

/** The learner's answer as a short sentence for the tutor ("b) Beta"). It is untrusted text on the server side. */
export function describeLearnerAnswer(q: Question, answer: Answer | null, skipped: boolean): string {
  if (skipped || !answer) return '';
  switch (answer.type) {
    case 'single':
    case 'multi': {
      if (q.type !== 'single' && q.type !== 'multi') return '';
      const ids = answer.type === 'single' ? [answer.selected] : answer.selected;
      return cut(ids.map((id) => `${id}) ${q.options.find((o) => o.id === id)?.text ?? id}`).join('; '));
    }
    case 'fill':
      return cut(`Filled blanks: ${answer.filled.map((f) => f ?? '(empty)').join(', ')}`);
    case 'order':
      return cut(`Order chosen: ${answer.arranged.join(' / ')}`);
    case 'spot':
      return `Lines chosen: ${[...answer.selected].sort((a, b) => a - b).join(', ')}`;
    case 'estimate':
      return `Estimate: ${answer.value}`;
    case 'match':
      if (q.type !== 'match') return '';
      return cut(q.pairs.map((p, i) => `${p.left} -> ${answer.chosen[i] ?? '(none)'}`).join('; '));
    case 'code':
      return cut(answer.source);
    case 'open':
      return cut(answer.text);
  }
}
