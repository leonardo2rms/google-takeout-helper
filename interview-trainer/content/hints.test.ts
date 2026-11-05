import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { allQuestions, type Question } from '../shared/src';
import { loadCourse } from '../shared/src/load-content';

const questions = allQuestions(loadCourse(resolve(dirname(fileURLToPath(import.meta.url)))));
const hinted = questions.filter((q) => q.hint);

const norm = (s: string) => s.toLowerCase().replace(/[`'"*]/g, '').replace(/\s+/g, ' ').trim();

/** The literal answer(s) of a question, where there is one short text to compare against. */
function answerTexts(q: Question): string[] {
  switch (q.type) {
    case 'single':
      return [q.options.find((o) => o.id === q.answer)!.text];
    case 'multi':
      return q.options.filter((o) => q.answers.includes(o.id)).map((o) => o.text);
    case 'fill':
      return q.answers;
    default:
      return [];
  }
}

describe('hints', () => {
  it('there are plenty, across every question type', () => {
    expect(hinted.length).toBeGreaterThanOrEqual(40);
    const types = new Set(hinted.map((q) => q.type));
    for (const t of ['single', 'multi', 'fill', 'order', 'spot', 'estimate', 'match', 'code', 'open'] as const) expect(types, t).toContain(t);
  });

  it('a hint is short, differs from the explanation and does not paste the answer', () => {
    for (const q of hinted) {
      const hint = norm(q.hint!);
      expect(hint.length, q.id).toBeLessThanOrEqual(220);
      expect(hint, `${q.id}: hint repeats the explanation`).not.toBe(norm(q.explanation ?? ''));
      for (const a of answerTexts(q)) {
        const text = norm(a);
        if (text.length >= 12) expect(hint, `${q.id}: hint contains the answer "${a}"`).not.toContain(text);
      }
    }
  });
});
