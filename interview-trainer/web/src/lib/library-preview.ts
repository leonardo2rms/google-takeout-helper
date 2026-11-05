import type { Question } from '@trainer/shared';

/** How many lines of code a Library card shows. */
export const PREVIEW_LINES = 2;
const MAX_LINE = 72;

/** The code a question is about, as plain text: its snippet, the template with blanks, the buggy source, the lines to order or the starter. */
function sourceOf(q: Question): string | null {
  if (q.type === 'fill') return q.template.replace(/\[\[\d+\]\]/g, '___');
  if (q.type === 'spot') return q.source;
  if (q.type === 'order') return q.kind === 'code' ? q.lines.join('\n') : null;
  if (q.type === 'code') return q.starter;
  return q.code?.source ?? null;
}

/**
 * The first couple of meaningful lines of a question's code, for the Library card. Many prompts read alike ("What does this
 * print?"); the code is what tells them apart. Blank lines and bare braces are skipped, long lines are cut.
 */
export function codePreview(q: Question): string[] {
  const text = sourceOf(q);
  if (!text) return [];
  const lines = text.split('\n').map((l) => l.replace(/\s+$/, '')).filter((l) => l.trim() !== '' && !/^[\s{}()\[\];]*$/.test(l));
  const indent = Math.min(...lines.map((l) => l.length - l.trimStart().length), Infinity);
  return lines.slice(0, PREVIEW_LINES).map((l) => {
    const line = l.slice(Number.isFinite(indent) ? indent : 0);
    return line.length > MAX_LINE ? `${line.slice(0, MAX_LINE - 1)}…` : line;
  });
}
