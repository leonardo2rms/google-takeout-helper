import { highlight } from '../lib/highlight';

/** Logs, stack traces and other non-code text: shown verbatim, no syntax colouring. */
const PLAIN = new Set(['text', 'log', 'plain']);

export function CodeBlock({ source, lang }: { source: string; lang?: string }) {
  return (
    <pre className={`code ${PLAIN.has(lang ?? '') ? 'wrap' : ''}`} data-lang={lang} tabIndex={0}>
      <code>{PLAIN.has(lang ?? '') ? source.replace(/\n$/, '') : highlight(source.replace(/\n$/, ''))}</code>
    </pre>
  );
}
