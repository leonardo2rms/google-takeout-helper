import { highlight } from '../lib/highlight';

export function CodeBlock({ source, lang }: { source: string; lang?: string }) {
  return (
    <pre className="code" data-lang={lang} tabIndex={0}>
      <code>{highlight(source.replace(/\n$/, ''))}</code>
    </pre>
  );
}
