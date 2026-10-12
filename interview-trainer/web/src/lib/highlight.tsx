import type { ReactNode } from 'react';

const KEYWORDS = new Set(
  (
    'fun val var class object interface data sealed enum open abstract override suspend inline value private public protected internal ' +
    'return if else when for while do in is as try catch finally throw null true false this super import package companion by lateinit ' +
    'const typealias init constructor where out reified crossinline noinline new static final void int long double boolean char byte short ' +
    'float extends implements throws synchronized volatile'
  ).split(' '),
);

// comment | string | annotation | number | identifier
const TOKEN = /(\/\/.*|\/\*[\s\S]*?\*\/|#.*)|("""[\s\S]*?"""|"(?:\\.|[^"\\\n])*")|(@[A-Za-z_]\w*)|(\b\d[\d_]*(?:\.\d+)?[LfF]?\b)|([A-Za-z_]\w*)/g;

/** Tiny, dependency-free highlighter: good enough for short interview snippets. */
export function highlight(source: string): ReactNode[] {
  const out: ReactNode[] = [];
  let last = 0;
  let key = 0;
  for (const m of source.matchAll(TOKEN)) {
    const at = m.index ?? 0;
    if (at > last) out.push(source.slice(last, at));
    const [text, comment, str, annotation, num, ident] = m;
    let cls: string | null = null;
    if (comment) cls = 'tok-comment';
    else if (str) cls = 'tok-string';
    else if (annotation) cls = 'tok-annotation';
    else if (num) cls = 'tok-number';
    else if (ident && KEYWORDS.has(ident)) cls = 'tok-keyword';
    else if (ident && /^[A-Z]/.test(ident)) cls = 'tok-type';
    out.push(cls ? <span key={key++} className={cls}>{text}</span> : text);
    last = at + text.length;
  }
  if (last < source.length) out.push(source.slice(last));
  return out;
}
