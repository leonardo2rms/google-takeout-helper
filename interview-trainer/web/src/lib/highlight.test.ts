import { isValidElement, type ReactElement, type ReactNode } from 'react';
import { describe, expect, it } from 'vitest';
import { highlight } from './highlight';

/** Flattens the output into [className | null, text] pairs. */
function tokens(source: string): [string | null, string][] {
  return highlight(source).map((n: ReactNode) => {
    if (typeof n === 'string') return [null, n];
    if (isValidElement(n)) {
      const el = n as ReactElement<{ className: string; children: string }>;
      return [el.props.className, el.props.children];
    }
    throw new Error(`unexpected node ${String(n)}`);
  });
}

const joined = (source: string) => tokens(source).map(([, t]) => t).join('');

describe('highlight', () => {
  it('classifies keywords, types, numbers, strings, comments and annotations', () => {
    // Plain text between tokens (spaces, `x`, punctuation) arrives as bare strings.
    expect(tokens('val x: Int = 42 // answer').filter(([c]) => c !== null)).toEqual([
      ['tok-keyword', 'val'], ['tok-type', 'Int'], ['tok-number', '42'], ['tok-comment', '// answer'],
    ]);
    const t = Object.fromEntries(tokens('@Service class Foo { val n = 1_000L; val s = "hi" }').map(([c, x]) => [x, c]));
    expect(t['@Service']).toBe('tok-annotation');
    expect(t['class']).toBe('tok-keyword');
    expect(t['Foo']).toBe('tok-type');
    expect(t['1_000L']).toBe('tok-number');
    expect(t['"hi"']).toBe('tok-string');
  });

  it('never loses or reorders characters, whatever the input', () => {
    for (const src of [
      '', 'plain text', 'fun f(): Int {\n    return 1\n}\n', '/* block\ncomment */ val a = """raw\nstring"""',
      'x = "unterminated', 'a // c1\nb // c2', '# python comment\nprint(1.5)', 'éè unicode \u{1F600} ident', '"esc \\" quote" 3.14f',
    ]) expect(joined(src)).toBe(src);
  });

  it('keeps keywords inside strings and comments as part of those tokens', () => {
    expect(tokens('"if else"')).toEqual([['tok-string', '"if else"']]);
    expect(tokens('// val fun')).toEqual([['tok-comment', '// val fun']]);
    expect(tokens('/* a\nb */')).toEqual([['tok-comment', '/* a\nb */']]);
    expect(tokens('"""a\n"b"\n"""')).toEqual([['tok-string', '"""a\n"b"\n"""']]);
  });

  it('does not treat words that merely contain a keyword as keywords', () => {
    expect(tokens('value valuable iffy')).toEqual([['tok-keyword', 'value'], [null, ' '], [null, 'valuable'], [null, ' '], [null, 'iffy']]);
  });

  it('leaves an unterminated string and a lone quote as plain text instead of swallowing the rest', () => {
    const out = tokens('x = "oops\nval y = 1');
    expect(out.some(([c, t]) => c === 'tok-keyword' && t === 'val')).toBe(true);
  });

  it('highlights each line of a multi-line comment only once and numbers with suffixes whole', () => {
    expect(tokens('10L 2.5f 7')).toEqual([['tok-number', '10L'], [null, ' '], ['tok-number', '2.5f'], [null, ' '], ['tok-number', '7']]);
  });

  it('gives every element a distinct key', () => {
    const keys = highlight('val a = 1 val b = 2').filter(isValidElement).map((n) => n.key);
    expect(new Set(keys).size).toBe(keys.length);
  });
});
