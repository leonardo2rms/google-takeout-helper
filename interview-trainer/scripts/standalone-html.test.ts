/** The single-file build's two HTML shapes (QA S1-7): the document is valid on its own, the fragment is safe to embed. */
import { describe, expect, it } from 'vitest';
// @ts-expect-error plain .mjs module shared with the build script
import { buildDocument, buildFragment, escapeInlineScript } from './standalone-html.mjs';

const parts = { style: 'body{color:red}', script: 'console.log("a·b")' };

describe('escapeInlineScript', () => {
  it('cannot be ended early by the bundle, in any case, and does not open a comment', () => {
    const out: string = escapeInlineScript('x</script>y</SCRIPT >z<!--w');
    expect(out).not.toMatch(/<\/script/i);
    expect(out).not.toContain('<!--');
    expect(out).toContain('<\\/script>');
  });
});

describe('document', () => {
  const html: string = buildDocument(parts);

  it('starts with a doctype (no quirks mode) and declares the language', () => {
    expect(html.startsWith('<!doctype html>\n<html lang="en">')).toBe(true);
  });

  it('declares UTF-8 within the first 1024 bytes, so the middle dot is not read as Latin-1', () => {
    expect(html.slice(0, 1024)).toContain('<meta charset="utf-8">');
    expect(html.indexOf('<meta charset')).toBeLessThan(html.indexOf('<title>'));
  });

  it('has a mobile viewport, a title, the root element and exactly one inline module script', () => {
    expect(html).toContain('<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">');
    expect(html).toContain('<title>Interview Trainer</title>');
    expect(html).toContain('<div id="root"></div>');
    expect(html.match(/<script/g)).toHaveLength(1);
    expect(html.match(/<\/script>/g)).toHaveLength(1);
    expect(html).toContain('body{color:red}');
    expect(html.trimEnd().endsWith('</html>')).toBe(true);
  });

  it('keeps non-ASCII characters of the bundle intact', () => {
    expect(html).toContain('a·b');
  });
});

describe('fragment', () => {
  const html: string = buildFragment(parts);

  it('has no document wrapper (the host supplies it) but leads with the charset and viewport', () => {
    expect(html).not.toMatch(/<!doctype|<html|<body/i);
    expect(html.startsWith('<meta charset="utf-8">')).toBe(true);
    expect(html).toContain('name="viewport"');
  });

  it('contains the same app: title, style, root and a single script', () => {
    expect(html).toContain('<title>Interview Trainer</title>');
    expect(html).toContain('<div id="root"></div>');
    expect(html.match(/<script/g)).toHaveLength(1);
    expect(html.match(/<\/script>/g)).toHaveLength(1);
  });
});
