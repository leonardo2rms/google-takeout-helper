import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
// @ts-expect-error plain .mjs module shared with the command line script
import { STYLESHEET, checkContrast, pairs, ratio, readThemes } from './contrast.mjs';

const css = readFileSync(STYLESHEET, 'utf8');

describe('design token contrast (WCAG AA, light and dark)', () => {
  it('computes ratios like the WCAG definition', () => {
    expect(ratio('#000000', '#FFFFFF')).toBeCloseTo(21, 5);
    expect(ratio('#777777', '#FFFFFF')).toBeCloseTo(4.48, 2);
  });

  it('every text and UI pairing passes in both themes, and the two dark blocks agree', () => {
    const { checked, failures } = checkContrast(css);
    expect(failures).toEqual([]);
    expect(checked).toBe(pairs().length * 2);
    expect(checked).toBeGreaterThan(200);
  });

  it('reads the tokens it checks from the stylesheet (no stale copy)', () => {
    const { light, dark } = readThemes(css);
    for (const name of ['paper', 'ink', 'graphite', 'edge', 'cobalt', 'teal', 'vermilion', 'ochre', 'sheet', 'hairline', 'code']) {
      expect(light[name], name).toMatch(/^#[0-9A-F]{6}$/);
      expect(dark[name], name).toMatch(/^#[0-9A-F]{6}$/);
    }
    expect(dark.paper).not.toBe(light.paper);
  });

  it('the checker would fail on a bad palette', () => {
    const bad = css.replace('--graphite: #4A5967;', '--graphite: #C0C8D0;');
    expect(checkContrast(bad).failures.length).toBeGreaterThan(0);
  });
});
