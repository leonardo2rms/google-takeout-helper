// WCAG 2.x contrast check for the design tokens in web/src/styles.css, in BOTH themes.
//
//   node scripts/contrast.mjs            (also runs inside `npm test`, see scripts/contrast.test.ts)
//
// The tokens are read straight from the stylesheet: the light values from the first `:root { ... }` block and the dark
// values from `:root[data-theme="dark"] { ... }`. The `@media (prefers-color-scheme: dark)` block must carry exactly the
// same values (checked too), so a change in one place cannot silently skip the other.
//
// Every pair below is a real combination used by the UI (the comment names where). Text needs 4.5:1, UI boundaries, icons and
// large fills need 3:1 (WCAG 1.4.3 / 1.4.11). Exit code 1 on any failure or missing token.
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export const TEXT = 4.5;
export const UI = 3;

const HEX = /--([\w-]+)\s*:\s*(#[0-9a-fA-F]{6})\b/g;
const tokensIn = (block) => Object.fromEntries([...block.matchAll(HEX)].map((m) => [m[1], m[2].toUpperCase()]));

/** Pulls the declaration block that follows `selector` (up to the closing brace that starts a line, or the matching indented one). */
function blockAfter(css, selector, indent = '') {
  const at = css.indexOf(selector);
  if (at === -1) throw new Error(`block not found: ${selector}`);
  const open = css.indexOf('{', at);
  const close = css.indexOf(`\n${indent}}`, open);
  return css.slice(open + 1, close);
}

export function readThemes(css) {
  const light = tokensIn(blockAfter(css, ':root {'));
  const darkExplicit = tokensIn(blockAfter(css, ':root[data-theme="dark"] {'));
  const darkMedia = tokensIn(blockAfter(css, ':root:not([data-theme="light"]) {', '  '));
  return { light, dark: { ...light, ...darkExplicit }, darkExplicit, darkMedia };
}

const channel = (hex, i) => parseInt(hex.slice(1 + i * 2, 3 + i * 2), 16) / 255;
const lin = (v) => (v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4);
export const luminance = (hex) => 0.2126 * lin(channel(hex, 0)) + 0.7152 * lin(channel(hex, 1)) + 0.0722 * lin(channel(hex, 2));
export function ratio(a, b) {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
}

const TINTED = ['cobalt', 'teal', 'vermilion', 'ochre'];
const SURFACES = ['paper', 'sheet', 'code'];
const PENS = ['kotlin', 'coroutines', 'spring', 'jvm', 'ddd', 'scale', 'complexity', 'ai', 'data', 'messaging', 'system-design', 'testing', 'security-observability'];
const TOKENS = ['keyword', 'type', 'string', 'number', 'annotation', 'comment'];

/** [foreground, background, minimum ratio, where it is used] */
export function pairs() {
  const out = [];
  const add = (fg, bg, min, use) => out.push([fg, bg, min, use]);

  for (const s of [...SURFACES, 'hairline']) {
    add('ink', s, TEXT, 'body text');
    add('graphite', s, TEXT, 'secondary text, labels, hints, code language tab');
  }
  for (const k of TINTED) {
    add('ink', `${k}-tint`, TEXT, `text on the ${k} tint (selected, verdict panels, notes)`);
    add('graphite', `${k}-tint`, TEXT, `line numbers and counters on the ${k} tint`);
  }
  add('paper', 'ink', TEXT, 'primary button, wordmark, done box');
  add('paper', 'graphite', TEXT, 'primary button while pressed');
  add('graphite', 'hairline', UI, 'disabled primary button (exempt from WCAG, kept legible)');

  for (const k of TINTED) {
    for (const bg of ['paper', 'sheet', `${k}-tint`]) {
      add(k, bg, TEXT, `${k} as text or icon label (verdict words, links, tags)`);
    }
    for (const bg of ['paper', 'sheet', `${k}-tint`]) add(k, bg, UI, `${k} as border, rail, icon`);
    add('paper', k, TEXT, `check / cross / digit drawn on a solid ${k} marker`);
  }
  add('cobalt', 'code', TEXT, 'links inside code panels');
  add('teal', 'code', UI, 'verdict icon inside a code panel');
  add('vermilion', 'code', UI, 'verdict icon inside a code panel');

  for (const s of SURFACES) add('edge', s, UI, 'control borders');
  add('ink', 'hairline', UI, 'progress fill on its track');
  add('ink', 'paper', UI, 'focus ring, gauge');
  add('cobalt', 'paper', UI, 'focus ring');
  add('cobalt', 'sheet', UI, 'focus ring');
  add('cobalt', 'code', UI, 'focus ring');

  for (const p of PENS) for (const bg of ['paper', 'sheet']) add(`pen-${p}`, bg, UI, 'unit glyph outline and spine');

  for (const t of TOKENS) {
    for (const bg of ['code', 'sheet', 'cobalt-tint', 'teal-tint', 'vermilion-tint']) add(`tok-${t}`, bg, TEXT, 'syntax colour');
  }
  return out;
}

/** Returns every failing or unresolvable pairing in both themes. */
export function checkContrast(css) {
  const { light, dark, darkExplicit, darkMedia } = readThemes(css);
  const failures = [];
  let checked = 0;
  for (const [name, theme] of [['light', light], ['dark', dark]]) {
    for (const [fg, bg, min, use] of pairs()) {
      checked++;
      if (!theme[fg] || !theme[bg]) {
        failures.push(`MISSING token ${!theme[fg] ? fg : bg} (${name}: ${fg} on ${bg})`);
        continue;
      }
      const r = ratio(theme[fg], theme[bg]);
      if (r < min) failures.push(`FAIL ${name}: ${fg} ${theme[fg]} on ${bg} ${theme[bg]} = ${r.toFixed(2)}, needs ${min} (${use})`);
    }
  }
  // The two dark blocks (OS preference and explicit data-theme) must define identical values.
  for (const key of new Set([...Object.keys(darkExplicit), ...Object.keys(darkMedia)])) {
    if (darkExplicit[key] !== darkMedia[key]) failures.push(`DARK MISMATCH --${key}: data-theme=${darkExplicit[key]} media=${darkMedia[key]}`);
  }
  return { checked, failures };
}

const here = dirname(fileURLToPath(import.meta.url));
export const STYLESHEET = resolve(here, '../web/src/styles.css');

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const { checked, failures } = checkContrast(readFileSync(process.argv[2] ?? STYLESHEET, 'utf8'));
  for (const f of failures) console.log(f);
  console.log(failures.length ? `${failures.length} problem(s) in ${checked} pairings` : `OK: ${checked} pairings pass in light and dark`);
  process.exit(failures.length ? 1 : 0);
}
