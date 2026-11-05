import { readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { ACHIEVEMENTS } from '../shared/src';
import { loadCourse } from '../shared/src/load-content';
import { unitStyle, FALLBACK_UNIT_ICON } from '../web/src/lib/unit-style';
import { ICONS, isIconName } from '../web/src/components/icons';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const course = loadCourse(join(root, 'content'));

describe('icons', () => {
  it('every unit in the course names an icon the app ships', () => {
    for (const u of course.units) {
      expect(u.icon, `unit ${u.id} has no icon`).toBeDefined();
      expect(isIconName(u.icon), `unit ${u.id}: unknown icon "${u.icon}"`).toBe(true);
    }
  });

  it('every badge names an icon the app ships', () => {
    for (const a of ACHIEVEMENTS) expect(isIconName(a.icon), `badge ${a.id}: unknown icon "${a.icon}"`).toBe(true);
  });

  it('unknown or missing unit icons fall back instead of breaking', () => {
    expect(unitStyle({ id: 'brand-new' }).icon).toBe(FALLBACK_UNIT_ICON);
    expect(unitStyle({ id: 'brand-new', icon: 'nope' }).icon).toBe(FALLBACK_UNIT_ICON);
    expect(unitStyle({ id: 'kotlin', icon: 'braces' }).style).toMatchObject({ '--pen': 'var(--pen-kotlin)' });
    expect(unitStyle({ id: 'brand-new' }).style).toMatchObject({ '--pen': 'var(--cobalt)' });
  });

  it('icon markup is plain shapes (no scripts, no handlers)', () => {
    for (const [name, markup] of Object.entries(ICONS)) {
      expect(markup, name).not.toMatch(/<script|on\w+=|href=/i);
      expect(markup, name).toMatch(/^<(path|circle|rect|line|ellipse|polyline)/);
    }
  });
});

describe('no emoji as UI icons', () => {
  const EMOJI = /[\u{1F000}-\u{1FFFF}\u{2190}-\u{21FF}\u{2300}-\u{23FF}\u{2600}-\u{27BF}\u{2B00}-\u{2BFF}️✓✗★☆]/u;
  const files = (dir: string): string[] =>
    readdirSync(dir).flatMap((f) => {
      const p = join(dir, f);
      return statSync(p).isDirectory() ? files(p) : /\.(tsx?|css|html)$/.test(f) && !/\.test\.ts$/.test(f) ? [p] : [];
    });

  it('the web sources and the shared badge definitions use icon names, not emoji or text glyphs', () => {
    const offenders: string[] = [];
    for (const f of [...files(join(root, 'web/src')), join(root, 'web/index.html'), join(root, 'shared/src/achievements.ts')]) {
      readFileSync(f, 'utf8').split('\n').forEach((line, i) => {
        if (EMOJI.test(line)) offenders.push(`${f.replace(root + '/', '')}:${i + 1}: ${line.trim().slice(0, 80)}`);
      });
    }
    expect(offenders).toEqual([]);
  });
});
