import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';
import { allQuestions } from './schema';
import { contentFiles, loadCourse } from './load-content';

const here = dirname(fileURLToPath(import.meta.url));
const dirs: string[] = [];
function dirWith(files: Record<string, string>): string {
  const dir = mkdtempSync(join(tmpdir(), 'trainer-content-'));
  dirs.push(dir);
  for (const [name, text] of Object.entries(files)) writeFileSync(join(dir, name), text);
  return dir;
}
afterEach(() => dirs.splice(0).forEach((d) => rmSync(d, { recursive: true, force: true })));

const single = (n: number, answer = 'a') =>
  `      - { type: single, prompt: "p${n}", explanation: e, options: [{ id: a, text: A }, { id: b, text: B }], answer: ${answer} }`;
const unit = (id: string, answer = 'a') => `id: ${id}
title: ${id}
emoji: x
description: d
lessons:
  - id: ${id}-l
    title: L
    questions:
${single(1, answer)}
${single(2)}
${single(3)}
`;

describe('contentFiles', () => {
  it('lists only .yaml/.yml files, sorted by name, ignoring everything else', () => {
    const dir = dirWith({ '02-b.yaml': '', '01-a.yml': '', 'notes.md': '', 'c.yaml.bak': '', '10-z.yaml': '' });
    expect(contentFiles(dir).map((f) => f.slice(dir.length + 1))).toEqual(['01-a.yml', '02-b.yaml', '10-z.yaml']);
  });
});

describe('loadCourse', () => {
  it('loads units in file-name order', () => {
    const dir = dirWith({ '02-second.yaml': unit('second'), '01-first.yaml': unit('first') });
    expect(loadCourse(dir).units.map((u) => u.id)).toEqual(['first', 'second']);
  });

  it('reports the file name when the YAML cannot be parsed', () => {
    const dir = dirWith({ '01-bad.yaml': 'id: [unclosed' });
    expect(() => loadCourse(dir)).toThrow(/01-bad\.yaml/);
  });

  it('lists schema problems with their path', () => {
    const dir = dirWith({ '01-a.yaml': 'id: a\ntitle: A\n' });
    expect(() => loadCourse(dir)).toThrow(/Invalid course content:[\s\S]*units\.0\./);
  });

  it('runs the cross-field validation (an answer that is not an option)', () => {
    const dir = dirWith({ '01-a.yaml': unit('a', 'zzz') });
    expect(() => loadCourse(dir)).toThrow(/Invalid course content/);
  });

  it('loads the real course, with unique ids', () => {
    const course = loadCourse(resolve(here, '../../content'));
    const ids = allQuestions(course).map((q) => q.id);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids.length).toBeGreaterThan(100);
  });
});
