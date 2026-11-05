/**
 * Keeps the documentation honest: every YAML example in docs/content-guide.md must validate (and meet the content rules),
 * every `npm run <script>` the docs mention must exist, and every repository path they mention must exist.
 */
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { parse } from 'yaml';
import { loadCourse } from '../shared/src/load-content';
import { Course, allQuestions, heuristicCoverage, openParts, runCodeTests, scoreFromCoverage, validateCourse, type CodeQ, type OpenQ } from '../shared/src';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const docsDir = resolve(root, 'docs');
const read = (f: string) => readFileSync(resolve(root, f), 'utf8');
const docFiles = ['README.md', ...readdirSync(docsDir).filter((f) => f.endsWith('.md')).map((f) => `docs/${f}`)];

/** Fenced blocks of one language, with their starting line. */
function blocks(markdown: string, lang: string): { text: string; line: number }[] {
  const out: { text: string; line: number }[] = [];
  const re = new RegExp('^```' + lang + '\\s*$([\\s\\S]*?)^```\\s*$', 'gm');
  for (const m of markdown.matchAll(re)) out.push({ text: m[1]!.trim(), line: markdown.slice(0, m.index).split('\n').length });
  return out;
}

const filler = (n: number) =>
  Array.from({ length: n }, (_, i) => ({ type: 'single', prompt: `filler ${i}`, explanation: 'e', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a' }));

describe('docs/content-guide.md examples', () => {
  const guide = read('docs/content-guide.md');
  const yamlBlocks = blocks(guide, 'yaml');

  it('has examples for all nine question types, a lesson skeleton and a unit', () => {
    const types = new Set(yamlBlocks.flatMap((b) => [...b.text.matchAll(/^- type: (\w+)/gm)].map((m) => m[1])));
    for (const t of ['single', 'multi', 'fill', 'order', 'spot', 'estimate', 'match', 'code', 'open']) expect(types, t).toContain(t);
    expect(yamlBlocks.some((b) => b.text.startsWith('id:'))).toBe(true);
  });

  it.each(yamlBlocks.map((b) => [`line ${b.line}`, b.text] as const))('the YAML block at %s validates', (_name, text) => {
    const parsed: unknown = parse(text);
    let units: unknown[];
    if (Array.isArray(parsed)) {
      // A list of questions: wrap them in a lesson, padded to the 3-question minimum.
      const questions = [...(parsed as unknown[]), ...filler(Math.max(0, 3 - parsed.length))];
      units = [{ id: 'doc', title: 'Doc', emoji: 'x', description: 'd', lessons: [{ id: 'doc-l', title: 'L', questions }] }];
    } else {
      units = [parsed];
    }
    const res = Course.safeParse({ units });
    expect(res.success, res.success ? '' : JSON.stringify(res.error.issues.slice(0, 3))).toBe(true);
    const course = res.data!;
    expect(validateCourse(course)).toEqual([]);

    for (const q of allQuestions(course)) {
      if (q.type === 'open') {
        for (const part of openParts(q as OpenQ)) {
          const covered = heuristicCoverage(part, part.modelAnswer);
          expect(part.keyPoints.filter((_, i) => !covered[i]).map((k) => k.point), `${part.id}: model answer misses its own keywords`).toEqual([]);
          expect(scoreFromCoverage(heuristicCoverage(part, 'I do not know'))).toBeLessThan(0.6);
        }
      }
      if (q.type === 'code') {
        const c = q as CodeQ;
        expect(runCodeTests(c.solution, c.entry, c.tests).ok, `${c.id}: solution`).toBe(true);
        const starter = runCodeTests(c.starter, c.entry, c.tests);
        expect(starter.ok).toBe(false);
        expect(starter.fatal).toBeUndefined();
        expect(c.kotlin).toBeTruthy();
        expect(c.tests.some((t) => t.hidden)).toBe(true);
      }
    }
  });
});

const GENERATED = new Set(['web/dist', 'web/dist-standalone', 'e2e/.results']);

describe('commands and paths mentioned in the docs', () => {
  const scripts = Object.keys((JSON.parse(read('package.json')) as { scripts: Record<string, string> }).scripts);
  const workspaceScripts = new Set(
    ['shared', 'server', 'web'].flatMap((w) => Object.keys((JSON.parse(read(`${w}/package.json`)) as { scripts?: Record<string, string> }).scripts ?? {})),
  );

  it.each(docFiles)('%s: every `npm run` script exists', (file) => {
    const text = read(file);
    const named = [...text.matchAll(/npm run ([\w:-]+)(?: -w (\w+))?/g)];
    for (const [, script, workspace] of named) {
      if (workspace) expect(workspaceScripts, `${file}: npm run ${script} -w ${workspace}`).toContain(script);
      else expect(scripts, `${file}: npm run ${script}`).toContain(script);
    }
  });

  it.each(docFiles)('%s: every repository path in backticks exists', (file) => {
    const text = read(file);
    const dirs = ['shared', 'server', 'web', 'e2e', 'content', 'scripts', 'docs'];
    const candidates = [...text.matchAll(/`((?:interview-trainer\/)?(?:shared|server|web|e2e|content|scripts|docs)\/[A-Za-z0-9_./@-]+)`/g)].map((m) => m[1]!);
    for (let path of candidates) {
      path = path.replace(/^interview-trainer\//, '').replace(/[.,;:]+$/, '');
      if (/[*<>]|NN-|\.\.\.|<|YYYY/.test(path)) continue; // a pattern or a placeholder, not a file
      if (path.endsWith('/')) path = path.slice(0, -1);
      if (GENERATED.has(path)) continue; // produced by a build or a test run, absent in a clean checkout
      expect(dirs.some((d) => path === d || path.startsWith(`${d}/`)), path).toBe(true);
      expect(existsSync(resolve(root, path)), `${file}: ${path} does not exist`).toBe(true);
    }
  });

  it('the documented environment variables are the ones the server reads', () => {
    const main = read('server/src/main.ts');
    const arch = read('docs/architecture.md');
    for (const name of ['ANTHROPIC_API_KEY', 'ANTHROPIC_AUTH_TOKEN', 'GRADER_MODEL', 'EXPLAIN_MODEL', 'GRADER_EFFORT', 'PORT']) {
      expect(main, name).toContain(name);
      expect(arch, name).toContain(`\`${name}\``);
    }
  });
});

/** GitHub's heading anchor: lower case, punctuation dropped, spaces to dashes. */
const slug = (heading: string) => heading.trim().toLowerCase().replace(/[^\p{L}\p{N}\s_-]/gu, '').replace(/\s/g, '-');

describe('links between the docs', () => {
  const anchorsOf = (file: string) => new Set([...read(file).matchAll(/^#{1,6}\s+(.+?)\s*$/gm)].map((m) => slug(m[1]!)));

  it.each(docFiles)('%s: relative links point at real files and headings', (file) => {
    const dir = dirname(resolve(root, file));
    const text = read(file).replace(/^```[\s\S]*?^```/gm, ''); // ignore code blocks
    for (const m of text.matchAll(/\]\((?!https?:|mailto:)([^)\s#]*)(?:#([^)\s]+))?\)/g)) {
      const [, target, anchor] = m;
      const targetPath = target ? resolve(dir, target) : resolve(root, file);
      expect(existsSync(targetPath), `${file}: link to ${target} is broken`).toBe(true);
      if (anchor && targetPath.endsWith('.md')) {
        const rel = targetPath.slice(root.length + 1);
        expect(anchorsOf(rel), `${file}: ${target ?? ''}#${anchor} has no such heading`).toContain(anchor);
      }
    }
  });
});


describe('README facts', () => {
  it('states the real number of questions, units and lessons', () => {
    const course = loadCourse(resolve(root, 'content'));
    const counts = `${allQuestions(course).length} questions / ${course.units.length} units / ${course.units.reduce((n, u) => n + u.lessons.length, 0)} lessons`;
    expect(read('README.md')).toContain(`**${counts}**`);
  });
});
