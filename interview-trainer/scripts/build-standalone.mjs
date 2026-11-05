// Builds web/ in "standalone" mode and inlines everything into single files, in two shapes (see standalone-html.mjs):
//   interview-trainer/dist/interview-trainer.html             a fragment, for hosts that wrap it in their own page (a claude.ai artifact)
//   interview-trainer/dist/interview-trainer.standalone.html  a complete document, for opening from disk (file://) or any static host
// No API server, no service worker, AI grading through the host's `sample` capability when it offers one, file offers
// (backup, share image) through its `downloads` capability. Publish the fragment as an artifact with
// capabilities: {"sample": {}, "downloads": true}.
import { execFileSync } from 'node:child_process';
import { mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildDocument, buildFragment } from './standalone-html.mjs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const web = join(root, 'web');
const out = join(web, 'dist-standalone');

execFileSync('npx', ['vite', 'build', '--mode', 'standalone', '--outDir', 'dist-standalone', '--emptyOutDir'], {
  cwd: web,
  stdio: 'inherit',
});

const assets = join(out, 'assets');
const files = readdirSync(assets);
const js = files.filter((f) => f.endsWith('.js'));
const css = files.filter((f) => f.endsWith('.css'));
if (js.length !== 1 || css.length > 1) throw new Error(`expected 1 js and <=1 css, got ${files.join(', ')}`);

const script = readFileSync(join(assets, js[0]), 'utf8');
const style = css.length ? readFileSync(join(assets, css[0]), 'utf8') : '';

const dest = join(root, 'dist');
mkdirSync(dest, { recursive: true });
for (const [name, html] of [
  ['interview-trainer.html', buildFragment({ style, script })],
  ['interview-trainer.standalone.html', buildDocument({ style, script })],
]) {
  const file = join(dest, name);
  writeFileSync(file, html);
  console.log(`Wrote ${file} (${(html.length / 1024).toFixed(0)} kB)`);
}

console.log('Publish as an artifact with capabilities: {"sample": {}, "downloads": true}');
