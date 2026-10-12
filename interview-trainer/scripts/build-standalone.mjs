// Builds web/ in "standalone" mode and inlines everything into ONE html fragment:
//   interview-trainer/dist/interview-trainer.html
// Meant for hosts that serve a single page (e.g. a claude.ai artifact): no API server, no service
// worker, AI grading through the host's `sample` capability when it offers one.
import { execFileSync } from 'node:child_process';
import { mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

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

// An inline <script> ends at the first "</script"; escape it inside the bundle.
const script = readFileSync(join(assets, js[0]), 'utf8').replace(/<\/script/gi, '<\\/script');
const style = css.length ? readFileSync(join(assets, css[0]), 'utf8') : '';

const html = `<title>Interview Trainer</title>
<style>
${style}
</style>
<div id="root"></div>
<script type="module">
${script}
</script>
`;

const dest = join(root, 'dist');
mkdirSync(dest, { recursive: true });
const file = join(dest, 'interview-trainer.html');
writeFileSync(file, html);
console.log(`\nWrote ${file} (${(html.length / 1024).toFixed(0)} kB)`);
