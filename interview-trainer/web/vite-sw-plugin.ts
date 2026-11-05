import { createHash } from 'node:crypto';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, relative, resolve, sep } from 'node:path';
import { build } from 'esbuild';
import type { Plugin } from 'vite';

/** Every file below `dir`, as forward-slash paths relative to it, sorted. */
export function listFiles(dir: string, base = dir): string[] {
  const out: string[] = [];
  for (const name of readdirSync(dir)) {
    const full = join(dir, name);
    if (statSync(full).isDirectory()) out.push(...listFiles(full, base));
    else out.push(relative(base, full).split(sep).join('/'));
  }
  return out.sort();
}

/** The paths the worker pre-caches, and a version that changes whenever any of those files' bytes change. */
export function precacheManifest(dir: string): { precache: string[]; version: string } {
  const files = listFiles(dir).filter((f) => f !== 'sw.js' && !f.endsWith('.map'));
  const hash = createHash('sha256');
  for (const f of files) hash.update(f).update('\0').update(readFileSync(join(dir, f))).update('\0');
  return { precache: ['/', ...files.filter((f) => f !== 'index.html').map((f) => `/${f}`)], version: hash.digest('hex').slice(0, 12) };
}

/**
 * After the app is built, bundles src/sw/sw.ts into `<outDir>/sw.js` with this build's file list and a version baked in.
 * A new deploy therefore always produces a byte-different worker, which is what makes browsers install the update.
 */
export function serviceWorker(): Plugin {
  let root = '';
  let outDir = '';
  return {
    name: 'service-worker',
    apply: 'build',
    configResolved(config) {
      root = config.root;
      outDir = resolve(root, config.build.outDir);
    },
    async writeBundle() {
      const { precache, version } = precacheManifest(outDir);
      await build({
        entryPoints: [resolve(root, 'src/sw/sw.ts')],
        outfile: join(outDir, 'sw.js'),
        bundle: true,
        format: 'iife',
        target: 'es2020',
        minify: true,
        define: { __PRECACHE__: JSON.stringify(precache), __VERSION__: JSON.stringify(version) },
        logLevel: 'warning',
      });
    },
  };
}
