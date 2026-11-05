import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { defineConfig, type Plugin } from 'vitest/config';
// Relative import on purpose (same reason as web/vite.config.ts): this file is bundled before workspaces resolve.
import { loadCourse } from './shared/src/load-content';

const root = fileURLToPath(new URL('.', import.meta.url));

/** Same `virtual:course` module the web build uses, so web/src/lib code that imports the course can be tested. */
function virtualCourse(): Plugin {
  const id = 'virtual:course';
  return {
    name: 'virtual-course',
    resolveId: (source) => (source === id ? `\0${id}` : null),
    load: (source) => (source === `\0${id}` ? `export default ${JSON.stringify(loadCourse(resolve(root, 'content')))};` : null),
  };
}

export default defineConfig({
  plugins: [virtualCourse()],
  test: {
    include: ['shared/src/**/*.test.ts', 'server/src/**/*.test.ts', 'web/src/**/*.test.ts', 'content/**/*.test.ts'],
    coverage: {
      provider: 'v8',
      include: ['shared/src/**', 'server/src/**', 'web/src/lib/**'],
      exclude: [
        '**/*.test.ts',
        '**/__fixtures__/**',
        '**/fixtures.ts',
        '**/*.d.ts',
        '**/*.worker.ts', // runs only inside a real Web Worker; exercised by e2e
        '**/index.ts', // barrel re-exports
        'server/src/main.ts', // process entry point
      ],
      reporter: ['text-summary', 'text', 'lcov'],
      reportsDirectory: 'coverage',
    },
  },
});
