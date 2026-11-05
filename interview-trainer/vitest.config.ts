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
        '**/arbitraries.ts', // test generators
        '**/*.d.ts',
        '**/*.worker.ts', // runs only inside a real Web Worker; exercised by e2e
        '**/index.ts', // barrel re-exports
        'server/src/main.ts', // process entry point
      ],
      reporter: ['text-summary', 'text', 'lcov'],
      reportsDirectory: 'coverage',
      // A ratchet: set slightly below what the suite achieves today, so coverage cannot silently regress.
      // Raise these when coverage goes up; never lower them to make a failing run pass (add the missing test instead).
      // Achieved when set (statements / branches / functions / lines):
      //   shared 99.7 / 97.0 / 99.8 / 100,  server 100 / 97.2 / 100 / 100,  web/src/lib 99.8 / 98.7 / 100 / 99.7
      thresholds: {
        statements: 99,
        branches: 96,
        functions: 99,
        lines: 99,
        'shared/src/**': { statements: 99, branches: 96, functions: 99, lines: 99 },
        'server/src/**': { statements: 99, branches: 96, functions: 99, lines: 99 },
        'web/src/lib/**': { statements: 98, branches: 97, functions: 99, lines: 98 },
      },
    },
  },
});
