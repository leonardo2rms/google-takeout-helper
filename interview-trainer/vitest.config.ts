import { defineConfig } from 'vitest/config';

export default defineConfig({
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
