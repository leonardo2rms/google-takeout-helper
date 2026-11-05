import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: { include: ['scripts/**/*.test.ts', 'shared/src/**/*.test.ts', 'server/src/**/*.test.ts', 'web/src/**/*.test.ts', 'content/**/*.test.ts'] },
});
