import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { loadCourse } from '@trainer/shared/node';
import { applyServerTimeouts, createApp } from './app';
import { AnthropicGrader } from './grader';
import { AnthropicTutor } from './tutor';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const course = loadCourse(resolve(root, 'content'));

const hasCredentials = Boolean(process.env.ANTHROPIC_API_KEY || process.env.ANTHROPIC_AUTH_TOKEN);
const EFFORTS = ['low', 'medium', 'high'] as const;
const effortEnv = process.env.GRADER_EFFORT?.trim();
if (effortEnv && !EFFORTS.includes(effortEnv as (typeof EFFORTS)[number])) {
  console.error(`GRADER_EFFORT must be one of ${EFFORTS.join(', ')} (got "${effortEnv}")`);
  process.exit(1);
}
const effort = (effortEnv || undefined) as (typeof EFFORTS)[number] | undefined;
const grader = hasCredentials ? new AnthropicGrader({ model: process.env.GRADER_MODEL, effort }) : undefined;

const tutor = hasCredentials ? new AnthropicTutor({ model: process.env.EXPLAIN_MODEL ?? process.env.GRADER_MODEL, effort }) : undefined;

const port = Number(process.env.PORT?.trim() || 8787); // an empty PORT= means "default", not "any free port"
// TRUST_PROXY=1 behind one reverse proxy, so rate limits use the real client address instead of the proxy's.
const trustProxy = process.env.TRUST_PROXY?.trim() ? (/^\d+$/.test(process.env.TRUST_PROXY.trim()) ? Number(process.env.TRUST_PROXY.trim()) : process.env.TRUST_PROXY.trim()) : undefined;
const server = createApp({ course, grader, tutor, staticDir: resolve(root, 'web/dist'), trustProxy }).listen(port, () => {
  const questions = course.units.reduce((n, u) => n + u.lessons.reduce((m, l) => m + l.questions.length, 0), 0);
  console.log(`Interview Trainer API on http://localhost:${port}  (${questions} questions)`);
  console.log(
    grader
      ? `AI grading: ON (${grader.model})`
      : 'AI grading: OFF - set ANTHROPIC_API_KEY to enable; open questions fall back to self-assessment',
  );
});
applyServerTimeouts(server);
