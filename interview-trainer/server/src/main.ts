import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { loadCourse } from '@trainer/shared/node';
import { createApp } from './app';
import { AnthropicGrader } from './grader';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const course = loadCourse(resolve(root, 'content'));

const hasCredentials = Boolean(process.env.ANTHROPIC_API_KEY || process.env.ANTHROPIC_AUTH_TOKEN);
const effort = process.env.GRADER_EFFORT as 'low' | 'medium' | 'high' | undefined;
const grader = hasCredentials ? new AnthropicGrader({ model: process.env.GRADER_MODEL, effort }) : undefined;

const port = Number(process.env.PORT ?? 8787);
createApp({ course, grader, staticDir: resolve(root, 'web/dist') }).listen(port, () => {
  const questions = course.units.reduce((n, u) => n + u.lessons.reduce((m, l) => m + l.questions.length, 0), 0);
  console.log(`Interview Trainer API on http://localhost:${port}  (${questions} questions)`);
  console.log(
    grader
      ? `AI grading: ON (${grader.model})`
      : 'AI grading: OFF - set ANTHROPIC_API_KEY to enable; open questions fall back to self-assessment',
  );
});
