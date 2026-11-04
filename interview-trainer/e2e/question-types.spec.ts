import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, test } from '@playwright/test';
import { allQuestions, type Lesson, type Question } from '../shared/src';
import { loadCourse } from '../shared/src/load-content';
import { KICKER, answerCorrectly, answerWrongly, check, expectNoHorizontalOverflow, next, solveAndContinue } from './solve';

const here = dirname(fileURLToPath(import.meta.url));
const course = loadCourse(resolve(here, '../content'));
const lessons = course.units.flatMap((u) => u.lessons);
const SHOTS = resolve(here, 'screenshots');

type QType = Question['type'];
const TYPES: QType[] = ['single', 'multi', 'fill', 'order', 'spot', 'estimate', 'match', 'open'];

/** The earliest question of this type in the course, with the lesson that holds it. */
function firstOf(type: QType): { lesson: Lesson; index: number } {
  for (const lesson of lessons) {
    const index = lesson.questions.findIndex((q) => q.type === type);
    if (index !== -1) return { lesson, index };
  }
  throw new Error(`no ${type} question in the course`);
}

test.describe('each question type works end to end', () => {
  for (const type of TYPES) {
    test(`${type}: answered correctly through the UI`, async ({ page }) => {
      const { lesson, index } = firstOf(type);
      await page.goto(`/#/lesson/${lesson.id}`);
      await page.locator('.prompt').waitFor();

      // Get to the question the way a learner would: answering the ones before it.
      for (const earlier of lesson.questions.slice(0, index)) await solveAndContinue(page, earlier);

      const q = lesson.questions[index]!;
      await expect(page.locator('.kicker')).toContainText(KICKER[type]);
      await expect(page.locator('.xp-chip')).toContainText(`${index * 10}`); // earlier answers were first-try
      await answerCorrectly(page, q);
      await expectNoHorizontalOverflow(page);
      await expect(page.getByRole('button', { name: 'Check', exact: true })).toBeEnabled();
      await page.screenshot({ path: `${SHOTS}/${type}-1-answered.png` });

      await check(page);
      await expect(page.locator('.feedback')).toHaveClass(/good/);
      await expect(page.locator('.lesson-bar')).toHaveClass(/good/);
      await page.screenshot({ path: `${SHOTS}/${type}-2-correct.png` });

      await expectNoHorizontalOverflow(page);

      await next(page);
      // XP is awarded: each first-try answer is worth 10; finishing a perfect lesson adds a 10 bonus.
      if (index === lesson.questions.length - 1) {
        await expect(page.locator('.tile.xp')).toContainText(`${(index + 1) * 10 + 10}`);
      } else {
        await expect(page.locator('.xp-chip')).toContainText(`${(index + 1) * 10}`);
      }
    });
  }

  test('the course covers every type this suite knows about', () => {
    const present = new Set(allQuestions(course).map((q) => q.type));
    expect([...present].sort()).toEqual([...TYPES].sort());
  });
});

test.describe('wrong answers are marked, explained and come back', () => {
  for (const type of ['single', 'spot', 'estimate', 'match'] as const) {
    test(`${type}: wrong answer shows the correction and the question returns`, async ({ page }) => {
      const { lesson, index } = firstOf(type);
      await page.goto(`/#/lesson/${lesson.id}`);
      await page.locator('.prompt').waitFor();
      for (const earlier of lesson.questions.slice(0, index)) await solveAndContinue(page, earlier);

      const q = lesson.questions[index]!;
      await answerWrongly(page, q);
      await expectNoHorizontalOverflow(page);
      await check(page);
      await expect(page.locator('.feedback')).toHaveClass(/bad/);
      await expect(page.locator('.feedback h2')).toHaveText('Not quite');
      await page.screenshot({ path: `${SHOTS}/${type}-3-wrong.png` });

      // The question comes back at the end of the lesson; here: finish the rest, then it reappears.
      await next(page);
      for (const rest of lesson.questions.slice(index + 1)) await solveAndContinue(page, rest);
      await expect(page.locator('.kicker')).toContainText('Try again');
      await solveAndContinue(page, q);
      // First attempt missed, so the lesson is not perfect, but it completes with a recovered answer.
      await expect(page.locator('.summary h1')).toHaveText('Lesson complete!');
    });
  }
});

test.describe('every lesson can be completed perfectly', () => {
  for (const lesson of lessons) {
    test(`${lesson.id}: all ${lesson.questions.length} questions are solvable`, async ({ page }) => {
      await page.goto(`/#/lesson/${lesson.id}`);
      await page.locator('.prompt').waitFor();
      for (const q of lesson.questions) await solveAndContinue(page, q);

      await expect(page.locator('.summary h1')).toHaveText('Perfect lesson!');
      await expect(page.locator('.tile.acc')).toContainText('100%');
      await page.getByRole('button', { name: 'Continue', exact: true }).click();
      await expect(page.locator('.stat').first()).toContainText('1'); // streak started
    });
  }
});
