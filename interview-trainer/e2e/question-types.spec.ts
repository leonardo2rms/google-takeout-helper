import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, test } from '@playwright/test';
import { allQuestions, type Lesson, type Question } from '../shared/src';
import { loadCourse } from '../shared/src/load-content';
import { KICKER, answerCorrectly, answerFollowUpIfAny, answerWrongly, check, expectNoHorizontalOverflow, next, runTests, solveAndContinue, visible, writeCode } from './solve';

const here = dirname(fileURLToPath(import.meta.url));
const course = loadCourse(resolve(here, '../content'));
const lessons = course.units.flatMap((u) => u.lessons);
const SHOTS = resolve(here, 'screenshots');

type QType = Question['type'];
const TYPES: QType[] = ['single', 'multi', 'fill', 'order', 'spot', 'estimate', 'match', 'code', 'open'];

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
      // A pending interviewer follow-up keeps the verdict open, so the bar stays neutral until it is answered.
      const followUpPending = q.type === 'open' && q.followUps.length > 0;

      await check(page);
      await expect(page.locator('.feedback')).toHaveClass(/good/);
      if (followUpPending) await expect(page.locator('.interviewer-note')).toBeVisible();
      else await expect(page.locator('.lesson-bar')).toHaveClass(/good/);
      await page.screenshot({ path: `${SHOTS}/${type}-2-correct.png` });

      await expectNoHorizontalOverflow(page);

      await next(page);
      if (q.type === 'open' && (await answerFollowUpIfAny(page, q))) {
        await page.screenshot({ path: `${SHOTS}/open-3-followup.png` });
        await next(page);
      }
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

test.describe('code questions run in a sandboxed worker', () => {
  const { lesson, index } = firstOf('code');
  const q = lesson.questions[index]!;
  if (q.type !== 'code') throw new Error('expected a code question');

  async function openIt(page: import('@playwright/test').Page) {
    await page.goto(`/#/lesson/${lesson.id}`);
    await page.locator('.prompt').waitFor();
    for (const earlier of lesson.questions.slice(0, index)) await solveAndContinue(page, earlier);
    await expect(page.locator('.kicker')).toContainText('Write the code');
  }

  test('the unedited starter fails, shows which tests failed, and reveals the reference solution', async ({ page }) => {
    await openIt(page);
    await runTests(page);
    await expect(page.locator('.tests li.fail').first()).toBeVisible();
    await check(page);
    await expect(page.locator('.feedback')).toHaveClass(/bad/);
    await expect(page.getByText('Reference solution')).toBeVisible();
    await expect(page.getByText('The same idea in Kotlin:')).toBeVisible();
    await page.screenshot({ path: `${SHOTS}/code-3-wrong.png` });
  });

  test('an infinite loop times out without freezing the page; syntax errors are reported', async ({ page }) => {
    await openIt(page);
    await writeCode(page, `function ${q.entry}() { while (true) {} }`);
    await runTests(page);
    await expect(page.locator('.test-fatal')).toContainText('Timed out');
    await page.screenshot({ path: `${SHOTS}/code-4-timeout.png` });
    // The page is still alive: the button is usable again and a syntax error is reported promptly.
    await writeCode(page, `function ${q.entry}( {`);
    await runTests(page);
    await expect(page.locator('.test-fatal')).toContainText('Syntax error');
  });

  test('learner code cannot reach the network or storage', async ({ page }) => {
    await openIt(page);
    await writeCode(page, `function ${q.entry}() { return [typeof fetch, typeof XMLHttpRequest, typeof WebSocket, typeof indexedDB, typeof importScripts].join(','); }`);
    await runTests(page);
    // Tests fail (wrong return value), and the "got" column shows what the sandbox exposed.
    await expect(page.locator('.tests')).toContainText('undefined,undefined,undefined,undefined,undefined');
  });

  test('the symbol bar inserts characters at the cursor', async ({ page }) => {
    await openIt(page);
    const editor = page.getByRole('textbox', { name: 'Your code' });
    await editor.fill('');
    await page.getByRole('group', { name: 'Code symbols' }).getByRole('button', { name: '{', exact: true }).click();
    await page.getByRole('group', { name: 'Code symbols' }).getByRole('button', { name: '}', exact: true }).click();
    await expect(editor).toHaveValue('{}');
  });
});

test.describe('interviewer follow-ups adapt to the answer', () => {
  // The first open question that has both a gap-probing and a go-deeper follow-up.
  const found = lessons
    .flatMap((l) => l.questions.map((q, i) => ({ l, q, i })))
    .find((x) => x.q.type === 'open' && x.q.followUps.some((f) => f.when === 'missed') && x.q.followUps.some((f) => f.when === 'covered'))!;
  const q = found.q as import('../shared/src').OpenQ;
  const expectedStrong = q.followUps.find((f) => f.when === 'covered')!;
  const missedFirst = q.followUps.find((f) => f.when === 'missed')!;

  async function toTheOpenQuestion(page: import('@playwright/test').Page) {
    await page.goto(`/#/lesson/${found.l.id}`);
    await page.locator('.prompt').waitFor();
    for (const earlier of found.l.questions.slice(0, found.i)) await solveAndContinue(page, earlier);
  }

  test('a strong answer gets a deeper question about something it got right', async ({ page }) => {
    await toTheOpenQuestion(page);
    await answerCorrectly(page, q);
    await check(page);
    await next(page);
    await expect(page.locator('.kicker')).toContainText('Interviewer follow-up');
    await expect(page.locator('.prompt')).toContainText(visible(expectedStrong.prompt).slice(0, 25));
  });

  test('a weak answer gets a question that probes the gap, and the question needs both answers to pass', async ({ page }) => {
    await toTheOpenQuestion(page);
    await page.getByRole('textbox', { name: 'Your answer' }).fill('I am not sure about this one.');
    await check(page);
    // Self-assessment: untick everything, as an honest learner who covered nothing would.
    const boxes = page.locator('.rubric input[type="checkbox"]');
    for (let i = 0; i < (await boxes.count()); i++) if (await boxes.nth(i).isChecked()) await boxes.nth(i).uncheck();
    await expect(page.locator('.feedback')).toHaveClass(/bad/);
    await next(page);

    await expect(page.locator('.kicker')).toContainText('Interviewer follow-up');
    await expect(page.locator('.prompt')).toContainText(visible(missedFirst.prompt).slice(0, 25));

    // Nail the follow-up: 0% + 100% averages to 50%, which is below the pass mark.
    await page.getByRole('textbox', { name: 'Your answer' }).fill(missedFirst.modelAnswer);
    await check(page);
    await expect(page.locator('.overall')).toContainText('50%');
    await expect(page.locator('.overall')).toContainText('not enough');
    await expect(page.locator('.lesson-bar')).toHaveClass(/bad/);
    await page.screenshot({ path: `${SHOTS}/open-4-followup-weak.png` });

    // The question comes back later in the lesson ("Try again").
    await next(page);
    for (const rest of found.l.questions.slice(found.i + 1)) await solveAndContinue(page, rest);
    await expect(page.locator('.kicker')).toContainText('Try again');
  });

  test('skipping the main question skips the follow-up too', async ({ page }) => {
    await toTheOpenQuestion(page);
    await page.getByRole('button', { name: "I don't know", exact: true }).click();
    await page.locator('.feedback').waitFor();
    await expect(page.locator('.interviewer-note')).toHaveCount(0);
  });
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
