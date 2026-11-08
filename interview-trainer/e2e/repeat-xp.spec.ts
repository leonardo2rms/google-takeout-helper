import { expect, test, type Page } from '@playwright/test';
import { KEY_V2, SHOTS, TODAY, day, lessons, seed, setDay, stored, useClock, v2 } from './engage';
import { answerCorrectly, check, next, solveAndContinue } from './solve';

/**
 * Doing the same question again the same day pays little XP and does not count for the daily goal again (S2-11), so
 * repeating one question cannot fill the goal or farm XP. The next day it pays in full again.
 */

const lesson = lessons[0]!;
const total = lesson.questions.length;
const XP_REPEAT = 2;
const XP_FIRST = 10;
const BONUS = 10;

const xpChip = (page: Page) => page.locator('.xp-chip');
const notes = (page: Page) => page.getByTestId('summary-notes');

/** Plays the lesson from the first question to its summary, every answer right the first time. */
async function playWholeLesson(page: Page): Promise<void> {
  await page.goto(`/#/lesson/${lesson.id}`);
  await page.locator('.prompt').waitFor();
  for (const q of lesson.questions) await solveAndContinue(page, q);
  await expect(page).toHaveURL(/#\/done$/);
}

test.beforeEach(async ({ page }) => {
  await useClock(page);
  await seed(page, { [KEY_V2]: v2() });
});

test.describe('the same lesson twice in one day', () => {
  test('the first time pays in full; the second pays XP_REPEAT per question, no bonus, and adds nothing to the goal', async ({ page }) => {
    await playWholeLesson(page);
    const first = (await stored(page))!;
    expect(first.xp).toBe(total * XP_FIRST + BONUS);
    expect(first.days[TODAY].answered).toBe(total);
    await expect(page.getByText('already done today')).toHaveCount(0); // nothing was a repeat

    await playWholeLesson(page);
    const second = (await stored(page))!;
    expect(second.xp - first.xp).toBe(total * XP_REPEAT);
    expect(second.days[TODAY].answered).toBe(total); // the goal did not move
    expect(second.days[TODAY].xp).toBe(second.xp);
    expect(second.stats.answered).toBe(total * 2); // but it was practice, and counts as practice
    await expect(notes(page)).toContainText(`${total} questions you had already done today`);
    await expect(notes(page)).toContainText(`${XP_REPEAT} XP each`);
    await page.screenshot({ path: `${SHOTS}/repeat-summary.png` });
    // the goal sheet states the rule the summary just applied (and says where interviewer follow-ups fit)
    await page.getByRole('button', { name: 'Back home', exact: true }).click();
    await page.getByTestId('goal-ring').click();
    await expect(page.getByTestId('goal-rule')).toContainText('Each question counts once a day');
    await expect(page.getByTestId('goal-rule')).toContainText('follow-up belongs to its question');
  });

  test('a repeated question says so, and the XP in the header is the reduced one', async ({ page }) => {
    await playWholeLesson(page);
    await page.goto(`/#/lesson/${lesson.id}`);
    await page.locator('.prompt').waitFor();
    await expect(page.getByTestId('repeat-note')).toContainText('earns less XP');
    await expect(xpChip(page)).toHaveAttribute('aria-label', '0 XP earned');
    await answerCorrectly(page, lesson.questions[0]!);
    await check(page);
    // the reward is shown the moment the answer is checked, and it is the reduced one
    await expect(xpChip(page)).toHaveAttribute('aria-label', `${XP_REPEAT} XP earned`);
    await expect(page.locator('.xp-gain')).toHaveText(`+${XP_REPEAT}`);
    await page.screenshot({ path: `${SHOTS}/repeat-note.png` });
    await next(page);
    await expect(xpChip(page)).toHaveAttribute('aria-label', `${XP_REPEAT} XP earned`);
  });

  test('a lesson played for the first time shows no such note', async ({ page }) => {
    await page.goto(`/#/lesson/${lesson.id}`);
    await page.locator('.prompt').waitFor();
    await expect(page.getByTestId('repeat-note')).toHaveCount(0);
  });

  test('leaving and resuming keeps the reduced XP of the questions that were repeats', async ({ page }) => {
    await playWholeLesson(page);
    await page.goto(`/#/lesson/${lesson.id}`);
    await page.locator('.prompt').waitFor();
    await solveAndContinue(page, lesson.questions[0]!);
    await page.getByRole('button', { name: 'Leave lesson' }).click();
    await page.getByTestId('resume-link').click();
    await expect(page.getByTestId('repeat-note')).toBeVisible();
    await expect(xpChip(page)).toHaveAttribute('aria-label', `${XP_REPEAT} XP earned`);
    await solveAndContinue(page, lesson.questions[1]!);
    await expect(xpChip(page)).toHaveAttribute('aria-label', `${2 * XP_REPEAT} XP earned`);
  });

  test('the next day the same lesson pays in full and counts for that day\'s goal', async ({ page }) => {
    await playWholeLesson(page);
    const first = (await stored(page))!;
    await setDay(page, day(1));
    await playWholeLesson(page);
    const next = (await stored(page))!;
    expect(next.xp - first.xp).toBe(total * XP_FIRST + BONUS);
    expect(next.days[day(1)].answered).toBe(total);
    expect(next.days[TODAY].answered).toBe(total);
  });
});

test.describe('one question again and again (Library practice)', () => {
  const q = lesson.questions[0]!;

  test('going back into the same question the instant it finishes still starts a fresh session (the finished screen is not reused)', async ({ page }) => {
    await page.goto(`/#/q/${q.id}`);
    await page.locator('.prompt').waitFor();
    await answerCorrectly(page, q);
    await check(page);
    // Finish and re-enter in one task: the app never gets to render the summary in between (this is what a fast script, or
    // a flaky moment under load, does). The second visit must not carry the first one's "never done today".
    await page.evaluate((id) => {
      (document.querySelector('.lesson-bar .btn.primary') as HTMLButtonElement).click();
      window.location.hash = `#/q/${id}`;
    }, q.id);
    await page.locator('.prompt').waitFor();
    await expect(page.getByTestId('repeat-note')).toBeVisible();
    await expect(xpChip(page)).toHaveAttribute('aria-label', '0 XP earned');
    await solveAndContinue(page, q);
    const p = (await stored(page))!;
    expect(p.xp).toBe(XP_FIRST + XP_REPEAT);
    expect(p.days[TODAY].answered).toBe(1);
  });

  test('eight rounds: 10 XP once, then XP_REPEAT each, and the goal gets 1, not 8', async ({ page }) => {
    const gained: number[] = [];
    let before = 0;
    for (let i = 0; i < 8; i++) {
      await page.goto(`/#/q/${q.id}`);
      await page.locator('.prompt').waitFor();
      await solveAndContinue(page, q);
      await expect(page).toHaveURL(/#\/done$/);
      const now = (await stored(page))!.xp;
      gained.push(now - before);
      before = now;
    }
    const p = (await stored(page))!;
    expect(gained, 'XP gained in each round').toEqual([XP_FIRST, ...Array(7).fill(XP_REPEAT)]);
    expect(p.xp).toBe(XP_FIRST + 7 * XP_REPEAT);
    expect(p.days[TODAY].answered).toBe(1);
    await page.getByRole('button', { name: 'Back home', exact: true }).click();
    await expect(page.getByTestId('goal-text')).toHaveText('1/8');
  });
});
