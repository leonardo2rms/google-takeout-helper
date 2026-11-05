/**
 * What a finished session is credited, and to which day (QA S1-8, S1-9):
 *  - the summary shows the XP that was really credited (a one-question session earns no perfect-lesson bonus);
 *  - the daily challenge belongs to the day it was served, so finishing after midnight does not consume the new day's.
 */
import { expect, test } from '@playwright/test';
import { CHALLENGE_XP, addDays, challengeQuestion } from '../shared/src';
import { KEY_V2, course, questions, seed, stored, useClock, v2, watchErrors } from './engage';
import { answerCorrectly, check, next, solveAndContinue } from './solve';

const SIMPLE = new Set(['single', 'multi', 'fill', 'order', 'spot', 'estimate']);

/** A day whose challenge and the next day's are both simple to solve through the UI. */
function twoSimpleDays(): string {
  for (let d = '2026-11-04'; ; d = addDays(d, 1)) {
    if (SIMPLE.has(challengeQuestion(course, d)!.type) && SIMPLE.has(challengeQuestion(course, addDays(d, 1))!.type)) return d;
  }
}

test.describe('the XP on the summary is the XP that was credited (S1-8)', () => {
  test('a single practised question shows +10, and Home agrees', async ({ page }) => {
    const q = questions.find((x) => x.type === 'single')!;
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto(`/#/q/${q.id}`);
    await solveAndContinue(page, q);
    await expect(page.locator('.tile.xp b')).toHaveText('+10');
    expect((await stored(page))!.xp).toBe(10);
  });

  test('the daily challenge shows question XP plus the +15 bonus, which is what was stored', async ({ page }) => {
    const day = twoSimpleDays();
    const [y, m, d] = day.split('-').map(Number) as [number, number, number];
    await useClock(page, new Date(y, m - 1, d, 10, 0, 0));
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/#/challenge');
    await solveAndContinue(page, challengeQuestion(course, day)!);
    await expect(page.locator('.tile.xp b')).toHaveText(`+${10 + CHALLENGE_XP}`);
    expect((await stored(page))!.xp).toBe(10 + CHALLENGE_XP);
  });
});

test.describe('a challenge finished after midnight (S1-9)', () => {
  test('is recorded for the day it was served, and the new day still has its own challenge', async ({ page }) => {
    const errors = watchErrors(page);
    const day = twoSimpleDays();
    const tomorrow = addDays(day, 1);
    const [y, m, d] = day.split('-').map(Number) as [number, number, number];
    await useClock(page, new Date(y, m - 1, d, 23, 59, 20));
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/#/challenge');

    const q = challengeQuestion(course, day)!;
    await page.locator('.prompt').waitFor();
    await answerCorrectly(page, q);
    await check(page);
    await expect(page.locator('.feedback')).toHaveClass(/good/);
    await page.clock.setSystemTime(new Date(y, m - 1, d + 1, 0, 0, 40)); // midnight passes while the feedback is on screen
    await next(page);
    await expect(page.locator('.summary h1')).toBeVisible();

    expect((await stored(page))!.challenge).toEqual({ [day]: 'pass' });
    expect((await stored(page))!.xp).toBe(10 + CHALLENGE_XP);

    // the new day's different question is still a real, unconsumed challenge
    await page.goto('/#/');
    await page.goto('/#/challenge');
    await solveAndContinue(page, challengeQuestion(course, tomorrow)!);
    expect((await stored(page))!.challenge).toEqual({ [day]: 'pass', [tomorrow]: 'pass' });
    expect((await stored(page))!.xp).toBe(2 * (10 + CHALLENGE_XP));
    expect(errors).toEqual([]);
  });
});
