import { expect, test, type Page } from '@playwright/test';
import { CHALLENGE_XP, challengeQuestion } from '../shared/src';
import { KEY_V2, SHOTS, TODAY, course, day, expectNoOverflow, seed, setDay, stored, useClock, v2 as blank } from './engage';
import { solveAndContinue, visible } from './solve';

const questionOn = (d: string) => challengeQuestion(course, d)!;
/** The daily challenge joins Home after the first session, so these learners have had one. */
const v2 = (over: Parameters<typeof blank>[0] = {}) => blank({ stats: { ...blank().stats, sessions: 1 }, ...over });
const card = (page: Page) => page.getByTestId('challenge-card');

/** Give up on the question: "I don't know" until the session ends (a missed question comes back twice). */
async function giveUp(page: Page) {
  for (let i = 0; i < 3; i++) {
    await page.getByRole('button', { name: "I don't know" }).click();
    await page.getByRole('button', { name: 'Continue', exact: true }).click();
  }
}

test.describe('daily challenge', () => {
  test('Home features today\'s question (the same on every load) and finishing it earns a bonus once', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    await expect(card(page)).toBeVisible();
    await expect(card(page)).toHaveAttribute('data-result', 'none');
    await expect(card(page)).toContainText(`+${CHALLENGE_XP} XP`);
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/challenge-card.png` });
    const q = questionOn(TODAY);

    await page.reload();
    await card(page).click();
    await expect(page.locator('.prompt')).toHaveText(visible(q.prompt).replace(/\s+/g, ' '), { useInnerText: true });
    await solveAndContinue(page, q);

    await expect(page.locator('.summary h1')).toBeVisible();
    await expect(page.getByTestId('summary-notes')).toContainText(`Daily challenge passed (+${CHALLENGE_XP} XP)`);
    await expect(page.getByTestId('summary-notes')).toContainText('Challenge streak: 1 day');
    await expect(page.getByTestId('celebration')).toHaveCount(0); // a session of one is never a "perfect lesson"
    await page.screenshot({ path: `${SHOTS}/challenge-done.png` });
    const p = await stored(page);
    expect(p!.challenge).toEqual({ [TODAY]: 'pass' });
    expect(p!.xp).toBe(10 + CHALLENGE_XP);
    expect(p!.stats.perfect).toBe(0);
    expect(p!.days[TODAY].answered).toBe(1);

    await page.getByRole('button', { name: 'Back home', exact: true }).click();
    await expect(card(page)).toHaveAttribute('data-result', 'pass');
    await expect(card(page)).toContainText('passed');
    await expect(page.getByTestId('challenge-streak')).toContainText('1 day in a row');

    // a replay is plain practice: no XP, no second result
    await page.goto('/#/challenge');
    await expect(page.locator('.kicker')).toBeVisible();
    await solveAndContinue(page, q);
    const again = await stored(page);
    expect(again!.xp).toBe(p!.xp + 10);
    expect(again!.challenge).toEqual({ [TODAY]: 'pass' });
  });

  test('giving up still completes the day: no bonus, the streak is kept, copy stays neutral', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ challenge: { [day(-1)]: 'pass' } }) });
    await page.goto('/#/challenge');
    await giveUp(page);
    await expect(page.getByTestId('summary-notes')).toContainText('Daily challenge done.');
    await expect(page.getByTestId('summary-notes')).toContainText('Challenge streak: 2 days');
    await page.getByRole('button', { name: 'Back home', exact: true }).click();
    await expect(card(page)).toHaveAttribute('data-result', 'fail');
    await expect(card(page)).toContainText('Tomorrow brings a new one');
    expect((await stored(page))!.xp).toBe(0);
    await expect(card(page)).not.toContainText(/lost|missed|streak is|danger|hurry/i);
  });

  test('a new local day brings a new question; the streak survives only consecutive days', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ challenge: { [day(-1)]: 'fail', [day(-2)]: 'pass' } }) });
    await page.goto('/');
    await expect(page.getByTestId('challenge-streak')).toHaveCount(0); // not done yet today
    await expect(card(page)).toContainText('2 days in a row');

    await setDay(page, day(1)); // tomorrow: yesterday (today) has no result, so the streak is open, not broken
    await page.reload();
    await expect(card(page)).toHaveAttribute('data-result', 'none');
    await expect(card(page)).not.toContainText('in a row'); // today (-1,-2 are older, with a gap)
    const q1 = questionOn(day(1));
    expect(q1.id).not.toBe(questionOn(TODAY).id);
    await card(page).click();
    await expect(page.locator('.prompt')).toContainText(visible(q1.prompt).slice(0, 25));
  });

  test('is keyboard accessible with a 44px tap target at 390px', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    const box = (await card(page).boundingBox())!;
    expect(box.height).toBeGreaterThanOrEqual(44);
    await card(page).focus();
    await page.keyboard.press('Enter');
    await expect(page).toHaveURL(/#\/challenge$/);
  });
});
