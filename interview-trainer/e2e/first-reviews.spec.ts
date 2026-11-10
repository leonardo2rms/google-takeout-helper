import { expect, test, type Page } from '@playwright/test';
import { FIRST_REVIEW_DAY_CAP } from '../shared/src';
import { KEY_V2, SHOTS, TODAY, day, dueCount, lessons, playLesson, seed, setDay, stored, useClock, v2, homeAfterSummary } from './engage';
import { answerCorrectly, check } from './solve';

/**
 * First reviews are spread (audit N-4): three lessons on day 1 used to leave about 25 cards due the next morning. At most
 * FIRST_REVIEW_DAY_CAP first reviews fall due on one day, the rest slide to the next day or two.
 */

const [l1, l2, l3] = lessons as [(typeof lessons)[number], (typeof lessons)[number], (typeof lessons)[number]];
const total = l1.questions.length + l2.questions.length + l3.questions.length;
const duePerDay = async (page: Page) => {
  const cards = Object.values((await stored(page))!.cards as Record<string, { due: string }>);
  const out: Record<string, number> = {};
  for (const c of cards) out[c.due] = (out[c.due] ?? 0) + 1;
  return out;
};

test.beforeEach(async ({ page }) => {
  await useClock(page);
  await seed(page, { [KEY_V2]: v2() });
});

test.describe('three lessons on the first day', () => {
  test('the new cards are spread over three days, never more than the cap on one', async ({ page }) => {
    await playLesson(page, l1.id);
    await homeAfterSummary(page);
    await playLesson(page, l2.id);
    await homeAfterSummary(page);
    await playLesson(page, l3.id);
    expect(total, 'the point of the test: more new cards than one day should hold').toBeGreaterThan(FIRST_REVIEW_DAY_CAP * 2);

    const per = await duePerDay(page);
    expect(Object.values(per).reduce((a, b) => a + b, 0)).toBe(total);
    expect(Math.max(...Object.values(per))).toBeLessThanOrEqual(FIRST_REVIEW_DAY_CAP);
    expect(Object.keys(per).sort()).toEqual([day(1), day(2), day(3)]);
    expect(per[day(1)]).toBe(FIRST_REVIEW_DAY_CAP);

    // the summary says the real schedule for everything that is coming, so it agrees with what Home will say tomorrow
    await expect(page.getByTestId('next-review')).toContainText(`${FIRST_REVIEW_DAY_CAP} questions tomorrow, ${FIRST_REVIEW_DAY_CAP} in 2 days, ${total - 2 * FIRST_REVIEW_DAY_CAP} in 3 days`);
    await page.screenshot({ path: `${SHOTS}/first-reviews-summary.png` });
  });

  test('on the first return the Review card shows the cap, not the whole pile; later days bring the rest', async ({ page }) => {
    for (const l of [l1, l2, l3]) {
      await playLesson(page, l.id);
      await homeAfterSummary(page);
    }
    await setDay(page, day(1));
    await page.goto('/');
    await page.locator('.home').waitFor();
    await expect(dueCount(page)).toHaveAttribute('data-count', String(FIRST_REVIEW_DAY_CAP));
    await expect(dueCount(page)).toContainText(`${FIRST_REVIEW_DAY_CAP} due`);
    await page.screenshot({ path: `${SHOTS}/first-reviews-day2.png` });

    await setDay(page, day(2));
    await page.goto('/');
    await page.locator('.home').waitFor();
    await expect(dueCount(page)).toHaveAttribute('data-count', String(FIRST_REVIEW_DAY_CAP * 2)); // the unreviewed rest of day 2 and what slid to day 3
  });
});

test.describe('one lesson a day is unchanged', () => {
  test('all its cards are due the next day, as before, and the summary says so', async ({ page }) => {
    await playLesson(page, l1.id);
    const per = await duePerDay(page);
    expect(per).toEqual({ [day(1)]: l1.questions.length });
    await expect(page.getByTestId('next-review')).toContainText('tomorrow');
    await expect(page.getByTestId('next-review')).not.toContainText('in 2 days');
  });
});

test.describe('what the lesson says about the next review', () => {
  test('a first review makes no promise about the day; a later review gives the exact one', async ({ page }) => {
    await page.goto(`/#/lesson/${l1.id}`);
    await page.locator('.prompt').waitFor();
    await answerCorrectly(page, l1.questions[0]!);
    await check(page);
    await expect(page.getByTestId('srs-note')).toHaveText("You'll see this again in the next few days.");

    // the next day, after it came due: the second correct answer is scheduled 3 days out, and the note says so
    await page.goto('/'); // leave the lesson that is still open on its first question
    await playLesson(page, l1.id);
    await setDay(page, day(1));
    await page.goto(`/#/lesson/${l1.id}`);
    await page.locator('.prompt').waitFor();
    await answerCorrectly(page, l1.questions[0]!);
    await check(page);
    await expect(page.getByTestId('srs-note')).toHaveText("You'll see this again in 3 days.");
    void TODAY;
  });
});
