import { expect, test, type Page } from '@playwright/test';
import { KEY_V2, SHOTS, course, day, dueCards, expectNoOverflow, lessons, openMore, playLesson, seed, setDay, stored, useClock, v2 } from './engage';

/**
 * First run and progressive disclosure: a brand-new learner sees one next step, and everything else is still one tap away.
 * Plus the numbers on Home, which have to agree with each other.
 */

const FIRST = lessons[0]!;
const ids = ['challenge-card', 'due-card', 'mix-card', 'progress-link', 'mock-link', 'library-link'] as const;

const visibleIds = async (page: Page) => {
  const out: string[] = [];
  for (const id of ids) if (await page.getByTestId(id).isVisible()) out.push(id);
  return out;
};

test.describe('first run', () => {
  for (const [w, h] of [[390, 844], [320, 640]] as const) {
    test(`at ${w}x${h} "Start your first lesson" is on screen without scrolling, and nothing else competes with it`, async ({ page }) => {
      await page.setViewportSize({ width: w, height: h });
      await useClock(page);
      await page.goto('/');
      const hero = page.getByTestId('daily-card');
      await expect(hero).toContainText('Start your first lesson');
      const box = (await hero.boundingBox())!;
      expect(box.y + box.height, 'the hero ends above the fold').toBeLessThanOrEqual(h);
      expect(await visibleIds(page)).toEqual([]); // no practice card on the page yet
      // the plan is one quiet line, not a form
      await expect(page.getByTestId('onboarding')).toBeVisible();
      await expect(page.locator('input[type="date"]')).toHaveCount(0);
      await expectNoOverflow(page);
      await page.screenshot({ path: `${SHOTS}/home-first-run-${w}.png` });
    });
  }

  test('every practice mode is still reachable under "More ways to practise"', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    await expect(page.getByTestId('more-practice')).toBeVisible();
    await openMore(page);
    expect(await visibleIds(page)).toEqual([...ids]);
    await page.screenshot({ path: `${SHOTS}/home-first-run-more.png` });
    await page.getByTestId('library-link').click();
    await expect(page).toHaveURL(/#\/library$/);
  });

  test('after the first lesson the daily modes appear; after three lessons everything is on the page', async ({ page }) => {
    test.setTimeout(90_000);
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    await playLesson(page, lessons[0]!.id);
    await page.getByRole('button', { name: 'Back home', exact: true }).click();
    expect(await visibleIds(page)).toEqual(['challenge-card', 'due-card', 'progress-link']);
    await expect(page.getByTestId('more-practice')).toBeVisible(); // mixed practice, mock and library are one tap away

    // two more lessons
    for (const l of [lessons[1]!, lessons[2]!]) {
      await playLesson(page, l.id);
      await page.getByRole('button', { name: 'Back home', exact: true }).click();
    }
    expect(await visibleIds(page)).toEqual([...ids]);
    await expect(page.getByTestId('more-practice')).toHaveCount(0);
    await page.screenshot({ path: `${SHOTS}/home-all-modes.png` });
  });

  test('Mixed practice for someone who has not started never opens with a typed or coded answer', async ({ page }) => {
    await useClock(page);
    for (let d = 0; d < 6; d++) {
      await setDay(page, day(d));
      await page.goto('/#/mix');
      for (let i = 0; i < 3; i++) {
        await page.locator('.kicker').waitFor();
        await expect(page.locator('.kicker')).not.toContainText(/own words|Write the code/);
        await page.getByRole('button', { name: "I don't know" }).click();
        await page.getByRole('button', { name: 'Continue', exact: true }).click();
      }
      await page.goto('/#/');
    }
  });

  test('finishing the first session ends first-run setup: the plan line is gone', async ({ page }) => {
    await useClock(page);
    await page.goto('/');
    await playLesson(page, FIRST.id);
    await page.getByRole('button', { name: 'Back home', exact: true }).click();
    await expect(page.getByTestId('onboarding')).toHaveCount(0);
    expect((await stored(page))!.profile.onboarded).toBe(true);
  });
});

test.describe('the numbers on Home agree with each other', () => {
  test('Today says how many of the due cards it takes, Review says it works in rounds, Progress waits for data', async ({ page }) => {
    const { cards } = dueCards(24);
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ cards, streak: 2, longestStreak: 2, lastDay: day(-1), stats: { sessions: 3, answered: 24, firstTry: 20, perfect: 1, practiceDays: 2 } }) });
    await page.goto('/');
    await expect(page.getByTestId('daily-card')).toContainText('5 of 24 due');
    await expect(page.getByTestId('due-count')).toHaveText('24 ready · 10 at a time');
    const seen = Math.round((24 / course.units.flatMap((u) => u.lessons).flatMap((l) => l.questions).length) * 100);
    await expect(page.getByTestId('readiness-line')).toHaveText(`Readiness: not enough data yet (${seen}% of the course seen)`);
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/home-numbers.png` });
  });
});
