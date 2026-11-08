import { expect, test, type Page } from '@playwright/test';
import { ACHIEVEMENTS, ACHIEVEMENT_GROUPS, type AchievementGroup } from '../shared/src';
import { KEY_V2, SHOTS, TODAY, cardFor, day, expectNoOverflow, lessons, questions, seed, useClock, v2 } from './engage';

/**
 * The Progress page for the person who uses it most (audit N-15, UX-33): 13 topics started, 10 mocks and every badge used to
 * make a 5,000px page. Earned badges now fold by group to one line with a count; a group with a recent badge stays open.
 */

const groups = Object.keys(ACHIEVEMENT_GROUPS) as AchievementGroup[];
const inGroup = (g: AchievementGroup) => ACHIEVEMENTS.filter((a) => a.group === g);
const group = (page: Page, g: AchievementGroup) => page.getByTestId(`trophy-group-${g}`);
const isOpen = (page: Page, g: AchievementGroup) => group(page, g).evaluate((el) => (el as HTMLDetailsElement).open);

/** Every badge earned a month ago, except `fresh` (earned today). */
async function heavyUser(page: Page, fresh: string[] = []) {
  await useClock(page);
  const done = Object.fromEntries(lessons.map((l) => [l.id, { bestAccuracy: 0.9, completions: 2, lastDay: TODAY }]));
  const cards = Object.fromEntries(questions.map((q) => [q.id, cardFor(q, { interval: 12, due: day(10), reps: 3, last: day(-2) })]));
  const mocks = Array.from({ length: 10 }, (_, i) => ({ day: day(-i - 1), total: 10, answered: 10, score: 50 + i * 4, limitMin: 15, usedSec: 500, timedOut: false, byUnit: { kotlin: { n: 10, score: 5 } } }));
  const achievements = Object.fromEntries(ACHIEVEMENTS.map((a) => [a.id, fresh.includes(a.id) ? TODAY : day(-30)]));
  await seed(page, { [KEY_V2]: v2({ xp: 4200, streak: 30, longestStreak: 30, lastDay: TODAY, lessons: done, cards, mocks, achievements, stats: { sessions: 60, answered: 700, firstTry: 560, perfect: 12, practiceDays: 40 } }) });
  await page.goto('/#/progress');
  await page.locator('.progress-page').waitFor();
}

test.describe('Progress for a heavy user', () => {
  test('earned badges fold by group, each with its count, and the page is much shorter', async ({ page }) => {
    await heavyUser(page);
    for (const g of groups) {
      await expect(group(page, g)).toBeVisible();
      expect(await isOpen(page, g), `${g} is folded`).toBe(false);
      await expect(page.getByTestId(`trophy-group-${g}-count`)).toHaveText(`${inGroup(g).length}/${inGroup(g).length}`);
    }
    await expect(page.getByTestId('badge-count')).toHaveText(`${ACHIEVEMENTS.length}/${ACHIEVEMENTS.length}`); // nothing is lost from the count
    const height = await page.evaluate(() => document.documentElement.scrollHeight);
    expect(height, 'was about 4,950px with every badge open').toBeLessThan(3300);
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/progress-heavy.png`, fullPage: true });
  });

  test('a folded group opens with a tap or from the keyboard and shows its badges as before', async ({ page }) => {
    await heavyUser(page);
    const g = groups[1]!;
    const sample = inGroup(g)[0]!;
    await expect(page.getByTestId(`badge-${sample.id}`)).toBeHidden();
    await group(page, g).locator(':scope > summary').click();
    await expect(page.getByTestId(`badge-${sample.id}`)).toBeVisible();
    await expect(page.getByTestId(`badge-${sample.id}`)).toHaveAttribute('data-unlocked', 'true');
    await expect(page.getByTestId(`badge-${sample.id}`)).toContainText('Unlocked');
    // keyboard: Enter and Space on the header
    const summary = group(page, groups[2]!).locator(':scope > summary');
    expect((await summary.boundingBox())!.height).toBeGreaterThanOrEqual(44);
    await summary.focus();
    await page.keyboard.press('Enter');
    expect(await isOpen(page, groups[2]!)).toBe(true);
    await page.keyboard.press('Space');
    expect(await isOpen(page, groups[2]!)).toBe(false);
  });

  test('a badge earned today keeps its group open, so a new badge is never hidden; the other groups stay folded', async ({ page }) => {
    const newest = inGroup(groups[3]!)[0]!;
    await heavyUser(page, [newest.id]);
    expect(await isOpen(page, groups[3]!)).toBe(true);
    await expect(page.getByTestId(`badge-${newest.id}`)).toBeVisible();
    for (const g of groups.filter((x) => x !== groups[3])) expect(await isOpen(page, g), g).toBe(false);
  });

  test('a badge earned yesterday still counts as recent; one earned a week ago does not', async ({ page }) => {
    const [a, b] = [inGroup(groups[0]!)[0]!, inGroup(groups[1]!)[0]!];
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ xp: 900, achievements: { [a.id]: day(-1), [b.id]: day(-7) } }) });
    await page.goto('/#/progress');
    await page.locator('.progress-page').waitFor();
    expect(await isOpen(page, groups[0]!), 'yesterday').toBe(true);
    expect(await isOpen(page, groups[1]!), 'a week ago').toBe(false);
  });

  test('no horizontal scroll at 320px with every group open', async ({ page }) => {
    await heavyUser(page);
    await page.setViewportSize({ width: 320, height: 700 });
    for (const g of groups) await group(page, g).locator(':scope > summary').click();
    await expectNoOverflow(page);
  });
});
