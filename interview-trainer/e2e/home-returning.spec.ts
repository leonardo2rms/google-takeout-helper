import { expect, test, type Page } from '@playwright/test';
import { KEY_V2, SHOTS, TODAY, course, dueCards, expectNoOverflow, lessons, openMore, seed, useClock, v2 } from './engage';

/**
 * Home for a returning learner (audit N-12, UX-15): Today stays the one primary action, the modes with live information are
 * full rows, Mixed / Mock / Library share one compact row, and the curriculum shows only the unit being worked in (every
 * other unit is one line, one tap from its lessons).
 */

const unitIds = course.units.map((u) => u.id);
const lessonRecord = { bestAccuracy: 1, completions: 1, lastDay: TODAY };
/** The first `n` lessons of the course are done. */
const doneLessons = (n: number) => Object.fromEntries(lessons.slice(0, n).map((l) => [l.id, lessonRecord]));
const unit = (page: Page, id: string) => page.getByTestId(`unit-${id}`);
const isOpen = (page: Page, id: string) => unit(page, id).evaluate((el) => (el as HTMLDetailsElement).open);
const openUnits = (page: Page) => page.evaluate(() => Array.from(document.querySelectorAll<HTMLDetailsElement>('details.unit-section')).filter((d) => d.open).map((d) => d.dataset.testid));

async function homeWith(page: Page, doneCount: number, extra: Record<string, unknown> = {}) {
  await useClock(page);
  await seed(page, { [KEY_V2]: v2({ xp: 520, streak: 9, longestStreak: 9, lastDay: TODAY, lessons: doneLessons(doneCount), stats: { sessions: 14, answered: 120, firstTry: 90, perfect: 2, practiceDays: 9 }, cards: dueCards(12).cards, ...extra }) });
  await page.goto('/');
  await page.locator('.home').waitFor();
}

test.describe('a learner with six lessons done', () => {
  test.beforeEach(async ({ page }) => homeWith(page, 6));

  test('Today is the one solid action; live rows are full width; Mixed, Mock and Library share one compact row', async ({ page }) => {
    await expect(page.getByTestId('daily-card')).toBeVisible();
    for (const id of ['up-next', 'challenge-card', 'due-card', 'progress-link']) await expect(page.getByTestId(id), id).toBeVisible();
    await expect(page.getByTestId('more-practice')).toHaveCount(0); // everything is on the page

    const tiles = await Promise.all(['mix-card', 'mock-link', 'library-link'].map((id) => page.getByTestId(id).boundingBox()));
    for (const b of tiles) {
      expect(b).not.toBeNull();
      expect(b!.height, 'a tap target of at least 44px').toBeGreaterThanOrEqual(44);
    }
    expect(new Set(tiles.map((b) => Math.round(b!.y))).size, 'one row').toBe(1);
    expect(tiles[0]!.x).toBeLessThan(tiles[1]!.x);
    expect(tiles[1]!.x).toBeLessThan(tiles[2]!.x);
    // the compact row sits below the full-width rows
    const progress = (await page.getByTestId('progress-link').boundingBox())!;
    expect(tiles[0]!.y).toBeGreaterThan(progress.y + progress.height - 1);
    await expectNoOverflow(page);
  });

  test('the tiles keep their names for assistive technology, and still lead where they did', async ({ page }) => {
    await expect(page.getByTestId('mix-card')).toHaveAccessibleName(/Mixed practice: 8 questions/);
    await expect(page.getByTestId('mock-link')).toHaveAccessibleName(/Mock interview: timed, no feedback until the end/);
    await expect(page.getByTestId('library-link')).toHaveAccessibleName(/Library: search every question/);
    await page.getByTestId('library-link').click();
    await expect(page).toHaveURL(/#\/library$/);
  });

  test('only the unit being worked in is open: finished units are one line, the rest too', async ({ page }) => {
    const current = course.units.find((u) => u.lessons.some((l) => !(l.id in doneLessons(6))))!;
    expect(await openUnits(page)).toEqual([`unit-${current.id}`]);
    const first = unitIds[0]!;
    await expect(unit(page, first)).toHaveClass(/complete/);
    expect(await isOpen(page, first)).toBe(false);
    // a collapsed unit is one tappable line of at least 44px and shows its name and tally
    const summary = unit(page, first).locator('summary');
    expect((await summary.boundingBox())!.height).toBeGreaterThanOrEqual(44);
    await expect(summary).toContainText(course.units[0]!.title);
    await expect(summary).toContainText(`${course.units[0]!.lessons.length}/${course.units[0]!.lessons.length}`);
    // the lessons of a collapsed unit are not on the page until it is opened
    await expect(page.locator(`a[href="#/lesson/${course.units[0]!.lessons[0]!.id}"]`)).toBeHidden();
    // the page is much shorter than with every unit open
    const height = await page.evaluate(() => document.documentElement.scrollHeight);
    await page.getByTestId('units-toggle').click();
    expect(await page.evaluate(() => document.documentElement.scrollHeight)).toBeGreaterThan(height + 1500);
  });

  test('a collapsed unit opens with a tap and its lessons are reachable', async ({ page }) => {
    const target = course.units[4]!; // not the current unit
    expect(await isOpen(page, target.id)).toBe(false);
    await unit(page, target.id).locator('summary').click();
    expect(await isOpen(page, target.id)).toBe(true);
    await page.locator(`a[href="#/lesson/${target.lessons[0]!.id}"]`).click();
    await expect(page).toHaveURL(new RegExp(`#/lesson/${target.lessons[0]!.id}$`));
    await expect(page.locator('.prompt')).toBeVisible();
  });

  test('a unit opens and closes from the keyboard (Enter and Space on its header)', async ({ page }) => {
    const target = course.units[5]!;
    const summary = unit(page, target.id).locator('summary');
    await summary.focus();
    await page.keyboard.press('Enter');
    expect(await isOpen(page, target.id)).toBe(true);
    await page.keyboard.press('Space');
    expect(await isOpen(page, target.id)).toBe(false);
  });

  test('Expand all opens every unit, Collapse all closes them, and the label follows', async ({ page }) => {
    const toggle = page.getByTestId('units-toggle');
    await expect(toggle).toHaveText('Expand all');
    await toggle.click();
    expect((await openUnits(page)).length).toBe(unitIds.length);
    await expect(toggle).toHaveText('Collapse all');
    await toggle.click();
    expect(await openUnits(page)).toEqual([]);
    await expect(toggle).toHaveText('Expand all');
    // opening every unit by hand flips the label too
    for (const id of unitIds) await unit(page, id).locator('summary').click();
    await expect(toggle).toHaveText('Collapse all');
  });

  test('the up-next lesson is highlighted inside the open unit and is also its own card', async ({ page }) => {
    const next = lessons[6]!;
    await expect(page.getByTestId('up-next')).toContainText(next.title);
    await expect(page.locator(`a.lesson-row.next[href="#/lesson/${next.id}"]`)).toBeVisible();
  });

  test('screenshot: the returning learner\'s Home, and no horizontal scroll at 320px', async ({ page }) => {
    await page.screenshot({ path: `${SHOTS}/home-returning.png` });
    await page.setViewportSize({ width: 320, height: 700 });
    await expectNoOverflow(page);
    for (const id of ['mix-card', 'mock-link', 'library-link']) {
      const b = (await page.getByTestId(id).boundingBox())!;
      expect(b.x + b.width, id).toBeLessThanOrEqual(320);
    }
  });
});

test.describe('other stages', () => {
  test('a new learner: the first unit is open, the others are one line, and the practice modes sit under "More ways to practise"', async ({ page }) => {
    await useClock(page);
    await page.goto('/');
    await page.locator('.home').waitFor();
    expect(await openUnits(page)).toEqual([`unit-${unitIds[0]}`]);
    await expect(page.getByTestId('more-practice')).toBeVisible();
    await openMore(page);
    for (const id of ['mix-card', 'mock-link', 'library-link', 'due-card', 'progress-link']) await expect(page.getByTestId(id), id).toBeVisible();
  });

  test('after one or two lessons the live rows are on the page and the compact row is under "More ways to practise"', async ({ page }) => {
    await homeWith(page, 2);
    for (const id of ['challenge-card', 'due-card', 'progress-link']) await expect(page.getByTestId(id), id).toBeVisible();
    await expect(page.getByTestId('mix-card')).toBeHidden();
    await openMore(page);
    for (const id of ['mix-card', 'mock-link', 'library-link']) await expect(page.getByTestId(id), id).toBeVisible();
  });

  test('every lesson done: no unit is open', async ({ page }) => {
    await homeWith(page, lessons.length);
    expect(await openUnits(page)).toEqual([]);
    await expect(page.getByTestId('units-toggle')).toHaveText('Expand all');
    for (const id of unitIds) await expect(unit(page, id)).toHaveClass(/complete/);
  });

  test('at the largest reading size the compact row wraps instead of squeezing the words', async ({ page }) => {
    await homeWith(page, 6);
    await page.evaluate(() => document.documentElement.setAttribute('data-text-size', 'XL'));
    await page.waitForTimeout(50);
    await expectNoOverflow(page);
    const labels = await page.locator('.mode-tile b').allTextContents();
    expect(labels).toEqual(['Mixed', 'Mock', 'Library']); // whole words, not "Librar" + "y"
    for (const id of ['mix-card', 'mock-link', 'library-link']) {
      const b = (await page.getByTestId(id).boundingBox())!;
      expect(b.x + b.width, id).toBeLessThanOrEqual(390);
    }
  });
});
