/** Hash routes tolerate trailing slashes, doubled slashes and a query part (QA S2-17). */
import { expect, test } from '@playwright/test';
import { KEY_V2, seed, useClock, v2, watchErrors } from './engage';

test.describe('route forms that used to fall through to Home or "not found"', () => {
  test.beforeEach(async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
  });

  for (const [hash, heading] of [
    ['#/library/', /Library|Browse|Questions/i],
    ['#/mock/', /Mock interview/],
    ['#/progress/', /Progress/],
  ] as const) {
    test(`${hash} shows the same screen as without the slash`, async ({ page }) => {
      const errors = watchErrors(page);
      await page.goto(`/${hash}`);
      await expect(page.getByRole('heading', { name: heading }).first()).toBeVisible();
      await expect(page.locator('.home-top')).toHaveCount(0); // Home's own header
      expect(errors).toEqual([]);
    });
  }

  test('a lesson link with a query string still opens the lesson', async ({ page }) => {
    await page.goto('/#/lesson/kotlin-null-types?utm=x');
    await expect(page.locator('.prompt')).toBeVisible();
  });

  test('"#//" is Home', async ({ page }) => {
    await page.goto('/#//');
    await expect(page.locator('.home-top')).toBeVisible();
  });
});
