/** The mock interview timer measures elapsed time, not the wall clock (QA S2-8). */
import { expect, test } from '@playwright/test';
import { KEY_V2, seed, useClock, v2 } from './engage';

test('moving the system clock forward or back during a mock does not end it or add time', async ({ page }) => {
  await useClock(page);
  await seed(page, { [KEY_V2]: v2() });
  await page.goto('/#/mock');
  await page.getByTestId('mock-start').click();
  await expect(page.getByTestId('mock-run')).toBeVisible();
  const before = await page.getByTestId('mock-timer').innerText();

  await page.clock.setSystemTime(new Date(2026, 10, 4, 13, 0, 0)); // +3 h on the wall clock
  await page.clock.runFor(2000);
  await expect(page.getByTestId('mock-run')).toBeVisible();
  await expect(page.getByTestId('mock-results')).toHaveCount(0);

  await page.clock.setSystemTime(new Date(2026, 10, 4, 7, 0, 0)); // and 3 h back
  await page.clock.runFor(2000);
  await expect(page.getByTestId('mock-run')).toBeVisible();
  const after = await page.getByTestId('mock-timer').innerText();
  expect(after).not.toBe(''); // still counting
  expect(before).not.toBe('');
});
