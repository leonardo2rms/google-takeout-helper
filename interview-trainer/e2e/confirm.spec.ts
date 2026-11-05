import { expect, test, type Page } from '@playwright/test';
import { KEY_V2, SHOTS, lessons, seed, stored, useClock, v2 } from './engage';
import { answerCorrectly, check, solveAndContinue } from './solve';

/**
 * The in-app confirmation dialog replaces window.confirm everywhere. It must look like the app, trap focus, close on Escape,
 * give focus back, and keep working where the browser blocks modal dialogs (a sandboxed iframe: confirm() just returns false).
 */

const lesson = lessons[0]!;

/** A page where the browser's own dialogs are blocked, as in a sandboxed host, and any native dialog is a test failure. */
async function blockNativeDialogs(page: Page, seen: string[]): Promise<void> {
  await page.addInitScript(() => {
    window.confirm = () => false;
    window.alert = () => undefined;
  });
  page.on('dialog', (d) => {
    seen.push(`${d.type()}: ${d.message()}`);
    void d.dismiss();
  });
}

async function startLessonAndAnswerOne(page: Page): Promise<void> {
  await page.goto(`/#/lesson/${lesson.id}`);
  await page.locator('.prompt').waitFor();
  await solveAndContinue(page, lesson.questions[0]!);
  await page.locator('.prompt').waitFor();
}

test.describe('confirmation dialog', () => {
  const native: string[] = [];
  test.beforeEach(async ({ page }) => {
    native.length = 0;
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ xp: 40 }) });
    await blockNativeDialogs(page, native);
  });
  test.afterEach(() => {
    expect(native, 'no native dialog may appear').toEqual([]);
  });

  test('quitting a lesson: in-app dialog, safe button focused, Escape keeps going and returns focus, confirm leaves', async ({ page }) => {
    await startLessonAndAnswerOne(page);
    const quit = page.getByRole('button', { name: 'Quit lesson' });
    await quit.click();

    const dialog = page.getByRole('alertdialog', { name: 'Quit this lesson?' });
    await expect(dialog).toBeVisible();
    await expect(dialog).toContainText('not saved');
    await expect(page.getByTestId('quit-confirm-cancel')).toBeFocused(); // the safe choice has focus
    await page.screenshot({ path: `${SHOTS}/confirm-quit-lesson.png` });

    // Tab stays inside (two buttons), both ways
    for (let i = 0; i < 5; i++) {
      await page.keyboard.press('Tab');
      expect(await dialog.evaluate((d) => d.contains(document.activeElement))).toBe(true);
    }
    await page.keyboard.press('Shift+Tab');
    expect(await dialog.evaluate((d) => d.contains(document.activeElement))).toBe(true);

    await page.keyboard.press('Escape');
    await expect(dialog).toBeHidden();
    await expect(quit).toBeFocused(); // focus is restored to what opened the dialog
    await expect(page.locator('.prompt')).toBeVisible(); // still in the lesson

    await quit.click();
    await page.getByTestId('quit-confirm-ok').click();
    await expect(page).toHaveURL(/#\/$|\/$/);
    await expect(page.locator('.home')).toBeVisible();
  });

  test('Enter on the dialog itself does not advance the lesson behind it, and the keys of a modal stay in the modal', async ({ page }) => {
    await startLessonAndAnswerOne(page);
    await answerCorrectly(page, lesson.questions[1]!); // an answer is ready: Enter on the page would check it
    const prompt = await page.locator('.prompt').textContent();
    await page.getByRole('button', { name: 'Quit lesson' }).click();
    const dialog = page.getByRole('alertdialog', { name: 'Quit this lesson?' });
    await dialog.getByRole('heading').click(); // focus lands on the dialog container, not on a button
    await page.keyboard.press('Enter');
    await page.keyboard.press('Enter');
    await expect(dialog).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(page.locator('.prompt')).toHaveText(prompt!); // still the same question, nothing was checked behind the dialog
    await expect(page.locator('.lesson-bar .btn.primary')).toHaveText('Check');
  });

  test('quitting after Check but before Continue asks first: the XP in the header is not silently dropped', async ({ page }) => {
    await page.goto(`/#/lesson/${lesson.id}`);
    await page.locator('.prompt').waitFor();
    await answerCorrectly(page, lesson.questions[0]!);
    await check(page);
    await page.getByRole('button', { name: 'Quit lesson' }).click();
    await expect(page.getByRole('alertdialog', { name: 'Quit this lesson?' })).toBeVisible();
    await page.getByTestId('quit-confirm-cancel').click();
    await expect(page.getByRole('button', { name: 'Continue' })).toBeVisible(); // still on the checked question
  });

  test('the backdrop and "Keep going" both keep the lesson', async ({ page }) => {
    await startLessonAndAnswerOne(page);
    await page.getByRole('button', { name: 'Quit lesson' }).click();
    await page.getByTestId('quit-confirm-backdrop').click({ position: { x: 5, y: 5 } });
    await expect(page.getByRole('alertdialog')).toBeHidden();
    await page.getByRole('button', { name: 'Quit lesson' }).click();
    await page.getByRole('button', { name: 'Keep going' }).click();
    await expect(page.getByRole('alertdialog')).toBeHidden();
    await expect(page.locator('.lesson')).toBeVisible();
  });

  test('quitting before answering anything leaves at once: there is nothing to lose', async ({ page }) => {
    await page.goto(`/#/lesson/${lesson.id}`);
    await page.locator('.prompt').waitFor();
    await page.getByRole('button', { name: 'Quit lesson' }).click();
    await expect(page.getByRole('alertdialog')).toHaveCount(0);
    await expect(page.locator('.home')).toBeVisible();
  });

  test('reset progress asks inside the Settings sheet; Escape closes only the dialog and keeps the data', async ({ page }) => {
    await page.goto('/');
    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    const sheet = page.getByRole('dialog', { name: 'Settings', exact: true });
    const reset = sheet.getByRole('button', { name: 'Reset progress' });
    await reset.click();
    const dialog = page.getByRole('alertdialog', { name: 'Reset all progress?' });
    await expect(dialog).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(dialog).toBeHidden();
    await expect(sheet).toBeVisible(); // one layer at a time
    await expect(reset).toBeFocused();
    expect((await stored(page))!.xp).toBe(40);

    await reset.click();
    await page.getByTestId('reset-confirm-ok').click();
    await expect(page.locator('.stat[aria-label="0 XP"]')).toBeVisible();
    expect((await stored(page))!.xp).toBe(0);
  });

  test('the mock submit confirmation is fully on screen, above the bottom bar', async ({ page }) => {
    await page.goto('/#/mock');
    await page.getByTestId('mock-start').click();
    await page.getByTestId('mock-run').waitFor();
    await page.getByTestId('mock-finish-early').click();
    const dialog = page.getByTestId('mock-confirm');
    await expect(dialog).toBeVisible();
    const view = page.viewportSize()!;
    for (const target of [dialog, page.getByTestId('mock-submit'), page.getByTestId('mock-confirm-cancel')]) {
      const box = (await target.boundingBox())!;
      expect(box.y).toBeGreaterThanOrEqual(0);
      expect(box.y + box.height).toBeLessThanOrEqual(view.height);
      expect(box.x).toBeGreaterThanOrEqual(0);
      expect(box.x + box.width).toBeLessThanOrEqual(view.width);
    }
    // nothing of the dialog sits under the fixed bottom bar
    const dialogBottom = (await dialog.boundingBox())!;
    const barTop = (await page.locator('.lesson-bar').boundingBox())!.y;
    const topmost = await page.evaluate(([x, y]) => document.elementFromPoint(x!, y!)?.closest('[data-testid="mock-confirm"]') !== null, [dialogBottom.x + dialogBottom.width / 2, Math.min(dialogBottom.y + dialogBottom.height - 4, barTop + 4)]);
    expect(topmost).toBe(true);
    await page.getByTestId('mock-confirm-cancel').click();
    await expect(page.getByTestId('mock-run')).toBeVisible();
  });

  test('quitting a mock interview uses the dialog too', async ({ page }) => {
    await page.goto('/#/mock');
    await page.getByTestId('mock-start').click();
    await page.getByTestId('mock-run').waitFor();
    await page.getByRole('button', { name: 'Quit mock interview' }).click();
    await expect(page.getByRole('alertdialog', { name: 'Quit this mock interview?' })).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(page.getByTestId('mock-run')).toBeVisible();
  });
});
