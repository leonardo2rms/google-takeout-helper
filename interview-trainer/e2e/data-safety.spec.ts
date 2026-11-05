/**
 * Data safety in a real browser (QA findings S0-1, S0-2, S1-5, S1-6, S2-18): a failed write never loses sessions,
 * blocked storage and unreadable data are explained, and nothing is overwritten before a copy exists.
 */
import { expect, test, type Page } from '@playwright/test';
import { KEY_V2, TODAY, questions, seed, stored, useClock, v2, watchErrors } from './engage';
import { solveAndContinue } from './solve';

const singles = questions.filter((q) => q.type === 'single').slice(0, 4);

async function practiceOne(page: Page, i: number) {
  await page.goto(`/#/q/${singles[i]!.id}`);
  await page.locator('.prompt').waitFor();
  await solveAndContinue(page, singles[i]!);
  await expect(page.locator('.summary h1')).toBeVisible();
}

/** Makes localStorage.setItem throw for keys matching `window.__failKey` (a RegExp source) while `window.__quota` is true. */
async function installFailingWrites(page: Page) {
  await page.addInitScript(() => {
    const w = window as unknown as { __quota?: boolean; __failKey?: string };
    const real = Storage.prototype.setItem;
    Storage.prototype.setItem = function (key: string, value: string) {
      if (w.__quota && new RegExp(w.__failKey ?? '.').test(key)) throw new DOMException('quota', 'QuotaExceededError');
      return real.call(this, key, value);
    };
  });
}

test.describe('a failed write', () => {
  test('S0-2: sessions played while the quota was exceeded are saved by the next successful write, and the learner is told', async ({ page }) => {
    const errors = watchErrors(page);
    await useClock(page);
    await installFailingWrites(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');

    await practiceOne(page, 0);
    expect((await stored(page))!.xp).toBe(10);
    await expect(page.getByTestId('not-saved')).toHaveCount(0);

    await page.evaluate(() => ((window as unknown as { __quota: boolean }).__quota = true));
    await practiceOne(page, 1);
    await expect(page.getByTestId('not-saved')).toBeVisible();
    await expect(page.getByTestId('not-saved')).toContainText('not being saved');
    expect((await stored(page))!.xp).toBe(10); // the second session is only in memory

    await page.evaluate(() => ((window as unknown as { __quota: boolean }).__quota = false));
    await practiceOne(page, 2);
    await expect(page.getByTestId('not-saved')).toHaveCount(0);
    expect(await stored(page)).toMatchObject({ xp: 30, stats: { sessions: 3 } });
    expect(errors).toEqual([]);
  });

  test('the retry timer saves on its own once the quota is available again', async ({ page }) => {
    await useClock(page);
    await installFailingWrites(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    await page.evaluate(() => ((window as unknown as { __quota: boolean }).__quota = true));
    await practiceOne(page, 0);
    await expect(page.getByTestId('not-saved')).toBeVisible();
    await page.evaluate(() => ((window as unknown as { __quota: boolean }).__quota = false));
    await page.clock.fastForward(5000);
    await expect(page.getByTestId('not-saved')).toHaveCount(0);
    expect((await stored(page))!.xp).toBe(10);
  });
});

test.describe('storage that is blocked outright (a sandboxed host)', () => {
  test('S1-5: the learner is told once progress cannot be kept, and can still take a backup', async ({ page }) => {
    const errors = watchErrors(page);
    await page.addInitScript(() => {
      Object.defineProperty(window, 'localStorage', {
        get() {
          throw new DOMException('The document is sandboxed.', 'SecurityError');
        },
      });
    });
    await page.goto('/');
    await expect(page.getByTestId('not-saved')).toHaveCount(0); // nothing to lose yet

    await practiceOne(page, 0);
    const banner = page.getByTestId('not-saved');
    await expect(banner).toContainText('Your progress is not being saved');
    await expect(banner).toContainText('blocks storage');

    const [download] = await Promise.all([page.waitForEvent('download'), banner.getByRole('button', { name: 'Download backup' }).click()]);
    expect(download.suggestedFilename()).toMatch(/^interview-trainer-progress-\d{4}-\d{2}-\d{2}\.json$/);

    await banner.getByRole('button', { name: 'Dismiss' }).click();
    await expect(banner).toHaveCount(0);
    expect(errors).toEqual([]);
  });

  test('when downloads and the clipboard are blocked the backup text is shown to select by hand', async ({ page }) => {
    await page.addInitScript(() => {
      Object.defineProperty(window, 'localStorage', {
        get() {
          throw new DOMException('denied', 'SecurityError');
        },
      });
      URL.createObjectURL = () => {
        throw new Error('blocked');
      };
      Object.defineProperty(navigator, 'clipboard', { value: { writeText: () => Promise.reject(new Error('blocked')) } });
    });
    await page.goto('/');
    await practiceOne(page, 0);
    const banner = page.getByTestId('not-saved');
    await banner.getByRole('button', { name: 'Copy backup text' }).click();
    await expect(banner.getByRole('textbox', { name: 'Backup text' })).toHaveValue(/"xp": 10/);
    await expect(banner).toContainText('Copying is blocked here');
  });
});

test.describe('unreadable data', () => {
  const future = JSON.stringify({ version: 3, xp: 999, note: 'written by a newer app' });

  test('S0-1: newer-version data is not overwritten while its backup cannot be written', async ({ page }) => {
    await useClock(page);
    await installFailingWrites(page);
    await page.addInitScript(() => ((window as unknown as { __failKey: string }).__failKey = '\\.backup-'));
    await seed(page, { [KEY_V2]: future });
    await page.addInitScript(() => ((window as unknown as { __quota: boolean }).__quota = true));
    await page.goto('/');
    await practiceOne(page, 0);
    expect(await page.evaluate((k) => localStorage.getItem(k), KEY_V2)).toBe(future);
    await expect(page.getByTestId('not-saved')).toContainText('could not be read');
    // the session itself still counts in this tab
    await page.goto('/#/');
    await expect(page.locator('.stat[aria-label="10 XP"]')).toBeVisible();

    // once copying works again the old data is set aside and the new progress is saved
    await page.evaluate(() => ((window as unknown as { __quota: boolean }).__quota = false));
    await page.clock.fastForward(5000);
    await expect(page.getByTestId('not-saved')).toHaveCount(0);
    expect(await stored(page)).toMatchObject({ version: 2, xp: 10 });
    const backups = await page.evaluate(() => Object.keys(localStorage).filter((k) => k.includes('.backup-')));
    expect(backups).toHaveLength(1);
    expect(await page.evaluate((k) => localStorage.getItem(k), backups[0]!)).toBe(future);
  });

  test('S1-6: parked data is announced, listed under Settings, downloadable, and the notice goes away once seen', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: future });
    await page.goto('/');
    await practiceOne(page, 0);
    await expect(page.getByTestId('recovered-notice')).toContainText('Some saved data could not be read');

    await page.goto('/#/');
    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    const section = page.getByTestId('recovered-data');
    await expect(section).toContainText('Recovered data');
    await expect(section.getByTestId('recovered-item')).toHaveCount(1);
    await expect(section).toContainText('Unreadable data set aside');
    await expect(section.getByRole('button', { name: /Restore/ })).toHaveCount(0); // this version cannot read it
    const [download] = await Promise.all([page.waitForEvent('download'), section.getByRole('button', { name: 'Download', exact: true }).click()]);
    expect(download.suggestedFilename()).toContain('recovered');
    await page.getByRole('button', { name: 'Close' }).click();

    await page.reload();
    await expect(page.getByTestId('recovered-notice')).toHaveCount(0);
  });

  test('a second corruption does not replace the first copy', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: '{first corrupt' });
    await page.goto('/');
    await practiceOne(page, 0);
    await page.evaluate((k) => localStorage.setItem(k, '{second corrupt'), KEY_V2);
    await page.reload();
    await practiceOne(page, 1);
    const copies = await page.evaluate(() =>
      Object.keys(localStorage).filter((k) => k.includes('.backup-')).sort().map((k) => localStorage.getItem(k)),
    );
    expect(copies).toEqual(['{first corrupt', '{second corrupt']);
  });
});

test.describe('restoring a safety copy', () => {
  test('S2-18: a copy kept before a replace can be merged back from Settings', async ({ page }) => {
    await useClock(page);
    const before = v2({ xp: 500, streak: 0 });
    await seed(page, { [KEY_V2]: v2({ xp: 20 }), [`interview-trainer.progress.before-${TODAY.replaceAll('-', '')}-01`]: before });
    await page.goto('/');
    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    const item = page.getByTestId('recovered-item');
    await expect(item).toContainText('before a replace or reset');
    await expect(item).toContainText('500 XP');
    await item.getByRole('button', { name: 'Restore (merge)' }).click();
    await expect(page.getByTestId('recovered-message')).toContainText('Merged');
    expect((await stored(page))!.xp).toBe(500);
  });

  test('restoring by replace keeps the previous progress as a new safety copy', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ xp: 20 }), [`interview-trainer.progress.before-${TODAY.replaceAll('-', '')}-01`]: v2({ xp: 500 }) });
    await page.goto('/');
    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    await page.getByRole('button', { name: 'Restore (replace)' }).click();
    await expect(page.getByTestId('recovered-item')).toHaveCount(2);
    expect((await stored(page))!.xp).toBe(500);
    const copies = await page.evaluate(() => Object.keys(localStorage).filter((k) => k.includes('.before-')).map((k) => JSON.parse(localStorage.getItem(k)!).xp));
    expect(copies.sort()).toEqual([20, 500]);
  });
});
