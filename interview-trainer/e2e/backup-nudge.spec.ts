import { expect, test, type Page } from '@playwright/test';
import { BACKUP_KEY, KEY_V2, SHOTS, TODAY, day, dueCards, expectNoOverflow, seed, useClock, v2 } from './engage';

/**
 * The backup reminder (P2-4): progress lives on this device only, so someone with real progress and no recent backup gets one quiet,
 * dismissible line on Home. A newcomer is never nagged, "Later" is respected, and only a backup that really left the device counts.
 */

const session = (practiceDays: number) => ({ sessions: 14, answered: 120, firstTry: 90, perfect: 2, practiceDays });
const note = (page: Page) => page.evaluate((k) => JSON.parse(localStorage.getItem(k) ?? 'null') as { lastBackup: string | null; snoozedUntil: string | null } | null, BACKUP_KEY);

async function homeWith(page: Page, practiceDays: number, backup: { lastBackup: string | null; snoozedUntil?: string | null } | null) {
  await useClock(page);
  const entries: Record<string, unknown> = { [KEY_V2]: v2({ xp: 520, streak: 3, longestStreak: 9, lastDay: TODAY, stats: session(practiceDays), cards: dueCards(3).cards }) };
  if (backup) entries[BACKUP_KEY] = { v: 1, snoozedUntil: null, ...backup };
  else entries[BACKUP_KEY] = 'unreadable'; // the shared seed would otherwise say "just backed up"
  await seed(page, entries);
  await page.goto('/');
  await page.locator('.home').waitFor();
}

const nudge = (page: Page) => page.getByTestId('backup-nudge');

test.describe('when it shows', () => {
  test('a learner with progress and no backup sees one calm line, with the days they would lose', async ({ page }) => {
    await homeWith(page, 9, null);
    await expect(nudge(page)).toBeVisible();
    await expect(nudge(page)).toContainText('stored only on this device');
    await expect(nudge(page)).toContainText('9 days of practice');
    await expect(page.getByRole('dialog')).toHaveCount(0); // never a modal
    await expect(page.getByTestId('daily-card')).toBeVisible(); // and Today is still the main action
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/home-backup-nudge.png` });
  });

  test('never for someone who has barely started', async ({ page }) => {
    await homeWith(page, 4, null);
    await expect(nudge(page)).toHaveCount(0);
  });

  test('not while the last backup is recent (13 days)', async ({ page }) => {
    await homeWith(page, 20, { lastBackup: day(-13) });
    await expect(nudge(page)).toHaveCount(0);
  });

  test('it returns, saying how old the backup is, once it is two weeks old', async ({ page }) => {
    await homeWith(page, 20, { lastBackup: day(-14) });
    await expect(nudge(page)).toContainText('Your last backup was 14 days ago');
  });

  test('an unreadable note is the same as no backup yet, and the page is fine', async ({ page }) => {
    await homeWith(page, 9, null);
    await expect(nudge(page)).toContainText('stored only on this device');
  });
});

test.describe('what the buttons do', () => {
  test('Download backup saves the file, remembers the day and the line goes away for good', async ({ page }) => {
    await homeWith(page, 9, null);
    const [file] = await Promise.all([page.waitForEvent('download'), page.getByTestId('backup-now').click()]);
    expect(file.suggestedFilename()).toBe(`interview-trainer-progress-${TODAY}.json`);
    await expect(nudge(page)).toHaveCount(0);
    expect(await note(page)).toMatchObject({ lastBackup: TODAY, snoozedUntil: null });
    await page.reload();
    await page.locator('.home').waitFor();
    await expect(nudge(page)).toHaveCount(0);

    // Settings says so too
    await page.getByRole('button', { name: 'Settings', exact: true }).first().click();
    await page.getByTestId('group-data').locator(':scope > summary').click();
    await expect(page.getByTestId('last-backup')).toHaveText('Last backup: today.');
  });

  test('Later hides it, keeps the old backup date, and survives a reload', async ({ page }) => {
    await homeWith(page, 9, { lastBackup: day(-30) });
    await expect(nudge(page)).toContainText('30 days ago');
    await page.getByTestId('backup-later').click();
    await expect(nudge(page)).toHaveCount(0);
    expect(await note(page)).toEqual({ v: 1, lastBackup: day(-30), snoozedUntil: day(7) });
    await page.reload();
    await page.locator('.home').waitFor();
    await expect(nudge(page)).toHaveCount(0);
  });

  test('a snooze that has run out brings it back', async ({ page }) => {
    await homeWith(page, 9, { lastBackup: day(-30), snoozedUntil: TODAY });
    await expect(nudge(page)).toBeVisible();
  });

  test('a download that is blocked does not count: the line stays and says what to do', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ xp: 520, streak: 3, lastDay: TODAY, stats: session(9), cards: dueCards(3).cards }), [BACKUP_KEY]: 'unreadable' });
    await page.addInitScript(() => {
      HTMLAnchorElement.prototype.click = () => {
        throw new Error('blocked');
      };
    });
    await page.goto('/');
    await page.locator('.home').waitFor();
    await page.getByTestId('backup-now').click();
    await expect(nudge(page)).toBeVisible();
    await expect(page.getByTestId('backup-nudge-msg')).toContainText(/blocked|Settings/);
    expect(await page.evaluate((k) => localStorage.getItem(k), BACKUP_KEY)).toBe('unreadable'); // nothing was recorded
  });

  test('Settings: Copy backup text counts as a backup; "never" reads plainly before', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    await homeWith(page, 9, null);
    await page.getByRole('button', { name: 'Settings', exact: true }).first().click();
    await page.getByTestId('group-data').locator(':scope > summary').click();
    await expect(page.getByTestId('last-backup')).toHaveText('No backup made from this device yet.');
    await page.getByRole('button', { name: 'Copy backup text' }).click();
    await expect(page.getByTestId('import-message')).toContainText('Backup copied');
    await expect(page.getByTestId('last-backup')).toHaveText('Last backup: today.');
    expect(await note(page)).toMatchObject({ lastBackup: TODAY });
  });

  test('Settings: a blocked clipboard is not a backup', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ xp: 520, streak: 3, lastDay: TODAY, stats: session(9), cards: dueCards(3).cards }), [BACKUP_KEY]: 'unreadable' });
    await page.addInitScript(() => Object.defineProperty(navigator, 'clipboard', { value: { writeText: () => Promise.reject(new Error('blocked')) } }));
    await page.goto('/');
    await page.locator('.home').waitFor();
    await page.getByRole('button', { name: 'Settings', exact: true }).first().click();
    await page.getByTestId('group-data').locator(':scope > summary').click();
    await page.getByRole('button', { name: 'Copy backup text' }).click();
    await expect(page.getByTestId('import-error')).toContainText('Copying is not allowed');
    await expect(page.getByTestId('last-backup')).toHaveText('No backup made from this device yet.');
    expect(await page.evaluate((k) => localStorage.getItem(k), BACKUP_KEY)).toBe('unreadable');
  });
});
