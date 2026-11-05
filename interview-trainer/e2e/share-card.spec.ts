import { copyFileSync, readFileSync } from 'node:fs';
import { expect, test, type Page } from '@playwright/test';
import { KEY_V2, SHOTS, TODAY, dueCards, expectNoOverflow, seed, useClock, v2 } from './engage';

const practised = () =>
  v2({ xp: 260, streak: 5, longestStreak: 5, lastDay: TODAY, stats: { sessions: 6, answered: 40, firstTry: 30, perfect: 1, practiceDays: 5 }, ...dueCards(14, { interval: 8, due: '2026-11-20' }) });

async function openCard(page: Page) {
  await useClock(page);
  await seed(page, { [KEY_V2]: practised() });
  await page.goto('/#/progress');
  await page.getByTestId('share-open').click();
  await expect(page.getByTestId('share-preview')).toBeVisible();
}

test.describe('share card', () => {
  test('is not offered before there is anything to share', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/#/progress');
    await page.locator('.progress-page').waitFor();
    await expect(page.getByTestId('share-open')).toHaveCount(0);
  });

  test('preview has real alt text, only the data the learner sees, and fits a phone', async ({ page }) => {
    await openCard(page);
    const img = page.getByTestId('share-preview');
    const alt = (await img.getAttribute('alt'))!;
    expect(alt).toMatch(/^Interview Trainer progress card: 5-day streak, rank Engineer, readiness \d+ \(.+\)/);
    expect(alt).toMatch(/strongest topic .+ \d+%/);
    expect(await img.evaluate((i: HTMLImageElement) => [i.naturalWidth, i.naturalHeight])).toEqual([1080, 1350]);
    await expectNoOverflow(page);
    await expect(page.getByRole('textbox', { name: 'Text version of the card' })).toHaveValue(/Streak: 5 days\nRank: Engineer\nReadiness:/);
    await page.screenshot({ path: `${SHOTS}/share-card-sheet.png` });
  });

  test('fallback: download a real PNG when the browser cannot share files', async ({ page }) => {
    await page.addInitScript(() => {
      Object.defineProperty(navigator, 'share', { value: undefined, configurable: true });
      Object.defineProperty(navigator, 'canShare', { value: undefined, configurable: true });
    });
    await openCard(page);
    await expect(page.getByTestId('share-native')).toHaveCount(0);
    const [file] = await Promise.all([page.waitForEvent('download'), page.getByTestId('share-download').click()]);
    expect(file.suggestedFilename()).toBe('interview-trainer-progress.png');
    const path = (await file.path())!;
    const bytes = readFileSync(path);
    expect([...bytes.subarray(0, 4)]).toEqual([0x89, 0x50, 0x4e, 0x47]);
    expect(bytes.length).toBeGreaterThan(5_000);
    copyFileSync(path, `${SHOTS}/share-card.png`);
    await expect(page.getByTestId('share-message')).toContainText('Image saved');
  });

  test('navigator.share receives the PNG file and the text', async ({ page }) => {
    await page.addInitScript(() => {
      Object.defineProperty(navigator, 'canShare', { value: (d: { files?: File[] }) => Array.isArray(d.files), configurable: true });
      Object.defineProperty(navigator, 'share', {
        configurable: true,
        value: async (d: { files?: File[]; text?: string; title?: string }) => {
          (window as unknown as { __shared: unknown }).__shared = { files: (d.files ?? []).map((f) => ({ name: f.name, type: f.type, size: f.size })), text: d.text, title: d.title };
        },
      });
    });
    await openCard(page);
    await page.getByTestId('share-native').click();
    const shared = await page.evaluate(() => (window as unknown as { __shared: any }).__shared);
    expect(shared.files).toHaveLength(1);
    expect(shared.files[0]).toMatchObject({ name: 'interview-trainer-progress.png', type: 'image/png' });
    expect(shared.files[0].size).toBeGreaterThan(5_000);
    expect(shared.text).toContain('Streak: 5 days');
  });

  test('cancelling the share sheet is not an error', async ({ page }) => {
    await page.addInitScript(() => {
      Object.defineProperty(navigator, 'canShare', { value: () => true, configurable: true });
      Object.defineProperty(navigator, 'share', {
        configurable: true,
        value: async () => {
          throw new DOMException('cancelled', 'AbortError');
        },
      });
    });
    await openCard(page);
    await page.getByTestId('share-native').click();
    await expect(page.getByTestId('share-message')).toHaveCount(0);
  });

  test('text-only share is used when files cannot be shared', async ({ page }) => {
    await page.addInitScript(() => {
      Object.defineProperty(navigator, 'canShare', { value: () => false, configurable: true });
      Object.defineProperty(navigator, 'share', {
        configurable: true,
        value: async (d: { files?: File[]; text?: string }) => {
          (window as unknown as { __shared: unknown }).__shared = { files: d.files?.length ?? 0, text: d.text };
        },
      });
    });
    await openCard(page);
    await page.getByTestId('share-native').click();
    const shared = await page.evaluate(() => (window as unknown as { __shared: any }).__shared);
    expect(shared.files).toBe(0);
    expect(shared.text).toContain('Rank: Engineer');
  });

  test('copy text works, or says why it cannot', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']).catch(() => {});
    await openCard(page);
    await page.getByTestId('share-copy').click();
    await expect(page.getByTestId('share-message')).toHaveText(/Text copied|Copying is not allowed/);
  });
});
