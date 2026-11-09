import { expect, test, type Page } from '@playwright/test';
import { defaultPrefs } from '../shared/src';
import { KEY_V2, SHOTS, TODAY, expectNoOverflow, openSettingsGroup, seed, stored, useClock, v2 } from './engage';

/**
 * Settings (audit N-14, UX-29): the long, rarely-used sections (Reading, Reminders, Your data) fold to one line that says what
 * they are set to, so the sheet is a short list instead of a 1,800px page. Plan, rest days, the effects and Reset stay in view.
 */

const GROUPS = ['reading', 'reminders', 'data'] as const;
const sheet = (page: Page) => page.getByRole('dialog', { name: 'Settings', exact: true });
const isOpen = (page: Page, id: string) => page.getByTestId(`group-${id}`).evaluate((el) => (el as HTMLDetailsElement).open);
const state = (page: Page, id: string) => page.getByTestId(`group-${id}-state`);

async function openSettings(page: Page, over: Parameters<typeof v2>[0] = {}) {
  await useClock(page);
  await seed(page, { [KEY_V2]: v2({ xp: 520, streak: 9, longestStreak: 9, lastDay: TODAY, ...over }) });
  await page.goto('/');
  await page.locator('.home').waitFor();
  await page.getByRole('button', { name: 'Settings', exact: true }).click();
  await sheet(page).waitFor();
}

test.describe('the Settings sheet is short', () => {
  test.beforeEach(async ({ page }) => openSettings(page));

  test('it is far shorter than before, with the three long sections folded', async ({ page }) => {
    const { scroll } = await sheet(page).evaluate((el) => ({ scroll: el.scrollHeight }));
    expect(scroll, 'was about 1,800px with everything open').toBeLessThan(1150);
    for (const id of GROUPS) expect(await isOpen(page, id), id).toBe(false);
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/settings-folded.png` });
  });

  test('the essentials need no tap: plan, rest days, the effects and Reset', async ({ page }) => {
    await expect(page.getByRole('button', { name: 'Edit my plan' })).toBeVisible();
    await expect(sheet(page).getByRole('heading', { name: 'Rest days' })).toBeVisible();
    for (const label of ['Confetti on big moments', 'Vibration on correct answers', 'Ask "How sure are you?"', "Interviewer's tip"]) {
      await expect(sheet(page).getByText(label)).toBeVisible();
    }
    await sheet(page).getByRole('button', { name: 'Reset progress' }).scrollIntoViewIfNeeded();
    await expect(sheet(page).getByRole('button', { name: 'Reset progress' })).toBeVisible();
  });

  test('a folded group says what it is set to', async ({ page }) => {
    await expect(state(page, 'reading')).toHaveText('Medium text');
    await expect(state(page, 'reminders')).toHaveText('Off');
    await expect(state(page, 'data')).toHaveText('Backup and import');
    // its controls are not on the page until it is opened
    await expect(page.getByTestId('text-size-XL')).toBeHidden();
    await expect(page.getByTestId('reminder-time')).toBeHidden();
    await expect(sheet(page).getByRole('button', { name: 'Download backup' })).toBeHidden();
  });

  test('the state follows the controls, and the group stays where the learner left it', async ({ page }) => {
    await openSettingsGroup(page, 'reading');
    await page.getByTestId('text-size-XL').check();
    await expect(state(page, 'reading')).toHaveText('Extra large text');
    await page.getByTestId('dyslexia-font').check();
    await expect(state(page, 'reading')).toHaveText('Extra large text, easier font');
    expect(await isOpen(page, 'reading')).toBe(true);

    await openSettingsGroup(page, 'reminders');
    await page.getByTestId('reminder-time').fill('19:30');
    await expect(state(page, 'reminders')).toHaveText('Daily at 19:30');
    await page.getByTestId('reminder-off').click();
    await expect(state(page, 'reminders')).toHaveText('Off');
  });

  test('every group opens and closes from the keyboard (Enter and Space)', async ({ page }) => {
    for (const id of GROUPS) {
      const summary = page.getByTestId(`group-${id}`).locator(':scope > summary');
      expect((await summary.boundingBox())!.height, `${id} tap target`).toBeGreaterThanOrEqual(44);
      await summary.focus();
      await page.keyboard.press('Enter');
      expect(await isOpen(page, id), `${id} opens`).toBe(true);
      await page.keyboard.press('Space');
      expect(await isOpen(page, id), `${id} closes`).toBe(false);
    }
  });

  test('each group has its own controls when opened', async ({ page }) => {
    await openSettingsGroup(page, 'reading');
    await expect(page.getByTestId('text-size-S')).toBeVisible();
    await expect(page.getByTestId('dyslexia-font')).toBeVisible();
    await openSettingsGroup(page, 'reminders');
    await expect(page.getByTestId('reminder-time')).toBeVisible();
    await openSettingsGroup(page, 'data');
    for (const name of ['Download backup', 'Copy backup text']) await expect(sheet(page).getByRole('button', { name })).toBeVisible();
    await expect(page.getByTestId('import-file')).toBeAttached();
    await expectNoOverflow(page);
  });

  test('no horizontal scroll at 320px, with every group open', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 700 });
    for (const id of GROUPS) await openSettingsGroup(page, id);
    await expectNoOverflow(page);
  });
});

test('a reminder that is set shows in the folded row without opening it', async ({ page }) => {
  await openSettings(page, { prefs: { ...defaultPrefs(), reminders: { time: '08:15', notify: false, lastNotified: null } } });
  await expect(state(page, 'reminders')).toHaveText('Daily at 08:15');
  expect(await isOpen(page, 'reminders')).toBe(false);
  expect((await stored(page))!.prefs.reminders.time).toBe('08:15');
});
