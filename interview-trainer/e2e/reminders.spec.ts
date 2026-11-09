import { readFileSync } from 'node:fs';
import { expect, test, type Page } from '@playwright/test';
import { defaultPrefs } from '../shared/src';
import { dueCards, expectNoOverflow, KEY_V2, openSettingsGroup, seed, setDay, SHOTS, stored, useClock, v2, watchErrors } from './engage';

/** Replaces the Notification API with a recorder (kept in localStorage so it survives reloads). */
async function fakeNotifications(page: Page, permission: 'granted' | 'denied' | 'default' | 'none') {
  await page.addInitScript((perm) => {
    if (perm === 'none') {
      // @ts-expect-error simulate a browser without the API
      delete window.Notification;
      return;
    }
    class FakeNotification {
      static permission = perm;
      static requestPermission() {
        return Promise.resolve(perm === 'default' ? 'granted' : perm);
      }
      constructor(title: string, options?: { body?: string }) {
        const list = JSON.parse(localStorage.getItem('__notes') ?? '[]');
        list.push({ title, body: options?.body ?? '' });
        localStorage.setItem('__notes', JSON.stringify(list));
      }
    }
    Object.defineProperty(window, 'Notification', { value: FakeNotification, configurable: true });
  }, permission);
}

const notes = (page: Page) => page.evaluate(() => JSON.parse(localStorage.getItem('__notes') ?? '[]') as { title: string; body: string }[]);

const withReminder = (over: Partial<ReturnType<typeof defaultPrefs>['reminders']> = {}, rest: Partial<Parameters<typeof v2>[0]> = {}) =>
  v2({ ...dueCards(5), prefs: { ...defaultPrefs(), reminders: { time: '09:00', notify: true, lastNotified: null, ...over } }, ...rest });

async function openReminders(page: Page) {
  await page.getByRole('button', { name: 'Settings', exact: true }).click();
  await openSettingsGroup(page, 'reminders');
  await page.getByRole('heading', { name: 'Reminders' }).scrollIntoViewIfNeeded();
}

test.describe('reminders: calendar file', () => {
  test('pick a time, download a recurring .ics that matches it', async ({ page }) => {
    const errors = watchErrors(page);
    await useClock(page); // Wed 2026-11-04 10:00
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    await openReminders(page);

    const download = page.getByTestId('reminder-ics');
    await expect(download).toBeDisabled(); // nothing chosen yet
    await page.getByTestId('reminder-time').fill('19:30');
    await expect(download).toBeEnabled();
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/reminders-settings.png` });

    const [file] = await Promise.all([page.waitForEvent('download'), download.click()]);
    expect(file.suggestedFilename()).toBe('interview-trainer-daily-reminder.ics');
    const text = readFileSync((await file.path())!, 'utf8');
    expect(text.startsWith('BEGIN:VCALENDAR\r\n')).toBe(true);
    expect(text).toContain('\r\nDTSTART:20261104T193000\r\n');
    expect(text).toContain('\r\nRRULE:FREQ=DAILY\r\n');
    expect(text).toContain('BEGIN:VALARM');
    await expect(page.getByTestId('reminder-message')).toContainText('Calendar file saved');
    expect((await stored(page))!.prefs.reminders.time).toBe('19:30');
    expect(errors).toEqual([]);
  });

  test('rest days become a weekly rule', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ profile: { ...v2().profile, restDays: [0, 6] }, prefs: { ...defaultPrefs(), reminders: { time: '08:15', notify: false, lastNotified: null } } }) });
    await page.goto('/');
    await openReminders(page);
    const [file] = await Promise.all([page.waitForEvent('download'), page.getByTestId('reminder-ics').click()]);
    const text = readFileSync((await file.path())!, 'utf8');
    expect(text).toContain('RRULE:FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR');
    expect(text).toContain('DTSTART:20261105T081500'); // 08:15 has passed today
  });

  test('turning reminders off clears the time and the notification choice', async ({ page }) => {
    await fakeNotifications(page, 'granted');
    await useClock(page);
    await seed(page, { [KEY_V2]: withReminder() });
    await page.goto('/');
    await openReminders(page);
    await expect(page.getByTestId('reminder-notify')).toBeChecked();
    await page.getByTestId('reminder-off').click();
    await expect(page.getByTestId('reminder-time')).toHaveValue('');
    await expect(page.getByTestId('reminder-notify')).not.toBeChecked();
    await expect(page.getByTestId('reminder-ics')).toBeDisabled();
    expect((await stored(page))!.prefs.reminders).toMatchObject({ time: null, notify: false });
  });
});

/** The notification choice sits in a disclosure ("Also notify me when I open the app"); open it (idempotent). */
const openNotify = (page: Page) => page.getByTestId('notify-more').evaluate((d) => ((d as HTMLDetailsElement).open = true));

test.describe('reminders: notification when the app is opened', () => {
  test('opting in asks for permission from the toggle, and explains the limits honestly', async ({ page }) => {
    await fakeNotifications(page, 'default');
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ prefs: { ...defaultPrefs(), reminders: { time: '09:00', notify: false, lastNotified: null } } }) });
    await page.goto('/');
    await openReminders(page);
    await openNotify(page);
    const toggle = page.getByTestId('reminder-notify');
    await expect(page.getByRole('dialog')).toContainText('Browsers usually cannot wake this app while it is closed');
    await toggle.check();
    await expect(toggle).toBeChecked();
    expect((await stored(page))!.prefs.reminders.notify).toBe(true);
  });

  test('denied permission: a calm explanation, the setting stays off', async ({ page }) => {
    await fakeNotifications(page, 'denied');
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ prefs: { ...defaultPrefs(), reminders: { time: '09:00', notify: false, lastNotified: null } } }) });
    await page.goto('/');
    await openReminders(page);
    await openNotify(page);
    await page.getByTestId('reminder-notify').click();
    await expect(page.getByTestId('reminder-message')).toContainText('blocked');
    await expect(page.getByTestId('reminder-notify')).not.toBeChecked();
    expect((await stored(page))!.prefs.reminders.notify).toBe(false);
  });

  test('no Notification API: the toggle is simply absent and the calendar still works', async ({ page }) => {
    await fakeNotifications(page, 'none');
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ prefs: { ...defaultPrefs(), reminders: { time: '09:00', notify: false, lastNotified: null } } }) });
    await page.goto('/');
    await openReminders(page);
    await expect(page.getByTestId('reminder-notify')).toHaveCount(0);
    await expect(page.getByTestId('reminder-ics')).toBeEnabled();
  });

  test('shows exactly one calm notification per day, only when due and not practised', async ({ page }) => {
    await fakeNotifications(page, 'granted');
    await useClock(page);
    await seed(page, { [KEY_V2]: withReminder() });
    await page.goto('/');
    await expect.poll(() => notes(page)).toHaveLength(1);
    const [n] = await notes(page);
    expect(n!.title).toBe('Interview Trainer');
    expect(n!.body).toMatch(/^5 questions are due for review/);
    expect(n!.body).not.toMatch(/streak|lose|miss|!/i);
    expect((await stored(page))!.prefs.reminders.lastNotified).toBe('2026-11-04');

    // Reopening the same day: quiet.
    await page.reload();
    await page.locator('.home').waitFor();
    await page.waitForTimeout(300);
    expect(await notes(page)).toHaveLength(1);

    // Next day, after the time: one more.
    await setDay(page, '2026-11-05');
    await page.reload();
    await expect.poll(() => notes(page)).toHaveLength(2);
  });

  async function quietCase(page: Page, at: Date, progress: ReturnType<typeof v2>) {
    await fakeNotifications(page, 'granted');
    await useClock(page, at);
    await seed(page, { [KEY_V2]: progress });
    await page.goto('/');
    await page.locator('.home').waitFor();
    await page.waitForTimeout(400);
    expect(await notes(page)).toHaveLength(0);
  }

  test('stays quiet before the chosen time', async ({ page }) => {
    await quietCase(page, new Date(2026, 10, 4, 8, 0), withReminder());
  });
  test('stays quiet on a rest day', async ({ page }) => {
    await quietCase(page, new Date(2026, 10, 4, 10, 0), withReminder({}, { profile: { ...v2().profile, restDays: [3] } })); // Wednesday
  });
  test('stays quiet when nothing is due', async ({ page }) => {
    await quietCase(page, new Date(2026, 10, 4, 10, 0), v2({ prefs: { ...defaultPrefs(), reminders: { time: '09:00', notify: true, lastNotified: null } } }));
  });
  test('stays quiet when the learner has switched notifications off', async ({ page }) => {
    await quietCase(page, new Date(2026, 10, 4, 10, 0), withReminder({ notify: false }));
  });
});
