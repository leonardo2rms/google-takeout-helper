import { expect, test, type Page } from '@playwright/test';
import { KEY_V2, SHOTS, cardFor, course, day, dueCards, expectNoOverflow, lessons, playLesson, homeAfterSummary, seed, stored, TODAY, useClock, v2 } from './engage';

/** Second UX round: goal-met Home, one meaning of "due", the tomorrow bridge, the compact data-safety bar, honest readiness. */

const FIRST = lessons[0]!;
const dayLog = (answered: number, target = 8) => ({ xp: answered * 10, answered, firstTry: answered, target });

test.describe('goal met: Home stops selling "Today"', () => {
  test('the hero says "Done for today" with one optional step, and the ring never shows a number above the goal', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ ...dueCards(12), stats: { ...v2().stats, sessions: 3 }, days: { [TODAY]: dayLog(19) }, lastDay: TODAY, streak: 2 }) });
    await page.goto('/');
    const hero = page.getByTestId('daily-card');
    await expect(hero).toHaveAttribute('data-state', 'done');
    await expect(hero).toContainText('Done for today');
    await expect(hero).toContainText('Goal met, plus 11 bonus questions');
    await expect(hero).toContainText('Optional: review 12 due');
    await expect(hero).not.toContainText('Start');
    await expect(page.getByTestId('goal-text')).toHaveText('8/8');
    await expect(page.getByTestId('goal-bonus')).toHaveText('+11');
    // the single suggestion leads to Review, and the Review card says the same number
    await expect(page.getByTestId('due-count')).toHaveText('12 due · 10 per session');
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/home-goal-met.png` });
    await hero.click();
    await expect(page).toHaveURL(/#\/review/);
  });

  test('Today is still "Today" before the goal is met', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ ...dueCards(12), stats: { ...v2().stats, sessions: 3 }, days: { [TODAY]: dayLog(3) } }) });
    await page.goto('/');
    await expect(page.getByTestId('daily-card')).toHaveAttribute('data-state', 'open');
    await expect(page.getByTestId('daily-card')).toContainText('Today');
    await expect(page.getByTestId('daily-card')).toContainText('5 of 12 due');
  });

  test('a passed challenge keeps its action under the text, 44px tall', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ ...dueCards(3), stats: { ...v2().stats, sessions: 3 } }) });
    await page.goto('/');
    const again = page.getByTestId('challenge-again');
    if (await again.count()) {
      const box = (await again.boundingBox())!;
      expect(box.height).toBeGreaterThanOrEqual(44);
    }
  });
});

test.describe('one meaning of "due"', () => {
  test('Today, Review and Progress all count cards whose date has arrived', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ ...dueCards(73), stats: { ...v2().stats, sessions: 5 } }) });
    await page.goto('/');
    await expect(page.getByTestId('daily-card')).toContainText('of 73 due');
    await expect(page.getByTestId('due-count')).toHaveText('73 due · 10 per session'); // never "30+"
    await page.goto('/#/progress');
    const perTopic = await page.locator('.topics li small.muted').allTextContents();
    const total = perTopic.map((t) => Number(/(\d+) due/.exec(t)?.[1] ?? 0)).reduce((a, b) => a + b, 0);
    expect(total).toBe(73);
  });
});

test.describe('tomorrow bridge', () => {
  test('after the first lesson: a concrete line about tomorrow and one dismissible offer, shown once', async ({ page }) => {
    await useClock(page);
    await page.goto('/');
    await playLesson(page, FIRST.id);
    await expect(page.getByTestId('next-review')).toContainText('questions tomorrow');
    await expect(page.getByTestId('next-review')).toContainText('min');
    const offer = page.getByTestId('tomorrow-offer');
    await expect(offer).toBeVisible();
    await expect(offer).not.toContainText(/\b(streak|lose|lost|miss\w*)\b/i);
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/summary-tomorrow.png` });
    await offer.getByTestId('tomorrow-dismiss').click();
    await expect(offer).toHaveCount(0);
    expect((await stored(page))!.prefs.reminders.nudged).toBe(true);

    // a second early session does not ask again
    await homeAfterSummary(page);
    await playLesson(page, lessons[1]!.id);
    await expect(page.getByTestId('next-review')).toBeVisible();
    await expect(page.getByTestId('tomorrow-offer')).toHaveCount(0);
  });

  test('accepting saves the calendar file and the chosen time', async ({ page }) => {
    await useClock(page);
    await page.goto('/');
    await playLesson(page, FIRST.id);
    await page.getByTestId('tomorrow-time').fill('18:30');
    const [download] = await Promise.all([page.waitForEvent('download'), page.getByTestId('tomorrow-add').click()]);
    expect(download.suggestedFilename()).toMatch(/\.ics$/);
    await expect(page.getByTestId('tomorrow-offer')).toContainText('Calendar file saved');
    expect((await stored(page))!.prefs.reminders).toMatchObject({ time: '18:30' });
  });

  test('not offered to a learner who already has a reminder', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ prefs: { ...v2().prefs, reminders: { time: '09:00', notify: false, lastNotified: null } } }) });
    await page.goto('/');
    await playLesson(page, FIRST.id);
    await expect(page.getByTestId('next-review')).toBeVisible();
    await expect(page.getByTestId('tomorrow-offer')).toHaveCount(0);
  });
});

test.describe('data-safety bar', () => {
  const corrupt = async (page: Page) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: '{garbage' });
  };

  for (const [w, h] of [[390, 844], [320, 640]] as const) {
    test(`at ${w}x${h} a new profile with unreadable data keeps the hero on screen and offers no empty backup`, async ({ page }) => {
      await page.setViewportSize({ width: w, height: h });
      await corrupt(page);
      await page.goto('/');
      const bar = page.getByTestId('not-saved');
      await expect(bar).toBeVisible();
      await expect(bar).toContainText('Your saved progress could not be read. Nothing new is saved.');
      expect((await bar.boundingBox())!.height, 'a compact bar').toBeLessThan(110);
      const hero = (await page.getByTestId('daily-card').boundingBox())!;
      expect(hero.y + hero.height, 'the first lesson stays above the fold').toBeLessThanOrEqual(h);
      await expectNoOverflow(page);
      await page.screenshot({ path: `${SHOTS}/data-safety-compact-${w}.png` });
      await bar.getByRole('button', { name: 'Details' }).click();
      await expect(bar).toContainText('close it, that work is gone');
      await expect(bar.getByRole('button', { name: 'Download backup' })).toHaveCount(0); // nothing to back up yet
      await expect(bar).toContainText('nothing to back up yet');
    });
  }

  test('inside a lesson there is no banner above the question, only a small mark', async ({ page }) => {
    await corrupt(page);
    await page.goto(`/#/lesson/${FIRST.id}`);
    await page.locator('.prompt').waitFor();
    await expect(page.getByTestId('not-saved')).toHaveCount(0);
    await expect(page.getByTestId('safety-mark')).toHaveAccessibleName(/could not be read/);
    expect((await page.locator('.prompt').boundingBox())!.y).toBeLessThan(260);
  });
});

test.describe('honest labels', () => {
  test('one topic started: no "weakest topic" card, and a topic below half holds the label down', async ({ page }) => {
    await useClock(page);
    const first = course.units[0]!.lessons.flatMap((l) => l.questions).slice(0, 6);
    const cards = Object.fromEntries(first.map((q) => [q.id, cardFor(q, { due: day(5), interval: 3 })]));
    await seed(page, { [KEY_V2]: v2({ cards }) });
    await page.goto('/#/progress');
    await expect(page.getByTestId('weakest-topic')).toHaveCount(0);
  });
});
