import { expect, test } from '@playwright/test';
import { ACHIEVEMENTS, questionHash } from '../shared/src';
import { KEY_V2, SHOTS, day, expectNoOverflow, homeAfterSummary, lessons, playLesson, questions, seed, stored, useClock, v2 } from './engage';

const LESSON = 'kotlin-null-types';
const toast = (page: import('@playwright/test').Page) => page.getByTestId('badge-toast');

test.describe('achievements', () => {
  /** Library: save the first result for `term` with a private note, which unlocks Note taker. */
  async function saveNote(page: import('@playwright/test').Page, term: string) {
    await page.goto('/#/library');
    await page.getByRole('searchbox', { name: 'Search questions' }).fill(term);
    await page.getByRole('button', { name: 'Save or note' }).first().click();
    await page.getByTestId(/^lib-.*-note-btn$/).first().click();
    await page.getByRole('textbox', { name: 'Your private note' }).fill('x');
    await page.getByTestId(/^lib-.*-note-save$/).first().click();
  }

  test('finishing a perfect first lesson shows its three badges on the summary, not in a notice over it', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    await playLesson(page, LESSON);
    await expect(page.locator('.summary h1')).toHaveText('Clean run');
    const badges = page.getByTestId('summary-badges');
    await expect(badges).toContainText('3 new badges');
    await expect(badges).toContainText('First lesson');
    await expect(toast(page)).toHaveCount(0); // nothing floats over the result
    // the heading, the badges and the actions are all reachable on screen without overlap
    const h1 = (await page.locator('.summary h1').boundingBox())!;
    expect(h1.y).toBeGreaterThanOrEqual(0);
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/badge-summary.png` });

    await homeAfterSummary(page);
    await expect(toast(page)).toHaveCount(0); // and nothing follows the learner home
    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    await expect(page.getByRole('dialog', { name: 'Settings', exact: true })).toBeVisible();
    await page.getByRole('button', { name: 'Close' }).click();

    const p = await stored(page);
    expect(Object.keys(p!.achievements).sort()).toEqual(['first-lesson', 'first-open', 'perfect-lesson']);

    // the trophy case
    await page.goto('/#/progress');
    const case_ = page.getByTestId('trophy-case');
    await expect(case_).toBeVisible();
    await expect(page.getByTestId('badge-count')).toHaveText(`3/${ACHIEVEMENTS.length}`);
    await expect(page.getByTestId('badge-first-lesson')).toHaveAttribute('data-unlocked', 'true');
    await expect(page.getByTestId('badge-first-lesson')).toContainText(/Unlocked Nov 4, 2026/);
    await expect(page.getByTestId('badge-streak-30')).toHaveAttribute('data-unlocked', 'false');
    await expect(page.getByTestId('badge-streak-30')).toContainText('Locked');
    await expect(page.getByTestId('badge-streak-30')).toContainText('30 days in a row');
    await expectNoOverflow(page);
    await case_.scrollIntoViewIfNeeded();
    await page.screenshot({ path: `${SHOTS}/trophy-case.png` });
  });

  test('outside a session a new badge is a short notice above the bottom bar, and it can be dismissed', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await saveNote(page, 'lateinit');
    await expect(toast(page)).toContainText('New badge: Note taker');
    // above the bottom edge of the screen, clear of the title at the top
    const box = (await toast(page).boundingBox())!;
    const view = page.viewportSize()!;
    expect(box.y + box.height).toBeLessThanOrEqual(view.height - 60);
    expect(box.y).toBeGreaterThan(100);
    await page.screenshot({ path: `${SHOTS}/badge-toast.png` });
    await toast(page).getByRole('button', { name: 'Dismiss badge notification' }).click();
    await expect(toast(page)).toHaveCount(0);
  });

  /** The fifth saved question unlocks Curator: four are already saved. */
  async function saveFifth(page: import('@playwright/test').Page) {
    const some = questions.slice(20, 24);
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ bookmarks: Object.fromEntries(some.map((q) => [q.id, { h: questionHash(q), day: day(-1) }])) }) });
    await page.goto('/#/library');
    const fifth = page.getByTestId('lib-item').nth(4); // the first four are the ones already saved
    await fifth.getByRole('button', { name: 'Save or note' }).click();
    await fifth.getByTestId(/-star$/).click();
    await expect(toast(page)).toContainText('Curator');
  }

  test('a notice goes away by itself after a few seconds', async ({ page }) => {
    await saveFifth(page);
    await page.clock.fastForward(2000);
    await expect(toast(page)).toHaveCount(1);
    await page.clock.fastForward(3000);
    await expect(toast(page)).toHaveCount(0);
  });

  test('a notice does not follow the learner to another screen', async ({ page }) => {
    await saveFifth(page);
    await page.goto('/#/progress');
    await expect(page.locator('.progress-page')).toBeVisible();
    await expect(toast(page)).toHaveCount(0);
  });

  test('motion: the notice slides in only when the device allows motion', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await saveNote(page, 'lateinit');
    await expect(toast(page)).toBeVisible();
    expect(await toast(page).evaluate((el) => getComputedStyle(el).animationName)).toBe('none');

    const second = await page.context().newPage();
    await useClock(second);
    await second.emulateMedia({ reducedMotion: 'no-preference' });
    await second.goto('/');
    await second.evaluate(() => localStorage.clear());
    await second.reload();
    await saveNote(second, 'lateinit');
    await expect(toast(second)).toBeVisible();
    expect(await toast(second).evaluate((el) => getComputedStyle(el).animationName)).toBe('badge-in');
  });

  test('a profile from before badges gets what it already earned silently, once', async ({ page }) => {
    await useClock(page);
    await seed(page, {
      [KEY_V2]: v2({
        xp: 620, longestStreak: 9, streak: 2, lastDay: day(-1),
        stats: { sessions: 14, answered: 130, firstTry: 100, perfect: 2, practiceDays: 12 },
        lessons: { [LESSON]: { bestAccuracy: 1, completions: 1, lastDay: day(-1) } },
      }),
    });
    await page.goto('/');
    await expect(page.getByTestId('rank')).toBeVisible();
    await expect(toast(page)).toHaveCount(0);
    await expect.poll(async () => Object.keys((await stored(page))!.achievements).sort()).toEqual([
      'first-lesson', 'perfect-lesson', 'practice-days-10', 'questions-100', 'rank-senior', 'streak-7',
    ]);
    expect((await stored(page))!.milestones['ach:init']).toBe('2026-11-04');

    await page.goto('/#/progress');
    await expect(page.getByTestId('badge-count')).toHaveText(`6/${ACHIEVEMENTS.length}`);
    await page.reload();
    await expect(toast(page)).toHaveCount(0);
  });

  test('a brand new profile is not written to just by opening the app', async ({ page }) => {
    await useClock(page);
    await page.goto('/');
    await expect(page.getByTestId('rank')).toBeVisible();
    expect(await page.evaluate((k) => localStorage.getItem(k), KEY_V2)).toBeNull();
  });

  test('saving a fifth question unlocks Curator from the Library', async ({ page }) => {
    await useClock(page);
    const some = questions.slice(0, 4);
    await seed(page, { [KEY_V2]: v2({ bookmarks: Object.fromEntries(some.map((q) => [q.id, { h: questionHash(q), day: day(-1) }])) }) });
    await page.goto('/#/library');
    const fifth = page.getByTestId('lib-item').nth(4); // the first four are the ones already saved
    await fifth.getByRole('button', { name: 'Save or note' }).click();
    await fifth.getByTestId(/-star$/).click();
    await expect(toast(page)).toContainText('Curator');
    expect(Object.keys((await stored(page))!.bookmarks)).toHaveLength(5);
    expect(lessons.length).toBeGreaterThan(0);
  });

  test('badge copy has no guilt or fake urgency', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/#/progress');
    const text = (await page.getByTestId('trophy-case').innerText()).toLowerCase();
    for (const bad of ['lose', 'lost', 'hurry', 'expire', 'last chance', 'limited', "don't break"]) expect(text).not.toContain(bad);
  });
});
