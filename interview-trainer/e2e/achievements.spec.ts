import { expect, test } from '@playwright/test';
import { ACHIEVEMENTS, questionHash } from '../shared/src';
import { KEY_V2, SHOTS, day, expectNoOverflow, homeAfterSummary, lessons, playLesson, questions, seed, stored, useClock, v2 } from './engage';

const LESSON = 'kotlin-null-types';
const toast = (page: import('@playwright/test').Page) => page.getByTestId('badge-toast');

test.describe('achievements', () => {
  test('finishing a perfect first lesson unlocks three badges with one calm toast that never blocks the page', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    await playLesson(page, LESSON);
    await expect(page.locator('.summary h1')).toHaveText('Clean run');
    await expect(toast(page)).toBeVisible();
    await expect(toast(page)).toContainText('3 badges unlocked');
    await expect(toast(page)).toContainText('First lesson');
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/badge-toast.png` });

    // the toast never intercepts taps: Continue and the Settings gear work while it is still on screen
    await homeAfterSummary(page);
    await expect(toast(page)).toBeVisible();
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
    await expect(page.getByTestId('badge-first-lesson')).toContainText('Unlocked 2026-11-04');
    await expect(page.getByTestId('badge-streak-30')).toHaveAttribute('data-unlocked', 'false');
    await expect(page.getByTestId('badge-streak-30')).toContainText('Locked');
    await expect(page.getByTestId('badge-streak-30')).toContainText('30 days in a row');
    await expectNoOverflow(page);
    await case_.scrollIntoViewIfNeeded();
    await page.screenshot({ path: `${SHOTS}/trophy-case.png` });
  });

  test('the toast can be dismissed, and goes away by itself', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ stats: { ...v2().stats, perfect: 0 } }) });
    await page.goto('/');
    await playLesson(page, LESSON);
    await expect(toast(page)).toBeVisible();
    await toast(page).getByRole('button', { name: 'Dismiss badge notification' }).click();
    await expect(toast(page)).toHaveCount(0);

    // a second unlock, left alone, times out
    await page.goto('/#/library');
    await page.getByRole('searchbox', { name: 'Search questions' }).fill('lateinit');
    await page.getByRole('button', { name: 'Save or note' }).first().click();
    await page.getByTestId(/^lib-.*-note-btn$/).first().click();
    await page.getByRole('textbox', { name: 'Your private note' }).fill('x');
    await page.getByTestId(/^lib-.*-note-save$/).first().click();
    await expect(toast(page)).toContainText('Note taker');
    await page.clock.fastForward(7000);
    await expect(toast(page)).toHaveCount(0);
  });

  test('motion: the toast slides in only when the device allows motion', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto('/');
    await playLesson(page, LESSON);
    await expect(toast(page)).toBeVisible();
    expect(await toast(page).evaluate((el) => getComputedStyle(el).animationName)).toBe('none');

    const second = await page.context().newPage();
    await useClock(second);
    await second.emulateMedia({ reducedMotion: 'no-preference' });
    await second.goto('/');
    await second.evaluate(() => localStorage.clear());
    await second.reload();
    await playLesson(second, LESSON);
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
