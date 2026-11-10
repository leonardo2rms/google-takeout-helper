import { expect, test, type Page } from '@playwright/test';
import { KEY_V2, SHOTS, TODAY, course, dueCards, expectNoOverflow, lessons, playLesson, seed, stored, useClock, v2 } from './engage';

/**
 * Role-targeted tracks (P2-6): the role chosen in the plan puts its units first on Home, leads "Start" and "Up next" through
 * them, and favours them in Mock. Unit and lesson numbers stay the course's, so "Unit 05" means the same for everyone.
 */

const lessonRecord = { bestAccuracy: 1, completions: 1, lastDay: TODAY };
const session = { sessions: 14, answered: 120, firstTry: 90, perfect: 2, practiceDays: 9 };
const unitOrder = (page: Page) => page.evaluate(() => Array.from(document.querySelectorAll<HTMLElement>('details.unit-section')).map((d) => d.dataset.testid!.replace('unit-', '')));
const firstLessonOf = (unitId: string) => course.units.find((u) => u.id === unitId)!.lessons[0]!;
const ARCHITECT_FIRST = ['ddd', 'scale', 'complexity', 'ai', 'system-design', 'security-observability'];
const BACKEND_FIRST = ['kotlin', 'coroutines', 'spring', 'jvm', 'data', 'messaging', 'testing'];

async function homeAs(page: Page, role: 'backend-kotlin' | 'staff-architect' | 'generalist' | null, over: Record<string, unknown> = {}) {
  await useClock(page);
  const base = v2();
  await seed(page, { [KEY_V2]: v2({ profile: { ...base.profile, role }, ...over }) });
  await page.goto('/');
  await page.locator('.home').waitFor();
}

test.describe('Home follows the role', () => {
  test('an architect sees the architect units first, labelled, with the course numbers', async ({ page }) => {
    await homeAs(page, 'staff-architect');
    const order = await unitOrder(page);
    expect(order.slice(0, ARCHITECT_FIRST.length)).toEqual(ARCHITECT_FIRST);
    expect(order).toHaveLength(course.units.length);
    await expect(page.getByTestId('track-track')).toHaveText('Your track · Staff / architect');
    await expect(page.getByTestId('track-other')).toHaveText('Other topics');
    // the heading sits right before the first unit of each group
    await expect(page.getByTestId('track-track').locator('xpath=following-sibling::*[1]')).toHaveAttribute('data-testid', 'unit-ddd');
    await expect(page.getByTestId('track-other').locator('xpath=following-sibling::*[1]')).toHaveAttribute('data-testid', 'unit-kotlin');
    // numbers are the course's: DDD is unit 5 wherever it is listed
    const ddd = course.units.findIndex((u) => u.id === 'ddd') + 1;
    await expect(page.getByTestId('unit-ddd').locator('summary .label')).toHaveText(`Unit ${String(ddd).padStart(2, '0')}`);
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/home-track.png` });
  });

  test('a backend learner sees the backend units first', async ({ page }) => {
    await homeAs(page, 'backend-kotlin');
    expect((await unitOrder(page)).slice(0, BACKEND_FIRST.length)).toEqual(BACKEND_FIRST);
    await expect(page.getByTestId('track-track')).toContainText('Backend / Kotlin');
  });

  for (const role of ['generalist', null] as const) {
    test(`${role ?? 'no role'}: the course order is kept and there are no headings`, async ({ page }) => {
      await homeAs(page, role);
      expect(await unitOrder(page)).toEqual(course.units.map((u) => u.id));
      await expect(page.getByTestId('track-track')).toHaveCount(0);
      await expect(page.getByTestId('track-other')).toHaveCount(0);
    });
  }

  test('the first lesson, and Up next, lead through the track', async ({ page }) => {
    // day 0: "Start your first lesson" is the track's first lesson
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ profile: { ...v2().profile, role: 'staff-architect' }, stats: { sessions: 0, answered: 0, firstTry: 0, perfect: 0, practiceDays: 0 } }) });
    await page.goto('/');
    await page.locator('.home').waitFor();
    await expect(page.getByTestId('daily-card')).toHaveAttribute('href', `#/lesson/${firstLessonOf('ddd').id}`);
  });

  test('Up next is the next lesson of the track, and the open unit is the one being worked in', async ({ page }) => {
    const ddd = course.units.find((u) => u.id === 'ddd')!;
    const done = Object.fromEntries(ddd.lessons.map((l) => [l.id, lessonRecord]));
    await homeAs(page, 'staff-architect', { lessons: done, xp: 300, streak: 3, longestStreak: 3, lastDay: TODAY, stats: session, cards: dueCards(3).cards });
    // the whole of DDD is done, so the next lesson is the first of the next track unit (scale), not the course's first lesson
    await expect(page.getByTestId('up-next')).toHaveAttribute('href', `#/lesson/${firstLessonOf('scale').id}`);
    expect(await page.getByTestId('unit-scale').evaluate((el) => (el as HTMLDetailsElement).open)).toBe(true);
    expect(await page.getByTestId('unit-kotlin').evaluate((el) => (el as HTMLDetailsElement).open)).toBe(false);
  });

  test('finishing a lesson offers the next one of the track', async ({ page }) => {
    const first = firstLessonOf('ddd');
    await homeAs(page, 'staff-architect', { xp: 300, streak: 3, longestStreak: 3, lastDay: TODAY, stats: session });
    await playLesson(page, first.id);
    const next = course.units.find((u) => u.id === 'ddd')!.lessons[1]!;
    await expect(page.getByRole('link', { name: /Next lesson/ })).toHaveAttribute('href', `#/lesson/${next.id}`);
  });

  test('choosing a role in the plan reorders Home at once, and Settings names the track', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ profile: { ...v2().profile, onboarded: false }, xp: 300, streak: 3, longestStreak: 3, lastDay: TODAY, stats: session }) });
    await page.goto('/');
    await page.locator('.home').waitFor();
    expect((await unitOrder(page))[0]).toBe('kotlin');
    const card = page.getByTestId('onboarding');
    await expect(card.getByText('DDD, scale, system design and AI first')).toBeVisible();
    await card.getByLabel(/Staff \/ architect/).check();
    await card.getByRole('button', { name: 'Save plan', exact: true }).click();
    expect((await unitOrder(page))[0]).toBe('ddd');
    expect((await stored(page))!.profile.role).toBe('staff-architect');

    await page.getByRole('button', { name: 'Settings', exact: true }).first().click();
    await expect(page.getByRole('dialog')).toContainText('Track: Staff / architect');
  });

  test('every lesson stays reachable: all units are listed, the lesson count is unchanged', async ({ page }) => {
    await homeAs(page, 'staff-architect');
    await page.getByTestId('units-toggle').click();
    await expect(page.locator('a.lesson-row')).toHaveCount(lessons.length);
  });
});

test.describe('Mock follows the role', () => {
  test('the topic list groups the track first and says Mock leans towards it', async ({ page }) => {
    await homeAs(page, 'staff-architect', { xp: 300, streak: 3, longestStreak: 3, lastDay: TODAY, stats: session });
    await page.goto('/#/mock');
    const select = page.getByTestId('mock-topic');
    await expect(select.locator('optgroup')).toHaveCount(2);
    await expect(select.locator('optgroup').first()).toHaveAttribute('label', 'Your track · Staff / architect');
    expect(await select.locator('optgroup').first().locator('option').count()).toBe(ARCHITECT_FIRST.length);
    await expect(page.getByText(/come up a little more often/)).toBeVisible();
    await select.selectOption('kotlin'); // any topic stays choosable
    await expect(page.getByText(/come up a little more often/)).toHaveCount(0);
  });

  test('a generalist sees a plain list, as before', async ({ page }) => {
    await homeAs(page, 'generalist', { xp: 300, streak: 3, longestStreak: 3, lastDay: TODAY, stats: session });
    await page.goto('/#/mock');
    await expect(page.getByTestId('mock-topic').locator('optgroup')).toHaveCount(0);
    await expect(page.getByText(/come up a little more often/)).toHaveCount(0);
  });
});
