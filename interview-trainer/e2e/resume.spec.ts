import { expect, test, type Page } from '@playwright/test';
import { KEY_V2, RESUME_KEY, SHOTS, course, currentQuestion, expectNoOverflow, lessons, seed, stored, useClock, v2 } from './engage';
import { answerCorrectly, check, next, solveAndContinue } from './solve';

/**
 * Leaving a lesson half way (the X, or the browser's Back button) loses nothing: the session is saved after every
 * question and Home offers to carry on. Opening the same lesson or practice again carries on too.
 */

const lesson = lessons[0]!;
const other = lessons[1]!;
const total = lesson.questions.length;

const savedRecord = (page: Page) => page.evaluate((k) => JSON.parse(localStorage.getItem(k) ?? 'null') as Record<string, any> | null, RESUME_KEY);
const leave = (page: Page) => page.getByRole('button', { name: 'Leave lesson' }).click();

/** Opens the lesson and answers its first `n` questions, ending on question n + 1. */
async function playFirst(page: Page, n: number, target = lesson): Promise<void> {
  await page.goto(`/#/lesson/${target.id}`);
  await page.locator('.prompt').waitFor();
  for (const q of target.questions.slice(0, n)) await solveAndContinue(page, q);
  await page.locator('.prompt').waitFor();
}

test.beforeEach(async ({ page }) => {
  await useClock(page);
  await seed(page, { [KEY_V2]: v2({ xp: 40 }) });
});

test.describe('leaving a lesson half way', () => {
  test('the X leaves at once, with no question asked, and Home offers to carry on', async ({ page }) => {
    await playFirst(page, 2);
    await leave(page);
    await expect(page.getByRole('alertdialog')).toHaveCount(0);
    await expect(page.locator('.home')).toBeVisible();

    const card = page.getByTestId('resume-card');
    await expect(card).toBeVisible();
    await expect(card).toContainText(lesson.title);
    await expect(card).toContainText(`2 of ${total} done`);
    await expect(page.getByTestId('daily-card')).toHaveCount(0); // the main card would open this same lesson: the Resume card replaces it
    await expectNoOverflow(page);
    for (const id of ['resume-link', 'resume-discard']) expect((await page.getByTestId(id).boundingBox())!.height, id).toBeGreaterThanOrEqual(44);
    await page.screenshot({ path: `${SHOTS}/resume-card-home.png` });

    // nothing was counted yet: the XP and the lesson are only recorded when the lesson is finished
    const progress = (await stored(page))!;
    expect(progress.xp).toBe(40);
    expect(progress.lessons[lesson.id]).toBeUndefined();
  });

  test('the browser Back button does the same', async ({ page }) => {
    await page.goto('/');
    await page.locator('.home').waitFor();
    await playFirst(page, 2);
    await page.goBack();
    await expect(page.locator('.home')).toBeVisible();
    await expect(page.getByTestId('resume-card')).toContainText(`2 of ${total} done`);
  });

  test('Resume carries on from the next question with the XP so far, and finishing records the lesson once', async ({ page }) => {
    await playFirst(page, 2);
    await leave(page);
    await page.getByTestId('resume-link').click();

    await expect(page.locator('.prompt')).toContainText(lesson.questions[2]!.prompt.replace(/`/g, '').slice(0, 30));
    await expect(page.locator('.xp-chip')).toHaveAttribute('aria-label', '20 XP earned');
    await expect(page.locator('[role="progressbar"]').first()).toHaveAttribute('aria-valuenow', String(Math.round((2 / total) * 100)));
    await page.screenshot({ path: `${SHOTS}/resume-continued.png` });

    for (const q of lesson.questions.slice(2)) await solveAndContinue(page, q);
    await expect(page).toHaveURL(/#\/done$/);
    await expect(page.locator('.summary')).toBeVisible();

    const progress = (await stored(page))!;
    expect(progress.lessons[lesson.id].completions).toBe(1);
    expect(progress.xp).toBeGreaterThanOrEqual(40 + total * 10); // every question was right the first time, as before leaving
    expect(await savedRecord(page)).toBeNull(); // finished: nothing left to resume
    await page.getByRole('button', { name: 'Back home', exact: true }).click();
    await expect(page.getByTestId('resume-card')).toHaveCount(0);
  });

  test('opening the same lesson again (from its link, after a reload) carries on; another lesson starts fresh and replaces the saved one', async ({ page }) => {
    await playFirst(page, 2);
    await leave(page);
    await page.reload();
    await page.goto(`/#/lesson/${lesson.id}`);
    await page.locator('.prompt').waitFor();
    await expect(page.locator('.xp-chip')).toHaveAttribute('aria-label', '20 XP earned');

    await leave(page);
    await page.goto(`/#/lesson/${other.id}`);
    await page.locator('.prompt').waitFor();
    await expect(page.locator('.xp-chip')).toHaveAttribute('aria-label', '0 XP earned'); // fresh
    await solveAndContinue(page, other.questions[0]!);
    await leave(page);
    const card = page.getByTestId('resume-card');
    await expect(card).toContainText(other.title);
    await expect(card).not.toContainText(lesson.title); // one session at a time
    await expect(page.getByTestId('daily-card')).toBeVisible(); // it would open a different session: it stays
    expect((await savedRecord(page))!.route).toBe(`/lesson/${other.id}`);
  });

  test('leaving after Check but before Continue: that question is asked again, the ones before it are kept', async ({ page }) => {
    await playFirst(page, 1);
    await answerCorrectly(page, lesson.questions[1]!);
    await check(page);
    await expect(page.locator('.feedback')).toHaveClass(/good/);
    await leave(page);

    await expect(page.getByTestId('resume-card')).toContainText(`1 of ${total} done`);
    await page.getByTestId('resume-link').click();
    await expect(page.locator('.lesson-bar .btn.primary')).toHaveText('Check'); // not yet answered
    await expect(page.locator('.xp-chip')).toHaveAttribute('aria-label', '10 XP earned');
    await solveAndContinue(page, lesson.questions[1]!);
    await expect(page.locator('.xp-chip')).toHaveAttribute('aria-label', '20 XP earned');
  });

  test('a missed question is kept as missed: it comes back at the end after resuming', async ({ page }) => {
    await page.goto(`/#/lesson/${lesson.id}`);
    await page.locator('.prompt').waitFor();
    const { answerWrongly } = await import('./solve');
    await answerWrongly(page, lesson.questions[0]!);
    await check(page);
    await expect(page.locator('.feedback')).toHaveClass(/bad/);
    await next(page);
    await solveAndContinue(page, lesson.questions[1]!);
    await leave(page);
    await page.getByTestId('resume-link').click();
    await page.locator('.prompt').waitFor();

    const queue = (await savedRecord(page))!.queue as { id: string }[];
    expect(queue.map((q) => q.id)).toContain(lesson.questions[0]!.id); // waiting at the end
    expect(queue.at(-1)!.id).toBe(lesson.questions[0]!.id);
    expect((await savedRecord(page))!.state.missed).toEqual([lesson.questions[0]!.id]);
  });

  test('nothing answered, nothing saved: leaving at once leaves no card', async ({ page }) => {
    await page.goto(`/#/lesson/${lesson.id}`);
    await page.locator('.prompt').waitFor();
    await leave(page);
    await expect(page.locator('.home')).toBeVisible();
    await expect(page.getByTestId('resume-card')).toHaveCount(0);
    expect(await savedRecord(page)).toBeNull();
  });
});

test.describe('practice sessions', () => {
  test('a mixed practice resumes with the same questions, even if what the app would pick has changed since', async ({ page }) => {
    await page.goto('/#/mix');
    await page.locator('.prompt').waitFor();
    const seen: string[] = [];
    for (let i = 0; i < 2; i++) {
      const q = (await currentQuestion(page))!;
      seen.push(q.id);
      await solveAndContinue(page, q);
    }
    const third = (await currentQuestion(page))!;
    await leave(page);
    await expect(page.getByTestId('resume-card')).toContainText('Mixed practice');
    expect(await savedRecord(page)).toMatchObject({ route: '/mix', kind: 'mix' });

    // what "mixed practice" would pick now is different: the seed moves with the number of sessions
    await page.evaluate(([k]) => {
      const p = JSON.parse(localStorage.getItem(k!)!);
      p.stats.sessions += 7;
      localStorage.setItem(k!, JSON.stringify(p));
    }, [KEY_V2]);
    await page.reload();
    await page.getByTestId('resume-link').click();
    expect((await currentQuestion(page))!.id).toBe(third.id);
    expect(seen).not.toContain(third.id);
  });

  test('a one-question session (Library practice) is never saved', async ({ page }) => {
    const q = course.units[0]!.lessons[0]!.questions[0]!;
    await page.goto(`/#/q/${q.id}`);
    await page.locator('.prompt').waitFor();
    await leave(page);
    expect(await savedRecord(page)).toBeNull();
    await expect(page.getByTestId('resume-card')).toHaveCount(0);
  });
});

test.describe('discarding, and what is dropped', () => {
  test('Discard asks first; Keep it keeps the session, Discard drops it and the lesson starts again from the first question', async ({ page }) => {
    await playFirst(page, 2);
    await leave(page);

    await page.getByTestId('resume-discard').click();
    const dialog = page.getByRole('alertdialog', { name: 'Discard this session?' });
    await expect(dialog).toContainText('Nothing from');
    await expect(dialog).toContainText('2 questions');
    await expect(page.getByTestId('resume-discard-confirm-cancel')).toBeFocused();
    await page.getByRole('button', { name: 'Keep it' }).click();
    await expect(page.getByTestId('resume-card')).toBeVisible();
    expect(await savedRecord(page)).not.toBeNull();

    await page.getByTestId('resume-discard').click();
    await page.getByTestId('resume-discard-confirm-ok').click();
    await expect(page.getByTestId('resume-card')).toHaveCount(0);
    expect(await savedRecord(page)).toBeNull();

    await page.goto(`/#/lesson/${lesson.id}`);
    await page.locator('.prompt').waitFor();
    await expect(page.locator('.xp-chip')).toHaveAttribute('aria-label', '0 XP earned');
  });

  test('resetting progress forgets an unfinished session with it', async ({ page }) => {
    await playFirst(page, 2);
    await leave(page);
    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    await page.getByRole('dialog', { name: 'Settings', exact: true }).getByRole('button', { name: 'Reset progress' }).click();
    await page.getByTestId('reset-confirm-ok').click();
    expect(await savedRecord(page)).toBeNull();
    await page.keyboard.press('Escape');
    await expect(page.getByTestId('resume-card')).toHaveCount(0);
  });

  test('another tab finishing or discarding the session updates this one', async ({ page, context }) => {
    await playFirst(page, 2);
    await leave(page);
    await expect(page.getByTestId('resume-card')).toBeVisible();
    const tab2 = await context.newPage();
    await tab2.goto('/');
    await tab2.evaluate((k) => localStorage.removeItem(k), RESUME_KEY);
    await expect(page.getByTestId('resume-card')).toHaveCount(0);
  });

  test('a saved session that no longer matches the course is ignored: no card, the lesson starts from the start', async ({ page }) => {
    await playFirst(page, 2);
    await leave(page);
    await page.evaluate((k) => {
      const r = JSON.parse(localStorage.getItem(k)!);
      r.queue[0].h = 'not-what-it-asked-before';
      localStorage.setItem(k, JSON.stringify(r));
    }, RESUME_KEY);
    await page.reload();
    await expect(page.locator('.home')).toBeVisible();
    await expect(page.getByTestId('resume-card')).toHaveCount(0);
    await page.goto(`/#/lesson/${lesson.id}`);
    await page.locator('.prompt').waitFor();
    await expect(page.locator('.xp-chip')).toHaveAttribute('aria-label', '0 XP earned');
  });

  test('damaged text in the saved-session key is ignored without an error', async ({ page }) => {
    const errors: string[] = [];
    page.on('pageerror', (e) => errors.push(e.message));
    await page.addInitScript((k) => localStorage.setItem(k, '{"v":1,"route":'), RESUME_KEY);
    await page.goto('/');
    await expect(page.locator('.home')).toBeVisible();
    await expect(page.getByTestId('resume-card')).toHaveCount(0);
    expect(errors).toEqual([]);
  });
});
