import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, test, type Page } from '@playwright/test';
import { fnv1a, readiness, type Progress } from '../shared/src';
import {
  KEY_V1, KEY_V2, SHOTS, TODAY, course, cardFor, currentQuestion, day, dueCards, dueCount, expectNoOverflow, homeAfterSummary, lessons, playLesson,
  playSession, questions, seed, setDay, stored, unitOf, useClock, v2, watchErrors,
} from './engage';
import { answerCorrectly, answerWrongly, check, expectNoHorizontalOverflow, next, solveAndContinue } from './solve';

const here = dirname(fileURLToPath(import.meta.url));
const LESSON = 'kotlin-null-types'; // 7 questions
const LESSON_Q = lessons.find((l) => l.id === LESSON)!.questions;

const goalText = (page: Page) => page.getByTestId('goal-text');
const chip = (page: Page) => page.getByTestId('streak-chip');

test.describe('upgrading from v1', () => {
  test('a real v1 profile keeps its xp, streak and lessons; v1 stays untouched; v2 is written', async ({ page }) => {
    const v1Text = readFileSync(resolve(here, '../shared/src/__fixtures__/progress-v1.json'), 'utf8');
    const v1 = JSON.parse(v1Text);
    const errors = watchErrors(page);
    await useClock(page);
    await seed(page, { [KEY_V1]: { ...v1, lastDay: day(-1) } });
    await page.goto('/');

    await expect(chip(page)).toContainText('4');
    await expect(chip(page).locator('.freeze')).toContainText('1'); // one-time welcome freeze
    await expect(page.locator('.stat[aria-label="320 XP"]')).toBeVisible();
    await expect(page.getByTestId('rank')).toContainText('Engineer');
    await expect(page.locator('.progress-line')).toContainText(`3 / ${lessons.length} lessons`);
    // the v1 questions that were missed are due straight away; the rest are spread over the week
    await expect(dueCount(page)).toContainText('ready to review');
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/home-upgraded-from-v1.png` });

    const v2Blob = await stored(page);
    expect(v2Blob).toMatchObject({ version: 2, xp: 320, streak: 4, freezes: 1, longestStreak: 4 });
    expect(Object.keys(v2Blob!.cards).length).toBeGreaterThan(20);
    const v1After = await stored(page, KEY_V1);
    expect(v1After).toEqual({ ...v1, lastDay: day(-1) });
    expect(errors).toEqual([]);

    // the welcome gift is announced once, in the next summary, without a retroactive rank-up
    await playLesson(page, LESSON);
    await expect(page.getByTestId('summary-notes')).toContainText('welcome gift');
    await expect(page.getByTestId('celebration')).not.toContainText('New rank');
  });

  test('unreadable or newer storage never crashes, is not overwritten on load, and is backed up before the first save', async ({ page }) => {
    const errors = watchErrors(page);
    const future = JSON.stringify({ version: 3, xp: 999, note: 'written by a newer app' });
    await useClock(page);
    await seed(page, { [KEY_V2]: future });
    await page.goto('/');
    await expect(page.locator('.stat[aria-label="0 XP"]')).toBeVisible();
    expect(await page.evaluate((k) => localStorage.getItem(k), KEY_V2)).toBe(future);

    await playLesson(page, LESSON);
    await expect(page.locator('.summary h1')).toBeVisible();
    const backups = await page.evaluate(() => Object.keys(localStorage).filter((k) => k.includes('backup-')));
    expect(backups).toEqual(['interview-trainer.progress.backup-20261104']);
    expect(await page.evaluate((k) => localStorage.getItem(k), backups[0]!)).toBe(future);
    expect(await stored(page)).toMatchObject({ version: 2 });
    expect(errors).toEqual([]);
  });
});

test.describe('daily goal ring', () => {
  test('a fresh profile can deep-link into a lesson; the ring fills; the review queue appears the next day', async ({ page }) => {
    await useClock(page);
    await page.goto(`/#/lesson/${LESSON}`);
    await page.locator('.prompt').waitFor();

    // the first answer already shows when this question will come back
    await solveAndContinueWithHint(page);
    for (const q of LESSON_Q.slice(1)) await solveAndContinue(page, q);

    await expect(page.locator('.summary h1')).toHaveText('Clean run');
    await expect(page.getByTestId('summary-goal')).toContainText('7 of 8 questions today');
    await expect(page.getByTestId('next-review')).toContainText('tomorrow (7)');
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/summary-perfect-lesson.png` });

    await homeAfterSummary(page);
    await expect(goalText(page)).toHaveText('7/8');
    await expect(page.getByTestId('goal-ring')).toHaveAttribute('aria-label', /7 of 8 questions today/);
    await expect(dueCount(page)).toHaveText('All caught up'); // everything is scheduled for tomorrow

    // the next day: a fresh ring and the questions come due
    await setDay(page, day(1));
    await page.reload();
    await expect(goalText(page)).toHaveText('0/8');
    await expect(dueCount(page)).toHaveText('7 ready to review');
    await expect(page.getByTestId('streak-chip')).toContainText('1');

    // yesterday's bar is in the chart
    await page.getByTestId('goal-ring').click();
    const sheet = page.getByRole('dialog', { name: 'Daily goal' });
    await expect(sheet).toBeVisible();
    await expect(sheet.locator(`rect[data-day="${TODAY}"]`)).toHaveAttribute('data-answered', '7');
    await expect(sheet).toContainText('0 of 8 questions today');
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/goal-sheet.png` });
    await page.keyboard.press('Escape');
    await expect(sheet).toBeHidden();
  });

  test('changing the goal in the sheet applies today and persists', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    await page.getByTestId('goal-ring').click();
    await page.getByRole('radio', { name: /^16/ }).check();
    await page.getByRole('button', { name: 'Close' }).click();
    await expect(goalText(page)).toHaveText('0/16');
    await page.reload();
    await expect(goalText(page)).toHaveText('0/16');
  });

  test('a screen left open across midnight rolls over to the new day without a reload', async ({ page }) => {
    await useClock(page, new Date(2026, 10, 4, 23, 58, 0));
    await seed(page, { [KEY_V2]: v2({ streak: 3, longestStreak: 3, lastDay: TODAY, days: { [TODAY]: { xp: 50, answered: 5, firstTry: 5, target: 8 } } }) });
    await page.goto('/');
    await expect(goalText(page)).toHaveText('5/8');
    await page.clock.fastForward('03:00');
    await expect(goalText(page)).toHaveText('0/8');
    await expect(chip(page)).toContainText('3'); // practised yesterday: the streak is still open
  });

  test('after a week away the goal is halved for one easy-restart day', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ streak: 5, longestStreak: 5, lastDay: day(-9) }) });
    await page.goto('/');
    await expect(goalText(page)).toHaveText('0/4');
    await page.getByTestId('goal-ring').click();
    await expect(page.getByRole('dialog')).toContainText('Easy restart');
  });
});

async function solveAndContinueWithHint(page: Page) {
  const q = LESSON_Q[0]!;
  await answerCorrectly(page, q);
  await check(page);
  await expect(page.getByTestId('srs-note')).toHaveText("You'll see this again tomorrow.");
  await next(page);
}

test.describe('review queue', () => {
  test('a missed question stays in the queue until answered correctly; next day everything learned is due and clears', async ({ page }) => {
    await useClock(page);
    await page.goto(`/#/lesson/${LESSON}`);
    await page.locator('.prompt').waitFor();

    // Miss the first single-choice question until its retries run out, so it stays in the missed pile.
    const failing = LESSON_Q.findIndex((q) => q.type === 'single');
    for (const q of LESSON_Q.slice(0, failing)) await solveAndContinue(page, q);
    const target = LESSON_Q[failing]!;
    await answerWrongly(page, target);
    await check(page);
    await next(page);
    for (const q of LESSON_Q.slice(failing + 1)) await solveAndContinue(page, q);
    for (let attempt = 0; attempt < 2; attempt++) {
      await expect(page.locator('.kicker')).toContainText('Try again');
      await answerWrongly(page, target);
      await check(page);
      await next(page);
    }
    await expect(page.locator('.summary h1')).toHaveText('Lesson complete');
    await expect(page.locator('.summary')).toContainText('1 question to revisit');
    await homeAfterSummary(page);

    // same day: only the missed question is waiting
    await expect(dueCount(page)).toHaveText('1 ready to review');
    await page.screenshot({ path: `${SHOTS}/home-review-due.png` });
    await page.getByTestId('due-card').click();
    await expect(page.locator('.prompt')).toBeVisible();
    await solveAndContinue(page, target);
    await expect(page.locator('.summary h1')).toBeVisible();
    await homeAfterSummary(page);
    await expect(dueCount(page)).toHaveText('All caught up');

    // next day: every question from the lesson comes due; answering them all correctly clears the queue
    await setDay(page, day(1));
    await page.reload();
    await expect(dueCount(page)).toHaveText(`${LESSON_Q.length} ready to review`);
    await page.getByTestId('due-card').click();
    await playSession(page, LESSON_Q.length);
    await expect(page.locator('.summary h1')).toBeVisible();
    await expect(page.getByTestId('next-review')).toContainText('in 3 days');
    await homeAfterSummary(page);
    await expect(dueCount(page)).toHaveText('All caught up');

    // and it survives a reload
    await page.reload();
    await expect(dueCount(page)).toHaveText('All caught up');
    const cards = (await stored(page))!.cards as Record<string, { interval: number }>;
    expect(Object.values(cards).every((c) => c.interval >= 1)).toBe(true);
  });

  test('empty review shows a calm "all clear" page', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/#/review');
    await expect(page.locator('h1')).toHaveText('Nothing to review');
  });
});

test.describe('forgiving streak', () => {
  test('a freeze covers a missed day: shown on Home, spent only when you practise, and one is earned back at 7', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ streak: 6, longestStreak: 6, lastDay: day(-2), freezes: 1, milestones: { 'rank:0': day(-9) } }) });
    await page.goto('/');
    await expect(chip(page)).toContainText('6');
    await expect(chip(page).locator('.freeze')).toContainText('1');
    await expect(page.getByTestId('streak-note')).toHaveText('covered yesterday');
    // reloading does not spend it
    await page.reload();
    expect((await stored(page)) ?? {}).toMatchObject({ freezes: 1, streak: 6 });

    await chip(page).click();
    const sheet = page.getByRole('dialog', { name: 'Your streak' });
    await expect(sheet).toContainText('A freeze will cover the day you missed');
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/streak-sheet.png` });
    await page.keyboard.press('Escape');

    await playLesson(page, LESSON);
    await expect(page.getByTestId('summary-notes')).toContainText('A freeze covered the day you missed');
    await expect(page.getByTestId('summary-notes')).toContainText('You earned a streak freeze');
    await expect(page.getByTestId('celebration')).toContainText('7-day streak');
    await homeAfterSummary(page);
    await expect(chip(page)).toContainText('7');
    await expect(chip(page).locator('.freeze')).toContainText('1');
    await expect(page.getByTestId('streak-note')).toHaveCount(0);
  });

  test('a gap too big to cover starts fresh and keeps the freeze; longest streak and practice days are never lost', async ({ page }) => {
    await useClock(page);
    await seed(page, {
      [KEY_V2]: v2({ streak: 12, longestStreak: 23, lastDay: day(-3), freezes: 1, stats: { sessions: 30, answered: 300, firstTry: 200, perfect: 4, practiceDays: 41 } }),
    });
    await page.goto('/');
    await expect(chip(page)).toContainText('0');
    await expect(page.getByTestId('streak-note')).toContainText('Fresh start. Longest: 23 · 41 days practised');
    await expect(page.getByText(/in danger|lose|expire/i)).toHaveCount(0);

    await playLesson(page, LESSON);
    await homeAfterSummary(page);
    await expect(chip(page)).toContainText('1');
    await expect(chip(page).locator('.freeze')).toContainText('1');
    const saved = await stored(page);
    expect(saved).toMatchObject({ streak: 1, longestStreak: 23, freezes: 1 });
    expect(saved!.stats.practiceDays).toBe(42);
  });

  test('chosen rest days cover a gap for free', async ({ page }) => {
    // Today is Wed 2026-11-04; last practised Sun 11-01; Monday and Tuesday are the chosen rest days, so nothing was missed
    await useClock(page);
    await seed(page, {
      [KEY_V2]: v2({ streak: 9, longestStreak: 9, lastDay: '2026-11-01', freezes: 0, profile: { ...v2().profile, restDays: [1, 2] } }),
    });
    await page.goto('/');
    await expect(chip(page)).toContainText('9');
    await expect(page.getByTestId('streak-note')).toHaveCount(0);
  });

  test('a clock that moved backwards holds the streak and heals on the next practice', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ streak: 3, longestStreak: 3, lastDay: day(5) }) });
    await page.goto('/');
    await expect(chip(page)).toContainText('3');
    await playLesson(page, LESSON);
    await homeAfterSummary(page);
    expect(await stored(page)).toMatchObject({ streak: 3, lastDay: TODAY });
  });

  test('rest days can be chosen in the sheet and are limited to three', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    await chip(page).click();
    const group = page.getByRole('dialog').getByRole('group', { name: 'Rest days' });
    for (const d of ['Saturday', 'Sunday', 'Friday']) await group.getByRole('button', { name: d }).click();
    await expect(group.getByRole('button', { name: 'Monday' })).toBeDisabled();
    await group.getByRole('button', { name: 'Friday' }).click();
    await expect(group.getByRole('button', { name: 'Monday' })).toBeEnabled();
    expect((await stored(page))!.profile.restDays).toEqual([0, 6]);
  });
});

test.describe('Daily Practice', () => {
  test('Today mixes due cards, weak spots and new questions from several topics, and finishing drains the queue and moves the ring', async ({ page }) => {
    test.setTimeout(120_000);
    const { cards, ids } = dueCards(12);
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ cards, streak: 2, longestStreak: 2, lastDay: day(-1) }) });
    await page.goto('/');

    await expect(page.getByTestId('daily-card')).toContainText('8 questions');
    await expect(page.getByTestId('daily-card')).toContainText('5 due');
    await expect(dueCount(page)).toHaveText('12 ready to review');
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/home-daily-card.png` });

    await page.getByTestId('daily-card').click();
    const played = await playSession(page, 8);
    expect(new Set(played.map((q) => q.id)).size).toBe(8);
    expect(new Set(played.map(unitOf)).size).toBeGreaterThanOrEqual(3);
    // no two neighbours share a topic
    played.forEach((q, i) => i > 0 && expect(unitOf(q)).not.toBe(unitOf(played[i - 1]!)));

    await expect(page.getByTestId('summary-goal')).toContainText('8 of 8 questions today');
    await expect(page.getByTestId('summary-goal')).toContainText('Daily goal met');
    await expect(page.getByTestId('celebration')).toBeVisible();
    await homeAfterSummary(page);
    await expect(goalText(page)).toHaveText('8/8');
    const stillDue = ids.filter((id) => !played.some((q) => q.id === id)).length;
    await expect(dueCount(page)).toHaveText(stillDue ? `${stillDue} ready to review` : 'All caught up');
  });

  test('a new learner is invited to start the first lesson, with no fake due items', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    await expect(page.getByTestId('daily-card')).toContainText('Start your first lesson');
    await expect(page.getByTestId('daily-card')).not.toContainText('due');
    await expect(dueCount(page)).toHaveText('All caught up');
  });
});

test.describe('onboarding card', () => {
  test('is inline (not a modal), saves the plan, shows the countdown and never comes back', async ({ page }) => {
    await useClock(page);
    await page.goto('/');
    const card = page.getByTestId('onboarding');
    await expect(card).toBeVisible();
    await expect(page.getByRole('dialog')).toHaveCount(0);
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/home-onboarding.png` });

    // a past date is refused with a friendly message
    await card.getByLabel('Interview date (optional)').fill(day(-3));
    await card.getByRole('button', { name: 'Save', exact: true }).click();
    await expect(card.getByRole('alert')).toContainText('today or a later date');
    await expect(card).toBeVisible();

    await card.getByLabel(/Staff \/ architect/).check();
    await card.getByLabel('Interview date (optional)').fill(day(10));
    await card.getByLabel(/^16/).check();
    await card.getByLabel(/When will you usually practise/).fill('07:30');
    await card.getByRole('button', { name: 'Save', exact: true }).click();

    await expect(card).toHaveCount(0);
    await expect(page.getByTestId('countdown')).toContainText('10 days to interview');
    await expect(goalText(page)).toHaveText('0/16');
    expect((await stored(page))!.profile).toEqual({ onboarded: true, role: 'staff-architect', interviewDate: day(10), restDays: [], practiceTime: '07:30' });

    await page.reload();
    await expect(page.getByTestId('onboarding')).toHaveCount(0);
    await expect(page.getByTestId('countdown')).toContainText('10 days to interview');

    // it can be reopened from Settings
    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    await page.getByRole('button', { name: 'Edit my plan' }).click();
    await expect(page.getByTestId('onboarding')).toBeVisible();
  });

  test('skipping is free and permanent; a direct lesson link still works with no setup', async ({ page }) => {
    await useClock(page);
    await page.goto('/');
    await page.getByRole('button', { name: 'Skip for now' }).click();
    await expect(page.getByTestId('onboarding')).toHaveCount(0);
    await expect(page.getByTestId('countdown')).toHaveCount(0); // no date, no countdown, no nagging
    await page.reload();
    await expect(page.getByTestId('onboarding')).toHaveCount(0);
    expect(await stored(page)).toMatchObject({ goal: { target: 8 }, profile: { onboarded: true, role: null, interviewDate: null } });
  });

  test('the warm-up counts towards today: 3 questions, ring at 3/8', async ({ page }) => {
    test.setTimeout(60_000);
    await useClock(page);
    await page.goto('/');
    await page.getByRole('button', { name: /Save and warm up/ }).click();
    await expect(page).toHaveURL(/#\/warmup/);
    await playSession(page, 3);
    await expect(page.getByTestId('summary-goal')).toContainText('3 of 8 questions today');
    await homeAfterSummary(page);
    await expect(goalText(page)).toHaveText('3/8');
  });

  test('interview date: a calm plan, and "final review" when it is tomorrow', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ profile: { ...v2().profile, interviewDate: day(90) } }) });
    await page.goto('/');
    await expect(page.getByTestId('countdown')).toContainText('90 days to interview');
    await expect(page.getByTestId('countdown')).toContainText("At 8/day you'll have seen everything");

    await page.evaluate(([k, d]) => {
      const p = JSON.parse(localStorage.getItem(k!)!);
      p.profile.interviewDate = d;
      localStorage.setItem(k!, JSON.stringify(p));
    }, [KEY_V2, day(1)]);
    await page.reload();
    await expect(page.getByTestId('countdown')).toContainText('1 day to interview');
    await expect(page.getByTestId('countdown')).toContainText('Final review');
  });
});

test.describe('export and import', () => {
  test('download a backup, reset, import it back; garbage is rejected and changes nothing', async ({ page }, info) => {
    await useClock(page);
    await page.goto('/');
    await page.getByRole('button', { name: 'Skip for now' }).click();
    await playLesson(page, LESSON);
    await homeAfterSummary(page);
    const before = await stored(page);
    expect(before!.xp).toBe(80);

    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    const sheet = page.getByRole('dialog', { name: 'Settings', exact: true });
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/settings-sheet.png` });
    const [download] = await Promise.all([page.waitForEvent('download'), sheet.getByRole('button', { name: 'Download backup' }).click()]);
    expect(download.suggestedFilename()).toBe('interview-trainer-progress-2026-11-04.json');
    const file = info.outputPath('backup.json');
    await download.saveAs(file);
    const text = readFileSync(file, 'utf8');
    expect(JSON.parse(text)).toMatchObject({ app: 'interview-trainer', format: 2, progress: { xp: 80 } });

    // reset wipes it
    await sheet.getByRole('button', { name: 'Reset progress' }).click();
    await page.getByTestId('reset-confirm-ok').click();
    await expect(page.locator('.stat[aria-label="0 XP"]')).toBeVisible();
    expect(await stored(page)).toMatchObject({ version: 2, xp: 0 });

    // a broken file is rejected
    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    const input = page.getByTestId('import-file');
    await input.setInputFiles({ name: 'junk.json', mimeType: 'application/json', buffer: Buffer.from('this is not a backup') });
    await expect(page.getByTestId('import-error')).toContainText('not a valid backup');
    await input.setInputFiles({ name: 'other.json', mimeType: 'application/json', buffer: Buffer.from(JSON.stringify({ hello: 'world' })) });
    await expect(page.getByTestId('import-error')).toContainText('does not look like Interview Trainer progress');
    // a tampered backup is caught by its checksum
    writeFileSync(file, text.replace('"xp": 80', '"xp": 99999'));
    await input.setInputFiles(file);
    await expect(page.getByTestId('import-error')).toContainText('checksum');
    expect(await stored(page)).toMatchObject({ xp: 0 });

    // the real one restores everything
    writeFileSync(file, text);
    await input.setInputFiles(file);
    const confirm = page.getByTestId('import-confirm');
    await expect(confirm).toContainText('80 XP');
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/settings-import-confirm.png` });
    await confirm.getByRole('button', { name: 'Replace my progress' }).click();
    await page.getByTestId('replace-confirm-ok').click();
    await expect(page.getByTestId('import-message')).toContainText('replaced');
    await page.getByRole('button', { name: 'Close' }).click();
    await expect(page.locator('.stat[aria-label="80 XP"]')).toBeVisible();
    await expect(goalText(page)).toHaveText('7/8');
    const after = await stored(page);
    expect(after!.cards).toEqual(before!.cards);
    expect(after).toEqual(before);

    // restored state survives a reload
    await page.reload();
    await expect(page.locator('.stat[aria-label="80 XP"]')).toBeVisible();
  });

  test('importing merges two devices: the larger progress and the newer cards win', async ({ page }, info) => {
    await useClock(page);
    const other: Progress = v2({ xp: 500, streak: 2, longestStreak: 9, lastDay: day(-1), stats: { sessions: 9, answered: 9, firstTry: 9, perfect: 0, practiceDays: 12 } });
    const file = info.outputPath('other-device.json');
    writeFileSync(file, JSON.stringify(other));
    await seed(page, { [KEY_V2]: v2({ xp: 100, streak: 4, longestStreak: 4, lastDay: TODAY }) });
    await page.goto('/');
    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    await page.getByTestId('import-file').setInputFiles(file);
    await page.getByRole('button', { name: /Merge/ }).click();
    await page.getByRole('button', { name: 'Close' }).click();
    await expect(page.locator('.stat[aria-label="500 XP"]')).toBeVisible();
    await expect(chip(page)).toContainText('4'); // streak from the side that practised most recently
    expect(await stored(page)).toMatchObject({ xp: 500, longestStreak: 9, streak: 4 });
  });

  test('pasting backup text works where files cannot be chosen', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ xp: 33 }) });
    await page.goto('/');
    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    await page.getByText('Paste backup text instead').click();
    await page.getByLabel('Backup text').fill(JSON.stringify(v2({ xp: 777 })));
    await page.getByRole('button', { name: 'Check pasted text' }).click();
    await page.getByRole('button', { name: 'Replace my progress' }).click();
    await page.getByTestId('replace-confirm-ok').click();
    await page.getByRole('button', { name: 'Close' }).click();
    await expect(page.locator('.stat[aria-label="777 XP"]')).toBeVisible();
  });
});

test.describe('several tabs', () => {
  test('progress made in one tab appears in the other, and a stale tab does not overwrite it', async ({ context }) => {
    const a = await context.newPage();
    const b = await context.newPage();
    await useClock(a);
    await useClock(b);
    await seed(context, { [KEY_V2]: v2() });
    await a.goto('/');
    await b.goto('/');
    await expect(b.locator('.stat[aria-label="0 XP"]')).toBeVisible();

    await playLesson(a, LESSON);
    await expect(a.locator('.summary h1')).toBeVisible();
    // no reload in tab B: the storage event brings the new state in
    await expect(b.locator('.stat[aria-label="80 XP"]')).toBeVisible();
    await expect(b.getByTestId('goal-text')).toHaveText('7/8');

    // B changes a setting; A's xp must survive the write
    await b.getByTestId('goal-ring').click();
    await b.getByRole('radio', { name: /^24/ }).check();
    await b.getByRole('button', { name: 'Close' }).click();
    await homeAfterSummary(a);
    await expect(a.locator('.stat[aria-label="80 XP"]')).toBeVisible();
    await expect(a.getByTestId('goal-text')).toHaveText('7/24');
    expect(await stored(b)).toMatchObject({ xp: 80, goal: { target: 24 } });
  });
});

test.describe('celebrations', () => {
  test.describe('when motion is allowed', () => {
    test.use({ contextOptions: { reducedMotion: 'no-preference' } });

    test('a perfect lesson shows the milestone card and a confetti burst; the h1 is unchanged', async ({ page }) => {
      await page.goto(`/#/lesson/${LESSON}`);
      await page.locator('.prompt').waitFor();
      for (const q of LESSON_Q) await solveAndContinue(page, q);
      await expect(page.locator('.summary h1')).toHaveText('Clean run');
      await expect(page.getByTestId('celebration')).toBeVisible();
      await expect(page.getByTestId('confetti')).toBeAttached();
      await expect(page.getByTestId('confetti')).toHaveAttribute('aria-hidden', 'true');
      await expect(page.getByTestId('confetti')).toHaveCount(0, { timeout: 5000 }); // it removes itself
    });

    test('confetti can be switched off in settings', async ({ page }) => {
      await seed(page, { [KEY_V2]: v2({ settings: { ...v2().settings, confetti: false } }) });
      await page.goto(`/#/lesson/${LESSON}`);
      await page.locator('.prompt').waitFor();
      for (const q of LESSON_Q) await solveAndContinue(page, q);
      await expect(page.getByTestId('celebration')).toBeVisible();
      await page.waitForTimeout(300);
      await expect(page.getByTestId('confetti')).toHaveCount(0);
    });
  });

  test('with reduced motion there is no canvas at all, but the milestone is still told', async ({ page }) => {
    await page.goto(`/#/lesson/${LESSON}`);
    await page.locator('.prompt').waitFor();
    for (const q of LESSON_Q) await solveAndContinue(page, q);
    await expect(page.locator('.summary h1')).toHaveText('Clean run');
    await expect(page.getByTestId('celebration')).toBeVisible();
    await page.waitForTimeout(300);
    await expect(page.locator('canvas')).toHaveCount(0);
  });

  test('ranks: crossing an XP floor shows one rank-up card, naming the new title', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ xp: 499, milestones: { 'rank:0': day(-3), 'rank:1': day(-3) } }) });
    await page.goto('/');
    await expect(page.getByTestId('rank')).toContainText('Engineer');
    await playLesson(page, LESSON);
    await expect(page.getByTestId('celebration')).toContainText('New rank: Senior');
    await expect(page.getByTestId('celebration')).toHaveAttribute('data-kind', 'rank-up');
    await page.screenshot({ path: `${SHOTS}/summary-rank-up.png` });
    await homeAfterSummary(page);
    await expect(page.getByTestId('rank')).toContainText('Senior');
  });

  test('"Worth remembering" appears with a small bonus when the deterministic dice say so, and can be switched off', async ({ page }) => {
    await useClock(page);
    // find a session count whose dice show (fnv1a(today|sessions) % 6 === 0) and a learned card to remind of
    let sessions = 0;
    while (fnv1a(`${TODAY}|${sessions}`) % 6 !== 0) sessions++;
    const learned = questions.find((q) => !LESSON_Q.includes(q))!;
    const p = v2({ cards: { [learned.id]: cardFor(learned, { interval: 30, due: day(20) }) }, stats: { sessions, answered: 0, firstTry: 0, perfect: 0, practiceDays: 0 } });
    await seed(page, { [KEY_V2]: p });
    await page.goto('/');
    await playLesson(page, LESSON);
    await expect(page.getByTestId('tip')).toContainText('Worth remembering');
    await expect(page.getByTestId('tip')).toContainText('+5 XP');
    await homeAfterSummary(page);
    await expect(page.locator('.stat[aria-label="85 XP"]')).toBeVisible();
  });
});

test.describe('progress and readiness', () => {
  test('shows an honest readiness estimate that matches the engine, a weakest topic and per-topic practice', async ({ page }) => {
    const { cards } = dueCards(40, { interval: 14, due: day(5), last: day(-9) });
    const p = v2({ cards });
    await useClock(page);
    await seed(page, { [KEY_V2]: p });
    await page.goto('/#/progress');
    const expected = readiness(course, p, TODAY);

    await expect(page.getByTestId('readiness-score')).toHaveText(String(expected.score));
    await expect(page.getByTestId('readiness-label')).toHaveText(expected.label === 'early' ? 'Early estimate' : /Building|Solid|Interview-ready/);
    await expect(page.getByText('not a prediction of any interview result')).toBeVisible();
    await expect(page.locator('.topics li')).toHaveCount(course.units.length);
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/progress.png` });

    const weakest = page.getByTestId('weakest-topic');
    await expect(weakest).toBeVisible();
    await weakest.click();
    await expect(page).toHaveURL(/#\/topic\//);
    const q = await currentQuestion(page);
    expect(q && unitOf(q)).toBe(expected.weakestUnitId);
  });

  test('an empty profile is calm: zero, early estimate, nothing red', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/#/progress');
    await expect(page.getByTestId('readiness-score')).toHaveText('0');
    await expect(page.getByTestId('readiness-label')).toHaveText('Early estimate');
    await expect(page.getByTestId('weakest-topic')).toHaveCount(0);
    await page.getByRole('link', { name: 'Back to home' }).click();
    await expect(page.getByTestId('readiness-line')).toHaveText('Readiness 0 · Early estimate');
  });
});

test.describe('phone layout', () => {
  test('Home, sheets and the progress page never scroll sideways at 390px and keep 44px tap targets', async ({ page }) => {
    const { cards } = dueCards(30, { due: day(-2) });
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ cards, streak: 8, longestStreak: 8, freezes: 2, lastDay: day(-1), xp: 1234, profile: { ...v2().profile, interviewDate: day(25), role: 'generalist' } }) });
    await page.goto('/');
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/home-active.png` });

    for (const control of [page.getByTestId('streak-chip'), page.getByTestId('goal-ring'), page.getByRole('button', { name: 'Settings', exact: true })]) {
      const box = (await control.boundingBox())!;
      expect(box.height, 'tap target height').toBeGreaterThanOrEqual(44);
      expect(box.width, 'tap target width').toBeGreaterThanOrEqual(44);
    }

    for (const [open, name] of [[page.getByTestId('streak-chip'), 'Your streak'], [page.getByTestId('goal-ring'), 'Daily goal'], [page.getByRole('button', { name: 'Settings', exact: true }), 'Settings']] as const) {
      await open.click();
      const dialog = page.getByRole('dialog', { name });
      await expect(dialog).toBeVisible();
      await expectNoOverflow(page);
      const dialogBox = (await dialog.boundingBox())!;
      expect(dialogBox.x + dialogBox.width).toBeLessThanOrEqual(390);
      await page.getByRole('button', { name: 'Close' }).click();
      await expect(dialog).toBeHidden();
    }
    await page.goto('/#/progress');
    await expectNoOverflow(page);
  });

  test('sheets are keyboard accessible: focus moves in, Tab stays inside, Escape closes and returns focus', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    const opener = page.getByRole('button', { name: 'Settings', exact: true });
    await opener.focus();
    await page.keyboard.press('Enter');
    const dialog = page.getByRole('dialog', { name: 'Settings', exact: true });
    await expect(dialog).toBeVisible();
    for (let i = 0; i < 40; i++) {
      await page.keyboard.press('Tab');
      expect(await page.evaluate(() => !!document.activeElement?.closest('[role="dialog"]'))).toBe(true);
    }
    await page.keyboard.press('Escape');
    await expect(dialog).toBeHidden();
    await expect(opener).toBeFocused();
    await expectNoHorizontalOverflow(page);
  });
});

test.describe('keeping the data safe', () => {
  test('asks the browser for persistent storage once, after the first finished session', async ({ page }) => {
    await page.addInitScript(() => {
      (window as unknown as { __persist: number }).__persist = 0;
      Object.defineProperty(navigator, 'storage', {
        configurable: true,
        value: { persist: async () => { (window as unknown as { __persist: number }).__persist++; return true; } },
      });
    });
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    expect(await page.evaluate(() => (window as unknown as { __persist: number }).__persist)).toBe(0);
    await playLesson(page, LESSON);
    await expect(page.locator('.summary h1')).toBeVisible();
    expect(await page.evaluate(() => (window as unknown as { __persist: number }).__persist)).toBe(1);
    await playLesson(page, 'kotlin-idioms');
    await expect(page.locator('.summary h1')).toBeVisible();
    expect(await page.evaluate(() => (window as unknown as { __persist: number }).__persist)).toBe(1);
  });

  test('on iPhone Safari (not installed) a one-time Add to Home Screen tip is shown and can be dismissed for good', async ({ page }) => {
    await page.addInitScript(() => {
      Object.defineProperty(navigator, 'userAgent', {
        configurable: true,
        get: () => 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
      });
    });
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    const tip = page.getByRole('note');
    await expect(tip).toContainText('Add to Home Screen');
    await expectNoOverflow(page);
    await tip.getByRole('button', { name: 'Got it' }).click();
    await expect(tip).toHaveCount(0);
    await page.reload();
    await expect(page.getByRole('note')).toHaveCount(0);
  });

  test('other browsers never see the iOS tip', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    await expect(page.getByRole('note')).toHaveCount(0);
  });
});
