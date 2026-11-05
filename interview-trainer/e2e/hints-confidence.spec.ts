import { expect, test, type Page } from '@playwright/test';
import { HINT_COST } from '../shared/src';
import { KEY_V2, SHOTS, cardFor, dueCount, expectNoOverflow, lessons, seed, stored, useClock, v2 } from './engage';
import { answerCorrectly, answerWrongly, check, next, solveAndContinue, visible } from './solve';

const LESSON = 'kotlin-null-types';
const LESSON_Q = lessons.find((l) => l.id === LESSON)!.questions;
const Q1 = LESSON_Q[0]!; // single, has a hint
const Q4 = LESSON_Q[3]!; // single, no hint

/** An overdue card with a long interval: good, hard and easy give clearly different next intervals. */
const overdue = (q: (typeof LESSON_Q)[number]) => cardFor(q, { interval: 10, due: '2026-11-03', reps: 3, last: '2026-10-24' });

async function finishRest(page: Page, from: number) {
  for (let i = from; i < LESSON_Q.length; i++) await solveAndContinue(page, LESSON_Q[i]!);
}

test.describe('hints', () => {
  test('a hint is offered only where authored, is revealed once, and costs XP', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto(`/#/lesson/${LESSON}`);

    expect(Q1.hint).toBeTruthy();
    const btn = page.getByTestId('hint-btn');
    await expect(btn).toBeVisible();
    expect((await btn.boundingBox())!.height).toBeGreaterThanOrEqual(44);
    await expect(page.getByTestId('hint-text')).toHaveCount(0);
    await btn.click();
    await expect(page.getByTestId('hint-text')).toContainText(visible(Q1.hint!).slice(0, 20));
    await expect(btn).toHaveCount(0); // one reveal per question
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/hint-revealed.png` });

    await answerCorrectly(page, Q1);
    await check(page);
    await next(page);
    await expect(page.locator('.xp-chip')).toHaveText(`XP ${10 - HINT_COST}`);

    // the next question has its own hint state: hidden until asked for, and the button only exists where a hint does
    await expect(page.getByTestId('hint-text')).toHaveCount(0);
    await expect(page.getByTestId('hint-btn')).toHaveCount(LESSON_Q[1]!.hint ? 1 : 0);
    expect(LESSON_Q.some((q) => !q.hint), 'the lesson should contain a question without a hint').toBe(true);
  });

  test('a hinted answer is scheduled as hard, an unhinted one as good', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ cards: { [Q1.id]: overdue(Q1), [Q4.id]: overdue(Q4) } }) });
    await page.goto(`/#/lesson/${LESSON}`);
    await page.getByTestId('hint-btn').click();
    for (const q of LESSON_Q) await solveAndContinue(page, q);
    await page.locator('.summary h1').waitFor();
    const p = await stored(page);
    expect(p!.cards[Q1.id].interval).toBe(12); // hard: round(10 x 1.2)
    expect(p!.cards[Q1.id].ease).toBeCloseTo(2.15);
    expect(p!.cards[Q4.id].interval).toBe(23); // good: round(10 x 2.3)
  });
});

test.describe('confidence ratings', () => {
  test('are off by default, and can be switched on in Settings', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto(`/#/lesson/${LESSON}`);
    await expect(page.getByTestId('confidence')).toHaveCount(0);

    await page.goto('/#/');
    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    await page.getByLabel(/How sure are you/).check();
    expect((await stored(page))!.settings.confidence).toBe(true);
    await page.reload();
    await page.goto(`/#/lesson/${LESSON}`);
    await expect(page.getByTestId('confidence')).toBeVisible();
  });

  test('a one-tap rating before Check; skippable; toggles off again', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ settings: { ...v2().settings, confidence: true } }) });
    await page.goto(`/#/lesson/${LESSON}`);
    const row = page.getByTestId('confidence');
    await expect(row).toBeVisible();
    const chips = row.getByRole('radio');
    await expect(chips).toHaveText(['Guess', 'Think so', 'Sure']);
    for (let i = 0; i < 3; i++) expect((await chips.nth(i).boundingBox())!.height).toBeGreaterThanOrEqual(44);
    await expectNoOverflow(page);

    await chips.nth(2).click();
    await expect(chips.nth(2)).toHaveAttribute('aria-checked', 'true');
    await chips.nth(2).click();
    await expect(chips.nth(2)).toHaveAttribute('aria-checked', 'false');
    await chips.nth(1).click();
    await answerCorrectly(page, Q1);
    await page.screenshot({ path: `${SHOTS}/confidence-row.png` });

    // Check works without any rating on the next question
    await check(page);
    await next(page);
    await answerCorrectly(page, LESSON_Q[1]!);
    await check(page);
    await expect(page.locator('.feedback')).toHaveClass(/good/);
  });

  test('wrong while sure: a closer-look note, and the card comes straight back (due today, flagged)', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ settings: { ...v2().settings, confidence: true } }) });
    await page.goto(`/#/lesson/${LESSON}`);

    await page.getByTestId('confidence').getByRole('radio', { name: 'Sure' }).click();
    await answerWrongly(page, Q1);
    await page.getByRole('button', { name: 'Check', exact: true }).click();
    await page.locator('.feedback').waitFor();
    await expect(page.getByTestId('confidence-note')).toContainText('Worth a closer look');
    await page.screenshot({ path: `${SHOTS}/confidence-sure-wrong.png` });
    await next(page);
    await finishRest(page, 1);
    // the missed question returns at the end (no rating row on a retry)
    await expect(page.getByTestId('confidence')).toHaveCount(0);
    await solveAndContinue(page, Q1);
    await page.locator('.summary h1').waitFor();

    const p = await stored(page);
    expect(p!.cards[Q1.id]).toMatchObject({ hc: true, due: '2026-11-04', lapses: 1, interval: 1 });
    expect(p!.cards[Q1.id].ease).toBeCloseTo(1.95);
    expect(p!.calibration).toEqual({ n: [0, 0, 1], right: [0, 0, 0] });
    await page.goto('/#/');
    await expect(dueCount(page)).toContainText('ready to review');
  });

  test('right but a guess: kept short (hard) and noted', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ settings: { ...v2().settings, confidence: true }, cards: { [Q4.id]: overdue(Q4) } }) });
    await page.goto(`/#/lesson/${LESSON}`);
    for (let i = 0; i < 3; i++) await solveAndContinue(page, LESSON_Q[i]!);
    await page.getByTestId('confidence').getByRole('radio', { name: 'Guess' }).click();
    await answerCorrectly(page, Q4);
    await check(page);
    await expect(page.getByTestId('confidence-note')).toContainText('unsure');
    await next(page);
    await finishRest(page, 4);
    await page.locator('.summary h1').waitFor();
    const p = await stored(page);
    expect(p!.cards[Q4.id].interval).toBe(12);
    expect(p!.calibration.n).toEqual([1, 0, 0]);
  });

  test('the Progress screen states how reliable "Sure" has been, only with enough data', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ calibration: { n: [3, 4, 20], right: [1, 3, 17] } }) });
    await page.goto('/#/progress');
    await expect(page.getByTestId('calibration')).toContainText('When you said Sure you were right 85% of the time (20 ratings).');
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/progress-calibration.png` });
  });
});
