import { expect, test } from '@playwright/test';
import { HINT_COST } from '../shared/src';
import { SHOTS, expectNoOverflow, lessons, seed, stored, useClock, v2, KEY_V2, cardFor, questions } from './engage';
import { answerCorrectly, check, next, visible } from './solve';

const LESSON = 'kotlin-null-types';
const LESSON_Q = lessons.find((l) => l.id === LESSON)!.questions;

test.describe('hints', () => {
  test('a hint is offered only where authored, is revealed once, and costs XP and review quality', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto(`/#/lesson/${LESSON}`);

    // question 1 has a hint
    const q1 = LESSON_Q[0]!;
    expect(q1.hint).toBeTruthy();
    const btn = page.getByTestId('hint-btn');
    await expect(btn).toBeVisible();
    expect((await btn.boundingBox())!.height).toBeGreaterThanOrEqual(44);
    await expect(page.getByTestId('hint-text')).toHaveCount(0);
    await btn.click();
    await expect(page.getByTestId('hint-text')).toContainText(visible(q1.hint!).slice(0, 20));
    await expect(btn).toHaveCount(0); // one reveal per question
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/hint-revealed.png` });

    await answerCorrectly(page, q1);
    await check(page);
    await next(page);
    await expect(page.locator(".xp-chip")).toHaveText(`⚡ ${10 - HINT_COST}`);

    // the next question has its own hint state: hidden until asked for, and the button only exists where a hint does
    await expect(page.getByTestId('hint-text')).toHaveCount(0);
    await expect(page.getByTestId('hint-btn')).toHaveCount(LESSON_Q[1]!.hint ? 1 : 0);
    const withoutHint = LESSON_Q.findIndex((q) => !q.hint);
    expect(withoutHint, 'the lesson should contain a question without a hint').toBeGreaterThan(-1);
  });

  test('a hinted answer is scheduled as hard (shorter interval, lower ease), an unhinted one as good', async ({ page }) => {
    await useClock(page);
    const first = LESSON_Q[0]!;
    // an overdue card with a long interval makes the difference between good and hard visible
    await seed(page, { [KEY_V2]: v2({ cards: { [first.id]: cardFor(first, { interval: 10, due: '2026-11-03', reps: 3, last: '2026-10-24' }) } }) });
    await page.goto(`/#/lesson/${LESSON}`);
    await page.getByTestId('hint-btn').click();
    for (let i = 0; i < LESSON_Q.length; i++) {
      await answerCorrectly(page, LESSON_Q[i]!);
      await check(page);
      await next(page);
      const fu = page.locator('.kicker', { hasText: 'Interviewer follow-up' });
      if (await fu.count()) {
        await page.getByRole('textbox', { name: 'Your answer' }).fill((LESSON_Q[i] as { modelAnswer: string }).modelAnswer);
        await check(page);
        await next(page);
      }
    }
    await page.locator('.summary h1').waitFor();
    const p = await stored(page);
    expect(p!.cards[first.id].interval).toBe(12); // hard: round(10 x 1.2); good would be 23
    expect(p!.cards[first.id].ease).toBeCloseTo(2.15);
    expect(questions.length).toBeGreaterThan(0);
  });
});
