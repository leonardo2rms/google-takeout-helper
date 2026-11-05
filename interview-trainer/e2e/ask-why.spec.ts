import { expect, test, type Page } from '@playwright/test';
import { SHOTS, lessons, seed, v2, KEY_V2, watchErrors } from './engage';
import { answerWrongly, check, expectNoHorizontalOverflow } from './solve';

const LESSON = lessons[0]!;
const FIRST = LESSON.questions[0]!; // a single-choice question

/** The test server has no AI key, so the tutor endpoint is faked in the browser. */
async function fakeTutor(page: Page, opts: { status?: number } = {}) {
  const bodies: any[] = [];
  await page.route('**/api/health', (r) => r.fulfill({ json: { ok: true, ai: false, explain: true } }));
  await page.route('**/api/explain', async (r) => {
    const body = r.request().postDataJSON();
    bodies.push(body);
    if (opts.status && opts.status !== 200) return r.fulfill({ status: opts.status, json: { error: 'The AI tutor failed.' } });
    const last = body.messages[body.messages.length - 1].content as string;
    await r.fulfill({ json: { reply: `Tutor says: ${last.slice(0, 40)}` } });
  });
  return bodies;
}

async function toFeedback(page: Page) {
  await seed(page, { [KEY_V2]: v2() });
  await page.goto(`/#/lesson/${LESSON.id}`);
  await page.locator('.prompt').waitFor();
  await answerWrongly(page, FIRST);
  await check(page);
}

test.describe('Ask why', () => {
  test('shows one quiet line instead of a dead button when no AI is available, and nothing is broken', async ({ page }) => {
    const errors = watchErrors(page);
    await toFeedback(page);
    await expect(page.getByTestId('ask-why')).toHaveCount(0);
    await expect(page.getByTestId('ask-why-off')).toContainText('Ask why needs AI');
    await expectNoHorizontalOverflow(page);
    await page.screenshot({ path: `${SHOTS}/ask-why-1-disabled.png` });
    await page.getByRole('button', { name: 'Continue', exact: true }).click(); // the lesson carries on
    await expect(page.locator('.kicker')).toBeVisible();
    expect(errors).toEqual([]);
  });

  test('chat about a wrong answer: sends the question id and the answer, shows replies, honest note', async ({ page }) => {
    const bodies = await fakeTutor(page);
    await toFeedback(page);
    await page.getByTestId('ask-why').click();
    const dialog = page.getByRole('dialog', { name: 'Ask why' });
    await expect(dialog).toBeVisible();
    await expect(page.getByTestId('ask-why-note')).toContainText('AI can be wrong');
    await expect(page.getByTestId('chat-left')).toHaveText('6 of 6 questions left');
    await expectNoHorizontalOverflow(page);

    await page.getByRole('button', { name: 'Why is my answer wrong?' }).click();
    await expect(dialog.locator('.bubble.assistant')).toContainText('Tutor says: Why is my answer wrong?');
    expect(bodies[0]).toMatchObject({ questionId: FIRST.id, correct: false, messages: [{ role: 'user', content: 'Why is my answer wrong?' }] });
    expect(bodies[0].answer).not.toBe('');

    await dialog.getByRole('textbox', { name: /Your question/ }).fill('Give me an example please');
    await dialog.getByRole('button', { name: 'Send' }).click();
    await expect(dialog.locator('.bubble.assistant').nth(1)).toContainText('Give me an example');
    expect(bodies[1].messages.map((m: any) => m.role)).toEqual(['user', 'assistant', 'user']);
    await expect(page.getByTestId('chat-left')).toHaveText('4 of 6 questions left');
    await page.screenshot({ path: `${SHOTS}/ask-why-2-chat.png` });

    // Escape closes and puts focus back on the button; the Continue key handler did not fire.
    await page.keyboard.press('Escape');
    await expect(dialog).toBeHidden();
    await expect(page.getByTestId('ask-why')).toBeFocused();
    await expect(page.getByRole('button', { name: 'Continue', exact: true })).toBeVisible();

    // Re-opening keeps the conversation for this question.
    await page.getByTestId('ask-why').click();
    await expect(dialog.locator('.bubble')).toHaveCount(4);
  });

  test('the conversation is capped', async ({ page }) => {
    await fakeTutor(page);
    await toFeedback(page);
    await page.getByTestId('ask-why').click();
    const box = page.getByRole('textbox', { name: /Your question/ });
    for (let i = 1; i <= 6; i++) {
      await box.fill(`question ${i}`);
      await page.getByRole('button', { name: 'Send' }).click();
      await expect(page.locator('.bubble.assistant')).toHaveCount(i);
    }
    await expect(page.getByTestId('chat-limit')).toBeVisible();
    await expect(box).toHaveCount(0);
  });

  test('a failing tutor shows a calm message and keeps the question; upstream detail is never shown', async ({ page }) => {
    await fakeTutor(page, { status: 502 });
    await toFeedback(page);
    await page.getByTestId('ask-why').click();
    const box = page.getByRole('textbox', { name: /Your question/ });
    await box.fill('why is B wrong?');
    await page.getByRole('button', { name: 'Send' }).click();
    await expect(page.getByTestId('chat-error')).toContainText('could not answer');
    await expect(box).toHaveValue('why is B wrong?');
    await expect(page.getByTestId('chat-left')).toHaveText('6 of 6 questions left');
    await expect(page.getByTestId('chat-error')).not.toContainText('502');
  });
});
