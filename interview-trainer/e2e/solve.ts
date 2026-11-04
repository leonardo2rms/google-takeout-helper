import { expect, type Page } from '@playwright/test';
import type { Question } from '../shared/src';

/** Text as the learner sees it: inline-code backticks are rendered, not shown. */
export const visible = (s: string) => s.replace(/`/g, '').trim();

/** What the kicker above each prompt says, per question type (mirrors web/src/screens/Lesson.tsx). */
export const KICKER: Record<Question['type'], string> = {
  single: 'Choose the best answer',
  multi: 'Select all that apply',
  fill: 'Complete the code',
  order: 'Put the code in order',
  spot: 'Find the bug',
  estimate: 'Estimate the number',
  match: 'Match the pairs',
  open: 'Answer in your own words',
};

async function clickByText(page: Page, selector: string, wanted: string, exact = true) {
  const texts = (await page.locator(selector).allTextContents()).map((t) => (exact ? t : t.trim()));
  const index = texts.findIndex((t) => (exact ? t === wanted : t.trim() === wanted));
  if (index === -1) throw new Error(`"${wanted}" not found in ${selector}: ${JSON.stringify(texts)}`);
  await page.locator(selector).nth(index).click();
}

/** Fills in the correct answer for `q` using only what a learner can see and tap. */
export async function answerCorrectly(page: Page, q: Question): Promise<void> {
  switch (q.type) {
    case 'single': {
      const text = q.options.find((o) => o.id === q.answer)!.text;
      await clickByText(page, '.option-text', visible(text), false);
      break;
    }
    case 'multi':
      for (const id of q.answers) await clickByText(page, '.option-text', visible(q.options.find((o) => o.id === id)!.text), false);
      break;
    case 'fill':
      // Used tokens become disabled, so duplicates (async, async) resolve one at a time.
      for (const token of q.answers) await clickByText(page, '.bank .chip:not([disabled])', token, false);
      break;
    case 'order':
      for (const line of q.lines) await clickByText(page, '.order-pool .code-line:not([disabled])', line, true);
      break;
    case 'spot':
      for (const n of q.answers) await page.locator(`button[data-line="${n}"]`).click();
      break;
    case 'estimate':
      await page.getByRole('textbox', { name: 'Your estimate' }).fill(String(q.answer));
      break;
    case 'match':
      for (const p of q.pairs) {
        await clickByText(page, '[data-side="left"]', visible(p.left), false);
        await clickByText(page, '[data-side="right"]:not(.paired)', visible(p.right), false);
      }
      break;
    case 'open':
      await page.getByRole('textbox', { name: 'Your answer' }).fill(q.modelAnswer);
      break;
  }
}

/** A deliberately wrong answer for the types where that is easy to express. */
export async function answerWrongly(page: Page, q: Question): Promise<void> {
  switch (q.type) {
    case 'single': {
      const wrong = q.options.find((o) => o.id !== q.answer)!;
      await clickByText(page, '.option-text', visible(wrong.text), false);
      break;
    }
    case 'spot': {
      const lines = q.source.replace(/\n+$/, '').split('\n');
      const wrong = lines.findIndex((l, i) => l.trim() !== '' && !q.answers.includes(i + 1)) + 1;
      await page.locator(`button[data-line="${wrong}"]`).click();
      break;
    }
    case 'estimate':
      await page.getByRole('textbox', { name: 'Your estimate' }).fill(String(q.answer * 3 + 7));
      break;
    case 'match': {
      // Connect every item to the NEXT item's partner, which is wrong for every pair.
      const n = q.pairs.length;
      for (let i = 0; i < n; i++) {
        await clickByText(page, '[data-side="left"]', visible(q.pairs[i]!.left), false);
        await clickByText(page, '[data-side="right"]:not(.paired)', visible(q.pairs[(i + 1) % n]!.right), false);
      }
      break;
    }
    default:
      throw new Error(`no wrong-answer helper for ${q.type}`);
  }
}

/** The page must never scroll sideways on a phone: long code/identifiers have to wrap or scroll in place. */
export async function expectNoHorizontalOverflow(page: Page): Promise<void> {
  const { scrollWidth, clientWidth } = await page.evaluate(() => ({
    scrollWidth: document.documentElement.scrollWidth,
    clientWidth: document.documentElement.clientWidth,
  }));
  expect(scrollWidth, 'page is wider than the viewport').toBeLessThanOrEqual(clientWidth);
}

export async function check(page: Page): Promise<void> {
  await page.getByRole('button', { name: 'Check', exact: true }).click();
  await page.locator('.feedback').waitFor();
  // Open questions have no AI in tests: tick any self-assessment boxes the keyword pre-check missed.
  const boxes = page.locator('.rubric input[type="checkbox"]');
  for (let i = 0; i < (await boxes.count()); i++) if (!(await boxes.nth(i).isChecked())) await boxes.nth(i).check();
  await page.waitForTimeout(350); // let the feedback scroll into view above the bottom bar
}

export const next = (page: Page) => page.getByRole('button', { name: 'Continue', exact: true }).click();

/** Solve and confirm the current question correctly, then move on. */
export async function solveAndContinue(page: Page, q: Question): Promise<void> {
  await expect(page.locator('.kicker')).toContainText(KICKER[q.type]);
  await answerCorrectly(page, q);
  await expectNoHorizontalOverflow(page);
  await check(page);
  await expectNoHorizontalOverflow(page);
  await expect(page.locator('.feedback')).toHaveClass(/good/);
  await next(page);
}
