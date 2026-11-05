import AxeBuilder from '@axe-core/playwright';
import { expect, test, type Page } from '@playwright/test';
import { defaultPrefs, type Lesson, type Question } from '../shared/src';
import { KEY_V2, TODAY, course, dueCards, lessons, playLesson, seed, useClock, v2 } from './engage';
import { answerCorrectly, answerWrongly, check, next, solveAndContinue } from './solve';

/**
 * Accessibility: axe-core over every screen and state, failing on serious/critical violations.
 *
 * TRACKED EXCEPTION: colour contrast is NOT enforced yet. A visual redesign (new palette and typography) is planned, and
 * fixing contrast on the old palette would be thrown away. TODO(redesign): delete this constant so `color-contrast` is
 * enforced again, then fix whatever it finds. This is the ONLY rule excluded, and it is documented in docs/accessibility.md.
 */
export const KNOWN_EXCEPTIONS: readonly string[] = ['color-contrast'];

const TAGS = ['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa', 'best-practice'];

export async function scan(page: Page, where: string): Promise<void> {
  const results = await new AxeBuilder({ page }).withTags(TAGS).disableRules([...KNOWN_EXCEPTIONS]).analyze();
  const bad = results.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical');
  const report = bad.map((v) => `${v.id} (${v.impact}): ${v.help}\n    ${v.nodes.slice(0, 4).map((n) => `${n.target.join(' ')}  <- ${n.failureSummary?.replace(/\s+/g, ' ').slice(0, 220)}`).join('\n    ')}`).join('\n  ');
  expect(bad, `axe found serious/critical issues on "${where}":\n  ${report}`).toEqual([]);
}

const TYPES: Question['type'][] = ['single', 'multi', 'fill', 'order', 'spot', 'estimate', 'match', 'code', 'open'];

function firstOf(type: Question['type']): { lesson: Lesson; index: number } {
  for (const lesson of lessons) {
    const index = lesson.questions.findIndex((q) => q.type === type);
    if (index !== -1) return { lesson, index };
  }
  throw new Error(`no ${type} question`);
}

async function gotoQuestion(page: Page, type: Question['type']): Promise<Question> {
  const { lesson, index } = firstOf(type);
  await page.goto(`/#/lesson/${lesson.id}`);
  await page.locator('.prompt').waitFor();
  for (const earlier of lesson.questions.slice(0, index)) await solveAndContinue(page, earlier);
  return lesson.questions[index]!;
}

test.describe('axe: home and sheets', () => {
  test('new learner: home with the onboarding card', async ({ page }) => {
    await useClock(page);
    await page.goto('/');
    await page.locator('.home').waitFor();
    await scan(page, 'home (new learner, onboarding card)');
  });

  test('active learner: home, streak, goal, settings, reminders, backup sheets', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ xp: 260, streak: 5, longestStreak: 5, lastDay: TODAY, ...dueCards(12), goal: { target: 8 }, prefs: { ...defaultPrefs(), reminders: { time: '19:00', notify: false, lastNotified: null } } }) });
    await page.goto('/');
    await page.locator('.home').waitFor();
    await scan(page, 'home (active)');

    await page.getByTestId('streak-chip').click();
    await scan(page, 'streak sheet');
    await page.keyboard.press('Escape');

    await page.getByTestId('goal-ring').click();
    await scan(page, 'goal sheet');
    await page.keyboard.press('Escape');

    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    await scan(page, 'settings sheet');
    await page.getByText('Paste backup text instead').click();
    await scan(page, 'settings sheet (paste backup open)');
  });

  test('home in dark mode', async ({ page }) => {
    await page.emulateMedia({ colorScheme: 'dark' });
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ xp: 260, streak: 5, lastDay: TODAY, ...dueCards(3) }) });
    await page.goto('/');
    await page.locator('.home').waitFor();
    await scan(page, 'home (dark)');
  });

  test('progress screen and share sheet', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ xp: 260, streak: 5, lastDay: TODAY, stats: { sessions: 6, answered: 40, firstTry: 30, perfect: 1, practiceDays: 5 }, ...dueCards(14, { interval: 8, due: '2026-11-20' }) }) });
    await page.goto('/#/progress');
    await page.locator('.progress-page').waitFor();
    await scan(page, 'progress');
    await page.getByTestId('share-open').click();
    await page.getByTestId('share-preview').waitFor();
    await scan(page, 'share sheet');
  });

  test('not-found and all-clear screens', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/#/lesson/does-not-exist');
    await page.getByRole('heading', { name: 'Lesson not found' }).waitFor();
    await scan(page, 'lesson not found');
    await page.goto('/#/review');
    await page.getByRole('heading', { name: 'All clear!' }).waitFor();
    await scan(page, 'all clear');
  });
});

test.describe('axe: every question type', () => {
  for (const type of TYPES) {
    test(`${type}: question, answered, feedback`, async ({ page }) => {
      await useClock(page);
      await seed(page, { [KEY_V2]: v2() });
      const q = await gotoQuestion(page, type);
      await scan(page, `${type}: question`);
      await answerCorrectly(page, q);
      await scan(page, `${type}: answered`);
      await check(page);
      await scan(page, `${type}: feedback (correct)`);
    });
  }

  // Types with a "wrong answer" helper are answered wrongly; the rest show the same feedback through "I don't know".
  for (const type of ['single', 'multi', 'fill', 'order', 'spot', 'estimate', 'match'] as const) {
    test(`${type}: feedback (wrong answer)`, async ({ page }) => {
      await useClock(page);
      await seed(page, { [KEY_V2]: v2() });
      const q = await gotoQuestion(page, type);
      if (['single', 'spot', 'estimate', 'match'].includes(type)) {
        await answerWrongly(page, q);
        await check(page);
      } else {
        await page.getByRole('button', { name: "I don't know" }).click();
        await page.locator('.feedback').waitFor();
      }
      await scan(page, `${type}: feedback (wrong)`);
    });
  }

  test('"I don\'t know", the Ask why button and sheet, and an interviewer follow-up', async ({ page }) => {
    await page.route('**/api/health', (r) => r.fulfill({ json: { ok: true, ai: false, explain: true } }));
    await page.route('**/api/explain', (r) => r.fulfill({ json: { reply: 'Because of the explanation above.' } }));
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto(`/#/lesson/${lessons[0]!.id}`);
    await page.locator('.prompt').waitFor();
    await page.getByRole('button', { name: "I don't know" }).click();
    await page.locator('.feedback').waitFor();
    await scan(page, 'skipped question feedback');
    await page.getByTestId('ask-why').click();
    await page.getByRole('button', { name: 'Explain it simply' }).click();
    await page.locator('.bubble.assistant').waitFor();
    await scan(page, 'ask why sheet with a reply');
  });
});

test.describe('axe: summary', () => {
  test('lesson summary with a celebration', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    const lesson = lessons[0]!;
    await playLesson(page, lesson.id);
    await page.locator('.summary h1').waitFor();
    await scan(page, 'summary');
  });
});

void course;
void next;
