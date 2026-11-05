import AxeBuilder from '@axe-core/playwright';
import { expect, test, type Page } from '@playwright/test';
import { defaultPrefs, questionHash, type Lesson, type Question } from '../shared/src';
import { KEY_V2, TODAY, course, day, dueCards, lessons, playLesson, questions, seed, useClock, v2 } from './engage';
import { answerCorrectly, answerWrongly, check, next, solveAndContinue } from './solve';

/**
 * Accessibility: axe-core over every screen and state, failing on serious/critical violations. Nothing is excluded:
 * colour contrast (WCAG AA) is enforced like every other rule, in the light and in the dark theme.
 */
const SCHEMES = ['light', 'dark'] as const;
const TAGS = ['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa', 'best-practice'];

export async function scan(page: Page, where: string): Promise<void> {
  const results = await new AxeBuilder({ page }).withTags(TAGS).analyze();
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

for (const scheme of SCHEMES) test.describe(`axe: home and sheets (${scheme})`, () => {
  test.use({ colorScheme: scheme });
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
    await page.getByRole('heading', { name: 'Nothing to review' }).waitFor();
    await scan(page, 'all clear');
  });
});

for (const scheme of SCHEMES) test.describe(`axe: every question type (${scheme})`, () => {
  test.use({ colorScheme: scheme });
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

for (const scheme of SCHEMES) test.describe(`axe: summary (${scheme})`, () => {
  test.use({ colorScheme: scheme });
  test('lesson summary with a celebration', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    const lesson = lessons[0]!;
    await playLesson(page, lesson.id);
    await page.locator('.summary h1').waitFor();
    await scan(page, 'summary');
  });
});


for (const scheme of SCHEMES) test.describe(`axe: learning features (${scheme})`, () => {
  test.use({ colorScheme: scheme });

  test('library: list with saved questions and notes, filters, note editor', async ({ page }) => {
    await useClock(page);
    const some = questions.slice(0, 4);
    await seed(page, {
      [KEY_V2]: v2({
        ...dueCards(6),
        bookmarks: Object.fromEntries(some.map((q) => [q.id, { h: questionHash(q), day: day(-1) }])),
        notes: { [some[0]!.id]: { h: questionHash(some[0]!), text: 'Remember the Elvis operator.', day: day(-1) } },
      }),
    });
    await page.goto('/#/library');
    await page.getByTestId('lib-list').waitFor();
    await scan(page, 'library');
    await page.getByTestId('filter-bookmarked').click();
    await scan(page, 'library (filter on)');
    await page.getByRole('button', { name: 'Edit note' }).first().click();
    await scan(page, 'library (note editor)');
    await page.getByRole('searchbox', { name: 'Search questions' }).fill('zzzz-nothing');
    await scan(page, 'library (empty)');
  });

  test('mock interview: setup, running, confirm, results, saved', async ({ page }) => {
    await useClock(page);
    await seed(page, {
      [KEY_V2]: v2({ mocks: [{ day: day(-2), total: 10, answered: 9, score: 70, limitMin: 15, usedSec: 700, timedOut: false, byUnit: { kotlin: { n: 10, score: 7 } } }] }),
    });
    await page.goto('/#/mock');
    await page.getByTestId('mock-start').waitFor();
    await scan(page, 'mock setup');
    await page.getByTestId('mock-start').click();
    await page.getByTestId('mock-run').waitFor();
    await page.getByTestId('mock-flag').click();
    await page.getByTestId('mock-next').click();
    await scan(page, 'mock running (flagged question in the palette)');
    await page.getByTestId('mock-finish-early').click();
    await scan(page, 'mock confirm');
    await page.getByTestId('mock-submit').click();
    await page.getByTestId('mock-results').waitFor();
    await scan(page, 'mock results');
    await page.getByTestId('mock-save').click();
    await page.getByTestId('mock-saved').waitFor();
    await scan(page, 'mock saved');
  });

  test('progress: readiness, confidence, mock history, badges', async ({ page }) => {
    await useClock(page);
    await seed(page, {
      [KEY_V2]: v2({
        xp: 540, streak: 9, longestStreak: 9, lastDay: TODAY,
        stats: { sessions: 12, answered: 90, firstTry: 60, perfect: 2, practiceDays: 9 },
        ...dueCards(20, { interval: 8, due: day(12) }),
        calibration: { n: [12, 14, 20], right: [3, 8, 19] },
        mocks: [
          { day: day(-1), total: 10, answered: 10, score: 80, limitMin: 15, usedSec: 640, timedOut: false, byUnit: { kotlin: { n: 10, score: 8 } } },
          { day: day(-5), total: 10, answered: 8, score: 50, limitMin: 15, usedSec: 900, timedOut: true, byUnit: { kotlin: { n: 10, score: 5 } } },
        ],
        achievements: { 'first-lesson': day(-6), 'perfect-lesson': day(-3), 'streak-7': day(-1), 'first-code': day(-2) },
      }),
    });
    await page.goto('/#/progress');
    await page.getByTestId('trophy-case').waitFor();
    await scan(page, 'progress (full)');
  });

  test('lesson: hint, confidence, feedback, saved question and note', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ settings: { ...v2().settings, confidence: true } }) });
    const withHint = questions.find((q) => q.hint && q.type === 'single')!;
    const lesson = lessons.find((l) => l.questions.some((q) => q.id === withHint.id))!;
    await page.goto(`/#/lesson/${lesson.id}`);
    await page.locator('.prompt').waitFor();
    for (const earlier of lesson.questions.slice(0, lesson.questions.indexOf(withHint))) await solveAndContinue(page, earlier);
    await scan(page, 'question with a hint button and the confidence row');
    await page.getByTestId('hint-btn').click();
    await page.getByTestId('hint-text').waitFor();
    await page.getByRole('radio', { name: 'Sure' }).click();
    await scan(page, 'hint revealed, confidence chosen');
    await answerWrongly(page, withHint);
    await check(page);
    await page.getByTestId('tools-star').click();
    await page.getByTestId('tools-note-btn').click();
    await scan(page, 'feedback with a saved question and the note editor');
  });

  test('home with a finished daily challenge and an interview countdown', async ({ page }) => {
    await useClock(page);
    await seed(page, {
      [KEY_V2]: v2({ xp: 90, streak: 2, lastDay: TODAY, challenge: { [TODAY]: 'pass' }, profile: { ...v2().profile, interviewDate: day(12), role: 'generalist' }, ...dueCards(4) }),
    });
    await page.goto('/');
    await page.getByTestId('challenge-card').waitFor();
    await scan(page, 'home (challenge done, countdown)');
  });

  test('settings: reading section, reminders on, import confirmation', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ xp: 80, prefs: { ...defaultPrefs(), reminders: { time: '19:00', notify: false, lastNotified: null } } }) });
    await page.goto('/');
    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    await page.getByRole('heading', { name: 'Reading' }).scrollIntoViewIfNeeded();
    await page.getByTestId('text-size-XL').check();
    await scan(page, 'settings at XL text size');
    await page.getByTestId('dyslexia-font').check();
    await scan(page, 'settings with the easier-to-read font');
    await page.getByTestId('import-file').setInputFiles({ name: 'junk.json', mimeType: 'application/json', buffer: Buffer.from('this is not a backup') });
    await scan(page, 'settings with an import error');
  });
});

void course;
void next;
