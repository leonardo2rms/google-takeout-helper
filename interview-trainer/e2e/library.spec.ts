import { readFileSync } from 'node:fs';
import { expect, test } from '@playwright/test';
import { KEY_V2, SHOTS, cardFor, day, dueCards, expectNoOverflow, lessons, questions, seed, stored, useClock, v2 } from './engage';
import { answerCorrectly, check, next, solveAndContinue } from './solve';

const LESSON = 'kotlin-null-types';
const LESSON_Q = lessons.find((l) => l.id === LESSON)!.questions;
const Q1 = LESSON_Q[0]!;

const items = (page: import('@playwright/test').Page) => page.getByTestId('lib-item');
const count = (page: import('@playwright/test').Page) => page.getByTestId('lib-count');

test.describe('bookmarks and notes', () => {
  test('star a question and add a note from the feedback panel; both persist and show in the Library', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto(`/#/lesson/${LESSON}`);
    // tools only appear after answering
    await expect(page.getByTestId('tools')).toHaveCount(0);
    await answerCorrectly(page, Q1);
    await check(page);
    const tools = page.getByTestId('tools');
    await expect(tools).toBeVisible();
    const star = page.getByTestId('tools-star');
    expect((await star.boundingBox())!.height).toBeGreaterThanOrEqual(44);
    await expect(star).toHaveAttribute('aria-pressed', 'false');
    await star.click();
    await expect(star).toHaveAttribute('aria-pressed', 'true');
    await page.getByTestId('tools-note-btn').click();
    await page.getByRole('textbox', { name: 'Your private note' }).fill('?. gives null, ?: gives the fallback');
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/library-note-editor.png` });
    await page.getByTestId('tools-note-save').click();
    await expect(page.getByTestId('tools-note')).toContainText('?. gives null');

    // Ctrl+Enter / Enter inside the note must not skip the lesson ahead
    await page.getByTestId('tools-note-btn').click();
    await page.getByRole('textbox', { name: 'Your private note' }).press('Enter');
    await expect(page.locator('.feedback')).toBeVisible();
    await page.getByTestId('tools-note-save').click();

    expect((await stored(page))!.bookmarks[Q1.id]).toMatchObject({ day: '2026-11-04' });
    expect((await stored(page))!.notes[Q1.id].text).toContain('?. gives null');

    // finish the lesson, open the Library: the question is listed as saved with its note
    await next(page);
    for (const q of LESSON_Q.slice(1)) await solveAndContinue(page, q);
    await page.goto('/#/library');
    await page.getByTestId('filter-bookmarked').click();
    await expect(items(page)).toHaveCount(1);
    await expect(items(page).first()).toContainText('What does this print?');
    await expect(page.getByTestId(`lib-${Q1.id}-note`)).toContainText('?. gives null');
    await expect(page.getByTestId(`lib-${Q1.id}-star`)).toHaveAttribute('aria-pressed', 'true');

    // un-star from the Library
    await page.getByTestId(`lib-${Q1.id}-star`).click();
    await expect(page.getByTestId('lib-empty')).toContainText('Nothing saved yet');
  });

  test('a backup carries bookmarks and notes across reset and import', async ({ page }, info) => {
    await useClock(page);
    await seed(page, {
      [KEY_V2]: v2({
        bookmarks: { [Q1.id]: { h: (await import('../shared/src')).questionHash(Q1), day: day(-1) } },
        notes: { [Q1.id]: { h: (await import('../shared/src')).questionHash(Q1), text: 'portable note', day: day(-1) } },
      }),
    });
    await page.goto('/');
    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    const sheet = page.getByRole('dialog', { name: 'Settings', exact: true });
    const [download] = await Promise.all([page.waitForEvent('download'), sheet.getByRole('button', { name: 'Download backup' }).click()]);
    const file = info.outputPath('backup.json');
    await download.saveAs(file);
    expect(JSON.parse(readFileSync(file, 'utf8')).progress.notes[Q1.id].text).toBe('portable note');

    await sheet.getByRole('button', { name: 'Reset progress' }).click();
    await page.getByTestId('reset-confirm-ok').click();
    expect((await stored(page))!.notes).toEqual({});

    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    await page.getByTestId('import-file').setInputFiles(file);
    await page.getByTestId('import-confirm').getByRole('button', { name: 'Replace my progress' }).click();
    await page.getByTestId('replace-confirm-ok').click();
    await expect(page.getByTestId('import-message')).toContainText('replaced');
    const after = await stored(page);
    expect(after!.notes[Q1.id].text).toBe('portable note');
    expect(Object.keys(after!.bookmarks)).toEqual([Q1.id]);
  });
});

test.describe('library', () => {
  test('lists everything, searches prompt / tags / explanation, and filters', async ({ page }) => {
    await useClock(page);
    const due = dueCards(3);
    await seed(page, { [KEY_V2]: v2({ cards: due.cards }) });
    await page.goto('/#/library');
    await expect(page.getByRole('heading', { name: 'Library' })).toBeVisible();
    await expect(count(page)).toHaveText(`${questions.length} questions`);
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/library.png` });

    const search = page.getByRole('searchbox', { name: 'Search questions' });
    await search.fill('idempotency');
    const n = Number(await count(page).getAttribute('data-count'));
    expect(n).toBeGreaterThan(0);
    expect(n).toBeLessThan(questions.length);
    await expect(count(page)).toContainText(`of ${questions.length} questions`);
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/library-search.png` });

    // explanation-only matches are found too: pick a word that only occurs in an explanation
    const q = questions.find((x) => x.explanation && /self-invocation/i.test(x.explanation))!;
    await search.fill('self-invocation');
    await expect(items(page).first()).toBeVisible();
    expect(await items(page).evaluateAll((els) => els.map((e) => e.getAttribute('data-qid')))).toContain(q.id);

    await search.fill('zzzz-no-such-thing');
    await expect(page.getByTestId('lib-empty')).toContainText('No question matches');
    await page.getByRole('button', { name: 'Clear filters' }).click();
    await expect(count(page)).toHaveText(`${questions.length} questions`);

    // filters
    await page.getByRole('combobox', { name: 'Question type' }).selectOption('spot');
    const spots = questions.filter((x) => x.type === 'spot').length;
    await expect(count(page)).toContainText(`${spots} of ${questions.length}`);
    await page.getByRole('combobox', { name: 'Question type' }).selectOption('');
    await page.getByRole('combobox', { name: 'Topic' }).selectOption('scale');
    expect(Number(await count(page).getAttribute('data-count'))).toBeGreaterThan(10);
    await page.getByRole('combobox', { name: 'Topic' }).selectOption('');

    await page.getByTestId('filter-due').click();
    await expect(count(page)).toContainText(`3 of ${questions.length}`);
    for (const s of await page.getByTestId('lib-status').allTextContents()) expect(s).toBe('Due');
    await page.getByTestId('filter-due').click();
    await page.getByTestId('filter-neverSeen').click();
    await expect(count(page)).toContainText(`${questions.length - 3} of ${questions.length}`);
    await page.getByTestId('filter-neverSeen').click();
    await page.getByTestId('filter-hasNote').click();
    await expect(count(page)).toContainText(`0 of ${questions.length}`);
  });

  test('paged results, 44px targets, no sideways scroll at 390px', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/#/library');
    await expect(items(page)).toHaveCount(30);
    await page.getByTestId('lib-more').click();
    await expect(items(page)).toHaveCount(60);
    for (const sel of ['.lib-chip', '.lib-input', '.lib-select', '[data-testid=lib-practise]']) {
      const box = (await page.locator(sel).first().boundingBox())!;
      expect(box.height, sel).toBeGreaterThanOrEqual(44);
    }
    await expectNoOverflow(page);
  });

  test('"Practise this one" opens a one-question session that schedules the card', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/#/library');
    await page.getByRole('searchbox', { name: 'Search questions' }).fill('lateinit');
    const row = items(page).first();
    const id = (await row.getAttribute('data-qid'))!;
    const target = questions.find((x) => x.id === id)!;
    await row.getByTestId('lib-practise').click();
    await expect(page).toHaveURL(new RegExp(`#/q/${id}$`));
    await expect(page.locator('.bar')).toBeVisible();
    await solveAndContinue(page, target);
    await expect(page.locator('.summary h1')).toBeVisible();
    const p = await stored(page);
    expect(Object.keys(p!.cards)).toEqual([id]);
    expect(p!.stats).toMatchObject({ sessions: 1, answered: 1, perfect: 0 }); // a session of one is not a "perfect lesson"
    await expect(page.getByTestId('celebration')).toHaveCount(0);
  });

  test('an unknown question id shows the not-found page', async ({ page }) => {
    await page.goto('/#/q/no-such/1');
    await expect(page.getByRole('heading', { name: 'Lesson not found' })).toBeVisible();
  });

  test('the Library is linked from Home and shows how many are saved', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ bookmarks: { [Q1.id]: { h: 'x', day: day(-1) } }, cards: { [Q1.id]: cardFor(Q1) } }) });
    await page.goto('/');
    await expect(page.getByTestId('library-link')).toContainText('1 saved');
    await page.getByTestId('library-link').click();
    await expect(page).toHaveURL(/#\/library$/);
  });
});
