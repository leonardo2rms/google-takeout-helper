/**
 * The single-file build, opened the way people open it (QA S1-3, S1-7): from disk as a complete document, and inside a
 * host that offers a `sample` capability that may never answer.
 */
import { execFileSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, test } from '@playwright/test';
import { questions, watchErrors } from './engage';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const FILE = `file://${resolve(root, 'dist/interview-trainer.standalone.html')}`;
const open = questions.find((q) => q.type === 'open')!;

test.describe.configure({ mode: 'serial' });
test.beforeAll(() => {
  test.setTimeout(120_000);
  execFileSync('node', ['scripts/build-standalone.mjs'], { cwd: root, stdio: 'ignore' });
});

test.describe('opened from disk', () => {
  test('is a complete standards-mode UTF-8 document with a phone viewport', async ({ page }) => {
    const errors = watchErrors(page);
    await page.goto(FILE);
    await expect(page.locator('.home')).toBeVisible();
    const doc = await page.evaluate(() => ({
      compat: document.compatMode,
      charset: document.characterSet,
      viewport: document.querySelector('meta[name=viewport]')?.getAttribute('content'),
      width: document.documentElement.clientWidth,
      scroll: document.documentElement.scrollWidth,
    }));
    expect(doc.compat).toBe('CSS1Compat');
    expect(doc.charset).toBe('UTF-8');
    expect(doc.viewport).toContain('width=device-width');
    expect(doc.width).toBe(390);
    expect(doc.scroll).toBeLessThanOrEqual(doc.width);
    expect(errors).toEqual([]);
  });

  test('shows non-ASCII text correctly (no Latin-1 mojibake)', async ({ page }) => {
    await page.goto(`${FILE}#/progress`);
    await expect(page.locator('h1').first()).toBeVisible();
    expect(await page.locator('body').innerText()).not.toMatch(/Â|â€/);
  });
});

test.describe('a host whose sample capability never answers', () => {
  test('grading falls back to self-assessment instead of hanging on "Grading..."', async ({ page }) => {
    await page.clock.install();
    await page.addInitScript(() => {
      const hang = Object.assign(() => new Promise(() => {}), { json: () => new Promise(() => {}) });
      (window as unknown as { claude: unknown }).claude = { use: async (name: string) => (name === 'sample' ? hang : null) };
    });
    await page.goto(`${FILE}#/q/${open.id}`);
    await page.getByRole('textbox', { name: 'Your answer' }).fill('some answer that the model never judges');
    await page.getByRole('button', { name: 'Check', exact: true }).click();
    await expect(page.getByRole('button', { name: /Grading/ })).toBeVisible();
    await page.clock.fastForward(26_000);
    await expect(page.locator('.feedback')).toBeVisible();
    await expect(page.locator('.rubric input[type="checkbox"]').first()).toBeVisible(); // self-check, the learner decides
  });
});

test.describe('a host whose sample capability returns a well-shaped but junk verdict (N-S2-6)', () => {
  test('the learner is asked to self-check, with an honest note, instead of seeing a confident INCORRECT', async ({ page }) => {
    await page.addInitScript(() => {
      // Right shape, right length, nothing behind it: every point false, empty feedback.
      const junk = Object.assign(() => Promise.resolve({ text: '' }), { json: () => Promise.resolve({ points: [{ covered: false }, { covered: false }, { covered: false }, { covered: false }, { covered: false }, { covered: false }], feedback: '' }) });
      (window as unknown as { claude: unknown }).claude = { use: async (name: string) => (name === 'sample' ? junk : null) };
    });
    await page.goto(`${FILE}#/q/${open.id}`);
    await page.getByRole('textbox', { name: 'Your answer' }).fill('A thorough answer that mentions the main points of the question in some detail.');
    await page.getByRole('button', { name: 'Check', exact: true }).click();
    await expect(page.getByTestId('grading-note')).toContainText('did not look reliable');
    await expect(page.locator('.rubric input[type="checkbox"]').first()).toBeVisible();
    await expect(page.getByTestId('verdict')).not.toContainText(/incorrect/i);
  });
});
