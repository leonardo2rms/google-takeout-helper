/**
 * The single-file build, opened the way people open it (QA S1-3, S1-7): from disk as a complete document, and inside a
 * host that offers a `sample` capability that may never answer.
 */
import { execFileSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, test, type Page } from '@playwright/test';
import { KEY_V2, TODAY, dueCards, questions, seed, useClock, v2, watchErrors } from './engage';

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

/**
 * File offers in the hosted viewer: `<a download>` does nothing there, so saving goes through the `downloads` capability
 * (`window.claude.use('downloads').save({ filename, data })`), which asks the learner to confirm and may be declined.
 */
type Saved = { filename: string; kind: 'string' | 'blob'; text?: string; head?: number[]; size?: number };
const practised = () =>
  v2({ xp: 260, streak: 5, longestStreak: 5, lastDay: TODAY, stats: { sessions: 6, answered: 40, firstTry: 30, perfect: 1, practiceDays: 5 }, ...dueCards(14, { interval: 8, due: '2026-11-20' }) });

/** A pretend viewer. Every accepted save is recorded on window.__saves, every attempt on window.__calls. */
async function fakeViewer(page: Page, downloads: 'accept' | 'decline' | 'absent') {
  await page.addInitScript((mode) => {
    const w = window as unknown as { claude: unknown; __saves: unknown[]; __calls: string[] };
    w.__saves = [];
    w.__calls = [];
    const save = async (req: { filename: string; data: string | Blob }) => {
      w.__calls.push(req.filename);
      if (mode === 'decline') throw { code: 'declined', message: 'The viewer said no.' };
      if (typeof req.data === 'string') w.__saves.push({ filename: req.filename, kind: 'string', text: req.data });
      else w.__saves.push({ filename: req.filename, kind: 'blob', head: [...new Uint8Array(await req.data.arrayBuffer()).subarray(0, 4)], size: req.data.size });
      return { status: 'saved' };
    };
    w.claude = { use: async (name: string) => (name === 'downloads' && mode !== 'absent' ? { save } : null) };
  }, downloads);
}
const savesOf = (page: Page) => page.evaluate(() => (window as unknown as { __saves: Saved[] }).__saves);
const callsOf = (page: Page) => page.evaluate(() => (window as unknown as { __calls: string[] }).__calls);
const noShare = (page: Page) =>
  page.addInitScript(() => {
    Object.defineProperty(navigator, 'share', { value: undefined, configurable: true });
    Object.defineProperty(navigator, 'canShare', { value: undefined, configurable: true });
  });

async function openHosted(page: Page, route: string) {
  await useClock(page);
  await seed(page, { [KEY_V2]: practised() });
  await page.goto(`${FILE}${route}`);
}
async function openSettings(page: Page) {
  await openHosted(page, '');
  await page.getByRole('button', { name: 'Settings', exact: true }).click();
  return page.getByRole('dialog', { name: 'Settings', exact: true });
}

test.describe('hosted viewer with the downloads capability', () => {
  test('Download backup hands the viewer a JSON file with the dated name', async ({ page }) => {
    const errors = watchErrors(page);
    await fakeViewer(page, 'accept');
    const sheet = await openSettings(page);
    await sheet.getByRole('button', { name: 'Download backup' }).click();
    await expect(page.getByTestId('import-message')).toContainText('Backup saved');
    const [saved] = await savesOf(page);
    expect(saved!.filename).toBe('interview-trainer-progress-2026-11-04.json');
    expect(saved!.kind).toBe('string');
    expect(JSON.parse(saved!.text!)).toMatchObject({ app: 'interview-trainer', format: 2, progress: { xp: 260, streak: 5 } });
    expect(errors).toEqual([]);
  });

  test('a decline is met with a calm message, saves nothing, and can simply be tried again', async ({ page }) => {
    await fakeViewer(page, 'decline');
    const sheet = await openSettings(page);
    await sheet.getByRole('button', { name: 'Download backup' }).click();
    await expect(page.getByTestId('import-message')).toContainText('Nothing was saved');
    await expect(page.getByTestId('import-error')).toHaveCount(0);
    await sheet.getByRole('button', { name: 'Download backup' }).click();
    await expect.poll(() => callsOf(page)).toHaveLength(2);
    expect(await savesOf(page)).toEqual([]);
  });

  test('the share card is saved as a real PNG (a Blob)', async ({ page }) => {
    await fakeViewer(page, 'accept');
    await noShare(page);
    await openHosted(page, '#/progress');
    await page.getByTestId('share-open').click();
    await expect(page.getByTestId('share-preview')).toBeVisible();
    await page.getByTestId('share-download').click();
    await expect(page.getByTestId('share-message')).toContainText('Image saved');
    const [saved] = await savesOf(page);
    expect(saved).toMatchObject({ filename: 'interview-trainer-progress.png', kind: 'blob', head: [0x89, 0x50, 0x4e, 0x47] });
    expect(saved!.size).toBeGreaterThan(5_000);
  });

  test('declining the share image is calm too', async ({ page }) => {
    await fakeViewer(page, 'decline');
    await noShare(page);
    await openHosted(page, '#/progress');
    await page.getByTestId('share-open').click();
    await page.getByTestId('share-download').click();
    await expect(page.getByTestId('share-message')).toContainText('Nothing was saved');
  });

  test('calendar (.ics) files are not on the viewer allowlist, so the button is replaced by an honest note', async ({ page }) => {
    await fakeViewer(page, 'accept');
    const sheet = await openSettings(page);
    await sheet.getByRole('heading', { name: 'Reminders' }).scrollIntoViewIfNeeded();
    await expect(page.getByTestId('reminder-ics')).toHaveCount(0);
    await expect(page.getByTestId('reminder-ics-off')).toContainText('cannot save calendar');
  });
});

test.describe('hosted viewer without the downloads capability', () => {
  test('the old fallbacks show: copy text, a note instead of the calendar button, press-and-hold for the card', async ({ page }) => {
    await fakeViewer(page, 'absent');
    await noShare(page);
    const sheet = await openSettings(page);
    await sheet.getByRole('button', { name: 'Download backup' }).click();
    await expect(page.getByTestId('import-error')).toContainText('Downloads are blocked here');
    await expect(sheet.getByRole('button', { name: 'Copy backup text' })).toBeVisible();
    await sheet.getByRole('heading', { name: 'Reminders' }).scrollIntoViewIfNeeded();
    await expect(page.getByTestId('reminder-ics')).toHaveCount(0);
    await expect(page.getByTestId('reminder-ics-off')).toBeVisible();

    await page.goto(`${FILE}#/progress`);
    await page.getByTestId('share-open').click();
    await expect(page.getByTestId('share-preview')).toBeVisible();
    await expect(page.getByTestId('share-download')).toHaveCount(0);
    await expect(page.getByTestId('share-hint')).toContainText('press and hold');
  });
});
