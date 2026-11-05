/**
 * The PWA shell against the real server (QA S1-2): missing assets are real 404s, the worker pre-caches this build's
 * files under a version-keyed name, the app works offline, and an update announces itself without reloading the page.
 */
import { expect, test, type Page } from '@playwright/test';
import { questions } from './engage';

const single = questions.find((q) => q.type === 'single')!;

async function controlled(page: Page) {
  await page.goto('/');
  await page.evaluate(() => navigator.serviceWorker.ready);
  await page.reload(); // the first load is not controlled until the worker claims the page
  await page.waitForFunction(() => navigator.serviceWorker.controller !== null);
}

test.describe('missing files', () => {
  test('a missing script is a 404 that is not HTML, and is never cached', async ({ page }) => {
    await controlled(page);
    const res = await page.evaluate(async () => {
      const r = await fetch('/assets/index-DEADBEEF.js');
      return { status: r.status, type: r.headers.get('content-type') };
    });
    expect(res.status).toBe(404);
    expect(res.type).not.toContain('html');
    expect(await page.evaluate(async () => (await caches.match('/assets/index-DEADBEEF.js')) !== undefined)).toBe(false);
  });

  test('the server marks the shell and worker for revalidation and bundles as immutable, with security headers', async ({ request }) => {
    const index = await request.get('/');
    expect(index.headers()['cache-control']).toBe('no-cache');
    expect(index.headers()['content-security-policy']).toContain("default-src 'self'");
    expect(index.headers()['x-content-type-options']).toBe('nosniff');
    expect((await request.get('/sw.js')).headers()['cache-control']).toBe('no-cache');
    const html = await index.text();
    const bundle = /\/assets\/[^"']+\.js/.exec(html)![0];
    expect((await request.get(bundle)).headers()['cache-control']).toContain('immutable');
  });
});

test.describe('installed worker', () => {
  test('pre-caches every file of this build in a version-keyed cache, and nothing else', async ({ page, request }) => {
    await controlled(page);
    const worker = await (await request.get('/sw.js')).text();
    const listed: string[] = JSON.parse(/\["\/",[^\]]*\]/.exec(worker)![0]);
    const cached = await page.evaluate(async () => {
      const names = await caches.keys();
      const entries: Record<string, string[]> = {};
      for (const n of names) entries[n] = (await (await caches.open(n)).keys()).map((r) => new URL(r.url).pathname);
      return entries;
    });
    const names = Object.keys(cached);
    expect(names).toHaveLength(1);
    expect(names[0]).toMatch(/^interview-trainer-[0-9a-f]{12}$/);
    for (const path of listed) expect(cached[names[0]!], path).toContain(path);
    expect(listed.some((p) => p.endsWith('.js'))).toBe(true);
  });

  test('works offline after the first visit', async ({ page, context }) => {
    await controlled(page);
    await context.setOffline(true);
    await page.reload();
    await expect(page.locator('.home')).toBeVisible();
    await page.goto(`/#/q/${single.id}`);
    await expect(page.locator('.prompt')).toBeVisible();
    await context.setOffline(false);
  });

  test('a new version announces itself with a quiet notice and does not reload the page', async ({ page }) => {
    await controlled(page);
    await expect(page.getByTestId('update-ready')).toHaveCount(0);
    await page.evaluate(() => {
      (window as unknown as { __marker: number }).__marker = 1;
      navigator.serviceWorker.dispatchEvent(new Event('controllerchange'));
    });
    const notice = page.getByTestId('update-ready');
    await expect(notice).toContainText('A new version is ready');
    expect(await page.evaluate(() => (window as unknown as { __marker?: number }).__marker)).toBe(1); // still the same page
    await notice.getByRole('button', { name: 'Later' }).click();
    await expect(notice).toHaveCount(0);
  });

  test('Reload in the update notice reloads the page', async ({ page }) => {
    await controlled(page);
    await page.evaluate(() => {
      (window as unknown as { __marker: number }).__marker = 1;
      navigator.serviceWorker.dispatchEvent(new Event('controllerchange'));
    });
    await page.getByTestId('update-ready').getByRole('button', { name: 'Reload' }).click();
    await expect(page.locator('.home')).toBeVisible();
    expect(await page.evaluate(() => (window as unknown as { __marker?: number }).__marker)).toBeUndefined();
  });
});
