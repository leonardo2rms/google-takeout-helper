import { expect, test, type Page } from '@playwright/test';
import { defaultPrefs, questionHash } from '../shared/src';
import { KEY_V2, SHOTS, TODAY, day, dueCards, lessons, questions, seed, useClock, v2 } from './engage';

/**
 * Design-system guarantees that no screenshot diff would catch: 44px tap targets and no sideways scrolling on every main screen,
 * at 390px and at the narrowest phone we support (320px), in the light and in the dark theme. The visual record is e2e/screenshots.
 */

interface Small {
  name: string;
  w: number;
  h: number;
}

/** Interactive elements smaller than 44px in either direction. Inline links inside a sentence and code blanks are exempt (WCAG 2.5.8 inline exception). */
async function smallTargets(page: Page): Promise<Small[]> {
  return page.evaluate(() => {
    const out: { name: string; w: number; h: number }[] = [];
    const sel = 'a[href], button, select, textarea, summary, input:not([type="hidden"])';
    for (const el of Array.from(document.querySelectorAll<HTMLElement>(sel))) {
      const cs = getComputedStyle(el);
      if (cs.visibility === 'hidden' || cs.display === 'none') continue;
      if (el.closest('.sr-only, [hidden], pre.code') && !el.closest('.spot')) continue; // fill blanks live inside a code sentence
      if (el.classList.contains('skip-link')) continue;
      let box = el.getBoundingClientRect();
      // a native checkbox / radio / file input is operated through its wrapping label
      if (el instanceof HTMLInputElement && ['checkbox', 'radio', 'file'].includes(el.type)) {
        const label = el.closest('label');
        if (!label) continue;
        box = label.getBoundingClientRect();
      }
      if (box.width === 0 || box.height === 0) continue;
      // an inline link in the middle of a sentence
      const inline = el instanceof HTMLAnchorElement && cs.display === 'inline' && !el.classList.contains('btn');
      if (inline && el.closest('p, li, small')) continue;
      if (box.height < 43.5 || box.width < 43.5) {
        out.push({ name: `${el.tagName.toLowerCase()}.${(el.className || '').toString().split(' ')[0]} "${(el.getAttribute('aria-label') ?? el.textContent ?? '').trim().slice(0, 28)}"`, w: Math.round(box.width), h: Math.round(box.height) });
      }
    }
    return out;
  });
}

const overflow = (page: Page) => page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);

const SCREENS: [string, string, string][] = [
  ['home', '/', '.home'],
  ['progress', '/#/progress', '.progress-page'],
  ['library', '/#/library', '.library-page'],
  ['mock setup', '/#/mock', '.mock-setup'],
];

for (const scheme of ['light', 'dark'] as const) {
  for (const width of [390, 320]) {
    test.describe(`${scheme}, ${width}px`, () => {
      test.use({ colorScheme: scheme, viewport: { width, height: 760 } });

      test('main screens: no sideways scroll, every control at least 44px', async ({ page }) => {
        await useClock(page);
        await seed(page, {
          [KEY_V2]: v2({
            xp: 540, streak: 6, longestStreak: 9, freezes: 1, lastDay: TODAY,
            stats: { sessions: 8, answered: 60, firstTry: 44, perfect: 2, practiceDays: 7 },
            ...dueCards(14, { interval: 8, due: day(9) }),
            bookmarks: { [questions[0]!.id]: { h: questionHash(questions[0]!), day: day(-1) } },
            achievements: { 'first-lesson': day(-6), 'streak-7': day(-1) },
            prefs: { ...defaultPrefs(), reminders: { time: '19:00', notify: false, lastNotified: null } },
          }),
        });
        for (const [name, url, ready] of SCREENS) {
          await page.goto(url);
          await page.locator(ready).waitFor();
          expect(await overflow(page), `${name}: horizontal overflow`).toBeLessThanOrEqual(0);
          expect(await smallTargets(page), `${name}: targets under 44px`).toEqual([]);
        }
        await page.goto('/');
        await page.getByRole('button', { name: 'Settings', exact: true }).click();
        await page.getByRole('dialog').waitFor();
        expect(await overflow(page), 'settings: horizontal overflow').toBeLessThanOrEqual(0);
        expect(await smallTargets(page), 'settings: targets under 44px').toEqual([]);
        if (width === 320) await page.screenshot({ path: `${SHOTS}/narrow-${scheme}-settings.png` });
      });

      test('a lesson question and its feedback', async ({ page }) => {
        await useClock(page);
        await seed(page, { [KEY_V2]: v2() });
        await page.goto(`/#/lesson/${lessons[0]!.id}`);
        await page.locator('.prompt').waitFor();
        expect(await overflow(page), 'question: horizontal overflow').toBeLessThanOrEqual(0);
        expect(await smallTargets(page), 'question: targets under 44px').toEqual([]);
        await page.getByRole('button', { name: "I don't know" }).click();
        await page.locator('.feedback').waitFor();
        expect(await overflow(page), 'feedback: horizontal overflow').toBeLessThanOrEqual(0);
        expect(await smallTargets(page), 'feedback: targets under 44px').toEqual([]);
        if (width === 320) await page.screenshot({ path: `${SHOTS}/narrow-${scheme}-feedback.png` });
      });
    });
  }
}
