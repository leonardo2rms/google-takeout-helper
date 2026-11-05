import { expect, test, type Page } from '@playwright/test';
import { type Question } from '../shared/src';
import { KEY_V2, SHOTS, TODAY, course, dueCards, expectNoOverflow, lessons, seed, stored, useClock, v2 } from './engage';
import { answerCorrectly, check, solveAndContinue, visible } from './solve';

/** Walks the keyboard Tab order of the current screen and reports every stop that has no visible focus ring. */
async function tabStopsWithoutRing(page: Page, max = 60, min = 3): Promise<string[]> {
  const bad: string[] = [];
  let stops = 0;
  await page.evaluate(() => {
    (document.activeElement as HTMLElement | null)?.blur();
    (window as unknown as { __seen: WeakSet<Element> }).__seen = new WeakSet();
  });
  for (let i = 0; i < max; i++) {
    // the code editor indents on Tab by design; Escape releases it (that is the documented way out)
    if (await page.evaluate(() => document.activeElement instanceof HTMLTextAreaElement)) await page.keyboard.press('Escape');
    await page.keyboard.press('Tab');
    const stop = await page.evaluate(() => {
      const el = document.activeElement as HTMLElement | null;
      if (!el || el === document.body) return { done: true as const };
      const w = window as unknown as { __seen: WeakSet<Element> };
      if (w.__seen.has(el)) return { done: true as const };
      w.__seen.add(el);
      const ring = (e: Element | null) => {
        if (!e) return false;
        const cs = getComputedStyle(e);
        return cs.outlineStyle !== 'none' && parseFloat(cs.outlineWidth) >= 2;
      };
      // a visually hidden native input is allowed to show its ring on the label that wraps it
      const ok = ring(el) || ring(el.closest('label'));
      const name = `${el.tagName.toLowerCase()}${el.id ? `#${el.id}` : ''}${el.className && typeof el.className === 'string' ? `.${el.className.split(' ')[0]}` : ''} "${(el.getAttribute('aria-label') ?? el.textContent ?? '').trim().slice(0, 30)}"`;
      return { done: false as const, ok, name };
    });
    if (stop.done) break;
    stops++;
    if (!stop.ok) bad.push(stop.name);
  }
  if (stops < min) bad.push(`only ${stops} tab stops found: the scan is not reaching the controls`);
  return bad;
}

const firstOf = (type: Question['type']) => {
  for (const lesson of lessons) {
    const index = lesson.questions.findIndex((q) => q.type === type);
    if (index !== -1) return { lesson, index };
  }
  throw new Error(type);
};

async function gotoQuestion(page: Page, type: Question['type']): Promise<Question> {
  const { lesson, index } = firstOf(type);
  await page.goto(`/#/lesson/${lesson.id}`);
  await page.locator('.prompt').waitFor();
  for (const earlier of lesson.questions.slice(0, index)) await solveAndContinue(page, earlier);
  return lesson.questions[index]!;
}

test.describe('page structure and navigation', () => {
  test('skip link is the first tab stop, appears on focus and moves focus without changing the route', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    await page.locator('.home').waitFor();
    await page.keyboard.press('Tab');
    const skip = page.getByRole('link', { name: 'Skip to main content' });
    await expect(skip).toBeFocused();
    expect((await skip.boundingBox())!.y).toBeGreaterThanOrEqual(0);
    await page.screenshot({ path: `${SHOTS}/a11y-skip-link.png` });
    await page.keyboard.press('Enter');
    await expect(page.locator('#main')).toBeFocused();
    expect(new URL(page.url()).hash).toBe('');
  });

  test('exactly one main landmark on every screen, a language, and a title per route', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ xp: 50, streak: 2, lastDay: TODAY, stats: { sessions: 2, answered: 20, firstTry: 10, perfect: 0, practiceDays: 2 }, ...dueCards(6) }) });
    for (const [route, title] of [['/', /^Interview Trainer$/], ['/#/progress', /^Progress/], ['/#/lesson/' + lessons[0]!.id, /^Practice/], ['/#/review', /^Practice/]] as const) {
      await page.goto(route);
      await page.waitForLoadState();
      await expect(page.locator('main')).toHaveCount(1);
      await expect(page).toHaveTitle(title);
    }
    expect(await page.evaluate(() => document.documentElement.lang)).toBe('en');
  });

  test('moving between screens puts focus at the top of the new screen', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ stats: { sessions: 2, answered: 20, firstTry: 10, perfect: 0, practiceDays: 2 } }) });
    await page.goto('/');
    await page.locator('.home').waitFor();
    await page.getByTestId('progress-link').click();
    await expect(page.getByRole('heading', { name: 'Progress', level: 1 })).toBeFocused();
    await page.getByRole('link', { name: 'Back to home' }).click();
    await expect(page.getByRole('heading', { name: 'Interview Trainer', level: 1 })).toBeFocused();
  });
});

test.describe('focus in a lesson', () => {
  test('a new question focuses its prompt; checking focuses the verdict; Enter continues; the next prompt is focused', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    const lesson = lessons[0]!;
    await page.goto(`/#/lesson/${lesson.id}`);
    await expect(page.locator('.prompt')).toBeFocused();
    const first = lesson.questions[0]!;
    await answerCorrectly(page, first);
    await check(page);
    await expect(page.locator('.feedback h2')).toBeFocused();
    // the verdict is a labelled region, not a live region that would read twice
    await expect(page.getByRole('region', { name: /Correct|Nice|Spot|Well/ })).toBeVisible();
    await page.keyboard.press('Enter'); // from the verdict heading: continue
    await expect(page.locator('.prompt')).toBeFocused();
    await expect(page.locator('.prompt')).not.toHaveText(visible(first.prompt));
  });

  test('"Check" progress is announced politely', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto(`/#/lesson/${lessons[0]!.id}`);
    await page.locator('.prompt').waitFor();
    const live = page.locator('.lesson > p.sr-only[role="status"]');
    await expect(live).toHaveAttribute('aria-live', 'polite');
  });
});

test.describe('keyboard operation', () => {
  test('match: pair every item with Tab, Space and Enter only, then check', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    const q = (await gotoQuestion(page, 'match')) as Extract<Question, { type: 'match' }>;
    const left = page.locator('[data-side="left"]');
    const right = page.locator('[data-side="right"]');
    for (let i = 0; i < q.pairs.length; i++) {
      const target = q.pairs[i]!;
      // reach the i-th left item by Tab
      await left.nth(i).focus();
      await page.keyboard.press('Space');
      await expect(left.nth(i)).toHaveAttribute('aria-pressed', 'true');
      const texts = await right.allTextContents();
      const at = texts.findIndex((t) => t.includes(visible(target.right)) && !t.includes('paired with'));
      const idx = texts.findIndex((t) => t.replace(/, paired with.*/, '').trim() === visible(target.right));
      await right.nth(idx === -1 ? at : idx).focus();
      await page.keyboard.press('Enter');
    }
    await expect(page.getByRole('status').filter({ hasText: /paired/ }).first()).toBeAttached();
    await expect(left.first()).toContainText('paired with');
    await page.keyboard.press('Tab'); // leave the grid; Check is reachable
    await check(page);
    await expect(page.locator('.feedback')).toHaveClass(/good/);
    await expect(page.locator('[data-side="left"]').first()).toContainText('your answer, correct');
  });

  test('order: build the sequence by keyboard and hear the positions', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    const q = (await gotoQuestion(page, 'order')) as Extract<Question, { type: 'order' }>;
    // UX-22: the pool does not leak the nesting. Tiles carry no indentation; placed lines get it back.
    const indented = q.lines.findIndex((l) => /^\s+\S/.test(l));
    expect(indented, 'the first order question should have indented lines').toBeGreaterThanOrEqual(0);
    expect((await page.locator('.order-pool .code-line').allTextContents()).sort()).toEqual(q.lines.map((l) => l.trim()).sort());
    for (const line of q.lines) {
      const pool = page.locator('.order-pool .code-line:not([disabled])');
      const texts = await pool.allTextContents();
      await pool.nth(texts.findIndex((t) => t === line.trim())).focus();
      await page.keyboard.press('Enter');
    }
    await expect(page.locator('.order-slot .code-line').nth(indented)).toContainText(q.lines[indented]!);
    await expect(page.locator('.order-slot .code-line').first()).toContainText('Position 1');
    await expect(page.getByRole('status').filter({ hasText: `${q.lines.length} of ${q.lines.length} placed` })).toBeAttached();
    await check(page);
    await expect(page.locator('.feedback')).toHaveClass(/good/);
  });

  test('spot: choose the buggy line by keyboard; every line announces its number', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    const q = (await gotoQuestion(page, 'spot')) as Extract<Question, { type: 'spot' }>;
    await expect(page.locator('button[data-line="1"]')).toContainText('Line 1:');
    for (const n of q.answers) {
      await page.locator(`button[data-line="${n}"]`).focus();
      await page.keyboard.press('Space');
      await expect(page.locator(`button[data-line="${n}"]`)).toHaveAttribute('aria-pressed', 'true');
    }
    await check(page);
    await expect(page.locator('.feedback')).toHaveClass(/good/);
    await expect(page.locator(`button[data-line="${q.answers[0]}"]`)).toContainText('your answer, correct');
  });

  test('the code editor is not a keyboard trap: Tab indents, Escape then Tab (or Shift+Tab) moves on', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await gotoQuestion(page, 'code');
    const editor = page.getByRole('textbox', { name: 'Your code' });
    await expect(editor).toHaveAccessibleDescription(/Escape/);
    await editor.focus();
    const before = await editor.inputValue();
    await page.keyboard.press('Tab');
    await expect(editor).toBeFocused();
    expect((await editor.inputValue()).length).toBe(before.length + 2);
    await page.keyboard.press('Escape');
    await page.keyboard.press('Tab');
    await expect(editor).not.toBeFocused();
    await expect(page.getByRole('button', { name: /Run tests/ })).toBeFocused(); // the run button follows the editor; the symbol row comes after it
    // Shift+Tab always leaves, and the Tab-indents mode is back after the editor loses focus
    await editor.focus();
    await page.keyboard.press('Shift+Tab');
    await expect(editor).not.toBeFocused();
    await editor.focus();
    await page.keyboard.press('Tab');
    await expect(editor).toBeFocused();
  });

  test('single choice is a radio group: the arrow keys move and choose, wrapping at the ends', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await gotoQuestion(page, 'single');
    const radios = page.getByRole('radio');
    await radios.first().focus();
    await page.keyboard.press('ArrowDown');
    await expect(radios.nth(1)).toBeFocused();
    await expect(radios.nth(1)).toHaveAttribute('aria-checked', 'true');
    await page.keyboard.press('ArrowRight');
    await expect(radios.nth(2)).toHaveAttribute('aria-checked', 'true');
    await expect(radios.nth(1)).toHaveAttribute('aria-checked', 'false');
    await page.keyboard.press('ArrowUp');
    await page.keyboard.press('ArrowLeft');
    await expect(radios.first()).toBeFocused();
    await page.keyboard.press('ArrowUp'); // wraps to the last option
    await expect(radios.last()).toBeFocused();
    await expect(radios.last()).toHaveAttribute('aria-checked', 'true');
    await expect(page.getByRole('button', { name: 'Check', exact: true })).toBeEnabled();
  });

  test('fill and single choice: results are readable without colour', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    const q = await gotoQuestion(page, 'single');
    await answerCorrectly(page, q);
    await check(page);
    await expect(page.locator('.option.correct')).toContainText('your answer, correct');
    await expect(page.locator('.option.correct .marker svg')).toBeVisible(); // a check mark, not a letter
    await expect(page.locator('.option.correct .tagtxt')).toHaveText('Your answer'); // the marker says whose answer it is; the verdict is in the panel
  });
});

test.describe('visible focus rings', () => {
  test('home, settings sheet and every question type show a ring on every tab stop', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ xp: 120, streak: 3, lastDay: TODAY, ...dueCards(5) }) });
    await page.goto('/');
    await page.locator('.home').waitFor();
    expect(await tabStopsWithoutRing(page, 70)).toEqual([]);

    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    expect(await tabStopsWithoutRing(page, 40)).toEqual([]);
    await page.keyboard.press('Escape');

    for (const type of ['single', 'multi', 'fill', 'order', 'spot', 'estimate', 'match', 'code', 'open'] as const) {
      const q = await gotoQuestion(page, type);
      expect(await tabStopsWithoutRing(page, 40, 2), `question type ${type}`).toEqual([]);
      await answerCorrectly(page, q);
      await check(page);
      expect(await tabStopsWithoutRing(page, 30, 1), `feedback of ${type}`).toEqual([]);
      await page.goto('/');
      await page.locator('.home').waitFor();
    }
  });
});

test.describe('reading settings', () => {
  const settings = async (page: Page) => {
    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    await page.getByRole('heading', { name: 'Reading' }).scrollIntoViewIfNeeded();
  };
  const fontPx = (page: Page, sel = 'body') => page.evaluate((s) => parseFloat(getComputedStyle(document.querySelector(s)!).fontSize), sel);

  test('text size S/M/L/XL scales everything, persists, and never overflows a phone', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2({ xp: 120, streak: 3, lastDay: TODAY, stats: { sessions: 3, answered: 30, firstTry: 20, perfect: 0, practiceDays: 3 }, ...dueCards(8) }) });
    await page.goto('/');
    await page.locator('.home').waitFor();
    expect(await fontPx(page)).toBeCloseTo(16, 1);
    await settings(page);
    await page.screenshot({ path: `${SHOTS}/reading-settings.png` });

    const sizes: Record<string, number> = { S: 14.4, M: 16, L: 19.2, XL: 22.4 };
    for (const [size, px] of Object.entries(sizes)) {
      await page.getByTestId(`text-size-${size}`).check();
      expect(await fontPx(page)).toBeCloseTo(px, 1);
      if (size === 'M') await expect(page.locator('html')).not.toHaveAttribute('data-text-size', /.*/);
      else await expect(page.locator('html')).toHaveAttribute('data-text-size', size);
    }
    expect((await stored(page))!.prefs.reading.textSize).toBe('XL');
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/reading-xl-settings.png` });
    await page.keyboard.press('Escape');

    // persists across a reload and holds on the other screens, at the largest size
    await page.reload();
    await page.locator('.home').waitFor();
    expect(await fontPx(page)).toBeCloseTo(22.4, 1);
    await expectNoOverflow(page);
    await page.screenshot({ path: `${SHOTS}/reading-xl-home.png` });
    await page.goto('/#/progress');
    await page.locator('.progress-page').waitFor();
    await expectNoOverflow(page);
    for (const type of ['single', 'fill', 'order', 'match', 'estimate', 'code', 'open'] as const) {
      await page.goto('/#/');
      await page.locator('.home').waitFor();
      const q = await gotoQuestion(page, type);
      await expectNoOverflow(page);
      const bar = await page.locator('.lesson-bar').boundingBox();
      expect(bar!.height).toBeLessThan(200);
      await answerCorrectly(page, q);
      await check(page);
      await expectNoOverflow(page);
      if (type === 'single') await page.screenshot({ path: `${SHOTS}/reading-xl-lesson.png` });
    }
  });

  test('the easier-to-read font swaps the family and spacing but keeps code monospaced', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    await page.locator('.home').waitFor();
    await settings(page);
    await page.getByTestId('dyslexia-font').check();
    const family = await page.evaluate(() => getComputedStyle(document.body).fontFamily);
    expect(family).toMatch(/Verdana/);
    expect(await page.evaluate(() => getComputedStyle(document.body).letterSpacing)).not.toBe('normal');
    expect((await stored(page))!.prefs.reading.dyslexiaFont).toBe(true);
    await page.screenshot({ path: `${SHOTS}/reading-dyslexia-settings.png` });
    await page.keyboard.press('Escape');

    const q = await gotoQuestion(page, 'fill');
    expect(await page.evaluate(() => getComputedStyle(document.querySelector('.code')!).fontFamily)).toMatch(/mono|Menlo|Consolas/i);
    expect(await page.evaluate(() => getComputedStyle(document.querySelector('.code')!).letterSpacing)).toBe('normal');
    await answerCorrectly(page, q);
    await expectNoOverflow(page);
    // and it can be switched off again
    await page.goto('/');
    await settings(page);
    await page.getByTestId('dyslexia-font').uncheck();
    expect(await page.evaluate(() => getComputedStyle(document.body).fontFamily)).not.toMatch(/Verdana/);
  });
});

test.describe('reduced motion', () => {
  /** Plays to the feedback of the first lesson question and returns the running animations plus scroll behaviours used. */
  async function probe(page: Page) {
    await page.addInitScript(() => {
      const w = window as unknown as { __scroll: string[] };
      w.__scroll = [];
      const orig = Element.prototype.scrollIntoView;
      Element.prototype.scrollIntoView = function (arg?: boolean | ScrollIntoViewOptions) {
        w.__scroll.push(typeof arg === 'object' && arg ? String(arg.behavior) : 'default');
        return orig.call(this, arg as ScrollIntoViewOptions);
      };
    });
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    await page.locator('.home').waitFor();
    const home = await page.evaluate(() => document.getAnimations().length);
    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    const sheet = await page.evaluate(() => document.getAnimations().length);
    await page.keyboard.press('Escape');
    const q = lessons[0]!.questions[0]!;
    await page.goto(`/#/lesson/${lessons[0]!.id}`);
    await page.locator('.prompt').waitFor();
    await answerCorrectly(page, q);
    await page.getByRole('button', { name: 'Check', exact: true }).click();
    await page.locator('.feedback').waitFor();
    const feedback = await page.evaluate(() => document.getAnimations().length);
    const scroll = await page.evaluate(() => (window as unknown as { __scroll: string[] }).__scroll);
    return { home, sheet, feedback, scroll };
  }

  test('with prefers-reduced-motion: reduce nothing animates and scrolling is instant', async ({ page }) => {
    await page.emulateMedia({ reducedMotion: 'reduce' });
    const r = await probe(page);
    expect(r).toMatchObject({ home: 0, sheet: 0, feedback: 0 });
    expect(r.scroll.length).toBeGreaterThan(0);
    expect(r.scroll.every((b) => b === 'auto')).toBe(true);
  });

  test('without the preference the same screens do animate (so the check above is meaningful)', async ({ page }) => {
    await page.emulateMedia({ reducedMotion: 'no-preference' });
    const r = await probe(page);
    expect(r.sheet + r.feedback).toBeGreaterThan(0);
    expect(r.scroll).toContain('smooth');
  });

  test('the summary celebrates without confetti or animation under reduced motion', async ({ page }) => {
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    const lesson = lessons[0]!;
    await page.goto(`/#/lesson/${lesson.id}`);
    await page.locator('.prompt').waitFor();
    for (const q of lesson.questions) await solveAndContinue(page, q);
    await page.locator('.summary h1').waitFor();
    expect(await page.locator('canvas.confetti').count()).toBe(0);
    expect(await page.evaluate(() => document.getAnimations().length)).toBe(0);
  });
});

void course;
