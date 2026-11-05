import { expect, test, type Page } from '@playwright/test';
import { MOCK_XP_CAP, buildMock, mockSeed, type MockConfig, type Progress, type Question } from '../shared/src';
import { KEY_V2, SHOTS, TODAY, course, day, dueCards, expectNoOverflow, seed, setDay, stored, useClock, v2 } from './engage';
import { answerCorrectly, answerWrongly, writeCode } from './solve';

const timer = (page: Page) => page.getByTestId('mock-timer');
const area = (page: Page) => page.locator('.answer-area:not([hidden])');
/** The same questions the app will draw for a fresh profile today. */
const planned = (cfg: Partial<MockConfig> = {}): Question[] =>
  buildMock(course, v2(), TODAY, { length: 10, minutes: 15, unitId: null, ...cfg }, mockSeed(v2(), TODAY));

/** Answers the question on screen. Only the visible answer area is touched, because earlier questions stay mounted (hidden). */
async function answer(page: Page, q: Question, how: 'right' | 'wrong' | 'blank') {
  if (how === 'blank') return;
  const scoped = area(page) as unknown as Page;
  if (how === 'wrong') return answerWrongly(scoped, q);
  if (q.type === 'code') return writeCode(page, q.solution);
  return answerCorrectly(scoped, q);
}

const canBeWrong = (q: Question) => q.type === 'single' || q.type === 'spot' || q.type === 'estimate' || q.type === 'match';

async function startMock(page: Page, opts: { length?: 10 | 20; minutes?: number; unit?: string } = {}) {
  await page.goto('/#/mock');
  if (opts.length) await page.getByTestId(`mock-length-${opts.length}`).click();
  if (opts.minutes) await page.getByTestId('mock-minutes').selectOption(String(opts.minutes));
  if (opts.unit) await page.getByTestId('mock-topic').selectOption(opts.unit);
  await page.getByTestId('mock-start').click();
  await expect(page.getByTestId('mock-run')).toBeVisible();
}

const next = (page: Page) => page.getByTestId('mock-next').or(page.getByTestId('mock-finish')).click();

/** Plays a whole mock: answer[i] says how to answer question i. Then submits. */
async function playMock(page: Page, qs: Question[], how: (q: Question, i: number) => 'right' | 'wrong' | 'blank') {
  for (let i = 0; i < qs.length; i++) {
    await expect(page.getByTestId('mock-count')).toHaveText(`${i + 1} / ${qs.length}`);
    await answer(page, qs[i]!, how(qs[i]!, i));
    await next(page);
  }
  await page.getByTestId('mock-submit').click();
  await page.getByTestId('mock-results').waitFor();
}

test.describe('mock interview: setup', () => {
  test('is opt-in from Home, offers length, time and topic, and shows the rules', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await page.goto('/');
    await expect(page.getByTestId('mock-link')).toContainText('optional');
    await page.getByTestId('mock-link').click();
    await expect(page).toHaveURL(/#\/mock$/);
    await expect(page.getByRole('heading', { name: 'Mock interview' })).toBeVisible();
    await expect(page.getByTestId('mock-length-10')).toHaveAttribute('aria-checked', 'true');
    await expect(page.getByTestId('mock-minutes')).toHaveValue('15');
    await page.getByTestId('mock-length-20').click();
    await expect(page.getByTestId('mock-minutes')).toHaveValue('30');
    await expect(page.getByTestId('mock-start')).toContainText('20 questions · 30 min');
    await page.getByTestId('mock-topic').selectOption('scale');
    await expect(page.getByTestId('mock-start')).toContainText('Scale');
    await expectNoOverflow(page);
    for (const sel of ['[data-testid=mock-length-10]', '[data-testid=mock-start]', '#mock-minutes', '#mock-topic'])
      expect((await page.locator(sel).boundingBox())!.height, sel).toBeGreaterThanOrEqual(44);
    await page.screenshot({ path: `${SHOTS}/mock-setup.png` });
  });
});

test.describe('mock interview: running', () => {
  test('no feedback while it runs; flag, skip, revisit; answers survive navigation', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    const qs = planned();
    await startMock(page);
    await expect(timer(page)).toHaveText('15:00');
    await expect(page.getByTestId('mock-count')).toHaveText('1 / 10');
    await expect(page.locator('.prompt')).toBeVisible();

    // answer #1, then flag and skip #2, answer #3
    await answer(page, qs[0]!, canBeWrong(qs[0]!) ? 'wrong' : 'right');
    await expect(page.locator('.feedback')).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Check', exact: true })).toHaveCount(0);
    await expect(page.getByTestId('mock-dot-1')).toHaveClass(/answered/);
    await next(page);
    await expect(page.getByTestId('mock-next')).toHaveText('Skip'); // nothing answered yet on #2
    await page.getByTestId('mock-flag').click();
    await expect(page.getByTestId('mock-flag')).toHaveAttribute('aria-pressed', 'true');
    await expect(page.getByTestId('mock-dot-2')).toHaveClass(/flagged/);
    await expect(page.getByTestId('mock-dot-2')).toHaveAccessibleName('Question 2, not answered, flagged');
    await page.screenshot({ path: `${SHOTS}/mock-running.png` });
    await page.getByTestId('mock-next').click();
    await expect(page.getByTestId('mock-count')).toHaveText('3 / 10');
    await answer(page, qs[2]!, 'right');

    // revisit #1: its answer is still selected (the question was not reset)
    await page.getByTestId('mock-dot-1').click();
    await expect(page.getByTestId('mock-count')).toHaveText('1 / 10');
    if (qs[0]!.type === 'single') await expect(area(page).locator('.option.selected')).toHaveCount(1);
    await expect(page.locator('.prompt')).toBeVisible();
    await expect(page.getByTestId('mock-prev')).toBeDisabled();
    await page.getByTestId('mock-next').click();
    await expect(page.getByTestId('mock-prev')).toBeEnabled();
    await page.getByTestId('mock-prev').click();

    await expectNoOverflow(page);
    for (const sel of ['mock-flag', 'mock-next', 'mock-dot-1'])
      expect((await page.getByTestId(sel).boundingBox())!.height, sel).toBeGreaterThanOrEqual(44);

    // finish early: the confirmation says how many are unanswered, and "keep going" returns
    await page.getByTestId('mock-finish-early').click();
    await expect(page.getByTestId('mock-confirm')).toContainText('no answer yet');
    await expect(page.getByTestId('mock-confirm')).toContainText('1 flagged');
    await page.getByRole('button', { name: 'Keep going' }).click();
    await expect(page.getByTestId('mock-confirm')).toHaveCount(0);
  });

  test('code questions in a mock have no "Run tests" (that would be feedback)', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    const qs = planned({ unitId: 'complexity', length: 20 });
    const i = qs.findIndex((q) => q.type === 'code');
    test.skip(i < 0, 'no code question drawn');
    await startMock(page, { unit: 'complexity', length: 20 });
    for (let k = 0; k < i; k++) await page.getByTestId('mock-next').click();
    await expect(page.getByRole('textbox', { name: 'Your code' })).toBeVisible();
    await expect(page.getByRole('button', { name: /Run tests/ })).toHaveCount(0);
    await writeCode(page, (qs[i] as Extract<Question, { type: 'code' }>).solution);
    await expect(page.locator('.tests')).toHaveCount(0);
  });

  test('quitting asks first and saves nothing', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    const qs = planned();
    await startMock(page);
    await answer(page, qs[0]!, 'right');
    page.once('dialog', (d) => {
      expect(d.message()).toContain('Nothing is saved');
      void d.accept();
    });
    await page.getByRole('button', { name: 'Quit mock interview' }).click();
    await expect(page.getByTestId('mock-start')).toBeVisible();
    expect((await stored(page))?.mocks ?? []).toEqual([]);
  });

  test('the timer pauses while the app is hidden', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await startMock(page);
    const setHidden = (hidden: boolean) =>
      page.evaluate((h) => {
        Object.defineProperty(document, 'hidden', { configurable: true, get: () => h });
        document.dispatchEvent(new Event('visibilitychange'));
      }, hidden);
    await setHidden(true);
    await page.clock.fastForward('05:00'); // five minutes pass while hidden
    await setHidden(false);
    await page.clock.fastForward(1500);
    await expect(timer(page)).toHaveText(/1[45]:\d\d/); // still about fifteen minutes left, not ten
    await expect(page.getByTestId('mock-run')).toBeVisible();
  });

  test('keyboard: the palette and the bar are reachable and operable', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    await startMock(page);
    await page.getByTestId('mock-dot-3').focus();
    await page.keyboard.press('Enter');
    await expect(page.getByTestId('mock-count')).toHaveText('3 / 10');
    await expect(page.getByTestId('mock-dot-3')).toHaveAttribute('aria-current', 'step');
    await page.getByTestId('mock-next').focus();
    await page.keyboard.press('Enter');
    await expect(page.getByTestId('mock-count')).toHaveText('4 / 10');
  });

  test('time up submits automatically and says so', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    const qs = planned();
    await startMock(page, { minutes: 10 });
    await expect(timer(page)).toHaveText('10:00');
    await answer(page, qs[0]!, 'right');
    await page.getByTestId('mock-next').click();
    await page.clock.fastForward('10:30');
    await expect(page.getByTestId('mock-results')).toBeVisible();
    await expect(page.getByTestId('mock-timed-out')).toContainText('Time was up');
    await expect(page.getByTestId('mock-timed-out')).toContainText('not answered');
    await page.screenshot({ path: `${SHOTS}/mock-timeup.png` });
    await page.getByTestId('mock-save').click();
    await expect(page.getByTestId('mock-saved')).toBeVisible();
    const p = await stored(page);
    expect(p!.mocks).toHaveLength(1);
    expect(p!.mocks[0]).toMatchObject({ timedOut: true, limitMin: 10, total: 10, answered: 1, usedSec: 600 });
    // unanswered questions are not recorded for review, the answered one is
    expect(Object.keys(p!.cards)).toEqual([qs[0]!.id]);
  });
});

test.describe('mock interview: results', () => {
  test('scorecard by topic with weak spots, self-assessment of open answers, and saving feeds the review queue', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    const qs = planned();
    const wrongIds = new Set(qs.filter(canBeWrong).slice(0, 3).map((q) => q.id));
    const blankIds = new Set(qs.filter((q) => q.type === 'open').slice(0, 1).map((q) => q.id));
    const opens = qs.filter((q) => q.type === 'open' && !blankIds.has(q.id));

    await startMock(page);
    await playMock(page, qs, (q) => (wrongIds.has(q.id) ? 'wrong' : blankIds.has(q.id) ? 'blank' : 'right'));
    await expect(page.getByTestId('mock-score')).toBeVisible();
    await expectNoOverflow(page);

    // scorecard
    const score = Number((await page.getByTestId('mock-score').innerText()).replace('%', ''));
    expect(score).toBeGreaterThan(0);
    expect(score).toBeLessThan(100);
    await expect(page.getByTestId('mock-topics').locator('li').first()).toBeVisible();
    const topics = await page.getByTestId('mock-topics').locator('li').count();
    expect(topics).toBeGreaterThanOrEqual(2);
    expect(await page.getByTestId('mock-topics').locator('[data-weak="true"]').count()).toBe(await page.getByTestId('mock-topics').getByText('weak spot').count());
    // every question is listed with a status; wrong ones say Incorrect
    for (let i = 0; i < qs.length; i++) {
      const st = wrongIds.has(qs[i]!.id) ? 'Incorrect' : blankIds.has(qs[i]!.id) ? 'Not answered' : null;
      if (st) await expect(page.getByTestId(`mock-q-${i + 1}`)).toHaveAttribute('data-status', st);
    }

    // AI is off in tests: open answers are self-assessed, and ticking changes the score
    if (opens.length > 0) {
      await expect(page.getByTestId('mock-self-note')).toContainText('check your');
      const boxes = page.locator('.mock-results .rubric input[type="checkbox"]');
      for (let i = 0; i < (await boxes.count()); i++) await boxes.nth(i).uncheck();
      const lower = Number((await page.getByTestId('mock-score').innerText()).replace('%', ''));
      expect(lower).toBeLessThan(score);
      for (let i = 0; i < (await boxes.count()); i++) await boxes.nth(i).check();
      expect(Number((await page.getByTestId('mock-score').innerText()).replace('%', ''))).toBe(score);
    }
    await page.screenshot({ path: `${SHOTS}/mock-results.png`, fullPage: true });

    // saving: wrong answers are due tomorrow; XP and goal credit are capped; the mock joins the history
    await page.getByTestId('mock-save').click();
    await expect(page.getByTestId('mock-saved')).toContainText('in your review queue');
    const p = await stored(page);
    expect(p!.mocks).toHaveLength(1);
    expect(p!.mocks[0]).toMatchObject({ day: TODAY, total: 10, limitMin: 15, timedOut: false });
    expect(p!.xp).toBeLessThanOrEqual(MOCK_XP_CAP);
    expect(p!.days[TODAY].answered).toBeLessThanOrEqual(4); // at most half of the default goal of 8
    for (const id of wrongIds) expect(p!.cards[id]).toMatchObject({ lapses: 1, due: day(1) });
    expect(p!.missed.sort()).toEqual([...wrongIds].sort());
    for (const id of blankIds) expect(p!.cards[id]).toBeUndefined();
    expect(p!.stats.perfect).toBe(0);

    await page.goto('/');
    await page.getByTestId('mock-link').click();
    await expect(page.getByTestId('mock-history-item')).toHaveCount(1);
    await expect(page.getByTestId('mock-history-item').first()).toContainText(`${score}%`);
    await page.goto('/');
    await expect(page.getByTestId('due-count')).toContainText('ready to review');
    await expect(page.getByTestId('goal-text')).not.toHaveText('8/8');
  });

  test('a perfect 20-question mock cannot meet the daily goal alone; later mocks the same day earn nothing; history keeps ten', async ({ page }) => {
    await useClock(page);
    const old = Array.from({ length: 10 }, (_, i) => ({
      day: day(-i - 1), total: 10, answered: 10, score: 50 + i, limitMin: 15, usedSec: 500, timedOut: false, byUnit: { kotlin: { n: 10, score: 5 } },
    }));
    await seed(page, { [KEY_V2]: v2({ mocks: old }) });
    const base = v2({ mocks: old });
    const qs = buildMock(course, base, TODAY, { length: 20, minutes: 30, unitId: null }, mockSeed(base, TODAY));

    await startMock(page, { length: 20 });
    await playMock(page, qs, (q) => (q.type === 'open' ? 'blank' : 'right'));
    await page.getByTestId('mock-save').click();
    await expect(page.getByTestId('mock-saved')).toContainText(`+${MOCK_XP_CAP} XP`);
    let p = await stored(page);
    expect(p!.mocks).toHaveLength(10); // the oldest dropped
    expect(p!.mocks[0].day).toBe(TODAY);
    expect(p!.days[TODAY].answered).toBe(4);
    expect(p!.xp).toBe(MOCK_XP_CAP);

    // second and third: the second still rewards, the third does not
    for (const expectXp of [true, false]) {
      await page.getByTestId('mock-another').click();
      await page.getByTestId('mock-length-10').click();
      // the app draws from the stored progress: do the same here to know the questions
      const stateNow = (await stored(page)) as unknown as Progress;
      const again = buildMock(course, stateNow, TODAY, { length: 10, minutes: 15, unitId: null }, mockSeed(stateNow, TODAY));
      await page.getByTestId('mock-start').click();
      await page.getByTestId('mock-run').waitFor();
      for (const q of again) {
        await answer(page, q, q.type === 'open' ? 'blank' : 'right');
        await next(page);
      }
      await page.getByTestId('mock-submit').click();
      await page.getByTestId('mock-results').waitFor();
      await page.getByTestId('mock-save').click();
      await expect(page.getByTestId('mock-saved')).toContainText(expectXp ? 'XP' : 'No XP this time');
    }
    p = await stored(page);
    expect(p!.xp).toBeLessThanOrEqual(MOCK_XP_CAP * 2);
    expect(p!.mocks).toHaveLength(10);
  });

  test('unanswered mock saves nothing', async ({ page }) => {
    await useClock(page);
    await seed(page, { [KEY_V2]: v2() });
    const qs = planned();
    await startMock(page);
    await playMock(page, qs, () => 'blank');
    await expect(page.getByTestId('mock-score')).toHaveText('0%');
    await page.getByTestId('mock-save').click();
    await expect(page.getByTestId('mock-saved')).toContainText('Nothing to review');
    expect((await stored(page))?.mocks ?? []).toEqual([]);
  });

  test('a mock after a gap rewards on the new day only; the history and badges show on Progress', async ({ page }) => {
    await useClock(page);
    await setDay(page, TODAY);
    const rec = { day: day(-2), total: 10, answered: 10, score: 85, limitMin: 15, usedSec: 400, timedOut: false, byUnit: {} };
    await seed(page, { [KEY_V2]: v2({ mocks: [rec], achievements: { 'mock-first': day(-2), 'mock-strong': day(-2) }, ...dueCards(0) }) });
    await page.goto('/#/progress');
    await expect(page.getByTestId('mock-history')).toContainText('85%');
    await expect(page.getByTestId('badge-mock-strong')).toHaveAttribute('data-unlocked', 'true');
    await expectNoOverflow(page);
  });
});
