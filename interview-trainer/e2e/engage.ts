import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, type BrowserContext, type Page } from '@playwright/test';
import {
  addDays, allQuestions, emptyProgress, questionHash, unitOfFor, type Card, type Progress, type Question,
} from '../shared/src';
import { loadCourse } from '../shared/src/load-content';
import { solveAndContinue, visible } from './solve';

const here = dirname(fileURLToPath(import.meta.url));
export const course = loadCourse(resolve(here, '../content'));
export const questions = allQuestions(course);
export const lessons = course.units.flatMap((u) => u.lessons);
export const unitOf = unitOfFor(course);
export const SHOTS = resolve(here, 'screenshots');

export const KEY_V1 = 'interview-trainer.progress.v1';
export const KEY_V2 = 'interview-trainer.progress.v2';

/** The pretend "now" for deterministic tests: Wednesday 2026-11-04, 10:00 local time. */
export const NOW = new Date(2026, 10, 4, 10, 0, 0);
export const TODAY = '2026-11-04';
export const day = (offset: number) => addDays(TODAY, offset);

/** Freezes the page's clock at NOW (time still flows) so day boundaries are under the test's control. */
export async function useClock(page: Page, at: Date = NOW): Promise<void> {
  await page.clock.install({ time: at });
}

/** Moves the page's clock to local noon on `dayKey`. */
export async function setDay(page: Page, dayKey: string): Promise<void> {
  const [y, m, d] = dayKey.split('-').map(Number) as [number, number, number];
  await page.clock.setSystemTime(new Date(y, m - 1, d, 10, 0, 0));
}

/** Writes localStorage entries before the app loads. Only once per context, so reloads keep what the app saved. */
export async function seed(target: Page | BrowserContext, entries: Record<string, unknown>): Promise<void> {
  const flat = Object.fromEntries(Object.entries(entries).map(([k, v]) => [k, typeof v === 'string' ? v : JSON.stringify(v)]));
  await target.addInitScript((e: Record<string, string>) => {
    if (localStorage.getItem('__seeded')) return;
    for (const [k, v] of Object.entries(e)) localStorage.setItem(k, v);
    localStorage.setItem('__seeded', '1');
  }, flat);
}

/** A complete v2 progress, onboarded by default so the setup card does not get in the way. */
export function v2(over: Partial<Progress> = {}): Progress {
  const base = emptyProgress();
  return { ...base, profile: { ...base.profile, onboarded: true }, ...over };
}

export function cardFor(q: Question, over: Partial<Card> = {}): Card {
  return { h: questionHash(q), ease: 2.3, interval: 3, due: day(-1), reps: 1, lapses: 0, seen: 1, firstTry: 1, last: day(-4), ...over };
}

/** `n` due cards spread over as many units as possible. */
export function dueCards(n: number, over: Partial<Card> = {}): { cards: Progress['cards']; ids: string[] } {
  const byUnit = new Map<string, Question[]>();
  for (const q of questions) byUnit.set(unitOf(q), [...(byUnit.get(unitOf(q)) ?? []), q]);
  const pools = [...byUnit.values()];
  const picked: Question[] = [];
  for (let i = 0; picked.length < n; i++) for (const pool of pools) if (pool[i] && picked.length < n) picked.push(pool[i]!);
  return { cards: Object.fromEntries(picked.map((q) => [q.id, cardFor(q, over)])), ids: picked.map((q) => q.id) };
}

/** Collects anything that would show up in the browser console as an error. */
export function watchErrors(page: Page): string[] {
  const errors: string[] = [];
  page.on('pageerror', (e) => errors.push(`pageerror: ${e.message}`));
  page.on('console', (m) => {
    if (m.type() === 'error') errors.push(`console: ${m.text()}`);
  });
  return errors;
}

/** The question on screen, found by its prompt (a follow-up prompt is not in the course, so undefined). */
export async function currentQuestion(page: Page): Promise<Question | undefined> {
  await page.locator('.prompt').waitFor();
  const shown = (await page.locator('.prompt').innerText()).replace(/\s+/g, ' ').trim();
  return questions.find((q) => visible(q.prompt).replace(/\s+/g, ' ') === shown);
}

/** Answers every question of a session correctly, whatever order the app chose. Returns the questions in order. */
export async function playSession(page: Page, count: number): Promise<Question[]> {
  const seen: Question[] = [];
  for (let i = 0; i < count; i++) {
    const q = await currentQuestion(page);
    if (!q) throw new Error('could not match the question on screen to the course');
    seen.push(q);
    await solveAndContinue(page, q);
  }
  return seen;
}

export const playLesson = async (page: Page, lessonId: string): Promise<void> => {
  const lesson = lessons.find((l) => l.id === lessonId)!;
  await page.goto(`/#/lesson/${lessonId}`);
  await page.locator('.prompt').waitFor();
  for (const q of lesson.questions) await solveAndContinue(page, q);
};

export const homeAfterSummary = async (page: Page): Promise<void> => {
  await page.getByRole('button', { name: 'Continue', exact: true }).click();
  await page.locator('.home').waitFor();
};

export async function expectNoOverflow(page: Page): Promise<void> {
  const { scrollWidth, clientWidth } = await page.evaluate(() => ({
    scrollWidth: document.documentElement.scrollWidth,
    clientWidth: document.documentElement.clientWidth,
  }));
  expect(scrollWidth, 'page is wider than the viewport').toBeLessThanOrEqual(clientWidth);
}

export const dueCount = (page: Page) => page.getByTestId('due-count');
export const stored = (page: Page, key = KEY_V2) => page.evaluate((k) => JSON.parse(localStorage.getItem(k) ?? 'null') as Record<string, any> | null, key);
