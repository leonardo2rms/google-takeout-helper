import { expect, test, type Page } from '@playwright/test';
import { SHOTS, questions } from './engage';
import { answerCorrectly, check } from './solve';

/**
 * Layout guards for content the stylesheet has to cope with: a class of bugs that unit tests and axe cannot see.
 * The redesign once broke every match item that mixed prose and inline code into single letters ("Rec / eive / r is",
 * a vertical "t h i s"): the text nodes and the <code> became separate flex items that shrank to one character each.
 */

interface Broken {
  item: string;
  words: string[];
  lines: number;
  overflow: boolean;
}

/** For every match item: no word of up to 14 characters is split across lines, the item is not absurdly tall and nothing overflows its box. */
async function brokenItems(page: Page): Promise<Broken[]> {
  return page.evaluate(() => {
    const out: { item: string; words: string[]; lines: number; overflow: boolean }[] = [];
    for (const item of Array.from(document.querySelectorAll<HTMLElement>('.match-item'))) {
      const box = item.querySelector<HTMLElement>('.match-text') ?? item;
      const words: string[] = [];
      const walker = document.createTreeWalker(box, NodeFilter.SHOW_TEXT);
      for (let n = walker.nextNode(); n; n = walker.nextNode()) {
        const re = /[^\s\-/]+/g; // a hyphen or slash is a legitimate place to break
        for (let m = re.exec(n.nodeValue ?? ''); m; m = re.exec(n.nodeValue ?? '')) {
          if (m[0].length > 14) continue; // a genuinely long identifier may wrap
          const r = document.createRange();
          r.setStart(n, m.index);
          r.setEnd(n, m.index + m[0].length);
          const rows = new Set(Array.from(r.getClientRects()).map((c) => Math.round(c.top)));
          if (rows.size > 1) words.push(m[0]);
        }
      }
      const lineHeight = parseFloat(getComputedStyle(box).lineHeight) || 20;
      const lines = Math.round(box.getBoundingClientRect().height / lineHeight);
      const overflow = item.scrollWidth > item.clientWidth + 1;
      out.push({ item: (box.textContent ?? '').slice(0, 50), words, lines, overflow });
    }
    return out.filter((b) => b.words.length > 0 || b.overflow || b.lines > 9);
  });
}

const matchQuestions = questions.filter((q) => q.type === 'match');

for (const width of [390, 320]) {
  test.describe(`match items at ${width}px`, () => {
    test.use({ viewport: { width, height: 800 } });

    test(`all ${matchQuestions.length} match questions keep words whole (empty, paired, checked)`, async ({ page }) => {
      for (const q of matchQuestions) {
        await page.goto(`/#/q/${q.id}`);
        await page.locator('.match-item').first().waitFor();
        expect(await brokenItems(page), `${q.id} empty`).toEqual([]);
        await answerCorrectly(page, q);
        expect(await brokenItems(page), `${q.id} paired`).toEqual([]);
        await check(page);
        expect(await brokenItems(page), `${q.id} checked`).toEqual([]);
      }
    });
  });
}

test.describe('the match board', () => {
  test('a question with inline code is readable (screenshots); phones stack the columns, wider screens use two', async ({ page }) => {
    const q = matchQuestions.find((m) => m.pairs.some((p) => p.right.includes('`'))) ?? matchQuestions[0]!;
    const columns = () => page.locator('.match-col').evaluateAll((els) => new Set(els.map((e) => Math.round(e.getBoundingClientRect().left))).size);
    await page.goto(`/#/q/${q.id}`);
    for (const [width, height, cols] of [[390, 800, 1], [320, 760, 1], [700, 800, 2]] as const) {
      await page.setViewportSize({ width, height });
      await page.locator('.match-item').first().waitFor();
      expect(await columns(), `${width}px`).toBe(cols);
      expect(await brokenItems(page), `${width}px`).toEqual([]);
      if (width !== 700) await page.screenshot({ path: `${SHOTS}/match-inline-code-${width}.png` });
    }
  });
});
