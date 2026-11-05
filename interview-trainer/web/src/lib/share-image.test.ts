/**
 * The share card drawing against a recording fake of CanvasRenderingContext2D: what text ends up on the image
 * (and, as important, what does not), the text-fitting loop, and the null results when canvas is unavailable.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { ShareCard } from '@trainer/shared';
import { CARD_H, CARD_W, drawShareCard, renderShareCard } from './share-image';

const card: ShareCard = {
  streak: 12,
  rank: 'Senior',
  readinessScore: 64,
  readinessLabel: 'Getting there',
  coveragePct: 40,
  strongest: { title: 'Kotlin', pct: 90 },
  weakest: { title: 'Concurrency and coroutines in depth', pct: 30 },
  alt: 'alt',
  text: 'text',
};

function fakeCtx(charWidth = 10) {
  const texts: string[] = [];
  const rects: unknown[][] = [];
  const ctx = {
    fillStyle: '', strokeStyle: '', lineWidth: 0, font: '', textAlign: 'left', textBaseline: '',
    fillRect: (...a: unknown[]) => void rects.push(a),
    fillText: (t: string) => void texts.push(t),
    measureText: (t: string) => ({ width: t.length * charWidth }),
    beginPath: () => {}, roundRect: (...a: unknown[]) => void rects.push(a), fill: () => {}, stroke: () => {},
  };
  return { ctx, texts, rects };
}
const canvasWith = (ctx: unknown) => ({ width: 0, height: 0, getContext: () => ctx }) as unknown as HTMLCanvasElement;

afterEach(() => vi.unstubAllGlobals());

describe('drawShareCard', () => {
  it('sizes the canvas and draws only the numbers and names the Progress screen already shows', () => {
    const { ctx, texts } = fakeCtx();
    const canvas = canvasWith(ctx);
    drawShareCard(canvas, card);
    expect([canvas.width, canvas.height]).toEqual([CARD_W, CARD_H]);
    expect(texts).toEqual(expect.arrayContaining(['Interview Trainer', '12', 'day streak', 'Senior', '64', 'Getting there', '40% of the course seen', 'Strongest topic', '90%', 'Kotlin', 'Weakest topic', '30%']));
    expect(texts.some((t) => /estimate of recall/.test(t))).toBe(true);
    // Nothing else (dates, roles, question text) can sneak in: every drawn string is accounted for.
    for (const t of texts) expect(t).toMatch(/^(Interview Trainer|\d+|day streak|Senior|Rank|Readiness|Getting there|\d+% of the course seen|Strongest topic|Weakest topic|Topic|\d+%|Kotlin|.*…|Concurrency.*|Readiness is an estimate.*)$/);
  });

  it('labels a single started topic just "Topic" and draws no second row', () => {
    const { ctx, texts } = fakeCtx();
    drawShareCard(canvasWith(ctx), { ...card, strongest: null, weakest: { title: 'Only', pct: 10 } });
    expect(texts.filter((t) => t === 'Topic')).toHaveLength(1);
    expect(texts).not.toContain('Strongest topic');
    expect(texts).not.toContain('Weakest topic');
  });

  it('draws no topic rows at all when nothing was started', () => {
    const { ctx, texts } = fakeCtx();
    drawShareCard(canvasWith(ctx), { ...card, strongest: null, weakest: null });
    expect(texts.some((t) => /topic|Topic/.test(t))).toBe(false);
  });

  it('shortens a title that does not fit with an ellipsis, and keeps one that does', () => {
    const { ctx, texts } = fakeCtx(40); // wide characters force truncation
    drawShareCard(canvasWith(ctx), card);
    const shown = texts.find((t) => t.startsWith('Concurrency'))!;
    expect(shown.endsWith('…')).toBe(true);
    expect(shown.length).toBeLessThan(card.weakest!.title.length);
    const narrow = fakeCtx(1);
    drawShareCard(canvasWith(narrow.ctx), card);
    expect(narrow.texts).toContain(card.weakest!.title);
  });

  it('keeps the progress bar visible for tiny percentages (minimum width)', () => {
    const { ctx, rects } = fakeCtx();
    drawShareCard(canvasWith(ctx), { ...card, strongest: null, weakest: { title: 'T', pct: 0 } });
    const widths = rects.filter((r) => r.length === 5).map((r) => r[2] as number);
    expect(widths).toContain(22);
  });

  it('uses the CSS colour tokens when present and falls back to defaults when styles are unavailable', () => {
    const a = fakeCtx();
    vi.stubGlobal('document', { documentElement: {} });
    vi.stubGlobal('getComputedStyle', () => ({ getPropertyValue: (n: string) => (n === '--bg' ? ' #123456 ' : '') }));
    drawShareCard(canvasWith(a.ctx), card);
    vi.stubGlobal('getComputedStyle', () => { throw new Error('no styles'); });
    expect(() => drawShareCard(canvasWith(fakeCtx().ctx), card)).not.toThrow();
  });

  it('throws a clear error when there is no 2d context', () => {
    expect(() => drawShareCard(canvasWith(null), card)).toThrow('no canvas');
  });
});

describe('renderShareCard', () => {
  it('resolves with the PNG blob from toBlob', async () => {
    const blob = new Blob(['png'], { type: 'image/png' });
    const canvas = { ...canvasWith(fakeCtx().ctx), toBlob: (cb: (b: Blob | null) => void, type: string) => { expect(type).toBe('image/png'); cb(blob); } };
    vi.stubGlobal('document', { createElement: () => canvas, documentElement: {} });
    vi.stubGlobal('getComputedStyle', () => ({ getPropertyValue: () => '' }));
    expect(await renderShareCard(card)).toBe(blob);
  });

  it('resolves null when the browser cannot encode, or when there is no canvas at all', async () => {
    const canvas = { ...canvasWith(fakeCtx().ctx), toBlob: (cb: (b: Blob | null) => void) => cb(null) };
    vi.stubGlobal('document', { createElement: () => canvas, documentElement: {} });
    vi.stubGlobal('getComputedStyle', () => ({ getPropertyValue: () => '' }));
    expect(await renderShareCard(card)).toBeNull();
    vi.stubGlobal('document', { createElement: () => canvasWith(null), documentElement: {} });
    expect(await renderShareCard(card)).toBeNull();
    vi.stubGlobal('document', undefined);
    expect(await renderShareCard(card)).toBeNull();
  });
});
