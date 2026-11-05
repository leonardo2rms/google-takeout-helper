/**
 * The share card drawing against a recording fake of CanvasRenderingContext2D: what text ends up on the image
 * (and, as important, what does not), the text-fitting loop, and the null results when canvas is unavailable.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
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

/** The light "drafting sheet" palette the card is always drawn in (see C in share-image.ts and docs/design-system.md). */
const PALETTE = { paper: '#F2F4F6', ink: '#0F1B26', graphite: '#4A5967', hairline: '#D3D9DF' };

interface Text { text: string; font: string; fill: string; align: string; y: number }
interface Rect { x: number; y: number; w: number; h: number; fill: string }

/** A recording fake of CanvasRenderingContext2D (only what the card uses). */
function fakeCtx(charWidth = 10) {
  const raw: Text[] = [];
  const rects: Rect[] = [];
  const fills = new Set<string>();
  const ctx = {
    fillStyle: '', strokeStyle: '', lineWidth: 0, font: '', textAlign: 'left', textBaseline: '', lineCap: '', lineJoin: '',
    fillRect(x: number, y: number, w: number, h: number) { rects.push({ x, y, w, h, fill: this.fillStyle }); fills.add(this.fillStyle); },
    strokeRect() { fills.add(this.strokeStyle); },
    fillText(text: string, _x: number, y: number) { raw.push({ text, font: this.font, fill: this.fillStyle, align: this.textAlign, y }); fills.add(this.fillStyle); },
    measureText: (t: string) => ({ width: t.length * charWidth }),
    beginPath() {}, roundRect() {}, fill() { fills.add(this.fillStyle); }, stroke() { fills.add(this.strokeStyle); },
    save() {}, restore() {}, translate() {}, scale() {},
  };
  // Letter-spaced labels are drawn glyph by glyph on one baseline; join those runs back into the words a reader sees.
  const drawn = (): Text[] => {
    const out: (Text & { run?: boolean })[] = [];
    for (const t of raw) {
      const last = out[out.length - 1];
      if (t.text.length === 1 && last?.run && last.font === t.font && last.y === t.y && last.fill === t.fill) last.text += t.text;
      else out.push({ ...t, run: t.text.length === 1 });
    }
    return out;
  };
  return { ctx, rects, fills, get drawn() { return drawn(); }, get texts() { return drawn().map((t) => t.text); } };
}
const canvasWith = (ctx: unknown) => ({ width: 0, height: 0, getContext: () => ctx }) as unknown as HTMLCanvasElement;

/** Path2D is a browser global; the drawing code only constructs it and hands it to ctx.stroke. */
const stubPath2D = () => vi.stubGlobal('Path2D', class { constructor(public d?: string) {} });

beforeEach(stubPath2D);
afterEach(() => vi.unstubAllGlobals());

/** The tick gauges: 20 rects of height 20, filled ink (on) or hairline (off). */
const gaugeTicks = (rects: Rect[]) => rects.filter((r) => r.h === 20);
const litTicks = (rects: Rect[]) => gaugeTicks(rects).filter((r) => r.fill === PALETTE.ink).length;

/** Draws `c` (default: the main card) on a fresh fake and returns the recording. */
function draw(c: ShareCard = card, charWidth = 10) {
  const f = fakeCtx(charWidth);
  drawShareCard(canvasWith(f.ctx), c);
  return { texts: f.texts, drawn: f.drawn, rects: f.rects, fills: f.fills };
}

describe('drawShareCard', () => {
  it('sizes the canvas and draws only the numbers and names the Progress screen already shows', () => {
    const canvas = canvasWith(fakeCtx().ctx);
    drawShareCard(canvas, card);
    expect([canvas.width, canvas.height]).toEqual([CARD_W, CARD_H]);
    const { texts } = draw();
    expect(texts).toEqual(expect.arrayContaining(['Interview Trainer', 'PROGRESS', 'STREAK', '12', 'days', 'RANK', 'Senior', 'READINESS', '64  Getting there', 'COURSE SEEN', '40%', 'STRONGEST TOPIC', '90%', 'Kotlin', 'WEAKEST TOPIC', '30%']));
    expect(texts.some((t) => /estimate of recall/.test(t))).toBe(true);
    // Nothing else (dates, roles, question text) can sneak in: every drawn string is accounted for.
    for (const t of texts) expect(t).toMatch(/^(Interview Trainer|PROGRESS|STREAK|\d+|days?|RANK|Senior|READINESS|\d+ {2}Getting there|COURSE SEEN|\d+%|STRONGEST TOPIC|WEAKEST TOPIC|TOPIC|Kotlin|.*…|Concurrency.*|Readiness is an estimate.*)$/);
  });

  it('says "day" for a streak of one and "days" otherwise', () => {
    const one = draw({ ...card, streak: 1 }).texts;
    expect(one).toContain('day');
    expect(one).not.toContain('days');
    expect(draw({ ...card, streak: 0 }).texts).toContain('days');
  });

  it('labels a single started topic just "Topic" and draws no second row', () => {
    const { texts, rects } = draw({ ...card, strongest: null, weakest: { title: 'Only', pct: 10 } });
    expect(texts.filter((t) => t === 'TOPIC')).toHaveLength(1);
    expect(texts).not.toContain('STRONGEST TOPIC');
    expect(texts).not.toContain('WEAKEST TOPIC');
    expect(texts).toContain('Only');
    expect(gaugeTicks(rects)).toHaveLength(20); // one gauge, not two
    // the same holds the other way round (only a strongest topic)
    expect(draw({ ...card, strongest: { title: 'Solo', pct: 80 }, weakest: null }).texts.filter((t) => t === 'TOPIC')).toHaveLength(1);
  });

  it('draws no topic rows at all when nothing was started', () => {
    const { texts, rects } = draw({ ...card, strongest: null, weakest: null });
    expect(texts.some((t) => /topic/i.test(t))).toBe(false);
    expect(gaugeTicks(rects)).toHaveLength(0);
  });

  it('shortens a title that does not fit with an ellipsis, and keeps one that does', () => {
    const shown = draw(card, 40).texts.find((t) => t.startsWith('Concurrency'))!; // wide characters force truncation
    expect(shown.endsWith('…')).toBe(true);
    expect(shown.length).toBeLessThan(card.weakest!.title.length);
    expect(draw(card, 1).texts).toContain(card.weakest!.title);
  });

  it('keeps the progress gauge visible for tiny percentages (at least one lit tick), and empty only at zero', () => {
    const lit = (pct: number) => litTicks(draw({ ...card, strongest: null, weakest: { title: 'T', pct } }).rects);
    expect(lit(1)).toBe(1);
    expect(lit(2)).toBe(1);
    expect(lit(0)).toBe(0);
    expect(lit(50)).toBe(10);
    expect(lit(100)).toBe(20);
    expect(lit(250)).toBe(20); // out-of-range values are clamped
  });

  it('draws only in the fixed light palette, whatever theme the app is in (it never reads the live styles)', () => {
    const spy = vi.fn(() => ({ getPropertyValue: () => '#123456' }));
    vi.stubGlobal('document', { documentElement: {} });
    vi.stubGlobal('getComputedStyle', spy);
    const { drawn, rects, fills } = draw();
    expect(spy).not.toHaveBeenCalled();
    const allowed = new Set([...Object.values(PALETTE), '#FBFCFD', '#7B8996', '#2447C4'].map((c) => c.toLowerCase()));
    for (const c of fills) expect(allowed.has(c.toLowerCase())).toBe(true);
    // The sheet itself is paper-coloured, the title is ink and the small labels are graphite.
    expect(rects[0]).toMatchObject({ x: 0, y: 0, w: CARD_W, h: CARD_H, fill: PALETTE.paper });
    expect(drawn.find((t) => t.text === 'Interview Trainer')!.fill).toBe(PALETTE.ink);
    expect(drawn.find((t) => t.text === 'STREAK')!.fill).toBe(PALETTE.graphite);
    // Unavailable styles are no problem either.
    vi.stubGlobal('getComputedStyle', () => { throw new Error('no styles'); });
    expect(() => draw()).not.toThrow();
  });

  it('uses the Plex Sans face (with system fallbacks) for text and the system mono for labels and numbers', () => {
    const { drawn } = draw();
    expect(drawn.find((t) => t.text === 'Interview Trainer')!.font).toMatch(/^600 52px "Plex Sans Var", .*sans-serif$/);
    expect(drawn.find((t) => t.text === 'STREAK')!.font).toMatch(/^500 26px ui-monospace, .*monospace$/);
    expect(drawn.find((t) => t.text === '12')!.font).toMatch(/^500 220px ui-monospace/);
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
