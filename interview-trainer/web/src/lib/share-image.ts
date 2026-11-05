import type { ShareCard } from '@trainer/shared';

export const CARD_W = 1080;
export const CARD_H = 1350;
export const CARD_FILE = 'interview-trainer-progress.png';

/**
 * The card is always drawn on the light "drafting sheet" palette (the same values as the light theme tokens in styles.css),
 * whatever theme the app is in: a picture that leaves the app should look the same wherever it lands. Every pairing here is
 * one the contrast script checks (ink, graphite on paper; paper on ink).
 */
const C = { paper: '#F2F4F6', sheet: '#FBFCFD', ink: '#0F1B26', graphite: '#4A5967', edge: '#7B8996', hairline: '#D3D9DF', cobalt: '#2447C4' };

const SANS = '"Plex Sans Var", ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif';
const MONO = 'ui-monospace, "SF Mono", SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace';
const sans = (size: number, weight = 600) => `${weight} ${size}px ${SANS}`;
const mono = (size: number, weight = 500) => `${weight} ${size}px ${MONO}`;

// Lucide (ISC) shapes, drawn with the same 24x24 grid as the in-app icons (see components/icons.tsx).
const FLAME = 'M12 3q1 4 4 6.5t3 5.5a1 1 0 0 1-14 0 5 5 0 0 1 1-3 1 1 0 0 0 5 0c0-2-1.5-3-1.5-5q0-2 2.5-4';
const CHECK = 'M20 6 9 17l-5-5';

function icon(ctx: CanvasRenderingContext2D, d: string, x: number, y: number, size: number, color: string, width: number): void {
  ctx.save();
  ctx.translate(x, y);
  ctx.scale(size / 24, size / 24);
  ctx.strokeStyle = color;
  ctx.lineWidth = width;
  ctx.lineCap = 'round';
  ctx.lineJoin = 'round';
  ctx.stroke(new Path2D(d));
  ctx.restore();
}

function fit(ctx: CanvasRenderingContext2D, text: string, maxWidth: number): string {
  if (ctx.measureText(text).width <= maxWidth) return text;
  let t = text;
  while (t.length > 1 && ctx.measureText(`${t}…`).width > maxWidth) t = t.slice(0, -1);
  return `${t.trimEnd()}…`;
}

/** Letter-spaced uppercase label in the mono face (canvas has no letter-spacing property everywhere, so it is drawn glyph by glyph). */
function label(ctx: CanvasRenderingContext2D, text: string, x: number, y: number, size = 26, align: 'left' | 'right' = 'left'): void {
  ctx.font = mono(size, 500);
  ctx.fillStyle = C.graphite;
  ctx.textAlign = 'left';
  const gap = size * 0.12;
  const upper = text.toUpperCase();
  const width = [...upper].reduce((w, ch) => w + ctx.measureText(ch).width + gap, -gap);
  let cx = align === 'right' ? x - width : x;
  for (const ch of upper) {
    ctx.fillText(ch, cx, y);
    cx += ctx.measureText(ch).width + gap;
  }
}

/** A gauge of `ticks` segments, filled in proportion to `pct` (the same idiom as the in-app progress gauges). */
function gauge(ctx: CanvasRenderingContext2D, x: number, y: number, w: number, h: number, pct: number, ticks = 20): void {
  const gap = 4;
  const seg = (w - gap * (ticks - 1)) / ticks;
  const on = Math.round((Math.min(100, Math.max(0, pct)) / 100) * ticks);
  for (let i = 0; i < ticks; i++) {
    ctx.fillStyle = i < on ? C.ink : C.hairline;
    ctx.fillRect(x + i * (seg + gap), y, seg, h);
  }
}

/** Draws the card on a canvas. Pure drawing: the data comes from `buildShareCard`. */
export function drawShareCard(canvas: HTMLCanvasElement, card: ShareCard): void {
  canvas.width = CARD_W;
  canvas.height = CARD_H;
  const ctx = canvas.getContext('2d');
  if (!ctx) throw new Error('no canvas');
  const M = 80;
  const inner = CARD_W - 2 * M;
  ctx.textBaseline = 'alphabetic';
  ctx.textAlign = 'left';

  // the sheet: paper, with a drawing border
  ctx.fillStyle = C.paper;
  ctx.fillRect(0, 0, CARD_W, CARD_H);
  ctx.strokeStyle = C.ink;
  ctx.lineWidth = 3;
  ctx.strokeRect(36, 36, CARD_W - 72, CARD_H - 72);

  // title block
  ctx.fillStyle = C.ink;
  ctx.beginPath();
  ctx.roundRect(M, 92, 72, 72, 12);
  ctx.fill();
  icon(ctx, CHECK, M + 16, 108, 40, C.paper, 2.6);
  ctx.fillStyle = C.ink;
  ctx.font = sans(52, 600);
  ctx.fillText('Interview Trainer', M + 100, 144);
  label(ctx, 'Progress', CARD_W - M, 142, 26, 'right');
  ctx.fillStyle = C.hairline;
  ctx.fillRect(M, 196, inner, 2);

  // streak
  label(ctx, 'Streak', M, 270);
  icon(ctx, FLAME, M, 300, 100, C.ink, 1.6);
  ctx.fillStyle = C.ink;
  ctx.font = mono(220, 500);
  ctx.textAlign = 'left';
  ctx.fillText(String(card.streak), M + 130, 470);
  const numW = ctx.measureText(String(card.streak)).width;
  ctx.fillStyle = C.graphite;
  ctx.font = mono(52, 500);
  ctx.fillText(card.streak === 1 ? 'day' : 'days', M + 130 + numW + 24, 470);

  // ruled readout: rank, readiness, coverage
  const rows: [string, string][] = [
    ['Rank', card.rank],
    ['Readiness', `${card.readinessScore}  ${card.readinessLabel}`],
    ['Course seen', `${card.coveragePct}%`],
  ];
  let y = 560;
  ctx.fillStyle = C.ink;
  ctx.fillRect(M, y, inner, 3);
  for (const [name, value] of rows) {
    label(ctx, name, M, y + 54, 28);
    ctx.fillStyle = C.ink;
    ctx.font = sans(46, 600);
    ctx.textAlign = 'right';
    ctx.fillText(fit(ctx, value, inner - 360), CARD_W - M, y + 58);
    ctx.textAlign = 'left';
    y += 84;
    ctx.fillStyle = C.hairline;
    ctx.fillRect(M, y, inner, 2);
  }

  // topics
  const topics = [
    card.strongest && { name: card.weakest ? 'Strongest topic' : 'Topic', ...card.strongest },
    card.weakest && { name: card.strongest ? 'Weakest topic' : 'Topic', ...card.weakest },
  ].filter((r): r is { name: string; title: string; pct: number } => Boolean(r));
  y = 892;
  for (const t of topics) {
    label(ctx, t.name, M, y, 26);
    ctx.fillStyle = C.ink;
    ctx.font = mono(44, 600);
    ctx.textAlign = 'right';
    ctx.fillText(`${t.pct}%`, CARD_W - M, y + 6);
    ctx.textAlign = 'left';
    ctx.font = sans(42, 600);
    ctx.fillText(fit(ctx, t.title, inner - 170), M, y + 54);
    gauge(ctx, M, y + 78, inner, 20, t.pct);
    y += 160;
  }

  // footer
  ctx.fillStyle = C.hairline;
  ctx.fillRect(M, CARD_H - 128, inner, 2);
  ctx.fillStyle = C.graphite;
  ctx.font = sans(27, 400);
  ctx.fillText('Readiness is an estimate of recall, not a prediction of any result.', M, CARD_H - 80);
}

/** Waits for the app font so the picture is drawn in Plex Sans, not the fallback (errors and missing support are harmless). */
async function fontsReady(): Promise<void> {
  try {
    await Promise.all([document.fonts.load(sans(52, 600)), document.fonts.load(sans(27, 400))]);
  } catch {
    /* draw with the fallback font */
  }
}

/** Renders the card to a PNG blob; null when the browser cannot (no canvas / toBlob). */
export async function renderShareCard(card: ShareCard): Promise<Blob | null> {
  try {
    await fontsReady();
    const canvas = document.createElement('canvas');
    drawShareCard(canvas, card);
    return await new Promise((resolve) => canvas.toBlob((b) => resolve(b), 'image/png'));
  } catch {
    return null;
  }
}
