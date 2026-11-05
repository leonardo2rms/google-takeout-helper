import type { ShareCard } from '@trainer/shared';

export const CARD_W = 1080;
export const CARD_H = 1350;
export const CARD_FILE = 'interview-trainer-progress.png';

const token = (name: string, fallback: string) => {
  try {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim() || fallback;
  } catch {
    return fallback;
  }
};

function fit(ctx: CanvasRenderingContext2D, text: string, maxWidth: number): string {
  if (ctx.measureText(text).width <= maxWidth) return text;
  let t = text;
  while (t.length > 1 && ctx.measureText(`${t}…`).width > maxWidth) t = t.slice(0, -1);
  return `${t.trimEnd()}…`;
}

/** Draws the card on a canvas using the app's colour tokens. Pure drawing: the data comes from `buildShareCard`. */
export function drawShareCard(canvas: HTMLCanvasElement, card: ShareCard): void {
  canvas.width = CARD_W;
  canvas.height = CARD_H;
  const ctx = canvas.getContext('2d');
  if (!ctx) throw new Error('no canvas');
  const c = {
    bg: token('--bg', '#ffffff'),
    surface: token('--surface', '#f7f7f7'),
    border: token('--border', '#e5e5e5'),
    text: token('--text', '#3c3c3c'),
    muted: token('--muted', '#777777'),
    green: token('--green', '#58cc02'),
    greenDark: token('--green-dark', '#58a700'),
    blue: token('--blue', '#1cb0f6'),
    red: token('--red', '#ff4b4b'),
  };
  const font = (size: number, weight = 700) => `${weight} ${size}px system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", sans-serif`;
  const M = 80;
  const inner = CARD_W - 2 * M;

  ctx.fillStyle = c.bg;
  ctx.fillRect(0, 0, CARD_W, CARD_H);
  ctx.fillStyle = c.green;
  ctx.fillRect(0, 0, CARD_W, 24);

  ctx.textBaseline = 'alphabetic';
  ctx.textAlign = 'left';
  ctx.fillStyle = c.text;
  ctx.font = font(52, 800);
  ctx.fillText('Interview Trainer', M, 130);

  // Streak
  ctx.fillStyle = c.greenDark;
  ctx.font = font(260, 800);
  ctx.fillText(String(card.streak), M, 440);
  const numW = ctx.measureText(String(card.streak)).width;
  ctx.fillStyle = c.text;
  ctx.font = font(56, 700);
  ctx.fillText('day streak', M + numW + 28, 440);

  // Rank
  ctx.fillStyle = c.surface;
  ctx.strokeStyle = c.border;
  ctx.lineWidth = 4;
  ctx.beginPath();
  ctx.roundRect(M, 490, inner, 120, 28);
  ctx.fill();
  ctx.stroke();
  ctx.fillStyle = c.muted;
  ctx.font = font(34, 600);
  ctx.fillText('Rank', M + 36, 562);
  ctx.fillStyle = c.text;
  ctx.font = font(56, 800);
  ctx.textAlign = 'right';
  ctx.fillText(card.rank, CARD_W - M - 36, 568);
  ctx.textAlign = 'left';

  // Readiness
  ctx.fillStyle = c.muted;
  ctx.font = font(34, 600);
  ctx.fillText('Readiness', M, 700);
  ctx.fillStyle = c.text;
  ctx.font = font(120, 800);
  ctx.fillText(String(card.readinessScore), M, 820);
  const scoreW = ctx.measureText(String(card.readinessScore)).width;
  ctx.font = font(44, 700);
  ctx.fillText(card.readinessLabel, M + scoreW + 28, 800);
  ctx.fillStyle = c.muted;
  ctx.font = font(34, 500);
  ctx.fillText(`${card.coveragePct}% of the course seen`, M + scoreW + 28, 850);

  // Topics
  const rows = [
    card.strongest && { label: card.weakest ? 'Strongest topic' : 'Topic', ...card.strongest, color: c.green },
    card.weakest && { label: card.strongest ? 'Weakest topic' : 'Topic', ...card.weakest, color: c.blue },
  ].filter((r): r is { label: string; title: string; pct: number; color: string } => Boolean(r));
  let y = 950;
  for (const r of rows) {
    ctx.fillStyle = c.muted;
    ctx.font = font(32, 600);
    ctx.fillText(r.label, M, y);
    ctx.fillStyle = c.text;
    ctx.font = font(40, 800);
    ctx.textAlign = 'right';
    ctx.fillText(`${r.pct}%`, CARD_W - M, y + 2);
    ctx.textAlign = 'left';
    const w = inner - 130;
    ctx.fillText(fit(ctx, r.title, w), M, y + 56);
    ctx.fillStyle = c.border;
    ctx.beginPath();
    ctx.roundRect(M, y + 78, inner, 22, 11);
    ctx.fill();
    ctx.fillStyle = r.color;
    ctx.beginPath();
    ctx.roundRect(M, y + 78, Math.max(22, (inner * r.pct) / 100), 22, 11);
    ctx.fill();
    y += 170;
  }

  ctx.fillStyle = c.muted;
  ctx.font = font(28, 500);
  ctx.fillText('Readiness is an estimate of recall, not a prediction of any result.', M, CARD_H - 70);
}

/** Renders the card to a PNG blob; null when the browser cannot (no canvas / toBlob). */
export function renderShareCard(card: ShareCard): Promise<Blob | null> {
  try {
    const canvas = document.createElement('canvas');
    drawShareCard(canvas, card);
    return new Promise((resolve) => canvas.toBlob((b) => resolve(b), 'image/png'));
  } catch {
    return Promise.resolve(null);
  }
}
