import { useEffect, useRef } from 'react';

const COLORS = ['#58cc02', '#1cb0f6', '#ffc800', '#ff9600', '#ce82ff', '#ff4b4b'];

/**
 * A short confetti burst on a canvas (~1.2 s, no dependencies). Decorative: aria-hidden and pointer-events none.
 * Callers must not mount it under prefers-reduced-motion or when the learner turned effects off.
 */
export function Confetti({ onDone }: { onDone?: () => void }) {
  const ref = useRef<HTMLCanvasElement>(null);
  const done = useRef(onDone);
  done.current = onDone;

  useEffect(() => {
    const canvas = ref.current;
    const ctx = canvas?.getContext('2d');
    if (!canvas || !ctx) {
      done.current?.();
      return;
    }
    const w = (canvas.width = window.innerWidth);
    const h = (canvas.height = window.innerHeight);
    const parts = Array.from({ length: 70 }, (_, i) => ({
      x: w / 2 + (Math.random() - 0.5) * 80,
      y: h * 0.35,
      vx: (Math.random() - 0.5) * 9,
      vy: -Math.random() * 9 - 3,
      size: 5 + Math.random() * 5,
      color: COLORS[i % COLORS.length]!,
      spin: Math.random() * Math.PI,
    }));
    const started = performance.now();
    let raf = 0;
    const frame = (now: number) => {
      const t = now - started;
      ctx.clearRect(0, 0, w, h);
      for (const p of parts) {
        p.vy += 0.28;
        p.x += p.vx;
        p.y += p.vy;
        p.spin += 0.2;
        ctx.save();
        ctx.globalAlpha = Math.max(0, 1 - t / 1200);
        ctx.translate(p.x, p.y);
        ctx.rotate(p.spin);
        ctx.fillStyle = p.color;
        ctx.fillRect(-p.size / 2, -p.size / 4, p.size, p.size / 2);
        ctx.restore();
      }
      if (t < 1200) raf = requestAnimationFrame(frame);
      else done.current?.();
    };
    raf = requestAnimationFrame(frame);
    return () => cancelAnimationFrame(raf);
  }, []);

  return <canvas ref={ref} className="confetti" aria-hidden="true" data-testid="confetti" />;
}
