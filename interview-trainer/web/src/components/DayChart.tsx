import type { DayBar } from '@trainer/shared';

/** Questions answered per day as a small SVG bar chart. A bar is filled when that day's goal was met. */
export function DayChart({ bars }: { bars: readonly DayBar[] }) {
  const W = 280;
  const H = 84;
  const gap = 4;
  const bw = (W - gap * (bars.length - 1)) / bars.length;
  const top = Math.max(1, ...bars.map((b) => Math.max(b.answered, b.target)));
  const summary = bars.map((b) => `${b.day}: ${b.answered}`).join(', ');
  return (
    <figure className="day-chart" data-testid="day-chart">
      <svg viewBox={`0 0 ${W} ${H + 4}`} role="img" aria-label={`Questions answered per day, last ${bars.length} days. ${summary}`}>
        {bars.map((b, i) => {
          const h = b.answered === 0 ? 2 : Math.max(4, (b.answered / top) * H);
          return (
            <g key={b.day}>
              <rect
                className={`bar-col ${b.met ? 'met' : ''} ${b.answered === 0 ? 'zero' : ''}`}
                x={i * (bw + gap)}
                y={H - h + 2}
                width={bw}
                height={h}
                rx={3}
                data-day={b.day}
                data-answered={b.answered}
              >
                <title>{`${b.day}: ${b.answered} questions`}</title>
              </rect>
            </g>
          );
        })}
      </svg>
      <figcaption className="muted">Questions per day. Solid bars reached that day's goal.</figcaption>
    </figure>
  );
}
