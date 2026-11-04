import { useEffect, useState, type ReactNode } from 'react';

interface RingProps {
  /** 0..1 */
  fraction: number;
  size?: number;
  children?: ReactNode;
}

/** A progress ring. Purely visual (aria-hidden); callers provide the accessible name. */
export function Ring({ fraction, size = 52, children }: RingProps) {
  const stroke = 5;
  const r = (size - stroke) / 2;
  const c = 2 * Math.PI * r;
  const f = Math.min(1, Math.max(0, fraction));
  return (
    <span className="ring" style={{ width: size, height: size }} aria-hidden="true">
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
        <circle className="ring-track" cx={size / 2} cy={size / 2} r={r} strokeWidth={stroke} fill="none" />
        <circle
          className="ring-fill"
          cx={size / 2}
          cy={size / 2}
          r={r}
          strokeWidth={stroke}
          fill="none"
          strokeLinecap="round"
          strokeDasharray={c}
          strokeDashoffset={c * (1 - f)}
          transform={`rotate(-90 ${size / 2} ${size / 2})`}
        />
      </svg>
      {children !== undefined && <span className="ring-label">{children}</span>}
    </span>
  );
}

/** Starts at `from` and moves to `to` (the CSS transition is disabled under prefers-reduced-motion). */
export function AnimatedRing({ from, to, size, children }: { from: number; to: number; size?: number; children?: ReactNode }) {
  const [value, setValue] = useState(from);
  useEffect(() => {
    const id = requestAnimationFrame(() => setValue(to));
    return () => cancelAnimationFrame(id);
  }, [to]);
  return <Ring fraction={value} size={size}>{children}</Ring>;
}
