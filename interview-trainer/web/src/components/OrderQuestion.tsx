import { useMemo, useState } from 'react';
import { shuffleDifferent, type OrderQ } from '@trainer/shared';
import type { Answer } from '../lib/evaluate';

interface Props {
  q: OrderQ;
  seed: number;
  checked: boolean;
  onAnswer: (a: Answer | null) => void;
}

/** Tap lines from the pool to build the snippet in order; tap a placed line to send it back. */
export function OrderQuestion({ q, seed, checked, onAnswer }: Props) {
  const pool = useMemo(() => shuffleDifferent(q.lines, seed), [q, seed]);
  const [placed, setPlaced] = useState<number[]>([]); // indexes into `pool`, in answer order

  function commit(next: number[]) {
    setPlaced(next);
    onAnswer(next.length === pool.length ? { type: 'order', arranged: next.map((i) => pool[i]!) } : null);
  }

  return (
    <>
      <p className="hint">Tap the lines in the right order</p>
      <div className={`order-slot ${placed.length === 0 ? 'empty' : ''}`} aria-label="Your arrangement">
        {placed.length === 0 && <span className="placeholder">Your code goes here</span>}
        {placed.map((poolIndex, pos) => {
          const line = pool[poolIndex]!;
          const state = checked ? (line === q.lines[pos] ? 'correct' : 'wrong') : '';
          return (
            <button
              key={poolIndex}
              type="button"
              className={`code-line ${state}`}
              disabled={checked}
              onClick={() => commit(placed.filter((p) => p !== poolIndex))}
            >
              {line}
            </button>
          );
        })}
      </div>
      <div className="order-pool">
        {pool.map((line, i) => (
          <button
            key={i}
            type="button"
            className={`code-line ${placed.includes(i) ? 'used' : ''}`}
            disabled={checked || placed.includes(i)}
            onClick={() => commit([...placed, i])}
          >
            {line}
          </button>
        ))}
      </div>
    </>
  );
}
