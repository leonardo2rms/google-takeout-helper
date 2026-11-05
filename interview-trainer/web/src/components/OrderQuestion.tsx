import { useMemo, useState } from 'react';
import { shuffleDifferent, type OrderQ } from '@trainer/shared';
import type { Answer } from '../lib/evaluate';
import { Icon } from './icons';
import { ResultCue, type ResultState } from './ResultCue';

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
      <div className={`order-slot ${q.kind === 'steps' ? 'steps' : ''} ${placed.length === 0 ? 'empty' : ''}`} role="group" aria-label="Your arrangement">
        {placed.length === 0 && <span className="placeholder">{q.kind === 'steps' ? 'Tap the steps below in order' : 'Tap the lines below in order'}</span>}
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
              <span className="sr-only">Position {pos + 1}: </span>
              {line}
              {checked ? <ResultCue state={(state === 'correct' ? 'correct' : state === 'wrong' ? 'wrong' : '') as ResultState} /> : <span className="sr-only"> (tap to remove)</span>}
              {checked && <Icon name={state === 'correct' ? 'check' : 'x'} className="vmark" strokeWidth={2.5} />}
            </button>
          );
        })}
      </div>
      <p className="sr-only" role="status" aria-live="polite">{checked ? '' : `${placed.length} of ${pool.length} placed`}</p>
      <div className={`order-pool ${q.kind === 'steps' ? 'steps' : ''} ${placed.length === pool.length ? 'all-placed' : ''}`} role="group" aria-label="Lines to place">
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
