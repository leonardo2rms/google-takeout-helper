import { useMemo, useState } from 'react';
import { spotLines, type SpotQ } from '@trainer/shared';
import { highlight } from '../lib/highlight';
import type { Answer } from '../lib/evaluate';

interface Props {
  q: SpotQ;
  checked: boolean;
  onAnswer: (a: Answer | null) => void;
}

/** Tap the line(s) with the bug. Rows are buttons so they work with touch, mouse and keyboard. */
export function SpotQuestion({ q, checked, onAnswer }: Props) {
  const lines = useMemo(() => spotLines(q.source), [q]);
  const [selected, setSelected] = useState<number[]>([]);
  const correct = new Set(q.answers);

  function toggle(n: number) {
    if (checked) return;
    const next = selected.includes(n) ? selected.filter((x) => x !== n) : [...selected, n];
    setSelected(next);
    onAnswer(next.length ? { type: 'spot', selected: next } : null);
  }

  return (
    <>
      <p className="hint">
        {q.answers.length === 1 ? 'Tap the line with the problem' : `Tap the ${q.answers.length} lines with the problem`}
      </p>
      <div className="spot code" role="group" aria-label="Code lines">
        {lines.map((line, i) => {
          const n = i + 1;
          const isSel = selected.includes(n);
          let state = '';
          if (checked) {
            if (correct.has(n)) state = isSel ? 'correct' : 'missed';
            else if (isSel) state = 'wrong';
          }
          return (
            <button
              key={n}
              type="button"
              data-line={n}
              className={`src-line ${isSel ? 'selected' : ''} ${state}`}
              aria-pressed={isSel}
              aria-label={`Line ${n}`}
              disabled={checked || line.trim() === ''}
              onClick={() => toggle(n)}
            >
              <span className="ln" aria-hidden="true">{n}</span>
              <span className="src">{line === '' ? ' ' : highlight(line)}</span>
            </button>
          );
        })}
      </div>
    </>
  );
}
