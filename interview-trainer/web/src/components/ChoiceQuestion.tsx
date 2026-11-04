import { useMemo, useState } from 'react';
import { seededShuffle, type MultiQ, type SingleQ } from '@trainer/shared';
import type { Answer } from '../lib/evaluate';
import { Inline } from './Inline';

interface Props {
  q: SingleQ | MultiQ;
  seed: number;
  checked: boolean;
  onAnswer: (a: Answer | null) => void;
}

/** Single and multi select. Options are shuffled per attempt so the answer is never positional. */
export function ChoiceQuestion({ q, seed, checked, onAnswer }: Props) {
  const options = useMemo(() => seededShuffle(q.options, seed), [q, seed]);
  const [selected, setSelected] = useState<string[]>([]);
  const multi = q.type === 'multi';
  const correct = new Set(q.type === 'single' ? [q.answer] : q.answers);

  function toggle(id: string) {
    if (checked) return;
    const next = multi ? (selected.includes(id) ? selected.filter((s) => s !== id) : [...selected, id]) : [id];
    setSelected(next);
    if (next.length === 0) onAnswer(null);
    else onAnswer(multi ? { type: 'multi', selected: next } : { type: 'single', selected: next[0]! });
  }

  return (
    <div className="options" role={multi ? 'group' : 'radiogroup'} aria-label="Answer options">
      {multi && <p className="hint">Select all that apply</p>}
      {options.map((o) => {
        const isSel = selected.includes(o.id);
        let state = '';
        if (checked) {
          if (correct.has(o.id)) state = isSel ? 'correct' : 'missed';
          else if (isSel) state = 'wrong';
        }
        return (
          <button
            key={o.id}
            type="button"
            className={`option ${multi ? 'multi' : ''} ${isSel ? 'selected' : ''} ${state}`}
            role={multi ? 'checkbox' : 'radio'}
            aria-checked={isSel}
            disabled={checked}
            onClick={() => toggle(o.id)}
          >
            <span className="marker" aria-hidden="true">{isSel ? (multi ? '✓' : '●') : ''}</span>
            <span className="option-text"><Inline text={o.text} /></span>
          </button>
        );
      })}
    </div>
  );
}
