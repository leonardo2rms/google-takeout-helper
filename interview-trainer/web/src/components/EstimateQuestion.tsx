import { useState } from 'react';
import { parseNumber, type EstimateQ } from '@trainer/shared';
import type { Answer } from '../lib/evaluate';
import { Icon } from './icons';

interface Props {
  q: EstimateQ;
  checked: boolean;
  onAnswer: (a: Answer | null) => void;
}

export function EstimateQuestion({ q, checked, onAnswer }: Props) {
  const [text, setText] = useState('');
  const parsed = parseNumber(text);
  const percent = Math.round(q.tolerance * 100 * 10) / 10;

  return (
    <div className="estimate">
      <div className="estimate-field" data-invalid={text.trim() !== '' && parsed === null}>
        <input
          className="estimate-input"
          type="text"
          inputMode="decimal"
          autoComplete="off"
          autoCorrect="off"
          spellCheck={false}
          enterKeyHint="done"
          placeholder="Your estimate"
          aria-label="Your estimate"
          aria-invalid={text.trim() !== '' && parsed === null}
          disabled={checked}
          value={text}
          onChange={(e) => {
            const value = parseNumber(e.target.value);
            setText(e.target.value);
            onAnswer(value === null ? null : { type: 'estimate', value });
          }}
        />
        {q.unit && <span className="unit">{q.unit}</span>}
      </div>
      {text.trim() !== '' && parsed === null ? (
        <p className="hint error"><Icon name="x" size="1.1em" strokeWidth={2.5} />Enter a number, e.g. 18.5</p>
      ) : (
        <p className="hint">{q.tolerance === 0 ? 'Give the exact number.' : `Back-of-the-envelope is fine: within ${percent}% counts.`}</p>
      )}
    </div>
  );
}
