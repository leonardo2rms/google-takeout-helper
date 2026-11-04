import { useState } from 'react';
import { parseNumber, type EstimateQ } from '@trainer/shared';
import type { Answer } from '../lib/evaluate';

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
      <div className="estimate-row">
        <input
          id="estimate-input"
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
      <p className="hint">
        {text.trim() !== '' && parsed === null
          ? 'Enter a number, e.g. 18.5'
          : q.tolerance === 0
            ? 'Give the exact number.'
            : `Back-of-the-envelope is fine: within ${percent}% counts.`}
      </p>
    </div>
  );
}
