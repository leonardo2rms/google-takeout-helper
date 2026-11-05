import { useMemo, useState } from 'react';
import { seededShuffle, type MultiQ, type SingleQ } from '@trainer/shared';
import type { Answer } from '../lib/evaluate';
import { Icon } from './icons';
import { Inline } from './Inline';
import { ResultCue, type ResultState } from './ResultCue';

/** The marker says whose answer it is; the verdict itself is said once, in the feedback panel. */
const TAG = { correct: 'Your answer', wrong: 'Your answer', missed: 'Answer' } as const;

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
    <div className="options" role={multi ? 'group' : 'radiogroup'} aria-label={multi ? 'Answer options, select all that apply' : 'Answer options'}>
      {options.map((o, index) => {
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
            <span className="marker" aria-hidden="true">
              {state === 'wrong' ? <Icon name="x" size={15} strokeWidth={2.75} /> : state || isSel ? <Icon name="check" size={15} strokeWidth={2.75} /> : String.fromCharCode(65 + index)}
            </span>
            <span className="option-text"><Inline text={o.text} /><ResultCue state={state as ResultState} /></span>
            {state && <span className="tagtxt" aria-hidden="true">{TAG[state as 'correct' | 'wrong' | 'missed']}</span>}
          </button>
        );
      })}
      {checked && multi && <p className="hint multi-count" data-testid="multi-count">{countLine(selected, correct)}</p>}
    </div>
  );
}

/** "Right: 2 of 3 · Missed: 1 · Not an answer: 1": what the colours alone would leave the learner to count. */
function countLine(selected: readonly string[], correct: ReadonlySet<string>): string {
  const right = selected.filter((id) => correct.has(id)).length;
  const wrong = selected.length - right;
  const missed = correct.size - right;
  if (wrong === 0 && missed === 0) return `All ${correct.size} correct answers chosen.`;
  const parts = [`Right: ${right} of ${correct.size}`];
  if (missed > 0) parts.push(`Missed: ${missed}`);
  if (wrong > 0) parts.push(`Not an answer: ${wrong}`);
  return parts.join(' · ');
}
