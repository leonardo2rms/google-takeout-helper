import { Fragment, useMemo, useState } from 'react';
import { seededShuffle, type FillQ } from '@trainer/shared';
import type { Answer } from '../lib/evaluate';

interface Props {
  q: FillQ;
  seed: number;
  checked: boolean;
  onAnswer: (a: Answer | null) => void;
}

/** Tap a token to drop it into the next empty blank; tap a filled blank to take it back. */
export function FillQuestion({ q, seed, checked, onAnswer }: Props) {
  const bank = useMemo(() => seededShuffle(q.bank, seed), [q, seed]);
  const parts = useMemo(() => q.template.replace(/\n$/, '').split(/\[\[(\d+)\]\]/), [q]);
  // slots[b] = index into `bank` placed in blank b+1 (or null)
  const [slots, setSlots] = useState<(number | null)[]>(() => q.answers.map(() => null));

  function commit(next: (number | null)[]) {
    setSlots(next);
    onAnswer(next.every((s) => s !== null) ? { type: 'fill', filled: next.map((s) => bank[s!]!) } : null);
  }

  function place(bankIndex: number) {
    if (checked || slots.includes(bankIndex)) return;
    const free = slots.indexOf(null);
    if (free === -1) return;
    commit(slots.map((s, i) => (i === free ? bankIndex : s)));
  }

  function clear(blank: number) {
    if (checked || slots[blank] === null) return;
    commit(slots.map((s, i) => (i === blank ? null : s)));
  }

  return (
    <>
      <pre className="code fill" tabIndex={0}>
        <code>
          {parts.map((part, i) => {
            if (i % 2 === 0) return <Fragment key={i}>{part}</Fragment>;
            const blank = Number(part) - 1;
            const slot = slots[blank] ?? null;
            const token = slot === null ? null : bank[slot]!;
            const state = checked && token !== null ? (token === q.answers[blank] ? 'correct' : 'wrong') : '';
            return (
              <button
                key={i}
                type="button"
                className={`blank ${token !== null ? 'filled' : ''} ${state}`}
                onClick={() => clear(blank)}
                aria-label={token ? `Blank ${blank + 1}: ${token}. Tap to remove` : `Blank ${blank + 1}, empty`}
              >
                {token ?? '    '}
              </button>
            );
          })}
        </code>
      </pre>
      <div className="bank" aria-label="Token bank">
        {bank.map((token, i) => (
          <button
            key={i}
            type="button"
            className={`chip mono ${slots.includes(i) ? 'used' : ''}`}
            disabled={checked || slots.includes(i)}
            onClick={() => place(i)}
          >
            {token}
          </button>
        ))}
      </div>
    </>
  );
}
