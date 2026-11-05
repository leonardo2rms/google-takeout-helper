import { useMemo, useState } from 'react';
import { seededShuffle, type MatchQ } from '@trainer/shared';
import type { Answer } from '../lib/evaluate';
import { Icon } from './icons';
import { Inline } from './Inline';
import { ResultCue, type ResultState } from './ResultCue';

interface Props {
  q: MatchQ;
  seed: number;
  checked: boolean;
  onAnswer: (a: Answer | null) => void;
}

/**
 * Tap an item on the left, then its partner on the right. Tap a connected item to undo.
 * Each connection gets its own colour (data-pair) so the pairing is visible without lines.
 */
export function MatchQuestion({ q, seed, checked, onAnswer }: Props) {
  const rights = useMemo(() => seededShuffle(q.pairs.map((p) => p.right), seed), [q, seed]);
  const [chosen, setChosen] = useState<(string | null)[]>(() => q.pairs.map(() => null));
  const [active, setActive] = useState<number | null>(null);

  function commit(next: (string | null)[]) {
    setChosen(next);
    onAnswer(next.every((c) => c !== null) ? { type: 'match', chosen: next } : null);
  }

  function pickLeft(i: number) {
    if (checked) return;
    if (chosen[i] !== null) {
      commit(chosen.map((c, j) => (j === i ? null : c))); // undo this connection
      setActive(i);
    } else setActive(i);
  }

  function pickRight(text: string) {
    if (checked) return;
    const owner = chosen.indexOf(text);
    if (owner !== -1) {
      commit(chosen.map((c, j) => (j === owner ? null : c))); // undo
      setActive(owner);
    } else if (active !== null) {
      commit(chosen.map((c, j) => (j === active ? text : c)));
      // continue with the next unmatched left item, which keeps the flow fast on a phone
      const nextLeft = chosen.findIndex((c, j) => c === null && j !== active);
      setActive(nextLeft === -1 ? null : nextLeft);
    }
  }

  return (
    <>
      <p className="hint">Tap an item, then the one it belongs with</p>
      <p className="sr-only" role="status" aria-live="polite">
        {checked ? '' : active !== null ? `Selected: ${q.pairs[active]!.left}. Now choose its match.` : `${chosen.filter((c) => c !== null).length} of ${q.pairs.length} paired`}
      </p>
      <div className="match">
        <div className="match-col" role="group" aria-label="Items">
          {q.pairs.map((p, i) => {
            const state = checked ? (chosen[i] === p.right ? 'correct' : 'wrong') : '';
            return (
              <button
                key={p.left}
                type="button"
                data-side="left"
                data-pair={chosen[i] !== null ? i : undefined}
                data-no={chosen[i] !== null ? i + 1 : undefined}
                className={`match-item ${active === i ? 'active' : ''} ${chosen[i] !== null ? 'paired' : ''} ${state}`}
                aria-pressed={active === i}
                disabled={checked}
                onClick={() => pickLeft(i)}
              >
                <span className="match-text"><Inline text={p.left} /></span>
                {chosen[i] !== null && <span className="sr-only">, paired with {chosen[i]}</span>}
                <ResultCue state={(state === 'correct' ? 'correct' : state === 'wrong' ? 'wrong' : '') as ResultState} />
                {state && <Icon name={state === 'correct' ? 'check' : 'x'} className="vmark" strokeWidth={2.5} />}
              </button>
            );
          })}
        </div>
        <div className="match-col" role="group" aria-label="Matches">
          {rights.map((r) => {
            const owner = chosen.indexOf(r);
            const state = checked && owner !== -1 ? (q.pairs[owner]!.right === r ? 'correct' : 'wrong') : '';
            return (
              <button
                key={r}
                type="button"
                data-side="right"
                data-pair={owner !== -1 ? owner : undefined}
                data-no={owner !== -1 ? owner + 1 : undefined}
                className={`match-item ${owner !== -1 ? 'paired' : ''} ${state}`}
                disabled={checked}
                onClick={() => pickRight(r)}
              >
                <span className="match-text"><Inline text={r} /></span>
                {owner !== -1 && <span className="sr-only">, paired with {q.pairs[owner]!.left}</span>}
                <ResultCue state={(state === 'correct' ? 'correct' : state === 'wrong' ? 'wrong' : '') as ResultState} />
                {state && <Icon name={state === 'correct' ? 'check' : 'x'} className="vmark" strokeWidth={2.5} />}
              </button>
            );
          })}
        </div>
      </div>
    </>
  );
}
