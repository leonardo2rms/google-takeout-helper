import { HINT_COST } from '@trainer/shared';
import { Inline } from './Inline';

interface Props {
  text: string;
  revealed: boolean;
  /** Reveal is only offered before the answer is checked. */
  canReveal: boolean;
  onReveal: () => void;
}

/** "Hint" button + revealed hint. One reveal per question; revealing costs a little XP and caps the review quality. */
export function Hint({ text, revealed, canReveal, onReveal }: Props) {
  if (revealed) {
    return (
      <p className="hint-box" role="note" data-testid="hint-text">
        <b>💡 Hint</b> <Inline text={text} />
      </p>
    );
  }
  if (!canReveal) return null;
  return (
    <button type="button" className="btn secondary small hint-btn" data-testid="hint-btn" onClick={onReveal}>
      💡 Hint <span>(−{HINT_COST} XP)</span>
    </button>
  );
}
