import { HINT_COST } from '@trainer/shared';
import { Icon } from './icons';
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
        <Icon name="lightbulb" />
        <span><b>Hint</b><Inline text={text} /></span>
      </p>
    );
  }
  if (!canReveal) return null;
  return (
    <button type="button" className="btn small hint-btn" data-testid="hint-btn" onClick={onReveal}>
      <Icon name="lightbulb" />
      <span>Hint (−{HINT_COST} XP)</span>
    </button>
  );
}
