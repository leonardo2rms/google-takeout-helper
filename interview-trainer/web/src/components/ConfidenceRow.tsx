import { CONFIDENCE_LABELS, type Confidence } from '@trainer/shared';

interface Props {
  value: Confidence | null;
  onChange: (c: Confidence | null) => void;
}

const LEVELS: Confidence[] = [1, 2, 3];

/**
 * "How sure are you?" Asked BEFORE the answer is checked: after the feedback the rating would just be hindsight.
 * Optional: Check works without choosing, and tapping the chosen chip again clears it.
 */
export function ConfidenceRow({ value, onChange }: Props) {
  return (
    <div className="confidence" role="radiogroup" aria-label="How sure are you? (optional)" data-testid="confidence">
      <span className="confidence-label">How sure are you? <small>optional</small></span>
      <div className="confidence-chips">
        {LEVELS.map((level) => (
          <button
            key={level}
            type="button"
            role="radio"
            aria-checked={value === level}
            className={`chip confidence-chip ${value === level ? 'on' : ''}`}
            data-level={level}
            onClick={() => onChange(value === level ? null : level)}
          >
            {CONFIDENCE_LABELS[level]}
          </button>
        ))}
      </div>
    </div>
  );
}
