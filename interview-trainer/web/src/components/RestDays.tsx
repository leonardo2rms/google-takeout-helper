import { MAX_REST_DAYS } from '@trainer/shared';
import { WEEKDAYS } from '../lib/format';

interface Props {
  value: readonly number[];
  onChange: (days: number[]) => void;
}

const SHORT = ['S', 'M', 'T', 'W', 'T', 'F', 'S'];

/** Weekday toggles for rest days. A rest day never breaks the streak and costs nothing. */
export function RestDays({ value, onChange }: Props) {
  const full = value.length >= MAX_REST_DAYS;
  return (
    <div role="group" aria-label="Rest days" className="rest-days">
      {WEEKDAYS.map((name, i) => {
        const on = value.includes(i);
        return (
          <button
            key={name}
            type="button"
            className={`day-toggle ${on ? 'on' : ''}`}
            aria-pressed={on}
            aria-label={name}
            disabled={!on && full}
            onClick={() => onChange(on ? value.filter((d) => d !== i) : [...value, i].sort())}
          >
            {SHORT[i]}
          </button>
        );
      })}
    </div>
  );
}
