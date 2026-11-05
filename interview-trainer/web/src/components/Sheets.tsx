import { GOAL_PRESETS, MAX_FREEZES, answeredInLast, goalToday, lastDays, setGoalTarget, streakStatus, type Progress } from '@trainer/shared';
import { plural } from '../lib/format';
import { Icon } from './icons';
import { DayChart } from './DayChart';
import { RestDays } from './RestDays';
import { Sheet } from './Sheet';

interface Props {
  progress: Progress;
  today: string;
  update: (fn: (p: Progress) => Progress) => void;
  onClose: () => void;
}

/** One calm sentence about what the streak is doing right now. */
export function streakNote(p: Progress, today: string): string | null {
  const s = streakStatus(p, today);
  if (s.state === 'saved-by-freeze') return s.freezesNeeded === 1 ? 'covered yesterday' : `covers ${s.freezesNeeded} missed days`;
  return null;
}

export function StreakSheet({ progress, today, update, onClose }: Props) {
  const s = streakStatus(progress, today);
  return (
    <Sheet title="Your streak" onClose={onClose}>
      <p className="big-number" data-testid="streak-value"><Icon name="flame" size="1em" /> {s.streak} {s.streak === 1 ? 'day' : 'days'}</p>
      {s.state === 'saved-by-freeze' && (
        <p className="notice"><Icon name="snowflake" /><span>{s.freezesNeeded === 1 ? 'A freeze will cover the day you missed' : `${s.freezesNeeded} freezes will cover the days you missed`} as soon as you practise. Nothing is lost.</span></p>
      )}
      {s.state === 'lapsed' && <p>Fresh start. Your best so far stays on record, and so do your practice days.</p>}
      {s.state === 'new' && <p>Practise today to start a streak.</p>}
      <dl className="facts">
        <div><dt>Longest streak</dt><dd data-testid="longest-streak">{progress.longestStreak}</dd></div>
        <div><dt>Days practised overall</dt><dd>{progress.stats.practiceDays}</dd></div>
        <div><dt>Freezes held</dt><dd><Icon name="snowflake" size="1em" /> {progress.freezes} of {MAX_FREEZES}</dd></div>
      </dl>
      <p className="muted">
        Practise any day to keep your streak. Every 7 days in a row earns a freeze, and a freeze covers a missed day on its own. Rest days you pick below are covered for free.
      </p>
      <h3>Rest days</h3>
      <RestDays value={progress.profile.restDays} onChange={(restDays) => update((p) => ({ ...p, profile: { ...p.profile, restDays } }))} />
      <p className="muted">Up to 3 a week. A rest day never breaks the streak.</p>
    </Sheet>
  );
}

const GOAL_LABELS: [keyof typeof GOAL_PRESETS, string][] = [['light', 'Light'], ['regular', 'Regular'], ['serious', 'Serious']];

export function GoalSheet({ progress, today, update, onClose }: Props) {
  const g = goalToday(progress, today);
  const week = answeredInLast(progress, today, 7);
  return (
    <Sheet title="Daily goal" onClose={onClose}>
      <p className="big-number" data-testid="goal-value">{g.done} of {g.target} questions today</p>
      {g.comeback && <p>Easy restart: {g.target} today. Welcome back.</p>}
      {g.done === 0 && !g.comeback && <p className="muted">{GOAL_PRESETS.light} questions is about five minutes.</p>}
      {g.met && <p>Goal met. Anything more is a bonus.</p>}
      <DayChart bars={lastDays(progress, today, 14)} />
      <dl className="facts">
        <div><dt>Days practised overall</dt><dd>{progress.stats.practiceDays}</dd></div>
        <div><dt>This week</dt><dd>{plural(week, 'question')}</dd></div>
      </dl>
      <h3>Questions per day</h3>
      <div className="choice-grid three" role="radiogroup" aria-label="Daily goal">
        {GOAL_LABELS.map(([key, label]) => (
          <label key={key} className={`choice ${progress.goal.target === GOAL_PRESETS[key] ? 'on' : ''}`}>
            <input
              type="radio"
              name="goal-sheet"
              checked={progress.goal.target === GOAL_PRESETS[key]}
              onChange={() => update((p) => setGoalTarget(p, GOAL_PRESETS[key], today))}
            />
            <span><b>{GOAL_PRESETS[key]}</b><small>{label}</small></span>
          </label>
        ))}
      </div>
    </Sheet>
  );
}
