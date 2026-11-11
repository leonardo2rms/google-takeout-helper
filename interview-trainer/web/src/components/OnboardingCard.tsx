import { useState } from 'react';
import { GOAL_PRESETS, ROLE_HINT, ROLE_LABEL, ROLE_IDS, isDay, setGoalTarget, type Progress, type RoleId } from '@trainer/shared';
import { Icon } from './icons';

interface Props {
  today: string;
  progress: Progress;
  update: (fn: (p: Progress) => Progress) => void;
  /** Opens the form straight away (editing an existing plan); first-run shows one quiet line instead. */
  startOpen?: boolean;
}

const ROLES = ROLE_IDS.map((id) => ({ id, label: ROLE_LABEL[id], hint: ROLE_HINT[id] }));

const GOALS: { key: keyof typeof GOAL_PRESETS; label: string }[] = [
  { key: 'light', label: 'Light' },
  { key: 'regular', label: 'Regular' },
  { key: 'serious', label: 'Serious' },
];

/**
 * Plan setup as an inline card on Home (never a modal or a route, so deep links keep working). On first run it is one
 * quiet line under the "Start your first lesson" button; the form opens on tap. Every field is optional and skipping costs nothing.
 */
export function OnboardingCard({ today, progress, update, startOpen = false }: Props) {
  const [open, setOpen] = useState(startOpen);
  const [role, setRole] = useState<RoleId | null>(progress.profile.role);
  const [date, setDate] = useState(progress.profile.interviewDate ?? '');
  const [goal, setGoal] = useState<number>(progress.goal.target);
  const [time, setTime] = useState(progress.profile.practiceTime ?? '');
  const [error, setError] = useState('');

  const save = () => {
    if (date !== '' && (!isDay(date) || date < today)) {
      setError('Pick today or a later date, or leave it empty.');
      return;
    }
    update((p) => {
      const withGoal = setGoalTarget(p, goal, today);
      return {
        ...withGoal,
        profile: { ...p.profile, onboarded: true, role, interviewDate: date === '' ? null : date, practiceTime: time === '' ? null : time },
      };
    });
  };

  const skip = () => {
    update((p) => ({ ...p, profile: { ...p.profile, onboarded: true } }));
  };

  if (!open) {
    return (
      <section className="onboarding-line" data-testid="onboarding">
        <button type="button" className="card action onb-toggle" aria-expanded={false} aria-controls="onb-form" onClick={() => setOpen(true)}>
          <span className="tile-i" aria-hidden="true"><Icon name="goal" size={22} /></span>
          <span><b>Set a goal and interview date</b><small>Optional. Shapes Today's practice; change it any time in Settings.</small></span>
          <Icon name="chevron-down" className="go" />
        </button>
      </section>
    );
  }

  return (
    <section className="onboarding card-block" id="onb-form" aria-labelledby="onb-title" data-testid="onboarding">
      <h2 id="onb-title">Your plan</h2>
      <p className="muted">Every field is optional. You can change all of it later in Settings.</p>

      <fieldset>
        <legend>What are you preparing for?</legend>
        <div className="choice-grid">
          {ROLES.map((r) => (
            <label key={r.id} className={`choice ${role === r.id ? 'on' : ''}`}>
              <input type="radio" name="role" checked={role === r.id} onChange={() => setRole(r.id)} />
              <span><b>{r.label}</b><small>{r.hint}</small></span>
            </label>
          ))}
        </div>
      </fieldset>

      <div className="field">
        <label htmlFor="onb-date">Interview date (optional)</label>
        <input id="onb-date" type="date" min={today} value={date} onChange={(e) => { setDate(e.target.value); setError(''); }} />
        <small className="muted">Used for a calm plan and countdown. Leave empty and nothing is shown.</small>
        {error && <p role="alert" className="field-error">{error}</p>}
      </div>

      <fieldset>
        <legend>Daily goal</legend>
        <div className="choice-grid three">
          {GOALS.map((g) => (
            <label key={g.key} className={`choice ${goal === GOAL_PRESETS[g.key] ? 'on' : ''}`}>
              <input type="radio" name="goal" checked={goal === GOAL_PRESETS[g.key]} onChange={() => setGoal(GOAL_PRESETS[g.key])} />
              <span><b>{GOAL_PRESETS[g.key]}</b><small>{g.label}</small></span>
            </label>
          ))}
        </div>
        <small className="muted">Questions a day. Eight is about five minutes.</small>
      </fieldset>

      <div className="field">
        <label htmlFor="onb-time">When will you usually practise? (optional)</label>
        <input id="onb-time" type="time" value={time} onChange={(e) => setTime(e.target.value)} />
        <small className="muted">Saying "after coffee" or "on the train" makes it stick. We never send reminders on our own.</small>
      </div>

      <div className="onb-actions">
        <button type="button" className="btn primary wide" onClick={save}>Save plan</button>
        <button type="button" className="btn ghost wide" onClick={skip}>Not now</button>
      </div>
    </section>
  );
}
