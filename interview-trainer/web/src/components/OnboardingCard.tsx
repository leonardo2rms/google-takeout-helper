import { useState } from 'react';
import { GOAL_PRESETS, isDay, setGoalTarget, type Progress, type RoleId } from '@trainer/shared';

interface Props {
  today: string;
  progress: Progress;
  update: (fn: (p: Progress) => Progress) => void;
  /** Called after saving with whether the learner asked for the 3-question warm-up. */
  onDone: (warmUp: boolean) => void;
}

const ROLES: { id: RoleId; label: string; hint: string }[] = [
  { id: 'backend-kotlin', label: 'Backend / Kotlin', hint: 'Kotlin, coroutines, Spring, JVM first' },
  { id: 'staff-architect', label: 'Staff / architect', hint: 'DDD, scale and AI first' },
  { id: 'generalist', label: 'Generalist', hint: 'Everything equally' },
];

const GOALS: { key: keyof typeof GOAL_PRESETS; label: string }[] = [
  { key: 'light', label: 'Light' },
  { key: 'regular', label: 'Regular' },
  { key: 'serious', label: 'Serious' },
];

/**
 * First-run setup as an inline card on Home (never a modal or a route, so deep links keep working).
 * Every field is optional and skipping costs nothing.
 */
export function OnboardingCard({ today, progress, update, onDone }: Props) {
  const [role, setRole] = useState<RoleId | null>(progress.profile.role);
  const [date, setDate] = useState(progress.profile.interviewDate ?? '');
  const [goal, setGoal] = useState<number>(progress.goal.target);
  const [time, setTime] = useState(progress.profile.practiceTime ?? '');
  const [error, setError] = useState('');

  const save = (warmUp: boolean) => {
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
    onDone(warmUp);
  };

  const skip = () => {
    update((p) => ({ ...p, profile: { ...p.profile, onboarded: true } }));
    onDone(false);
  };

  return (
    <section className="onboarding card-block" aria-labelledby="onb-title" data-testid="onboarding">
      <h2 id="onb-title">Make it yours</h2>
      <p className="muted">Four optional taps so Today's practice fits you. You can change all of it later in Settings.</p>

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
        <button type="button" className="btn primary wide" onClick={() => save(true)}>Save and warm up (3 questions)</button>
        <button type="button" className="btn secondary wide" onClick={() => save(false)}>Save</button>
        <button type="button" className="btn ghost wide" onClick={skip}>Skip for now</button>
      </div>
    </section>
  );
}
