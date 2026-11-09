import { ACHIEVEMENT_GROUPS, achievementStatus, addDays, type AchievementGroup, type Progress } from '@trainer/shared';
import { formatDay } from '../lib/format';
import { useToday } from '../lib/today';
import { Icon, isIconName } from './icons';

/** A group of badges stays open for this many days after one of them was earned, so a new badge is never hidden. */
const FRESH_DAYS = 2;

/**
 * All badges, grouped. Each group of earned badges folds to one line with its count ("Habit 4/6"), and stays open while it
 * holds a badge earned in the last couple of days. Locked ones are listed with what it takes: nothing is hidden, nothing expires.
 */
export function TrophyCase({ progress }: { progress: Progress }) {
  const today = useToday();
  const status = achievementStatus(progress);
  const got = status.filter((s) => s.on).length;
  const groups = Object.keys(ACHIEVEMENT_GROUPS) as AchievementGroup[];
  const locked = status.length - got;

  const badge = ({ def, on }: (typeof status)[number]) => (
    <li key={def.id} className={`badge ${on ? 'got' : 'locked'}`} data-testid={`badge-${def.id}`} data-unlocked={on ? 'true' : 'false'}>
      <span className="badge-icon" aria-hidden="true"><Icon name={isIconName(def.icon) ? def.icon : 'award'} size={20} /></span>
      <span className="badge-text">
        <b>{def.title}</b>
        <small>{def.description}</small>
        <small className="badge-state">{on ? `Unlocked ${formatDay(on)}` : 'Locked'}</small>
      </span>
    </li>
  );

  return (
    <section className="trophies" aria-labelledby="trophy-title" data-testid="trophy-case">
      <h2 id="trophy-title">Badges <span className="muted" data-testid="badge-count">{got}/{status.length}</span></h2>
      {got === 0 && <p className="muted">Your first badge comes with your first lesson.</p>}
      {groups.map((g) => {
        const mine = status.filter((s) => s.def.group === g && s.on);
        if (mine.length === 0) return null;
        const total = status.filter((s) => s.def.group === g).length;
        const fresh = mine.some((s) => s.on !== null && s.on >= addDays(today, -FRESH_DAYS));
        return (
          <details key={g} className="trophy-group" open={fresh} data-testid={`trophy-group-${g}`}>
            <summary>
              <h3>{ACHIEVEMENT_GROUPS[g]}</h3>
              <span className="muted" data-testid={`trophy-group-${g}-count`}>{mine.length}/{total}</span>
              <Icon name="chevron-down" className="chev" size={18} />
            </summary>
            <ul className="badge-grid">{mine.map(badge)}</ul>
          </details>
        );
      })}
      {locked > 0 && (
        <details className="locked-badges" data-testid="locked-badges">
          <summary><Icon name="chevron-down" className="chev" />{locked} {locked === 1 ? 'badge' : 'badges'} still to unlock</summary>
          {groups.map((g) => {
            const mine = status.filter((s) => s.def.group === g && !s.on);
            if (mine.length === 0) return null;
            return (
              <div key={g} className="trophy-group">
                <h3>{ACHIEVEMENT_GROUPS[g]}</h3>
                <ul className="badge-grid compact">{mine.map(badge)}</ul>
              </div>
            );
          })}
        </details>
      )}
    </section>
  );
}
