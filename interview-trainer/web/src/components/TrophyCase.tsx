import { formatDay } from '../lib/format';
import { Icon, isIconName } from './icons';
import { ACHIEVEMENT_GROUPS, achievementStatus, type AchievementGroup, type Progress } from '@trainer/shared';

/** All badges, grouped. Locked ones are shown dimmed with what it takes: nothing is hidden, nothing expires. */
export function TrophyCase({ progress }: { progress: Progress }) {
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
        return (
          <div key={g} className="trophy-group">
            <h3>{ACHIEVEMENT_GROUPS[g]}</h3>
            <ul className="badge-grid">{mine.map(badge)}</ul>
          </div>
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
