import { ACHIEVEMENT_GROUPS, achievementStatus, type AchievementGroup, type Progress } from '@trainer/shared';

/** All badges, grouped. Locked ones are shown dimmed with what it takes: nothing is hidden, nothing expires. */
export function TrophyCase({ progress }: { progress: Progress }) {
  const status = achievementStatus(progress);
  const got = status.filter((s) => s.on).length;
  const groups = Object.keys(ACHIEVEMENT_GROUPS) as AchievementGroup[];
  return (
    <section className="trophies" aria-labelledby="trophy-title" data-testid="trophy-case">
      <h2 id="trophy-title">Badges <span className="muted" data-testid="badge-count">{got}/{status.length}</span></h2>
      {groups.map((g) => (
        <div key={g} className="trophy-group">
          <h3>{ACHIEVEMENT_GROUPS[g]}</h3>
          <ul className="badge-grid">
            {status.filter((s) => s.def.group === g).map(({ def, on }) => (
              <li
                key={def.id}
                className={`badge ${on ? 'got' : 'locked'}`}
                data-testid={`badge-${def.id}`}
                data-unlocked={on ? 'true' : 'false'}
              >
                <span className="badge-emoji" aria-hidden="true">{def.emoji}</span>
                <span className="badge-text">
                  <b>{def.title}</b>
                  <small>{def.description}</small>
                  <small className="badge-state">{on ? `Unlocked ${on}` : 'Locked'}</small>
                </span>
              </li>
            ))}
          </ul>
        </div>
      ))}
    </section>
  );
}
