import type { SessionSummary } from '@trainer/shared';

export interface Finished {
  title: string;
  summary: SessionSummary;
  streak: number;
  nextLessonId?: string;
}

export function Summary({ finished, onHome }: { finished: Finished; onHome: () => void }) {
  const { summary, streak } = finished;
  const pct = Math.round(summary.accuracy * 100);
  const outstanding = summary.missed.filter((id) => !summary.recovered.includes(id)).length;
  return (
    <div className="summary">
      <div className="trophy" aria-hidden="true">{summary.perfect ? '🏆' : pct >= 70 ? '🎉' : '💪'}</div>
      <h1>{summary.perfect ? 'Perfect lesson!' : pct >= 70 ? 'Lesson complete!' : 'Keep going!'}</h1>
      <p className="muted">{finished.title}</p>

      <div className="tiles">
        <div className="tile xp"><small>Total XP</small><b>⚡ {summary.xp}</b></div>
        <div className="tile acc"><small>First-try accuracy</small><b>🎯 {pct}%</b></div>
        <div className="tile fire"><small>Day streak</small><b>🔥 {streak}</b></div>
      </div>

      {outstanding > 0 && (
        <p className="muted">
          {outstanding} question{outstanding === 1 ? '' : 's'} to revisit. {outstanding === 1 ? 'It is' : 'They are'} saved in <a href="#/review">Review mistakes</a>.
        </p>
      )}

      <div className="summary-actions">
        {finished.nextLessonId && <a className="btn secondary" href={`#/lesson/${finished.nextLessonId}`}>Next lesson</a>}
        <button type="button" className="btn primary" onClick={onHome} autoFocus>Continue</button>
      </div>
    </div>
  );
}
