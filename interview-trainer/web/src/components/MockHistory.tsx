import { clockText, type MockRecord } from '@trainer/shared';
import { formatDay } from '../lib/format';
import { Icon } from './icons';

/** Mocks shown before the rest fold away under "Earlier mocks". */
const RECENT = 3;

/** The last ten mock interviews, newest first (the three latest in view). Scores are plain numbers: no ranking, no streak of mocks. */
export function MockHistory({ mocks }: { mocks: readonly MockRecord[] }) {
  if (mocks.length === 0) return null;
  const best = Math.max(...mocks.map((m) => m.score));
  const row = (m: MockRecord, i: number) => (
    <li key={`${m.day}-${i}`} data-testid="mock-history-item">
      <span className="mock-history-day">{formatDay(m.day)}</span>
      <b>{m.score}%</b>
      <span className="muted">
        {m.total} questions · {clockText(m.usedSec * 1000)} of {m.limitMin}:00{m.timedOut ? ' · time up' : ''}{m.score === best && mocks.length > 1 ? ' · best' : ''}
      </span>
    </li>
  );
  const older = mocks.slice(RECENT);
  return (
    <section className="mock-history" aria-labelledby="mock-history-title" data-testid="mock-history">
      <h2 id="mock-history-title">Your last {mocks.length === 1 ? 'mock' : `${mocks.length} mocks`}</h2>
      <ol>{mocks.slice(0, RECENT).map(row)}</ol>
      {older.length > 0 && (
        <details className="more-mocks" data-testid="older-mocks">
          <summary><Icon name="chevron-down" className="chev" />Earlier mocks ({older.length})</summary>
          <ol start={RECENT + 1}>{older.map((m, i) => row(m, i + RECENT))}</ol>
        </details>
      )}
    </section>
  );
}
