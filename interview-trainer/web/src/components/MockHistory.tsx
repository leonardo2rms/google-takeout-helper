import { clockText, type MockRecord } from '@trainer/shared';

/** The last ten mock interviews, newest first. Scores are plain numbers: no ranking, no streak of mocks. */
export function MockHistory({ mocks }: { mocks: readonly MockRecord[] }) {
  if (mocks.length === 0) return null;
  const best = Math.max(...mocks.map((m) => m.score));
  return (
    <section className="mock-history" aria-labelledby="mock-history-title" data-testid="mock-history">
      <h2 id="mock-history-title">Your last {mocks.length === 1 ? 'mock' : `${mocks.length} mocks`}</h2>
      <ol>
        {mocks.map((m, i) => (
          <li key={`${m.day}-${i}`} data-testid="mock-history-item">
            <span className="mock-history-day">{m.day}</span>
            <b>{m.score}%</b>
            <span className="muted">
              {m.total} questions · {clockText(m.usedSec * 1000)} of {m.limitMin}:00{m.timedOut ? ' · time up' : ''}{m.score === best && mocks.length > 1 ? ' · best' : ''}
            </span>
          </li>
        ))}
      </ol>
    </section>
  );
}
