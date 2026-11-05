import { useMemo } from 'react';
import { MOCK_WEAK_BELOW, PASS_SCORE, buildReport, clockText, type Course, type MockCredit, type Question } from '@trainer/shared';
import { isAnswered, mockResults } from '../lib/mock-marking';
import { outcomeScore, type Answer, type Outcome } from '../lib/evaluate';
import { Feedback } from './Feedback';
import { Icon } from './icons';
import { Inline } from './Inline';
import { unitStyle } from '../lib/unit-style';
import { plural } from '../lib/format';

export interface SavedMock {
  credit: MockCredit;
  /** Wrong answers that joined the review queue. */
  wrong: number;
}

interface Props {
  course: Course;
  questions: Question[];
  answers: Record<string, Answer | null>;
  outcomes: Record<string, Outcome>;
  flagged: ReadonlySet<string>;
  usedSec: number;
  limitMin: number;
  timedOut: boolean;
  /** Set once the result has been written to Progress. */
  saved: SavedMock | null;
  onOutcome: (id: string, o: Outcome) => void;
  onSave: () => void;
  onAnother: () => void;
}

function status(q: Question, answers: Record<string, Answer | null>, o: Outcome | undefined): { label: string; tone: 'good' | 'meh' | 'bad' | 'none' } {
  if (!isAnswered(q, answers[q.id]) || !o) return { label: 'Not answered', tone: 'none' };
  const s = outcomeScore(o);
  if (s >= PASS_SCORE) return { label: 'Correct', tone: 'good' };
  return s > 0 ? { label: 'Partly', tone: 'meh' } : { label: 'Incorrect', tone: 'bad' };
}

/** The scorecard: overall, by topic with weak spots, and every question with its explanation. Self-assessment of open answers happens here. */
export function MockResults({ course, questions, answers, outcomes, flagged, usedSec, limitMin, timedOut, saved, onOutcome, onSave, onAnother }: Props) {
  const results = useMemo(() => mockResults(course, questions, answers, outcomes, flagged), [course, questions, answers, outcomes, flagged]);
  const report = useMemo(() => buildReport(results), [results]);
  const titleOf = (id: string) => course.units.find((u) => u.id === id)?.title ?? id;
  const unitOf = (id: string) => course.units.find((u) => u.id === id);
  const pendingSelf = questions.filter((q) => q.type === 'open' && isAnswered(q, answers[q.id]) && outcomes[q.id]?.open?.needsSelfAssessment);
  const notReached = report.total - report.answered;

  return (
    <div className="home mock-results" data-testid="mock-results">
      <header className="page-top">
        <a className="icon-btn back" href="#/" aria-label="Back to home"><Icon name="arrow-left" /></a>
        <h1>Mock interview results</h1>
      </header>

      <section className="mock-score" aria-labelledby="mock-score-title">
        <h2 id="mock-score-title" className="sr-only">Overall</h2>
        <p className="result-big" data-testid="mock-score" role="img" aria-label={`${report.score} percent`}>{report.score}<small>%</small></p>
        <p>
          <b>{report.correct} of {report.total}</b> correct · {clockText(usedSec * 1000)} of {limitMin}:00 used
        </p>
        {timedOut && (
          <p className="muted" data-testid="mock-timed-out">
            Time was up{notReached > 0 ? `, so ${plural(notReached, 'question')} ${notReached === 1 ? 'was' : 'were'} not answered` : ''}. That is useful to know: pacing is part of the interview.
          </p>
        )}
        {!timedOut && notReached > 0 && <p className="muted">{plural(notReached, 'question')} left unanswered (counted as zero, never held against your review queue).</p>}
      </section>

      {!saved && pendingSelf.length === 0 && (
        <p className="muted" data-testid="mock-unsaved">Not saved yet. Saving adds the questions you missed to your review queue and records the XP.</p>
      )}

      {pendingSelf.length > 0 && !saved && (
        <p className="notice" role="note" data-testid="mock-self-note">
          AI grading is not available, so check your {plural(pendingSelf.length, 'open answer')} yourself: open each one, compare with the model answer and tick the points you covered. The score above updates as you tick.
        </p>
      )}

      <section aria-labelledby="mock-topics-title">
        <h2 id="mock-topics-title">By topic</h2>
        <ul className="topics" data-testid="mock-topics">
          {report.byTopic.map((t) => (
            <li key={t.unitId} data-testid={`mock-topic-${t.unitId}`} data-weak={t.weak} style={unitStyle(unitOf(t.unitId) ?? { id: t.unitId }).style}>
              <div className="topic-head">
                <span className="glyph" aria-hidden="true"><Icon name={unitStyle(unitOf(t.unitId) ?? { id: t.unitId }).icon} /></span>
                <b>{titleOf(t.unitId)}</b>
                <span className="muted">{t.pct}%</span>
              </div>
              <div className="meter" role="progressbar" aria-label={`${titleOf(t.unitId)} score`} aria-valuemin={0} aria-valuemax={100} aria-valuenow={t.pct}>
                <div style={{ width: `${t.pct}%` }} />
              </div>
              <div className="topic-foot">
                <small className="muted">{plural(t.n, 'question')}{t.weak ? ' · weak spot' : ''}</small>
                {t.weak && <a className="practise-link" href={`#/topic/${t.unitId}`} aria-label={`Practise ${titleOf(t.unitId)}`}>Practise</a>}
              </div>
            </li>
          ))}
        </ul>
        {report.weakSpots.length > 0 ? (
          <p data-testid="mock-weak">Weak spots: <b>{report.weakSpots.map(titleOf).join(', ')}</b>. They are good candidates for your next practice.</p>
        ) : (
          <p className="muted">No topic fell below {MOCK_WEAK_BELOW}%.</p>
        )}
      </section>

      <section aria-labelledby="mock-q-title">
        <h2 id="mock-q-title">Question by question</h2>
        <ol className="mock-qlist">
          {questions.map((q, i) => {
            const o = outcomes[q.id];
            const st = status(q, answers, o);
            const needsTick = q.type === 'open' && o?.open?.needsSelfAssessment && isAnswered(q, answers[q.id]) && !saved;
            return (
              <li key={q.id} className={`mock-q ${st.tone}`} data-testid={`mock-q-${i + 1}`} data-status={st.label}>
                <details open={Boolean(needsTick)}>
                  <summary>
                    <span className="mock-q-num">{i + 1}</span>
                    <span className="mock-q-text"><Inline text={q.prompt} /></span>
                    <span className="mock-q-state">
                      {st.tone !== 'none' && <Icon name={st.tone === 'good' ? 'check' : st.tone === 'bad' ? 'x' : 'contrast'} size="1.1em" strokeWidth={2.5} />}
                      {st.label}{flagged.has(q.id) ? ' · flagged' : ''}
                    </span>
                  </summary>
                  {o && <Feedback q={q} outcome={o} onCoveredChange={(covered) => onOutcome(q.id, { ...o, open: o.open && { ...o.open, covered } })} />}
                </details>
              </li>
            );
          })}
        </ol>
      </section>

      {saved && (
        <section className="mock-saved" role="status" data-testid="mock-saved">
          <h2>Saved</h2>
          <p>
            {saved.credit.rewarded ? `+${saved.credit.xp} XP` : 'No XP this time (mock practice earns it for the first two of a day)'}
            {saved.wrong > 0 ? `. ${plural(saved.wrong, 'question')} you missed ${saved.wrong === 1 ? 'is' : 'are'} in your review queue.` : '. Nothing to review from this one.'}
          </p>
          <p className="muted">Mock practice counts for at most half of your daily goal, so it can never replace the daily session.</p>
          {saved.wrong > 0 && <a className="btn secondary" href="#/review">Review those now</a>}
        </section>
      )}

      <footer className="lesson-bar mock-results-bar">
        {saved ? (
          <>
            <button type="button" className="btn ghost" onClick={onAnother} data-testid="mock-another">Another mock</button>
            <a className="btn primary" href="#/" data-testid="mock-home">Home</a>
          </>
        ) : (
          <button type="button" className="btn primary wide" data-testid="mock-save" onClick={onSave}>Save results</button>
        )}
      </footer>
    </div>
  );
}
