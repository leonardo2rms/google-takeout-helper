import { useCallback, useEffect, useRef, useState } from 'react';
import { monotonicNow } from '../lib/monotonic';
import { clockText, elapsedMs, isTimeUp, pauseTimer, remainingMs, resumeTimer, startTimer, timeAnnouncement, type MockTimer, type Question } from '@trainer/shared';
import { isAnswered } from '../lib/mock-marking';
import { runCodeInWorker } from '../lib/run-code';
import type { Answer } from '../lib/evaluate';
import { CodeBlock } from './CodeBlock';
import { ChoiceQuestion } from './ChoiceQuestion';
import { CodeQuestion } from './CodeQuestion';
import { EstimateQuestion } from './EstimateQuestion';
import { FillQuestion } from './FillQuestion';
import { Icon } from './icons';
import { Inline } from './Inline';
import { MatchQuestion } from './MatchQuestion';
import { OpenQuestion } from './OpenQuestion';
import { OrderQuestion } from './OrderQuestion';
import { SpotQuestion } from './SpotQuestion';

export interface MockRunResult {
  answers: Record<string, Answer | null>;
  flagged: string[];
  usedSec: number;
  timedOut: boolean;
}

interface Props {
  questions: Question[];
  minutes: number;
  aiAvailable: boolean;
  onQuit: () => void;
  onDone: (r: MockRunResult) => void;
}

const LABEL: Record<Question['type'], string> = {
  single: 'Choose the best answer',
  multi: 'Select all that apply',
  fill: 'Complete the code',
  order: 'Put in the right order',
  spot: 'Find the bug',
  estimate: 'Estimate the number',
  match: 'Match the pairs',
  code: 'Write the code',
  open: 'Answer in your own words',
};

function hash(s: string): number {
  let h = 2166136261;
  for (const c of s) h = Math.imul(h ^ c.charCodeAt(0), 16777619);
  return h >>> 0;
}

/**
 * A timed run: one question at a time, free to move back and forth, flag and skip, with NO feedback of any kind until the
 * end. Questions that have been visited stay mounted (hidden) so their answers survive navigation. The timer is
 * timestamp-based, pauses while the app is hidden and submits automatically at zero.
 */
export function MockRun({ questions, minutes, aiAvailable, onQuit, onDone }: Props) {
  const [timer, setTimer] = useState<MockTimer>(() => startTimer(monotonicNow(), minutes));
  const [now, setNow] = useState(() => monotonicNow());
  const [index, setIndex] = useState(0);
  const [visited, setVisited] = useState<ReadonlySet<number>>(() => new Set([0]));
  const [answers, setAnswers] = useState<Record<string, Answer | null>>({});
  const [flagged, setFlagged] = useState<ReadonlySet<string>>(() => new Set());
  const [confirming, setConfirming] = useState(false);
  const [announcement, setAnnouncement] = useState('');
  const submitted = useRef(false);
  const lastRemaining = useRef(Infinity);

  const submit = useCallback(
    (timedOut: boolean) => {
      if (submitted.current) return;
      submitted.current = true;
      const t = monotonicNow();
      onDone({ answers, flagged: [...flagged], usedSec: Math.round(Math.min(elapsedMs(timer, t), timer.limitMs) / 1000), timedOut });
    },
    [answers, flagged, timer, onDone],
  );

  // The clock: a tick each second reads a monotonic clock (the wall clock can be changed under us), so a throttled background tab cannot drift.
  useEffect(() => {
    const id = window.setInterval(() => setNow(monotonicNow()), 1000);
    return () => window.clearInterval(id);
  }, []);

  // Leaving the app pauses the timer; coming back resumes it.
  useEffect(() => {
    const onVis = () => setTimer((t) => (document.hidden ? pauseTimer(t, monotonicNow()) : resumeTimer(t, monotonicNow())));
    document.addEventListener('visibilitychange', onVis);
    return () => document.removeEventListener('visibilitychange', onVis);
  }, []);

  const left = remainingMs(timer, now);
  useEffect(() => {
    const said = timeAnnouncement(lastRemaining.current === Infinity ? timer.limitMs : lastRemaining.current, left);
    lastRemaining.current = left;
    if (said) setAnnouncement(said);
  }, [left, timer.limitMs]);

  useEffect(() => {
    if (isTimeUp(timer, now)) submit(true);
  }, [timer, now, submit]);

  const q = questions[index]!;
  const answeredCount = questions.filter((x) => isAnswered(x, answers[x.id])).length;
  const unanswered = questions.length - answeredCount;
  const go = (i: number) => {
    setIndex(i);
    setVisited((v) => new Set(v).add(i));
    setConfirming(false);
    window.scrollTo(0, 0);
  };
  const setAnswer = (id: string) => (a: Answer | null) => setAnswers((prev) => ({ ...prev, [id]: a }));
  const toggleFlag = () =>
    setFlagged((f) => {
      const n = new Set(f);
      if (!n.delete(q.id)) n.add(q.id);
      return n;
    });
  const isLast = index === questions.length - 1;

  return (
    <div className="lesson mock-run" data-testid="mock-run">
      <header className="lesson-top mock-top">
        <button
          type="button"
          className="icon-btn"
          aria-label="Quit mock interview"
          onClick={() => {
            if (window.confirm('Quit this mock interview? Nothing is saved and nothing is lost: it is only practice.')) onQuit();
          }}
        >
          <Icon name="x" />
        </button>
        <span className="mock-count" data-testid="mock-count">{index + 1} / {questions.length}</span>
        <span className={`mock-timer ${left <= 60_000 ? 'low' : ''}`} role="timer" aria-label="Time left" data-testid="mock-timer">
          <Icon name="timer" size="1.1em" /> {clockText(left)}
        </span>
      </header>
      <p className="sr-only" role="status" aria-live="polite" data-testid="mock-announce">{announcement}</p>

      <main className="lesson-body">
        <p className="kicker">{LABEL[q.type]}{flagged.has(q.id) ? ' · flagged' : ''}</p>
        <h1 className="prompt"><Inline text={q.prompt} /></h1>
        {q.code && <CodeBlock source={q.code.source} lang={q.code.lang} />}

        {questions.map((x, i) =>
          visited.has(i) ? (
            <div key={x.id} className="answer-area" hidden={i !== index} data-qid={x.id}>
              {(x.type === 'single' || x.type === 'multi') && <ChoiceQuestion q={x} seed={hash(x.id)} checked={false} onAnswer={setAnswer(x.id)} />}
              {x.type === 'fill' && <FillQuestion q={x} seed={hash(x.id)} checked={false} onAnswer={setAnswer(x.id)} />}
              {x.type === 'order' && <OrderQuestion q={x} seed={hash(x.id)} checked={false} onAnswer={setAnswer(x.id)} />}
              {x.type === 'spot' && <SpotQuestion q={x} checked={false} onAnswer={setAnswer(x.id)} />}
              {x.type === 'estimate' && <EstimateQuestion q={x} checked={false} onAnswer={setAnswer(x.id)} />}
              {x.type === 'match' && <MatchQuestion q={x} seed={hash(x.id)} checked={false} onAnswer={setAnswer(x.id)} />}
              {x.type === 'code' && <CodeQuestion q={x} checked={false} blind run={runCodeInWorker} onAnswer={setAnswer(x.id)} />}
              {x.type === 'open' && <OpenQuestion q={x} checked={false} aiAvailable={aiAvailable} onAnswer={setAnswer(x.id)} />}
            </div>
          ) : null,
        )}

        <nav className="mock-palette" aria-label="Questions">
          {questions.map((x, i) => {
            const done = isAnswered(x, answers[x.id]);
            const flag = flagged.has(x.id);
            return (
              <button
                key={x.id}
                type="button"
                className={`mock-dot ${i === index ? 'current' : ''} ${done ? 'answered' : ''} ${flag ? 'flagged' : ''}`}
                aria-current={i === index ? 'step' : undefined}
                aria-label={`Question ${i + 1}, ${done ? 'answered' : 'not answered'}${flag ? ', flagged' : ''}`}
                data-testid={`mock-dot-${i + 1}`}
                onClick={() => go(i)}
              >
                {i + 1}{flag ? <Icon name="flag" /> : null}
              </button>
            );
          })}
        </nav>
        <p className="hint mock-legend">Filled = answered · dashed with a flag = flagged to come back to · {answeredCount} of {questions.length} answered</p>
        {!isLast && !confirming && (
          <button type="button" className="btn ghost small" data-testid="mock-finish-early" onClick={() => setConfirming(true)}>Finish early</button>
        )}

        {confirming && (
          <section className="mock-confirm" role="alertdialog" aria-labelledby="mock-confirm-title" data-testid="mock-confirm">
            <h2 id="mock-confirm-title">Submit your answers?</h2>
            <p>
              {unanswered === 0 ? 'Every question has an answer.' : `${unanswered} ${unanswered === 1 ? 'question has' : 'questions have'} no answer yet and will count as zero.`}
              {flagged.size > 0 ? ` ${flagged.size} flagged.` : ''}
            </p>
            <div className="mock-confirm-actions">
              <button type="button" className="btn ghost" onClick={() => setConfirming(false)}>Keep going</button>
              <button type="button" className="btn primary" data-testid="mock-submit" onClick={() => submit(false)}>Submit</button>
            </div>
          </section>
        )}
      </main>

      <footer className="lesson-bar mock-bar">
        <button type="button" className={`btn ghost mock-flag ${flagged.has(q.id) ? 'on' : ''}`} aria-pressed={flagged.has(q.id)} data-testid="mock-flag" onClick={toggleFlag}>
          <Icon name="flag" size="1.1em" /> {flagged.has(q.id) ? 'Flagged' : 'Flag'}
        </button>
        <button type="button" className="btn ghost" data-testid="mock-prev" disabled={index === 0} onClick={() => go(index - 1)}>Back</button>
        {isLast ? (
          <button type="button" className="btn primary" data-testid="mock-finish" onClick={() => setConfirming(true)}>Finish</button>
        ) : (
          <button type="button" className="btn primary" data-testid="mock-next" onClick={() => go(index + 1)}>
            {isAnswered(q, answers[q.id]) ? 'Next' : 'Skip'}
          </button>
        )}
      </footer>
    </div>
  );
}
