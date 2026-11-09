import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import {
  MOCK_LENGTHS, MOCK_MINUTES, MOCK_REWARDED_PER_DAY, MOCK_XP_CAP, applyMock, buildMock, dayKey, defaultMinutes, mockSeed,
  type Course, type MockConfig, type MockLength, type Progress, type Question,
} from '@trainer/shared';
import { Icon } from '../components/icons';
import { MockHistory } from '../components/MockHistory';
import { MockResults, type SavedMock } from '../components/MockResults';
import { MockRun, type MockRunResult } from '../components/MockRun';
import type { GradeFn, Outcome } from '../lib/evaluate';
import { runCodeInWorker } from '../lib/run-code';
import { isAnswered, markMock, mockResults } from '../lib/mock-marking';
import { updateProgress } from '../lib/store';
import { useToday } from '../lib/today';

interface Props {
  course: Course;
  progress: Progress;
  /** AI grading for open answers, when available. */
  grade?: GradeFn;
}

type ResultsStage = { name: 'results'; questions: Question[]; minutes: number; run: MockRunResult; outcomes: Record<string, Outcome>; saved: SavedMock | null };
type Stage =
  | { name: 'setup' }
  | { name: 'running'; questions: Question[]; minutes: number }
  | { name: 'marking'; questions: Question[]; minutes: number; run: MockRunResult; done: number }
  | ResultsStage;

/**
 * What to do when the learner leaves while a result is still unsaved. The listeners are registered once, when this module
 * loads, which is before the app mounts and so before its own `hashchange` listener. That order matters: React renders a
 * route change right after the app's listener returns, which unmounts the results screen, and a listener added by the screen
 * itself would be removed before it got its turn.
 */
let saveOnLeave: (() => void) | null = null;
if (typeof window !== 'undefined') {
  for (const type of ['hashchange', 'pagehide'] as const) window.addEventListener(type, () => saveOnLeave?.());
}

/**
 * The scorecard stage. The result is saved without a button press, so it can never be lost by leaving:
 * - when nothing is left to self-assess (the AI marked everything, or there were no open answers) it is saved as soon as it shows;
 * - when the learner has open answers to tick against the model answer (no AI), "Save results" is how they say they are done,
 *   and leaving the page by any route (Back, a link, closing the tab) saves what is on screen, so nothing is dropped.
 */
function ResultsScreen({ course, stage, setStage }: { course: Course; stage: ResultsStage; setStage: (s: Stage) => void }) {
  const latest = useRef(stage);
  latest.current = stage;
  const needsSelfCheck = stage.questions.some((q) => q.type === 'open' && isAnswered(q, stage.run.answers[q.id]) && stage.outcomes[q.id]?.open?.needsSelfAssessment);

  const save = () => {
    const s = latest.current;
    if (s.saved) return;
    let saved: SavedMock | null = null;
    updateProgress((p) => {
      const res = applyMock(
        p,
        { results: mockResults(course, s.questions, s.run.answers, s.outcomes, new Set(s.run.flagged)), limitMin: s.minutes, usedSec: s.run.usedSec, timedOut: s.run.timedOut },
        { today: dayKey(new Date()), course },
      );
      saved = { credit: res.credit, wrong: res.report.wrong.length };
      return res.progress;
    });
    latest.current = { ...s, saved }; // at once: a second trigger (leaving right after) must not save twice
    setStage(latest.current);
  };

  const autosaved = useRef(false); // refs survive the double effect run of StrictMode
  useEffect(() => {
    if (autosaved.current || needsSelfCheck) return;
    autosaved.current = true;
    save();
  }, []);

  // A layout effect, not a passive one: it runs at commit, before the screen can receive a click, so there is no gap in which
  // leaving would find nothing registered.
  useLayoutEffect(() => {
    saveOnLeave = save;
    return () => {
      if (saveOnLeave === save) saveOnLeave = null;
    };
  }, []);

  return (
    <MockResults
      course={course}
      questions={stage.questions}
      answers={stage.run.answers}
      outcomes={stage.outcomes}
      flagged={new Set(stage.run.flagged)}
      usedSec={stage.run.usedSec}
      limitMin={stage.minutes}
      timedOut={stage.run.timedOut}
      saved={stage.saved}
      onOutcome={(id, o) => setStage({ ...stage, outcomes: { ...stage.outcomes, [id]: o } })}
      onSave={save}
      onAnother={() => setStage({ name: 'setup' })}
    />
  );
}

/**
 * A timed mock interview. Opt-in, no feedback while it runs, open answers marked at the end (by the AI when it is
 * available, otherwise by the learner against the model answer). Quitting at any point loses nothing and costs nothing.
 */
export function MockScreen({ course, progress, grade }: Props) {
  const today = useToday();
  const [stage, setStage] = useState<Stage>({ name: 'setup' });
  const [cfg, setCfg] = useState<MockConfig>({ length: 10, minutes: defaultMinutes(10), unitId: null });

  const start = () => {
    const questions = buildMock(course, progress, today, cfg, mockSeed(progress, today));
    if (questions.length === 0) return;
    setStage({ name: 'running', questions, minutes: cfg.minutes });
  };

  const finishRun = async (questions: Question[], minutes: number, run: MockRunResult) => {
    setStage({ name: 'marking', questions, minutes, run, done: 0 });
    const outcomes = await markMock(questions, run.answers, grade, runCodeInWorker, (done) =>
      setStage((s) => (s.name === 'marking' ? { ...s, done } : s)),
    );
    setStage({ name: 'results', questions, minutes, run, outcomes, saved: null });
  };

  if (stage.name === 'running') {
    return (
      <MockRun
        questions={stage.questions}
        minutes={stage.minutes}
        aiAvailable={Boolean(grade)}
        onQuit={() => setStage({ name: 'setup' })}
        onDone={(run) => void finishRun(stage.questions, stage.minutes, run)}
      />
    );
  }

  if (stage.name === 'marking') {
    const answered = stage.questions.length;
    return (
      <div className="summary plain" data-testid="mock-marking">
        <p className="label">Mock interview</p>
        <h1>{stage.run.timedOut ? 'Time is up' : 'Marking your answers'}</h1>
        <p className="muted" role="status">Checking {stage.done} of {answered}…</p>
      </div>
    );
  }

  if (stage.name === 'results') return <ResultsScreen course={course} stage={stage} setStage={setStage} />;

  const unitTitle = cfg.unitId ? course.units.find((u) => u.id === cfg.unitId)?.title : null;
  return (
    <div className="home mock-setup">
      <header className="page-top">
        <a className="icon-btn back" href="#/" aria-label="Back to home"><Icon name="arrow-left" /></a>
        <h1>Mock interview</h1>
      </header>

      <p>
        A timed run that feels like the real thing: mixed formats, weighted toward the topics you know least, <b>no feedback until the end</b>.
        It is optional, and quitting at any point costs nothing.
      </p>

      <section className="field" role="radiogroup" aria-labelledby="mock-len">
        <b id="mock-len">Length</b>
        <div className="mock-choices">
          {MOCK_LENGTHS.map((n) => (
            <button
              key={n}
              type="button"
              role="radio"
              aria-checked={cfg.length === n}
              className={`chip mock-choice ${cfg.length === n ? 'on' : ''}`}
              data-testid={`mock-length-${n}`}
              onClick={() => setCfg({ ...cfg, length: n as MockLength, minutes: defaultMinutes(n as MockLength) })}
            >
              {n} questions
            </button>
          ))}
        </div>
      </section>

      <section className="field">
        <label htmlFor="mock-minutes"><b>Time limit</b></label>
        <select id="mock-minutes" className="lib-select" data-testid="mock-minutes" value={cfg.minutes} onChange={(e) => setCfg({ ...cfg, minutes: Number(e.target.value) })}>
          {[...MOCK_MINUTES[cfg.length]].sort((a, b) => a - b).map((m) => <option key={m} value={m}>{m} minutes</option>)}
        </select>
      </section>

      <section className="field">
        <label htmlFor="mock-topic"><b>Topic</b></label>
        <select id="mock-topic" className="lib-select" data-testid="mock-topic" value={cfg.unitId ?? ''} onChange={(e) => setCfg({ ...cfg, unitId: e.target.value || null })}>
          <option value="">All topics (weighted to your weak spots)</option>
          {course.units.map((u) => <option key={u.id} value={u.id}>{u.title}</option>)}
        </select>
      </section>

      <ul className="mock-rules">
        <li>The timer is always visible and pauses if you leave the app. At zero your answers are submitted automatically.</li>
        <li>No feedback while it runs. You can flag a question, skip it and come back before you submit.</li>
      </ul>
      <details className="how-calc">
        <summary><Icon name="chevron-down" className="chev" />How scoring works</summary>
        <ul className="mock-rules">
          <li>Open answers are marked at the end: by the AI when it is available, otherwise by you against the model answer.</li>
          <li>Questions you get wrong join your review queue. Mock practice earns up to {MOCK_XP_CAP} XP for the first {MOCK_REWARDED_PER_DAY} mocks of a day and counts for at most half of your daily goal.</li>
        </ul>
      </details>

      <button type="button" className="btn primary wide" data-testid="mock-start" onClick={start}>
        Start{unitTitle ? ` · ${unitTitle}` : ''} · {cfg.length} questions · {cfg.minutes} min
      </button>

      <MockHistory mocks={progress.mocks} />
    </div>
  );
}
