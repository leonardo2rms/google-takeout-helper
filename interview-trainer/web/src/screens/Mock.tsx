import { useState } from 'react';
import {
  MOCK_LENGTHS, MOCK_MINUTES, MOCK_REWARDED_PER_DAY, MOCK_XP_CAP, applyMock, buildMock, dayKey, defaultMinutes, mockSeed,
  type Course, type MockConfig, type MockLength, type Progress, type Question,
} from '@trainer/shared';
import { MockHistory } from '../components/MockHistory';
import { MockResults, type SavedMock } from '../components/MockResults';
import { MockRun, type MockRunResult } from '../components/MockRun';
import type { GradeFn, Outcome } from '../lib/evaluate';
import { runCodeInWorker } from '../lib/run-code';
import { markMock, mockResults } from '../lib/mock-marking';
import { updateProgress } from '../lib/store';
import { useToday } from '../lib/today';

interface Props {
  course: Course;
  progress: Progress;
  /** AI grading for open answers, when available. */
  grade?: GradeFn;
}

type Stage =
  | { name: 'setup' }
  | { name: 'running'; questions: Question[]; minutes: number }
  | { name: 'marking'; questions: Question[]; minutes: number; run: MockRunResult; done: number }
  | { name: 'results'; questions: Question[]; minutes: number; run: MockRunResult; outcomes: Record<string, Outcome>; saved: SavedMock | null };

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
      <div className="summary" data-testid="mock-marking">
        <div className="trophy" aria-hidden="true">📝</div>
        <h1>{stage.run.timedOut ? 'Time is up' : 'Marking your answers'}</h1>
        <p className="muted" role="status">Checking {stage.done} of {answered}…</p>
      </div>
    );
  }

  if (stage.name === 'results') {
    const { questions, run, minutes } = stage;
    const flagged = new Set(run.flagged);
    const save = () => {
      let saved: SavedMock | null = null;
      const todayKey = dayKey(new Date());
      updateProgress((p) => {
        const res = applyMock(
          p,
          { results: mockResults(course, questions, run.answers, stage.outcomes, flagged), limitMin: minutes, usedSec: run.usedSec, timedOut: run.timedOut },
          { today: todayKey, course },
        );
        saved = { credit: res.credit, wrong: res.report.wrong.length };
        return res.progress;
      });
      setStage({ ...stage, saved });
    };
    return (
      <MockResults
        course={course}
        questions={questions}
        answers={run.answers}
        outcomes={stage.outcomes}
        flagged={flagged}
        usedSec={run.usedSec}
        limitMin={minutes}
        timedOut={run.timedOut}
        saved={stage.saved}
        onOutcome={(id, o) => setStage({ ...stage, outcomes: { ...stage.outcomes, [id]: o } })}
        onSave={save}
        onAnother={() => setStage({ name: 'setup' })}
      />
    );
  }

  const unitTitle = cfg.unitId ? course.units.find((u) => u.id === cfg.unitId)?.title : null;
  return (
    <div className="home mock-setup">
      <header className="page-top">
        <a className="icon-btn back" href="#/" aria-label="Back to home">←</a>
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
        <li>You can flag a question, skip it and come back before you submit.</li>
        <li>Open answers are marked at the end: by the AI when it is available, otherwise by you against the model answer.</li>
        <li>Questions you get wrong join your review queue. Mock practice earns up to {MOCK_XP_CAP} XP for the first {MOCK_REWARDED_PER_DAY} mocks of a day and counts for at most half of your daily goal.</li>
      </ul>

      <button type="button" className="btn primary wide" data-testid="mock-start" onClick={start}>
        Start{unitTitle ? ` · ${unitTitle}` : ''} · {cfg.length} questions · {cfg.minutes} min
      </button>

      <MockHistory mocks={progress.mocks} />
    </div>
  );
}
