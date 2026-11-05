import { useCallback, useEffect, useRef, useState } from 'react';
import {
  PASS_SCORE, createSession, currentQuestion, followUpAsOpen, pickFollowUp, progress as sessionProgress, submit, summarize,
  type Confidence, type OpenQ, type Question, type SessionSummary,
} from '@trainer/shared';
import { AskWhy } from '../components/AskWhy';
import { ChoiceQuestion } from '../components/ChoiceQuestion';
import { CodeBlock } from '../components/CodeBlock';
import { CodeQuestion } from '../components/CodeQuestion';
import { EstimateQuestion } from '../components/EstimateQuestion';
import { Feedback } from '../components/Feedback';
import { ConfidenceRow } from '../components/ConfidenceRow';
import { Hint } from '../components/Hint';
import { QuestionTools } from '../components/QuestionTools';
import { FillQuestion } from '../components/FillQuestion';
import { Inline } from '../components/Inline';
import { MatchQuestion } from '../components/MatchQuestion';
import { OpenQuestion } from '../components/OpenQuestion';
import { OrderQuestion } from '../components/OrderQuestion';
import { SpotQuestion } from '../components/SpotQuestion';
import type { ExplainFn } from '../lib/explain';
import { describeLearnerAnswer } from '../lib/explain';
import { scrollIntoViewGently } from '../lib/a11y';
import { evaluate, isPass, outcomeScore, skipOutcome, type Answer, type GradeFn, type Outcome } from '../lib/evaluate';
import { runCodeInWorker } from '../lib/run-code';
import { useProgress } from '../lib/store';

interface Props {
  title: string;
  color: string;
  questions: Question[];
  /** Grades open answers with AI; undefined means offline (self-assessment). */
  grade?: GradeFn;
  /** "Ask why" tutor chat; undefined means unavailable (the button is shown disabled). */
  explain?: ExplainFn;
  /** Optional: when a question will come back ("tomorrow", "in 3 days"), shown under the feedback. */
  reviewHint?: (q: Question, outcome: 'first' | 'retry' | 'failed') => string | null;
  /** Short vibration on a correct answer (where supported). */
  haptics?: boolean;
  onExit: () => void;
  onFinish: (summary: SessionSummary) => void;
}

type Phase = 'answer' | 'grading' | 'feedback';

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

/** The interviewer's follow-up, plus how the learner did on the main question it follows. */
interface FollowUpStage {
  q: OpenQ;
  main: Outcome;
}

function hash(s: string): number {
  let h = 2166136261;
  for (const c of s) h = Math.imul(h ^ c.charCodeAt(0), 16777619);
  return h >>> 0;
}

export function LessonScreen({ title, color, questions, grade, explain, reviewHint, haptics, onExit, onFinish }: Props) {
  const aiAvailable = Boolean(grade);
  const [session, setSession] = useState(() => createSession(questions));
  const [answer, setAnswer] = useState<Answer | null>(null);
  const [phase, setPhase] = useState<Phase>('answer');
  const [outcome, setOutcome] = useState<Outcome | null>(null);
  const [followUp, setFollowUp] = useState<FollowUpStage | null>(null);
  /** Questions whose hint was revealed (kept across retries; one reveal per question). */
  const [hinted, setHinted] = useState<ReadonlySet<string>>(() => new Set());
  /** Optional "How sure are you?" for the question on screen (off unless switched on in Settings). */
  const confidenceOn = useProgress().progress.settings.confidence;
  const [sure, setSure] = useState<Confidence | null>(null);

  const feedbackRef = useRef<HTMLDivElement>(null);
  const q = currentQuestion(session);
  /** What is on screen: the lesson's question, or the interviewer's follow-up to it. */
  const view: Question | undefined = followUp ? followUp.q : q;
  const attempt = q ? (session.attempts[q.id] ?? 0) : 0;
  const seed = q ? hash(q.id) + attempt * 977 : 0;

  // A follow-up applies after a (non-skipped) open answer, chosen by which rubric points were covered.
  const pending =
    !followUp && q?.type === 'open' && outcome?.open && !outcome.skipped ? pickFollowUp(q, outcome.open.covered) : null;

  // With a follow-up, the question counts as passed on the average of both answers.
  const overallScore = followUp && outcome ? (outcomeScore(followUp.main) + outcomeScore(outcome)) / 2 : null;
  const passed = outcome ? (overallScore !== null ? overallScore >= PASS_SCORE : isPass(outcome)) : false;

  const check = useCallback(async () => {
    if (!view || !answer || phase !== 'answer') return;
    setPhase('grading');
    const result = await evaluate(view, answer, grade, runCodeInWorker);
    setOutcome(result);
    setPhase('feedback');
  }, [view, answer, phase, grade]);

  const skip = useCallback(() => {
    if (!view || phase !== 'answer') return;
    setOutcome(skipOutcome(view));
    setPhase('feedback');
  }, [view, phase]);

  const next = useCallback(() => {
    if (!outcome || !q || phase !== 'feedback') return;
    setAnswer(null);
    setOutcome(null);
    setPhase('answer');

    if (pending && q.type === 'open') {
      setFollowUp({ q: followUpAsOpen(q, pending.index), main: outcome });
      return;
    }
    const pass = followUp ? (outcomeScore(followUp.main) + outcomeScore(outcome)) / 2 >= PASS_SCORE : isPass(outcome);
    setFollowUp(null);
    const s = submit(session, pass, { hinted: hinted.has(q.id), confidence: sure ?? undefined });
    setSure(null);
    if (currentQuestion(s) === undefined) onFinish(summarize(s));
    else setSession(s);
  }, [outcome, q, phase, pending, followUp, session, hinted, sure, onFinish]);

  // A tiny tick on a correct answer, if the learner allows it and the device can.
  useEffect(() => {
    if (phase === 'feedback' && passed && haptics) navigator.vibrate?.(10);
  }, [phase, passed, haptics]);

  // Bring the feedback into view above the fixed bottom bar; on phones it starts below the fold.
  useEffect(() => {
    if (phase === 'feedback') scrollIntoViewGently(feedbackRef.current);
  }, [phase]);

  // Focus follows the flow, so keyboard and screen-reader users never get lost: a new question puts focus on its
  // prompt; checking an answer moves it to the verdict heading (Tab then reaches the explanation and "Continue").
  const promptRef = useRef<HTMLHeadingElement>(null);
  const viewKey = view ? `${view.id}#${attempt}` : '';
  useEffect(() => {
    promptRef.current?.focus({ preventScroll: true });
  }, [viewKey]);
  useEffect(() => {
    if (phase === 'feedback') feedbackRef.current?.querySelector<HTMLElement>('h2')?.focus({ preventScroll: true });
  }, [phase]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Enter' || e.repeat) return;
      if (e.target instanceof Element && e.target.closest('[data-no-lesson-keys]')) return; // the note editor keeps its own Enter
      if (e.target instanceof Element && e.target.closest('[role="dialog"]')) return; // a sheet (Ask why) owns its keys
      const inText = e.target instanceof HTMLTextAreaElement;
      if (inText && !(e.ctrlKey || e.metaKey)) return;
      if (e.target instanceof HTMLButtonElement) return; // keep native activation
      if (e.target instanceof HTMLInputElement && e.target.type === 'checkbox') return;
      e.preventDefault();
      if (phase === 'answer') void check();
      else if (phase === 'feedback') next();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [phase, check, next]);

  if (!q || !view) return null;
  const checked = phase !== 'answer';
  const key = `${view.id}#${attempt}`;

  return (
    <div className="lesson" style={{ ['--accent' as string]: color }}>
      <header className="lesson-top">
        <button
          type="button"
          className="icon-btn"
          aria-label="Quit lesson"
          onClick={() => {
            if (window.confirm('Quit this lesson? Progress in it will be lost.')) onExit();
          }}
        >
          ✕
        </button>
        <div className="bar" role="progressbar" aria-label={title} aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(sessionProgress(session) * 100)}>
          <div className="bar-fill" style={{ width: `${Math.max(sessionProgress(session) * 100, 3)}%` }} />
        </div>
        <span className="xp-chip" aria-label={`${session.xp} XP earned`}>⚡ {session.xp}</span>
      </header>

      <div className="lesson-body">
        <p className="kicker">
          {attempt > 0 ? '↻ Try again · ' : ''}
          {followUp ? 'Interviewer follow-up' : LABEL[view.type]}
        </p>
        <h1 className="prompt" ref={promptRef} tabIndex={-1}><Inline text={view.prompt} /></h1>
        {view.code && <CodeBlock source={view.code.source} lang={view.code.lang} />}
        {!followUp && view.hint && (
          <Hint
            text={view.hint}
            revealed={hinted.has(view.id)}
            canReveal={phase === 'answer'}
            onReveal={() => setHinted((h) => new Set(h).add(view.id))}
          />
        )}

        <div key={key} className="answer-area">
          {(view.type === 'single' || view.type === 'multi') && <ChoiceQuestion q={view} seed={seed} checked={checked} onAnswer={setAnswer} />}
          {view.type === 'fill' && <FillQuestion q={view} seed={seed} checked={checked} onAnswer={setAnswer} />}
          {view.type === 'order' && <OrderQuestion q={view} seed={seed} checked={checked} onAnswer={setAnswer} />}
          {view.type === 'spot' && <SpotQuestion q={view} checked={checked} onAnswer={setAnswer} />}
          {view.type === 'estimate' && <EstimateQuestion q={view} checked={checked} onAnswer={setAnswer} />}
          {view.type === 'match' && <MatchQuestion q={view} seed={seed} checked={checked} onAnswer={setAnswer} />}
          {view.type === 'code' && <CodeQuestion q={view} checked={checked} run={runCodeInWorker} onAnswer={setAnswer} />}
          {view.type === 'open' && <OpenQuestion q={view} checked={checked} aiAvailable={aiAvailable} onAnswer={setAnswer} />}
        </div>

        {confidenceOn && phase === 'answer' && attempt === 0 && !followUp && view.type !== 'open' && view.type !== 'code' && (
          <ConfidenceRow value={sure} onChange={setSure} />
        )}

        {phase === 'feedback' && outcome && (
          <div ref={feedbackRef} className="feedback-anchor">
            <Feedback
              q={view}
              outcome={outcome}
              overall={overallScore === null ? undefined : { score: overallScore, pass: passed }}
              onCoveredChange={(covered) => setOutcome({ ...outcome, open: outcome.open && { ...outcome.open, covered } })}
              actions={
                <AskWhy
                  key={`${view.id}#${attempt}`}
                  questionId={view.id}
                  answer={describeLearnerAnswer(view, answer, Boolean(outcome.skipped))}
                  correct={passed}
                  explain={explain}
                />
              }
            />
            <QuestionTools q={q} />
            {sure !== null && !followUp && (passed ? sure === 1 : sure === 3) && (
              <p className="confidence-note" data-testid="confidence-note">
                {passed ? 'Right, though you were unsure: this one will come back soon to make it stick.' : 'Worth a closer look: you were sure. A surprise like this is the best moment to learn it, so it comes back soon.'}
              </p>
            )}
            {pending && <p className="interviewer-note">🎤 The interviewer has a follow-up question for you.</p>}
            {!pending && !followUp && reviewHint && (() => {
              const hint = reviewHint(view, passed && attempt === 0 ? 'first' : passed ? 'retry' : 'failed');
              return hint ? <p className="srs-note" data-testid="srs-note">{hint}</p> : null;
            })()}
          </div>
        )}
      </div>

      {/* Announces the wait for AI grading / test runs; the verdict itself is announced by moving focus to it. */}
      <p className="sr-only" role="status" aria-live="polite">{phase === 'grading' ? (view.type === 'code' ? 'Running your code' : 'Checking your answer') : ''}</p>

      <footer className={`lesson-bar ${phase === 'feedback' && !pending ? (passed ? 'good' : 'bad') : ''}`}>
        {phase === 'feedback' ? (
          <button type="button" className="btn primary wide" onClick={next}>Continue</button>
        ) : (
          <>
            <button type="button" className="btn ghost" onClick={skip} disabled={phase === 'grading'}>I don't know</button>
            <button type="button" className="btn primary" onClick={() => void check()} disabled={!answer || phase === 'grading'}>
              {phase === 'grading' ? (view.type === 'code' ? 'Running…' : 'Grading…') : 'Check'}
            </button>
          </>
        )}
      </footer>
    </div>
  );
}
