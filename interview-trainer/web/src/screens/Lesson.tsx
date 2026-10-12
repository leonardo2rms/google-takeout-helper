import { useCallback, useEffect, useRef, useState } from 'react';
import {
  createSession, currentQuestion, progress as sessionProgress, submit, summarize,
  type Question, type SessionSummary,
} from '@trainer/shared';
import { ChoiceQuestion } from '../components/ChoiceQuestion';
import { CodeBlock } from '../components/CodeBlock';
import { Feedback } from '../components/Feedback';
import { Inline } from '../components/Inline';
import { FillQuestion } from '../components/FillQuestion';
import { OpenQuestion } from '../components/OpenQuestion';
import { OrderQuestion } from '../components/OrderQuestion';
import { gradeOpen } from '../lib/api';
import { evaluate, isPass, skipOutcome, type Answer, type Outcome } from '../lib/evaluate';

interface Props {
  title: string;
  color: string;
  questions: Question[];
  aiAvailable: boolean;
  onExit: () => void;
  onFinish: (summary: SessionSummary) => void;
}

type Phase = 'answer' | 'grading' | 'feedback';

const LABEL: Record<Question['type'], string> = {
  single: 'Choose the best answer',
  multi: 'Select all that apply',
  fill: 'Complete the code',
  order: 'Put the code in order',
  open: 'Answer in your own words',
};

function hash(s: string): number {
  let h = 2166136261;
  for (const c of s) h = Math.imul(h ^ c.charCodeAt(0), 16777619);
  return h >>> 0;
}

export function LessonScreen({ title, color, questions, aiAvailable, onExit, onFinish }: Props) {
  const [session, setSession] = useState(() => createSession(questions));
  const [answer, setAnswer] = useState<Answer | null>(null);
  const [phase, setPhase] = useState<Phase>('answer');
  const [outcome, setOutcome] = useState<Outcome | null>(null);

  const feedbackRef = useRef<HTMLDivElement>(null);
  const q = currentQuestion(session);
  const attempt = q ? (session.attempts[q.id] ?? 0) : 0;
  const seed = q ? hash(q.id) + attempt * 977 : 0;

  const check = useCallback(async () => {
    if (!q || !answer || phase !== 'answer') return;
    setPhase('grading');
    const result = await evaluate(q, answer, aiAvailable ? gradeOpen : undefined);
    setOutcome(result);
    setPhase('feedback');
  }, [q, answer, phase, aiAvailable]);

  const skip = useCallback(() => {
    if (!q || phase !== 'answer') return;
    setOutcome(skipOutcome(q));
    setPhase('feedback');
  }, [q, phase]);

  const next = useCallback(() => {
    if (!outcome || phase !== 'feedback') return;
    const s = submit(session, isPass(outcome));
    setAnswer(null);
    setOutcome(null);
    setPhase('answer');
    if (currentQuestion(s) === undefined) onFinish(summarize(s));
    else setSession(s);
  }, [outcome, phase, session, onFinish]);

  // Bring the feedback into view above the fixed bottom bar; on phones it starts below the fold.
  useEffect(() => {
    if (phase === 'feedback') feedbackRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }, [phase]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Enter' || e.repeat) return;
      const inText = e.target instanceof HTMLTextAreaElement;
      if (inText && !(e.ctrlKey || e.metaKey)) return;
      if (e.target instanceof HTMLButtonElement || e.target instanceof HTMLInputElement) return; // keep native activation
      e.preventDefault();
      if (phase === 'answer') void check();
      else if (phase === 'feedback') next();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [phase, check, next]);

  if (!q) return null;
  const checked = phase !== 'answer';
  const key = `${q.id}#${attempt}`;

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

      <main className="lesson-body">
        <p className="kicker">{attempt > 0 ? '↻ Try again · ' : ''}{LABEL[q.type]}</p>
        <h1 className="prompt"><Inline text={q.prompt} /></h1>
        {q.code && <CodeBlock source={q.code.source} lang={q.code.lang} />}

        <div key={key} className="answer-area">
          {(q.type === 'single' || q.type === 'multi') && <ChoiceQuestion q={q} seed={seed} checked={checked} onAnswer={setAnswer} />}
          {q.type === 'fill' && <FillQuestion q={q} seed={seed} checked={checked} onAnswer={setAnswer} />}
          {q.type === 'order' && <OrderQuestion q={q} seed={seed} checked={checked} onAnswer={setAnswer} />}
          {q.type === 'open' && <OpenQuestion q={q} checked={checked} aiAvailable={aiAvailable} onAnswer={setAnswer} />}
        </div>

        {phase === 'feedback' && outcome && (
          <div ref={feedbackRef} className="feedback-anchor">
            <Feedback q={q} outcome={outcome} onCoveredChange={(covered) => setOutcome({ ...outcome, open: outcome.open && { ...outcome.open, covered } })} />
          </div>
        )}
      </main>

      <footer className={`lesson-bar ${phase === 'feedback' ? (isPass(outcome!) ? 'good' : 'bad') : ''}`}>
        {phase === 'feedback' ? (
          <button type="button" className="btn primary wide" onClick={next} autoFocus>Continue</button>
        ) : (
          <>
            <button type="button" className="btn ghost" onClick={skip} disabled={phase === 'grading'}>I don't know</button>
            <button type="button" className="btn primary" onClick={() => void check()} disabled={!answer || phase === 'grading'}>
              {phase === 'grading' ? 'Grading…' : 'Check'}
            </button>
          </>
        )}
      </footer>
    </div>
  );
}
