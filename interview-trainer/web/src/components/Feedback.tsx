import { scoreFromCoverage, spotLines, verdictFor, type Question } from '@trainer/shared';
import type { ReactNode } from 'react';
import { isPass, type Outcome } from '../lib/evaluate';
import { CodeBlock } from './CodeBlock';
import { Icon, type IconName } from './icons';
import { Inline } from './Inline';
import { TestResults } from './TestResults';

interface Props {
  q: Question;
  outcome: Outcome;
  /** Set on an interviewer follow-up: the combined result of the main answer and this one. */
  overall?: { score: number; pass: boolean };
  onCoveredChange: (covered: boolean[]) => void;
  /** Extra actions at the bottom of the panel (e.g. "Ask why"). */
  actions?: ReactNode;
}

function solution(q: Question): { text: string; lang: string } | null {
  if (q.type === 'fill') {
    const text = q.template.replace(/\[\[(\d+)\]\]/g, (_m, n: string) => q.answers[Number(n) - 1] ?? '?');
    return { text, lang: q.lang };
  }
  if (q.type === 'order' && q.kind === 'code') return { text: q.lines.join('\n'), lang: q.lang };
  return null;
}

export function Feedback({ q, outcome, overall, onCoveredChange, actions }: Props) {
  const pass = isPass(outcome);
  const open = outcome.open;
  const oq = q.type === 'open' ? q : null;
  const score = open ? scoreFromCoverage(open.covered) : pass ? 1 : 0;
  const verdict = open ? verdictFor(score) : pass ? 'correct' : 'incorrect';

  // On a follow-up the card shows this answer, the bar shows the combined result: when they disagree, say so with amber.
  const own = pass ? 'good' : verdict === 'partial' ? 'meh' : 'bad';
  const tone = overall && (overall.pass ? 'good' : 'bad') !== own ? 'meh' : own;
  const sol = !pass ? solution(q) : null;
  const answerLine = q.type === 'estimate' ? estimateLine(q) : null;
  /** Comparing with the model answer is the learner's job: until they have, the verdict mark is neutral ink, not a hue. */
  const selfCheck = Boolean(open?.needsSelfAssessment) && !outcome.skipped;
  const word = verdictWord(tone, outcome);
  const covered = open ? open.covered.filter(Boolean).length : 0;
  const modelAnswer = oq && (
    <details open={!pass || outcome.skipped || selfCheck || undefined}>
      <summary><Icon name="chevron-down" className="chev" />Model answer</summary>
      {oq.format === 'code' ? <CodeBlock source={oq.modelAnswer} /> : <p><Inline text={oq.modelAnswer} /></p>}
    </details>
  );

  return (
    <section className={`feedback ${tone}`} aria-labelledby="feedback-title">
      {/* The verdict is said once: icon, word and the rail. This heading is what keyboard and screen-reader focus lands on. */}
      <h2 id="feedback-title" className={`verdict ${selfCheck ? 'neutral' : ''}`} tabIndex={-1} data-testid="verdict">
        <Icon name={selfCheck ? 'square-pen' : VERDICT_ICON[tone]} size="1.1em" strokeWidth={2.5} />
        <span>{word}</span>
      </h2>

      {open && oq && (
        <>
          <p className="score">
            {selfCheck ? `${covered} of ${open.covered.length} points ticked` : `${covered}/${open.covered.length} key points`}
            {open.gradedBy === 'ai' ? ' · graded by AI' : ''}
          </p>
          {open.feedback && <p className="ai-feedback"><Inline text={open.feedback} /></p>}
          {selfCheck && <p className="hint">Compare your answer with the model answer, then tick the points you made.</p>}
          {/* Self-checking is comparing: the model answer is open for it, and for any miss or skip. */}
          {selfCheck && modelAnswer}
          {selfCheck && <p className="rubric-title">Which points did you make?</p>}
          <ul className="rubric" aria-label={selfCheck ? 'Which points did you make?' : 'Key points'}>
            {oq.keyPoints.map((kp, i) => (
              <li key={i}>
                {open.needsSelfAssessment ? (
                  <label>
                    <input
                      type="checkbox"
                      checked={open.covered[i] ?? false}
                      onChange={(e) => onCoveredChange(open.covered.map((c, j) => (j === i ? e.target.checked : c)))}
                    />
                    <span><Inline text={kp.point} /></span>
                  </label>
                ) : (
                  <span className={open.covered[i] ? 'yes' : 'no'}>
                    <Icon name={open.covered[i] ? 'check' : 'x'} strokeWidth={2.5} />
                    <span><span className="sr-only">{open.covered[i] ? 'Covered: ' : 'Missed: '}</span><Inline text={kp.point} /></span>
                  </span>
                )}
              </li>
            ))}
          </ul>
          {!selfCheck && modelAnswer}
        </>
      )}

      {overall && (
        <p className="overall">
          Combined result: <b>{Math.round(overall.score * 100)}%</b> over both answers, which {overall.pass ? 'counts as correct' : 'is not enough yet'}.
        </p>
      )}

      {q.type === 'code' && outcome.code && !outcome.skipped && <TestResults result={outcome.code} />}

      {q.type === 'code' && (
        <details open={!pass}>
          <summary><Icon name="chevron-down" className="chev" />Reference solution</summary>
          <CodeBlock source={q.solution} lang="javascript" />
          {q.kotlin && (
            <>
              <p className="hint">The same idea in Kotlin:</p>
              <CodeBlock source={q.kotlin} lang="kotlin" />
            </>
          )}
        </details>
      )}

      {q.explanation && (
        <p className="explanation"><Inline text={q.explanation} /></p>
      )}

      {answerLine && <p className="answer-line">{answerLine}</p>}

      {q.type === 'match' && !pass && (
        <ul className="match-solution">
          {q.pairs.map((p) => (
            <li key={p.left}><b><Inline text={p.left} /></b> <Icon name="arrow-right" size="1em" /> <Inline text={p.right} /></li>
          ))}
        </ul>
      )}

      {q.type === 'order' && q.kind === 'steps' && !pass && (
        <>
          <p className="hint">Correct order</p>
          <ol className="steps-solution">
            {q.lines.map((l) => <li key={l}>{l}</li>)}
          </ol>
        </>
      )}

      {q.type === 'spot' && !pass && (
        <ul className="spot-solution">
          {q.answers.map((n) => (
            <li key={n}>Line {n}: <code>{spotLines(q.source)[n - 1]?.trim()}</code></li>
          ))}
        </ul>
      )}

      {sol && (
        <>
          <p className="hint">Correct solution</p>
          <CodeBlock source={sol.text} lang={sol.lang} />
        </>
      )}

      {actions}
    </section>
  );
}

const VERDICT_ICON: Record<'good' | 'bad' | 'meh', IconName> = { good: 'check', bad: 'x', meh: 'contrast' };

/** The verdict as one word, so it never depends on colour or on the icon alone. Points and detail live in their own line. */
function verdictWord(tone: 'good' | 'bad' | 'meh', outcome: Outcome): string {
  if (outcome.skipped) return 'Skipped';
  if (outcome.open?.needsSelfAssessment) return 'Self-check';
  if (tone === 'good') return 'Correct';
  if (tone === 'meh') return 'Partial';
  return 'Incorrect';
}

function estimateLine(q: Extract<Question, { type: 'estimate' }>): string {
  const unit = q.unit ? ` ${q.unit}` : '';
  const tol = q.tolerance === 0 ? 'exact' : `accepted: ${trim(q.answer * (1 - q.tolerance))} to ${trim(q.answer * (1 + q.tolerance))}${unit}`;
  return `Answer: ${trim(q.answer)}${unit} (${tol})`;
}

const trim = (n: number) => String(Math.round(n * 100) / 100);
