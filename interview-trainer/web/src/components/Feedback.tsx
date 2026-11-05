import { PASS_SCORE, scoreFromCoverage, spotLines, verdictFor, type OpenVerdict, type Question } from '@trainer/shared';
import type { ReactNode } from 'react';
import { isPass, type Outcome } from '../lib/evaluate';
import { CodeBlock } from './CodeBlock';
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

  const title = headline(outcome, verdict, q.id);

  // On a follow-up the card shows this answer, the bar shows the combined result: when they disagree, say so with amber.
  const own = pass ? 'good' : verdict === 'partial' ? 'meh' : 'bad';
  const tone = overall && (overall.pass ? 'good' : 'bad') !== own ? 'meh' : own;
  const sol = !pass ? solution(q) : null;
  const answerLine = q.type === 'estimate' ? estimateLine(q) : null;

  return (
    <section className={`feedback ${tone}`} aria-labelledby="feedback-title">
      <h2 id="feedback-title" tabIndex={-1}>{title}</h2>

      {open && oq && (
        <>
          <p className="score">
            {open.covered.filter(Boolean).length}/{open.covered.length} key points
            {open.gradedBy === 'ai' ? ' · graded by AI' : ' · self-assessed'}
            {score >= PASS_SCORE ? ' ✓' : ''}
          </p>
          {open.feedback && <p className="ai-feedback"><Inline text={open.feedback} /></p>}
          {open.needsSelfAssessment && (
            <p className="hint">AI grading isn't available, so compare with the model answer and tick the points you covered.</p>
          )}
          <ul className="rubric">
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
                    <b aria-hidden="true">{open.covered[i] ? '✓' : '✗'}</b> <Inline text={kp.point} />
                  </span>
                )}
              </li>
            ))}
          </ul>
          <details open={!pass || outcome.skipped}>
            <summary>Model answer</summary>
            {oq.format === 'code' ? <CodeBlock source={oq.modelAnswer} /> : <p><Inline text={oq.modelAnswer} /></p>}
          </details>
        </>
      )}

      {overall && (
        <p className="overall">
          Interview result: <b>{Math.round(overall.score * 100)}%</b> across both answers, which {overall.pass ? 'counts as correct' : 'is not enough yet'}.
        </p>
      )}

      {q.type === 'code' && outcome.code && !outcome.skipped && <TestResults result={outcome.code} />}

      {q.type === 'code' && (
        <details open={!pass}>
          <summary>Reference solution</summary>
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
            <li key={p.left}><b><Inline text={p.left} /></b> → <Inline text={p.right} /></li>
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

function pick<T>(items: T[], seed: string): T {
  let h = 0;
  for (const c of seed) h = (h * 31 + c.charCodeAt(0)) | 0;
  return items[Math.abs(h) % items.length]!;
}

function estimateLine(q: Extract<Question, { type: 'estimate' }>): string {
  const unit = q.unit ? ` ${q.unit}` : '';
  const tol = q.tolerance === 0 ? 'exact' : `accepted: ${trim(q.answer * (1 - q.tolerance))} to ${trim(q.answer * (1 + q.tolerance))}${unit}`;
  return `Answer: ${trim(q.answer)}${unit} (${tol})`;
}

const trim = (n: number) => String(Math.round(n * 100) / 100);

function headline(outcome: Outcome, verdict: OpenVerdict, seed: string): string {
  if (outcome.skipped) return 'Here is the answer';
  if (outcome.open?.needsSelfAssessment) return 'Check your answer';
  if (verdict === 'correct') return pick(['Correct!', 'Nice one!', 'Spot on!', 'Well done!'], seed);
  return verdict === 'partial' ? 'Partly there' : 'Not quite';
}
