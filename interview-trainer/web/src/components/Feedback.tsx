import { PASS_SCORE, scoreFromCoverage, verdictFor, type OpenVerdict, type Question } from '@trainer/shared';
import { isPass, type Outcome } from '../lib/evaluate';
import { CodeBlock } from './CodeBlock';
import { Inline } from './Inline';

interface Props {
  q: Question;
  outcome: Outcome;
  onCoveredChange: (covered: boolean[]) => void;
}

function solution(q: Question): { text: string; lang: string } | null {
  if (q.type === 'fill') {
    const text = q.template.replace(/\[\[(\d+)\]\]/g, (_m, n: string) => q.answers[Number(n) - 1] ?? '?');
    return { text, lang: q.lang };
  }
  if (q.type === 'order') return { text: q.lines.join('\n'), lang: q.lang };
  return null;
}

export function Feedback({ q, outcome, onCoveredChange }: Props) {
  const pass = isPass(outcome);
  const open = outcome.open;
  const oq = q.type === 'open' ? q : null;
  const score = open ? scoreFromCoverage(open.covered) : pass ? 1 : 0;
  const verdict = open ? verdictFor(score) : pass ? 'correct' : 'incorrect';

  const title = headline(outcome, verdict, q.id);

  const sol = !pass ? solution(q) : null;

  return (
    <div className={`feedback ${pass ? 'good' : verdict === 'partial' ? 'meh' : 'bad'}`} role="status" aria-live="polite">
      <h2>{title}</h2>

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

      {q.explanation && (
        <p className="explanation"><Inline text={q.explanation} /></p>
      )}

      {sol && (
        <>
          <p className="hint">Correct solution</p>
          <CodeBlock source={sol.text} lang={sol.lang} />
        </>
      )}
    </div>
  );
}

function pick<T>(items: T[], seed: string): T {
  let h = 0;
  for (const c of seed) h = (h * 31 + c.charCodeAt(0)) | 0;
  return items[Math.abs(h) % items.length]!;
}

function headline(outcome: Outcome, verdict: OpenVerdict, seed: string): string {
  if (outcome.skipped) return 'Here is the answer';
  if (outcome.open?.needsSelfAssessment) return 'Check your answer';
  if (verdict === 'correct') return pick(['Correct!', 'Nice one!', 'Spot on!', 'Well done!'], seed);
  return verdict === 'partial' ? 'Partly there' : 'Not quite';
}
