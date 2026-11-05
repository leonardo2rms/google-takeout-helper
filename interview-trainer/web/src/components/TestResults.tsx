import type { CodeRunResult } from '@trainer/shared';
import { Icon } from './icons';

export function TestResults({ result }: { result: CodeRunResult }) {
  if (result.fatal) return <p className="test-fatal" role="alert"><Icon name="x" strokeWidth={2.5} /><span>{result.fatal}</span></p>;
  const passed = result.results.filter((r) => r.passed).length;
  return (
    <div className="tests" role="status">
      <p className="score">{passed}/{result.results.length} tests passed</p>
      <ul>
        {result.results.map((r, i) => (
          <li key={i} className={r.passed ? 'pass' : 'fail'}>
            <Icon name={r.passed ? 'check' : r.hidden ? 'eye-off' : 'x'} />
            {r.hidden ? (
              <span>Hidden test {r.passed ? 'passed' : 'failed'}</span>
            ) : (
              <span>
                <span className="sr-only">{r.passed ? 'Passed: ' : 'Failed: '}</span>
                <code>{r.call}</code>
                {!r.passed && (
                  <dl className="diff">
                    <dt>expected</dt><dd><code>{r.expected}</code></dd>
                    {r.got !== undefined && <><dt>got</dt><dd><code>{r.got}</code></dd></>}
                    {r.note && <><dt>note</dt><dd>{r.note}</dd></>}
                  </dl>
                )}
              </span>
            )}
          </li>
        ))}
      </ul>
    </div>
  );
}
