import type { CodeRunResult } from '@trainer/shared';

export function TestResults({ result }: { result: CodeRunResult }) {
  if (result.fatal) return <p className="test-fatal" role="alert">{result.fatal}</p>;
  const passed = result.results.filter((r) => r.passed).length;
  return (
    <div className="tests" role="status">
      <p className="score">{passed}/{result.results.length} tests passed</p>
      <ul>
        {result.results.map((r, i) => (
          <li key={i} className={r.passed ? 'pass' : 'fail'}>
            <b aria-hidden="true">{r.passed ? '✓' : '✗'}</b>{' '}
            {r.hidden ? (
              <span>Hidden test {r.passed ? 'passed' : 'failed'}</span>
            ) : (
              <span>
                <code>{r.call}</code>
                {!r.passed && (
                  <>
                    <br />
                    <small>expected <code>{r.expected}</code>{r.got !== undefined && <> · got <code>{r.got}</code></>}{r.note && <> · {r.note}</>}</small>
                  </>
                )}
              </span>
            )}
          </li>
        ))}
      </ul>
    </div>
  );
}
