import { useEffect, useRef, useState } from 'react';
import type { CodeQ, CodeRunResult } from '@trainer/shared';
import type { Answer, RunCodeFn } from '../lib/evaluate';
import { TestResults } from './TestResults';

interface Props {
  q: CodeQ;
  checked: boolean;
  run: RunCodeFn;
  onAnswer: (a: Answer | null) => void;
}

// Symbols that are slow to reach on a phone keyboard.
const KEYS = ['{', '}', '(', ')', '[', ']', ';', '=', '>', '<', '"', "'", '+', '-', '*', '/', '!', '&', '|', '.', ','];

/** A small code editor for phones and desktops, with a "Run tests" loop before the final check. */
export function CodeQuestion({ q, checked, run, onAnswer }: Props) {
  const [source, setSource] = useState(q.starter.replace(/\n$/, ''));
  const [result, setResult] = useState<CodeRunResult | null>(null);
  const [running, setRunning] = useState(false);
  const box = useRef<HTMLTextAreaElement>(null);

  function update(next: string) {
    setSource(next);
    onAnswer(next.trim() ? { type: 'code', source: next } : null);
  }

  function insert(text: string) {
    const el = box.current;
    if (!el || checked) return;
    const { selectionStart: a, selectionEnd: b } = el;
    update(source.slice(0, a) + text + source.slice(b));
    requestAnimationFrame(() => {
      el.focus();
      el.setSelectionRange(a + text.length, a + text.length);
    });
  }

  async function runTests() {
    setRunning(true);
    setResult(null);
    setResult(await run(q, source));
    setRunning(false);
  }

  // The starter counts as an answer from the start, so "Check" is available like in a real editor.
  useEffect(() => {
    onAnswer({ type: 'code', source: q.starter.replace(/\n$/, '') });
    // eslint-disable-next-line react-hooks/exhaustive-deps -- once, on mount
  }, []);

  return (
    <div className="code-editor">
      <textarea
        ref={box}
        className="answer-box mono editor"
        value={source}
        rows={Math.min(14, Math.max(7, source.split('\n').length + 1))}
        disabled={checked}
        spellCheck={false}
        autoCapitalize="off"
        autoCorrect="off"
        aria-label="Your code"
        onChange={(e) => update(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Tab') {
            e.preventDefault();
            insert('  ');
          }
        }}
      />
      {!checked && (
        <div className="keys" role="group" aria-label="Code symbols">
          <button type="button" className="key wide" onMouseDown={(e) => e.preventDefault()} onClick={() => insert('  ')}>Tab</button>
          {KEYS.map((k) => (
            <button key={k} type="button" className="key" onMouseDown={(e) => e.preventDefault()} onClick={() => insert(k)}>{k}</button>
          ))}
        </div>
      )}
      <div className="run-row">
        <button type="button" className="btn secondary small" disabled={checked || running} onClick={() => void runTests()}>
          {running ? 'Running…' : '▶ Run tests'}
        </button>
        <span className="hint">Some tests are hidden. Check runs all of them.</span>
      </div>
      {result && !checked && <TestResults result={result} />}
    </div>
  );
}
