import { useEffect, useId, useRef, useState } from 'react';
import type { CodeQ, CodeRunResult } from '@trainer/shared';
import type { Answer, RunCodeFn } from '../lib/evaluate';
import { scrollIntoViewGently } from '../lib/a11y';
import { Icon } from './icons';
import { TestResults } from './TestResults';

interface Props {
  q: CodeQ;
  checked: boolean;
  run: RunCodeFn;
  onAnswer: (a: Answer | null) => void;
  /** Mock interview: no "Run tests" and no results, so nothing tells the learner how they are doing until the end. */
  blind?: boolean;
}

// Symbols that are slow to reach on a phone keyboard.
const KEYS = ['{', '}', '(', ')', '[', ']', ';', '=', '>', '<', '"', "'", '+', '-', '*', '/', '!', '&', '|', '.', ','];

/** A small code editor for phones and desktops, with a "Run tests" loop before the final check. */
export function CodeQuestion({ q, checked, run, onAnswer, blind = false }: Props) {
  const [source, setSource] = useState(q.starter.replace(/\n$/, ''));
  /** The last test run and the source it ran against: edits after it make it stale. */
  const [lastRun, setRun] = useState<{ result: CodeRunResult; source: string } | null>(null);
  const result = lastRun?.result ?? null;
  const stale = lastRun !== null && lastRun.source !== source;
  const hintId = useId();
  const [running, setRunning] = useState(false);
  const box = useRef<HTMLTextAreaElement>(null);
  const resultsRef = useRef<HTMLDivElement>(null);
  /** Tab indents, which would trap keyboard users: Escape releases it, so Escape then Tab (or Shift+Tab) moves on. */
  const tabMovesFocus = useRef(false);

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
    const ran = source;
    setRunning(true);
    setRun(null);
    setRun({ result: await run(q, ran), source: ran });
    setRunning(false);
  }

  // The results are the point of pressing Run: bring them into view (above the sticky keys and the bottom bar).
  useEffect(() => {
    if (result) scrollIntoViewGently(resultsRef.current, 'nearest');
  }, [result]);

  // The starter counts as an answer from the start, so "Check" is available like in a real editor.
  useEffect(() => {
    onAnswer({ type: 'code', source: q.starter.replace(/\n$/, '') });
    // eslint-disable-next-line react-hooks/exhaustive-deps -- once, on mount
  }, []);

  return (
    <div className="code-editor">
      <p className="editor-lang" data-testid="editor-lang"><span className="mono">JavaScript</span><span>runs in your browser</span></p>
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
        aria-describedby={hintId}
        onChange={(e) => update(e.target.value)}
        onBlur={() => { tabMovesFocus.current = false; }}
        onKeyDown={(e) => {
          if (e.key === 'Escape') tabMovesFocus.current = true;
          else if (e.key === 'Tab' && !e.shiftKey && !tabMovesFocus.current) {
            e.preventDefault();
            insert('  ');
          }
        }}
      />
      <p id={hintId} className="sr-only">In the editor, Tab inserts two spaces. Press Escape and then Tab, or Shift and Tab, to move on.</p>
      {!blind && (
        <div className="run-row">
          <button type="button" className="btn small" disabled={checked || running} onClick={() => void runTests()}>
            {running ? 'Running…' : <><Icon name="play" size="1em" /> Run tests</>}
          </button>
          <span className="hint">Some tests are hidden. Check runs all of them.</span>
        </div>
      )}
      <p className="sr-only" role="status" aria-live="polite">{running ? 'Running tests' : ''}</p>
      {result && !checked && !blind && <div ref={resultsRef} className="tests-anchor" data-testid="run-results">{stale && <p className="hint" data-testid="stale-results">You edited the code after this run. Run the tests again to see how it does now.</p>}<TestResults result={result} /></div>}
      {!checked && (
        /* One row that scrolls sideways and stays above the bottom bar while the editor is on screen, instead of four rows of keys. */
        <div className="keys" role="group" aria-label="Code symbols">
          <button type="button" className="key wide" onMouseDown={(e) => e.preventDefault()} onClick={() => insert('  ')}>Tab</button>
          {KEYS.map((k) => (
            <button key={k} type="button" className="key" onMouseDown={(e) => e.preventDefault()} onClick={() => insert(k)}>{k}</button>
          ))}
        </div>
      )}
    </div>
  );
}
