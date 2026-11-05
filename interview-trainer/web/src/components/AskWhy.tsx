import { createPortal } from 'react-dom';
import { useEffect, useRef, useState, type FormEvent, type KeyboardEvent } from 'react';
import { EXPLAIN_MAX_USER_CHARS, EXPLAIN_MAX_USER_MESSAGES, type ExplainMessage } from '@trainer/shared';
import type { ExplainFn } from '../lib/explain';
import { Icon } from './icons';
import { Sheet } from './Sheet';

interface Props {
  questionId: string;
  /** The learner's answer as short text; "" if they skipped. */
  answer: string;
  correct: boolean;
  /** Undefined when no AI is available (offline, no key): one quiet line says so instead of a dead button. */
  explain?: ExplainFn;
}

const SUGGESTIONS_WRONG = ['Why is my answer wrong?', 'Explain it simply', 'Give me an example'];
const SUGGESTIONS_RIGHT = ['Explain it simply', 'Give me an example', 'What is a common mistake here?'];

/** "Ask why": a short tutor chat about this one question. Lives in the feedback panel. */
export function AskWhy({ questionId, answer, correct, explain }: Props) {
  const [open, setOpen] = useState(false);
  const [messages, setMessages] = useState<ExplainMessage[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [draft, setDraft] = useState('');
  const alive = useRef(true);
  useEffect(() => {
    alive.current = true;
    return () => void (alive.current = false);
  }, []);

  if (!explain) {
    return (
      <p className="ask-why off hint" data-testid="ask-why-off"><Icon name="info" size="1.1em" />Ask why needs AI, which is off.</p>
    );
  }

  const asked = messages.filter((m) => m.role === 'user').length;
  const left = EXPLAIN_MAX_USER_MESSAGES - asked;

  async function send(text: string) {
    const content = text.trim().slice(0, EXPLAIN_MAX_USER_CHARS);
    if (!content || busy || left <= 0 || !explain) return;
    const next: ExplainMessage[] = [...messages, { role: 'user', content }];
    setMessages(next);
    setDraft('');
    setError('');
    setBusy(true);
    try {
      const reply = await explain({ questionId, answer, correct, messages: next });
      if (alive.current) setMessages([...next, { role: 'assistant', content: reply }]);
    } catch {
      // Keep the question so it can be sent again; never show upstream details.
      if (alive.current) {
        setMessages(messages);
        setDraft(content);
        setError('The tutor could not answer just now. Your question is still in the box; try again in a moment.');
      }
    } finally {
      if (alive.current) setBusy(false);
    }
  }

  const onSubmit = (e: FormEvent) => {
    e.preventDefault();
    void send(draft);
  };
  const onKey = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      e.preventDefault();
      e.stopPropagation();
      void send(draft);
    }
  };

  return (
    <div className="ask-why">
      <button type="button" className="btn ghost small" onClick={() => setOpen(true)} data-testid="ask-why">Ask why</button>
      {open && createPortal(
        <Sheet title="Ask why" onClose={() => setOpen(false)}>
          <p className="muted ask-note" data-testid="ask-why-note">
            Ask a follow-up about this question. An AI answers, and AI can be wrong: the explanation on the card is the reference.
          </p>

          <div className="chat" role="log" aria-live="polite" aria-label="Conversation" data-testid="chat-log">
            {messages.map((m, i) => (
              <p key={i} className={`bubble ${m.role}`} data-role={m.role}>
                <span className="sr-only">{m.role === 'user' ? 'You: ' : 'Tutor: '}</span>
                {m.content}
              </p>
            ))}
            {busy && <p className="bubble assistant pending" data-testid="chat-pending">Thinking…</p>}
          </div>

          {asked === 0 && !busy && (
            <div className="chips" role="group" aria-label="Suggested questions">
              {(correct ? SUGGESTIONS_RIGHT : SUGGESTIONS_WRONG).map((s) => (
                <button key={s} type="button" className="chip" onClick={() => void send(s)}>{s}</button>
              ))}
            </div>
          )}

          {error && <p role="alert" className="notice bad" data-testid="chat-error">{error}</p>}

          {left > 0 ? (
            <form onSubmit={onSubmit} className="chat-form">
              <label htmlFor="ask-why-input" className="sr-only">Your question about this exercise</label>
              <textarea
                id="ask-why-input"
                className="answer-box"
                rows={2}
                maxLength={EXPLAIN_MAX_USER_CHARS}
                value={draft}
                onChange={(e) => setDraft(e.target.value)}
                onKeyDown={onKey}
                placeholder="e.g. Why is B wrong?"
              />
              <div className="chat-actions">
                <small className="muted" data-testid="chat-left">{left} of {EXPLAIN_MAX_USER_MESSAGES} questions left</small>
                <button type="submit" className="btn secondary small" disabled={busy || draft.trim() === ''}>Send</button>
              </div>
            </form>
          ) : (
            <p className="notice" data-testid="chat-limit">That is the limit for this question. Re-read the explanation, or carry on; it will come back for review.</p>
          )}
        </Sheet>,
        document.body,
      )}
    </div>
  );
}
