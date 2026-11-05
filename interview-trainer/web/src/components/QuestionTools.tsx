import { useEffect, useRef, useState } from 'react';
import { MAX_NOTE_CHARS, dayKey, isBookmarked, noteOf, setNote, toggleBookmark, type Question } from '@trainer/shared';
import { useProgress } from '../lib/store';
import { Icon } from './icons';

interface Props {
  q: Question;
  /** Start with the note editor open (the Library uses this). */
  startEditing?: boolean;
  /** Where this appears, for stable test ids. */
  testIdPrefix?: string;
}

/** Star a question and attach a short private note. Used under the lesson feedback and in the Library. */
export function QuestionTools({ q, startEditing = false, testIdPrefix = 'tools' }: Props) {
  const { progress, update } = useProgress();
  const starred = isBookmarked(progress, q);
  const saved = noteOf(progress, q);
  const [editing, setEditing] = useState(startEditing);
  const [draft, setDraft] = useState(saved ?? '');

  // Another tab (or an import) may change the note while it is closed.
  useEffect(() => {
    if (!editing) setDraft(saved ?? '');
  }, [saved, editing]);

  // A note being typed is not thrown away when the learner moves on (Continue unmounts this panel): it is saved as they leave.
  const unsaved = useRef<{ q: Question; text: string } | null>(null);
  unsaved.current = editing && draft !== (saved ?? '') ? { q, text: draft } : null;
  useEffect(
    () => () => {
      const pending = unsaved.current;
      if (pending) update((p) => setNote(p, pending.q, pending.text, dayKey(new Date())));
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps -- unmount only; `update` is stable
    [],
  );

  const commit = () => {
    update((p) => setNote(p, q, draft, dayKey(new Date())));
    setEditing(false);
  };

  return (
    <div className="q-tools" data-testid={testIdPrefix} data-no-lesson-keys>
      <div className="q-tools-row">
        <button
          type="button"
          className={`btn small tool-btn ${starred ? 'on' : ''}`}
          aria-pressed={starred}
          data-testid={`${testIdPrefix}-star`}
          onClick={() => update((p) => toggleBookmark(p, q, dayKey(new Date())))}
        >
          <Icon name="bookmark" /> {starred ? 'Saved' : 'Save'}
        </button>
        <button
          type="button"
          className="btn small tool-btn"
          aria-expanded={editing}
          data-testid={`${testIdPrefix}-note-btn`}
          onClick={() => (editing ? setEditing(false) : (setDraft(saved ?? ''), setEditing(true)))}
        >
          <Icon name="square-pen" /> {saved ? 'Edit note' : 'Add note'}
        </button>
      </div>

      {!editing && saved && (
        <p className="q-note" data-testid={`${testIdPrefix}-note`}>
          <b>Your note</b> {saved}
        </p>
      )}

      {editing && (
        <div className="q-note-editor">
          <label htmlFor={`note-${q.id}`} className="sr-only">Your private note</label>
          <textarea
            id={`note-${q.id}`}
            className="answer-box"
            rows={3}
            maxLength={MAX_NOTE_CHARS}
            value={draft}
            placeholder="A private note, only on this device."
            autoFocus
            onChange={(e) => setDraft(e.target.value)}
          />
          <div className="q-note-actions">
            <span className="hint">{draft.length}/{MAX_NOTE_CHARS}</span>
            <button type="button" className="btn small ghost" onClick={() => setEditing(false)}>Cancel</button>
            <button type="button" className="btn small primary" data-testid={`${testIdPrefix}-note-save`} onClick={commit}>Save note</button>
          </div>
        </div>
      )}
    </div>
  );
}
