import { useMemo, useState } from 'react';
import { STATUS_LABEL, TYPE_LABEL, allQuestions, searchLibrary, type Course, type Progress, type Question } from '@trainer/shared';
import { Icon } from '../components/icons';
import { Inline } from '../components/Inline';
import { QuestionTools } from '../components/QuestionTools';
import { plural } from '../lib/format';
import { useToday } from '../lib/today';
import { unitStyle } from '../lib/unit-style';

interface Props {
  course: Course;
  progress: Progress;
}

const PAGE = 30;
const TYPES = Object.keys(TYPE_LABEL) as Question['type'][];

type Toggle = 'bookmarked' | 'hasNote' | 'due' | 'neverSeen';
const TOGGLES: { key: Toggle; label: string }[] = [
  { key: 'bookmarked', label: 'Saved' },
  { key: 'hasNote', label: 'Has note' },
  { key: 'due', label: 'Due' },
  { key: 'neverSeen', label: 'Never seen' },
];

/** Every question in the course: full-text search, filters, your stars and notes, and a one-question practice session. */
export function Library({ course, progress }: Props) {
  const today = useToday();
  const [text, setText] = useState('');
  const [type, setType] = useState<Question['type'] | ''>('');
  const [unitId, setUnitId] = useState('');
  const [on, setOn] = useState<Record<Toggle, boolean>>({ bookmarked: false, hasNote: false, due: false, neverSeen: false });
  const [shown, setShown] = useState(PAGE);
  const [editing, setEditing] = useState<string | null>(null);

  const total = useMemo(() => allQuestions(course).length, [course]);
  const results = useMemo(
    () => searchLibrary(course, progress, today, { text, type, unitId, ...on }),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- `on` is spread into the dependency list below
    [course, progress, today, text, type, unitId, on.bookmarked, on.hasNote, on.due, on.neverSeen],
  );
  const savedCount = Object.keys(progress.bookmarks).length;
  const filtered = Boolean(text.trim() || type || unitId || Object.values(on).some(Boolean));
  const styleOf = (id: string) => unitStyle(course.units.find((u) => u.id === id) ?? { id });

  const reset = () => {
    setText('');
    setType('');
    setUnitId('');
    setOn({ bookmarked: false, hasNote: false, due: false, neverSeen: false });
    setShown(PAGE);
  };

  return (
    <div className="home library-page">
      <header className="page-top">
        <a className="icon-btn back" href="#/" aria-label="Back to home"><Icon name="arrow-left" /></a>
        <h1>Library</h1>
      </header>

      <div className="lib-search">
        <label htmlFor="lib-q" className="sr-only">Search questions</label>
        <input
          id="lib-q"
          type="search"
          className="lib-input"
          placeholder="Search questions, topics, notes"
          value={text}
          autoComplete="off"
          onChange={(e) => {
            setText(e.target.value);
            setShown(PAGE);
          }}
        />
      </div>

      <div className="lib-filters">
        <div className="lib-selects">
          <label>
            <span className="sr-only">Question type</span>
            <select className="lib-select" aria-label="Question type" value={type} onChange={(e) => { setType(e.target.value as Question['type'] | ''); setShown(PAGE); }}>
              <option value="">All types</option>
              {TYPES.map((t) => <option key={t} value={t}>{TYPE_LABEL[t]}</option>)}
            </select>
          </label>
          <label>
            <span className="sr-only">Topic</span>
            <select className="lib-select" aria-label="Topic" value={unitId} onChange={(e) => { setUnitId(e.target.value); setShown(PAGE); }}>
              <option value="">All topics</option>
              {course.units.map((u) => <option key={u.id} value={u.id}>{u.title}</option>)}
            </select>
          </label>
        </div>
        <div className="lib-toggles" role="group" aria-label="Filters">
          {TOGGLES.map((t) => (
            <button
              key={t.key}
              type="button"
              className={`chip lib-chip ${on[t.key] ? 'on' : ''}`}
              aria-pressed={on[t.key]}
              data-testid={`filter-${t.key}`}
              onClick={() => {
                setOn((o) => ({ ...o, [t.key]: !o[t.key] }));
                setShown(PAGE);
              }}
            >
              {t.label}{t.key === 'bookmarked' && savedCount > 0 ? ` (${savedCount})` : ''}
            </button>
          ))}
        </div>
      </div>

      <p className="lib-count" role="status" aria-live="polite" data-testid="lib-count" data-count={results.length}>
        {filtered ? `${results.length} of ${total} questions` : `${total} questions`}
        {filtered && <> · <button type="button" className="link inline" onClick={reset}>Clear filters</button></>}
      </p>

      {results.length === 0 ? (
        <p className="muted lib-empty" data-testid="lib-empty">
          {on.bookmarked && !text && !type && !unitId ? 'Nothing saved yet. Tap Save under a question after you answer it.' : 'No question matches. Try fewer words or clear the filters.'}
        </p>
      ) : (
        <ul className="lib-list" data-testid="lib-list">
          {results.slice(0, shown).map((e) => (
            <li key={e.q.id} className="lib-item" data-testid="lib-item" data-qid={e.q.id}>
              <p className="lib-prompt"><Inline text={e.q.prompt} /></p>
              <p className="lib-meta">
                <span className="unit-tag" style={styleOf(e.unitId).style}><Icon name={styleOf(e.unitId).icon} size="1.1em" />{e.unitTitle}</span>
                <span>{TYPE_LABEL[e.q.type]}</span>
                <span className={`pill status-${e.status}`} data-testid="lib-status">{STATUS_LABEL[e.status]}</span>
              </p>
              <div className="lib-actions">
                <a className="btn small secondary" href={`#/q/${e.q.id}`} aria-label={`Practise this one: ${plain(e.q.prompt)}`} data-testid="lib-practise">Practise this one</a>
                {!e.note && !e.bookmarked && (
                  <button type="button" className="btn small ghost" onClick={() => setEditing(editing === e.q.id ? null : e.q.id)} aria-expanded={editing === e.q.id}>
                    {editing === e.q.id ? 'Hide' : 'Save or note'}
                  </button>
                )}
              </div>
              {(editing === e.q.id || e.note || e.bookmarked) && <QuestionTools q={e.q} testIdPrefix={`lib-${e.q.id}`} />}
            </li>
          ))}
        </ul>
      )}

      {results.length > shown && (
        <button type="button" className="btn ghost wide" data-testid="lib-more" onClick={() => setShown((n) => n + PAGE)}>
          Show more ({plural(results.length - shown, 'question')} left)
        </button>
      )}
    </div>
  );
}

const plain = (s: string): string => s.replace(/`/g, '').slice(0, 80);
