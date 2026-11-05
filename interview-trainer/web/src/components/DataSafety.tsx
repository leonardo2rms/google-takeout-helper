import { useEffect, useMemo, useState } from 'react';
import { backupFileName, dayKey, importProgress, summarizeProgress, type ParkedCopy } from '@trainer/shared';
import { DECLINED_NOTE, copyText, saveFile } from '../lib/files';
import { formatDay } from '../lib/format';
import { acknowledgeRecovered, exportText, importContext, listParkedCopies, mergeIntoProgress, replaceProgress, useProgress, useSafetyStatus, type SafetyStatus } from '../lib/store';
import { useUpdateReady } from '../lib/sw-register';
import { Icon } from './icons';

/** Routes that run a session (lesson, review, mix, mock ...): nothing sits above a question, so only a small mark is shown there. */
export const SESSION_ROUTE = /^\/(lesson\/|mix$|review$|daily$|warmup$|topic\/|challenge$|q\/|mock)/;

/** What a notice says: one line, and the details behind a button. */
export interface RiskCopy {
  line: string;
  details: string;
}

/** The words for "your work is not being kept". Pure so the copy is testable. */
export function riskCopy(s: Pick<SafetyStatus, 'unreadablePending' | 'persistence'>): RiskCopy {
  if (s.unreadablePending) {
    return {
      line: 'Your saved progress could not be read. Nothing new is saved.',
      details:
        'Your old data has not been touched or deleted. Until this is sorted out, what you do now stays in this tab only: if you close it, that work is gone.',
    };
  }
  if (s.persistence === 'memory') {
    return {
      line: 'Your progress is not being saved.',
      details: 'This browser blocks storage here, so what you do is lost when you reload or close the page.',
    };
  }
  return {
    line: 'Your progress is not being saved.',
    details: 'The device may be full. Your work is kept in this tab and we keep trying.',
  };
}

/**
 * Quiet, non-blocking notices about the safety of the learner's data and the freshness of the app. They sit above the
 * page content, never cover it, only appear when there is something the learner can act on, and show ONE at a time
 * (the most important first). Inside a lesson nothing is shown here: see `SafetyMark`.
 */
export function DataSafetyBanners({ route = '/' }: { route?: string }) {
  const status = useSafetyStatus();
  const updateReady = useUpdateReady();
  const [dismissedRisk, setDismissedRisk] = useState(false);
  const [dismissedUpdate, setDismissedUpdate] = useState(false);
  if (SESSION_ROUTE.test(route)) return null;
  const showRisk = (status.atRisk || status.unreadablePending) && !dismissedRisk;
  const showRecovered = status.recovered.length > 0;
  const showUpdate = updateReady && !dismissedUpdate;
  return (
    <div className="safety-bars">
      {showRisk ? (
        <RiskBar status={status} onDismiss={() => setDismissedRisk(true)} />
      ) : showRecovered ? (
        <div className="notice" role="status" data-testid="recovered-notice">
          <div>
            <p>
              <b>Some saved data could not be read.</b> A copy was set aside and nothing was deleted. You can download or restore it under
              Settings, Recovered data.
            </p>
            <span className="safety-actions">
              <button type="button" className="btn small ghost" onClick={acknowledgeRecovered}>Got it</button>
            </span>
          </div>
        </div>
      ) : (
        showUpdate && (
          <div className="notice" role="status" data-testid="update-ready">
            <span>A new version is ready.</span>
            <span className="safety-actions">
              <button type="button" className="btn small secondary" onClick={() => window.location.reload()}>Reload</button>
              <button type="button" className="btn small ghost" onClick={() => setDismissedUpdate(true)}>Later</button>
            </span>
          </div>
        )
      )}
    </div>
  );
}

/** The compact bar: one line, "Details" to expand, "Dismiss". A backup is offered only when there is something to save. */
function RiskBar({ status, onDismiss }: { status: SafetyStatus; onDismiss: () => void }) {
  const [open, setOpen] = useState(false);
  const { progress } = useProgress();
  const copy = riskCopy(status);
  const hasProgress = progress.xp > 0 || progress.stats.sessions > 0;
  return (
    <div className="notice bad compact" role="status" data-testid="not-saved">
      <div className="safety-main">
        <p className="safety-line"><Icon name="info" size="1.1em" /><b>{copy.line}</b></p>
        <span className="safety-actions">
          <button type="button" className="btn small ghost" aria-expanded={open} aria-controls="safety-details" onClick={() => setOpen((o) => !o)}>
            Details
          </button>
          <button type="button" className="btn small ghost" onClick={onDismiss}>Dismiss</button>
        </span>
      </div>
      {open && (
        <div id="safety-details" className="safety-details">
          <p>{copy.details}</p>
          {hasProgress ? <ExportActions /> : <p className="muted">There is nothing to back up yet.</p>}
        </div>
      )}
    </div>
  );
}

/** A small mark in a lesson's header while progress is not being kept, in place of a banner above the question. */
export function SafetyMark() {
  const status = useSafetyStatus();
  if (!(status.atRisk || status.unreadablePending)) return null;
  const { line } = riskCopy(status);
  return (
    <span className="safety-mark" role="img" aria-label={line} title={line} data-testid="safety-mark">
      <Icon name="info" size="1.2em" />
    </span>
  );
}

/** Download / copy / show-the-text for the current profile; the text is shown when both of the others are blocked. */
function ExportActions() {
  const [text, setText] = useState('');
  const [note, setNote] = useState('');
  const download = async () => {
    const t = exportText();
    const result = await saveFile(backupFileName(dayKey(new Date())), t, 'application/json');
    if (result === 'saved') setNote('Backup saved to your downloads.');
    else if (result === 'declined') setNote(DECLINED_NOTE);
    else {
      setText(t);
      setNote('Downloads are blocked here. Select the text below and copy it somewhere safe.');
    }
  };
  const copy = async () => {
    const t = exportText();
    if (await copyText(t)) setNote('Backup copied. Paste it into a note or message to yourself.');
    else {
      setText(t);
      setNote('Copying is blocked here. Select the text below and copy it by hand.');
    }
  };
  return (
    <>
      <span className="safety-actions">
        <button type="button" className="btn small secondary" onClick={() => void download()}>Download backup</button>
        <button type="button" className="btn small ghost" onClick={() => void copy()}>Copy backup text</button>
      </span>
      {note && <p className="muted" role="status">{note}</p>}
      {text && (
        <>
          <label htmlFor="safety-backup-text" className="sr-only">Backup text</label>
          <textarea id="safety-backup-text" className="answer-box mono" rows={4} readOnly value={text} onFocus={(e) => e.currentTarget.select()} />
        </>
      )}
    </>
  );
}

const kindLabel = (c: ParkedCopy) => (c.kind === 'unreadable' ? 'Unreadable data set aside' : 'Your progress before a replace or reset');

/** Settings sheet section: every copy the app has set aside, with download, copy and (when readable) restore. */
export function RecoveredData() {
  const [copies, setCopies] = useState(() => listParkedCopies());
  const [message, setMessage] = useState('');
  useEffect(() => acknowledgeRecovered(), []);
  const parsed = useMemo(
    () => new Map(copies.map((c) => [c.key, importProgress(c.raw, importContext())] as const)),
    [copies],
  );
  if (copies.length === 0) return null;
  const nameOf = (c: ParkedCopy) => `interview-trainer-recovered-${c.key.split('.').pop()}.json`;

  return (
    <section data-testid="recovered-data">
      <h3>Recovered data</h3>
      <p className="muted">
        Copies the app set aside instead of deleting: data this version could not read, and your progress as it was before you replaced or reset it.
        The oldest ones are removed only when newer ones need the room.
      </p>
      <ul className="recovered-list">
        {copies.map((c) => {
          const res = parsed.get(c.key)!;
          const sum = res.ok ? summarizeProgress(res.progress) : null;
          return (
            <li key={c.key} className="notice" data-testid="recovered-item">
              <div>
                <p>
                  <b>{kindLabel(c)}</b>{c.day ? `, ${formatDay(c.day)}` : ''}
                </p>
                <p className="muted">
                  {sum ? `${sum.xp} XP, ${sum.streak}-day streak, ${sum.lessons} lessons` : 'This version cannot read it, but the text is kept in full.'} ({Math.max(1, Math.round(c.raw.length / 1024))} kB)
                </p>
                <span className="safety-actions">
                  <button type="button" className="btn small secondary" onClick={() => void saveFile(nameOf(c), c.raw, 'application/json').then((r) => setMessage(r === 'saved' ? 'Saved to your downloads.' : r === 'declined' ? DECLINED_NOTE : 'Downloads are blocked here. Use "Copy" instead.'))}>Download</button>
                  <button type="button" className="btn small ghost" onClick={() => void copyText(c.raw).then((ok) => setMessage(ok ? 'Copied.' : 'Copying is blocked here.'))}>Copy</button>
                  {res.ok && (
                    <>
                      <button type="button" className="btn small ghost" onClick={() => { mergeIntoProgress(res.progress); setMessage('Merged into your progress. The best of both is kept.'); }}>Restore (merge)</button>
                      <button type="button" className="btn small ghost" onClick={() => { replaceProgress(res.progress); setCopies(listParkedCopies()); setMessage('Progress replaced. Your previous progress was kept as a copy here.'); }}>Restore (replace)</button>
                    </>
                  )}
                </span>
              </div>
            </li>
          );
        })}
      </ul>
      {message && <p role="status" className="notice good" data-testid="recovered-message">{message}</p>}
    </section>
  );
}
