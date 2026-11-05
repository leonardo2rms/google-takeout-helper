import { useRef, useState } from 'react';
import { TIP_BONUS_XP, TIP_ONE_IN, backupFileName, dayKey, importProgress, summarizeProgress, type Progress, type Settings } from '@trainer/shared';
import { exportText, importContext, mergeIntoProgress, replaceProgress } from '../lib/store';
import { useConfirm } from './ConfirmDialog';
import { RecoveredData } from './DataSafety';
import { Icon } from './icons';
import { ReadingSection } from './ReadingSection';
import { RemindersSection } from './RemindersSection';
import { RestDays } from './RestDays';
import { Sheet } from './Sheet';

interface Props {
  progress: Progress;
  update: (fn: (p: Progress) => Progress) => void;
  onEditPlan: () => void;
  onReset: () => void;
  onClose: () => void;
}

interface Pending {
  progress: Progress;
  exportedAt: string | null;
}

const TOGGLES: { key: keyof Pick<Settings, 'confetti' | 'haptics' | 'tips' | 'confidence'>; label: string; hint?: string }[] = [
  { key: 'confetti', label: 'Confetti on big moments', hint: 'Only for real milestones; off when your device asks for reduced motion.' },
  { key: 'haptics', label: 'Vibration on correct answers', hint: 'Where your device supports it.' },
  {
    key: 'confidence',
    label: 'Ask "How sure are you?"',
    hint: 'A one-tap rating before you check. Sure and wrong brings a question back sooner. Skippable.',
  },
  {
    key: 'tips',
    label: "Interviewer's tip",
    hint: `About 1 session in ${TIP_ONE_IN} shows a short reminder, worth +${TIP_BONUS_XP} XP.`,
  },
];

export function SettingsSheet({ progress, update, onEditPlan, onReset, onClose }: Props) {
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');
  const [pending, setPending] = useState<Pending | null>(null);
  const [pasted, setPasted] = useState('');
  const fileRef = useRef<HTMLInputElement>(null);
  const confirm = useConfirm();

  const say = (m: string) => {
    setError('');
    setMessage(m);
  };
  const fail = (m: string) => {
    setMessage('');
    setError(m);
  };

  const download = () => {
    const blob = new Blob([exportText()], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = backupFileName(dayKey(new Date()));
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    say('Backup saved to your downloads. Keep it somewhere safe.');
  };

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(exportText());
      say('Backup copied. Paste it into a note or message to yourself.');
    } catch {
      fail('Copying is not allowed here. Use "Download backup" instead.');
    }
  };

  const check = (text: string) => {
    const res = importProgress(text, importContext());
    if (!res.ok) {
      setPending(null);
      fail(res.error);
      return;
    }
    setError('');
    setMessage('');
    setPending({ progress: res.progress, exportedAt: res.exportedAt });
  };

  const onFile = async (file: File | undefined) => {
    if (!file) return;
    try {
      check(await file.text());
    } catch {
      fail('That file could not be read.');
    }
    if (fileRef.current) fileRef.current.value = '';
  };

  const summary = pending ? summarizeProgress(pending.progress) : null;

  return (
    <Sheet title="Settings" onClose={onClose}>
      <section>
        <h3>Plan</h3>
        <p className="muted">
          {progress.profile.interviewDate ? `Interview on ${progress.profile.interviewDate}. ` : 'No interview date set. '}
          Goal: {progress.goal.target} questions a day.
        </p>
        <button type="button" className="btn secondary wide" onClick={onEditPlan}>Edit my plan</button>
      </section>

      <section>
        <h3>Rest days</h3>
        <RestDays value={progress.profile.restDays} onChange={(restDays) => update((p) => ({ ...p, profile: { ...p.profile, restDays } }))} />
        <p className="muted">Up to 3 a week. They never break your streak.</p>
      </section>

      <section>
        <h3>Effects and prompts</h3>
        {TOGGLES.map((t) => (
          <label key={t.key} className="toggle-row">
            <input type="checkbox" checked={progress.settings[t.key]} onChange={(e) => update((p) => ({ ...p, settings: { ...p.settings, [t.key]: e.target.checked } }))} />
            <span><b>{t.label}</b>{t.hint && <small>{t.hint}</small>}</span>
          </label>
        ))}
      </section>

      <ReadingSection progress={progress} update={update} />

      <RemindersSection progress={progress} update={update} />

      <section>
        <h3>Your data</h3>
        <p className="muted">Stored on this device only. A backup file moves you to a new phone and protects you if the browser clears its storage.</p>
        <div className="stack">
          <button type="button" className="btn secondary wide" onClick={download}>Download backup</button>
          <button type="button" className="btn secondary wide" onClick={() => void copy()}>Copy backup text</button>
          <label className="btn secondary wide file-btn">
            Import backup file
            <input ref={fileRef} type="file" accept=".json,application/json,text/plain" data-testid="import-file" onChange={(e) => void onFile(e.target.files?.[0])} />
          </label>
        </div>
        <details className="how-calc">
          <summary><Icon name="chevron-down" className="chev" />Paste backup text instead</summary>
          <label htmlFor="paste-backup" className="sr-only">Backup text</label>
          <textarea id="paste-backup" className="answer-box mono" rows={4} value={pasted} onChange={(e) => setPasted(e.target.value)} placeholder="Paste the text you copied" />
          <button type="button" className="btn ghost small" onClick={() => check(pasted)} disabled={pasted.trim() === ''}>Check pasted text</button>
        </details>

        {error && <p role="alert" className="notice bad" data-testid="import-error">{error}</p>}
        {message && <p role="status" className="notice good" data-testid="import-message">{message}</p>}

        {pending && summary && (
          <div className="notice" role="group" aria-label="Confirm import" data-testid="import-confirm">
            <p>
              <b>Backup found{pending.exportedAt ? ` (${pending.exportedAt.slice(0, 10)})` : ''}:</b> {summary.xp} XP, {summary.streak}-day streak, {summary.lessons} lessons, {summary.cards} scheduled questions.
            </p>
            <p className="muted">Your current progress: {progress.xp} XP, {progress.streak}-day streak, {Object.keys(progress.lessons).length} lessons.</p>
            <div className="stack">
              <button type="button" className="btn primary wide" onClick={() => { mergeIntoProgress(pending.progress); setPending(null); say('Merged. The best of both is now on this device.'); }}>
                Merge (keep the best of both)
              </button>
              <button
                type="button"
                className="btn secondary wide"
                onClick={async () => {
                  const ok = await confirm({
                    title: 'Replace your progress?',
                    body: `This overwrites the ${progress.xp} XP, ${Object.keys(progress.lessons).length} lessons and review schedule on this device with the backup. Merge keeps the best of both instead.`,
                    confirmLabel: 'Replace with the backup',
                    cancelLabel: 'Keep mine',
                    tone: 'danger',
                    testId: 'replace-confirm',
                  });
                  if (!ok) return;
                  replaceProgress(pending.progress);
                  setPending(null);
                  say('Progress replaced with the backup.');
                }}
              >
                Replace my progress
              </button>
              <button type="button" className="btn ghost wide" onClick={() => setPending(null)}>Cancel</button>
            </div>
          </div>
        )}
      </section>

      <RecoveredData />

      <section>
        <h3>Reset</h3>
        <p className="muted">Erases your progress on this device. Download a backup first if you are unsure.</p>
        <button
          type="button"
          className="btn secondary wide danger-btn"
          onClick={async () => {
            const ok = await confirm({
              title: 'Reset all progress?',
              body: 'XP, streak, lessons, notes and the review schedule on this device are erased. This cannot be undone. Download a backup first if you are unsure.',
              confirmLabel: 'Reset everything',
              cancelLabel: 'Keep my progress',
              tone: 'danger',
              testId: 'reset-confirm',
            });
            if (ok) onReset();
          }}
        >
          Reset progress
        </button>
      </section>
    </Sheet>
  );
}
