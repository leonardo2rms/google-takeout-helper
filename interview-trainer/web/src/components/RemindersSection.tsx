import { useState } from 'react';
import { ICS_FILE_NAME, buildReminderIcs, defaultPrefs, type NotifyPermission, type Progress } from '@trainer/shared';
import { DECLINED_NOTE, saveFile, useCanDownloadFiles } from '../lib/files';
import { notificationPermission, requestNotificationPermission } from '../lib/reminders';

interface Props {
  progress: Progress;
  update: (fn: (p: Progress) => Progress) => void;
}

/** Settings -> Reminders: a calendar event that always works, and an honest, optional, once-a-day notification. */
export function RemindersSection({ progress, update }: Props) {
  const { time, notify } = progress.prefs.reminders;
  const [permission, setPermission] = useState<NotifyPermission>(notificationPermission);
  const [message, setMessage] = useState('');
  const usual = progress.profile.practiceTime;

  const setReminders = (patch: Partial<Progress['prefs']['reminders']>) =>
    update((p) => ({ ...p, prefs: { ...p.prefs, reminders: { ...p.prefs.reminders, ...patch } } }));

  const onTime = (value: string) => {
    setMessage('');
    if (value === '') setReminders({ time: null, notify: false });
    else setReminders({ time: value });
  };

  const canDownload = useCanDownloadFiles(ICS_FILE_NAME);

  const download = async () => {
    if (!time) return;
    const ics = buildReminderIcs({ time, now: new Date(), restDays: progress.profile.restDays });
    const result = await saveFile(ICS_FILE_NAME, ics, 'text/calendar');
    if (result === 'saved') setMessage('Calendar file saved. Open it to add a repeating event to your calendar app.');
    else if (result === 'declined') setMessage(DECLINED_NOTE);
    else setMessage('This page cannot save calendar files. Open the installed app to add a repeating event.');
  };

  const toggleNotify = async (on: boolean) => {
    setMessage('');
    if (!on) return setReminders({ notify: false });
    const result = permission === 'granted' ? 'granted' : await requestNotificationPermission();
    setPermission(result);
    if (result === 'granted') setReminders({ notify: true });
    else {
      setReminders({ notify: false });
      setMessage(
        result === 'denied'
          ? 'Notifications are blocked for this site in your browser settings. The calendar event still works.'
          : 'Notifications are not available here. The calendar event still works.',
      );
    }
  };

  const off = () => {
    update((p) => ({ ...p, prefs: { ...p.prefs, reminders: { ...defaultPrefs().reminders, lastNotified: p.prefs.reminders.lastNotified } } }));
    setMessage('Reminders are off.');
  };

  const canNotify = permission !== 'unsupported';

  return (
    <section aria-labelledby="reminders-h">
      <h3 id="reminders-h">Reminders</h3>
      <p className="muted">
        Optional. Nothing is sent from a server and nothing is tracked.
      </p>

      <div className="field">
        <label htmlFor="reminder-time"><b>Daily reminder time</b></label>
        <input id="reminder-time" className="time-input" type="time" value={time ?? ''} onChange={(e) => onTime(e.target.value)} data-testid="reminder-time" />
        {!time && usual && (
          <button type="button" className="link" onClick={() => onTime(usual)}>Use my usual practice time ({usual})</button>
        )}
      </div>

      {canDownload ? (
        <button type="button" className="btn secondary wide" onClick={() => void download()} disabled={!time} data-testid="reminder-ics">
          Add to my calendar (.ics)
        </button>
      ) : (
        <p className="hint" data-testid="reminder-ics-off">This hosted page cannot save calendar (.ics) files. Open the installed app to add a repeating event.</p>
      )}
      <p className="muted">
        The event repeats daily{progress.profile.restDays.length ? ', skipping your rest days' : ''} and works while the app is closed.
      </p>

      {canNotify && (
        <label className="toggle-row">
          <input type="checkbox" checked={notify && permission === 'granted'} disabled={!time} onChange={(e) => void toggleNotify(e.target.checked)} data-testid="reminder-notify" />
          <span>
            <b>Notify me when I open the app</b>
            <small>
              At most once a day, after your reminder time, if something is due. Browsers usually cannot wake this app while it is closed: use the calendar event for that.
            </small>
          </span>
        </label>
      )}

      {message && <p role="status" className="notice" data-testid="reminder-message">{message}</p>}
      {(time || notify) && <button type="button" className="link" onClick={off} data-testid="reminder-off">Turn reminders off</button>}
    </section>
  );
}
