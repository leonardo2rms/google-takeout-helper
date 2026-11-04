import { useState } from 'react';
import { ICS_FILE_NAME, buildReminderIcs, defaultPrefs, type NotifyPermission, type Progress } from '@trainer/shared';
import { canDownloadFiles, notificationPermission, requestNotificationPermission, saveTextFile } from '../lib/reminders';

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

  const download = () => {
    if (!time) return;
    const ics = buildReminderIcs({ time, now: new Date(), restDays: progress.profile.restDays });
    saveTextFile(ICS_FILE_NAME, ics, 'text/calendar');
    setMessage('Calendar file saved. Open it to add a repeating event to your calendar app.');
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
        Totally optional. Pick a time that suits you; nothing is sent from a server and nothing is tracked. A reminder never asks for more than a few minutes, and you can skip any day.
      </p>

      <div className="field">
        <label htmlFor="reminder-time"><b>Daily reminder time</b></label>
        <input id="reminder-time" className="time-input" type="time" value={time ?? ''} onChange={(e) => onTime(e.target.value)} data-testid="reminder-time" />
        {!time && usual && (
          <button type="button" className="link" onClick={() => onTime(usual)}>Use my usual practice time ({usual})</button>
        )}
      </div>

      {canDownloadFiles() ? (
        <button type="button" className="btn secondary wide" onClick={download} disabled={!time} data-testid="reminder-ics">
          Add to my calendar (.ics)
        </button>
      ) : (
        <p className="hint" data-testid="reminder-ics-off">Calendar files cannot be downloaded from this hosted page. Open the installed app to add a repeating event.</p>
      )}
      <p className="muted">
        The calendar event repeats at that time{progress.profile.restDays.length ? ', skipping your rest days' : ''} and works even when this app is closed. Re-importing it updates the same event.
      </p>

      {canNotify && (
        <label className="toggle-row">
          <input type="checkbox" checked={notify && permission === 'granted'} disabled={!time} onChange={(e) => void toggleNotify(e.target.checked)} data-testid="reminder-notify" />
          <span>
            <b>Notify me when I open the app</b>
            <small>
              Once a day at most, only after your reminder time and only if something is due. Browsers can usually not wake this app while it is closed, so
              this will not arrive on its own: use the calendar event for that.
            </small>
          </span>
        </label>
      )}

      {message && <p role="status" className="notice" data-testid="reminder-message">{message}</p>}
      {(time || notify) && <button type="button" className="link" onClick={off} data-testid="reminder-off">Turn reminders off</button>}
    </section>
  );
}
