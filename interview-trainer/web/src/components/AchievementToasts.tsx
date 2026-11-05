import { useEffect } from 'react';
import { listNames } from '../lib/format';
import { clearBadgeToasts, dismissBadgeToast, useBadgeToast } from '../lib/toasts';
import { Icon, isIconName } from './icons';

/** How long a notice stays: long enough to read two short lines, and it can always be dismissed. */
const SHOW_MS = 4000;

/**
 * A calm "badge unlocked" notice. It sits above the bottom bar, never over a heading or a prompt, never blocks the page
 * (only its own dismiss button takes taps), goes away by itself, does not follow the learner to another screen and is
 * announced politely to screen readers. A finished session shows its new badges inline on the summary instead.
 */
export function AchievementToasts() {
  const toast = useBadgeToast();
  const id = toast?.id;

  useEffect(() => {
    if (id === undefined) return;
    const t = window.setTimeout(() => dismissBadgeToast(id), SHOW_MS);
    return () => window.clearTimeout(t);
  }, [id]);

  useEffect(() => {
    window.addEventListener('hashchange', clearBadgeToasts);
    return () => window.removeEventListener('hashchange', clearBadgeToasts);
  }, []);

  return (
    <div className="badge-toast-region" role="status" aria-live="polite">
      {toast && (
        <div className="badge-toast" data-testid="badge-toast" key={toast.id}>
          <span className="badge-icon" aria-hidden="true"><Icon name={isIconName(toast.badges[0]!.icon) ? toast.badges[0]!.icon : 'award'} size={20} /></span>
          <span className="badge-toast-text">
            <b>{toast.badges.length === 1 ? `New badge: ${toast.badges[0]!.title}` : `${toast.badges.length} new badges`}</b>
            <small>{toast.badges.length === 1 ? toast.badges[0]!.description : listNames(toast.badges.map((b) => b.title))}</small>
          </span>
          <button type="button" className="icon-btn" aria-label="Dismiss badge notification" onClick={() => dismissBadgeToast(toast.id)}><Icon name="x" /></button>
        </div>
      )}
    </div>
  );
}
