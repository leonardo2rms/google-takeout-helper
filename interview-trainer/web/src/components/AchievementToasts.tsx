import { useEffect } from 'react';
import { dismissBadgeToast, useBadgeToast } from '../lib/toasts';
import { Icon, isIconName } from './icons';

/** How long a notice stays: long enough to read, and it can always be dismissed. */
const SHOW_MS = 6000;

/**
 * A calm "badge unlocked" notice. It never blocks the page (only its own dismiss button takes taps), is announced politely
 * to screen readers and only slides in when the device allows motion (see the stylesheet).
 */
export function AchievementToasts() {
  const toast = useBadgeToast();
  const id = toast?.id;

  useEffect(() => {
    if (id === undefined) return;
    const t = window.setTimeout(() => dismissBadgeToast(id), SHOW_MS);
    return () => window.clearTimeout(t);
  }, [id]);

  return (
    <div className="badge-toast-region" role="status" aria-live="polite">
      {toast && (
        <div className="badge-toast" data-testid="badge-toast" key={toast.id}>
          <button type="button" className="icon-btn" aria-label="Dismiss badge notification" onClick={() => dismissBadgeToast(toast.id)}><Icon name="x" /></button>
          <span className="badge-icon" aria-hidden="true"><Icon name={isIconName(toast.badges[0]!.icon) ? toast.badges[0]!.icon : 'award'} size={20} /></span>
          <span className="badge-toast-text">
            <b>{toast.badges.length === 1 ? `Badge unlocked: ${toast.badges[0]!.title}` : `${toast.badges.length} badges unlocked`}</b>
            <small>
              {toast.badges.length === 1 ? toast.badges[0]!.description : toast.badges.map((b) => b.title).join(', ')}
              {' '}You can see all of them on the Progress page.
            </small>
          </span>
        </div>
      )}
    </div>
  );
}
