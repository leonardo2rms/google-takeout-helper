import { useEffect, useState } from 'react';
import { dayKey } from '@trainer/shared';

/**
 * The current local day, refreshed when the tab becomes visible again and once a minute, so a screen left
 * open across midnight rolls over to the new day (fresh ring, new due cards) without a reload.
 */
export function useToday(): string {
  const [today, setToday] = useState(() => dayKey(new Date()));
  useEffect(() => {
    const refresh = () => setToday(dayKey(new Date()));
    const id = window.setInterval(refresh, 60_000);
    document.addEventListener('visibilitychange', refresh);
    window.addEventListener('focus', refresh);
    return () => {
      window.clearInterval(id);
      document.removeEventListener('visibilitychange', refresh);
      window.removeEventListener('focus', refresh);
    };
  }, []);
  return today;
}
