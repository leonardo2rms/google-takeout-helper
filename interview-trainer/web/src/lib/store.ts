import { useCallback, useState } from 'react';
import { emptyProgress, type Progress } from '@trainer/shared';

const KEY = 'interview-trainer.progress.v1';

function load(): Progress {
  try {
    const parsed = JSON.parse(localStorage.getItem(KEY) ?? 'null') as Partial<Progress> | null;
    if (parsed && parsed.version === 1 && typeof parsed.xp === 'number') return { ...emptyProgress(), ...parsed };
  } catch {
    /* corrupt or unavailable storage: start fresh */
  }
  return emptyProgress();
}

function save(p: Progress) {
  try {
    localStorage.setItem(KEY, JSON.stringify(p));
  } catch {
    /* private mode / quota: progress just won't persist */
  }
}

export function useProgress() {
  const [progress, setProgress] = useState<Progress>(load);
  const update = useCallback((fn: (p: Progress) => Progress) => {
    setProgress((prev) => {
      const next = fn(prev);
      save(next);
      return next;
    });
  }, []);
  const reset = useCallback(() => update(() => emptyProgress()), [update]);
  return { progress, update, reset };
}
