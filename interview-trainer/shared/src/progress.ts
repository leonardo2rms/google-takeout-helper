export interface LessonRecord {
  bestAccuracy: number;
  completions: number;
  lastDay: string;
}

export interface Progress {
  version: 1;
  xp: number;
  streak: number;
  /** Local calendar day (YYYY-MM-DD) of the last finished lesson. */
  lastDay: string | null;
  lessons: Record<string, LessonRecord>;
  /** Question ids the learner has missed and not yet recovered in a review session. */
  missed: string[];
}

export const emptyProgress = (): Progress => ({ version: 1, xp: 0, streak: 0, lastDay: null, lessons: {}, missed: [] });

/** Local-time YYYY-MM-DD. */
export function dayKey(d: Date): string {
  const p = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}

function dayNumber(key: string): number {
  const [y, m, d] = key.split('-').map(Number) as [number, number, number];
  return Math.round(Date.UTC(y, m - 1, d) / 86_400_000);
}

export function nextStreak(streak: number, lastDay: string | null, today: string): number {
  if (lastDay === null) return 1;
  const gap = dayNumber(today) - dayNumber(lastDay);
  if (gap <= 0) return Math.max(streak, 1);
  return gap === 1 ? streak + 1 : 1;
}

/** A streak is "alive" if the learner practised today or yesterday; otherwise it has lapsed. */
export function liveStreak(p: Progress, today: string): number {
  if (p.lastDay === null) return 0;
  return dayNumber(today) - dayNumber(p.lastDay) <= 1 ? p.streak : 0;
}

export interface LessonResult {
  /** Omit for review sessions that aren't tied to one lesson. */
  lessonId?: string;
  accuracy: number;
  xp: number;
  missed: string[];
  recovered: string[];
  /** Question ids that were answered correctly in this session (clears them from `missed`). */
  cleared?: string[];
}

export function recordResult(p: Progress, r: LessonResult, today: string): Progress {
  const lessons = { ...p.lessons };
  if (r.lessonId) {
    const prev = lessons[r.lessonId];
    lessons[r.lessonId] = {
      bestAccuracy: Math.max(prev?.bestAccuracy ?? 0, r.accuracy),
      completions: (prev?.completions ?? 0) + 1,
      lastDay: today,
    };
  }
  const cleared = new Set([...(r.cleared ?? []), ...r.recovered]);
  const missed = [...new Set([...p.missed.filter((id) => !cleared.has(id)), ...r.missed.filter((id) => !r.recovered.includes(id))])];
  return {
    ...p,
    xp: p.xp + r.xp,
    streak: nextStreak(p.streak, p.lastDay, today),
    lastDay: today,
    lessons,
    missed,
  };
}
