/** Local calendar day, `YYYY-MM-DD`. All day arithmetic is done on UTC day numbers, so DST never matters. */
export type Day = string;

const DAY_RE = /^(\d{4})-(\d{2})-(\d{2})$/;
const MS_PER_DAY = 86_400_000;

/** True for a real calendar date in `YYYY-MM-DD` form (rejects `2026-02-30`, `2026-13-01`, non-strings). */
export function isDay(x: unknown): x is Day {
  if (typeof x !== 'string') return false;
  const m = DAY_RE.exec(x);
  if (!m) return false;
  const y = Number(m[1]);
  const mo = Number(m[2]);
  const d = Number(m[3]);
  if (y < 1970 || y > 2999) return false;
  const dt = new Date(Date.UTC(y, mo - 1, d));
  return dt.getUTCFullYear() === y && dt.getUTCMonth() === mo - 1 && dt.getUTCDate() === d;
}

/** Local-time YYYY-MM-DD of an instant. The only function here that depends on the machine's timezone. */
export function dayKey(d: Date): Day {
  const p = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}

/** Days since 1970-01-01 (UTC based, so it is DST-safe). */
export function dayNumber(k: Day): number {
  const [y, m, d] = k.split('-').map(Number) as [number, number, number];
  return Math.round(Date.UTC(y, m - 1, d) / MS_PER_DAY);
}

export function fromDayNumber(n: number): Day {
  return new Date(n * MS_PER_DAY).toISOString().slice(0, 10);
}

export const addDays = (k: Day, n: number): Day => fromDayNumber(dayNumber(k) + n);

/** `to - from` in days; negative when `to` is earlier. */
export const daysBetween = (from: Day, to: Day): number => dayNumber(to) - dayNumber(from);

/** 0 = Sunday .. 6 = Saturday. */
export const weekday = (k: Day): number => (((dayNumber(k) + 4) % 7) + 7) % 7;
