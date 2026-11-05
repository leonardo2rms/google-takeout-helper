/**
 * `Progress.prefs`: device preferences that are not learning data. One namespaced key so later additions
 * (reminders, reading, ...) never collide with other fields of the schema. Settings stay local on merge/import.
 */
import { isDay, type Day } from './day';

export const TEXT_SIZES = ['S', 'M', 'L', 'XL'] as const;
export type TextSize = (typeof TEXT_SIZES)[number];

export interface RemindersPrefs {
  /** 'HH:mm' chosen by the learner for their daily reminder; null = none chosen. */
  time: string | null;
  /** The learner allowed (and asked for) a local notification when they open the app after `time` with something due. */
  notify: boolean;
  /** The last day a notification was shown: at most one per day. */
  lastNotified: Day | null;
}

export interface ReadingPrefs {
  textSize: TextSize;
  /** Use a plain system font stack with looser spacing, which many dyslexic readers find easier. */
  dyslexiaFont: boolean;
}

export interface Prefs {
  reminders: RemindersPrefs;
  reading: ReadingPrefs;
}

export const defaultPrefs = (): Prefs => ({
  reminders: { time: null, notify: false, lastNotified: null },
  reading: { textSize: 'M', dyslexiaFont: false },
});

export const isClockTime = (x: unknown): x is string => typeof x === 'string' && /^([01]\d|2[0-3]):[0-5]\d$/.test(x);

type Obj = Record<string, unknown>;
const isObj = (x: unknown): x is Obj => typeof x === 'object' && x !== null && !Array.isArray(x);

/** Fills in defaults and drops anything invalid. Never throws; idempotent. */
export function normalizePrefs(raw: unknown): Prefs {
  const d = defaultPrefs();
  const p: Obj = isObj(raw) ? raw : {};
  const r: Obj = isObj(p.reminders) ? p.reminders : {};
  const rd: Obj = isObj(p.reading) ? p.reading : {};
  const time = isClockTime(r.time) ? r.time : null;
  return {
    reminders: {
      time,
      // A notification preference without a time makes no sense.
      notify: time !== null && r.notify === true,
      lastNotified: isDay(r.lastNotified) ? r.lastNotified : null,
    },
    reading: {
      textSize: TEXT_SIZES.find((s) => s === rd.textSize) ?? d.reading.textSize,
      dyslexiaFont: rd.dyslexiaFont === true,
    },
  };
}
