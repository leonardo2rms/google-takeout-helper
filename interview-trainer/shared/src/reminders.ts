/**
 * Daily reminders, built to be calm and honest:
 *  - an iCalendar (RFC 5545) recurring event the learner imports into the calendar they already trust, and
 *  - a pure rule for the one local notification we may show when the app is opened after the chosen time.
 * There is no push server, so nothing here can wake the app while it is closed. All functions take the clock as an argument.
 */
import { addDays, dayKey, weekday, type Day } from './day';
import { isClockTime, type RemindersPrefs } from './prefs';

// ---------- .ics ----------

export const ICS_FILE_NAME = 'interview-trainer-daily-reminder.ics';
/** Stable, so importing the file again updates the same event instead of piling up duplicates. */
export const ICS_UID = 'daily-practice@interview-trainer';

const BYDAY = ['SU', 'MO', 'TU', 'WE', 'TH', 'FR', 'SA'] as const;
const CRLF = '\r\n';
const encoder = new TextEncoder();

/** TEXT values: backslash, semicolon, comma and newlines are escaped (RFC 5545 section 3.3.11). */
export function escapeIcsText(s: string): string {
  return s.replace(/\\/g, '\\\\').replace(/;/g, '\\;').replace(/,/g, '\\,').replace(/\r\n|\r|\n/g, '\\n');
}

/**
 * Folds a content line at 75 octets (RFC 5545 section 3.1): continuation lines start with one space, and
 * a multi-byte UTF-8 character is never split.
 */
export function foldIcsLine(line: string): string {
  if (encoder.encode(line).length <= 75) return line;
  const out: string[] = [];
  let current = '';
  let bytes = 0;
  let limit = 75; // continuation lines lose one octet to their leading space
  for (const ch of line) {
    const n = encoder.encode(ch).length;
    if (bytes + n > limit) {
      out.push(current);
      current = '';
      bytes = 0;
      limit = 74;
    }
    current += ch;
    bytes += n;
  }
  out.push(current);
  return out.join(`${CRLF} `);
}

const p2 = (n: number) => String(n).padStart(2, '0');
const compactDay = (d: Day) => d.replace(/-/g, '');
/** UTC timestamp `YYYYMMDDTHHMMSSZ`. */
const utcStamp = (d: Date) => `${d.getUTCFullYear()}${p2(d.getUTCMonth() + 1)}${p2(d.getUTCDate())}T${p2(d.getUTCHours())}${p2(d.getUTCMinutes())}${p2(d.getUTCSeconds())}Z`;

export const minutesOfDay = (hhmm: string): number => Number(hhmm.slice(0, 2)) * 60 + Number(hhmm.slice(3, 5));

export interface IcsOptions {
  /** 'HH:mm' in the learner's local time. */
  time: string;
  /** "Now" on the learner's clock: decides the first occurrence and the DTSTAMP. */
  now: Date;
  /** Weekdays (0 = Sunday) with no reminder: the learner's rest days. */
  restDays?: readonly number[];
  summary?: string;
  description?: string;
  /** Event length; the reminder fires at its start. */
  minutes?: number;
}

/**
 * A recurring VEVENT with a display alarm at the start. The time is a *floating* local time (no TZID): "19:00
 * wherever you are", which is what a daily habit wants, follows you when you travel and needs no VTIMEZONE rules.
 */
export function buildReminderIcs(o: IcsOptions): string {
  if (!isClockTime(o.time)) throw new Error('time must be HH:mm');
  const summary = o.summary ?? 'Interview Trainer: a few questions';
  const description = o.description ?? 'A short practice session, about five minutes. Skip a day whenever you need to.';
  const rest = new Set(o.restDays ?? []);
  const days = BYDAY.filter((_, i) => !rest.has(i));
  const byDay = days.length === 0 || days.length === 7 ? null : days.join(',');

  // First occurrence: today if the time is still ahead, otherwise tomorrow; then the first day that is not a rest day.
  let first: Day = dayKey(o.now);
  if (o.now.getHours() * 60 + o.now.getMinutes() >= minutesOfDay(o.time)) first = addDays(first, 1);
  for (let i = 0; i < 7 && byDay && rest.has(weekday(first)); i++) first = addDays(first, 1);

  const hhmmss = `${o.time.slice(0, 2)}${o.time.slice(3, 5)}00`;
  const lines = [
    'BEGIN:VCALENDAR',
    'VERSION:2.0',
    'PRODID:-//Interview Trainer//Daily reminder//EN',
    'CALSCALE:GREGORIAN',
    'METHOD:PUBLISH',
    'BEGIN:VEVENT',
    `UID:${ICS_UID}`,
    `DTSTAMP:${utcStamp(o.now)}`,
    `DTSTART:${compactDay(first)}T${hhmmss}`,
    `DURATION:PT${Math.max(1, Math.round(o.minutes ?? 10))}M`,
    byDay ? `RRULE:FREQ=WEEKLY;BYDAY=${byDay}` : 'RRULE:FREQ=DAILY',
    `SUMMARY:${escapeIcsText(summary)}`,
    `DESCRIPTION:${escapeIcsText(description)}`,
    'TRANSP:TRANSPARENT',
    'BEGIN:VALARM',
    'ACTION:DISPLAY',
    `DESCRIPTION:${escapeIcsText(summary)}`,
    'TRIGGER:PT0S',
    'END:VALARM',
    'END:VEVENT',
    'END:VCALENDAR',
  ];
  return lines.map(foldIcsLine).join(CRLF) + CRLF;
}

// ---------- the one notification ----------

export type NotifyPermission = 'granted' | 'denied' | 'default' | 'unsupported';

export interface NotifyInput {
  prefs: RemindersPrefs;
  now: Date;
  today: Day;
  /** Questions ready to review right now. */
  dueCount: number;
  /** The learner already practised today: no reminder needed. */
  practicedToday: boolean;
  restDays: readonly number[];
  permission: NotifyPermission;
}

export type NotifyDecision =
  | { show: true; title: string; body: string }
  | { show: false; reason: 'off' | 'no-permission' | 'too-early' | 'already-today' | 'nothing-due' | 'practised' | 'rest-day' };

/**
 * Whether to show a notification now. Never more than one a day, never on a rest day, never when the learner has
 * already practised or nothing is due, never before the chosen time, never without permission.
 */
export function reminderDecision(i: NotifyInput): NotifyDecision {
  const { prefs } = i;
  if (!prefs.notify || prefs.time === null) return { show: false, reason: 'off' };
  if (i.permission !== 'granted') return { show: false, reason: 'no-permission' };
  if (prefs.lastNotified === i.today) return { show: false, reason: 'already-today' };
  if (i.restDays.includes(weekday(i.today))) return { show: false, reason: 'rest-day' };
  if (i.now.getHours() * 60 + i.now.getMinutes() < minutesOfDay(prefs.time)) return { show: false, reason: 'too-early' };
  if (i.practicedToday) return { show: false, reason: 'practised' };
  if (i.dueCount <= 0) return { show: false, reason: 'nothing-due' };
  const n = i.dueCount;
  return {
    show: true,
    title: 'Interview Trainer',
    body: `${n >= 30 ? '30+' : n} ${n === 1 ? 'question is' : 'questions are'} ready to review. A short session takes about five minutes, whenever it suits you.`,
  };
}

/** The preference after a notification was shown today. */
export const markNotified = (prefs: RemindersPrefs, today: Day): RemindersPrefs => ({ ...prefs, lastNotified: today });
