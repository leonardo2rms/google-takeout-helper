import { describe, expect, it } from 'vitest';
import { defaultPrefs, normalizePrefs } from './prefs';
import { emptyProgress } from './progress';
import { normalizeProgress } from './migrate';
import {
  buildReminderIcs, escapeIcsText, foldIcsLine, markNotified, reminderDecision, type NotifyInput,
} from './reminders';

// Wednesday 2026-11-04, 10:00 local
const NOW = new Date(2026, 10, 4, 10, 0, 0);

const unfold = (ics: string) => ics.replace(/\r\n /g, '');
const lines = (ics: string) => unfold(ics).split('\r\n').filter(Boolean);
const octets = (s: string) => new TextEncoder().encode(s).length;

describe('escapeIcsText', () => {
  it('escapes backslash, semicolon, comma and newlines', () => {
    expect(escapeIcsText('a\\b; c, d\ne\r\nf')).toBe('a\\\\b\\; c\\, d\\ne\\nf');
  });
});

describe('foldIcsLine', () => {
  it('leaves short lines alone', () => {
    expect(foldIcsLine('SUMMARY:short')).toBe('SUMMARY:short');
  });
  it('folds at 75 octets with CRLF + space and unfolds back to the original', () => {
    const long = `DESCRIPTION:${'word '.repeat(40)}`;
    const folded = foldIcsLine(long);
    const parts = folded.split('\r\n');
    expect(parts.length).toBeGreaterThan(2);
    for (const [i, part] of parts.entries()) {
      expect(octets(part)).toBeLessThanOrEqual(75);
      if (i > 0) expect(part.startsWith(' ')).toBe(true);
    }
    expect(folded.replace(/\r\n /g, '')).toBe(long);
  });
  it('never splits a multi-byte character', () => {
    const long = `SUMMARY:${'é€😀'.repeat(30)}`;
    const folded = foldIcsLine(long);
    for (const part of folded.split('\r\n')) expect(octets(part)).toBeLessThanOrEqual(75);
    expect(folded.replace(/\r\n /g, '')).toBe(long);
  });
});

describe('buildReminderIcs', () => {
  const ics = buildReminderIcs({ time: '19:30', now: NOW });
  const L = lines(ics);

  it('is a well-formed VCALENDAR with CRLF line ends', () => {
    expect(ics.endsWith('\r\n')).toBe(true);
    expect(ics.replace(/\r\n/g, '')).not.toMatch(/[\r\n]/);
    expect(L[0]).toBe('BEGIN:VCALENDAR');
    expect(L).toContain('VERSION:2.0');
    expect(L.some((l) => l.startsWith('PRODID:'))).toBe(true);
    expect(L[L.length - 1]).toBe('END:VCALENDAR');
    expect(L.filter((l) => l.startsWith('BEGIN:')).map((l) => l.slice(6))).toEqual(['VCALENDAR', 'VEVENT', 'VALARM']);
    expect(L.filter((l) => l.startsWith('END:')).map((l) => l.slice(4))).toEqual(['VALARM', 'VEVENT', 'VCALENDAR']);
  });
  it('repeats daily from today when the time is still ahead, as floating local time', () => {
    expect(L).toContain('DTSTART:20261104T193000');
    expect(L).toContain('RRULE:FREQ=DAILY');
    expect(L.some((l) => /^DTSTART.*(TZID|Z$)/.test(l))).toBe(false);
  });
  it('starts tomorrow when the time has passed (clock injected)', () => {
    expect(lines(buildReminderIcs({ time: '09:00', now: NOW }))).toContain('DTSTART:20261105T090000');
    expect(lines(buildReminderIcs({ time: '10:00', now: NOW }))).toContain('DTSTART:20261105T100000');
  });
  it('stamps in UTC, keeps a stable UID and carries a display alarm at the start', () => {
    const stamp = L.find((l) => l.startsWith('DTSTAMP:'))!;
    expect(stamp).toMatch(/^DTSTAMP:\d{8}T\d{6}Z$/);
    expect(L).toContain('UID:daily-practice@interview-trainer');
    expect(L).toContain('TRIGGER:PT0S');
    expect(L).toContain('ACTION:DISPLAY');
    expect(L).toContain('DURATION:PT10M');
  });
  it('skips rest days with a weekly rule, and starts on the first allowed day', () => {
    // Wed 4 Nov 2026; rest on Wed(3) and Thu(4): starts Friday 6th even though 19:30 is still ahead
    const r = lines(buildReminderIcs({ time: '19:30', now: NOW, restDays: [3, 4, 0] }));
    expect(r).toContain('RRULE:FREQ=WEEKLY;BYDAY=MO,TU,FR,SA');
    expect(r).toContain('DTSTART:20261106T193000');
  });
  it('escapes and folds user-visible text', () => {
    const text = buildReminderIcs({ time: '08:00', now: NOW, summary: 'Practice; then, relax', description: `${'long '.repeat(40)}\nsecond line` });
    for (const raw of text.split('\r\n')) expect(octets(raw)).toBeLessThanOrEqual(75);
    const u = lines(text);
    expect(u).toContain('SUMMARY:Practice\\; then\\, relax');
    expect(u.find((l) => l.startsWith('DESCRIPTION:long'))).toMatch(/\\nsecond line$/);
  });
  it('rejects a bad time', () => {
    expect(() => buildReminderIcs({ time: '25:00', now: NOW })).toThrow();
    expect(() => buildReminderIcs({ time: '7:00', now: NOW })).toThrow();
  });
});

describe('reminderDecision', () => {
  const base: NotifyInput = {
    prefs: { time: '09:00', notify: true, lastNotified: null },
    now: NOW,
    today: '2026-11-04',
    dueCount: 6,
    practicedToday: false,
    restDays: [],
    permission: 'granted',
  };
  const why = (over: Partial<NotifyInput>) => {
    const d = reminderDecision({ ...base, ...over });
    return d.show ? 'show' : d.reason;
  };

  it('shows one calm notification when opened after the time with something due', () => {
    const d = reminderDecision(base);
    expect(d).toMatchObject({ show: true, title: 'Interview Trainer' });
    if (d.show) {
      expect(d.body).toContain('6 questions are due for review');
      expect(d.body).not.toMatch(/!|streak|lose|don't|miss/i);
    }
  });
  it('shows the exact count and singularises', () => {
    const body = (n: number) => { const d = reminderDecision({ ...base, dueCount: n }); return d.show ? d.body : ''; };
    expect(body(1)).toMatch(/^1 question is due/);
    expect(body(80)).toMatch(/^80 questions are due/);
  });
  it('stays quiet in every other case', () => {
    expect(why({ prefs: { ...base.prefs, notify: false } })).toBe('off');
    expect(why({ prefs: { ...base.prefs, time: null } })).toBe('off');
    expect(why({ permission: 'denied' })).toBe('no-permission');
    expect(why({ permission: 'default' })).toBe('no-permission');
    expect(why({ permission: 'unsupported' })).toBe('no-permission');
    expect(why({ prefs: { ...base.prefs, time: '10:01' } })).toBe('too-early');
    expect(why({ prefs: { ...base.prefs, lastNotified: '2026-11-04' } })).toBe('already-today');
    expect(why({ dueCount: 0 })).toBe('nothing-due');
    expect(why({ practicedToday: true })).toBe('practised');
    expect(why({ restDays: [3] })).toBe('rest-day'); // 2026-11-04 is a Wednesday
  });
  it('at most once a day: after markNotified the same day is quiet, the next day shows again', () => {
    const prefs = markNotified(base.prefs, '2026-11-04');
    expect(why({ prefs })).toBe('already-today');
    expect(why({ prefs, today: '2026-11-05', now: new Date(2026, 10, 5, 12, 0) })).toBe('show');
  });
});

describe('prefs', () => {
  it('defaults are quiet and idempotent through normalize', () => {
    const d = defaultPrefs();
    expect(d.reminders).toEqual({ time: null, notify: false, lastNotified: null });
    expect(d.reading).toEqual({ textSize: 'M', dyslexiaFont: false });
    expect(normalizePrefs(d)).toEqual(d);
    expect(normalizePrefs(undefined)).toEqual(d);
    expect(normalizePrefs('junk')).toEqual(d);
  });
  it('drops invalid values and refuses notify without a time', () => {
    const p = normalizePrefs({ reminders: { time: '25:61', notify: true, lastNotified: 'yesterday' }, reading: { textSize: 'XXL', dyslexiaFont: 'yes' } });
    expect(p.reminders).toEqual({ time: null, notify: false, lastNotified: null });
    expect(p.reading).toEqual({ textSize: 'M', dyslexiaFont: false });
    expect(normalizePrefs({ reminders: { time: '07:05', notify: true, lastNotified: '2026-11-03' }, reading: { textSize: 'XL', dyslexiaFont: true } })).toEqual({
      reminders: { time: '07:05', notify: true, lastNotified: '2026-11-03' },
      reading: { textSize: 'XL', dyslexiaFont: true },
    });
  });
  it('progress without prefs (an older save) gets defaults; prefs survive normalizeProgress', () => {
    const ctx = { today: '2026-11-04', questions: [] };
    const { prefs: _omit, ...old } = emptyProgress();
    expect(normalizeProgress(old, ctx).prefs).toEqual(defaultPrefs());
    const withPrefs = { ...emptyProgress(), prefs: { ...defaultPrefs(), reading: { textSize: 'L' as const, dyslexiaFont: true } } };
    expect(normalizeProgress(normalizeProgress(withPrefs, ctx), ctx)).toEqual(normalizeProgress(withPrefs, ctx));
    expect(normalizeProgress(withPrefs, ctx).prefs.reading.textSize).toBe('L');
  });
});
