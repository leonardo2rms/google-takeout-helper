/**
 * Data-safety rules of the storage layer: unreadable data is never overwritten before a copy of it exists, copies are
 * never replaced by later ones, they rotate, and they can be listed. Regression tests for QA findings S0-1 and S1-6.
 */
import { describe, expect, it } from 'vitest';
import {
  BACKUP_PREFIX, KEY_V2, MAX_PARKED, SAFETY_PREFIX, backupKey, emptyProgress, listParked, loadProgress, parkRaw, parkedKey, saveProgress,
  type StorageLike,
} from './index';

const ctx = { today: '2026-11-05', questions: [] };

interface Fake extends StorageLike {
  data: Map<string, string>;
  failKeys: (k: string) => boolean;
}
function fake(initial: Record<string, string> = {}): Fake {
  const data = new Map(Object.entries(initial));
  const s: Fake = {
    data,
    failKeys: () => false,
    getItem: (k) => data.get(k) ?? null,
    setItem(k, v) {
      if (s.failKeys(k)) throw new DOMException('quota', 'QuotaExceededError');
      data.set(k, v);
    },
    removeItem: (k) => void data.delete(k),
    get length() {
      return data.size;
    },
    key: (i) => [...data.keys()][i] ?? null,
  };
  return s;
}

const future = JSON.stringify({ version: 3, xp: 999 });

describe('saveProgress never destroys the only copy of unreadable data (S0-1)', () => {
  it('does not write at all when the copy cannot be made', () => {
    const s = fake({ [KEY_V2]: future });
    s.failKeys = (k) => k.startsWith(BACKUP_PREFIX);
    const l = loadProgress(s, ctx);
    expect(l.protectedRaw).toBe(future);
    const saved = saveProgress(s, { ...emptyProgress(), xp: 10 }, l.protectedRaw, '2026-11-05');
    expect(saved).toMatchObject({ backedUp: false, written: false, skipped: true });
    expect(s.data.get(KEY_V2)).toBe(future); // the newer-version data survives
  });

  it('writes once the copy succeeds on a later attempt', () => {
    const s = fake({ [KEY_V2]: future });
    s.failKeys = (k) => k.startsWith(BACKUP_PREFIX);
    saveProgress(s, emptyProgress(), future, '2026-11-05');
    s.failKeys = () => false;
    const saved = saveProgress(s, { ...emptyProgress(), xp: 10 }, future, '2026-11-05');
    expect(saved).toMatchObject({ backedUp: true, written: true, skipped: false });
    expect(s.data.get(backupKey('2026-11-05'))).toBe(future);
    expect(JSON.parse(s.data.get(KEY_V2)!).xp).toBe(10);
  });

  it('reports a failed main write without claiming success', () => {
    const s = fake();
    s.failKeys = (k) => k === KEY_V2;
    const saved = saveProgress(s, emptyProgress(), null, '2026-11-05');
    expect(saved).toMatchObject({ written: false, skipped: false, backedUp: false });
  });
});

describe('parked copies', () => {
  it('a second corruption on the same day does not replace the first copy', () => {
    const s = fake();
    const a = parkRaw(s, '{first corrupt', '2026-11-05', 'unreadable');
    const b = parkRaw(s, '{second corrupt', '2026-11-05', 'unreadable');
    expect(a).toBe(backupKey('2026-11-05'));
    expect(b).toBe(parkedKey('unreadable', '2026-11-05', 2));
    expect(s.data.get(a!)).toBe('{first corrupt');
    expect(s.data.get(b!)).toBe('{second corrupt');
  });

  it('an identical copy is reused, not duplicated', () => {
    const s = fake();
    const a = parkRaw(s, 'same', '2026-11-05', 'unreadable');
    expect(parkRaw(s, 'same', '2026-11-06', 'unreadable')).toBe(a);
    expect(listParked(s)).toHaveLength(1);
  });

  it('rotates: keeps the newest MAX_PARKED and drops the oldest only after writing the new one', () => {
    const s = fake();
    for (let i = 0; i < MAX_PARKED.unreadable + 3; i++) parkRaw(s, `bad-${i}`, '2026-11-05', 'unreadable');
    const kept = listParked(s).map((c) => c.raw);
    expect(kept).toHaveLength(MAX_PARKED.unreadable);
    expect(kept[0]).toBe(`bad-${MAX_PARKED.unreadable + 2}`);
    expect(kept).not.toContain('bad-0');
  });

  it('returns null and drops nothing when the write fails', () => {
    const s = fake();
    parkRaw(s, 'one', '2026-11-05', 'unreadable');
    s.failKeys = () => true;
    expect(parkRaw(s, 'two', '2026-11-05', 'unreadable')).toBeNull();
    expect(listParked(s).map((c) => c.raw)).toEqual(['one']);
  });

  it('continues on the next day stamp when a day has used up every suffix, instead of refusing to save', () => {
    const s = fake();
    for (let i = 0; i < 99; i++) s.data.set(parkedKey('unreadable', '2026-11-05', i + 1), `x${i}`);
    const key = parkRaw(s, 'new', '2026-11-05', 'unreadable');
    expect(key).toBe(parkedKey('unreadable', '2026-11-06', 1));
    expect(listParked(s)[0]).toMatchObject({ key, raw: 'new' });
  });

  it('keeps the copy it just wrote when the clock was set back (copies from a later day exist)', () => {
    const s = fake();
    for (let i = 0; i < MAX_PARKED.unreadable; i++) s.data.set(parkedKey('unreadable', '2027-01-01', i + 1), `later-${i}`);
    const key = parkRaw(s, future, '2026-11-05', 'unreadable');
    expect(key).not.toBeNull();
    const kept = listParked(s);
    expect(kept).toHaveLength(MAX_PARKED.unreadable);
    expect(kept.map((c) => c.raw)).toContain(future); // the newest-version data survives
    expect(kept[0]!.key).toBe(key); // and sorts newest, so the "recovered" banner (keys above the acknowledged one) fires
    expect(kept.map((c) => c.raw)).not.toContain('later-0'); // the oldest was the one dropped
  });

  it('keeps safety copies from being dropped in the same clock-set-back case', () => {
    const s = fake();
    for (let i = 0; i < MAX_PARKED.safety; i++) s.data.set(parkedKey('safety', '2027-01-01', i + 1), `later-${i}`);
    expect(parkRaw(s, 'mine', '2026-11-05', 'safety')).not.toBeNull();
    expect(listParked(s).map((c) => c.raw)).toContain('mine');
  });

  it('does not overwrite an existing slot in a storage that cannot list its keys', () => {
    const data = new Map<string, string>();
    const blind: StorageLike = { getItem: (k) => data.get(k) ?? null, setItem: (k, v) => void data.set(k, v) };
    const k1 = parkRaw(blind, 'first', '2026-11-05', 'unreadable');
    const k2 = parkRaw(blind, 'second', '2026-11-05', 'unreadable');
    expect(k1).not.toBe(k2);
    expect([...data.values()].sort()).toEqual(['first', 'second']);
  });

  it('never reuses a slot after rotation freed a lower one, and ignores keys with an impossible date', () => {
    const s = fake();
    s.data.set(`${BACKUP_PREFIX}99999999`, 'impossible-date');
    for (let i = 0; i < 12; i++) parkRaw(s, `b${i}`, '2026-11-05', 'unreadable');
    const raws = listParked(s).map((c) => c.raw);
    expect(raws).toContain('b11');
    expect(new Set(raws).size).toBe(raws.length);
  });

  it('lists both kinds with their day, and ignores other keys', () => {
    const s = fake({ other: 'x', [KEY_V2]: '{}' });
    parkRaw(s, 'u', '2026-11-04', 'unreadable');
    parkRaw(s, 's', '2026-11-05', 'safety');
    s.data.set(`${BACKUP_PREFIX}garbage`, 'g');
    const l = listParked(s);
    expect(l).toHaveLength(3);
    expect(l.find((c) => c.raw === 'u')).toMatchObject({ kind: 'unreadable', day: '2026-11-04' });
    expect(l.find((c) => c.raw === 's')).toMatchObject({ kind: 'safety', day: '2026-11-05' });
    expect(l.find((c) => c.raw === 'g')).toMatchObject({ kind: 'unreadable', day: null });
    expect(l.every((c) => c.key.startsWith(BACKUP_PREFIX) || c.key.startsWith(SAFETY_PREFIX))).toBe(true);
    expect(l.map((c) => c.key)).toEqual([...l.map((c) => c.key)].sort().reverse()); // newest key first
  });

  it('keeps safety copies apart from unreadable ones when rotating', () => {
    const s = fake();
    parkRaw(s, 'u', '2026-11-05', 'unreadable');
    for (let i = 0; i < MAX_PARKED.safety + 2; i++) parkRaw(s, `s${i}`, '2026-11-05', 'safety');
    const l = listParked(s);
    expect(l.filter((c) => c.kind === 'safety')).toHaveLength(MAX_PARKED.safety);
    expect(l.filter((c) => c.kind === 'unreadable')).toHaveLength(1);
  });

  it('lists nothing from a storage that cannot enumerate or that throws', () => {
    expect(listParked({ getItem: () => null, setItem: () => {} })).toEqual([]);
    expect(listParked({ getItem: () => null, setItem: () => {}, length: 1, key: () => { throw new Error('denied'); } })).toEqual([]);
  });

  it('survives removeItem throwing during rotation', () => {
    const s = fake();
    s.removeItem = () => { throw new Error('denied'); };
    for (let i = 0; i < MAX_PARKED.unreadable + 1; i++) expect(parkRaw(s, `b${i}`, '2026-11-05', 'unreadable')).not.toBeNull();
  });
});

describe('loadProgress keeps a copy before normalising away data (N-S2-4)', () => {
  const valid = () => JSON.parse(JSON.stringify({ ...emptyProgress(), xp: 40 })) as Record<string, unknown>;

  it('parks the original when a same-version profile carries a field this version does not know', () => {
    const raw = JSON.stringify({ ...valid(), futureThing: { a: 1 } });
    const s = fake({ [KEY_V2]: raw });
    const res = loadProgress(s, ctx);
    expect(res.progress.xp).toBe(40);
    expect(res.protectedRaw).toBeNull();
    expect(listParked(s).map((c) => c.raw)).toEqual([raw]); // the original text, extra field included
    expect(s.data.get(KEY_V2)).not.toContain('futureThing');
  });

  it('parks the original when a field is malformed (a streak of "x")', () => {
    const raw = JSON.stringify({ ...valid(), streak: 'x' });
    const s = fake({ [KEY_V2]: raw });
    loadProgress(s, ctx);
    expect(listParked(s).map((c) => c.raw)).toEqual([raw]);
  });

  it('parks when a nested field is unknown or an invalid record is dropped', () => {
    const p = valid() as { settings: Record<string, unknown>; cards: Record<string, unknown> };
    p.settings.hologram = true;
    const raw = JSON.stringify(p);
    const s = fake({ [KEY_V2]: raw });
    loadProgress(s, ctx);
    expect(listParked(s)).toHaveLength(1);
  });

  it('does not write the normalised profile when the copy cannot be made: the original stays, protected', () => {
    const raw = JSON.stringify({ ...valid(), futureThing: 1 });
    const s = fake({ [KEY_V2]: raw });
    s.failKeys = (k) => k.startsWith(BACKUP_PREFIX);
    const res = loadProgress(s, ctx);
    expect(res.protectedRaw).toBe(raw);
    expect(s.data.get(KEY_V2)).toBe(raw);
    expect(res.progress.xp).toBe(40);
  });

  it('stays quiet for routine changes: defaults filled in, clamped numbers, retired days', () => {
    const old = valid() as Record<string, unknown>;
    delete old.longestStreak; // a default gets filled in
    old.freezes = 99; // clamped
    old.days = { '2020-01-01': { xp: 5, answered: 1, firstTry: 1, target: 3 } }; // older than the retention window
    const s = fake({ [KEY_V2]: JSON.stringify(old) });
    const res = loadProgress(s, ctx);
    expect(res.protectedRaw).toBeNull();
    expect(listParked(s)).toEqual([]);
    expect(s.data.get(KEY_V2)).not.toContain('2020-01-01'); // still normalised and rewritten
  });
});
