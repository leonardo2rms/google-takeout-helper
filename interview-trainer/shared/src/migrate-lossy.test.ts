/** Normalising a stored profile: what counts as throwing data away (so a copy is kept first) and what is routine repair (N-S2-4). */
import { describe, expect, it } from 'vitest';
import { emptyProgress, migrateProgress, type MigrationContext } from './index';

const ctx: MigrationContext = { today: '2026-11-04', questions: [] };

type Draft = Record<string, unknown> & { profile: Record<string, unknown> };
const base = (): Draft =>
  JSON.parse(JSON.stringify({ ...emptyProgress(), xp: 40, missed: ['q1', 'q2'], profile: { ...emptyProgress().profile, restDays: [0, 6] } })) as Draft;
const lossy = (mutate: (p: Draft) => void): boolean | undefined => {
  const p = base();
  mutate(p);
  return migrateProgress(p, ctx).lossy;
};

describe('migrateProgress: lossy normalisation is told apart from routine repair', () => {
  it('is false for a clean profile and for repairs that lose nothing', () => {
    expect(migrateProgress(base(), ctx)).toMatchObject({ changed: false, lossy: false });
    expect(lossy((p) => { p.xp = -5; })).toBe(false); // a clamped number
    expect(lossy((p) => { p.freezes = 1e9; })).toBe(false);
    expect(lossy((p) => { delete p.longestStreak; })).toBe(false); // a default filled in
    expect(lossy((p) => { p.missed = ['q1', 'q1', 'q2']; })).toBe(false); // a duplicate carries no information
    expect(lossy((p) => { p.profile.restDays = [6, 0]; })).toBe(false); // reordered
    expect(lossy((p) => { p.days = { '2020-01-01': { xp: 1, answered: 1, firstTry: 1, target: 3 } }; })).toBe(false); // routine ageing
    expect(lossy((p) => { p.lastDay = null; p.profile.role = null; })).toBe(false);
  });

  it('is true when something that was stored is dropped or rewritten', () => {
    expect(lossy((p) => { p.futureThing = { a: 1 }; })).toBe(true); // unknown field
    expect(lossy((p) => { p.profile.hologram = 1; })).toBe(true); // unknown nested field
    expect(lossy((p) => { p.streak = 'x'; })).toBe(true); // a number replaced by a string
    expect(lossy((p) => { p.profile.role = 'wizard'; })).toBe(true); // a string that is not a role
    expect(lossy((p) => { p.lastDay = 'yesterday'; })).toBe(true);
    expect(lossy((p) => { p.missed = ['q1', 7, 'q2']; })).toBe(true); // an array element that is not kept
    expect(lossy((p) => { p.profile.restDays = [0, 6, 9]; })).toBe(true);
    expect(lossy((p) => { p.profile = 'x' as never; })).toBe(true); // an object replaced by a primitive
    expect(lossy((p) => { p.missed = { 0: 'q1' }; })).toBe(true); // an array replaced by an object
    expect(lossy((p) => { p.cards = [] as never; })).toBe(true); // an object replaced by an array
    expect(lossy((p) => { p.cards = { 'bad id': 5 } as never; })).toBe(true); // an invalid record
  });

  it('is not reported at all when nothing was normalised (nothing stored, a problem)', () => {
    expect(migrateProgress(null, ctx).lossy).toBeUndefined();
    expect(migrateProgress('{oops', ctx).lossy).toBeUndefined();
    expect(migrateProgress({ version: 3 }, ctx).lossy).toBeUndefined();
  });
});
