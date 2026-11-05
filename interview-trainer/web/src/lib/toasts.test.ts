import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { AchievementDef } from '@trainer/shared';

let subscribe: (l: () => void) => () => void;
vi.mock('react', () => ({
  useSyncExternalStore: (sub: typeof subscribe, get: () => unknown) => {
    subscribe = sub;
    return get();
  },
}));

const badge = (id: string): AchievementDef => ({ id, title: `T-${id}`, icon: 'sprout', description: `D-${id}`, group: 'start', test: () => true });

describe('badge toast queue', () => {
  beforeEach(() => vi.resetModules());

  it('shows the oldest toast first, one per push, and drops only the dismissed one', async () => {
    const t = await import('./toasts');
    expect(t.useBadgeToast()).toBeUndefined();
    t.pushBadgeToast([badge('a'), badge('b')]);
    t.pushBadgeToast([badge('c')]);
    const first = t.useBadgeToast()!;
    expect(first.badges.map((b) => b.id)).toEqual(['a', 'b']);
    t.dismissBadgeToast(first.id + 99); // unknown id: nothing happens
    expect(t.useBadgeToast()!.id).toBe(first.id);
    t.dismissBadgeToast(first.id);
    expect(t.useBadgeToast()!.badges.map((b) => b.id)).toEqual(['c']);
  });

  it('copies only the display fields (never the test function)', async () => {
    const t = await import('./toasts');
    t.pushBadgeToast([badge('a')]);
    expect(Object.keys(t.useBadgeToast()!.badges[0]!).sort()).toEqual(['description', 'icon', 'id', 'title']);
  });

  it('ignores an empty push', async () => {
    const t = await import('./toasts');
    const listener = vi.fn();
    t.useBadgeToast();
    subscribe(listener);
    t.pushBadgeToast([]);
    expect(listener).not.toHaveBeenCalled();
    expect(t.useBadgeToast()).toBeUndefined();
  });

  it('notifies subscribers on push and dismiss, and stops after unsubscribing', async () => {
    const t = await import('./toasts');
    t.useBadgeToast();
    const listener = vi.fn();
    const off = subscribe(listener);
    t.pushBadgeToast([badge('a')]);
    t.dismissBadgeToast(t.useBadgeToast()!.id);
    expect(listener).toHaveBeenCalledTimes(2);
    off();
    t.pushBadgeToast([badge('b')]);
    expect(listener).toHaveBeenCalledTimes(2);
  });
});
