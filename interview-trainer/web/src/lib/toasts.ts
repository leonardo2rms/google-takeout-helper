import { useSyncExternalStore } from 'react';
import type { AchievementDef } from '@trainer/shared';

export interface BadgeToast {
  id: number;
  badges: Pick<AchievementDef, 'id' | 'title' | 'icon' | 'description'>[];
}

/** A tiny queue of "badge unlocked" notices, shown one at a time by <AchievementToasts />. */
let queue: BadgeToast[] = [];
let nextId = 1;
const listeners = new Set<() => void>();
const emit = () => listeners.forEach((l) => l());

export function pushBadgeToast(badges: readonly AchievementDef[]): void {
  if (badges.length === 0) return;
  queue = [...queue, { id: nextId++, badges: badges.map(({ id, title, icon, description }) => ({ id, title, icon, description })) }];
  emit();
}

export function dismissBadgeToast(id: number): void {
  queue = queue.filter((t) => t.id !== id);
  emit();
}

/** Drops every pending notice: they belong to the screen they appeared on and must not follow the learner to the next one. */
export function clearBadgeToasts(): void {
  if (queue.length === 0) return;
  queue = [];
  emit();
}

const subscribe = (l: () => void) => {
  listeners.add(l);
  return () => void listeners.delete(l);
};
const snapshot = () => queue;

/** The toast to show now (the oldest), or undefined. */
export function useBadgeToast(): BadgeToast | undefined {
  return useSyncExternalStore(subscribe, snapshot, snapshot)[0];
}
