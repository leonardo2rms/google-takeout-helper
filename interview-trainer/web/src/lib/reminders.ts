import { useEffect } from 'react';
import {
  allQuestions, dayKey, markNotified, reminderDecision, reviewQueue, unitOfFor,
  type Course, type NotifyPermission,
} from '@trainer/shared';
import { isStandalone } from './sample-grader';
import { getProgress, updateProgress } from './store';

export function notificationPermission(): NotifyPermission {
  try {
    if (typeof Notification === 'undefined') return 'unsupported';
    return Notification.permission;
  } catch {
    return 'unsupported';
  }
}

/** Must be called from a click. Resolves with the resulting permission; never throws. */
export async function requestNotificationPermission(): Promise<NotifyPermission> {
  try {
    if (typeof Notification === 'undefined') return 'unsupported';
    return await Notification.requestPermission();
  } catch {
    return notificationPermission();
  }
}

/** Shows a local notification: `new Notification` first, the service worker as the fallback (Android Chrome requires it). */
export async function showLocalNotification(title: string, body: string): Promise<boolean> {
  const options: NotificationOptions = { body, tag: 'daily-reminder', icon: '/icon.svg' };
  try {
    new Notification(title, options);
    return true;
  } catch {
    try {
      const reg = await navigator.serviceWorker?.getRegistration();
      if (!reg) return false;
      await reg.showNotification(title, options);
      return true;
    } catch {
      return false;
    }
  }
}

/** Saves text as a file. False if the host cannot download (the hosted single-file build's frame blocks it). */
export function canDownloadFiles(): boolean {
  return !isStandalone;
}

export function saveTextFile(name: string, text: string, type: string): void {
  const url = URL.createObjectURL(new Blob([text], { type }));
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/**
 * When the app is opened (or comes back to the front), show the one daily reminder if the learner asked for it,
 * the time has passed, something is due and they have not practised. The rules live in shared/reminders.ts.
 */
export function useReminderCheck(course: Course): void {
  useEffect(() => {
    const questions = allQuestions(course);
    const unitOf = unitOfFor(course);
    let running = false;

    const check = async () => {
      if (running || document.visibilityState === 'hidden') return;
      running = true;
      try {
        const now = new Date();
        const today = dayKey(now);
        const p = getProgress();
        if (!p.prefs.reminders.notify) return;
        const decision = reminderDecision({
          prefs: p.prefs.reminders,
          now,
          today,
          dueCount: reviewQueue(p, questions, today, unitOf).all.length,
          practicedToday: (p.days[today]?.answered ?? 0) > 0,
          restDays: p.profile.restDays,
          permission: notificationPermission(),
        });
        if (!decision.show) return;
        // Mark first: if showing fails we still never try twice in a day.
        updateProgress((cur) => ({ ...cur, prefs: { ...cur.prefs, reminders: markNotified(cur.prefs.reminders, today) } }));
        await showLocalNotification(decision.title, decision.body);
      } finally {
        running = false;
      }
    };

    void check();
    document.addEventListener('visibilitychange', check);
    return () => document.removeEventListener('visibilitychange', check);
  }, [course]);
}
