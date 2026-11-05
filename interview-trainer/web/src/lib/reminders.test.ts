/**
 * Notification helpers and the "remind me when I open the app" check, against a fake Notification, service worker,
 * storage and document. The decision rules themselves are tested in shared/src/reminders.test.ts; here we test that
 * the browser plumbing honours them (one a day, marked before showing, never throws).
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { KEY_V2, allQuestions, emptyProgress, type Course, type Progress } from '@trainer/shared';
import course from 'virtual:course';

type Cleanup = void | (() => void);
const effects: (() => Cleanup)[] = [];
vi.mock('react', () => ({
  useEffect: (fn: () => Cleanup) => void effects.push(fn),
  useCallback: <T,>(fn: T) => fn,
  useSyncExternalStore: (_s: unknown, get: () => unknown) => get(),
}));

class FakeNotification {
  static permission: NotificationPermission = 'granted';
  static instances: { title: string; options: NotificationOptions }[] = [];
  static throwOnConstruct = false;
  static requestPermission = vi.fn(async () => FakeNotification.permission);
  constructor(title: string, options: NotificationOptions) {
    if (FakeNotification.throwOnConstruct) throw new TypeError('Illegal constructor');
    FakeNotification.instances.push({ title, options });
  }
}

const store = new Map<string, string>();
let visibility: 'visible' | 'hidden';
const docListeners = new Set<() => void>();
let workerReg: { showNotification: ReturnType<typeof vi.fn> } | undefined;

const q1 = allQuestions(course as Course)[0]!;
const base = (over: Partial<Progress['prefs']['reminders']> = {}, p: Partial<Progress> = {}): Progress => {
  const e = emptyProgress();
  return { ...e, missed: [q1.id], ...p, prefs: { ...e.prefs, reminders: { time: '09:00', notify: true, lastNotified: null, ...over } } };
};
const seed = (p: Progress) => store.set(KEY_V2, JSON.stringify(p));
const saved = (): Progress => JSON.parse(store.get(KEY_V2)!);

async function startCheck() {
  vi.resetModules();
  const mod = await import('./reminders');
  effects.length = 0;
  mod.useReminderCheck(course as Course);
  const cleanups = effects.map((e) => e());
  await vi.advanceTimersByTimeAsync(0);
  return { mod, cleanups };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date(2026, 5, 15, 10, 30, 0)); // Monday 10:30
  store.clear();
  visibility = 'visible';
  docListeners.clear();
  workerReg = undefined;
  FakeNotification.permission = 'granted';
  FakeNotification.instances = [];
  FakeNotification.throwOnConstruct = false;
  FakeNotification.requestPermission.mockClear();
  vi.stubGlobal('Notification', FakeNotification);
  vi.stubGlobal('window', { localStorage: { getItem: (k: string) => store.get(k) ?? null, setItem: (k: string, v: string) => void store.set(k, v) }, addEventListener: () => {} });
  vi.stubGlobal('navigator', { serviceWorker: { getRegistration: async () => workerReg } });
  vi.stubGlobal('document', {
    get visibilityState() { return visibility; },
    addEventListener: (_t: string, f: () => void) => void docListeners.add(f),
    removeEventListener: (_t: string, f: () => void) => void docListeners.delete(f),
  });
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

describe('permission helpers', () => {
  it('reports the current permission, or "unsupported" when there is no Notification API', async () => {
    const { notificationPermission } = await import('./reminders');
    FakeNotification.permission = 'denied';
    expect(notificationPermission()).toBe('denied');
    vi.stubGlobal('Notification', undefined);
    expect(notificationPermission()).toBe('unsupported');
  });

  it('treats a Notification API that throws on access (locked-down webviews) as unsupported', async () => {
    const { notificationPermission } = await import('./reminders');
    vi.stubGlobal('Notification', new Proxy({}, { get() { throw new Error('denied by policy'); } }));
    expect(notificationPermission()).toBe('unsupported');
  });

  it('requests permission and never throws (a rejecting prompt falls back to the current state)', async () => {
    const { requestNotificationPermission } = await import('./reminders');
    FakeNotification.permission = 'default';
    FakeNotification.requestPermission.mockImplementationOnce(async () => 'granted');
    expect(await requestNotificationPermission()).toBe('granted');
    FakeNotification.requestPermission.mockRejectedValueOnce(new Error('blocked'));
    expect(await requestNotificationPermission()).toBe('default');
    vi.stubGlobal('Notification', undefined);
    expect(await requestNotificationPermission()).toBe('unsupported');
  });
});

describe('showLocalNotification', () => {
  it('uses the Notification constructor with a tag so repeats replace each other', async () => {
    const { showLocalNotification } = await import('./reminders');
    expect(await showLocalNotification('T', 'B')).toBe(true);
    expect(FakeNotification.instances[0]).toEqual({ title: 'T', options: { body: 'B', tag: 'daily-reminder', icon: '/icon.svg' } });
  });

  it('falls back to the service worker (Android Chrome), and gives up quietly without one', async () => {
    const { showLocalNotification } = await import('./reminders');
    FakeNotification.throwOnConstruct = true;
    workerReg = { showNotification: vi.fn().mockResolvedValue(undefined) };
    expect(await showLocalNotification('T', 'B')).toBe(true);
    expect(workerReg.showNotification).toHaveBeenCalledWith('T', expect.objectContaining({ body: 'B' }));
    workerReg = undefined;
    expect(await showLocalNotification('T', 'B')).toBe(false);
    workerReg = { showNotification: vi.fn().mockRejectedValue(new Error('no')) };
    expect(await showLocalNotification('T', 'B')).toBe(false);
    vi.stubGlobal('navigator', {});
    expect(await showLocalNotification('T', 'B')).toBe(false);
  });
});

describe('useReminderCheck', () => {
  it('shows exactly one notification when everything lines up, and records it before showing', async () => {
    seed(base());
    await startCheck();
    expect(FakeNotification.instances).toHaveLength(1);
    expect(FakeNotification.instances[0]!.title).toBe('Interview Trainer');
    expect(FakeNotification.instances[0]!.options.body).toMatch(/1 question is ready to review/);
    expect(saved().prefs.reminders.lastNotified).toBe('2026-06-15');

    // Coming back to the tab the same day does not show it again.
    docListeners.forEach((f) => f());
    await vi.advanceTimersByTimeAsync(0);
    expect(FakeNotification.instances).toHaveLength(1);
  });

  it('shows it again the next day', async () => {
    seed(base());
    const { cleanups } = await startCheck();
    expect(FakeNotification.instances).toHaveLength(1);
    vi.setSystemTime(new Date(2026, 5, 16, 10, 30, 0));
    docListeners.forEach((f) => f());
    await vi.advanceTimersByTimeAsync(0);
    expect(FakeNotification.instances).toHaveLength(2);
    expect(saved().prefs.reminders.lastNotified).toBe('2026-06-16');
    cleanups.forEach((c) => typeof c === 'function' && c());
    expect(docListeners.size).toBe(0);
  });

  it.each([
    ['reminders are off', base({ notify: false })],
    ['no time is chosen', base({ time: null })],
    ['it is too early', base({ time: '11:00' })],
    ['it was already shown today', base({ lastNotified: '2026-06-15' })],
    ['nothing is due', base({}, { missed: [] })],
    ['today is a rest day', base({}, { profile: { ...emptyProgress().profile, restDays: [1] } })],
    ['the learner already practised today', base({}, { days: { '2026-06-15': { xp: 5, answered: 3, firstTry: 3, target: 8 } } })],
  ])('stays silent when %s', async (_why, progress) => {
    seed(progress);
    await startCheck();
    expect(FakeNotification.instances).toHaveLength(0);
  });

  it('stays silent without permission and when the Notification API is missing', async () => {
    seed(base());
    FakeNotification.permission = 'denied';
    await startCheck();
    expect(FakeNotification.instances).toHaveLength(0);
    vi.stubGlobal('Notification', undefined);
    await startCheck();
    expect(saved().prefs.reminders.lastNotified).toBeNull();
  });

  it('does nothing while the page is hidden', async () => {
    seed(base());
    visibility = 'hidden';
    await startCheck();
    expect(FakeNotification.instances).toHaveLength(0);
    expect(saved().prefs.reminders.lastNotified).toBeNull();
  });

  it('marks the day even when showing fails, so a broken environment is never retried all day', async () => {
    seed(base());
    FakeNotification.throwOnConstruct = true;
    await startCheck();
    expect(saved().prefs.reminders.lastNotified).toBe('2026-06-15');
    docListeners.forEach((f) => f());
    await vi.advanceTimersByTimeAsync(0);
    expect(FakeNotification.instances).toHaveLength(0);
  });
});
