/**
 * Small hooks (a11y, today) run against a stub of React's effect hooks and a hand-made `document`, so their logic
 * (attribute mirroring, focus management, midnight rollover) is tested without a DOM library.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

type Cleanup = void | (() => void);
const effects: (() => Cleanup)[] = [];
let refs: { current: unknown }[] = [];
let refIndex = 0;
let state: unknown[] = [];
let stateIndex = 0;

vi.mock('react', () => ({
  useEffect: (fn: () => Cleanup) => void effects.push(fn),
  useLayoutEffect: (fn: () => Cleanup) => void effects.push(fn),
  useRef: (initial: unknown) => (refs[refIndex] ??= { current: initial }, refs[refIndex++]!),
  useState: (init: unknown) => {
    const i = stateIndex++;
    if (!(i in state)) state[i] = typeof init === 'function' ? (init as () => unknown)() : init;
    return [state[i], (v: unknown) => void (state[i] = v)];
  },
}));

/** Runs a "component render": resets hook cursors, calls the hook, then flushes its effects and returns their cleanups. */
function render<T>(hook: () => T): { result: T; cleanups: (() => void)[] } {
  refIndex = 0;
  stateIndex = 0;
  effects.length = 0;
  const result = hook();
  const cleanups = effects.map((e) => e()).filter((c): c is () => void => typeof c === 'function');
  return { result, cleanups };
}

class FakeEl {
  attrs = new Map<string, string>();
  focused = 0;
  children: Record<string, FakeEl> = {};
  constructor(public tag = 'div') {}
  hasAttribute(n: string) { return this.attrs.has(n); }
  setAttribute(n: string, v: string) { this.attrs.set(n, v); }
  removeAttribute(n: string) { this.attrs.delete(n); }
  focus() { this.focused++; }
  querySelector(sel: string) { return this.children[sel] ?? null; }
}

let root: FakeEl;
let main: FakeEl | null;
let docListeners: Map<string, Set<() => void>>;
let winListeners: Map<string, Set<() => void>>;
const listenerSet = (m: Map<string, Set<() => void>>, t: string) => m.get(t) ?? m.set(t, new Set()).get(t)!;

beforeEach(() => {
  refs = [];
  state = [];
  root = new FakeEl('html');
  main = new FakeEl('main');
  docListeners = new Map();
  winListeners = new Map();
  vi.stubGlobal('document', {
    title: '',
    documentElement: root,
    getElementById: (id: string) => (id === 'main' ? main : null),
    addEventListener: (t: string, f: () => void) => listenerSet(docListeners, t).add(f),
    removeEventListener: (t: string, f: () => void) => listenerSet(docListeners, t).delete(f),
  });
  vi.stubGlobal('window', {
    matchMedia: () => ({ matches: false }),
    setInterval: (f: () => void, ms: number) => setInterval(f, ms),
    clearInterval: (id: number) => clearInterval(id),
    addEventListener: (t: string, f: () => void) => listenerSet(winListeners, t).add(f),
    removeEventListener: (t: string, f: () => void) => listenerSet(winListeners, t).delete(f),
  });
  vi.useFakeTimers();
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('useReadingPrefs', () => {
  it('mirrors text size and font to <html>, and removes the attributes for the defaults', async () => {
    const { useReadingPrefs } = await import('./a11y');
    render(() => useReadingPrefs({ textSize: 'XL', dyslexiaFont: true }));
    expect(root.attrs.get('data-text-size')).toBe('XL');
    expect(root.attrs.get('data-font')).toBe('dyslexia');
    render(() => useReadingPrefs({ textSize: 'M', dyslexiaFont: false }));
    expect(root.attrs.has('data-text-size')).toBe(false);
    expect(root.attrs.has('data-font')).toBe(false);
    render(() => useReadingPrefs({ textSize: 'S', dyslexiaFont: false }));
    expect(root.attrs.get('data-text-size')).toBe('S');
  });
});

describe('useRouteAnnouncer', () => {
  it('sets the document title (with the app name, except for the home title)', async () => {
    const { useRouteAnnouncer } = await import('./a11y');
    render(() => useRouteAnnouncer('Library', false));
    expect(document.title).toBe('Library · Interview Trainer');
    render(() => useRouteAnnouncer('Interview Trainer', false));
    expect(document.title).toBe('Interview Trainer');
  });

  it('does not steal focus on the very first render, but focuses the heading on later route changes', async () => {
    const { useRouteAnnouncer } = await import('./a11y');
    const h1 = new FakeEl('h1');
    main!.children['h1'] = h1;
    render(() => useRouteAnnouncer('Home', false));
    expect(h1.focused).toBe(0);
    render(() => useRouteAnnouncer('Library', false));
    expect(h1.focused).toBe(1);
    expect(h1.attrs.get('tabindex')).toBe('-1');
  });

  it('focuses the main landmark when there is no heading, keeps an existing tabindex and respects skipFocus', async () => {
    const { useRouteAnnouncer } = await import('./a11y');
    main!.setAttribute('tabindex', '0');
    render(() => useRouteAnnouncer('Home', false));
    render(() => useRouteAnnouncer('Progress', false));
    expect(main!.focused).toBe(1);
    expect(main!.attrs.get('tabindex')).toBe('0');
    render(() => useRouteAnnouncer('Lesson', true));
    expect(main!.focused).toBe(1);
  });

  it('does not throw when the page has no #main', async () => {
    const { useRouteAnnouncer } = await import('./a11y');
    main = null;
    render(() => useRouteAnnouncer('Home', false));
    expect(() => render(() => useRouteAnnouncer('Other', false))).not.toThrow();
  });
});

describe('scrollIntoViewGently', () => {
  it('scrolls smoothly, or instantly under reduced motion, and ignores a missing element', async () => {
    const { scrollIntoViewGently } = await import('./a11y');
    const el = { scrollIntoView: vi.fn() };
    scrollIntoViewGently(el as unknown as Element);
    expect(el.scrollIntoView).toHaveBeenLastCalledWith({ behavior: 'smooth', block: 'nearest' });
    vi.stubGlobal('window', { matchMedia: () => ({ matches: true }) });
    scrollIntoViewGently(el as unknown as Element, 'center');
    expect(el.scrollIntoView).toHaveBeenLastCalledWith({ behavior: 'auto', block: 'center' });
    expect(() => scrollIntoViewGently(null)).not.toThrow();
  });
});

describe('useToday', () => {
  it('starts on the local day, rolls over at midnight via the minute timer, and cleans up its listeners', async () => {
    vi.setSystemTime(new Date(2026, 5, 15, 23, 59, 30));
    const { useToday } = await import('./today');
    const first = render(() => useToday());
    expect(first.result).toBe('2026-06-15');
    expect(docListeners.get('visibilitychange')?.size).toBe(1);
    expect(winListeners.get('focus')?.size).toBe(1);

    vi.advanceTimersByTime(60_000); // now 00:00:30 on the 16th
    const second = render(() => useToday());
    expect(second.result).toBe('2026-06-16');

    [...first.cleanups, ...second.cleanups].forEach((c) => c());
    expect(docListeners.get('visibilitychange')?.size).toBe(0);
    expect(winListeners.get('focus')?.size).toBe(0);
  });

  it('also refreshes when the tab becomes visible or gets focus again', async () => {
    vi.setSystemTime(new Date(2026, 5, 15, 12, 0, 0));
    const { useToday } = await import('./today');
    render(() => useToday());
    vi.setSystemTime(new Date(2026, 5, 17, 8, 0, 0));
    docListeners.get('visibilitychange')!.forEach((f) => f());
    expect(render(() => useToday()).result).toBe('2026-06-17');
    vi.setSystemTime(new Date(2026, 5, 18, 8, 0, 0));
    winListeners.get('focus')!.forEach((f) => f());
    expect(render(() => useToday()).result).toBe('2026-06-18');
  });
});
