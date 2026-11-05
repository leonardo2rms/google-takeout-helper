import { useEffect, useLayoutEffect, useRef } from 'react';
import type { Prefs } from '@trainer/shared';
import { prefersReducedMotion } from './format';

/** Scrolls an element into view, smoothly unless the learner asked for reduced motion. */
export function scrollIntoViewGently(el: Element | null, block: ScrollLogicalPosition = 'nearest'): void {
  el?.scrollIntoView({ behavior: prefersReducedMotion() ? 'auto' : 'smooth', block });
}

/** Mirrors the Reading preferences onto <html> (`data-text-size`, `data-font`), which the stylesheet reacts to. */
export function useReadingPrefs(reading: Prefs['reading']): void {
  useLayoutEffect(() => {
    const root = document.documentElement;
    if (reading.textSize === 'M') root.removeAttribute('data-text-size');
    else root.setAttribute('data-text-size', reading.textSize);
    if (reading.dyslexiaFont) root.setAttribute('data-font', 'dyslexia');
    else root.removeAttribute('data-font');
  }, [reading.textSize, reading.dyslexiaFont]);
}

/**
 * Hash routes do not move focus on their own. After the first render, every route change focuses the page container
 * (`#main`), so keyboard and screen-reader users start at the top of the new screen, and sets a useful document title.
 * Screens that manage their own focus (a lesson focuses its question) pass `skipFocus`.
 */
export function useRouteAnnouncer(title: string, skipFocus: boolean): void {
  const first = useRef(true);
  useEffect(() => {
    document.title = title === 'Interview Trainer' ? title : `${title} · Interview Trainer`;
  }, [title]);
  useEffect(() => {
    if (first.current) {
      first.current = false;
      return;
    }
    if (skipFocus) return;
    const main = document.getElementById('main');
    const target = main?.querySelector<HTMLElement>('h1') ?? main;
    if (target && !target.hasAttribute('tabindex')) target.setAttribute('tabindex', '-1');
    target?.focus();
  }, [title, skipFocus]);
}
