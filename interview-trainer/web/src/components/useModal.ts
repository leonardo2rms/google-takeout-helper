import { useEffect, useRef, type RefObject } from 'react';
import { nextTrapIndex } from '../lib/focus-trap';

const FOCUSABLE = 'a[href], button:not([disabled]), input:not([disabled]), textarea:not([disabled]), select:not([disabled]), [tabindex]:not([tabindex="-1"])';

/** Open modal dialogs, oldest first. Only the last one reacts to the keyboard, so Escape closes one layer at a time. */
const stack: symbol[] = [];

/** True while any modal dialog is open. Screen-level keyboard shortcuts (Enter in a lesson) must stand down: a modal owns every key. */
export const isModalOpen = (): boolean => stack.length > 0;

/**
 * The behaviour every modal dialog needs: focus moves in on open (to `initial`, else the dialog itself), Tab and Shift+Tab stay
 * inside, Escape asks to close, and focus returns to whatever opened it. Layered dialogs (a confirmation over a sheet) work:
 * only the top one handles keys.
 */
export function useModal(ref: RefObject<HTMLElement | null>, onClose: () => void, initial?: RefObject<HTMLElement | null>): void {
  const closeRef = useRef(onClose);
  closeRef.current = onClose;
  useEffect(() => {
    const id = Symbol('modal');
    stack.push(id);
    const opener = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    (initial?.current ?? ref.current)?.focus();
    const onKey = (e: KeyboardEvent) => {
      if (stack[stack.length - 1] !== id) return;
      if (e.key === 'Escape') {
        e.preventDefault();
        closeRef.current();
        return;
      }
      if (e.key !== 'Tab' || !ref.current) return;
      const items = Array.from(ref.current.querySelectorAll<HTMLElement>(FOCUSABLE));
      if (items.length === 0) {
        e.preventDefault();
        return;
      }
      const at = items.indexOf(document.activeElement as HTMLElement);
      const insideEdge = e.shiftKey ? at <= 0 : at === items.length - 1;
      // Let the browser move between the inner items; only wrap at the edges (or when focus has escaped).
      if (insideEdge || at === -1 || !ref.current.contains(document.activeElement)) {
        e.preventDefault();
        items[nextTrapIndex(at, items.length, e.shiftKey)]?.focus();
      }
    };
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('keydown', onKey);
      const i = stack.indexOf(id);
      if (i !== -1) stack.splice(i, 1);
      opener?.focus?.();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- mount-only
  }, []);
}
