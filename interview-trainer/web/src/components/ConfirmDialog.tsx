import { createContext, useCallback, useContext, useEffect, useId, useMemo, useRef, useState, type ReactNode } from 'react';
import { useModal } from './useModal';

export interface ConfirmOptions {
  title: string;
  /** What will happen, in a sentence or two. */
  body?: ReactNode;
  confirmLabel: string;
  /** Default "Cancel". */
  cancelLabel?: string;
  /** `danger` throws something away: the safe button is the primary one and gets the focus. */
  tone?: 'danger' | 'neutral';
  /** `data-testid` of the dialog (its buttons get `<testId>-ok` and `<testId>-cancel` unless `okTestId` names the confirm button). */
  testId?: string;
  okTestId?: string;
}

export type ConfirmFn = (options: ConfirmOptions) => Promise<boolean>;

/** Without a provider (a bare component in a unit test) nothing is confirmed: the safe answer. */
const Ctx = createContext<{ confirm: ConfirmFn; dismiss: () => void }>({ confirm: () => Promise.resolve(false), dismiss: () => {} });

/**
 * `const confirm = useConfirm(); if (await confirm({ ... })) doIt();`
 * An in-app replacement for `window.confirm`, which looks foreign in an installed app and is silently blocked (it returns
 * false) when the app runs in a sandboxed frame. Escape, the backdrop and the cancel button all answer `false`.
 */
export const useConfirm = (): ConfirmFn => useContext(Ctx).confirm;

/** Closes an open confirmation as "no". For a screen that ends on its own (a mock whose time ran out) while its question is showing. */
export const useDismissConfirm = (): (() => void) => useContext(Ctx).dismiss;

interface Open {
  options: ConfirmOptions;
  resolve: (ok: boolean) => void;
}

export function ConfirmProvider({ children }: { children: ReactNode }) {
  const [open, setOpen] = useState<Open | null>(null);
  const openRef = useRef<Open | null>(null);
  openRef.current = open;

  const confirm = useCallback<ConfirmFn>((options) => {
    // A second request while one is showing answers the first with "no" rather than stacking dialogs.
    openRef.current?.resolve(false);
    return new Promise<boolean>((resolve) => setOpen({ options, resolve }));
  }, []);

  const answer = useCallback((ok: boolean) => {
    openRef.current?.resolve(ok);
    openRef.current = null;
    setOpen(null);
  }, []);

  const dismiss = useCallback(() => {
    if (openRef.current) answer(false);
  }, [answer]);

  // Navigating away (browser Back, a link) must not leave a question hanging over the next screen.
  useEffect(() => {
    window.addEventListener('hashchange', dismiss);
    return () => window.removeEventListener('hashchange', dismiss);
  }, [dismiss]);

  const value = useMemo(() => ({ confirm, dismiss }), [confirm, dismiss]);
  return (
    <Ctx.Provider value={value}>
      {children}
      {open && <ConfirmDialog options={open.options} onAnswer={answer} />}
    </Ctx.Provider>
  );
}

function ConfirmDialog({ options, onAnswer }: { options: ConfirmOptions; onAnswer: (ok: boolean) => void }) {
  const { title, body, confirmLabel, cancelLabel = 'Cancel', tone = 'neutral', testId = 'confirm', okTestId = `${testId}-ok` } = options;
  const danger = tone === 'danger';
  const titleId = useId();
  const bodyId = useId();
  const ref = useRef<HTMLDivElement>(null);
  const okRef = useRef<HTMLButtonElement>(null);
  const cancelRef = useRef<HTMLButtonElement>(null);
  useModal(ref, () => onAnswer(false), danger ? cancelRef : okRef);

  return (
    <div className="sheet-backdrop confirm-backdrop" onClick={() => onAnswer(false)} data-testid={`${testId}-backdrop`}>
      <div
        ref={ref}
        className="confirm"
        role="alertdialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={body ? bodyId : undefined}
        tabIndex={-1}
        data-testid={testId}
        onClick={(e) => e.stopPropagation()}
      >
        <h2 id={titleId}>{title}</h2>
        {body && <div id={bodyId} className="confirm-body">{body}</div>}
        <div className="confirm-actions">
          {/* The safe choice comes first in the reading order and carries the primary look when something is thrown away. */}
          <button ref={cancelRef} type="button" className={`btn ${danger ? 'primary' : 'ghost'}`} data-testid={`${testId}-cancel`} onClick={() => onAnswer(false)}>{cancelLabel}</button>
          <button ref={okRef} type="button" className={`btn ${danger ? 'danger' : 'primary'}`} data-testid={okTestId} onClick={() => onAnswer(true)}>{confirmLabel}</button>
        </div>
      </div>
    </div>
  );
}
