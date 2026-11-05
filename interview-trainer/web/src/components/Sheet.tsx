import { useId, useRef, type ReactNode } from 'react';
import { useModal } from './useModal';
import { Icon } from './icons';

interface Props {
  title: string;
  onClose: () => void;
  children: ReactNode;
}

/** A bottom sheet: modal dialog, closes on Escape / backdrop tap / the close button, returns focus to what opened it. */
export function Sheet({ title, onClose, children }: Props) {
  const titleId = useId();
  const ref = useRef<HTMLDivElement>(null);
  useModal(ref, onClose);

  return (
    <div className="sheet-backdrop" onClick={onClose} data-testid="sheet-backdrop">
      <div ref={ref} className="sheet" role="dialog" aria-modal="true" aria-labelledby={titleId} tabIndex={-1} onClick={(e) => e.stopPropagation()}>
        <header className="sheet-head">
          <h2 id={titleId}>{title}</h2>
          <button type="button" className="icon-btn" aria-label="Close" onClick={onClose}><Icon name="x" /></button>
        </header>
        <div className="sheet-body">{children}</div>
      </div>
    </div>
  );
}
