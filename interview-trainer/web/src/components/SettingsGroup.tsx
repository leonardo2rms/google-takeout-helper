import type { ReactNode } from 'react';
import { Icon } from './icons';

interface Props {
  /** Stable key for tests and links, e.g. `reading`. */
  id: string;
  title: string;
  /** The current state in a few words ("Medium text", "Off"), so the closed group already answers "what is it set to?". */
  summary: string;
  /** Open on first render (e.g. when something inside needs attention). */
  open?: boolean;
  children: ReactNode;
}

/**
 * A Settings section that folds to one line. The long, rarely-used ones (Reading, Reminders, Your data) use it, so the
 * sheet is a short list of rows that each say what they are set to, instead of one long page of controls.
 */
export function SettingsGroup({ id, title, summary, open = false, children }: Props) {
  return (
    <details className="settings-group" data-testid={`group-${id}`} open={open}>
      <summary>
        <h3 id={`${id}-h`}>{title}</h3>
        <span className="state" data-testid={`group-${id}-state`}>{summary}</span>
        <Icon name="chevron-down" className="chev" size={18} />
      </summary>
      <div className="group-body">{children}</div>
    </details>
  );
}
