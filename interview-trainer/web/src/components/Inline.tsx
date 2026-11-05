import { Fragment } from 'react';
import { splitMarks } from '../lib/marks';

interface Props {
  text: string;
  /** Search words to highlight (the Library marks what it matched). */
  mark?: readonly string[];
}

/** Renders `backtick` spans as inline code. Everything else is plain text (no HTML injection). */
export function Inline({ text, mark }: Props) {
  const lit = (s: string) =>
    mark && mark.length > 0
      ? splitMarks(s, mark).map((piece, i) => (piece.hit ? <mark key={i}>{piece.text}</mark> : <Fragment key={i}>{piece.text}</Fragment>))
      : s;
  return (
    <>
      {text.split(/(`[^`]+`)/g).map((part, i) =>
        part.length > 2 && part.startsWith('`') && part.endsWith('`') ? (
          <code key={i} className="inline-code">{lit(part.slice(1, -1))}</code>
        ) : (
          <Fragment key={i}>{lit(part)}</Fragment>
        ),
      )}
    </>
  );
}
