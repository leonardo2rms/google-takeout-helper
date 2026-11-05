import { useEffect, useMemo, useState } from 'react';
import { buildShareCard, hasShareableProgress, type Course, type Progress } from '@trainer/shared';
import { canDownloadFiles } from '../lib/reminders';
import { CARD_FILE, renderShareCard } from '../lib/share-image';
import { Sheet } from './Sheet';

interface Props {
  course: Course;
  progress: Progress;
  today: string;
}

/** Progress screen: a "Share progress" button that opens a preview of the card with whatever sharing the browser can do. */
export function ShareCardButton({ course, progress, today }: Props) {
  const [open, setOpen] = useState(false);
  if (!hasShareableProgress(progress)) return null;
  return (
    <>
      <button type="button" className="btn secondary wide share-open" onClick={() => setOpen(true)} data-testid="share-open">Share my progress</button>
      {open && <ShareSheet course={course} progress={progress} today={today} onClose={() => setOpen(false)} />}
    </>
  );
}

function ShareSheet({ course, progress, today, onClose }: Props & { onClose: () => void }) {
  const card = useMemo(() => buildShareCard(course, progress, today), [course, progress, today]);
  const [blob, setBlob] = useState<Blob | null>(null);
  const [url, setUrl] = useState<string | null>(null);
  const [failed, setFailed] = useState(false);
  const [message, setMessage] = useState('');

  useEffect(() => {
    let alive = true;
    let objectUrl: string | null = null;
    void renderShareCard(card).then((b) => {
      if (!alive) return;
      if (!b) return setFailed(true);
      objectUrl = URL.createObjectURL(b);
      setBlob(b);
      setUrl(objectUrl);
    });
    return () => {
      alive = false;
      if (objectUrl) URL.revokeObjectURL(objectUrl);
    };
  }, [card]);

  const file = useMemo(() => (blob ? new File([blob], CARD_FILE, { type: 'image/png' }) : null), [blob]);
  const canShareFile = Boolean(file && typeof navigator.canShare === 'function' && navigator.canShare({ files: [file] }) && typeof navigator.share === 'function');
  const canShareText = !canShareFile && typeof navigator.share === 'function';
  const canDownload = Boolean(blob) && canDownloadFiles();
  const canCopy = typeof navigator.clipboard?.writeText === 'function';

  const share = async () => {
    try {
      if (canShareFile && file) await navigator.share({ files: [file], text: card.text, title: 'My Interview Trainer progress' });
      else await navigator.share({ text: card.text, title: 'My Interview Trainer progress' });
    } catch (e) {
      if (e instanceof DOMException && e.name === 'AbortError') return; // the learner closed the share sheet
      setMessage('Sharing did not work here. You can copy the text below instead.');
    }
  };

  const download = () => {
    if (!blob) return;
    const a = document.createElement('a');
    const href = URL.createObjectURL(blob);
    a.href = href;
    a.download = CARD_FILE;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(href), 1000);
    setMessage('Image saved to your downloads.');
  };

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(card.text);
      setMessage('Text copied.');
    } catch {
      setMessage('Copying is not allowed here. Select the text below instead.');
    }
  };

  return (
    <Sheet title="Share my progress" onClose={onClose}>
      <p className="muted">Only what you already see on this screen: streak, rank, readiness and your strongest and weakest topic. Nothing else is included, and nothing is uploaded.</p>
      {url ? (
        <img className="share-preview" src={url} alt={card.alt} width={1080} height={1350} data-testid="share-preview" />
      ) : (
        <p className="muted" role="status">{failed ? 'The picture could not be made in this browser. The text version below still works.' : 'Preparing your card…'}</p>
      )}
      <div className="stack">
        {(canShareFile || canShareText) && (
          <button type="button" className="btn primary wide" onClick={() => void share()} data-testid="share-native">Share</button>
        )}
        {canDownload && (
          <button type="button" className="btn secondary wide" onClick={download} data-testid="share-download">Download image</button>
        )}
        {canCopy && <button type="button" className="btn ghost wide" onClick={() => void copy()} data-testid="share-copy">Copy text</button>}
      </div>
      {!canShareFile && !canShareText && !canDownload && url && (
        <p className="hint" data-testid="share-hint">This browser cannot share or save files from here: press and hold the picture to save it, or select the text below.</p>
      )}
      {message && <p role="status" className="notice" data-testid="share-message">{message}</p>}
      <label htmlFor="share-text" className="sr-only">Text version of the card</label>
      <textarea id="share-text" className="answer-box" rows={6} readOnly value={card.text} onFocus={(e) => e.currentTarget.select()} />
    </Sheet>
  );
}
