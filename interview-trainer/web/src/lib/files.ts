/**
 * Getting data out of the page when the usual ways may be blocked (a sandboxed frame has no downloads and no
 * clipboard). Each helper reports whether it worked, so the UI can fall back to showing the text to select.
 */
import { useEffect, useState } from 'react';
import { isStandalone, withTimeout } from './sample-grader';

export type SaveResult = 'saved' | 'declined' | 'unsupported';

/** What to tell the learner when they said no to the hosted viewer's save prompt: calm, and nothing is lost. */
export const DECLINED_NOTE = 'Nothing was saved. You can try again whenever you like.';

/** The hosted viewer's `downloads` capability (see the artifact-capabilities typings): rejects with `{ code, message }`. */
interface Downloads {
  save(request: { filename: string; data: string | Blob }): Promise<{ status: string }>;
}
interface ClaudeHost {
  use(name: string): Promise<unknown>;
}

/** Extensions the hosted viewer accepts (anything else rejects with `rejected_extension`). Notably there is no `.ics`. */
export const HOSTED_FILE_EXTENSIONS: readonly string[] = [
  'gif', 'png', 'jpg', 'jpeg', 'webp', 'mp4', 'webm', 'txt', 'json', 'md',
  'docx', 'pptx', 'epub', 'csv', 'ttf', 'html', 'svg', 'pdf', 'xlsx', 'zip',
];

/** How long the host may take to hand over the capability. */
export const DOWNLOADS_LOAD_TIMEOUT_MS = 5_000;

let hosted: Promise<Downloads | null> | undefined;

/** The `downloads` capability, or null when this is not the hosted build or the viewer does not offer it. Cached. */
export function loadDownloads(): Promise<Downloads | null> {
  hosted ??= (async () => {
    const host = typeof window === 'undefined' ? undefined : (window as unknown as { claude?: ClaudeHost }).claude;
    if (!host?.use) return null;
    try {
      const found = (await withTimeout(host.use('downloads'), DOWNLOADS_LOAD_TIMEOUT_MS)) as Downloads | null | undefined;
      return found && typeof found.save === 'function' ? found : null;
    } catch {
      return null;
    }
  })();
  return hosted;
}

/** Forgets the cached capability (tests, or a host that attaches `window.claude` late). */
export function resetDownloads(): void {
  hosted = undefined;
}

function extensionOf(name: string): string {
  const dot = name.lastIndexOf('.');
  return dot < 0 ? '' : name.slice(dot + 1).toLowerCase();
}

/**
 * Whether a file can really be offered to the learner. The normal web build can always click a download link;
 * the hosted build can only if the viewer provides `downloads` (and, when `name` is given, accepts its extension).
 */
export async function canDownloadFiles(name?: string): Promise<boolean> {
  if (!isStandalone) return true;
  if (name !== undefined && !HOSTED_FILE_EXTENSIONS.includes(extensionOf(name))) return false;
  return (await loadDownloads()) !== null;
}

/** `canDownloadFiles` for rendering: false until it is known, so a button never appears and then fails. */
export function useCanDownloadFiles(name?: string): boolean {
  const [can, setCan] = useState(!isStandalone);
  useEffect(() => {
    let alive = true;
    void canDownloadFiles(name).then((ok) => {
      if (alive) setCan(ok);
    });
    return () => {
      alive = false;
    };
  }, [name]);
  return can;
}

function anchorDownload(name: string, data: string | Blob, type: string): SaveResult {
  try {
    const url = URL.createObjectURL(typeof data === 'string' ? new Blob([data], { type }) : data);
    const a = document.createElement('a');
    a.href = url;
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    return 'saved';
  } catch {
    return 'unsupported';
  }
}

/**
 * Offers `data` to the learner as a file called `name`. Normal build: a temporary download link. Hosted build: the
 * viewer's `downloads` capability, which asks the learner to confirm. Never throws.
 * 'declined' means the learner said no (nothing was saved); 'unsupported' means this page cannot save files at all,
 * so the UI should fall back to copying the text or to press-and-hold.
 */
export async function saveFile(name: string, data: string | Blob, type: string): Promise<SaveResult> {
  if (!isStandalone) return anchorDownload(name, data, type);
  const downloads = await loadDownloads();
  if (!downloads) return 'unsupported';
  try {
    await downloads.save({ filename: name, data });
    return 'saved';
  } catch (e) {
    const code = (e as { code?: unknown } | null)?.code;
    // `rate_limited` means a prompt is already open: nothing was saved and the learner can simply press again.
    return code === 'declined' || code === 'rate_limited' ? 'declined' : 'unsupported';
  }
}

/** Copies `text` to the clipboard. Returns false if the environment does not allow it. */
export async function copyText(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    return false;
  }
}
