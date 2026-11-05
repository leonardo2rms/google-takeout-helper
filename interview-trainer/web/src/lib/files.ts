/**
 * Getting text out of the page when the usual ways may be blocked (a sandboxed frame has no downloads and no
 * clipboard). Each helper reports whether it worked, so the UI can fall back to showing the text to select.
 */

/** Starts a download of `text` as a file. Returns false if the environment does not allow it. */
export function saveTextFile(name: string, text: string, type = 'application/json'): boolean {
  try {
    const url = URL.createObjectURL(new Blob([text], { type }));
    const a = document.createElement('a');
    a.href = url;
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    return true;
  } catch {
    return false;
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
