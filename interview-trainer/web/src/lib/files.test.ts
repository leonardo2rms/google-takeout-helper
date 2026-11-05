/** Getting text out of a page that may block downloads and the clipboard (QA S1-5). */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { copyText, saveTextFile } from './files';

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

function fakeDom() {
  const anchor = { href: '', download: '', click: vi.fn(), remove: vi.fn() };
  const body = { appendChild: vi.fn() };
  vi.stubGlobal('document', { createElement: vi.fn(() => anchor), body });
  const createObjectURL = vi.fn(() => 'blob:fake');
  const revokeObjectURL = vi.fn();
  vi.stubGlobal('URL', { createObjectURL, revokeObjectURL });
  return { anchor, body, createObjectURL, revokeObjectURL };
}

describe('saveTextFile', () => {
  it('clicks a temporary download link with the file name, then releases the blob URL', () => {
    vi.useFakeTimers();
    const dom = fakeDom();
    expect(saveTextFile('backup.json', '{"a":1}')).toBe(true);
    expect(dom.anchor).toMatchObject({ href: 'blob:fake', download: 'backup.json' });
    expect(dom.anchor.click).toHaveBeenCalledOnce();
    expect(dom.anchor.remove).toHaveBeenCalledOnce();
    expect(dom.revokeObjectURL).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1000);
    expect(dom.revokeObjectURL).toHaveBeenCalledWith('blob:fake');
    const blob = (dom.createObjectURL.mock.calls[0] as unknown as [Blob])[0];
    expect(blob.type).toBe('application/json');
  });

  it('accepts another content type', () => {
    const dom = fakeDom();
    saveTextFile('x.txt', 'hi', 'text/plain');
    expect(((dom.createObjectURL.mock.calls[0] as unknown as [Blob])[0]).type).toBe('text/plain');
  });

  it('reports false instead of throwing when the environment blocks it', () => {
    fakeDom();
    vi.stubGlobal('URL', { createObjectURL: () => { throw new Error('blocked'); } });
    expect(saveTextFile('backup.json', '{}')).toBe(false);
  });
});

describe('copyText', () => {
  it('copies and reports true', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal('navigator', { clipboard: { writeText } });
    expect(await copyText('hello')).toBe(true);
    expect(writeText).toHaveBeenCalledWith('hello');
  });

  it('reports false when the clipboard rejects or does not exist', async () => {
    vi.stubGlobal('navigator', { clipboard: { writeText: () => Promise.reject(new Error('denied')) } });
    expect(await copyText('x')).toBe(false);
    vi.stubGlobal('navigator', {});
    expect(await copyText('x')).toBe(false);
  });
});
