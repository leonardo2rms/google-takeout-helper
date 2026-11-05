/**
 * Getting files and text out of a page that may block downloads and the clipboard (QA S1-5): the one `saveFile`
 * helper in the normal build (download link) and in the hosted build (the viewer's `downloads` capability).
 */
import { afterEach, describe, expect, it, vi } from 'vitest';

type Cleanup = void | (() => void);
let effects: (() => Cleanup)[] = [];
let stateValue: unknown;
vi.mock('react', () => ({
  useEffect: (fn: () => Cleanup) => void effects.push(fn),
  useState: (init: unknown) => {
    stateValue = init;
    return [init, (v: unknown) => void (stateValue = v)];
  },
}));

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
  vi.resetModules();
  effects = [];
});

/** Loads a fresh copy of the module as the normal web build or as the hosted single-file build. */
async function load(hostedBuild: boolean) {
  vi.stubEnv('VITE_STANDALONE', hostedBuild ? '1' : '');
  vi.resetModules();
  return import('./files');
}

function fakeDom() {
  const anchor = { href: '', download: '', click: vi.fn(), remove: vi.fn() };
  const body = { appendChild: vi.fn() };
  vi.stubGlobal('document', { createElement: vi.fn(() => anchor), body });
  const createObjectURL = vi.fn(() => 'blob:fake');
  const revokeObjectURL = vi.fn();
  vi.stubGlobal('URL', { createObjectURL, revokeObjectURL });
  return { anchor, body, createObjectURL, revokeObjectURL };
}

const hostWith = (downloads: unknown) => {
  const use = vi.fn(async (name: string) => (name === 'downloads' ? downloads : null));
  vi.stubGlobal('window', { claude: { use } });
  return use;
};

describe('saveFile, normal web build', () => {
  it('clicks a temporary download link with the file name, then releases the blob URL', async () => {
    vi.useFakeTimers();
    const { saveFile } = await load(false);
    const dom = fakeDom();
    expect(await saveFile('backup.json', '{"a":1}', 'application/json')).toBe('saved');
    expect(dom.anchor).toMatchObject({ href: 'blob:fake', download: 'backup.json' });
    expect(dom.anchor.click).toHaveBeenCalledOnce();
    expect(dom.anchor.remove).toHaveBeenCalledOnce();
    expect(dom.revokeObjectURL).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1000);
    expect(dom.revokeObjectURL).toHaveBeenCalledWith('blob:fake');
    const blob = (dom.createObjectURL.mock.calls[0] as unknown as [Blob])[0];
    expect(blob.type).toBe('application/json');
    expect(await blob.text()).toBe('{"a":1}');
  });

  it('passes a Blob (a PNG) through unchanged and does not look for the hosted capability', async () => {
    const { saveFile } = await load(false);
    const dom = fakeDom();
    const use = hostWith({ save: vi.fn() });
    const png = new Blob([new Uint8Array([137, 80, 78, 71])], { type: 'image/png' });
    expect(await saveFile('card.png', png, 'image/png')).toBe('saved');
    expect((dom.createObjectURL.mock.calls[0] as unknown as [Blob])[0]).toBe(png);
    expect(use).not.toHaveBeenCalled();
  });

  it('reports unsupported instead of throwing when the environment blocks it', async () => {
    const { saveFile } = await load(false);
    fakeDom();
    vi.stubGlobal('URL', { createObjectURL: () => { throw new Error('blocked'); } });
    expect(await saveFile('backup.json', '{}', 'application/json')).toBe('unsupported');
  });
});

describe('saveFile, hosted build', () => {
  it('hands a text file to the downloads capability and reports saved', async () => {
    const { saveFile } = await load(true);
    const dom = fakeDom();
    const save = vi.fn().mockResolvedValue({ status: 'saved' });
    hostWith({ save });
    expect(await saveFile('backup.json', '{"a":1}', 'application/json')).toBe('saved');
    expect(save).toHaveBeenCalledWith({ filename: 'backup.json', data: '{"a":1}' });
    expect(dom.anchor.click).not.toHaveBeenCalled(); // an anchor does nothing in the sandboxed frame
  });

  it('hands a Blob over as it is (the capability reads it itself)', async () => {
    const save = vi.fn().mockResolvedValue({ status: 'saved' });
    hostWith({ save });
    const { saveFile } = await load(true);
    const png = new Blob([new Uint8Array([1, 2, 3])], { type: 'image/png' });
    expect(await saveFile('card.png', png, 'image/png')).toBe('saved');
    expect((save.mock.calls[0] as unknown as [{ data: Blob }])[0].data).toBe(png);
  });

  it('reports declined when the learner says no, and when a prompt is already open', async () => {
    const save = vi.fn().mockRejectedValueOnce({ code: 'declined', message: 'no' }).mockRejectedValueOnce({ code: 'rate_limited', message: 'wait' });
    hostWith({ save });
    const { saveFile } = await load(true);
    expect(await saveFile('a.json', '{}', 'application/json')).toBe('declined');
    expect(await saveFile('a.json', '{}', 'application/json')).toBe('declined');
  });

  it('reports unsupported for any other rejection (extension, size, odd values), never throwing', async () => {
    const save = vi.fn()
      .mockRejectedValueOnce({ code: 'rejected_extension', message: 'x' })
      .mockRejectedValueOnce(new Error('boom'))
      .mockRejectedValueOnce(null);
    hostWith({ save });
    const { saveFile } = await load(true);
    expect(await saveFile('a.ics', 'x', 'text/calendar')).toBe('unsupported');
    expect(await saveFile('a.json', 'x', 'application/json')).toBe('unsupported');
    expect(await saveFile('a.json', 'x', 'application/json')).toBe('unsupported');
  });

  it('reports unsupported when there is no window.claude, when use() gives null, throws, or never answers', async () => {
    let { saveFile } = await load(true);
    vi.stubGlobal('window', {});
    expect(await saveFile('a.json', '{}', 'application/json')).toBe('unsupported');

    hostWith(null);
    ({ saveFile } = await load(true));
    expect(await saveFile('a.json', '{}', 'application/json')).toBe('unsupported');

    hostWith({ notSave: 1 });
    ({ saveFile } = await load(true));
    expect(await saveFile('a.json', '{}', 'application/json')).toBe('unsupported');

    vi.stubGlobal('window', { claude: { use: () => Promise.reject(new Error('no')) } });
    ({ saveFile } = await load(true));
    expect(await saveFile('a.json', '{}', 'application/json')).toBe('unsupported');

    vi.useFakeTimers();
    vi.stubGlobal('window', { claude: { use: () => new Promise(() => {}) } });
    ({ saveFile } = await load(true));
    const pending = saveFile('a.json', '{}', 'application/json');
    await vi.advanceTimersByTimeAsync(6000);
    expect(await pending).toBe('unsupported');
  });

  it('is unsupported where there is no window at all', async () => {
    const { saveFile } = await load(true);
    expect(await saveFile('a.json', '{}', 'application/json')).toBe('unsupported');
  });

  it('asks the capability once, even for several saves', async () => {
    const use = hostWith({ save: vi.fn().mockResolvedValue({ status: 'saved' }) });
    const { saveFile } = await load(true);
    await saveFile('a.json', '{}', 'application/json');
    await saveFile('b.json', '{}', 'application/json');
    expect(use).toHaveBeenCalledTimes(1);
  });

  it('resetDownloads makes it look again', async () => {
    const use = hostWith(null);
    const { canDownloadFiles, resetDownloads } = await load(true);
    expect(await canDownloadFiles('a.json')).toBe(false);
    use.mockImplementation(async () => ({ save: vi.fn() }));
    expect(await canDownloadFiles('a.json')).toBe(false); // cached
    resetDownloads();
    expect(await canDownloadFiles('a.json')).toBe(true);
  });
});

describe('canDownloadFiles', () => {
  it('is always true in the normal web build', async () => {
    const { canDownloadFiles } = await load(false);
    expect(await canDownloadFiles()).toBe(true);
    expect(await canDownloadFiles('x.ics')).toBe(true);
  });

  it('in the hosted build is true only with the capability and an extension the viewer accepts', async () => {
    hostWith({ save: vi.fn() });
    const { canDownloadFiles } = await load(true);
    expect(await canDownloadFiles()).toBe(true);
    expect(await canDownloadFiles('backup.JSON')).toBe(true);
    expect(await canDownloadFiles('card.png')).toBe(true);
    expect(await canDownloadFiles('reminder.ics')).toBe(false); // .ics is not on the viewer's allowlist
    expect(await canDownloadFiles('noextension')).toBe(false);
  });

  it('in the hosted build is false without the capability', async () => {
    hostWith(null);
    const { canDownloadFiles } = await load(true);
    expect(await canDownloadFiles('backup.json')).toBe(false);
  });
});

describe('useCanDownloadFiles', () => {
  it('starts true in the normal build and false in the hosted build, then settles on the truth', async () => {
    let m = await load(false);
    expect(m.useCanDownloadFiles('a.json')).toBe(true);
    effects.forEach((e) => e());
    await Promise.resolve();
    await Promise.resolve();
    expect(stateValue).toBe(true);

    effects = [];
    hostWith({ save: vi.fn() });
    m = await load(true);
    expect(m.useCanDownloadFiles('a.json')).toBe(false);
    const cleanups = effects.map((e) => e());
    await vi.waitFor(() => expect(stateValue).toBe(true));
    cleanups.forEach((c) => typeof c === 'function' && c());
  });

  it('ignores an answer that arrives after the component went away', async () => {
    hostWith({ save: vi.fn() });
    const m = await load(true);
    m.useCanDownloadFiles('a.json');
    const cleanups = effects.map((e) => e());
    cleanups.forEach((c) => typeof c === 'function' && c());
    await m.canDownloadFiles('a.json');
    await Promise.resolve();
    expect(stateValue).toBe(false);
  });
});

describe('copyText', () => {
  it('copies and reports true', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal('navigator', { clipboard: { writeText } });
    const { copyText } = await load(false);
    expect(await copyText('hello')).toBe(true);
    expect(writeText).toHaveBeenCalledWith('hello');
  });

  it('reports false when the clipboard rejects or does not exist', async () => {
    const { copyText } = await load(false);
    vi.stubGlobal('navigator', { clipboard: { writeText: () => Promise.reject(new Error('denied')) } });
    expect(await copyText('x')).toBe(false);
    vi.stubGlobal('navigator', {});
    expect(await copyText('x')).toBe(false);
  });
});
