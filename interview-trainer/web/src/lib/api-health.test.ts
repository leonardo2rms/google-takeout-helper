/** The health probe is retried while the server boots, so one slow start does not switch AI off for the session (QA S2-7). */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { HEALTH_RETRY_DELAYS_MS, fetchHealthWithRetry } from './api';

const ok = () => new Response(JSON.stringify({ ok: true, ai: true }), { status: 200 });

beforeEach(() => vi.useFakeTimers());
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('fetchHealthWithRetry', () => {
  it('answers at once when the server is up', async () => {
    const fetchMock = vi.fn().mockResolvedValue(ok());
    vi.stubGlobal('fetch', fetchMock);
    expect(await fetchHealthWithRetry()).toEqual({ ok: true, ai: true });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('asks again after the configured waits and stops at the first answer', async () => {
    const fetchMock = vi.fn().mockRejectedValueOnce(new TypeError('boot')).mockRejectedValueOnce(new TypeError('boot')).mockResolvedValue(ok());
    vi.stubGlobal('fetch', fetchMock);
    const result = fetchHealthWithRetry();
    await vi.advanceTimersByTimeAsync(HEALTH_RETRY_DELAYS_MS[0]!);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(HEALTH_RETRY_DELAYS_MS[1]!);
    expect(await result).toEqual({ ok: true, ai: true });
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('gives up with null after the last attempt', async () => {
    const fetchMock = vi.fn().mockRejectedValue(new TypeError('down'));
    vi.stubGlobal('fetch', fetchMock);
    const result = fetchHealthWithRetry(undefined, [10, 20]);
    await vi.advanceTimersByTimeAsync(40);
    expect(await result).toBeNull();
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  it('stops waiting and resolves null when aborted (the screen went away)', async () => {
    const fetchMock = vi.fn().mockRejectedValue(new TypeError('down'));
    vi.stubGlobal('fetch', fetchMock);
    const abort = new AbortController();
    const result = fetchHealthWithRetry(abort.signal);
    await vi.advanceTimersByTimeAsync(0);
    abort.abort();
    expect(await result).toBeNull();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('resolves null when aborted during the probe itself', async () => {
    const abort = new AbortController();
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async () => {
      abort.abort();
      return ok();
    }));
    expect(await fetchHealthWithRetry(abort.signal)).toBeNull();
  });
});
