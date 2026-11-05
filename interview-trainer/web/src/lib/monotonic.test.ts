import { afterEach, describe, expect, it, vi } from 'vitest';
import { monotonicNow } from './monotonic';

afterEach(() => vi.useRealTimers());

describe('monotonicNow', () => {
  it('does not follow the wall clock when it is moved (QA S2-8)', () => {
    const before = monotonicNow();
    vi.useFakeTimers({ toFake: ['Date'] }); // fakes only Date, like a user changing the system clock
    vi.setSystemTime(new Date(2030, 0, 1));
    const jumpedForward = monotonicNow() - before;
    vi.setSystemTime(new Date(2000, 0, 1));
    const jumpedBack = monotonicNow() - before;
    expect(jumpedForward).toBeGreaterThanOrEqual(0);
    expect(jumpedForward).toBeLessThan(1000); // not 4 years
    expect(jumpedBack).toBeGreaterThanOrEqual(0);
  });

  it('never goes backwards', () => {
    let last = monotonicNow();
    for (let i = 0; i < 1000; i++) {
      const now = monotonicNow();
      expect(now).toBeGreaterThanOrEqual(last);
      last = now;
    }
  });
});
