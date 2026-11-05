/**
 * Milliseconds on a clock that only moves forward, for measuring how long something has taken.
 * `Date.now()` follows the wall clock, which the user, a network time sync or a timezone change can move by hours:
 * a mock-interview timer built on it ended at once when the clock jumped forward and gave free time when it went back.
 */
export const monotonicNow = (): number => performance.now();
