import { describe, expect, it } from 'vitest';
import { nextTrapIndex } from './focus-trap';

describe('nextTrapIndex', () => {
  it('moves forward and wraps from the last item to the first', () => {
    expect(nextTrapIndex(0, 3, false)).toBe(1);
    expect(nextTrapIndex(1, 3, false)).toBe(2);
    expect(nextTrapIndex(2, 3, false)).toBe(0);
  });
  it('moves backward and wraps from the first item to the last', () => {
    expect(nextTrapIndex(2, 3, true)).toBe(1);
    expect(nextTrapIndex(1, 3, true)).toBe(0);
    expect(nextTrapIndex(0, 3, true)).toBe(2);
  });
  it('pulls focus that escaped the trap back to an end (first forward, last backward)', () => {
    expect(nextTrapIndex(-1, 4, false)).toBe(0);
    expect(nextTrapIndex(-1, 4, true)).toBe(3);
  });
  it('a single item keeps focus; no items means nowhere to go', () => {
    expect(nextTrapIndex(0, 1, false)).toBe(0);
    expect(nextTrapIndex(0, 1, true)).toBe(0);
    expect(nextTrapIndex(-1, 0, false)).toBe(-1);
  });
});
