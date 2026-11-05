/** Where Tab goes next inside a focus trap of `count` items. `index` is -1 when focus is outside the items; the result wraps at both ends. */
export function nextTrapIndex(index: number, count: number, shift: boolean): number {
  if (count === 0) return -1;
  if (shift) return index <= 0 ? count - 1 : index - 1;
  return index === -1 || index >= count - 1 ? 0 : index + 1;
}
