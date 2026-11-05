import { describe, expect, it } from 'vitest';
import { splitMarks } from './marks';

describe('splitMarks', () => {
  it('returns one plain piece without words or without text', () => {
    expect(splitMarks('hello world', [])).toEqual([{ text: 'hello world', hit: false }]);
    expect(splitMarks('hello world', [''])).toEqual([{ text: 'hello world', hit: false }]);
    expect(splitMarks('', ['a'])).toEqual([{ text: '', hit: false }]);
  });
  it('marks every occurrence of every word, keeping the original case', () => {
    expect(splitMarks('A suspend fun in a Suspend context', ['suspend'])).toEqual([
      { text: 'A ', hit: false },
      { text: 'suspend', hit: true },
      { text: ' fun in a ', hit: false },
      { text: 'Suspend', hit: true },
      { text: ' context', hit: false },
    ]);
    expect(splitMarks('null or Nothing', ['null', 'nothing']).filter((p) => p.hit).map((p) => p.text)).toEqual(['null', 'Nothing']);
  });
  it('treats regex characters in a search word as plain text', () => {
    expect(splitMarks('a?.b and a.c', ['?.'])).toEqual([
      { text: 'a', hit: false },
      { text: '?.', hit: true },
      { text: 'b and a.c', hit: false },
    ]);
  });
  it('a match at the very start or end leaves no empty pieces', () => {
    expect(splitMarks('abc', ['abc'])).toEqual([{ text: 'abc', hit: true }]);
  });
});
