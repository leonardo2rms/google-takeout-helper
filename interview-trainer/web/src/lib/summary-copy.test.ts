import { describe, expect, it } from 'vitest';
import { endingLine, failedCount, resultHeading } from './summary-copy';

const ids = (n: number, from = 0) => Array.from({ length: n }, (_, i) => `q${from + i}`);
/** `firstTry` right first time, `recovered` missed then right, `failed` never right. */
const session = (firstTry: number, recovered: number, failed: number) => ({
  perfect: recovered === 0 && failed === 0 && firstTry > 0,
  correct: [...ids(firstTry), ...ids(recovered, firstTry)],
  missed: [...ids(recovered, firstTry), ...ids(failed, firstTry + recovered)],
  recovered: ids(recovered, firstTry),
});

describe('resultHeading', () => {
  it('a perfect session is a clean run, whatever the kind', () => {
    expect(resultHeading('lesson', session(8, 0, 0))).toBe('Clean run');
    expect(resultHeading('review', session(3, 0, 0))).toBe('Clean run');
  });
  it('a lesson where every miss was recovered is complete even at 57% first-try accuracy', () => {
    expect(resultHeading('lesson', session(4, 3, 0))).toBe('Lesson complete');
  });
  it('reviews and other practice name themselves', () => {
    expect(resultHeading('review', session(6, 2, 0))).toBe('Review done');
    expect(resultHeading('daily', session(6, 2, 0))).toBe('Practice complete');
    expect(resultHeading('mix', session(6, 2, 0))).toBe('Practice complete');
  });
  it('a couple of questions still wrong out of many does not make the session a failure', () => {
    expect(resultHeading('lesson', session(6, 0, 2))).toBe('Lesson complete'); // 2 of 8 failed: under a third
  });
  it('"Another pass needed" is for sessions that ended with a third or more still wrong', () => {
    expect(resultHeading('lesson', session(4, 0, 2))).toBe('Another pass needed'); // 2 of 6
    expect(resultHeading('review', session(0, 0, 3))).toBe('Another pass needed');
  });
});

describe('endingLine', () => {
  it('says nothing for a perfect session or an empty one', () => {
    expect(endingLine(session(5, 0, 0))).toBeNull();
    expect(endingLine({ perfect: false, correct: [], missed: [], recovered: [] })).toBeNull();
  });
  it('reports that everything was answered right in the end, and how many needed another try', () => {
    expect(endingLine(session(4, 3, 0))).toBe('All 7 answered right by the end; 3 needed another try.');
  });
  it('reports what is still to revisit', () => {
    expect(endingLine(session(4, 1, 2))).toBe('5 of 7 answered right by the end; 2 still to revisit.');
  });
});

describe('failedCount', () => {
  it('counts misses that were never recovered', () => {
    expect(failedCount({ missed: ['a', 'b', 'c'], recovered: ['b'] })).toBe(2);
    expect(failedCount({ missed: [], recovered: [] })).toBe(0);
  });
});
