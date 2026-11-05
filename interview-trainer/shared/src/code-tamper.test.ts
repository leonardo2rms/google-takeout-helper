/**
 * Learner code runs in the same realm as the test harness (QA S2-12). A snippet that rewrites a built-in the verdict depends on
 * must not turn a wrong solution into a pass. Each snippet below is wrong on purpose and tries one trick; the built-ins it
 * touches are restored afterwards so the rest of the suite is unaffected.
 */
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { runCodeTests } from './index';

type Slot = { owner: object; key: PropertyKey; descriptor: PropertyDescriptor | undefined };
const WATCHED: Array<[object, PropertyKey]> = [
  [Array.prototype, 'every'], [Array.prototype, 'map'], [Array.prototype, 'push'], [Array.prototype, 'join'], [Array.prototype, Symbol.iterator],
  [Array, 'isArray'], [Object, 'is'], [Object, 'keys'], [JSON, 'stringify'], [String.prototype, 'slice'], [String.prototype, 'includes'],
  [Number, 'isFinite'], [Reflect, 'apply'], [globalThis, 'structuredClone'], [globalThis, 'String'],
];
let saved: Slot[] = [];
const restore = () => {
  // Indexed on purpose: a `for...of` would use the very array iterator a snippet may have replaced.
  for (let i = 0; i < saved.length; i++) {
    const { owner, key, descriptor } = saved[i]!;
    if (descriptor) Object.defineProperty(owner, key, descriptor);
  }
};
beforeEach(() => {
  saved = WATCHED.map(([owner, key]) => ({ owner, key, descriptor: Object.getOwnPropertyDescriptor(owner, key) }));
});
afterEach(restore);

/** Runs the snippet and puts every built-in back before anything is asserted (the test runner needs them too). */
function runTampered(source: string) {
  try {
    return runCodeTests(source, 'f', tests);
  } finally {
    restore();
  }
}

const wrong = (body: string) => `function f(x) { ${body} return -1; }`;
const tests = [{ args: [1], expect: 2 }, { args: [2], expect: 4, hidden: true }];

describe('a wrong solution cannot talk the harness into a pass by rewriting built-ins', () => {
  const tricks: Record<string, string> = {
    'Array.prototype.every always true': 'Array.prototype.every = () => true;',
    'Array.prototype.map returns nothing useful': 'Array.prototype.map = () => [];',
    'Array.prototype.push drops results': 'Array.prototype.push = () => 0;',
    'Array.prototype.join': 'Array.prototype.join = () => "";',
    'Array iterator ends the loop over the tests early': 'Array.prototype[Symbol.iterator] = function* () {};',
    'Array.isArray lies': 'Array.isArray = () => true;',
    'Object.is always true': 'Object.is = () => true;',
    'Object.keys returns nothing': 'Object.keys = () => [];',
    'JSON.stringify returns a constant': 'JSON.stringify = () => "2";',
    'String.prototype.slice / includes': 'String.prototype.slice = () => ""; String.prototype.includes = () => true;',
    'Number.isFinite': 'Number.isFinite = () => false;',
    'Reflect.apply': 'Reflect.apply = () => true;',
    'structuredClone returns something else': 'globalThis.structuredClone = () => [2];',
    'String is replaced': 'globalThis.String = () => "2";',
  };
  for (const [name, body] of Object.entries(tricks)) {
    it(name, () => {
      const result = runTampered(wrong(body));
      expect(result.ok, name).toBe(false);
      expect(result.results.length).toBe(2); // every test still ran
      expect(result.results.some((r) => !r.passed)).toBe(true);
    });
  }

  it('the harness itself reports correctly after learner code tampered with a built-in (a fresh run is also unaffected)', () => {
    runTampered(wrong('Object.is = () => true;'));
    expect(runCodeTests('function f(x) { return x * 2; }', 'f', tests).ok).toBe(true);
  });
});
