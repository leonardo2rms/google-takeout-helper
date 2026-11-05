/**
 * Pure, synchronous test harness for `code` questions. It knows nothing about workers or timeouts:
 * the browser runs it inside a sandboxed Web Worker and terminates the worker if it runs too long;
 * tests run it directly in Node to prove every reference solution passes.
 */

export interface CodeTestCase {
  args: unknown[] | { js: string };
  expect?: unknown;
  throws?: string;
  hidden?: boolean;
}

export interface CodeTestResult {
  hidden: boolean;
  passed: boolean;
  /** Human-readable call, e.g. `twoSum([2,7,11], 9)`. Omitted for hidden tests. */
  call?: string;
  expected?: string;
  got?: string;
  /** Why it failed when that is not obvious from expected/got (threw, wrong error, ...). */
  note?: string;
}

export interface CodeRunResult {
  /** True only if every test passed. */
  ok: boolean;
  results: CodeTestResult[];
  /** The code could not run at all (syntax error, missing function, timeout). */
  fatal?: string;
}

// The learner's code runs in the same realm as this harness, so it can reassign built-ins (`Array.prototype.every = () => true`,
// `Object.is = () => true`). Everything the verdict depends on is therefore captured when this module loads, before any
// learner code exists, and used only through those captures (QA S2-12). This stops copy-pasted tricks; it is not a vault.
const isArray = Array.isArray;
const objectIs = Object.is;
const keysOf = Object.keys;
const stringify = JSON.stringify;
const isFiniteNumber = Number.isFinite;
const toText = String;
const apply = Reflect.apply;
const strSlice = String.prototype.slice;
const strIncludes = String.prototype.includes;
const arrJoin = Array.prototype.join;
const cloneValue = structuredClone;
const FunctionCtor = Function;
const hasKey = (o: object, k: string): boolean => k in o;

const slice = (text: string, from: number, to: number): string => apply(strSlice, text, [from, to]) as string;
const includes = (text: string, part: string): boolean => apply(strIncludes, text, [part]) as boolean;
const join = (items: string[], sep: string): string => apply(arrJoin, items, [sep]) as string;

export function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b || objectIs(a, b)) return true; // also treats -0 and 0 as equal via ===
  if (typeof a !== typeof b || a === null || b === null || typeof a !== 'object') return false;
  if (isArray(a) || isArray(b)) {
    if (!isArray(a) || !isArray(b) || a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) if (!deepEqual(a[i], b[i])) return false;
    return true;
  }
  const ka = keysOf(a as object);
  const kb = keysOf(b as object);
  if (ka.length !== kb.length) return false;
  for (let i = 0; i < ka.length; i++) {
    const k = ka[i]!;
    if (!hasKey(b as object, k) || !deepEqual((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k])) return false;
  }
  return true;
}

export function show(value: unknown): string {
  if (value === undefined) return 'undefined';
  if (typeof value === 'number' && !isFiniteNumber(value)) return toText(value);
  let text: string;
  try {
    text = stringify(value) ?? toText(value);
  } catch {
    text = toText(value);
  }
  return text.length > 140 ? `${slice(text, 0, 137)}...` : text;
}

const message = (e: unknown) => (e instanceof Error ? e.message : toText(e));

export function runCodeTests(source: string, entry: string, tests: readonly CodeTestCase[]): CodeRunResult {
  let fn: unknown;
  try {
    // Strict mode keeps the learner's code from leaking globals; `new Function` gives it its own scope.
    fn = new FunctionCtor(`"use strict";\n${source}\n;return typeof ${entry} === 'function' ? ${entry} : undefined;`)();
  } catch (e) {
    return { ok: false, results: [], fatal: `${e instanceof SyntaxError ? 'Syntax error' : 'Error'}: ${message(e)}` };
  }
  if (typeof fn !== 'function') return { ok: false, results: [], fatal: `Define a function named ${entry}.` };

  const results: CodeTestResult[] = [];
  let ok = true;
  const add = (r: CodeTestResult) => {
    results[results.length] = r;
    if (!r.passed) ok = false;
  };
  for (let ti = 0; ti < tests.length; ti++) {
    const t = tests[ti]!;
    const hidden = t.hidden === true;
    let args: unknown[];
    try {
      args = isArray(t.args) ? (cloneValue(t.args) as unknown[]) : (new FunctionCtor(`"use strict"; return (${t.args.js});`)() as unknown[]);
    } catch (e) {
      add({ hidden, passed: false, note: `could not build test input: ${message(e)}` });
      continue;
    }
    const shown: string[] = [];
    if (isArray(t.args)) for (let i = 0; i < args.length; i++) shown[shown.length] = show(args[i]);
    else shown[0] = '...large input';
    const call = `${entry}(${join(shown, ', ')})`;
    const base = hidden ? { hidden } : { hidden, call, expected: t.throws !== undefined ? `throws${t.throws ? ` "${t.throws}"` : ''}` : show(t.expect) };

    try {
      const got = (fn as (...a: unknown[]) => unknown)(...args);
      if (t.throws !== undefined) add({ ...base, passed: false, ...(hidden ? {} : { got: show(got), note: 'expected an error to be thrown' }) });
      else add({ ...base, passed: deepEqual(got, t.expect), ...(hidden ? {} : { got: show(got) }) });
    } catch (e) {
      const msg = message(e);
      if (t.throws !== undefined) {
        const matches = includes(msg, t.throws);
        add({ ...base, passed: matches, ...(hidden || matches ? {} : { got: `threw "${msg}"` }) });
      } else {
        add({ ...base, passed: false, ...(hidden ? {} : { note: `threw ${msg}` }) });
      }
    }
  }
  return { ok, results };
}
