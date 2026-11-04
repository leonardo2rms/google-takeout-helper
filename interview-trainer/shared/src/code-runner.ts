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

export function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b || Object.is(a, b)) return true; // also treats -0 and 0 as equal via ===
  if (typeof a !== typeof b || a === null || b === null || typeof a !== 'object') return false;
  if (Array.isArray(a) || Array.isArray(b)) {
    return Array.isArray(a) && Array.isArray(b) && a.length === b.length && a.every((x, i) => deepEqual(x, b[i]));
  }
  const ka = Object.keys(a as object);
  const kb = Object.keys(b as object);
  return ka.length === kb.length && ka.every((k) => k in (b as object) && deepEqual((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k]));
}

export function show(value: unknown): string {
  if (value === undefined) return 'undefined';
  if (typeof value === 'number' && !Number.isFinite(value)) return String(value);
  let text: string;
  try {
    text = JSON.stringify(value) ?? String(value);
  } catch {
    text = String(value);
  }
  return text.length > 140 ? `${text.slice(0, 137)}...` : text;
}

const message = (e: unknown) => (e instanceof Error ? e.message : String(e));

export function runCodeTests(source: string, entry: string, tests: readonly CodeTestCase[]): CodeRunResult {
  let fn: unknown;
  try {
    // Strict mode keeps the learner's code from leaking globals; `new Function` gives it its own scope.
    fn = new Function(`"use strict";\n${source}\n;return typeof ${entry} === 'function' ? ${entry} : undefined;`)();
  } catch (e) {
    return { ok: false, results: [], fatal: `${e instanceof SyntaxError ? 'Syntax error' : 'Error'}: ${message(e)}` };
  }
  if (typeof fn !== 'function') return { ok: false, results: [], fatal: `Define a function named ${entry}.` };

  const results: CodeTestResult[] = [];
  for (const t of tests) {
    const hidden = t.hidden === true;
    let args: unknown[];
    try {
      args = Array.isArray(t.args) ? structuredClone(t.args) : (new Function(`"use strict"; return (${t.args.js});`)() as unknown[]);
    } catch (e) {
      results.push({ hidden, passed: false, note: `could not build test input: ${message(e)}` });
      continue;
    }
    const call = `${entry}(${(Array.isArray(t.args) ? args.map(show) : ['...large input']).join(', ')})`;
    const base = hidden ? { hidden } : { hidden, call, expected: t.throws !== undefined ? `throws${t.throws ? ` "${t.throws}"` : ''}` : show(t.expect) };

    try {
      const got = (fn as (...a: unknown[]) => unknown)(...args);
      if (t.throws !== undefined) results.push({ ...base, passed: false, ...(hidden ? {} : { got: show(got), note: 'expected an error to be thrown' }) });
      else results.push({ ...base, passed: deepEqual(got, t.expect), ...(hidden ? {} : { got: show(got) }) });
    } catch (e) {
      const msg = message(e);
      if (t.throws !== undefined) {
        const matches = msg.includes(t.throws);
        results.push({ ...base, passed: matches, ...(hidden || matches ? {} : { got: `threw "${msg}"` }) });
      } else {
        results.push({ ...base, passed: false, ...(hidden ? {} : { note: `threw ${msg}` }) });
      }
    }
  }
  return { ok: results.every((r) => r.passed), results };
}
