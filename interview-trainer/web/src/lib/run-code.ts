import type { CodeRunResult } from '@trainer/shared';
import type { RunCodeFn } from './evaluate';
// `?worker&inline` bundles the worker into the main script, so the single-file build still works.
import RunnerWorker from './code-runner.worker?worker&inline';

/** Runs `source` against the question's tests in a fresh Web Worker, killing it after `q.timeoutMs`. */
export const runCodeInWorker: RunCodeFn = (q, source) =>
  new Promise((resolve) => {
    let worker: Worker;
    try {
      worker = new RunnerWorker();
    } catch (e) {
      resolve({ ok: false, results: [], fatal: `Could not start the code runner: ${e instanceof Error ? e.message : e}` });
      return;
    }
    let settled = false;
    const finish = (result: CodeRunResult) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      worker.terminate();
      resolve(result);
    };
    const timer = setTimeout(
      () => finish({ ok: false, results: [], fatal: `Timed out after ${q.timeoutMs / 1000}s: an infinite loop, or too slow for the hidden tests.` }),
      q.timeoutMs,
    );
    worker.onmessage = (e: MessageEvent<CodeRunResult>) => finish(e.data);
    worker.onerror = (e) => finish({ ok: false, results: [], fatal: e.message || 'The code runner crashed.' });
    worker.postMessage({ source, entry: q.entry, tests: q.tests });
  });
