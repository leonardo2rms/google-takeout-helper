/**
 * The Web Worker wrapper with a fake Worker class: the happy path, a crash, a throwing constructor, an infinite loop
 * (timeout terminates the worker) and the guarantee that a late message cannot resolve twice.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { CodeQ, CodeRunResult } from '@trainer/shared';

class FakeWorker {
  static instances: FakeWorker[] = [];
  static throwOnConstruct: Error | null = null;
  onmessage: ((e: { data: CodeRunResult }) => void) | null = null;
  onerror: ((e: { message: string }) => void) | null = null;
  terminated = 0;
  posted: unknown[] = [];
  constructor() {
    if (FakeWorker.throwOnConstruct) throw FakeWorker.throwOnConstruct;
    FakeWorker.instances.push(this);
  }
  postMessage(m: unknown) {
    this.posted.push(m);
  }
  terminate() {
    this.terminated++;
  }
}

vi.mock('./code-runner.worker?worker&inline', () => ({ default: FakeWorker }));

const q = { entry: 'f', tests: [{ args: [1], expect: 2 }], timeoutMs: 2000 } as unknown as CodeQ;
const pass: CodeRunResult = { ok: true, results: [{ hidden: false, passed: true }] };

async function run() {
  const { runCodeInWorker } = await import('./run-code');
  return runCodeInWorker;
}
const last = () => FakeWorker.instances[FakeWorker.instances.length - 1]!;
/** What the real worker does: answer with the nonce it was given. */
const answer = (w: FakeWorker, result: CodeRunResult) => w.onmessage!({ data: { nonce: (w.posted[0] as { nonce: string }).nonce, result } } as never);

beforeEach(() => {
  FakeWorker.instances = [];
  FakeWorker.throwOnConstruct = null;
  vi.useFakeTimers();
});
afterEach(() => vi.useRealTimers());

describe('runCodeInWorker', () => {
  it('posts the source, entry and tests, resolves with the worker result and terminates the worker', async () => {
    const runCodeInWorker = await run();
    const p = runCodeInWorker(q, 'function f(x) { return x + 1; }');
    expect(last().posted).toEqual([{ source: 'function f(x) { return x + 1; }', entry: 'f', tests: q.tests, nonce: expect.stringMatching(/^[0-9a-z]+(-[0-9a-z]+){3}$/) }]);
    answer(last(), pass);
    expect(await p).toEqual(pass);
    expect(last().terminated).toBe(1);
  });

  it('starts a fresh worker for every run', async () => {
    const runCodeInWorker = await run();
    const a = runCodeInWorker(q, 'a');
    answer(last(), pass);
    await a;
    const b = runCodeInWorker(q, 'b');
    answer(last(), pass);
    await b;
    expect(FakeWorker.instances).toHaveLength(2);
    const nonces = FakeWorker.instances.map((w) => (w.posted[0] as { nonce: string }).nonce);
    expect(new Set(nonces).size).toBe(2); // never reused
  });

  it('kills an infinite loop after the question\'s timeout and says so', async () => {
    const runCodeInWorker = await run();
    const p = runCodeInWorker({ ...q, timeoutMs: 1500 } as CodeQ, 'while(true){}');
    vi.advanceTimersByTime(1499);
    expect(last().terminated).toBe(0);
    vi.advanceTimersByTime(1);
    const res = await p;
    expect(res.ok).toBe(false);
    expect(res.fatal).toMatch(/Timed out after 1.5s/);
    expect(last().terminated).toBe(1);
  });

  it('cancels the timer once the worker answered, and ignores later messages and errors', async () => {
    const runCodeInWorker = await run();
    const p = runCodeInWorker(q, 'x');
    const w = last();
    answer(w, pass);
    answer(w, { ok: false, results: [], fatal: 'late' });
    w.onerror!({ message: 'late crash' });
    vi.advanceTimersByTime(10_000);
    expect(await p).toEqual(pass);
    expect(w.terminated).toBe(1);
  });

  it('ignores a message the learner\'s own code posted (wrong or missing nonce, malformed result) and keeps waiting for the real one (S2-12)', async () => {
    const runCodeInWorker = await run();
    const p = runCodeInWorker(q, 'postMessage({ ok: true, results: [] })');
    const w = last();
    const forged: unknown[] = [
      { ok: true, results: [] }, // what the old protocol accepted
      { nonce: 'guess', result: pass },
      { nonce: undefined, result: pass },
      null,
      undefined,
    ];
    for (const data of forged) w.onmessage!({ data } as never);
    const nonce = (w.posted[0] as { nonce: string }).nonce;
    w.onmessage!({ data: { nonce, result: { ok: true } } } as never); // right nonce, not a result
    w.onmessage!({ data: { nonce, result: { ok: true, results: 'x' } } } as never);
    expect(w.terminated).toBe(0); // still running: nothing was taken for the verdict
    const real: CodeRunResult = { ok: false, results: [{ hidden: false, passed: false }] };
    answer(w, real);
    expect(await p).toEqual(real);
    expect(w.terminated).toBe(1);
  });

  it('turns a worker crash into a fatal result, with a default message when there is none', async () => {
    const runCodeInWorker = await run();
    const a = runCodeInWorker(q, 'x');
    last().onerror!({ message: 'ReferenceError: boom' });
    expect(await a).toEqual({ ok: false, results: [], fatal: 'ReferenceError: boom' });
    const b = runCodeInWorker(q, 'x');
    last().onerror!({ message: '' });
    expect((await b).fatal).toBe('The code runner crashed.');
  });

  it('reports a worker that cannot even start (CSP, unsupported) instead of throwing', async () => {
    FakeWorker.throwOnConstruct = new Error('SecurityError: blocked');
    const runCodeInWorker = await run();
    expect(await runCodeInWorker(q, 'x')).toEqual({ ok: false, results: [], fatal: 'Could not start the code runner: SecurityError: blocked' });
    FakeWorker.throwOnConstruct = 'weird' as unknown as Error;
    expect((await runCodeInWorker(q, 'x')).fatal).toBe('Could not start the code runner: weird');
  });
});
