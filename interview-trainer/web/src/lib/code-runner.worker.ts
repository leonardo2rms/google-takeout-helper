/// <reference lib="webworker" />
// Runs a learner's code against tests, away from the page: a worker has no DOM, no localStorage and
// no cookies, and the page terminates it if it runs too long (infinite loops).
import { runCodeTests, type CodeTestCase } from '@trainer/shared';

// Best-effort hardening: the learner's code never needs the network or storage, so remove them
// before it runs. (It is the learner's own code on their own device; this stops copy-pasted snippets
// from phoning home, it is not a defence against a determined attacker.)
const FORBIDDEN = [
  'fetch', 'XMLHttpRequest', 'WebSocket', 'EventSource', 'importScripts', 'indexedDB', 'caches',
  'BroadcastChannel', 'SharedWorker', 'Worker', 'WebTransport', 'RTCPeerConnection', 'navigator',
];
for (let obj: object | null = self; obj; obj = Object.getPrototypeOf(obj)) {
  for (const name of FORBIDDEN) {
    try {
      Object.defineProperty(obj, name, { value: undefined, configurable: false, writable: false });
    } catch {
      /* not defined on this object, or already locked */
    }
  }
}

self.onmessage = (event: MessageEvent<{ source: string; entry: string; tests: CodeTestCase[] }>) => {
  const { source, entry, tests } = event.data;
  self.postMessage(runCodeTests(source, entry, tests));
};
