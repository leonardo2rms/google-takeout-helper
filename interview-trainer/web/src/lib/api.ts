import type { GradeResponse, HealthResponse } from '@trainer/shared';

/** Set VITE_API_URL when the API is hosted on another origin; empty means same origin / dev proxy. */
const BASE = import.meta.env.VITE_API_URL ?? '';

export async function fetchHealth(): Promise<HealthResponse | null> {
  try {
    const res = await fetch(`${BASE}/api/health`, { signal: AbortSignal.timeout(2500) });
    return res.ok ? ((await res.json()) as HealthResponse) : null;
  } catch {
    return null;
  }
}

/** Waits between attempts to reach the server at start-up: it may still be booting (the first probe is immediate). */
export const HEALTH_RETRY_DELAYS_MS = [3_000, 10_000, 30_000];

/** `fetchHealth`, retried after a short wait if the server did not answer. Resolves null once the attempts are used up or `signal` aborts. */
export async function fetchHealthWithRetry(signal?: AbortSignal, delays: readonly number[] = HEALTH_RETRY_DELAYS_MS): Promise<HealthResponse | null> {
  for (let attempt = 0; ; attempt++) {
    const health = await fetchHealth();
    if (signal?.aborted) return null;
    if (health || attempt >= delays.length) return health;
    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, delays[attempt]);
      signal?.addEventListener('abort', () => (clearTimeout(timer), resolve()), { once: true });
    });
    if (signal?.aborted) return null;
  }
}

export async function gradeOpen(questionId: string, answer: string): Promise<GradeResponse> {
  const res = await fetch(`${BASE}/api/grade`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ questionId, answer }),
    signal: AbortSignal.timeout(60_000),
  });
  if (!res.ok) throw new Error(`grading failed (${res.status})`);
  return (await res.json()) as GradeResponse;
}
