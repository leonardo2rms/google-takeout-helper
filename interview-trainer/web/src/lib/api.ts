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
