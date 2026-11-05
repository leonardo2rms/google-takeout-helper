import type { Confidence, Progress } from './progress';

/** Below this many ratings at a level the rate is not shown: it would say nothing. */
export const CALIBRATION_MIN = 10;

export const CONFIDENCE_LABELS: Record<Confidence, string> = { 1: 'Guess', 2: 'Think so', 3: 'Sure' };

export interface CalibrationRow {
  level: Confidence;
  label: string;
  n: number;
  right: number;
  /** 0..1, or null while n < CALIBRATION_MIN. */
  rate: number | null;
}

export function calibrationRows(p: Pick<Progress, 'calibration'>): CalibrationRow[] {
  return ([1, 2, 3] as const).map((level) => {
    const n = p.calibration.n[level - 1]!;
    const right = p.calibration.right[level - 1]!;
    return { level, label: CONFIDENCE_LABELS[level], n, right, rate: n >= CALIBRATION_MIN ? right / n : null };
  });
}

/**
 * One factual line about the learner's calibration, or null when there is not enough data.
 * "When you said Sure you were right 86% of the time (44 ratings)."
 */
export function calibrationSentence(p: Pick<Progress, 'calibration'>): string | null {
  const rows = calibrationRows(p);
  const sure = rows[2]!;
  if (sure.rate === null) return null;
  return `When you said Sure you were right ${Math.round(sure.rate * 100)}% of the time (${sure.n} ratings).`;
}
