import { CALIBRATION_MIN, calibrationRows, calibrationSentence, type Progress } from '@trainer/shared';

/** How well "how sure are you" matches reality. Shown only once the learner has given ratings. */
export function Calibration({ progress }: { progress: Progress }) {
  const rows = calibrationRows(progress);
  const total = rows.reduce((n, r) => n + r.n, 0);
  if (total === 0) return null;
  const sentence = calibrationSentence(progress);
  return (
    <section className="calibration" aria-labelledby="calib-title" data-testid="calibration">
      <h2 id="calib-title">Confidence</h2>
      <p>{sentence ?? `Not enough ratings yet (${CALIBRATION_MIN} per level are needed before a rate means anything).`}</p>
      <ul className="calib-rows">
        {rows.map((r) => (
          <li key={r.level}>
            <b>{r.label}</b>
            <span>{r.n === 0 ? 'no ratings' : r.rate === null ? `${r.n} rating${r.n === 1 ? '' : 's'}` : `${Math.round(r.rate * 100)}% right of ${r.n}`}</span>
          </li>
        ))}
      </ul>
    </section>
  );
}
