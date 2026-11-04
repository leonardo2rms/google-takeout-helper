import { useMemo } from 'react';
import { READINESS_TEXT, readiness, topicStats, type Course, type Progress } from '@trainer/shared';
import { useToday } from '../lib/today';

interface Props {
  course: Course;
  progress: Progress;
}

const pct = (x: number) => Math.round(x * 100);

export function ProgressScreen({ course, progress }: Props) {
  const today = useToday();
  const ready = useMemo(() => readiness(course, progress, today), [course, progress, today]);
  const topics = useMemo(() => topicStats(course, progress, today), [course, progress, today]);
  const colorOf = (id: string) => course.units.find((u) => u.id === id)?.color ?? 'var(--blue)';
  const emojiOf = (id: string) => course.units.find((u) => u.id === id)?.emoji ?? '';
  const weakest = topics.find((t) => t.unitId === ready.weakestUnitId);

  return (
    <div className="home progress-page">
      <header className="page-top">
        <a className="icon-btn back" href="#/" aria-label="Back to home">←</a>
        <h1>Progress</h1>
      </header>

      <section className="readiness" aria-labelledby="ready-title">
        <h2 id="ready-title" className="sr-only">Readiness</h2>
        <p className="big-number" data-testid="readiness-score">{ready.score}</p>
        <p><b data-testid="readiness-label">{READINESS_TEXT[ready.label]}</b> · {pct(ready.coverage)}% of the course seen</p>
        <p className="muted">
          An estimate of how much of <i>this course</i> you would recall today. It is not a prediction of any interview result.
          Questions you have not seen yet count as zero, so it only rises as you really learn the material.
        </p>
      </section>

      {weakest && (
        <a className="card action" href={`#/topic/${weakest.unitId}`} data-testid="weakest-topic">
          <span className="emoji" aria-hidden="true">🎯</span>
          <span><b>Your weakest topic: {weakest.title} ({pct(weakest.strength)}%)</b><small>Practise it</small></span>
        </a>
      )}

      <h2>Topics</h2>
      <ul className="topics">
        {topics.map((t) => (
          <li key={t.unitId} data-testid={`topic-${t.unitId}`}>
            <div className="topic-head">
              <span aria-hidden="true">{emojiOf(t.unitId)}</span>
              <b>{t.title}</b>
              <span className="muted">{pct(t.strength)}%</span>
            </div>
            <div
              className="meter"
              role="progressbar"
              aria-label={`${t.title} mastery`}
              aria-valuemin={0}
              aria-valuemax={100}
              aria-valuenow={pct(t.strength)}
            >
              <div style={{ width: `${pct(t.strength)}%`, background: colorOf(t.unitId) }} />
            </div>
            <div className="topic-foot">
              <small className="muted">{t.seen}/{t.total} seen{t.due > 0 ? ` · ${t.due} due` : ''}{t.mastered > 0 ? ` · ${t.mastered} mastered` : ''}</small>
              <a href={`#/topic/${t.unitId}`} aria-label={`Practise ${t.title}`} className="practise-link">Practise</a>
            </div>
          </li>
        ))}
      </ul>
    </div>
  );
}
