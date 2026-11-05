import { useMemo } from 'react';
import { READINESS_TEXT, readiness, topicStats, type Course, type Progress } from '@trainer/shared';
import { Calibration } from '../components/Calibration';
import { Icon } from '../components/icons';
import { MockHistory } from '../components/MockHistory';
import { TrophyCase } from '../components/TrophyCase';
import { ShareCardButton } from '../components/ShareCardButton';
import { useToday } from '../lib/today';
import { unitStyle } from '../lib/unit-style';

interface Props {
  course: Course;
  progress: Progress;
}

const pct = (x: number) => Math.round(x * 100);

export function ProgressScreen({ course, progress }: Props) {
  const today = useToday();
  const ready = useMemo(() => readiness(course, progress, today), [course, progress, today]);
  const topics = useMemo(() => topicStats(course, progress, today), [course, progress, today]);
  const styleOf = (id: string) => unitStyle(course.units.find((u) => u.id === id) ?? { id });
  const weakest = topics.find((t) => t.unitId === ready.weakestUnitId);

  return (
    <div className="home progress-page">
      <header className="page-top">
        <a className="icon-btn back" href="#/" aria-label="Back to home"><Icon name="arrow-left" /></a>
        <h1>Progress</h1>
      </header>

      <section className="readiness" aria-labelledby="ready-title">
        <h2 id="ready-title" className="sr-only">Readiness</h2>
        <p className="label" aria-hidden="true">Readiness · 0 to 100</p>
        <p className="big-number" data-testid="readiness-score">{ready.score}</p>
        <p><b data-testid="readiness-label">{READINESS_TEXT[ready.label]}</b> · {pct(ready.coverage)}% of the course seen</p>
        <p className="muted">
          An estimate of how much of <b>this course</b> you would recall today. It is not a prediction of any interview result.
          Questions you have not seen yet count as zero, so it only rises as you really learn the material.
        </p>
      </section>

      <Calibration progress={progress} />

      {weakest && (
        <a className="card action" href={`#/topic/${weakest.unitId}`} data-testid="weakest-topic">
          <span className="tile-i" aria-hidden="true"><Icon name="target" size={22} /></span>
          <span><b>Your weakest topic: {weakest.title} ({pct(weakest.strength)}%)</b><small>Practise it</small></span>
          <Icon name="chevron-right" className="go" />
        </a>
      )}

      <h2>Topics</h2>
      <ul className="topics">
        {topics.map((t) => (
          <li key={t.unitId} data-testid={`topic-${t.unitId}`} style={styleOf(t.unitId).style}>
            <div className="topic-head">
              <span className="glyph" aria-hidden="true"><Icon name={styleOf(t.unitId).icon} /></span>
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
              <div style={{ width: `${pct(t.strength)}%` }} />
            </div>
            <div className="topic-foot">
              <small className="muted">{t.seen}/{t.total} seen{t.due > 0 ? ` · ${t.due} due` : ''}{t.mastered > 0 ? ` · ${t.mastered} mastered` : ''}</small>
              <a href={`#/topic/${t.unitId}`} aria-label={`Practise ${t.title}`} className="practise-link">Practise</a>
            </div>
          </li>
        ))}
      </ul>

      <MockHistory mocks={progress.mocks} />

      <TrophyCase progress={progress} />
      <ShareCardButton course={course} progress={progress} today={today} />
    </div>
  );
}
