import { useMemo } from 'react';
import { READINESS_TEXT, readiness, topicStats, type Course, type Progress, type TopicStat } from '@trainer/shared';
import { Calibration } from '../components/Calibration';
import { Icon } from '../components/icons';
import { MockHistory } from '../components/MockHistory';
import { TrophyCase } from '../components/TrophyCase';
import { ShareCardButton } from '../components/ShareCardButton';
import { READINESS_NEEDS } from '../lib/home-copy';
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
  const started = topics.filter((t) => t.seen > 0);
  const idle = topics.filter((t) => t.seen === 0);
  const topicRow = (t: TopicStat) => (
    <li key={t.unitId} data-testid={`topic-${t.unitId}`} style={styleOf(t.unitId).style}>
      <div className="topic-head">
        <span className="glyph" aria-hidden="true"><Icon name={styleOf(t.unitId).icon} /></span>
        <b>{t.title}</b>
        <span className="muted">{pct(t.strength)}%</span>
      </div>
      <div
        className="meter"
        role="progressbar"
        aria-label={`${t.title} strength`}
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
  );

  return (
    <div className="home progress-page">
      <header className="page-top">
        <a className="icon-btn back" href="#/" aria-label="Back to home"><Icon name="arrow-left" /></a>
        <h1>Progress</h1>
      </header>

      <section className="readiness" aria-labelledby="ready-title">
        <h2 id="ready-title" className="sr-only">Readiness</h2>
        <p className="label" aria-hidden="true">Readiness · 0 to 100</p>
        {ready.enough ? (
          <>
            <p className="big-number" data-testid="readiness-score">{ready.score}</p>
            <p><b data-testid="readiness-label">{READINESS_TEXT[ready.label]}</b> · {pct(ready.coverage)}% of the course seen</p>
          </>
        ) : (
          <>
            <p className="big-number" data-testid="readiness-score" aria-label="No score yet"><span aria-hidden="true">–</span></p>
            <p><b data-testid="readiness-label">Not enough data yet</b> · {pct(ready.coverage)}% of the course seen</p>
            <p className="muted">A score from so few questions would say little. It appears once about {READINESS_NEEDS} of the course has been seen.</p>
          </>
        )}
        <p className="muted">
          An estimate of how much of <b>this course</b> you would recall today. It is not a prediction of any interview result.
        </p>
        <details className="how-calc">
          <summary><Icon name="chevron-down" className="chev" />How it is worked out</summary>
          <p className="muted">
            Each question you have seen counts by how long you can now leave it before it needs another look: about 5% after a first correct
            answer, rising to 100% for one that came back right after three weeks. Questions you have not seen count as zero. So the number
            rises with reviews that go well over time, not with how many lessons you finish in one day, and it falls when reviews are overdue.
          </p>
        </details>
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
      <p className="muted topics-note">Strength is how long you can now leave a topic's questions, not how many you got right: a perfect first lesson starts a topic at a few percent and it grows as reviews come back right.</p>
      <ul className="topics">{started.map(topicRow)}</ul>
      {idle.length > 0 && (
        <details className="idle-topics" data-testid="idle-topics">
          <summary><Icon name="chevron-down" className="chev" />{idle.length} {idle.length === 1 ? 'topic' : 'topics'} not started</summary>
          <ul className="topics">{idle.map(topicRow)}</ul>
        </details>
      )}

      <MockHistory mocks={progress.mocks} />

      <TrophyCase progress={progress} />
      <ShareCardButton course={course} progress={progress} today={today} />
    </div>
  );
}
