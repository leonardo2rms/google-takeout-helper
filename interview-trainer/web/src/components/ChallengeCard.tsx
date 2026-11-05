import { useMemo } from 'react';
import { CHALLENGE_XP, TYPE_LABEL, challengeQuestion, challengeResult, challengeStreak, unitOfFor, type Course, type Progress } from '@trainer/shared';
import { plural } from '../lib/format';
import { Icon } from './icons';

interface Props {
  course: Course;
  progress: Progress;
  today: string;
}

/** One featured question per local day, the same for everybody, chosen from the date alone. */
export function ChallengeCard({ course, progress, today }: Props) {
  const q = useMemo(() => challengeQuestion(course, today), [course, today]);
  const unitOf = useMemo(() => unitOfFor(course), [course]);
  if (!q) return null;
  const result = challengeResult(progress, today);
  const streak = challengeStreak(progress, today);
  const topic = course.units.find((u) => u.id === unitOf(q));
  const streakText = streak.current > 0 ? ` · ${plural(streak.current, 'day')} in a row` : '';

  if (result) {
    return (
      <div className="card challenge done" data-testid="challenge-card" data-result={result}>
        <span className="tile-i" aria-hidden="true"><Icon name={result === 'pass' ? 'circle-check' : 'calendar-check'} size={22} /></span>
        <span className="hero-text">
          <b>Daily challenge: {result === 'pass' ? 'passed' : 'done'}</b>
          <small data-testid="challenge-streak">
            {result === 'pass' ? 'Come back tomorrow for a new one' : 'Tomorrow brings a new one'}{streakText}
          </small>
          <a className="inline-action" href={`#/q/${q.id}`} data-testid="challenge-again">Practise it again <Icon name="arrow-right" size="1.1em" /></a>
        </span>
      </div>
    );
  }
  return (
    <a className="card challenge" href="#/challenge" data-testid="challenge-card" data-result="none">
      <span className="tile-i" aria-hidden="true"><Icon name="puzzle" size={22} /></span>
      <span className="hero-text">
        <b>Daily challenge</b>
        <small>
          {TYPE_LABEL[q.type]}{topic ? ` · ${topic.title}` : ''} · +{CHALLENGE_XP} XP if you get it first try{streakText}
        </small>
      </span>
      <Icon name="chevron-right" className="go" />
    </a>
  );
}
