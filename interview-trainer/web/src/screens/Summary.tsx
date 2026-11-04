import { useEffect, useRef, useState } from 'react';
import {
  READINESS_TEXT, isLargeCelebration, pickCelebration, type ForecastBucket, type GoalToday, type SessionEvent, type SessionSummary, type Settings,
} from '@trainer/shared';
import { Confetti } from '../components/Confetti';
import { Inline } from '../components/Inline';
import { AnimatedRing } from '../components/Ring';
import { forecastText, plural, prefersReducedMotion } from '../lib/format';

export interface Finished {
  title: string;
  summary: SessionSummary;
  streak: number;
  nextLessonId?: string;
  events: SessionEvent[];
  goalBefore: GoalToday;
  goalAfter: GoalToday;
  forecast: ForecastBucket[];
  /** "Worth remembering": the explanation of something already learned, with its bonus XP. */
  tip?: { text: string; bonusXp: number } | null;
}

/** What a headline moment says: a title and one line of meaning. Calm and factual. */
function headline(e: SessionEvent): { title: string; body: string } {
  switch (e.type) {
    case 'rank-up':
      return { title: `New rank: ${e.title}`, body: 'Your total XP crossed the next step on the ladder. Ranks never go down.' };
    case 'streak':
      return { title: `${e.value}-day streak`, body: `${e.value} days in a row. That is a real habit forming.` };
    case 'unit-mastered':
      return { title: `${e.title}: mastered`, body: 'Your recall estimate for this whole topic passed 80%.' };
    case 'goal-met':
      return e.first
        ? { title: 'Daily goal met', body: 'Your first time reaching a daily goal. Small, steady days add up.' }
        : { title: 'Daily goal met', body: 'Anything more today is a bonus.' };
    case 'perfect':
      return { title: 'Perfect lesson', body: 'Every question right on the first try.' };
    case 'readiness':
      return { title: `Readiness: ${READINESS_TEXT[e.label]}`, body: 'An estimate of how much of this course you would recall today.' };
    default:
      return { title: '', body: '' };
  }
}

/** Small factual lines for events that are not the headline. */
function note(e: SessionEvent, headlineType: string | undefined): string | null {
  if (e.type === headlineType) return null;
  switch (e.type) {
    case 'freeze-used':
      return `🧊 ${e.count === 1 ? 'A freeze covered the day you missed' : `${e.count} freezes covered the days you missed`}. Your streak carried on.`;
    case 'freeze-earned':
      return '🧊 You earned a streak freeze. You can hold up to 2.';
    case 'freeze-gift':
      return '🧊 A welcome gift for updating: one streak freeze. It covers a missed day automatically.';
    case 'rank-up':
      return `New rank: ${e.title}`;
    case 'unit-mastered':
      return `${e.title} mastered`;
    case 'goal-met':
      return 'Daily goal met';
    case 'readiness':
      return `Readiness: ${READINESS_TEXT[e.label]}`;
    case 'streak':
      return e.milestone ? `${e.value}-day streak` : null;
    default:
      return null;
  }
}

/** Only one big burst per couple of minutes, however fast sessions are finished. */
let lastBurst = -Infinity;

export function Summary({ finished, settings, onHome }: { finished: Finished; settings: Settings; onHome: () => void }) {
  const { summary, streak, events } = finished;
  const pct = Math.round(summary.accuracy * 100);
  const outstanding = summary.missed.filter((id) => !summary.recovered.includes(id)).length;
  const celebration = pickCelebration(events);
  const big = isLargeCelebration(celebration);
  const [burst, setBurst] = useState(false);
  const fired = useRef(false);

  // One effect per finished session: confetti (unless reduced motion / switched off) and a short vibration.
  useEffect(() => {
    if (fired.current || !big) return;
    fired.current = true;
    const now = Date.now();
    if (now - lastBurst < 120_000) return;
    lastBurst = now;
    if (settings.confetti && !prefersReducedMotion()) setBurst(true);
    if (settings.haptics) navigator.vibrate?.(20);
  }, [big, settings.confetti, settings.haptics]);

  const top = celebration && celebration.type !== 'tip' ? headline(celebration) : null;
  const notes = events.map((e) => note(e, celebration?.type)).filter((n): n is string => n !== null);
  const g = finished.goalAfter;

  return (
    <div className="summary">
      {burst && <Confetti onDone={() => setBurst(false)} />}
      <div className="trophy" aria-hidden="true">{summary.perfect ? '🏆' : pct >= 70 ? '🎉' : '💪'}</div>
      <h1>{summary.perfect ? 'Perfect lesson!' : pct >= 70 ? 'Lesson complete!' : 'Keep going!'}</h1>
      <p className="muted">{finished.title}</p>

      {top && (
        <section className="milestone" data-testid="celebration" data-kind={celebration!.type} aria-label="Milestone">
          <b>{top.title}</b>
          <span>{top.body}</span>
        </section>
      )}

      <div className="tiles">
        <div className="tile xp"><small>Total XP</small><b>⚡ {summary.xp}</b></div>
        <div className="tile acc"><small>First-try accuracy</small><b>🎯 {pct}%</b></div>
        <div className="tile fire"><small>Day streak</small><b>🔥 {streak}</b></div>
      </div>

      <div className="summary-goal" data-testid="summary-goal">
        <AnimatedRing from={finished.goalBefore.fraction} to={g.fraction} size={56}>{g.done}/{g.target}</AnimatedRing>
        <span>
          <b>{g.done} of {g.target} questions today</b>
          <small className="muted">{g.met ? 'Daily goal met.' : `${plural(g.target - g.done, 'more question')} to reach today's goal.`}</small>
        </span>
      </div>

      {finished.tip && (
        <section className="tip" data-testid="tip">
          <b>💡 Worth remembering <span className="muted">(+{finished.tip.bonusXp} XP)</span></b>
          <Inline text={finished.tip.text} />
        </section>
      )}

      {notes.length > 0 && (
        <ul className="notes" data-testid="summary-notes">
          {notes.map((n) => <li key={n}>{n}</li>)}
        </ul>
      )}

      {finished.forecast.length > 0 && (
        <p className="muted" data-testid="next-review">Next review: {forecastText(finished.forecast)}</p>
      )}

      {outstanding > 0 && (
        <p className="muted">
          {outstanding} question{outstanding === 1 ? '' : 's'} to revisit. {outstanding === 1 ? 'It is' : 'They are'} saved in <a href="#/review">Review due</a>.
        </p>
      )}

      <div className="summary-actions">
        {finished.nextLessonId && <a className="btn secondary" href={`#/lesson/${finished.nextLessonId}`}>Next lesson</a>}
        <button type="button" className="btn primary" onClick={onHome} autoFocus>Continue</button>
      </div>
    </div>
  );
}
