import { useEffect, useRef, useState } from 'react';
import {
  READINESS_TEXT, isLargeCelebration, pickCelebration, type ForecastBucket, type GoalToday, type SessionEvent, type SessionSummary, type Settings,
} from '@trainer/shared';
import { Confetti } from '../components/Confetti';
import { Icon, type IconName } from '../components/icons';
import { Inline } from '../components/Inline';
import { AnimatedRing } from '../components/Ring';
import { forecastText, listNames, plural, prefersReducedMotion } from '../lib/format';
import { endingLine, failedCount, resultHeading, type SessionKind } from '../lib/summary-copy';

export interface Finished {
  title: string;
  kind: SessionKind;
  summary: SessionSummary;
  /** XP actually credited for the session (the engine's figure includes bonuses a one-question session does not get). */
  xpEarned: number;
  /** Badges this session unlocked, shown here instead of as a floating notice. */
  newBadges: { id: string; title: string; icon: string; description: string }[];
  streak: number;
  nextLessonId?: string;
  events: SessionEvent[];
  goalBefore: GoalToday;
  goalAfter: GoalToday;
  forecast: ForecastBucket[];
  /** "Worth remembering": the explanation of something already learned, with its bonus XP. */
  tip?: { text: string; bonusXp: number } | null;
  /** Extra factual lines (e.g. the daily challenge result), shown with the other notes. */
  extraNotes?: string[];
}

/** What a headline moment says: a title and one line of meaning. Calm and factual. */
function headline(e: SessionEvent): { title: string; body: string } {
  switch (e.type) {
    case 'rank-up':
      return { title: `New rank: ${e.title}`, body: 'Your total XP passed the next step. Ranks never go down.' };
    case 'streak':
      return { title: `${e.value}-day streak`, body: `${e.value} days in a row.` };
    case 'unit-mastered':
      return { title: `${e.title}: mastered`, body: 'Your recall estimate for this whole topic passed 80%.' };
    case 'goal-met':
      return e.first
        ? { title: 'Daily goal met', body: 'The first time you have reached your daily goal.' }
        : { title: 'Daily goal met', body: 'Anything more today is a bonus.' };
    case 'perfect':
      return { title: 'Perfect lesson', body: 'Every question right on the first try.' };
    case 'readiness':
      return { title: `Readiness: ${READINESS_TEXT[e.label]}`, body: 'An estimate of how much of this course you would recall today.' };
    default:
      return { title: '', body: '' };
  }
}

interface Note {
  text: string;
  icon?: IconName;
}

/** Small factual lines for events that are not the headline. */
function note(e: SessionEvent, headlineType: string | undefined): Note | null {
  if (e.type === headlineType) return null;
  switch (e.type) {
    case 'freeze-used':
      return { icon: 'snowflake', text: `${e.count === 1 ? 'A freeze covered the day you missed' : `${e.count} freezes covered the days you missed`}. Your streak carried on.` };
    case 'freeze-earned':
      return { icon: 'snowflake', text: 'You earned a streak freeze. You can hold up to 2.' };
    case 'freeze-gift':
      return { icon: 'snowflake', text: 'A welcome gift for updating: one streak freeze. It covers a missed day automatically.' };
    case 'rank-up':
      return { text: `New rank: ${e.title}` };
    case 'unit-mastered':
      return { text: `${e.title} mastered` };
    case 'goal-met':
      return null; // the goal ring above says it
    case 'readiness':
      return { text: `Readiness: ${READINESS_TEXT[e.label]}` };
    case 'streak':
      return e.milestone ? { text: `${e.value}-day streak` } : null;
    default:
      return null;
  }
}

/** Only one big burst per couple of minutes, however fast sessions are finished. */
let lastBurst = -Infinity;

export function Summary({ finished, settings, onHome }: { finished: Finished; settings: Settings; onHome: () => void }) {
  const { summary, streak, events } = finished;
  const pct = Math.round(summary.accuracy * 100);
  const outstanding = failedCount(summary);
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
  const notes: Note[] = [...events.map((e) => note(e, celebration?.type)).filter((n): n is Note => n !== null), ...(finished.extraNotes ?? []).map((text) => ({ text }))];
  const g = finished.goalAfter;
  const heading = resultHeading(finished.kind, summary);
  const ending = endingLine(summary);
  const isLesson = finished.kind === 'lesson';
  const hasNext = Boolean(finished.nextLessonId);
  const warn = heading === 'Another pass needed';
  const goalDone = Math.min(g.done, g.target);
  const bonus = Math.max(0, g.done - g.target);

  return (
    <div className={`summary ${warn ? 'warn' : ''}`}>
      {burst && <Confetti onDone={() => setBurst(false)} />}
      <p className="label">Result · {finished.title}</p>
      <h1>{heading}</h1>
      <p className="result-big" role="img" aria-label={`${pct} percent first-try accuracy`}>{pct}<small>%</small></p>
      <p className="result-cap" aria-hidden="true">first-try accuracy</p>
      {ending && <p className="muted ending-line" data-testid="ending-line">{ending}</p>}

      {top && (
        <section className="milestone" data-testid="celebration" data-kind={celebration!.type} aria-label="Milestone">
          <b>{top.title}</b>
          <span>{top.body}</span>
        </section>
      )}

      {finished.newBadges.length > 0 && (
        <section className="milestone badges-won" data-testid="summary-badges" aria-label="New badges">
          <b>{finished.newBadges.length === 1 ? `New badge: ${finished.newBadges[0]!.title}` : `${finished.newBadges.length} new badges`}</b>
          <span>
            {finished.newBadges.length === 1 ? finished.newBadges[0]!.description : listNames(finished.newBadges.map((b) => b.title))}
            {' '}<a href="#/progress">See all badges</a>
          </span>
        </section>
      )}

      <div className="tiles">
        <div className="tile xp"><small>XP earned</small><b>+{finished.xpEarned}</b></div>
        <div className="tile acc"><small>Accuracy</small><b>{pct}%</b></div>
        <div className="tile fire"><small>Streak</small><b><Icon name="flame" size="1em" /> {plural(streak, 'day')}</b></div>
        {outstanding > 0 && <div className="tile todo"><small>To revisit</small><b>{outstanding}</b></div>}
      </div>

      <div className="summary-goal" data-testid="summary-goal">
        <AnimatedRing from={finished.goalBefore.fraction} to={g.fraction} size={56}>{goalDone}/{g.target}</AnimatedRing>
        <span>
          <b>{goalDone} of {g.target} questions today</b>
          <small className="muted">{g.met ? (bonus > 0 ? `Goal met, plus ${plural(bonus, 'bonus question')}.` : 'Goal met.') : `${plural(g.target - g.done, 'more question')} to reach today's goal.`}</small>
        </span>
      </div>

      {finished.tip && (
        <section className="tip" data-testid="tip">
          <b><Icon name="lightbulb" /> Worth remembering <span className="muted">(+{finished.tip.bonusXp} XP)</span></b>
          <Inline text={finished.tip.text} />
        </section>
      )}

      {notes.length > 0 && (
        <ul className="notes" data-testid="summary-notes">
          {notes.map((n) => <li key={n.text}>{n.icon && <Icon name={n.icon} size="1.1em" />}<span>{n.text}</span></li>)}
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
        {/* After a lesson the next one is the way forward; after review, a challenge or a mix the way forward is home. */}
        {isLesson && hasNext && <a className="btn primary" href={`#/lesson/${finished.nextLessonId}`} autoFocus>Next lesson <Icon name="arrow-right" size="1.1em" /></a>}
        <button type="button" className={`btn ${isLesson && hasNext ? 'secondary' : 'primary'}`} onClick={onHome} autoFocus={!(isLesson && hasNext)}>Back home</button>
        {!isLesson && hasNext && <a className="btn secondary" href={`#/lesson/${finished.nextLessonId}`}>Next lesson <Icon name="arrow-right" size="1.1em" /></a>}
      </div>
    </div>
  );
}
