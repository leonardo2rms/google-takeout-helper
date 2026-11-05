import { useMemo, useState } from 'react';
import {
  allQuestions, buildDailySession, goalToday, planFor, rankFor, readiness, reviewQueue, streakStatus, unitOfFor,
  type Course, type HealthResponse, type Progress,
} from '@trainer/shared';
import { ChallengeCard } from '../components/ChallengeCard';
import { Icon } from '../components/icons';
import { OnboardingCard } from '../components/OnboardingCard';
import { Ring } from '../components/Ring';
import { SettingsSheet } from '../components/SettingsSheet';
import { GoalSheet, StreakSheet, streakNote } from '../components/Sheets';
import { formatCount, plural } from '../lib/format';
import { dueInToday, dueLine, homeTier, readinessLine } from '../lib/home-copy';
import { dismissIosTip, shouldShowIosTip } from '../lib/store';
import { useToday } from '../lib/today';
import { unitStyle } from '../lib/unit-style';

interface Props {
  course: Course;
  progress: Progress;
  health: HealthResponse | null;
  update: (fn: (p: Progress) => Progress) => void;
  onReset: () => void;
}

/** Three segments from the best first-try accuracy of a lesson (90% and 70% are the steps). */
const mastery = (accuracy: number) => (accuracy >= 0.9 ? 3 : accuracy >= 0.7 ? 2 : 1);
type SheetName = 'streak' | 'goal' | 'settings' | null;

export function Home({ course, progress, health, update, onReset }: Props) {
  const today = useToday();
  const [sheet, setSheet] = useState<SheetName>(null);
  const [iosTip, setIosTip] = useState(shouldShowIosTip);

  const questions = useMemo(() => allQuestions(course), [course]);
  const unitOf = useMemo(() => unitOfFor(course), [course]);
  const queue = useMemo(() => reviewQueue(progress, questions, today, unitOf), [progress, questions, today, unitOf]);
  const daily = useMemo(() => buildDailySession(course, progress, today), [course, progress, today]);
  const ready = useMemo(() => readiness(course, progress, today), [course, progress, today]);
  const plan = useMemo(() => planFor(course, progress, today), [course, progress, today]);

  const status = streakStatus(progress, today);
  const goal = goalToday(progress, today);
  const rank = rankFor(progress.xp);
  const note = streakNote(progress, today);
  const total = course.units.reduce((n, u) => n + u.lessons.length, 0);
  const done = Object.keys(progress.lessons).length;
  const nextLesson = course.units.flatMap((u) => u.lessons).find((l) => !progress.lessons[l.id]);
  const firstLesson = course.units[0]?.lessons[0];
  const dueCount = queue.all.length;

  const hero = (() => {
    if (daily.kind === 'first') {
      const lesson = nextLesson ?? firstLesson;
      return { href: lesson ? `#/lesson/${lesson.id}` : '#/daily', title: 'Start your first lesson', sub: `${lesson?.title ?? 'A short lesson'} · about 5 minutes`, cta: 'Start' };
    }
    const parts = [
      daily.mix.due && dueInToday(daily.mix.due, dueCount),
      daily.mix.weak && plural(daily.mix.weak, 'weak spot'),
      daily.mix.fresh && `${daily.mix.fresh} new`,
      daily.mix.filler && `${daily.mix.filler} to refresh`,
    ].filter(Boolean);
    return {
      href: '#/daily',
      title: daily.kind === 'bonus' ? 'Bonus round' : 'Today',
      sub: `${plural(daily.questions.length, 'question')} · about ${daily.minutes} min${parts.length ? ` · ${parts.join(', ')}` : ''}`,
      cta: daily.kind === 'bonus' ? 'Keep going' : 'Start',
    };
  })();

  const countdown = (() => {
    if (plan.datePassed) return { chip: null, line: 'Your interview date has passed.' };
    if (plan.daysLeft === null) return null;
    const chip = plan.daysLeft === 0 ? 'Interview today' : `${plural(plan.daysLeft, 'day')} to interview`;
    let line: string;
    if (plan.mode === 'final-review') line = "Final review: Today's practice focuses on what is due and on weak spots.";
    else if (plan.remaining === 0) line = 'You have seen everything. Keep reviewing and you are in good shape.';
    else if (plan.onTrack) line = `At ${progress.goal.target}/day you'll have seen everything with ${Math.max(0, plan.spareDays ?? 0)} days to spare.`;
    else line = `To see everything by then: about ${plan.perDay}/day, or focus on weak spots.`;
    return { chip, line };
  })();

  const tier = homeTier(progress);

  // The practice modes, shown on the page or tucked under "More ways to practise" depending on how far along the learner is.
  const reviewCard = (
    <a key="review" className={`card action ${dueCount ? '' : 'disabled'}`} href={dueCount ? '#/review' : undefined} aria-disabled={!dueCount} data-testid="due-card">
      <span className="tile-i" aria-hidden="true"><Icon name="rotate-ccw" size={22} /></span>
      <span>
        <b>Review due</b>
        <small data-testid="due-count" data-count={dueCount}>{dueLine(dueCount)}</small>
      </span>
      {dueCount > 0 && <Icon name="chevron-right" className="go" />}
    </a>
  );
  const mixCard = (
    <a key="mix" className="card action" href="#/mix" data-testid="mix-card">
      <span className="tile-i" aria-hidden="true"><Icon name="shuffle" size={22} /></span>
      <span><b>Mixed practice</b><small>{done === 0 ? '8 questions from across the course' : '8 questions, weighted toward your weak spots'}</small></span>
      <Icon name="chevron-right" className="go" />
    </a>
  );
  const progressCard = (
    <a key="progress" className="card action progress-link" href="#/progress" data-testid="progress-link">
      <span className="tile-i" aria-hidden="true"><Icon name="chart-column" size={22} /></span>
      <span>
        <b>Progress</b>
        <small data-testid="readiness-line">{readinessLine(ready)}</small>
      </span>
      <Icon name="chevron-right" className="go" />
    </a>
  );
  const mockCard = (
    <a key="mock" className="card action mock-link" href="#/mock" data-testid="mock-link">
      <span className="tile-i" aria-hidden="true"><Icon name="timer" size={22} /></span>
      <span>
        <b>Mock interview</b>
        <small>{progress.mocks[0] ? `Timed, no feedback until the end · last: ${progress.mocks[0].score}%` : 'Timed, no feedback until the end · optional'}</small>
      </span>
      <Icon name="chevron-right" className="go" />
    </a>
  );
  const libraryCard = (
    <a key="library" className="card action library-link" href="#/library" data-testid="library-link">
      <span className="tile-i" aria-hidden="true"><Icon name="library" size={22} /></span>
      <span>
        <b>Library</b>
        <small>Search every question{Object.keys(progress.bookmarks).length > 0 ? ` · ${Object.keys(progress.bookmarks).length} saved` : ''}</small>
      </span>
      <Icon name="chevron-right" className="go" />
    </a>
  );
  const challengeCard = <ChallengeCard key="challenge" course={course} progress={progress} today={today} />;
  const all = [
    { id: 'challenge', node: challengeCard },
    { id: 'review', node: reviewCard },
    { id: 'mix', node: mixCard },
    { id: 'progress', node: progressCard },
    { id: 'mock', node: mockCard },
    { id: 'library', node: libraryCard },
  ];
  const onPage = tier === 0 ? [] : tier === 1 ? ['challenge', 'review', 'progress'] : all.map((c) => c.id);
  const inline = all.filter((c) => onPage.includes(c.id));
  const more = all.filter((c) => !onPage.includes(c.id));

  return (
    <div className="home">
      <header className="home-top">
        <div className="wordmark">
          <span className="mk" aria-hidden="true"><Icon name="check" size={18} strokeWidth={2.5} /></span>
          <h1>Interview Trainer</h1>
        </div>
        <button type="button" className="icon-btn" aria-label="Settings" onClick={() => setSheet('settings')}><Icon name="settings" /></button>
      </header>

      <div className="rank-block">
        <p className="rank-line" data-testid="rank">
          <b>{rank.title}</b>
          {rank.next !== null ? ` · ${formatCount(progress.xp)}/${formatCount(rank.next)} XP to ${rank.nextTitle}` : ' · top rank'}
        </p>
        <div
          className="rank-bar"
          role="progressbar"
          aria-label={`Progress to ${rank.nextTitle ?? 'top rank'}`}
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuenow={Math.round(rank.fraction * 100)}
        >
          <div style={{ width: `${Math.round(rank.fraction * 100)}%` }} />
        </div>
      </div>

      <div className="home-stats">
        <button
          type="button"
          className="stat streak-chip"
          data-label="Streak"
          data-testid="streak-chip"
          aria-label={`${status.streak} day streak${progress.freezes ? `, ${progress.freezes} freeze${progress.freezes === 1 ? '' : 's'}` : ''}. Show details`}
          onClick={() => setSheet('streak')}
        >
          <b>
            <Icon name="flame" /> {status.streak}
            {progress.freezes > 0 && <span className="freeze"><Icon name="snowflake" size="1.1em" /> {progress.freezes}</span>}
          </b>
        </button>
        <span className="stat xp-stat" data-label="XP" title="Total XP" aria-label={`${progress.xp} XP`}><b>{formatCount(progress.xp)}</b></span>
        <button
          type="button"
          className="goal-ring"
          data-testid="goal-ring"
          aria-label={`Daily goal ${goal.done}/${goal.target}: ${goal.done} of ${goal.target} questions today. Show details`}
          onClick={() => setSheet('goal')}
        >
          <Ring fraction={goal.fraction} size={40} />
          <span className="goal-text" data-testid="goal-text">{goal.done}/{goal.target}</span>
        </button>
      </div>
      {note && <p className="stat-note" data-testid="streak-note"><Icon name="snowflake" size="1.1em" />{note}</p>}
      {status.state === 'lapsed' && (
        <p className="stat-note" data-testid="streak-note">
          Fresh start. Longest: {progress.longestStreak} · {plural(progress.stats.practiceDays, 'day')} practised
        </p>
      )}

      {/* Editing the plan from Settings (a learner who has already started) puts the open form first; on first run it is one quiet line under the lesson. */}
      {!progress.profile.onboarded && tier > 0 && <OnboardingCard today={today} progress={progress} update={update} startOpen />}

      <a className={`card hero ${tier === 0 ? 'first' : ''}`} href={hero.href} data-testid="daily-card">
        <span className="tile-i solid" aria-hidden="true"><Icon name="target" size={22} /></span>
        <span className="hero-text"><b>{hero.title}</b><small>{hero.sub}</small></span>
        <span className="cta" aria-hidden="true">{hero.cta}</span>
      </a>

      {!progress.profile.onboarded && tier === 0 && <OnboardingCard today={today} progress={progress} update={update} />}

      {iosTip && (
        <p className="notice" role="note">
          <Icon name="info" />
          <span>
            On iPhone, add this app to your Home Screen (Share, then Add to Home Screen) so Safari does not clear your progress.{' '}
            <button type="button" className="link" onClick={() => { dismissIosTip(); setIosTip(false); }}>Got it</button>
          </span>
        </p>
      )}

      {countdown && (
        <div className="countdown" data-testid="countdown">
          {countdown.chip && <span className="pill on">{countdown.chip}</span>}
          <p className="muted">{countdown.line}</p>
          {plan.datePassed && <button type="button" className="link" onClick={() => setSheet('settings')}>Set a new date?</button>}
        </div>
      )}

      {(tier > 0 || more.length > 0) && (
        <section className="practice" aria-label="Practice">
          {inline.map((c) => c.node)}
        </section>
      )}

      {more.length > 0 && (
        <details className="more-practice" data-testid="more-practice">
          <summary><Icon name="chevron-down" className="chev" />More ways to practise</summary>
          <div className="practice">{more.map((c) => c.node)}</div>
        </details>
      )}

      {nextLesson && daily.kind !== 'first' && (() => {
        const ui = course.units.findIndex((u) => u.lessons.some((l) => l.id === nextLesson.id));
        const li = course.units[ui]!.lessons.findIndex((l) => l.id === nextLesson.id);
        return (
          <a className="card up-next" href={`#/lesson/${nextLesson.id}`} data-testid="up-next">
            <span className="no" aria-hidden="true">{ui + 1}.{li + 1}</span>
            <span className="hero-text"><b>{nextLesson.title}</b><small>Up next · {plural(nextLesson.questions.length, 'question')}</small></span>
            <span className="cta" aria-hidden="true">Start</span>
          </a>
        );
      })()}

      <div className="progress-line">
        <div className="row">
          <span className="label">{done} / {total} lessons</span>
          {!nextLesson && <span className="label">All lessons done</span>}
        </div>
        <div className="segs" aria-hidden="true">
          {Array.from({ length: total }, (_, i) => <i key={i} className={i < done ? 'on' : ''} />)}
        </div>
      </div>

      {course.units.map((unit, ui) => {
        const completed = unit.lessons.filter((l) => progress.lessons[l.id]).length;
        const { icon, style } = unitStyle(unit);
        return (
          <section key={unit.id} className="unit-section" style={style} aria-labelledby={`unit-${unit.id}`}>
            <div className="unit-head">
              <span className="glyph" aria-hidden="true"><Icon name={icon} size={20} /></span>
              <span className="label">Unit {String(ui + 1).padStart(2, '0')}</span>
              <span className="tally" aria-label={`${completed} of ${unit.lessons.length} lessons done`}>{completed}/{unit.lessons.length}</span>
              <h2 id={`unit-${unit.id}`}>{unit.title}</h2>
              <p>{unit.description}</p>
            </div>
            <ol className="lessons">
              {unit.lessons.map((lesson, i) => {
                const rec = progress.lessons[lesson.id];
                const isNext = lesson.id === nextLesson?.id;
                const level = rec ? mastery(rec.bestAccuracy) : 0;
                return (
                  <li key={lesson.id}>
                    <a
                      className={`lesson-row ${isNext ? 'next' : ''} ${rec ? 'done' : ''}`}
                      href={`#/lesson/${lesson.id}`}
                    >
                      <span className="no" aria-hidden="true">{ui + 1}.{i + 1}</span>
                      <span>
                        <b>{lesson.title}</b>
                        <small>
                          {lesson.questions.length} questions
                          {isNext && <span className="tag-next">Up next</span>}
                          {rec && <span className="sr-only">, completed, mastery {level} of 3</span>}
                        </small>
                      </span>
                      <span className="status" aria-hidden="true">
                        {rec && (
                          <span className="mastery">
                            {[1, 2, 3].map((n) => <i key={n} className={n <= level ? 'on' : ''} />)}
                          </span>
                        )}
                        <span className={`box ${rec ? 'done' : ''}`}>{rec && <Icon name="check" size={14} strokeWidth={2.75} />}</span>
                      </span>
                    </a>
                  </li>
                );
              })}
            </ol>
          </section>
        );
      })}

      <footer className="home-foot">
        <p className="status-line">
          <Icon name={health?.ai ? 'bot' : 'wifi-off'} />
          {health?.ai ? `AI grading on · ${health.model}` : 'AI grading off. Open answers are self-assessed.'}
        </p>
        <button type="button" className="link" onClick={() => setSheet('settings')}>Settings and backup</button>
      </footer>

      {sheet === 'streak' && <StreakSheet progress={progress} today={today} update={update} onClose={() => setSheet(null)} />}
      {sheet === 'goal' && <GoalSheet progress={progress} today={today} update={update} onClose={() => setSheet(null)} />}
      {sheet === 'settings' && (
        <SettingsSheet
          progress={progress}
          update={update}
          onReset={() => { onReset(); setSheet(null); }}
          onEditPlan={() => { update((p) => ({ ...p, profile: { ...p.profile, onboarded: false } })); setSheet(null); window.scrollTo(0, 0); }}
          onClose={() => setSheet(null)}
        />
      )}
    </div>
  );
}
