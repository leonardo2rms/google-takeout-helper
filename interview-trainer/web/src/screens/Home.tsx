import { Fragment, useMemo, useState } from 'react';
import {
  buildDailySession, daysSinceBackup, dueOverview, goalDisplay, goalToday, hasTrack, isFocusUnit, nextLessonFor, planFor, rankFor, readiness, resumeProgress, ROLE_LABEL, shouldNudgeBackup, streakStatus, trackOrder,
  type Course, type HealthResponse, type Progress,
} from '@trainer/shared';
import { ChallengeCard } from '../components/ChallengeCard';
import { useConfirm } from '../components/ConfirmDialog';
import { Icon } from '../components/icons';
import { OnboardingCard } from '../components/OnboardingCard';
import { Ring } from '../components/Ring';
import { SettingsSheet } from '../components/SettingsSheet';
import { GoalSheet, StreakSheet, streakNote } from '../components/Sheets';
import { formatCount, plural } from '../lib/format';
import { currentUnitId, dueInToday, dueLine, goalMetHero, homeTier, readinessLine, reinforceText } from '../lib/home-copy';
import { downloadBackup, snoozeBackupNudge, useBackupNote } from '../lib/backup-note';
import { DECLINED_NOTE } from '../lib/files';
import { clearResume, useResume } from '../lib/resume';
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
  const resume = useResume();
  const backupNote = useBackupNote();
  const [backupMsg, setBackupMsg] = useState('');
  const confirm = useConfirm();
  // Only the unit being worked in is open; finished and future units are one line each (every lesson stays one tap away).
  const [openUnits, setOpenUnits] = useState<ReadonlySet<string>>(() => {
    const current = currentUnitId(trackOrder(course.units, progress.profile.role), progress.lessons);
    return new Set(current ? [current] : []);
  });
  const setUnitOpen = (id: string, open: boolean) =>
    setOpenUnits((s) => (s.has(id) === open ? s : new Set(open ? [...s, id] : [...s].filter((x) => x !== id))));
  const allUnitsOpen = openUnits.size === course.units.length;

  const overview = useMemo(() => dueOverview(course, progress, today), [course, progress, today]);
  const daily = useMemo(() => buildDailySession(course, progress, today), [course, progress, today]);
  const ready = useMemo(() => readiness(course, progress, today), [course, progress, today]);
  const plan = useMemo(() => planFor(course, progress, today), [course, progress, today]);

  const status = streakStatus(progress, today);
  const goal = goalToday(progress, today);
  const rank = rankFor(progress.xp);
  const note = streakNote(progress, today);
  const total = course.units.reduce((n, u) => n + u.lessons.length, 0);
  const done = Object.keys(progress.lessons).length;
  const role = progress.profile.role;
  const units = useMemo(() => trackOrder(course.units, role), [course, role]);
  const nextLesson = nextLessonFor(course, role, progress.lessons);
  const firstLesson = units[0]?.lessons[0];
  const dueCount = overview.due;
  const reviewable = overview.reviewable;
  const shownGoal = goalDisplay(goal);

  const hero = (() => {
    if (daily.kind === 'first') {
      const lesson = nextLesson ?? firstLesson;
      return { href: lesson ? `#/lesson/${lesson.id}` : '#/daily', title: 'Start your first lesson', sub: `${lesson?.title ?? 'A short lesson'} · about 5 minutes`, cta: 'Start', done: false as const };
    }
    // Today's goal is met: the "Today" session is done, so Home stops selling it and offers one optional next step.
    if (goal.met) {
      return { ...goalMetHero({ done: goal.done, target: goal.target, due: dueCount, nextLesson }), done: true as const };
    }
    const parts = [
      daily.mix.due && dueInToday(daily.mix.due, dueCount),
      daily.mix.weak && reinforceText(daily.mix.weak),
      daily.mix.fresh && `${daily.mix.fresh} new`,
      daily.mix.filler && `${daily.mix.filler} to refresh`,
    ].filter(Boolean);
    return {
      href: '#/daily',
      title: daily.kind === 'bonus' ? 'Bonus round' : 'Today',
      sub: `${plural(daily.questions.length, 'question')} · about ${daily.minutes} min${parts.length ? ` · ${parts.join(', ')}` : ''}`,
      cta: daily.kind === 'bonus' ? 'Keep going' : 'Start',
      done: false as const,
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
    <a key="review" className={`card action ${reviewable ? '' : 'disabled'}`} href={reviewable ? '#/review' : undefined} aria-disabled={!reviewable} data-testid="due-card">
      <span className="tile-i" aria-hidden="true"><Icon name="rotate-ccw" size={22} /></span>
      <span>
        <b>Review due</b>
        <small data-testid="due-count" data-count={reviewable}>{dueLine(overview.due, overview.retry)}</small>
      </span>
      {reviewable > 0 && <Icon name="chevron-right" className="go" />}
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
  // The three modes with nothing live to report share one compact row, so Today stays the one primary action.
  const saved = Object.keys(progress.bookmarks).length;
  const tiles = (
    <div className="mode-tiles" role="group" aria-label="More ways to practise" key="tiles">
      <a className="mode-tile" href="#/mix" data-testid="mix-card" aria-label={`Mixed practice: ${done === 0 ? '8 questions from across the course' : '8 questions, weighted toward what you know least'}`}>
        <Icon name="shuffle" size={22} />
        <b>Mixed</b>
        <small>8 questions</small>
      </a>
      <a className="mode-tile" href="#/mock" data-testid="mock-link" aria-label={`Mock interview: timed, no feedback until the end${progress.mocks[0] ? `, last score ${progress.mocks[0].score}%` : ', optional'}`}>
        <Icon name="timer" size={22} />
        <b>Mock</b>
        <small>{progress.mocks[0] ? `last ${progress.mocks[0].score}%` : 'timed · optional'}</small>
      </a>
      <a className="mode-tile" href="#/library" data-testid="library-link" aria-label={`Library: search every question${saved > 0 ? `, ${saved} saved` : ''}`}>
        <Icon name="library" size={22} />
        <b>Library</b>
        <small>{saved > 0 ? `${saved} saved` : 'search all'}</small>
      </a>
    </div>
  );
  const challengeCard = <ChallengeCard key="challenge" course={course} progress={progress} today={today} />;
  // Rows with something live to show (today's question, what is due, readiness), then the compact row. A newcomer sees none of
  // it on the page; after the first lessons the rows appear, after three the compact row too. What is not on the page is
  // one tap away under "More ways to practise".
  const rows = [challengeCard, reviewCard, progressCard];

  // "Up next" sits right under the hero once a learner has started (not on day 0, where the hero already is the first lesson), and is
  // left out when the hero already offers that very lesson.
  const heroIsNextLesson = hero.done && hero.href === `#/lesson/${nextLesson?.id}`;
  const upNext = nextLesson && tier > 0 && daily.kind !== 'first' && !heroIsNextLesson ? (() => {
    const ui = course.units.findIndex((u) => u.lessons.some((l) => l.id === nextLesson.id));
    const li = course.units[ui]!.lessons.findIndex((l) => l.id === nextLesson.id);
    return (
      <a className="card up-next" href={`#/lesson/${nextLesson.id}`} data-testid="up-next">
        <span className="no" aria-hidden="true">{ui + 1}.{li + 1}</span>
        <span className="hero-text"><b>{nextLesson.title}</b><small>Up next · {plural(nextLesson.questions.length, 'question')}</small></span>
        <span className="cta" aria-hidden="true">Start</span>
      </a>
    );
  })() : null;

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
          aria-label={`Daily goal ${shownGoal.ratio}: ${shownGoal.sentence}${shownGoal.bonus > 0 ? `, plus ${plural(shownGoal.bonus, 'bonus question')}` : ''}. Show details`}
          onClick={() => setSheet('goal')}
        >
          <Ring fraction={goal.fraction} size={40} />
          <span className="goal-text" data-testid="goal-text">{shownGoal.ratio}</span>
          {shownGoal.bonus > 0 && <span className="goal-bonus" data-testid="goal-bonus">+{shownGoal.bonus}</span>}
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

      {resume && (() => {
        const { done: answered, total: of } = resumeProgress(resume.record);
        return (
          <section className="card resume" aria-label="Unfinished session" data-testid="resume-card">
            <a className="resume-link" href={`#${resume.record.route}`} data-testid="resume-link">
              <span className="tile-i solid" aria-hidden="true"><Icon name="arrow-right" size={22} /></span>
              <span className="hero-text"><b>{resume.record.title}</b><small>Carry on · {answered} of {of} done</small></span>
              <span className="cta" aria-hidden="true">Resume</span>
            </a>
            <button
              type="button"
              className="link"
              data-testid="resume-discard"
              onClick={async () => {
                const ok = await confirm({
                  title: 'Discard this session?',
                  body: `Nothing from "${resume.record.title}" has been counted yet. Discarding drops the ${plural(answered, 'question')} you have done, and opening it again starts from the first question.`,
                  confirmLabel: 'Discard',
                  cancelLabel: 'Keep it',
                  tone: 'danger',
                  testId: 'resume-discard-confirm',
                });
                if (ok) clearResume(resume.record.route);
              }}
            >
              Discard
            </button>
          </section>
        );
      })()}

      {/* When the main card would open the very session that can be resumed (the same lesson, or Today), the Resume card above is the better one. */}
      {!(resume && hero.href === `#${resume.record.route}`) && (
        <a className={`card hero ${tier === 0 ? 'first' : ''} ${hero.done ? 'done' : ''}`} href={hero.href} data-testid="daily-card" data-state={hero.done ? 'done' : 'open'}>
          <span className={`tile-i ${hero.done ? '' : 'solid'}`} aria-hidden="true"><Icon name={hero.done ? 'circle-check' : 'target'} size={22} /></span>
          <span className="hero-text"><b>{hero.title}</b><small>{hero.sub}</small></span>
          <span className={`cta ${hero.done ? 'quiet' : ''}`} aria-hidden="true">{hero.cta}</span>
        </a>
      )}

      {upNext}

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

      {tier > 0 && (
        <section className="practice" aria-label="Practice">
          {rows}
          {tier >= 2 && tiles}
        </section>
      )}

      {tier < 2 && (
        <details className="more-practice" data-testid="more-practice">
          <summary><Icon name="chevron-down" className="chev" />More ways to practise</summary>
          <div className="practice">{tier === 0 && rows}{tiles}</div>
        </details>
      )}

      {shouldNudgeBackup(progress, backupNote, today) && (() => {
        const since = daysSinceBackup(backupNote, today);
        return (
          <section className="notice backup-nudge" aria-label="Back up your progress" data-testid="backup-nudge">
            <Icon name="info" />
            <div>
              <p>
                <b>{since === null ? 'Your progress is stored only on this device.' : `Your last backup was ${since} days ago.`}</b>{' '}
                A backup file keeps your {progress.stats.practiceDays} days of practice safe if the browser clears its storage or you change phone.
              </p>
              <span className="safety-actions">
                <button
                  type="button"
                  className="btn small secondary"
                  data-testid="backup-now"
                  onClick={async () => {
                    const result = await downloadBackup();
                    if (result === 'declined') setBackupMsg(DECLINED_NOTE);
                    else if (result === 'unsupported') setBackupMsg('Downloads are blocked here. Open Settings and use "Copy backup text".');
                  }}
                >
                  Download backup
                </button>
                <button type="button" className="btn small ghost" data-testid="backup-later" onClick={snoozeBackupNudge}>Later</button>
              </span>
              {backupMsg && <p className="muted" role="status" data-testid="backup-nudge-msg">{backupMsg}</p>}
            </div>
          </section>
        );
      })()}

      <div className="progress-line">
        <div className="row">
          <span className="label">{done} / {total} lessons</span>
          {!nextLesson && <span className="label">All lessons done</span>}
          <button type="button" className="link inline" data-testid="units-toggle" onClick={() => setOpenUnits(allUnitsOpen ? new Set() : new Set(course.units.map((u) => u.id)))}>
            {allUnitsOpen ? 'Collapse all' : 'Expand all'}
          </button>
        </div>
        <div className="segs" aria-hidden="true">
          {Array.from({ length: total }, (_, i) => <i key={i} className={i < done ? 'on' : ''} />)}
        </div>
      </div>

      {units.map((unit) => {
        const ui = course.units.indexOf(unit); // numbers are the course's, so "Unit 05" and "5.2" mean the same thing everywhere
        const split = hasTrack(role) ? (unit.id === units[0]!.id ? 'track' : unit.id === units.find((u) => !isFocusUnit(role, u.id))?.id ? 'other' : null) : null;
        const completed = unit.lessons.filter((l) => progress.lessons[l.id]).length;
        const { icon, style } = unitStyle(unit);
        return (
          <Fragment key={unit.id}>
          {split && <h3 className="track-split" data-testid={`track-${split}`}>{split === 'track' ? `Your track · ${ROLE_LABEL[role!]}` : 'Other topics'}</h3>}
          <details
            className={`unit-section ${completed === unit.lessons.length ? 'complete' : ''}`}
            style={style}
            open={openUnits.has(unit.id)}
            onToggle={(e) => setUnitOpen(unit.id, e.currentTarget.open)}
            data-testid={`unit-${unit.id}`}
          >
            <summary className="unit-head">
              <span className="glyph" aria-hidden="true"><Icon name={icon} size={20} /></span>
              <span className="label">Unit {String(ui + 1).padStart(2, '0')}</span>
              <span className="tally" aria-label={`${completed} of ${unit.lessons.length} lessons done`}>{completed}/{unit.lessons.length}</span>
              <h2 id={`unit-${unit.id}`}>{unit.title}</h2>
              <Icon name="chevron-down" className="chev unit-chev" size={18} />
            </summary>
            <p className="unit-desc">{unit.description}</p>
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
                          {isNext && tier > 0 && <span className="tag-next">Up next</span>}
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
          </details>
          </Fragment>
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
