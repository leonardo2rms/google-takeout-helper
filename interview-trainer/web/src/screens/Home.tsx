import { useMemo, useState } from 'react';
import {
  READINESS_TEXT, allQuestions, buildDailySession, goalToday, planFor, rankFor, readiness, reviewQueue, streakStatus, unitOfFor,
  type Course, type HealthResponse, type Progress,
} from '@trainer/shared';
import { ChallengeCard } from '../components/ChallengeCard';
import { OnboardingCard } from '../components/OnboardingCard';
import { Ring } from '../components/Ring';
import { SettingsSheet } from '../components/SettingsSheet';
import { GoalSheet, StreakSheet, streakNote } from '../components/Sheets';
import { cappedCount, plural } from '../lib/format';
import { dismissIosTip, shouldShowIosTip } from '../lib/store';
import { useToday } from '../lib/today';

interface Props {
  course: Course;
  progress: Progress;
  health: HealthResponse | null;
  update: (fn: (p: Progress) => Progress) => void;
  onReset: () => void;
}

const stars = (accuracy: number) => (accuracy >= 0.9 ? 3 : accuracy >= 0.7 ? 2 : 1);
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
      daily.mix.due && `${daily.mix.due} due`,
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

  const finishOnboarding = (warmUp: boolean) => {
    if (warmUp) window.location.hash = '/warmup';
  };

  return (
    <div className="home">
      <header className="home-top">
        <div>
          <h1>Interview Trainer</h1>
          <p className="rank-line" data-testid="rank">
            <b>{rank.title}</b>
            {rank.next !== null ? ` · ${progress.xp}/${rank.next} XP to ${rank.nextTitle}` : ' · top rank'}
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
        <button type="button" className="icon-btn" aria-label="Settings" onClick={() => setSheet('settings')}>⚙</button>
      </header>

      <div className="home-stats">
        <button
          type="button"
          className="stat streak-chip"
          data-testid="streak-chip"
          aria-label={`${status.streak} day streak${progress.freezes ? `, ${progress.freezes} freeze${progress.freezes === 1 ? '' : 's'}` : ''}. Show details`}
          onClick={() => setSheet('streak')}
        >
          🔥 {status.streak}
          {progress.freezes > 0 && <span className="freeze"> 🧊 {progress.freezes}</span>}
        </button>
        <span className="stat" title="Total XP" aria-label={`${progress.xp} XP`}>⚡ {progress.xp}</span>
        <button
          type="button"
          className="goal-ring"
          data-testid="goal-ring"
          aria-label={`Daily goal ${goal.done}/${goal.target}: ${goal.done} of ${goal.target} questions today. Show details`}
          onClick={() => setSheet('goal')}
        >
          <Ring fraction={goal.fraction} size={44} />
          <span className="goal-text" data-testid="goal-text">{goal.done}/{goal.target}</span>
        </button>
      </div>
      {note && <p className="stat-note" data-testid="streak-note">🧊 {note}</p>}
      {status.state === 'lapsed' && (
        <p className="stat-note" data-testid="streak-note">
          Fresh start. Longest: {progress.longestStreak} · {plural(progress.stats.practiceDays, 'day')} practised
        </p>
      )}

      {!progress.profile.onboarded && <OnboardingCard today={today} progress={progress} update={update} onDone={finishOnboarding} />}

      {iosTip && (
        <p className="notice" role="note">
          On iPhone, add this app to your Home Screen (Share, then Add to Home Screen) so Safari does not clear your progress.{' '}
          <button type="button" className="link" onClick={() => { dismissIosTip(); setIosTip(false); }}>Got it</button>
        </p>
      )}

      <a className="card hero" href={hero.href} data-testid="daily-card">
        <span className="emoji" aria-hidden="true">🎯</span>
        <span className="hero-text"><b>{hero.title}</b><small>{hero.sub}</small></span>
        <span className="cta" aria-hidden="true">{hero.cta}</span>
      </a>

      <ChallengeCard course={course} progress={progress} today={today} />

      {countdown && (
        <div className="countdown" data-testid="countdown">
          {countdown.chip && <span className="pill on">{countdown.chip}</span>}
          <p className="muted">{countdown.line}</p>
          {plan.datePassed && <button type="button" className="link" onClick={() => setSheet('settings')}>Set a new date?</button>}
        </div>
      )}

      <section className="practice">
        <a className={`card action ${dueCount ? '' : 'disabled'}`} href={dueCount ? '#/review' : undefined} aria-disabled={!dueCount} data-testid="due-card">
          <span className="emoji" aria-hidden="true">🩹</span>
          <span>
            <b>Review due</b>
            <small data-testid="due-count" data-count={dueCount}>
              {dueCount ? `${cappedCount(dueCount)} ready to review` : 'All caught up'}
            </small>
          </span>
        </a>
        <a className="card action" href="#/mix" data-testid="mix-card">
          <span className="emoji" aria-hidden="true">🎲</span>
          <span><b>Mixed practice</b><small>8 questions, weighted toward your weak spots</small></span>
        </a>
      </section>

      <a className="card action progress-link" href="#/progress" data-testid="progress-link">
        <span className="emoji" aria-hidden="true">📊</span>
        <span>
          <b>Progress</b>
          <small data-testid="readiness-line">Readiness {ready.score} · {READINESS_TEXT[ready.label]}</small>
        </span>
      </a>

      <a className="card action mock-link" href="#/mock" data-testid="mock-link">
        <span className="emoji" aria-hidden="true">⏱️</span>
        <span>
          <b>Mock interview</b>
          <small>{progress.mocks[0] ? `Timed, no feedback until the end · last: ${progress.mocks[0].score}%` : 'Timed, no feedback until the end · optional'}</small>
        </span>
      </a>

      <a className="card action library-link" href="#/library" data-testid="library-link">
        <span className="emoji" aria-hidden="true">📚</span>
        <span>
          <b>Library</b>
          <small>Search every question{Object.keys(progress.bookmarks).length > 0 ? ` · ${Object.keys(progress.bookmarks).length} saved` : ''}</small>
        </span>
      </a>

      <p className="overall">{done}/{total} lessons completed{nextLesson ? <> · up next: <a href={`#/lesson/${nextLesson.id}`}>{nextLesson.title}</a></> : ' 🎉'}</p>

      {course.units.map((unit) => {
        const completed = unit.lessons.filter((l) => progress.lessons[l.id]).length;
        return (
          <section key={unit.id} className="unit" style={{ ['--accent' as string]: unit.color }}>
            <div className="unit-banner">
              <span className="emoji big" aria-hidden="true">{unit.emoji}</span>
              <div>
                <h2>{unit.title}</h2>
                <p>{unit.description}</p>
              </div>
              <span className="unit-count">{completed}/{unit.lessons.length}</span>
            </div>
            <ol className="path">
              {unit.lessons.map((lesson, i) => {
                const rec = progress.lessons[lesson.id];
                return (
                  <li key={lesson.id} className={`node-row ${i % 2 ? 'right' : 'left'}`}>
                    <a className={`node ${rec ? 'done' : ''}`} href={`#/lesson/${lesson.id}`} aria-label={`${i + 1}. ${lesson.title}${rec ? `, completed with ${stars(rec.bestAccuracy)} stars` : ''}`}>
                      <span aria-hidden="true">{rec ? '★' : i + 1}</span>
                    </a>
                    <div className="node-label">
                      <b>{lesson.title}</b>
                      <small>
                        {lesson.questions.length} questions
                        {rec && <span className="stars" aria-hidden="true"> · {'★'.repeat(stars(rec.bestAccuracy))}{'☆'.repeat(3 - stars(rec.bestAccuracy))}</span>}
                      </small>
                    </div>
                  </li>
                );
              })}
            </ol>
          </section>
        );
      })}

      <footer className="home-foot">
        <p className={`pill ${health?.ai ? 'on' : ''}`}>
          {health?.ai ? `🤖 AI grading on · ${health.model}` : '📴 AI grading off · open questions are self-assessed'}
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
