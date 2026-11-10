import { useCallback, useEffect, useMemo, useState } from 'react';
import course from 'virtual:course';
import {
  ACHIEVEMENTS, XP_REPEAT, allQuestions, alreadyDoneToday, applySession, challengeQuestion, challengeResult, challengeStreak, findQuestion, settleChallenge, buildDailySession, dayKey, dayNumber, explanationOf, findLesson, goalToday, mixFilterFor, outcomesOf, pickMix, previewInterval,
  isFirstReview, isResumeKind, nextLessonFor, reviewForecast, reviewQueue, REVIEW_SESSION_SIZE, seededShuffle, streakStatus, unitOfFor, weakSpots,
  type HealthResponse, type Progress, type Question, type SessionEvent, type SessionSummary,
} from '@trainer/shared';
import { useReadingPrefs, useRouteAnnouncer } from './lib/a11y';
import { fetchHealthWithRetry, gradeOpen } from './lib/api';
import { routeOf } from './lib/route';
import { type SessionKind } from './lib/summary-copy';
import { createSampleExplainer, explainOnline, type ExplainFn } from './lib/explain';
import type { GradeFn } from './lib/evaluate';
import { inDays, plural } from './lib/format';
import { resumeFor } from './lib/resume';
import { createSampleGrader, isStandalone, loadSample } from './lib/sample-grader';
import { AchievementToasts } from './components/AchievementToasts';
import { ConfirmProvider } from './components/ConfirmDialog';
import { DataSafetyBanners } from './components/DataSafety';
import { useReminderCheck } from './lib/reminders';
import { backfillBadges, getProgress, requestPersistence, updateProgress, useProgress } from './lib/store';
import { Home } from './screens/Home';
import { Library } from './screens/Library';
import { MockScreen } from './screens/Mock';
import { LessonScreen } from './screens/Lesson';
import { ProgressScreen } from './screens/ProgressScreen';
import { Summary, type Finished } from './screens/Summary';

function useHashRoute(): string {
  const read = () => routeOf(window.location.hash);
  const [route, setRoute] = useState(read);
  useEffect(() => {
    const onChange = () => setRoute(read());
    window.addEventListener('hashchange', onChange);
    return () => window.removeEventListener('hashchange', onChange);
  }, []);
  return route;
}

/**
 * Counts every navigation, even one that ends on the route it started from (the hash handler reads the hash when it runs,
 * so a visit to /done and a return to /q/x queued together look like "no change"). A session screen is keyed by it, so
 * every visit to a lesson or practice route starts or resumes a session and never reuses a finished one's state.
 */
function useVisit(): number {
  const [visit, setVisit] = useState(0);
  useEffect(() => {
    const bump = () => setVisit((v) => v + 1);
    window.addEventListener('hashchange', bump);
    return () => window.removeEventListener('hashchange', bump);
  }, []);
  return visit;
}

const go = (route: string) => {
  window.location.hash = route;
};

const MIX_SIZE = 8;

type PracticeKind = Exclude<SessionKind, 'lesson'>;

/** Screens that put focus on their own question/prompt, so the route announcer must not steal it. */
const OWNS_FOCUS = /^\/(lesson\/|mix$|review$|daily$|warmup$|topic\/)/;

function titleFor(route: string): string {
  if (route === '/progress') return 'Progress';
  if (route === '/done') return 'Session complete';
  if (OWNS_FOCUS.test(route)) return 'Practice';
  return 'Interview Trainer';
}

/** Page chrome shared by every screen: skip link, the single <main> landmark, route announcements, reading preferences. */
export function App() {
  useEffect(() => backfillBadges(), []);
  return (
    <ConfirmProvider>
      <Shell />
      <AchievementToasts />
    </ConfirmProvider>
  );
}

/** Landmarks, skip link, reading preferences and route announcements around the screens. */
function Shell() {
  const route = useHashRoute();
  const { progress } = useProgress();
  useReadingPrefs(progress.prefs.reading);
  useRouteAnnouncer(titleFor(route), OWNS_FOCUS.test(route));
  return (
    <>
      <a
        className="skip-link"
        href="#main"
        onClick={(e) => {
          e.preventDefault(); // the hash is our router: move focus instead of navigating
          document.getElementById('main')?.focus();
        }}
      >
        Skip to main content
      </a>
      <DataSafetyBanners route={route} />
      <main id="main" tabIndex={-1}>
        <Screens />
      </main>
    </>
  );
}

function Screens() {
  const route = useHashRoute();
  const visit = useVisit();
  const { progress, update, reset } = useProgress();
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [finished, setFinished] = useState<Finished | null>(null);
  useReminderCheck(course);

  const [sampleGrade, setSampleGrade] = useState<GradeFn | undefined>();
  const [sampleExplain, setSampleExplain] = useState<ExplainFn | undefined>();

  useEffect(() => {
    if (!isStandalone) {
      // A server that is still starting must not switch AI off for the whole session: ask again, a few times.
      const abort = new AbortController();
      void fetchHealthWithRetry(abort.signal).then((h) => h && setHealth(h));
      return () => abort.abort();
    }
    // Hosted single-file build: no API server; grade through the viewer's own Claude when offered.
    void loadSample().then((sample) => {
      if (!sample) return;
      setSampleGrade(() => createSampleGrader(course, sample));
      setSampleExplain(() => createSampleExplainer(course, sample));
      setHealth({ ok: true, ai: true, model: 'your Claude account' });
    });
  }, []);

  // `servedOn` is the day the session was opened: the daily challenge is recorded against it, even when the answer comes after midnight.
  const finish = useCallback((title: string, summary: SessionSummary, sessionKind: SessionKind, lessonId?: string, servedOn?: string) => {
    const today = dayKey(new Date());
    const challengeDay = servedOn ?? today;
    // A session of one question (Library practice, the daily challenge) is stored as a "single".
    const kind = sessionKind === 'one' ? 'single' : sessionKind === 'challenge' ? 'challenge' : undefined;
    const outcomes = outcomesOf(summary);
    let before: Progress = getProgress();
    let events: SessionEvent[] = [];
    // A session of one question (Library practice, the daily challenge) earns no "perfect lesson" bonus.
    const baseXp = kind ? summary.xp - summary.bonus : summary.xp;
    const badgesBefore = getProgress().achievements;
    const pass = summary.accuracy >= 1;
    let challengeBonus = 0;
    updateProgress((p) => {
      before = p;
      const settled = kind === 'challenge' ? settleChallenge(p, challengeDay, pass) : null;
      challengeBonus = settled?.bonus ?? 0;
      const res = applySession(
        p,
        { lessonId, kind: kind ? 'single' : undefined, accuracy: summary.accuracy, xp: baseXp + challengeBonus, missed: summary.missed, recovered: summary.recovered, cleared: summary.correct, outcomes, hinted: summary.hinted, confidence: summary.confidence },
        { today, course },
      );
      events = res.events;
      // The first finished session ends first-run setup: the plan card is not shown again unless the learner asks to edit it.
      const done = { ...res.progress, profile: { ...res.progress.profile, onboarded: true } };
      return kind === 'challenge' ? settleChallenge(done, challengeDay, pass).progress : done;
    });
    const after = getProgress();
    if (before.stats.sessions === 0) requestPersistence(); // ask the browser to keep our storage once there is something worth keeping

    const tipEvent = events.find((e): e is Extract<SessionEvent, { type: 'tip' }> => e.type === 'tip');
    const tipQuestion = tipEvent ? allQuestions(course).find((q) => q.id === tipEvent.questionId) : undefined;
    const nextLesson = nextLessonFor(course, after.profile.role, after.lessons, lessonId);
    setFinished({
      title,
      kind: sessionKind,
      summary,
      xpEarned: baseXp + challengeBonus, // what was really credited (a one-question session earns no perfect-lesson bonus)
      newBadges: ACHIEVEMENTS.filter((a) => after.achievements[a.id] && !badgesBefore[a.id]).map(({ id, title: t, icon, description }) => ({ id, title: t, icon, description })),
      streak: streakStatus(after, today).streak,
      nextLessonId: nextLesson?.id,
      events,
      goalBefore: goalToday(before, today),
      goalAfter: goalToday(after, today),
      // Everything that is coming, not just this session's cards: first reviews are spread over several days, so what is due
      // tomorrow can come from earlier lessons, and the number must match what Home will say.
      forecast: reviewForecast(after.cards, Object.keys(after.cards), today),
      tip: tipEvent && tipQuestion ? { text: explanationOf(tipQuestion), bonusXp: tipEvent.bonusXp } : null,
      extraNotes: [
        ...(kind === 'challenge' ? [`Daily challenge ${pass ? `passed (+${challengeBonus} XP)` : 'done'}. Challenge streak: ${plural(challengeStreak(after, today).current, 'day')}.`] : []),
        ...(summary.repeats > 0 ? [`${plural(summary.repeats, 'question')} you had already done today earned at most ${XP_REPEAT} XP each and did not count again towards today's goal.`] : []),
      ],
    });
    go('/done');
  }, []);

  const grade = useMemo<GradeFn | undefined>(() => (isStandalone ? sampleGrade : health?.ai ? gradeOpen : undefined), [sampleGrade, health]);

  const explain = useMemo<ExplainFn | undefined>(() => (isStandalone ? sampleExplain : health?.explain ? explainOnline : undefined), [sampleExplain, health]);

  // "You'll see this again tomorrow": computed from the progress as it was when the lesson started.
  const reviewHint = useCallback(
    (q: Question, outcome: 'first' | 'retry' | 'failed') => {
      if (outcome === 'failed') return null; // it comes straight back in this session
      // A first review is spread over the next few days to keep any one day light, and where it lands depends on the other cards of
      // this session, which are not scheduled yet: so no exact day is promised (the summary, after scheduling, gives the real counts).
      if (outcome === 'first' && isFirstReview(getProgress(), q)) return "You'll see this again in the next few days.";
      const days = previewInterval(getProgress(), q, outcome, dayKey(new Date()));
      return days >= 1 ? `You'll see this again ${inDays(days)}.` : null;
    },
    [],
  );

  if (route.startsWith('/lesson/')) {
    const found = findLesson(course, route.slice('/lesson/'.length));
    if (!found) return <NotFound noun="Lesson" />;
    const ui = course.units.indexOf(found.unit);
    const num = `${ui + 1}.${found.unit.lessons.indexOf(found.lesson) + 1}`;
    return (
      <SessionScreen
        key={`${route}#${visit}`}
        route={route}
        kind="lesson"
        title={`${num} ${found.lesson.title}`}
        finishTitle={found.lesson.title}
        lessonId={found.lesson.id}
        questions={found.lesson.questions}
        progress={progress}
        grade={grade}
        explain={explain}
        reviewHint={reviewHint}
        onFinish={finish}
      />
    );
  }

  const practice = (kind: PracticeKind, title: string, unitId?: string) => (
    <SessionScreen key={`${kind}${unitId ?? ''}#${visit}`} route={practiceRoute(kind, unitId)} kind={kind} title={title} unitId={unitId} progress={progress} grade={grade} explain={explain} reviewHint={reviewHint} onFinish={finish} />
  );
  if (route === '/library') return <Library course={course} progress={progress} />;
  if (route === '/mock') return <MockScreen course={course} progress={progress} grade={grade} />;
  if (route === '/challenge') {
    // Today's featured question. Once it has a result, a replay is plain practice (no XP, no streak).
    const today = dayKey(new Date());
    const q = challengeQuestion(course, today);
    if (!q) return <NotFound noun="Question" />;
    return challengeResult(progress, today) ? practice('one', 'Practise one question', q.id) : practice('challenge', 'Daily challenge');
  }
  if (route.startsWith('/q/')) {
    // One question from the Library: a session of one (it never counts as a "perfect lesson").
    const q = findQuestion(course, route.slice('/q/'.length));
    if (!q) return <NotFound noun="Question" />;
    return practice('one', 'Practise one question', q.id);
  }
  if (route === '/mix') return practice('mix', 'Mixed practice');
  if (route === '/review') return practice('review', 'Review due');
  if (route === '/daily') return practice('daily', 'Today');
  if (route === '/warmup') return practice('warmup', 'Warm-up');
  if (route.startsWith('/topic/')) {
    const unitId = route.slice('/topic/'.length);
    const unit = course.units.find((u) => u.id === unitId);
    if (!unit) return <NotFound noun="Topic" />;
    return practice('topic', `Practise: ${unit.title}`, unit.id);
  }
  if (route === '/progress') return <ProgressScreen course={course} progress={progress} />;

  if (route === '/done') {
    if (!finished) {
      go('/');
      return null;
    }
    return <Summary finished={finished} settings={progress.settings} onHome={() => go('/')} />;
  }

  return <Home course={course} progress={progress} health={health} update={update} onReset={reset} />;
}

/** Picks the questions for a session once, when the screen mounts, so answering does not reshuffle it. */
function pickQuestions(kind: PracticeKind, progress: Progress, unitId?: string): Question[] {
  const today = dayKey(new Date());
  const seed = dayNumber(today) * 131 + progress.stats.sessions;
  switch (kind) {
    case 'review':
      return reviewQueue(progress, allQuestions(course), today, unitOfFor(course)).all.slice(0, REVIEW_SESSION_SIZE);
    case 'challenge': {
      const c = challengeQuestion(course, today);
      return c ? [c] : [];
    }
    case 'one': {
      const one = findQuestion(course, unitId ?? '');
      return one ? [one] : [];
    }
    case 'daily':
      return buildDailySession(course, progress, today).questions;
    case 'warmup':
      return buildDailySession(course, progress, today, { size: 3 }).questions;
    case 'topic': {
      const weak = weakSpots(course, progress, today, MIX_SIZE, seed, unitId);
      const ids = new Set(weak.map((q) => q.id));
      const unseen = course.units.find((u) => u.id === unitId)?.lessons.flatMap((l) => l.questions).filter((q) => !ids.has(q.id) && !progress.cards[q.id]) ?? [];
      return [...weak, ...seededShuffle(unseen, seed)].slice(0, MIX_SIZE);
    }
    default:
      return pickMix(course, progress, today, MIX_SIZE, seed, mixFilterFor(progress));
  }
}

/** The route that opens a practice session: opening it again carries on from a saved one. */
function practiceRoute(kind: PracticeKind, unitId?: string): string {
  switch (kind) {
    case 'topic':
      return `/topic/${unitId}`;
    case 'one':
      return `/q/${unitId}`;
    default:
      return `/${kind}`;
  }
}

/**
 * A lesson or a practice session. The questions are fixed when it opens (a lesson's own, or a pick for the day), unless
 * the learner left this same session half way: then it carries on from the saved one, with the same questions.
 * One-question sessions (Library practice, the daily challenge) are never saved.
 */
function SessionScreen({ route, kind, title, finishTitle = title, unitId, lessonId, questions: fixed, progress, grade, explain, reviewHint, onFinish }: {
  route: string;
  kind: SessionKind;
  title: string;
  /** The title the summary shows, when it differs from the one in the lesson header. */
  finishTitle?: string;
  unitId?: string;
  lessonId?: string;
  /** A lesson brings its questions; a practice session picks them. */
  questions?: Question[];
  progress: Progress;
  grade?: GradeFn;
  explain?: ExplainFn;
  reviewHint: (q: Question, outcome: 'first' | 'retry' | 'failed') => string | null;
  onFinish: (title: string, s: SessionSummary, kind: SessionKind, lessonId?: string, servedOn?: string) => void;
}) {
  const resumable = isResumeKind(kind);
  const [saved] = useState(() => (resumable ? resumeFor(route) : null));
  const [questions] = useState<Question[]>(() => (saved ? saved.session.queue : (fixed ?? pickQuestions(kind as PracticeKind, progress, unitId))));
  const [servedOn] = useState(() => saved?.record.servedOn ?? dayKey(new Date())); // the day the session was opened
  // Questions already answered today pay reduced XP and do not count again for the goal. Fixed when the session opens.
  const [repeats] = useState(() => alreadyDoneToday(progress, questions, dayKey(new Date())));

  if (questions.length === 0) {
    return (
      <div className="summary plain">
        <p className="label">Review</p>
        <h1>Nothing to review</h1>
        <p className="muted">Nothing is due right now. New reviews appear as they come due.</p>
        <div className="summary-actions"><a className="btn primary" href="#/">Back</a></div>
      </div>
    );
  }
  return (
    <LessonScreen
      title={title}
      questions={questions}
      grade={grade}
      explain={explain}
      reviewHint={reviewHint}
      haptics={progress.settings.haptics}
      save={resumable ? { route, kind, lessonId, servedOn } : undefined}
      initial={saved?.session}
      repeats={repeats}
      onExit={() => go('/')}
      onFinish={(s) => onFinish(finishTitle, s, kind, lessonId, servedOn)}
    />
  );
}

function NotFound({ noun }: { noun: 'Lesson' | 'Question' | 'Topic' }) {
  return (
    <div className="summary plain">
      <p className="label">404</p>
      <h1>{noun} not found</h1>
      <p className="muted">That link does not point to a {noun.toLowerCase()} in this course.</p>
      <div className="summary-actions"><a className="btn primary" href="#/">Back home</a></div>
    </div>
  );
}
