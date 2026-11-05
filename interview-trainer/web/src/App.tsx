import { useCallback, useEffect, useMemo, useState } from 'react';
import course from 'virtual:course';
import {
  CHALLENGE_XP, XP_PERFECT_BONUS, allQuestions, applySession, challengeQuestion, challengeResult, challengeStreak, findQuestion, recordChallenge, buildDailySession, dayKey, dayNumber, explanationOf, findLesson, goalToday, outcomesOf, pickMix, previewInterval,
  reviewForecast, reviewQueue, seededShuffle, streakStatus, unitOfFor, weakSpots,
  type HealthResponse, type Progress, type Question, type SessionEvent, type SessionSummary,
} from '@trainer/shared';
import { useReadingPrefs, useRouteAnnouncer } from './lib/a11y';
import { fetchHealth, gradeOpen } from './lib/api';
import { createSampleExplainer, explainOnline, type ExplainFn } from './lib/explain';
import type { GradeFn } from './lib/evaluate';
import { inDays, plural } from './lib/format';
import { createSampleGrader, isStandalone, loadSample } from './lib/sample-grader';
import { AchievementToasts } from './components/AchievementToasts';
import { useReminderCheck } from './lib/reminders';
import { backfillBadges, getProgress, requestPersistence, updateProgress, useProgress } from './lib/store';
import { Home } from './screens/Home';
import { Library } from './screens/Library';
import { MockScreen } from './screens/Mock';
import { LessonScreen } from './screens/Lesson';
import { ProgressScreen } from './screens/ProgressScreen';
import { Summary, type Finished } from './screens/Summary';

function useHashRoute(): string {
  const read = () => window.location.hash.replace(/^#/, '') || '/';
  const [route, setRoute] = useState(read);
  useEffect(() => {
    const onChange = () => setRoute(read());
    window.addEventListener('hashchange', onChange);
    return () => window.removeEventListener('hashchange', onChange);
  }, []);
  return route;
}

const go = (route: string) => {
  window.location.hash = route;
};

const MIX_SIZE = 8;
const REVIEW_SIZE = 10;

type PracticeKind = 'mix' | 'review' | 'daily' | 'warmup' | 'topic' | 'one' | 'challenge';

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
    <>
      <Shell />
      <AchievementToasts />
    </>
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
      <main id="main" tabIndex={-1}>
        <Screens />
      </main>
    </>
  );
}

function Screens() {
  const route = useHashRoute();
  const { progress, update, reset } = useProgress();
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [finished, setFinished] = useState<Finished | null>(null);
  useReminderCheck(course);

  const [sampleGrade, setSampleGrade] = useState<GradeFn | undefined>();
  const [sampleExplain, setSampleExplain] = useState<ExplainFn | undefined>();

  useEffect(() => {
    if (!isStandalone) {
      void fetchHealth().then(setHealth);
      return;
    }
    // Hosted single-file build: no API server; grade through the viewer's own Claude when offered.
    void loadSample().then((sample) => {
      if (!sample) return;
      setSampleGrade(() => createSampleGrader(course, sample));
      setSampleExplain(() => createSampleExplainer(course, sample));
      setHealth({ ok: true, ai: true, model: 'your Claude account' });
    });
  }, []);

  const finish = useCallback((title: string, summary: SessionSummary, lessonId?: string, kind?: 'single' | 'challenge') => {
    const today = dayKey(new Date());
    const outcomes = outcomesOf(summary);
    let before: Progress = getProgress();
    let events: SessionEvent[] = [];
    // A session of one question (Library practice, the daily challenge) earns no "perfect lesson" bonus.
    const baseXp = kind && summary.perfect ? summary.xp - XP_PERFECT_BONUS : summary.xp;
    const pass = summary.accuracy >= 1;
    let challengeBonus = 0;
    updateProgress((p) => {
      before = p;
      challengeBonus = kind === 'challenge' && pass && !p.challenge[today] ? CHALLENGE_XP : 0;
      const res = applySession(
        p,
        { lessonId, kind: kind ? 'single' : undefined, accuracy: summary.accuracy, xp: baseXp + challengeBonus, missed: summary.missed, recovered: summary.recovered, cleared: summary.correct, outcomes, hinted: summary.hinted, confidence: summary.confidence },
        { today, course },
      );
      events = res.events;
      return kind === 'challenge' ? recordChallenge(res.progress, today, pass) : res.progress;
    });
    const after = getProgress();
    if (before.stats.sessions === 0) requestPersistence(); // ask the browser to keep our storage once there is something worth keeping

    const tipEvent = events.find((e): e is Extract<SessionEvent, { type: 'tip' }> => e.type === 'tip');
    const tipQuestion = tipEvent ? allQuestions(course).find((q) => q.id === tipEvent.questionId) : undefined;
    const nextLesson = course.units.flatMap((u) => u.lessons).find((l) => !after.lessons[l.id] && l.id !== lessonId);
    setFinished({
      title,
      summary,
      streak: streakStatus(after, today).streak,
      nextLessonId: nextLesson?.id,
      events,
      goalBefore: goalToday(before, today),
      goalAfter: goalToday(after, today),
      forecast: reviewForecast(after.cards, Object.keys(outcomes), today),
      tip: tipEvent && tipQuestion ? { text: explanationOf(tipQuestion), bonusXp: tipEvent.bonusXp } : null,
      extraNotes:
        kind === 'challenge'
          ? [`Daily challenge ${pass ? `passed (+${challengeBonus} XP)` : 'done'}. Challenge streak: ${plural(challengeStreak(after, today).current, 'day')}.`]
          : [],
    });
    go('/done');
  }, []);

  const grade = useMemo<GradeFn | undefined>(() => (isStandalone ? sampleGrade : health?.ai ? gradeOpen : undefined), [sampleGrade, health]);

  const explain = useMemo<ExplainFn | undefined>(() => (isStandalone ? sampleExplain : health?.explain ? explainOnline : undefined), [sampleExplain, health]);

  // "You'll see this again tomorrow": computed from the progress as it was when the lesson started.
  const reviewHint = useCallback(
    (q: Question, outcome: 'first' | 'retry' | 'failed') => {
      if (outcome === 'failed') return null; // it comes straight back in this session
      const days = previewInterval(getProgress(), q, outcome, dayKey(new Date()));
      return days >= 1 ? `You'll see this again ${inDays(days)}.` : null;
    },
    [],
  );

  if (route.startsWith('/lesson/')) {
    const found = findLesson(course, route.slice('/lesson/'.length));
    if (!found) return <NotFound />;
    return (
      <LessonScreen
        key={found.lesson.id}
        title={found.lesson.title}
        color={found.unit.color}
        questions={found.lesson.questions}
        grade={grade}
        explain={explain}
        reviewHint={reviewHint}
        haptics={progress.settings.haptics}
        onExit={() => go('/')}
        onFinish={(s) => finish(found.lesson.title, s, found.lesson.id)}
      />
    );
  }

  const practice = (kind: PracticeKind, title: string, unitId?: string) => (
    <Practice key={`${kind}${unitId ?? ''}`} kind={kind} title={title} unitId={unitId} progress={progress} grade={grade} explain={explain} reviewHint={reviewHint} onFinish={finish} />
  );
  if (route === '/library') return <Library course={course} progress={progress} />;
  if (route === '/mock') return <MockScreen course={course} progress={progress} grade={grade} />;
  if (route === '/challenge') {
    // Today's featured question. Once it has a result, a replay is plain practice (no XP, no streak).
    const today = dayKey(new Date());
    const q = challengeQuestion(course, today);
    if (!q) return <NotFound />;
    return challengeResult(progress, today) ? practice('one', 'Practise one question', q.id) : practice('challenge', 'Daily challenge');
  }
  if (route.startsWith('/q/')) {
    // One question from the Library: a session of one (it never counts as a "perfect lesson").
    const q = findQuestion(course, route.slice('/q/'.length));
    if (!q) return <NotFound />;
    return practice('one', 'Practise one question', q.id);
  }
  if (route === '/mix') return practice('mix', 'Mixed practice');
  if (route === '/review') return practice('review', 'Review due');
  if (route === '/daily') return practice('daily', 'Today');
  if (route === '/warmup') return practice('warmup', 'Warm-up');
  if (route.startsWith('/topic/')) {
    const unitId = route.slice('/topic/'.length);
    const unit = course.units.find((u) => u.id === unitId);
    if (!unit) return <NotFound />;
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

const COLORS: Record<PracticeKind, string> = { mix: '#1cb0f6', review: '#ff9600', daily: '#58cc02', warmup: '#58cc02', topic: '#ce82ff', one: '#1cb0f6', challenge: '#ffc800' };

/** Picks the questions for a session once, when the screen mounts, so answering does not reshuffle it. */
function pickQuestions(kind: PracticeKind, progress: Progress, unitId?: string): Question[] {
  const today = dayKey(new Date());
  const seed = dayNumber(today) * 131 + progress.stats.sessions;
  switch (kind) {
    case 'review':
      return reviewQueue(progress, allQuestions(course), today, unitOfFor(course)).all.slice(0, REVIEW_SIZE);
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
      return pickMix(course, progress, today, MIX_SIZE, seed);
  }
}

function Practice({ kind, title, unitId, progress, grade, explain, reviewHint, onFinish }: {
  kind: PracticeKind;
  title: string;
  unitId?: string;
  progress: Progress;
  grade?: GradeFn;
  explain?: ExplainFn;
  reviewHint: (q: Question, outcome: 'first' | 'retry' | 'failed') => string | null;
  onFinish: (title: string, s: SessionSummary, lessonId?: string, kind?: 'single' | 'challenge') => void;
}) {
  const [questions] = useState<Question[]>(() => pickQuestions(kind, progress, unitId));

  if (questions.length === 0) {
    return (
      <div className="summary">
        <div className="trophy" aria-hidden="true">✨</div>
        <h1>All clear!</h1>
        <p className="muted">Nothing to review right now. New reviews appear as they come due.</p>
        <a className="btn primary" href="#/">Back</a>
      </div>
    );
  }
  return (
    <LessonScreen
      title={title}
      color={COLORS[kind]}
      questions={questions}
      grade={grade}
      explain={explain}
      reviewHint={reviewHint}
      haptics={progress.settings.haptics}
      onExit={() => go('/')}
      onFinish={(s) => onFinish(title, s, undefined, kind === 'one' ? 'single' : kind === 'challenge' ? 'challenge' : undefined)}
    />
  );
}

function NotFound() {
  return (
    <div className="summary">
      <div className="trophy" aria-hidden="true">🧭</div>
      <h1>Lesson not found</h1>
      <a className="btn primary" href="#/">Back home</a>
    </div>
  );
}
