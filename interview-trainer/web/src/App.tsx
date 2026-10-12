import { useCallback, useEffect, useMemo, useState } from 'react';
import course from 'virtual:course';
import {
  allQuestions, dayKey, findLesson, liveStreak, recordResult, seededShuffle,
  type HealthResponse, type Question, type SessionSummary,
} from '@trainer/shared';
import { fetchHealth, gradeOpen } from './lib/api';
import type { GradeFn } from './lib/evaluate';
import { createSampleGrader, isStandalone, loadSample } from './lib/sample-grader';
import { useProgress } from './lib/store';
import { Home } from './screens/Home';
import { LessonScreen } from './screens/Lesson';
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

export function App() {
  const route = useHashRoute();
  const { progress, update, reset } = useProgress();
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [finished, setFinished] = useState<Finished | null>(null);

  const [sampleGrade, setSampleGrade] = useState<GradeFn | undefined>();

  useEffect(() => {
    if (!isStandalone) {
      void fetchHealth().then(setHealth);
      return;
    }
    // Hosted single-file build: no API server; grade through the viewer's own Claude when offered.
    void loadSample().then((sample) => {
      if (!sample) return;
      setSampleGrade(() => createSampleGrader(course, sample));
      setHealth({ ok: true, ai: true, model: 'your Claude account' });
    });
  }, []);

  const finish = useCallback(
    (title: string, summary: SessionSummary, lessonId?: string) => {
      const today = dayKey(new Date());
      const next = recordResult(progress, { lessonId, accuracy: summary.accuracy, xp: summary.xp, missed: summary.missed, recovered: summary.recovered, cleared: summary.correct }, today);
      update(() => next);
      const nextLesson = course.units.flatMap((u) => u.lessons).find((l) => !next.lessons[l.id] && l.id !== lessonId);
      setFinished({ title, summary, streak: liveStreak(next, today), nextLessonId: nextLesson?.id });
      go('/done');
    },
    [progress, update],
  );

  const grade = useMemo<GradeFn | undefined>(() => (isStandalone ? sampleGrade : health?.ai ? gradeOpen : undefined), [sampleGrade, health]);

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
        onExit={() => go('/')}
        onFinish={(s) => finish(found.lesson.title, s, found.lesson.id)}
      />
    );
  }

  if (route === '/mix') return <Practice key="mix" kind="mix" title="Mixed practice" grade={grade} missed={progress.missed} onFinish={finish} />;
  if (route === '/review') return <Practice key="review" kind="review" title="Review mistakes" grade={grade} missed={progress.missed} onFinish={finish} />;

  if (route === '/done') {
    if (!finished) {
      go('/');
      return null;
    }
    return <Summary finished={finished} onHome={() => go('/')} />;
  }

  return <Home course={course} progress={progress} health={health} onReset={reset} />;
}

/** Mixed / review sessions: the question list is picked once, when the screen mounts. */
function Practice({ kind, title, grade, missed, onFinish }: {
  kind: 'mix' | 'review';
  title: string;
  grade?: GradeFn;
  missed: string[];
  onFinish: (title: string, s: SessionSummary) => void;
}) {
  const [questions] = useState<Question[]>(() => {
    const all = allQuestions(course);
    const pool = kind === 'review' ? all.filter((q) => missed.includes(q.id)) : all;
    return seededShuffle(pool, Math.floor(Math.random() * 2 ** 31)).slice(0, kind === 'review' ? REVIEW_SIZE : MIX_SIZE);
  });

  if (questions.length === 0) {
    return (
      <div className="summary">
        <div className="trophy" aria-hidden="true">✨</div>
        <h1>All clear!</h1>
        <p className="muted">No mistakes to review right now.</p>
        <a className="btn primary" href="#/">Back</a>
      </div>
    );
  }
  return (
    <LessonScreen
      title={title}
      color={kind === 'mix' ? '#1cb0f6' : '#ff9600'}
      questions={questions}
      grade={grade}
      onExit={() => go('/')}
      onFinish={(s) => onFinish(title, s)}
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
