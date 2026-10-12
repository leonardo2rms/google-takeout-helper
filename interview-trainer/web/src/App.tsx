import { useCallback, useEffect, useState } from 'react';
import course from 'virtual:course';
import {
  allQuestions, dayKey, findLesson, liveStreak, recordResult, seededShuffle,
  type HealthResponse, type Question, type SessionSummary,
} from '@trainer/shared';
import { fetchHealth } from './lib/api';
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

  useEffect(() => {
    void fetchHealth().then(setHealth);
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

  const ai = Boolean(health?.ai);

  if (route.startsWith('/lesson/')) {
    const found = findLesson(course, route.slice('/lesson/'.length));
    if (!found) return <NotFound />;
    return (
      <LessonScreen
        key={found.lesson.id}
        title={found.lesson.title}
        color={found.unit.color}
        questions={found.lesson.questions}
        aiAvailable={ai}
        onExit={() => go('/')}
        onFinish={(s) => finish(found.lesson.title, s, found.lesson.id)}
      />
    );
  }

  if (route === '/mix') return <Practice key="mix" kind="mix" title="Mixed practice" ai={ai} missed={progress.missed} onFinish={finish} />;
  if (route === '/review') return <Practice key="review" kind="review" title="Review mistakes" ai={ai} missed={progress.missed} onFinish={finish} />;

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
function Practice({ kind, title, ai, missed, onFinish }: {
  kind: 'mix' | 'review';
  title: string;
  ai: boolean;
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
      aiAvailable={ai}
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
