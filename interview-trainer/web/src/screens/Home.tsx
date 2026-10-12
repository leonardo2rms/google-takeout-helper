import { liveStreak, dayKey, type Course, type HealthResponse, type Progress } from '@trainer/shared';

interface Props {
  course: Course;
  progress: Progress;
  health: HealthResponse | null;
  onReset: () => void;
}

const stars = (accuracy: number) => (accuracy >= 0.9 ? 3 : accuracy >= 0.7 ? 2 : 1);

export function Home({ course, progress, health, onReset }: Props) {
  const streak = liveStreak(progress, dayKey(new Date()));
  const total = course.units.reduce((n, u) => n + u.lessons.length, 0);
  const done = Object.keys(progress.lessons).length;
  const nextLesson = course.units.flatMap((u) => u.lessons).find((l) => !progress.lessons[l.id]);

  return (
    <div className="home">
      <header className="home-top">
        <h1>Interview Trainer</h1>
        <div className="stats">
          <span className="stat" title="Day streak" aria-label={`${streak} day streak`}>🔥 {streak}</span>
          <span className="stat" title="Total XP" aria-label={`${progress.xp} XP`}>⚡ {progress.xp}</span>
        </div>
      </header>

      <section className="practice">
        <a className="card action" href="#/mix">
          <span className="emoji" aria-hidden="true">🎲</span>
          <span><b>Mixed practice</b><small>8 random questions across all topics</small></span>
        </a>
        <a className={`card action ${progress.missed.length ? '' : 'disabled'}`} href={progress.missed.length ? '#/review' : undefined} aria-disabled={!progress.missed.length}>
          <span className="emoji" aria-hidden="true">🩹</span>
          <span><b>Review mistakes</b><small>{progress.missed.length ? `${progress.missed.length} question${progress.missed.length === 1 ? '' : 's'} to revisit` : 'Nothing to review yet'}</small></span>
        </a>
      </section>

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
                    <a className={`node ${rec ? 'done' : ''}`} href={`#/lesson/${lesson.id}`} aria-label={`${lesson.title}${rec ? `, completed with ${stars(rec.bestAccuracy)} stars` : ''}`}>
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
        <button type="button" className="link" onClick={() => window.confirm('Reset all progress, XP and streak?') && onReset()}>
          Reset progress
        </button>
      </footer>
    </div>
  );
}
