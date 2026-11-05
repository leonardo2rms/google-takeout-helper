/** Test helpers: a tiny synthetic course and progress builders. Not exported from the package index. */
import { emptyProgress, type Card, type Progress } from './progress';
import { questionHash } from './hash';
import type { Course, Question } from './schema';

export function mkQuestion(id: string, prompt = `Prompt for ${id}`): Question {
  return {
    id,
    type: 'single',
    prompt,
    explanation: `Because ${id}`,
    difficulty: 'easy',
    tags: [],
    options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }],
    answer: 'a',
  } as Question;
}

/** `spec`: unit id -> lesson id -> number of questions. */
export function mkCourse(spec: Record<string, Record<string, number>>): Course {
  return {
    units: Object.entries(spec).map(([unitId, lessons]) => ({
      id: unitId,
      title: `Unit ${unitId}`,
      emoji: '*',
      color: '#000000',
      description: '',
      lessons: Object.entries(lessons).map(([lessonId, n]) => ({
        id: lessonId,
        title: `Lesson ${lessonId}`,
        questions: Array.from({ length: n }, (_, i) => mkQuestion(`${lessonId}/${i + 1}`)),
      })),
    })),
  } as unknown as Course;
}

export const card = (q: Question, over: Partial<Card> = {}): Card => ({
  h: questionHash(q),
  ease: 2.3,
  interval: 3,
  due: '2026-01-10',
  reps: 1,
  lapses: 0,
  seen: 1,
  firstTry: 1,
  last: '2026-01-07',
  ...over,
});

export const withCards = (p: Progress, entries: [Question, Partial<Card>][]): Progress => ({
  ...p,
  cards: { ...p.cards, ...Object.fromEntries(entries.map(([q, over]) => [q.id, card(q, over)])) },
});

export const fresh = (over: Partial<Progress> = {}): Progress => ({ ...emptyProgress(), ...over });
