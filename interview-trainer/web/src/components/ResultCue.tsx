/** What a checked item looked like, for screen readers (colour alone must never carry the result). */
export type ResultState = 'correct' | 'wrong' | 'missed' | '';

const TEXT: Record<Exclude<ResultState, ''>, string> = {
  correct: ' (your answer, correct)',
  wrong: ' (your answer, incorrect)',
  missed: ' (a correct answer you did not select)',
};

export function ResultCue({ state }: { state: ResultState }) {
  return state ? <span className="sr-only">{TEXT[state]}</span> : null;
}
