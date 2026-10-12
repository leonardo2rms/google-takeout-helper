import { z } from 'zod';

const slug = z.string().regex(/^[a-z0-9][a-z0-9-]*$/, 'use lowercase letters, digits and dashes');

const CodeBlock = z.object({
  lang: z.string().default('kotlin'),
  source: z.string().min(1),
});

/** Fields shared by every question type. */
const common = {
  prompt: z.string().min(1),
  /** Optional snippet shown under the prompt (e.g. "what is the time complexity of this?"). */
  code: CodeBlock.optional(),
  /** Shown after the answer is checked, whether the learner was right or wrong. */
  explanation: z.string().min(1),
  difficulty: z.enum(['easy', 'medium', 'hard']).default('medium'),
  tags: z.array(z.string()).default([]),
};

const Option = z.object({ id: z.string().min(1), text: z.string().min(1) });

export const SingleQuestion = z.object({
  type: z.literal('single'),
  ...common,
  options: z.array(Option).min(2).max(6),
  answer: z.string(),
});

export const MultiQuestion = z.object({
  type: z.literal('multi'),
  ...common,
  options: z.array(Option).min(3).max(7),
  answers: z.array(z.string()).min(1),
});

/**
 * Short coding challenge #1: tap tokens from a bank into the blanks of a snippet.
 * Blanks are written `[[1]]`, `[[2]]`, ... in `template`; `answers[i]` fills blank i+1.
 */
export const FillQuestion = z.object({
  type: z.literal('fill'),
  ...common,
  lang: z.string().default('kotlin'),
  template: z.string().min(1),
  bank: z.array(z.string().min(1)).min(2),
  answers: z.array(z.string().min(1)).min(1),
});

/** Short coding challenge #2: put the shuffled lines of a snippet back in order. */
export const OrderQuestion = z.object({
  type: z.literal('order'),
  ...common,
  lang: z.string().default('kotlin'),
  lines: z.array(z.string()).min(3).max(10),
});

/** Free-text (or free-code) answer graded by AI against a rubric of key points. */
export const OpenQuestion = z.object({
  type: z.literal('open'),
  ...common,
  /** Optional for open questions: the model answer is shown as the explanation. */
  explanation: z.string().optional(),
  format: z.enum(['text', 'code']).default('text'),
  modelAnswer: z.string().min(1),
  /**
   * The rubric. `keywords` are any-of synonyms used by the offline heuristic
   * (the AI grader ignores them and judges meaning).
   */
  keyPoints: z
    .array(z.object({ point: z.string().min(1), keywords: z.array(z.string().min(1)).min(1) }))
    .min(2)
    .max(6),
  maxChars: z.number().int().positive().default(1200),
});

const RawQuestion = z.discriminatedUnion('type', [
  SingleQuestion,
  MultiQuestion,
  FillQuestion,
  OrderQuestion,
  OpenQuestion,
]);

const RawLesson = z.object({
  id: slug,
  title: z.string().min(1),
  questions: z.array(RawQuestion).min(3),
});

/** Question ids are derived (`<lessonId>/<n>`) so authors never have to maintain them. */
const Lesson = RawLesson.transform((lesson) => ({
  ...lesson,
  questions: lesson.questions.map((q, i) => ({ ...q, id: `${lesson.id}/${i + 1}` })),
}));

export const Unit = z.object({
  id: slug,
  title: z.string().min(1),
  emoji: z.string().min(1),
  /** Any CSS colour; used for the unit's header and path nodes. */
  color: z.string().default('#58cc02'),
  description: z.string().min(1),
  lessons: z.array(Lesson).min(1),
});

export const Course = z.object({ units: z.array(Unit).min(1) });

export type Question = z.output<typeof Lesson>['questions'][number];
export type SingleQ = Extract<Question, { type: 'single' }>;
export type MultiQ = Extract<Question, { type: 'multi' }>;
export type FillQ = Extract<Question, { type: 'fill' }>;
export type OrderQ = Extract<Question, { type: 'order' }>;
export type OpenQ = Extract<Question, { type: 'open' }>;
export type Lesson = z.output<typeof Lesson>;
export type Unit = z.output<typeof Unit>;
export type Course = z.output<typeof Course>;

export const BLANK = /\[\[(\d+)\]\]/g;

/** Number of blanks in a fill template, as the highest blank index. */
export function blankIndexes(template: string): number[] {
  return [...template.matchAll(BLANK)].map((m) => Number(m[1]));
}

/**
 * Cross-field checks Zod can't express. Returns human-readable problems; empty = valid.
 * Used by the content loader, the `content:check` script and the test-suite.
 */
export function validateCourse(course: Course): string[] {
  const problems: string[] = [];
  const unitIds = new Set<string>();
  const lessonIds = new Set<string>();

  for (const unit of course.units) {
    if (unitIds.has(unit.id)) problems.push(`duplicate unit id "${unit.id}"`);
    unitIds.add(unit.id);

    for (const lesson of unit.lessons) {
      if (lessonIds.has(lesson.id)) problems.push(`duplicate lesson id "${lesson.id}"`);
      lessonIds.add(lesson.id);

      for (const q of lesson.questions) {
        const at = `${q.id}`;
        switch (q.type) {
          case 'single':
          case 'multi': {
            const ids = q.options.map((o) => o.id);
            if (new Set(ids).size !== ids.length) problems.push(`${at}: duplicate option ids`);
            const correct = q.type === 'single' ? [q.answer] : q.answers;
            for (const a of correct) if (!ids.includes(a)) problems.push(`${at}: answer "${a}" is not an option`);
            if (new Set(correct).size !== correct.length) problems.push(`${at}: duplicate answers`);
            if (q.type === 'multi' && correct.length < 2) problems.push(`${at}: multi needs >= 2 correct answers`);
            break;
          }
          case 'fill': {
            const blanks = blankIndexes(q.template);
            const expected = q.answers.map((_, i) => i + 1);
            if (blanks.slice().sort((a, b) => a - b).join() !== expected.join())
              problems.push(`${at}: blanks [[n]] must be 1..${q.answers.length} exactly once each`);
            const pool = [...q.bank];
            for (const a of q.answers) {
              const i = pool.indexOf(a);
              if (i === -1) problems.push(`${at}: answer "${a}" missing from bank`);
              else pool.splice(i, 1);
            }
            if (pool.length === 0) problems.push(`${at}: bank has no distractors`);
            break;
          }
          case 'order': {
            if (new Set(q.lines).size !== q.lines.length)
              problems.push(`${at}: lines must be unique including indentation (otherwise order is ambiguous)`);
            break;
          }
          case 'open':
            break;
        }
      }
    }
  }
  return problems;
}

export function allQuestions(course: Course): Question[] {
  return course.units.flatMap((u) => u.lessons.flatMap((l) => l.questions));
}

export function findLesson(course: Course, lessonId: string): { unit: Unit; lesson: Lesson } | undefined {
  for (const unit of course.units) {
    const lesson = unit.lessons.find((l) => l.id === lessonId);
    if (lesson) return { unit, lesson };
  }
  return undefined;
}
