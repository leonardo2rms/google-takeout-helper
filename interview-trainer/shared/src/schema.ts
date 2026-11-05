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
  /** Optional nudge that does not give the answer away. Revealing it costs XP and caps the review quality. */
  hint: z.string().min(1).max(220).optional(),
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
  /** `code` renders monospace lines; `steps` renders plain sentences (e.g. "put the saga steps in order"). */
  kind: z.enum(['code', 'steps']).default('code'),
  lines: z.array(z.string()).min(3).max(10),
});

/** Find the bug: tap the line(s) of a short snippet that cause the problem. */
export const SpotQuestion = z.object({
  type: z.literal('spot'),
  ...common,
  lang: z.string().default('kotlin'),
  /** The snippet, one line per row. Lines are numbered from 1. */
  source: z.string().min(1),
  /** 1-based numbers of the offending line(s); the learner must select exactly these. */
  answers: z.array(z.number().int().positive()).min(1).max(4),
});

/** Back-of-the-envelope numbers: the learner types a number, accepted within a relative tolerance. */
export const EstimateQuestion = z.object({
  type: z.literal('estimate'),
  ...common,
  answer: z.number().finite(),
  /** Relative tolerance as a fraction (0.05 = within 5%). Use 0 for an exact answer. */
  tolerance: z.number().min(0).max(0.5).default(0.05),
  /** Shown next to the input (e.g. "requests", "%", "MiB"). */
  unit: z.string().optional(),
});

/** Connect each item on the left to its description on the right. */
export const MatchQuestion = z.object({
  type: z.literal('match'),
  ...common,
  pairs: z.array(z.object({ left: z.string().min(1), right: z.string().min(1) })).min(3).max(6),
});

const KeyPoint = z.object({ point: z.string().min(1), keywords: z.array(z.string().min(1)).min(1) });

/**
 * An interviewer's follow-up to an open question. After the main answer, the app picks the first
 * follow-up that applies: `missed` ones probe a rubric point the learner did not cover, `covered`
 * ones push deeper on a point they did. Each is graded like an open question of its own.
 */
export const FollowUp = z.object({
  when: z.enum(['missed', 'covered']),
  /** 1-based index of the main question's key point this follow-up is about. */
  point: z.number().int().positive(),
  prompt: z.string().min(1),
  modelAnswer: z.string().min(1),
  keyPoints: z.array(KeyPoint).min(2).max(4),
  maxChars: z.number().int().positive().default(1000),
});

/** A value in a code test: JSON data, or `{ js: "expression" }` evaluated in the sandbox (for big inputs). */
const CodeArgs = z.union([z.array(z.unknown()), z.object({ js: z.string().min(1) })]);

export const CodeTest = z
  .object({
    args: CodeArgs,
    /** Expected return value (deep-equal). Omit when `throws` is set. */
    expect: z.unknown().optional(),
    /** The call must throw; the message must contain this text ("" accepts any error). */
    throws: z.string().optional(),
    /** Hidden tests only report pass/fail (use them for performance and edge cases). */
    hidden: z.boolean().default(false),
  })
  .refine((t) => t.throws !== undefined || t.expect !== undefined, { message: 'a test needs `expect` or `throws`' });

/**
 * Write a short function and run it against tests. The code runs in a sandboxed Web Worker in the
 * browser, so it works offline and on a phone. Challenges are JavaScript; `kotlin` shows the same
 * solution in the interview language afterwards.
 */
export const CodeQuestion = z.object({
  type: z.literal('code'),
  ...common,
  lang: z.literal('javascript').default('javascript'),
  starter: z.string().min(1),
  /** Name of the function the tests call. */
  entry: z.string().regex(/^[A-Za-z_$][\w$]*$/),
  tests: z.array(CodeTest).min(2).max(12),
  /** Reference solution: shown after the question and verified by the content tests. */
  solution: z.string().min(1),
  kotlin: z.string().optional(),
  /** Wall-clock budget for the whole run; exceeding it fails the attempt (infinite loop or too slow). */
  timeoutMs: z.number().int().min(300).max(10000).default(3000),
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
  keyPoints: z.array(KeyPoint).min(2).max(6),
  maxChars: z.number().int().positive().default(1200),
  /** Optional interviewer follow-ups, chosen by what the learner covered. See {@link FollowUp}. */
  followUps: z.array(FollowUp).max(4).default([]),
});

const RawQuestion = z.discriminatedUnion('type', [
  SingleQuestion,
  MultiQuestion,
  FillQuestion,
  OrderQuestion,
  SpotQuestion,
  EstimateQuestion,
  MatchQuestion,
  CodeQuestion,
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
export type SpotQ = Extract<Question, { type: 'spot' }>;
export type EstimateQ = Extract<Question, { type: 'estimate' }>;
export type MatchQ = Extract<Question, { type: 'match' }>;
export type CodeQ = Extract<Question, { type: 'code' }>;
export type FollowUp = z.output<typeof FollowUp>;
export type OpenQ = Extract<Question, { type: 'open' }>;
export type Lesson = z.output<typeof Lesson>;
export type Unit = z.output<typeof Unit>;
export type Course = z.output<typeof Course>;

export const BLANK = /\[\[(\d+)\]\]/g;

/** The rows of a `spot` snippet (trailing newlines dropped). */
export const spotLines = (source: string): string[] => source.replace(/\n+$/, '').split('\n');

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
          case 'spot': {
            const lines = spotLines(q.source);
            if (new Set(q.answers).size !== q.answers.length) problems.push(`${at}: duplicate answer lines`);
            for (const n of q.answers) {
              if (n > lines.length) problems.push(`${at}: answer line ${n} is past the end (${lines.length} lines)`);
              else if (lines[n - 1]!.trim() === '') problems.push(`${at}: answer line ${n} is blank`);
            }
            if (lines.length < 3 || lines.length > 20) problems.push(`${at}: spot snippets need 3-20 lines`);
            break;
          }
          case 'estimate':
            if (q.answer === 0) problems.push(`${at}: estimate answer must be non-zero (tolerance is relative)`);
            break;
          case 'match': {
            if (new Set(q.pairs.map((p) => p.left)).size !== q.pairs.length) problems.push(`${at}: duplicate left items`);
            if (new Set(q.pairs.map((p) => p.right)).size !== q.pairs.length) problems.push(`${at}: duplicate right items (matches would be ambiguous)`);
            break;
          }
          case 'code':
            if (!q.tests.some((t) => !t.hidden)) problems.push(`${at}: needs at least one visible test`);
            for (const src of [q.starter, q.solution])
              if (!new RegExp(`(function\\s+${q.entry.replace(/\$/g, '\\$')}\\b|(const|let|var)\\s+${q.entry.replace(/\$/g, '\\$')}\\b)`).test(src))
                problems.push(`${at}: starter and solution must define \`${q.entry}\``);
            break;
          case 'open':
            for (const [i, f] of q.followUps.entries())
              if (f.point > q.keyPoints.length) problems.push(`${at}: follow-up ${i + 1} refers to key point ${f.point}, but there are ${q.keyPoints.length}`);
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
