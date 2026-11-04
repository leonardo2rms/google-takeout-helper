/** Wire types for the AI grading endpoint, shared by server and web. */
import { z } from 'zod';

export const GradeRequest = z.object({
  questionId: z.string().min(1).max(200),
  answer: z.string().max(10_000),
});
export type GradeRequest = z.infer<typeof GradeRequest>;

export interface GradeResponse {
  /** 0..1, share of rubric key points covered. */
  score: number;
  /** One entry per rubric key point, in order. */
  covered: boolean[];
  /** Short, specific feedback addressed to the learner. */
  feedback: string;
  graded_by: 'ai';
}

export interface HealthResponse {
  ok: true;
  /** True when the server has credentials and can grade open questions with AI. */
  ai: boolean;
  model?: string;
}
