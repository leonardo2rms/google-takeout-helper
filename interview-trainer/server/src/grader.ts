import Anthropic from '@anthropic-ai/sdk';
import { zodOutputFormat } from '@anthropic-ai/sdk/helpers/zod';
import { z } from 'zod/v4';
import { SYSTEM_PROMPT, buildUserPrompt, type GradeResponse, type OpenQ } from '@trainer/shared';

export { SYSTEM_PROMPT, buildUserPrompt, sanitizeAnswer } from '@trainer/shared';

export interface Grader {
  readonly model: string;
  grade(question: OpenQ, answer: string): Promise<GradeResponse>;
}

/** What we ask the model to return: one verdict per rubric point plus feedback. */
const Verdict = z.object({
  points: z.array(z.object({ covered: z.boolean() })),
  feedback: z.string(),
});

export interface AnthropicGraderOptions {
  model?: string;
  effort?: 'low' | 'medium' | 'high';
  client?: Pick<Anthropic, 'messages'>;
}

export class AnthropicGrader implements Grader {
  readonly model: string;
  private readonly client: Pick<Anthropic, 'messages'>;
  private readonly effort: 'low' | 'medium' | 'high';

  constructor(opts: AnthropicGraderOptions = {}) {
    this.model = opts.model ?? 'claude-opus-5-5';
    this.effort = opts.effort ?? 'low';
    this.client = opts.client ?? new Anthropic({ maxRetries: 2, timeout: 60_000 });
  }

  async grade(q: OpenQ, answer: string): Promise<GradeResponse> {
    const response = await this.client.messages.parse({
      model: this.model,
      max_tokens: 8000,
      system: SYSTEM_PROMPT,
      messages: [{ role: 'user', content: buildUserPrompt(q, answer) }],
      output_config: { effort: this.effort, format: zodOutputFormat(Verdict) },
    });

    if (response.stop_reason === 'refusal') throw new Error('The model declined to grade this answer');
    const parsed = response.parsed_output;
    if (!parsed) throw new Error('The model returned an unparseable verdict');
    if (parsed.points.length !== q.keyPoints.length)
      throw new Error(`Expected ${q.keyPoints.length} rubric verdicts, got ${parsed.points.length}`);

    const covered = parsed.points.map((p) => p.covered);
    return {
      score: covered.filter(Boolean).length / covered.length,
      covered,
      feedback: parsed.feedback.trim(),
      graded_by: 'ai',
    };
  }
}
