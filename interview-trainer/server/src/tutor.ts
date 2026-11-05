import Anthropic from '@anthropic-ai/sdk';
import { buildExplainPrompt, cleanReply, type ExplainMessage, type Question } from '@trainer/shared';

/** The "Ask why" tutor. The route looks the question up by id; the learner's text is untrusted. */
export interface Tutor {
  readonly model: string;
  explain(q: Question, answer: string, correct: boolean | undefined, messages: readonly ExplainMessage[]): Promise<string>;
}

export interface AnthropicTutorOptions {
  model?: string;
  effort?: 'low' | 'medium' | 'high';
  client?: Pick<Anthropic, 'messages'>;
}

export class AnthropicTutor implements Tutor {
  readonly model: string;
  private readonly client: Pick<Anthropic, 'messages'>;
  private readonly effort: 'low' | 'medium' | 'high';

  /** Same client setup (and default model) as the grader; EXPLAIN_MODEL can override just the tutor. */
  constructor(opts: AnthropicTutorOptions = {}) {
    this.model = opts.model ?? 'claude-opus-5-5';
    this.effort = opts.effort ?? 'low';
    this.client = opts.client ?? new Anthropic({ maxRetries: 2, timeout: 60_000 });
  }

  async explain(q: Question, answer: string, correct: boolean | undefined, messages: readonly ExplainMessage[]): Promise<string> {
    const { system, user } = buildExplainPrompt(q, answer, correct, messages);
    const response = await this.client.messages.create({
      model: this.model,
      max_tokens: 4000,
      system,
      messages: [{ role: 'user', content: user }],
      output_config: { effort: this.effort },
    });
    if (response.stop_reason === 'refusal') throw new Error('The model declined to answer');
    const text = response.content
      .map((b) => (b.type === 'text' ? b.text : ''))
      .join('')
      .trim();
    if (!text) throw new Error('The model returned no text');
    return cleanReply(text);
  }
}
