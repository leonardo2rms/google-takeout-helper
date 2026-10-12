import { useState } from 'react';
import type { OpenQ } from '@trainer/shared';
import type { Answer } from '../lib/evaluate';

interface Props {
  q: OpenQ;
  checked: boolean;
  aiAvailable: boolean;
  onAnswer: (a: Answer | null) => void;
}

export function OpenQuestion({ q, checked, aiAvailable, onAnswer }: Props) {
  const [text, setText] = useState('');
  const isCode = q.format === 'code';

  return (
    <div className="open">
      <textarea
        className={`answer-box ${isCode ? 'mono' : ''}`}
        value={text}
        rows={isCode ? 9 : 6}
        maxLength={q.maxChars}
        disabled={checked}
        spellCheck={!isCode}
        autoCapitalize={isCode ? 'off' : 'sentences'}
        autoCorrect={isCode ? 'off' : 'on'}
        placeholder={isCode ? '// write your solution' : 'Type your answer. Be concise, as in an interview.'}
        aria-label="Your answer"
        onChange={(e) => {
          setText(e.target.value);
          onAnswer(e.target.value.trim().length >= 3 ? { type: 'open', text: e.target.value } : null);
        }}
      />
      <div className="open-meta">
        <span>{aiAvailable ? '🤖 Graded by AI against a rubric' : '📴 Offline: you will self-check against the model answer'}</span>
        <span>{text.length}/{q.maxChars}</span>
      </div>
    </div>
  );
}
