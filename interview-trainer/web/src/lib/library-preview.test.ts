import { describe, expect, it } from 'vitest';
import type { Question } from '@trainer/shared';
import { codePreview } from './library-preview';

const base = { explanation: '', difficulty: 'easy', tags: [] as string[] };
const single = (code?: string): Question =>
  ({ ...base, id: 'a/1', type: 'single', prompt: 'What does this print?', options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a', ...(code ? { code: { lang: 'kotlin', source: code } } : {}) }) as unknown as Question;

describe('codePreview', () => {
  it('shows nothing for a question without code', () => {
    expect(codePreview(single())).toEqual([]);
  });
  it('takes the first two meaningful lines, de-indented, skipping blanks and bare braces', () => {
    const q = single('\n    fun main() {\n\n        val x = listOf(1, 2)\n        println(x)\n    }\n');
    expect(codePreview(q)).toEqual(['fun main() {', '    val x = listOf(1, 2)']);
  });
  it('cuts long lines', () => {
    const [line] = codePreview(single(`val s = "${'x'.repeat(200)}"`));
    expect(line).toHaveLength(72);
    expect(line!.endsWith('…')).toBe(true);
  });
  it('previews the template of a fill question with its blanks as ___', () => {
    const q = { ...base, id: 'a/2', type: 'fill', prompt: 'p', template: 'val x = [[1]]()\nreturn [[2]]', lang: 'kotlin', answers: ['a', 'b'], bank: [] } as unknown as Question;
    expect(codePreview(q)).toEqual(['val x = ___()', 'return ___']);
  });
  it('previews the source of a spot question, the lines of a code order question and the starter of a code question', () => {
    const spot = { ...base, id: 'a/3', type: 'spot', prompt: 'p', source: 'a()\nb()\nc()', lang: 'kotlin', answers: [1] } as unknown as Question;
    expect(codePreview(spot)).toEqual(['a()', 'b()']);
    const order = { ...base, id: 'a/4', type: 'order', prompt: 'p', kind: 'code', lines: ['one()', 'two()', 'three()'], lang: 'kotlin' } as unknown as Question;
    expect(codePreview(order)).toEqual(['one()', 'two()']);
    const steps = { ...order, kind: 'steps' } as unknown as Question;
    expect(codePreview(steps)).toEqual([]);
    const code = { ...base, id: 'a/5', type: 'code', prompt: 'p', starter: 'function f(x) {\n  // TODO\n}', tests: [], solution: '' } as unknown as Question;
    expect(codePreview(code)).toEqual(['function f(x) {', '  // TODO']);
  });
});
