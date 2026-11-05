import { describe, expect, it } from 'vitest';
import { Unit } from './schema';

const lesson = (n: number) => ({
  id: 'l',
  title: 'L',
  questions: Array.from({ length: 3 }, (_, i) => ({ type: 'single', prompt: `p${n}${i}`, options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a', explanation: 'x' })),
});
const unit = (extra: Record<string, unknown>) => ({ id: 'u', title: 'U', description: 'd', lessons: [lesson(1)], ...extra });

describe('unit schema: icon, with emoji and color kept optional', () => {
  it('accepts an icon name and nothing else', () => {
    const u = Unit.parse(unit({ icon: 'braces' }));
    expect(u.icon).toBe('braces');
    expect(u.emoji).toBeUndefined();
    expect(u.color).toBeUndefined();
  });

  it('still accepts the legacy emoji and color fields (older course files)', () => {
    const u = Unit.parse(unit({ emoji: 'K', color: '#7f52ff' }));
    expect(u.emoji).toBe('K');
    expect(u.color).toBe('#7f52ff');
    expect(u.icon).toBeUndefined();
  });

  it('accepts a unit with none of them', () => {
    expect(() => Unit.parse(unit({}))).not.toThrow();
  });

  it('rejects an icon that is not a kebab-case name', () => {
    expect(() => Unit.parse(unit({ icon: 'Not An Icon' }))).toThrow();
    expect(() => Unit.parse(unit({ icon: '' }))).toThrow();
  });
});
