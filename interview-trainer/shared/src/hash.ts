import type { Question } from './schema';

/** 32-bit FNV-1a, unsigned. Stable across platforms: used for card hashes and deterministic "dice". */
export function fnv1a(s: string): number {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

const cache = new WeakMap<Question, string>();

/**
 * Identifies *what a question asks*, not where it sits in the YAML. Question ids are positional (`<lesson>/<n>`),
 * so a card stores this hash and is dropped when the prompt behind its id changes.
 */
export function questionHash(q: Question): string {
  let h = cache.get(q);
  if (h === undefined) {
    h = fnv1a(`${q.type}|${q.prompt}`).toString(16);
    cache.set(q, h);
  }
  return h;
}
