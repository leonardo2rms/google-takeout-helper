import { describe, expect, it } from 'vitest';
import { routeOf } from './route';

describe('routeOf', () => {
  it.each([
    ['', '/'],
    ['#', '/'],
    ['#/', '/'],
    ['#//', '/'],
    ['#/library', '/library'],
    ['#/library/', '/library'],
    ['#/mock/', '/mock'],
    ['#//library//', '/library'],
    ['#/lesson/kotlin-null-types', '/lesson/kotlin-null-types'],
    ['#/lesson/kotlin-null-types?x=1', '/lesson/kotlin-null-types'],
    ['#/lesson/kotlin-null-types/#top', '/lesson/kotlin-null-types'],
    ['#/q/kotlin-null-types/3', '/q/kotlin-null-types/3'],
    ['#library', '/library'],
    ['#?only=query', '/'],
  ])('%j -> %j', (hash, route) => {
    expect(routeOf(hash)).toBe(route);
  });

  it('is idempotent', () => {
    for (const h of ['#/a//b/', '#//', '#/x?y']) expect(routeOf(`#${routeOf(h)}`)).toBe(routeOf(h));
  });
});
