/**
 * The route of a `location.hash`: no leading `#`, one slash between segments, no trailing slash, no query or fragment
 * part. `#/library/`, `#//`, and `#/lesson/kotlin-null-types?x=1` then reach the same screens as their plain forms
 * instead of silently showing Home or "not found" (QA S2-17).
 */
export function routeOf(hash: string): string {
  const path = (hash.replace(/^#/, '').split(/[?#]/)[0] ?? '').replace(/\/{2,}/g, '/').replace(/\/+$/, '');
  return path === '' ? '/' : path.startsWith('/') ? path : `/${path}`;
}
