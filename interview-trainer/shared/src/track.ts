import type { RoleId } from './progress';
import type { Course, Lesson } from './schema';

/**
 * The units each target role works through first, by unit id. Order inside a track is the course order, so unit and lesson
 * numbers stay the same for everyone; the role only decides which units come first and which weigh more. `generalist` (and
 * no role at all) has no focus: the whole course is one track in course order.
 */
export const ROLE_FOCUS: Record<RoleId, readonly string[]> = {
  'backend-kotlin': ['kotlin', 'coroutines', 'spring', 'jvm', 'data', 'messaging', 'testing'],
  'staff-architect': ['ddd', 'scale', 'complexity', 'ai', 'system-design', 'security-observability'],
  generalist: [],
};

export const ROLE_LABEL: Record<RoleId, string> = {
  'backend-kotlin': 'Backend / Kotlin',
  'staff-architect': 'Staff / architect',
  generalist: 'Generalist',
};

/** What a role's hint says on the onboarding card. */
export const ROLE_HINT: Record<RoleId, string> = {
  'backend-kotlin': 'Kotlin, Spring, JVM, data and testing first',
  'staff-architect': 'DDD, scale, system design and AI first',
  generalist: 'Everything equally',
};

/** How much more a focus unit counts: in the readiness score, and in how often mixed practice and mocks draw from it. */
export const FOCUS_WEIGHT = 1.5;

/** Unit weights for a role (default 1 for units not listed). */
export function roleWeights(role: RoleId | null): Record<string, number> {
  return Object.fromEntries((role ? ROLE_FOCUS[role] : []).map((id) => [id, FOCUS_WEIGHT]));
}

export const isFocusUnit = (role: RoleId | null, unitId: string): boolean => (role ? ROLE_FOCUS[role].includes(unitId) : false);

/** Whether the role has a track at all (a generalist or no role does not: nothing is moved, nothing is labelled). */
export const hasTrack = (role: RoleId | null): role is RoleId => role !== null && ROLE_FOCUS[role].length > 0;

/** The units in the order the learner meets them: the role's focus units first, then the rest; each group in course order. */
export function trackOrder<T extends { id: string }>(units: readonly T[], role: RoleId | null): T[] {
  if (!hasTrack(role)) return [...units];
  return [...units.filter((u) => isFocusUnit(role, u.id)), ...units.filter((u) => !isFocusUnit(role, u.id))];
}

/** The first lesson not yet done, in track order: where "Start" and "Up next" lead. */
export function nextLessonFor(course: Course, role: RoleId | null, done: Readonly<Record<string, unknown>>, skip?: string): Lesson | undefined {
  return trackOrder(course.units, role)
    .flatMap((u) => u.lessons)
    .find((l) => !done[l.id] && l.id !== skip);
}
