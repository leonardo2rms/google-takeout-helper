import { useSyncExternalStore } from 'react';
import course from 'virtual:course';
import { RESUME_KEY, RESUME_MAX_AGE_MS, allQuestions, readResume, serializeResume, toResume, type Resumable, type ResumeMeta, type Session } from '@trainer/shared';

/**
 * The session the learner left half way, kept in its own storage key (see shared/src/resume.ts for the record and its
 * validation). This is scratch state, not progress: if it cannot be read or written the only cost is that the session
 * starts again, so nothing here raises the data-safety banners. One session at a time.
 */

const questions = new Map(allQuestions(course).map((q) => [q.id, q] as const));

/** Used instead of localStorage when it is unavailable or refuses a write (private windows, sandboxed frames): this tab only. */
let memory: string | null = null;
let useMemory = false;

function readRaw(): string | null {
  if (useMemory) return memory;
  try {
    return window.localStorage.getItem(RESUME_KEY);
  } catch {
    useMemory = true;
    return memory;
  }
}

function writeRaw(text: string | null): void {
  if (!useMemory) {
    try {
      if (text === null) window.localStorage.removeItem(RESUME_KEY);
      else window.localStorage.setItem(RESUME_KEY, text);
      return;
    } catch {
      useMemory = true; // from now on this tab keeps it in memory, so a read does not return an older stored copy
    }
  }
  memory = text;
}

const listeners = new Set<() => void>();
const emit = () => listeners.forEach((l) => l());

let cachedRaw: string | null | undefined;
let cached: Resumable | null = null;

/** The saved session, if there is one that is recent and still matches the course. Cached per stored text, so the same object comes back until it changes. */
export function loadResume(now: number = Date.now()): Resumable | null {
  const raw = readRaw();
  if (raw !== cachedRaw) {
    cachedRaw = raw;
    cached = readResume(raw, questions, now);
  }
  return cached && now - cached.record.savedAt <= RESUME_MAX_AGE_MS ? cached : null;
}

/** The saved session if it belongs to `route` (opening the same lesson or practice again carries on from it). */
export function resumeFor(route: string): Resumable | null {
  const r = loadResume();
  return r && r.record.route === route ? r : null;
}

/** Saves the session after a question was answered. Nothing is stored for a session with nothing answered or nothing left. */
export function saveResume(session: Session, meta: ResumeMeta): void {
  const record = toResume(session, meta, Date.now());
  if (!record) return;
  writeRaw(serializeResume(record));
  emit();
}

/** Forgets the saved session: because it was finished, or discarded, or the profile it belonged to was reset. With `route`, only if it is that session's. */
export function clearResume(route?: string): void {
  if (route !== undefined && loadResume()?.record.route !== route) return;
  if (readRaw() === null) return;
  writeRaw(null);
  emit();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  const onStorage = (e: StorageEvent) => {
    if (e.key === RESUME_KEY || e.key === null) listener(); // another tab saved, finished or discarded a session
  };
  window.addEventListener('storage', onStorage);
  return () => {
    listeners.delete(listener);
    window.removeEventListener('storage', onStorage);
  };
}

const snapshot = () => loadResume();

/** The saved session, kept up to date (Home shows it as a card). */
export function useResume(): Resumable | null {
  return useSyncExternalStore(subscribe, snapshot, snapshot);
}
