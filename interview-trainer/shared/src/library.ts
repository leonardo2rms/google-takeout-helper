import type { Day } from './day';
import { MAX_BOOKMARKS, MAX_NOTES, MAX_NOTE_CHARS } from './extras';
import { questionHash } from './hash';
import { MASTERED_INTERVAL, type Progress } from './progress';
import { allQuestions, type Course, type Question } from './schema';
import { cardFor, isDue } from './srs';

export type QuestionStatus = 'new' | 'due' | 'learning' | 'mastered';

export const STATUS_LABEL: Record<QuestionStatus, string> = { new: 'New', due: 'Due', learning: 'Learning', mastered: 'Mastered' };

export const TYPE_LABEL: Record<Question['type'], string> = {
  single: 'Single choice',
  multi: 'Multiple choice',
  fill: 'Fill the blanks',
  order: 'Put in order',
  spot: 'Spot the bug',
  estimate: 'Estimate',
  match: 'Match pairs',
  code: 'Write code',
  open: 'Open answer',
};

/** Never seen: no card. Due: the review date has come. Mastered: scheduled 3+ weeks out. Otherwise learning. */
export function questionStatus(p: Progress, q: Question, today: Day): QuestionStatus {
  const c = cardFor(p, q);
  if (!c) return 'new';
  if (isDue(c, today)) return 'due';
  return c.interval >= MASTERED_INTERVAL ? 'mastered' : 'learning';
}

// ---------- bookmarks and notes ----------

/** Starred? A star made on an older wording of the prompt no longer applies. */
export const isBookmarked = (p: Pick<Progress, 'bookmarks'>, q: Question): boolean => p.bookmarks[q.id]?.h === questionHash(q);

/** The private note for `q`, or null (also when the prompt was rewritten since the note was written). */
export function noteOf(p: Pick<Progress, 'notes'>, q: Question): string | null {
  const n = p.notes[q.id];
  return n && n.h === questionHash(q) ? n.text : null;
}

/** Adds or removes the star. A new star is ignored once MAX_BOOKMARKS are stored. */
export function toggleBookmark(p: Progress, q: Question, today: Day): Progress {
  const bookmarks = { ...p.bookmarks };
  if (isBookmarked(p, q)) delete bookmarks[q.id];
  else if (q.id in bookmarks || Object.keys(bookmarks).length < MAX_BOOKMARKS) bookmarks[q.id] = { h: questionHash(q), day: today };
  else return p;
  return { ...p, bookmarks };
}

/** Saves a note (trimmed, at most MAX_NOTE_CHARS). Empty text deletes it. Ignored for a new note once MAX_NOTES exist. */
export function setNote(p: Progress, q: Question, text: string, today: Day): Progress {
  const clean = text.trim().slice(0, MAX_NOTE_CHARS).trim();
  const notes = { ...p.notes };
  if (clean === '') {
    if (!(q.id in notes)) return p;
    delete notes[q.id];
  } else {
    if (!(q.id in notes) && Object.keys(notes).length >= MAX_NOTES) return p;
    if (notes[q.id]?.text === clean && notes[q.id]?.h === questionHash(q)) return p;
    notes[q.id] = { h: questionHash(q), text: clean, day: today };
  }
  return { ...p, notes };
}

// ---------- search ----------

export interface LibraryFilters {
  /** Free text: every word must appear in the prompt, tags, explanation, topic or your note. */
  text?: string;
  type?: Question['type'] | '';
  unitId?: string;
  bookmarked?: boolean;
  hasNote?: boolean;
  due?: boolean;
  neverSeen?: boolean;
}

export interface LibraryEntry {
  q: Question;
  unitId: string;
  unitTitle: string;
  lessonId: string;
  lessonTitle: string;
  status: QuestionStatus;
  bookmarked: boolean;
  note: string | null;
}

const plain = (s: string): string => s.toLowerCase().replace(/[`*_]/g, '').replace(/\s+/g, ' ');

/** The searchable text of a question: its prompt, the explanation (the model answer for open questions) and its tags. */
function fields(q: Question): { prompt: string; tags: string; body: string } {
  const body = q.type === 'open' ? `${q.explanation ?? ''} ${q.modelAnswer}` : (q.explanation ?? '');
  return { prompt: plain(q.prompt), tags: plain(q.tags.join(' ')), body: plain(body) };
}

export const searchWords = (text: string | undefined): string[] => plain(text ?? '').split(' ').filter((w) => w.length > 0);

/**
 * Full-text search and filters over the whole course. Every word of `text` has to match somewhere; prompt matches
 * rank above tag matches, above explanation / topic / note matches. Without text the course order is kept.
 */
export function searchLibrary(course: Course, p: Progress, today: Day, f: LibraryFilters = {}): LibraryEntry[] {
  const words = searchWords(f.text);
  const out: { e: LibraryEntry; score: number; order: number }[] = [];
  let order = 0;
  for (const unit of course.units) {
    if (f.unitId && unit.id !== f.unitId) continue;
    for (const lesson of unit.lessons) {
      for (const q of lesson.questions) {
        order++;
        if (f.type && q.type !== f.type) continue;
        const bookmarked = isBookmarked(p, q);
        const note = noteOf(p, q);
        const status = questionStatus(p, q, today);
        if (f.bookmarked && !bookmarked) continue;
        if (f.hasNote && note === null) continue;
        if (f.due && status !== 'due') continue;
        if (f.neverSeen && status !== 'new') continue;

        let score = 0;
        if (words.length > 0) {
          const t = fields(q);
          const extra = plain(`${unit.title} ${lesson.title} ${note ?? ''}`);
          let all = true;
          for (const w of words) {
            if (t.prompt.includes(w)) score += 3;
            else if (t.tags.includes(w)) score += 2;
            else if (t.body.includes(w) || extra.includes(w)) score += 1;
            else {
              all = false;
              break;
            }
          }
          if (!all) continue;
        }
        out.push({ e: { q, unitId: unit.id, unitTitle: unit.title, lessonId: lesson.id, lessonTitle: lesson.title, status, bookmarked, note }, score, order });
      }
    }
  }
  if (words.length > 0) out.sort((a, b) => b.score - a.score || a.order - b.order);
  return out.map((x) => x.e);
}

export const findQuestion = (course: Course, id: string): Question | undefined => allQuestions(course).find((q) => q.id === id);
