import { describe, expect, it } from 'vitest';
import {
  MAX_BOOKMARKS, MAX_NOTES, MAX_NOTE_CHARS, addDays, allQuestions, emptyProgress, findQuestion, isBookmarked, noteOf, questionStatus, searchLibrary, searchWords,
  setNote, toggleBookmark, type Course, type Question,
} from './index';
import { card, withCards } from './fixtures';

const today = '2026-11-04';

function q(id: string, over: Record<string, unknown> = {}): Question {
  return {
    id, type: 'single', prompt: `Prompt ${id}`, explanation: `Because ${id}`, difficulty: 'easy', tags: [],
    options: [{ id: 'a', text: 'A' }, { id: 'b', text: 'B' }], answer: 'a', ...over,
  } as unknown as Question;
}

const course = {
  units: [
    {
      id: 'kt', title: 'Kotlin Essentials', emoji: 'K', color: '#000', description: '',
      lessons: [
        {
          id: 'kt-1', title: 'Null safety',
          questions: [
            q('kt-1/1', { prompt: 'What does the `?.` operator do?', tags: ['null', 'operators'], explanation: 'Safe call short-circuits.' }),
            q('kt-1/2', { type: 'open', prompt: 'Explain lazy initialisation', explanation: undefined, modelAnswer: 'Use by lazy for thread-safe deferred init.', keyPoints: [], followUps: [], format: 'text' }),
          ],
        },
      ],
    },
    {
      id: 'sc', title: 'Scale', emoji: 'S', color: '#000', description: '',
      lessons: [{ id: 'sc-1', title: 'Resilience', questions: [q('sc-1/1', { type: 'spot', prompt: 'Find the retry storm', tags: ['retry'], explanation: 'Retries multiply.' }), q('sc-1/2')] }],
    },
  ],
} as unknown as Course;
const qs = allQuestions(course);
const ids = (r: { q: Question }[]) => r.map((x) => x.q.id);
const search = (p = emptyProgress(), f = {}) => searchLibrary(course, p, today, f);

describe('questionStatus', () => {
  it('new / due / learning / mastered', () => {
    const p = withCards(emptyProgress(), [
      [qs[0]!, { due: addDays(today, -1) }],
      [qs[1]!, { due: addDays(today, 5), interval: 5 }],
      [qs[2]!, { due: addDays(today, 40), interval: 30 }],
    ]);
    expect(qs.map((x) => questionStatus(p, x, today))).toEqual(['due', 'learning', 'mastered', 'new']);
  });
  it('a card of a rewritten question does not count', () => {
    const p = { ...emptyProgress(), cards: { 'kt-1/1': card(qs[0]!, { h: 'stale' }) } };
    expect(questionStatus(p, qs[0]!, today)).toBe('new');
  });
});

describe('bookmarks', () => {
  it('toggles on and off and survives the same prompt only', () => {
    let p = toggleBookmark(emptyProgress(), qs[0]!, today);
    expect(isBookmarked(p, qs[0]!)).toBe(true);
    expect(p.bookmarks['kt-1/1']!.day).toBe(today);
    expect(isBookmarked(p, { ...qs[0]!, prompt: 'rewritten' } as Question)).toBe(false);
    p = toggleBookmark(p, qs[0]!, today);
    expect(isBookmarked(p, qs[0]!)).toBe(false);
    expect(p.bookmarks).toEqual({});
  });
  it('a star made on an older wording shows as unstarred, and tapping it stars the current wording', () => {
    const p = { ...emptyProgress(), bookmarks: { 'kt-1/1': { h: 'stale', day: today } } };
    expect(isBookmarked(p, qs[0]!)).toBe(false);
    const next = toggleBookmark(p, qs[0]!, today);
    expect(isBookmarked(next, qs[0]!)).toBe(true);
    expect(Object.keys(next.bookmarks)).toEqual(['kt-1/1']);
  });
  it('stops adding at the cap but still removes', () => {
    const many = Object.fromEntries(Array.from({ length: MAX_BOOKMARKS }, (_, i) => [`x/${i}`, { h: 'h', day: today }]));
    const full = { ...emptyProgress(), bookmarks: many };
    expect(toggleBookmark(full, qs[0]!, today)).toBe(full);
    const { 'x/0': _drop, ...rest } = many;
    const starred = toggleBookmark({ ...emptyProgress(), bookmarks: rest }, qs[0]!, today); // 499 + 1 = at the cap
    expect(Object.keys(starred.bookmarks)).toHaveLength(MAX_BOOKMARKS);
    const removed = toggleBookmark(starred, qs[0]!, today);
    expect(Object.keys(removed.bookmarks)).toHaveLength(MAX_BOOKMARKS - 1);
    // a stale entry for the same question can be re-starred even at the cap
    const stale = { ...emptyProgress(), bookmarks: { ...rest, 'kt-1/1': { h: 'old', day: today } } };
    expect(isBookmarked(toggleBookmark(stale, qs[0]!, today), qs[0]!)).toBe(true);
  });
});

describe('notes', () => {
  it('saves trimmed, caps the length, edits, and deletes on empty text', () => {
    let p = setNote(emptyProgress(), qs[0]!, '  remember: ?. is a safe call  ', today);
    expect(noteOf(p, qs[0]!)).toBe('remember: ?. is a safe call');
    expect(setNote(p, qs[0]!, 'remember: ?. is a safe call', today)).toBe(p); // unchanged: no new object
    p = setNote(p, qs[0]!, 'x'.repeat(MAX_NOTE_CHARS + 50), today);
    expect(noteOf(p, qs[0]!)).toHaveLength(MAX_NOTE_CHARS);
    p = setNote(p, qs[0]!, '   ', today);
    expect(noteOf(p, qs[0]!)).toBeNull();
    expect(setNote(p, qs[0]!, '', today)).toBe(p);
  });
  it('is hidden when the prompt was rewritten, and a full notebook refuses new notes', () => {
    const p = setNote(emptyProgress(), qs[0]!, 'hello', today);
    expect(noteOf(p, { ...qs[0]!, prompt: 'changed' } as Question)).toBeNull();
    const many = Object.fromEntries(Array.from({ length: MAX_NOTES }, (_, i) => [`x/${i}`, { h: 'h', text: 't', day: today }]));
    const full = { ...emptyProgress(), notes: many };
    expect(setNote(full, qs[0]!, 'one more', today)).toBe(full);
    expect(noteOf(setNote({ ...full, notes: { ...many, 'kt-1/1': { h: 'old', text: 'old', day: today } } }, qs[0]!, 'edited', today), qs[0]!)).toBe('edited');
  });
});

describe('searchLibrary', () => {
  it('lists everything in course order without filters', () => {
    expect(ids(search())).toEqual(['kt-1/1', 'kt-1/2', 'sc-1/1', 'sc-1/2']);
    expect(search()[0]).toMatchObject({ unitTitle: 'Kotlin Essentials', lessonTitle: 'Null safety', status: 'new', bookmarked: false, note: null });
  });

  it('searches prompt, tags, explanation and the model answer, case-insensitively, ignoring backticks', () => {
    expect(ids(search(emptyProgress(), { text: 'OPERATOR' }))).toEqual(['kt-1/1']); // prompt
    expect(ids(search(emptyProgress(), { text: 'retry' }))).toEqual(['sc-1/1']); // tag + prompt
    expect(ids(search(emptyProgress(), { text: 'multiply' }))).toEqual(['sc-1/1']); // explanation
    expect(ids(search(emptyProgress(), { text: 'thread-safe' }))).toEqual(['kt-1/2']); // model answer of an open question
    expect(ids(search(emptyProgress(), { text: '?.' }))).toEqual(['kt-1/1']);
    expect(ids(search(emptyProgress(), { text: 'null safety' }))).toEqual(['kt-1/1', 'kt-1/2']); // lesson title
    expect(ids(search(emptyProgress(), { text: 'kotlin' }))).toEqual(['kt-1/1', 'kt-1/2']); // topic title
    expect(ids(search(emptyProgress(), { text: 'nothing matches this' }))).toEqual([]);
  });

  it('every word must match, and prompt hits rank above explanation hits', () => {
    expect(ids(search(emptyProgress(), { text: 'safe retry' }))).toEqual([]);
    const p = emptyProgress();
    const r = searchLibrary(
      { units: [{ ...course.units[0]!, lessons: [{ id: 'z', title: 'Z', questions: [q('z/1', { prompt: 'Something else', explanation: 'mentions caching' }), q('z/2', { prompt: 'All about caching' })] }] }] } as unknown as Course,
      p, today, { text: 'caching' },
    );
    expect(ids(r)).toEqual(['z/2', 'z/1']);
  });

  it('searches your own notes', () => {
    const p = setNote(emptyProgress(), qs[3]!, 'ask about idempotency tomorrow', today);
    expect(ids(search(p, { text: 'idempotency' }))).toEqual(['sc-1/2']);
  });

  it('filters by type, topic, bookmarked, has note, due and never seen', () => {
    let p = withCards(emptyProgress(), [[qs[0]!, { due: addDays(today, -2) }], [qs[2]!, { due: addDays(today, 3), interval: 3 }]]);
    p = toggleBookmark(p, qs[0]!, today);
    p = setNote(p, qs[2]!, 'note', today);
    expect(ids(search(p, { type: 'spot' }))).toEqual(['sc-1/1']);
    expect(ids(search(p, { unitId: 'sc' }))).toEqual(['sc-1/1', 'sc-1/2']);
    expect(ids(search(p, { bookmarked: true }))).toEqual(['kt-1/1']);
    expect(ids(search(p, { hasNote: true }))).toEqual(['sc-1/1']);
    expect(ids(search(p, { due: true }))).toEqual(['kt-1/1']);
    expect(ids(search(p, { neverSeen: true }))).toEqual(['kt-1/2', 'sc-1/2']);
    expect(ids(search(p, { neverSeen: true, unitId: 'sc', text: 'prompt' }))).toEqual(['sc-1/2']);
    expect(ids(search(p, { type: '', unitId: '' }))).toHaveLength(4);
  });

  it('searchWords and findQuestion', () => {
    expect(searchWords('  Hello   `World` ')).toEqual(['hello', 'world']);
    expect(searchWords(undefined)).toEqual([]);
    expect(findQuestion(course, 'sc-1/2')?.id).toBe('sc-1/2');
    expect(findQuestion(course, 'nope')).toBeUndefined();
  });
});
