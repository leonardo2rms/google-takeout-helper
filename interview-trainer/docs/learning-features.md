# Learning features: hints, confidence, library, daily challenge, badges, mock interviews

All of it is local and offline; none of it needs the server. The pure logic is in `shared/src/` (injected clock, unit-tested),
the screens in `web/src/`. The same ethics rules as in [`engagement.md`](engagement.md) apply: no guilt copy, no fake urgency,
nothing is ever taken away, every effect respects `prefers-reduced-motion`.

## Progress fields (no version bump)

These are **additive**: stored data without them loads with defaults (`normalizeExtras`), so `Progress` stays `version: 2` and
needs no migration. They live in `shared/src/extras.ts` (`ProgressExtras`, `emptyExtras`, `normalizeExtras`, `mergeExtras`), which
`emptyProgress`, `normalizeProgress` and `mergeProgress` call with one line each. Backups, imports and merges carry all of them.

| Field | Meaning |
|---|---|
| `calibration {n[3], right[3]}` | ratings given per level (guess / think so / sure) and how many were right first try |
| `bookmarks {qid -> {h, day}}`, `notes {qid -> {h, text, day}}` | stars and private notes; `h` is the question hash, so an entry made on an older wording of a prompt stops applying instead of attaching to a different question (it is kept, in case the text is restored). Caps: 500 stars, 300 notes of 500 characters |
| `challenge {day -> 'pass'\|'fail'}` | daily challenge results (400 days) |
| `achievements {badgeId -> day}` | unlocked badges; never removed |
| `mocks MockRecord[]` | the last 10 mock interviews, newest first |
| `passedByType {type -> n}` | questions answered correctly per format (behind some badges) |
| `cards[].hc` | optional flag on a spaced-repetition card: last answered wrong while "sure" |
| `milestones['ach:init']` | set once the one-time silent badge backfill has run |

## Hints

`hint:` is an optional field on every question type (`schema.ts`, at most 220 characters). In a lesson a **Hint** button appears only
when one exists; one reveal per question (it stays visible on a retry). Revealing costs `HINT_COST = 3` XP off that question
(never below `XP_MIN = 1`) and caps the review grade at `hard` (it was not recalled unaided, whatever the confidence). 92 questions
across all nine types have a hint. `content/hints.test.ts` checks that a hint is short, differs from the explanation and does not
paste a choice or a fill token. Author hints as a nudge towards the way of thinking ("which object receives the call?"), never the answer.

## Confidence ("How sure are you?")

Off by default (Settings, "Ask How sure are you?"). Three chips above Check: Guess / Think so / Sure; skippable, tap again to clear.
It is asked **before** the answer is checked, because after the feedback a rating is only hindsight, and not for `open` / `code`
(the grader is the signal) nor on retries. Only the first attempt's rating counts.

- Wrong while *sure* (hypercorrection): the card is due again **today** (a second look hours later), ease drops 0.35 instead of 0.2, it is flagged `hc` and sorted first in the due queue until the next success. The feedback says "Worth a closer look: you were sure."
- Right while *guessing*: graded `hard`, so the interval stays short. Right and *sure*: `easy`.
- Progress shows "When you said Sure you were right 86% of the time (44 ratings)" once there are 10+ ratings at that level.

## Bookmarks, notes and the Library

Under every lesson feedback: **Save** (star) and **Add note**. `#/library` lists every question with full-text search (all words must
match in the prompt, tags, explanation or model answer, topic and lesson titles, or your note; prompt hits rank first) and filters: type,
topic, Saved, Has note, Due, Never seen. Each row shows its status (New / Due / Learning / Mastered). **Practise this one** opens
`#/q/<question id>`, a session of one question. Single-question sessions (this and the challenge) never count as a "perfect lesson"
and get no perfect bonus. Pure functions: `shared/src/library.ts`.

## Daily challenge

`challengeQuestion(course, day)` is a pure function of the local date and the course: no backend, the same question for everybody on the
same content, never repeats yesterday's (the pick is walked from a fixed start). Weights favour spot-the-bug and estimates; `match` is
left out. `#/challenge` plays it as a session of one; a first-try pass earns `+15 XP` once. The challenge has its **own streak** of
consecutive days taken (pass or fail, so a hard question never costs it; it is not broken until the day has passed). A replay after
the first result is plain practice with no XP.

## Achievements

27 badges in `shared/src/achievements.ts`, each a pure test over Progress (`Facts`). They reward learning behaviours: first lesson,
perfect lessons, 7/30 days in a row, ten or thirty days practised, first code challenge / bug / estimate / open answer, all nine formats,
a mastered topic, 50 / 250 reviews, 100 questions, mock done / strong mock, five saved questions, a first note, the daily challenge and a
challenge week, "welcome back" after a week away. There is deliberately no badge for "never missing a day". The store evaluates badges after
every write; a new one raises one calm toast (never blocks taps, slides in only when motion is allowed, dismissible, auto-hides after
6 s). Profiles from before badges (and imports) get what they already earned **silently**, once. The trophy case is on the Progress screen.

## Mock interview

`#/mock`. Setup: 10 or 20 questions, a time limit (default 15 / 30 minutes), optional topic. `buildMock` draws without replacement weighted
by weak topics and weak questions, with a penalty for formats already chosen and caps for `code` (about 1 in 10) and `open` (about 1 in 5),
then interleaves topics. While it runs: the timer is always visible (timestamp-based, pauses while the app is hidden, announced politely at
5 and 1 minutes), questions can be flagged, skipped and revisited from a numbered palette (answers survive), and there is **no feedback of
any kind** (code questions have no "Run tests"). At zero the answers are submitted automatically. Marking: choice-type questions locally,
code in the worker, open questions by the AI when available, otherwise by the learner on the results screen (the score updates as boxes are ticked).

Results: overall score (unanswered counts as zero), a scorecard by topic with weak spots (below 60%), and every question with its explanation.
**Save results** then writes: wrong answers become due cards tomorrow, right ones are scheduled like first-try answers, unanswered ones are left alone
(no penalty for not reaching them), the mock joins the history (last 10, shown on the setup screen and on Progress).

Rewards are capped so a mock cannot inflate the daily goal: only the first **2 mocks of a day** earn anything, at most **40 XP** (4 per answer), and
the day's goal credit is at most **half of the daily goal**. A mock is a practice day for the streak, never a "perfect lesson", never shows a tip.
Quitting mid-run asks first and saves nothing; a reload mid-run discards it.

## Resuming an unfinished session

Leaving a lesson or a practice session half way (the X in the header, or the browser's Back button) saves it. Home then shows a **Resume** card ("Carry on · 2 of 8 done") with a **Discard** link that asks first. Opening the same lesson or practice again also carries on from where the learner was, with the same questions even when what the app would pick today has changed.

- **What is kept:** the questions still to ask, XP so far, misses (a missed question still waits at the end), hints and "How sure are you?" ratings. **What is not:** the question on screen (it is asked again, also when it was already checked but Continue was not pressed) and a half-done interviewer follow-up.
- **When it is forgotten:** when the lesson is finished, discarded, or the progress is reset or replaced by an import; after 48 hours; or when the course changed under it (a question gone, moved or reworded).
- **One at a time.** Starting something else and answering a question replaces the saved session. One-question sessions (Library practice, the daily challenge) and the mock interview are never saved.
- **Nothing is counted early.** XP, review cards, the streak and badges are settled only when a session finishes, exactly as before.

Code: `shared/src/resume.ts` (record, validation against the course), `web/src/lib/resume.ts` (storage key, `useResume`), the save in `screens/Lesson.tsx`, `SessionScreen` in `App.tsx`, the card in `screens/Home.tsx`. Tests: `shared/src/resume.test.ts`, `web/src/lib/resume.test.ts`, `e2e/resume.spec.ts`.

## Where things are

| Concern | Files |
|---|---|
| Hints, XP | `session.ts` (`HINT_COST`, `xpFor`), `srs.ts` (`gradeFor`), `web/src/components/Hint.tsx` |
| Confidence | `srs.ts` (`isHypercorrection`, `hc`), `calibration.ts`, `ConfidenceRow.tsx`, `Calibration.tsx` |
| Library | `library.ts`, `screens/Library.tsx`, `QuestionTools.tsx` |
| Challenge | `challenge.ts`, `ChallengeCard.tsx`, route `#/challenge` in `App.tsx` |
| Badges | `achievements.ts`, `lib/store.ts` (evaluation), `lib/toasts.ts`, `AchievementToasts.tsx`, `TrophyCase.tsx` |
| Mock | `mock.ts`, `lib/mock-marking.ts`, `screens/Mock.tsx`, `MockRun.tsx`, `MockResults.tsx`, `MockHistory.tsx` |
| Resume | `resume.ts`, `lib/resume.ts`, `screens/Lesson.tsx` (save), `App.tsx` (`SessionScreen`), `screens/Home.tsx` (card) |
| Styles | `web/src/features-a.css` (structure only; the colour and type pass comes later) |
