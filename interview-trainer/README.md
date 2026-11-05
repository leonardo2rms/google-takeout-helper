# Interview Trainer

Duolingo-style practice for **senior engineer interviews**: Kotlin, Spring, the JVM, DDD, scaling/resilience
and working with AI. Short lessons, instant feedback, XP and streaks, and a "review mistakes" loop, built to be
comfortable on a phone.

> Lives in its own folder so it can be lifted into a dedicated repo later:
> `git subtree split -P interview-trainer -b trainer-only`, or just copy the directory. Nothing outside it is touched.

## What's in the box

| | |
|---|---|
| **146 questions / 8 units / 16 lessons** | Kotlin · Coroutines · Spring · JVM · DDD · Scale & resilience · Algorithms & complexity · AI-assisted engineering |
| **Question types** | `single`, `multi`, `fill` (tap tokens into blanks), `order` (tap lines into order), `spot` (tap the buggy line), `estimate` (type a number, tolerance-graded), `match` (connect pairs), `code` (write a function, run against tests), `open` (AI-graded, with interviewer follow-ups) |
| **Lesson mechanics** | Check → feedback with explanation → continue. Misses return at the end of the lesson (max 2 retries). Perfect-lesson bonus. |
| **Habit loop** | A **Today** card (8 interleaved questions: due reviews, weak spots, new material), a **daily goal ring**, a **forgiving streak** (earned freezes, chosen rest days), **ranks** (Associate to Fellow) and celebrations scaled to real milestones. See [Engagement](#engagement-and-progress). |
| **Spaced repetition** | Every question you answer becomes a scheduled card (SM-2-lite). **Review due** shows what is ready, missed questions never get lost, and the schedule respects your interview date. |
| **Progress** | Per-topic mastery and an honest **readiness** estimate, weak-spot targeting, XP, stars per lesson, an optional interview-date countdown and plan. Stored in `localStorage`; **export / import** a backup file. |
| **Offline** | PWA: the whole course ships in the JS bundle and an app-shell service worker caches it. Works with no network. |

## Run it

```bash
cd interview-trainer
npm install

npm run dev          # web on :5173 (reloads when content YAML changes) + API on :8787
# or production-style, single process:
npm run build && npm start     # http://localhost:8787
```

To open it on your phone, `npm run dev` binds to the network (`vite --host`): use `http://<your-laptop-ip>:5173`.

```bash
npm test                # unit tests: grading, sessions, streaks, API, prompt building, all content
npm run typecheck
npm run content:check   # validates every YAML file and prints counts
npm run e2e             # Playwright: plays every question type (and every lesson) through the real UI
```

### AI grading (optional)

```bash
cp .env.example .env     # put ANTHROPIC_API_KEY in it
```

With a key, open questions are graded by Claude against the question's rubric (`GRADER_MODEL`, default
`claude-opus-5-5`, effort `low` for snappy grading; try `claude-haiku-5-5` for cheap and fast). Without a key, or
when you're offline, the app **degrades gracefully**: the server answers 503, a keyword heuristic pre-ticks the
rubric, and *you* confirm which points you covered by comparing with the model answer.

How the grader is built (`server/src/grader.ts`):
- The model judges **each rubric point** (covered / not) rather than inventing a score; the score is computed
  from those verdicts, so grading is consistent and explainable (the UI shows ✓/✗ per point).
- Structured output (JSON schema) means no brittle parsing; a refusal, malformed output or wrong number of verdicts
  is an error, and the client falls back to self-assessment.
- The candidate's answer is treated as **untrusted**: it's fenced in `<candidate_answer>` tags, the system prompt
  says to ignore instructions inside it, and any attempt to close the tag is stripped. (Fitting, for an AI-skills
  trainer.)
- Rate-limited (30/min/IP by default), 32 kB body limit, and upstream errors are never leaked to the client.

### Ask why (AI tutor)

On the feedback card, **Ask why** opens a short chat about *that* question ("why is B wrong?", "give me an example"). With no AI available
(offline, no key) the button is shown disabled with a one-line reason; nothing else changes.

- `POST /api/explain` (`server/src/app.ts`, `server/src/tutor.ts`): body `{ questionId, answer, correct, messages[] }`, zod-validated
  (`shared/src/explain.ts`): at most 6 learner messages of 500 characters each, strictly alternating turns, answer text up to 600 characters.
  Rate-limited separately from grading (20/min/IP by default). Same Anthropic client setup as the grader; `EXPLAIN_MODEL` overrides `GRADER_MODEL` for the tutor only.
- **The question, correct answer and explanation are looked up server-side by `questionId`** (follow-up ids such as `lesson/3~f1` work too). The client only
  supplies the learner's own answer text and chat, and all of that is untrusted: fenced in `<learner_answer>` / `<conversation>` tags, with any attempt to close or
  spoof a tag stripped (forged "assistant" turns are flattened into the fenced transcript, so they cannot act as instructions). The system prompt tells the model to
  stay on the question and to politely decline anything else.
- Plain-text reply, capped at about 150 words; upstream errors are never leaked (the client shows "could not answer just now").
- The hosted single-file build uses the `sample` capability with the same prompt builder (`buildExplainPlainPrompt`), exactly like grading does.
- The UI says plainly that an AI answers and can be wrong, and that the card's explanation is the reference.

### Single-file build (no server, no install)

```bash
npm run build:standalone    # -> dist/interview-trainer.html (one self-contained page, ~570 kB)
```

Everything (course, logic, styles) is inlined, with no service worker and no API server. Hosts that provide
a `sample` capability (a claude.ai artifact does) grade open questions through the viewer's own Claude account;
anywhere else it falls back to self-assessment. Handy for trying the app from a phone with nothing installed.

## Engagement and progress

Everything below is local to the device: no accounts, no tracking, nothing sent anywhere. Design notes and the ethics
rules we hold ourselves to are in [`docs/engagement.md`](docs/engagement.md).

**What you see on Home**

- **Today**: one tap to a session of 8 questions, interleaved across topics: up to 5 due reviews, 2 weak spots, the rest new. A brand-new learner is simply invited to start the first lesson.
- **Review due**: questions whose review date has come, plus anything you missed. Shown as "N ready to review" (capped at "30+").
- **Daily goal ring** (default 8 questions, presets 8 / 16 / 24): tap for a 14-day chart. After a week away the next day's goal is halved once ("Easy restart").
- **Streak chip**: practising any day keeps it alive. Every 7 days in a row earns a **freeze** (hold up to 2) that covers a missed day automatically; **rest days** you choose (up to 3 weekdays) are covered for free. A freeze is only spent when it covers the whole gap; otherwise you get a fresh start and keep the freeze. Longest streak and total days practised never reset.
- **Rank** under the title (XP only goes up, so a rank is never lost), and **Progress**: readiness (an estimate of how much of this course you would recall today, with coverage shown, never a prediction of a result), a bar per topic and one tap to practise the weakest.
- **Onboarding** is an inline card, not a modal: role, interview date, daily goal, usual practice time, all optional. Skipping is permanent and free. With an interview date you get a calm countdown and a plan line ("At 8/day you'll have seen everything with 6 days to spare"); the day before, practice switches to due cards and weak spots.
- **Celebrations** only for real milestones (rank-up, streak 7/30/100, topic mastered, first goal met, perfect lesson), at most one per session, confetti never under `prefers-reduced-motion` and switchable in Settings. About 1 session in 6 shows a "Worth remembering" tip (+5 XP), decided by the date and your session count (so a reload never re-rolls it); the odds are printed in Settings and it can be turned off.

**How scheduling works** (`shared/src/srs.ts`): a question answered right on the first try is scheduled 1 day out, then 3, then
`interval x ease` (capped at 120 days); a miss, or an answer that only came right after seeing the feedback, comes back tomorrow with a lower ease.
Reviewing early never lengthens the interval, so replaying a lesson ten times cannot game the schedule. With an interview date set,
intervals are capped at half the time left, so every card gets at least two more looks before the day. Cards carry a hash of the
question text: if a prompt in the YAML is rewritten, the old card is dropped instead of being attached to a different question.

### Data model (`Progress` v2)

Stored under `interview-trainer.progress.v2`:

```text
version: 2
xp, streak, lastDay, lessons{id -> bestAccuracy, completions, lastDay}, missed[]     same meaning as v1
longestStreak, freezes (0..2)
cards{questionId -> h, ease, interval, due, reps, lapses, seen, firstTry, last}      spaced-repetition state
days{YYYY-MM-DD -> xp, answered, firstTry, target}                                    per-day log, last 400 days
goal{target}, profile{onboarded, role, interviewDate, restDays[], practiceTime}
settings{haptics, sound, confetti, tips, confidence}, stats{sessions, answered, firstTry, perfect, practiceDays}
prefs{reminders{time,notify,lastNotified}, reading{textSize,dyslexiaFont}}               device preferences, namespaced; kept local on import/merge
milestones{key -> day}        dedupes celebrations ("rank:3", "streak:7", "unit:ddd:80", "goal:first", ...)
```

- **Upgrading from v1 is automatic and lossless.** The old `interview-trainer.progress.v1` value is read once, migrated and written under the v2 key; the v1 key is never touched, so rolling back to an older build still works. Existing streaks, XP and lessons carry over, cards are seeded for completed lessons (missed ones due now, the rest spread over the next week, so there is no avalanche), you get one welcome freeze, and ranks you already earned do not celebrate again. Running the migration on its own output changes nothing.
- **Unreadable or newer data is never destroyed.** If the stored value is corrupt or was written by a newer version, the app starts empty but leaves the value alone; before its first save it copies it to `interview-trainer.progress.backup-YYYYMMDD`.
- **Several tabs / the installed app at once** stay in sync via the `storage` event, and every write re-reads the latest state first so one tab cannot clobber another.
- The app asks the browser for persistent storage (`navigator.storage.persist()`) after the first finished session, and on iPhone Safari (not installed) shows a one-time "Add to Home Screen" tip, because Safari may clear script-written storage of sites you have not installed after about a week of inactivity.

### Export, import and moving to a new phone

Settings -> **Your data**:

- **Download backup** writes `interview-trainer-progress-YYYY-MM-DD.json` (a small envelope with the app name, format version, an export time, a checksum and the full progress). **Copy backup text** puts the same JSON on the clipboard; **Paste backup text instead** imports it again, for hosts where files are awkward (the single-file build in an iframe, for example).
- **Import backup file** validates everything first (bad JSON, wrong app, damaged or edited file by checksum, a newer format, wrong field types are all rejected with a plain message and **nothing changes**). A good file shows what is inside and asks: **Merge** (keep the best of both: larger XP and counters, the streak from whichever side practised most recently, each card from the side that saw it last, union of mistakes and milestones; settings stay as they are on this device) or **Replace my progress**.
- Older (v1) backups and a bare progress blob are accepted too. **Reset progress** asks for confirmation and suggests downloading a backup first.

### Reminders (optional)

Settings -> **Reminders**: pick a daily time, then either or both of:

- **Add to my calendar (.ics)**: a recurring iCalendar event (`shared/src/reminders.ts`, RFC 5545: CRLF line ends, 75-octet folding that never splits a UTF-8 character,
  escaped text, a stable `UID` so re-importing updates the same event, a display alarm at the start). The time is a *floating* local time ("19:30 wherever you are"), which
  needs no VTIMEZONE rules and follows you when you travel. Rest days become `RRULE:FREQ=WEEKLY;BYDAY=...`. This is the reliable option: it works while the app is closed.
- **Notify me when I open the app**: asks for the Notification permission from the toggle (never on load), and then shows **one** local notification when the app is opened
  after that time with something due. At most one per day (`prefs.reminders.lastNotified`), never on a rest day, never after you practised, never when nothing is due, and the copy is
  plain ("5 questions are ready to review"). There is no push server, so browsers generally cannot fire it while the app is closed, and the UI says so.

"Turn reminders off" clears the time and the choice. The hosted single-file build cannot download files from its frame, so it replaces the calendar button with a note.
Stored under `progress.prefs.reminders` (`{ time, notify, lastNotified }`); like other settings it stays local on import/merge.

### Share card

Progress -> **Share my progress** (shown once you have answered something) draws a 1080x1350 PNG on a canvas (`web/src/lib/share-image.ts`) from `buildShareCard`
(`shared/src/share.ts`): streak, rank, readiness with the share of the course seen, and your strongest and weakest started topic. It contains only what the Progress screen already shows:
no dates, role, interview date or question content, and nothing is uploaded. The image has real alt text. Delivery uses what the browser has: `navigator.share` with the PNG
(or text only when files cannot be shared), otherwise a download link, plus **Copy text** and a selectable text version. The hosted single-file build cannot download from its frame,
so the download button is hidden there; a sheet with no working option says how to save the picture.

### Accessibility and reading settings

Behaviour, rules and the tracked colour-contrast exception are in [`docs/accessibility.md`](docs/accessibility.md). In short: axe-core runs over every screen and state
(`e2e/a11y.spec.ts`, fails on serious/critical issues), plus behaviour tests for what axe cannot see (`e2e/a11y-behaviour.spec.ts`): a skip link and a single `main`
landmark, focus moved to the prompt on a new question and to the verdict after Check, results readable without colour, keyboard-only match / order / spot, a focus ring on every Tab stop,
and reduced motion. **Settings -> Reading** sets text size (S / M / L / XL) and an easier-to-read system font, stored in `progress.prefs.reading`.

## End-to-end tests

```bash
npm run e2e        # builds the app, serves it with AI grading off, drives it in headless Chromium at phone size
```

`e2e/engagement.spec.ts` covers the habit loop: upgrading a real v1 profile, the goal ring, the review queue appearing after a miss and clearing the next
day (the page clock is moved with `page.clock`), freezes and rest days, Daily Practice, onboarding, export/import (including garbage and tampered files),
two tabs staying in sync, celebrations with and without reduced motion, the readiness page, and 390px layout / tap-target / keyboard checks. Storage is
seeded with `localStorage` blobs so each test starts from an exact state.

`e2e/ask-why.spec.ts`, `e2e/reminders.spec.ts`, `e2e/share-card.spec.ts`, `e2e/a11y.spec.ts` and `e2e/a11y-behaviour.spec.ts` cover the tutor chat (the AI endpoint is faked in the browser), reminders (a recording
`Notification` stand-in, the downloaded `.ics`), the share card (a real PNG, `navigator.share` stubs) and accessibility. Run several agents or shards side by side with `E2E_PORT=4182 npx playwright test --workers=2`.

`e2e/question-types.spec.ts` plays **every question type through the real UI** (reading the correct answers from the
YAML, so it stays in sync with the content), and asserts the feedback, the XP awarded and that the page never
scrolls sideways on a phone. It also checks that wrong answers are marked and come back, and that **every lesson in
the course can be completed perfectly**. Screenshots of the passing runs are written to `e2e/screenshots/`
(`<type>-1-answered.png`, `<type>-2-correct.png`, `<type>-3-wrong.png`) and are committed on purpose, as a visual record.
First time on a machine: `npx playwright install chromium`.

## Layout

```
interview-trainer/
  content/          the course: one YAML file per unit (edit these to add questions)
  shared/           schema (zod), grading, lesson-session engine, streak/progress logic. Pure TS, fully tested
  server/           Express API: /api/health, /api/grade, /api/explain (+ serves web/dist in production)
  web/              React + Vite PWA
  e2e/              Playwright tests + screenshots
  docs/             design notes (engagement.md: the habit loop and its ethics rules; accessibility.md)
  scripts/          content:check, build-standalone
```

Content is plain YAML on purpose: it is the most portable part of this project (any backend, a Kotlin/Spring
migration, a CMS import, whatever you move to later). The Vite plugin turns it into a validated virtual module at
build time; the server loads the same files to look up rubrics.

## Adding content

Drop a question into any lesson in `content/*.yaml`, or add a new `NN-name.yaml` for a new unit (files are ordered
by name). Question ids are derived automatically (`<lesson-id>/<n>`). `npm run content:check` and `npm test`
validate the structure and cross-field rules (answers exist, blanks match, rubric sanity).

```yaml
# single / multi
- type: single                      # or: multi (use `answers: [a, c]`)
  prompt: "What is the time complexity?"
  code: { lang: kotlin, source: "for (x in xs) for (y in xs) use(x, y)" }   # optional snippet
  options: [{ id: a, text: "O(n²)" }, { id: b, text: "O(n)" }]
  answer: a
  explanation: "Shown after checking. `backticks` render as inline code."

# short coding challenge: tap tokens into blanks  ([[n]] = blank n; bank needs >= 1 distractor)
- type: fill
  prompt: "Return the length or 0"
  template: "val len = name[[1]]length [[2]] 0"
  bank: ["?.", "?:", "!!", "?"]
  answers: ["?.", "?:"]
  explanation: "..."

# short coding challenge: tap lines into order (indentation is part of the line; lines must be unique)
- type: order
  prompt: "Arrange the function"
  lines: ["fun f(): Int {", "    val x = 1", "    return x", "}"]
  explanation: "..."

# find the bug: tap the offending line(s) (1-based; the learner must pick exactly these)
- type: spot
  prompt: "Tap the line that causes the N+1 queries."
  source: |
    val orders = repo.findAll()
    orders.forEach { println(it.items.size) }
  answers: [2]
  explanation: "..."

# estimate: a number, accepted within a relative tolerance (0 = exact; default 0.05)
- type: estimate
  prompt: "200 req/s at 0.25 s average latency: how many requests are in flight?"
  answer: 50
  tolerance: 0
  unit: requests            # optional, shown next to the input
  explanation: "..."

# match: connect each left item to its right item (3-6 pairs, all texts unique)
- type: match
  prompt: "Match each pattern to the problem it solves."
  pairs:
    - { left: "Bulkhead", right: "Separate pools so one slow dependency can't starve the rest" }
    - { left: "Circuit breaker", right: "Fails fast while a dependency is unhealthy" }
    - { left: "Idempotency key", right: "Makes a retried write safe to repeat" }
  explanation: "..."

# code: write a function; tests run in a sandboxed Web Worker (JavaScript), so it works offline
- type: code
  prompt: "Write `isBalanced(s)` ..."
  starter: |
    function isBalanced(s) {
      // your code
    }
  entry: isBalanced
  tests:
    - { args: ["([]{})"], expect: true }
    - { args: ["([)]"], expect: false }
    - { args: [""], throws: "" }                        # `throws: text` expects an error mentioning text
    - { args: { js: "['('.repeat(200000)]" }, expect: false, hidden: true }   # big inputs via a JS expression
  solution: |             # verified against the tests by `npm test`, and shown after the question
    function isBalanced(s) { ... }
  kotlin: |               # the same solution in Kotlin, shown for transfer to the real interview
    fun isBalanced(s: String): Boolean { ... }
  explanation: "..."

# order of plain sentences instead of code: add `kind: steps`
- type: order
  kind: steps
  prompt: "Put the steps in the right order."
  lines: ["First...", "Then...", "Finally..."]
  explanation: "..."

# open: AI-graded against the rubric (format: code gives a monospace box)
- type: open
  prompt: "Explain X"
  modelAnswer: "A strong answer..."          # shown after answering (acts as the explanation)
  keyPoints:                                  # 2-6 rubric points
    - point: "Mentions Y"
      keywords: ["y", "why"]                  # any-of; ONLY used by the offline fallback
  followUps:                                  # optional: the "interviewer" asks one of these next
    - when: missed                            # missed = probe a rubric point the learner did NOT cover
      point: 2                                # (1-based index into keyPoints)
      prompt: "..."
      modelAnswer: "..."
      keyPoints: [{ point: "...", keywords: ["..."] }, { point: "...", keywords: ["..."] }]
    - when: covered                           # covered = go deeper on a point they DID cover
```

Authoring rules of thumb (enforced where possible by tests):
- **Option order is shuffled at runtime**, so don't write "all of the above" / "both A and B" options.
- Every lesson should mix at least 3 question types and include one `open` question.
- A model answer must satisfy its own `keywords` rubric (a test checks this), otherwise offline users would be told
  the perfect answer is incomplete.

## Code questions and the sandbox

`code` questions run the learner's function against the tests in a **Web Worker** (`web/src/lib/code-runner.worker.ts`):
no DOM, no `localStorage`, no cookies, a hard wall-clock timeout (the page terminates the worker, so an infinite loop
can't freeze the app) and `fetch`/`XMLHttpRequest`/`WebSocket`/`indexedDB` removed before the code runs. It is
best-effort isolation for the learner's *own* code on their *own* device, not a defence against a hostile
multi-tenant workload. That is also why there is no server-side runner: executing arbitrary Kotlin or Java on a
server needs real container isolation, and I did not want to ship a remote-code-execution endpoint. The challenges are
therefore JavaScript, with the Kotlin equivalent shown afterwards. A real Kotlin runner would plug in behind the same
`RunCodeFn` interface (`web/src/lib/evaluate.ts`).

## Interviewer follow-ups

An `open` question can list `followUps`. After the main answer, the app picks one by what the learner covered: first a
`missed` follow-up probing a gap, otherwise a `covered` one pushing deeper. The follow-up is graded like any open
question (AI or self-check), and the question passes on the average of both answers. The selection is authored, not
generated, so it works offline and is deterministic; the AI only grades.

## Design notes

- **Why TypeScript, not Kotlin/Spring?** Fastest path to a mobile-first PWA that could be built, run and tested end
  to end in one environment. The valuable assets are the YAML course and the grading contract
  (`shared/src/api.ts`); a Spring Boot API exposing `GET /api/health` and `POST /api/grade` is a drop-in
  replacement for `server/`.
- **Code challenges are tap-based** (fill / order) rather than free typing because they work on a phone and can be
  graded deterministically without a sandbox. Free-form code answers use `open` with `format: code`, graded by AI.
- The course, including correct answers, is in the client bundle, which is what makes instant feedback and offline
  mode possible. That's fine for a self-training tool, but not for anything proctored or competitive.

## Roadmap / ideas

- **Architecture board** (drag & connect components, validate against a reference topology): the question schema is a
  discriminated union (`shared/src/schema.ts`), so it is one new type plus one component. Deliberately left out for now.
- Next batch (specified, not built): achievements/badges, confidence ratings and calibration, a daily challenge, a weekly recap card. (Reminders, the share card, the Ask why tutor and the accessibility pass are done.)
- Later: timed "mock interview" mode, teach-back prompts, a transfer code / QR for moving between devices.
- Real code execution for coding challenges (sandboxed Kotlin runner) instead of AI grading.
