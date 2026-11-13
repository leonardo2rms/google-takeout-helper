# Handoff: what is built, what is pending

This is the single starting point for the next person (or Claude session) working on Interview Trainer. It records
every feature that exists, how it was built and verified, and a prioritised backlog with enough context to pick up
any item cold. Where it disagrees with an older note elsewhere, this file wins.

State described: `main` at merge commit `733ac7c` (2026-11-05), identical to branch
`claude/engineer-interview-training-82crnr` at `a1db40f` plus the merge. Everything lives in the `interview-trainer/`
folder of the `google-takeout-helper` repository (the Python script at the repository root is unrelated; this app was
put here only because no other repository was available, and is meant to move to its own repository, see P3-9).

---

## 1. Orientation

**What it is.** A phone-first, Duolingo-style trainer for **senior engineer interviews**: Kotlin, coroutines, Spring,
the JVM, DDD, scale and resilience, algorithms and complexity, AI-assisted engineering, data and persistence,
messaging, system design, testing, security and observability. Short lessons with instant feedback, nine question
types (including code you write and run, and open answers graded by AI with an interviewer's follow-up), spaced
repetition, a forgiving habit loop, mock interviews. Works offline, needs no account, and also ships as one
self-contained HTML page.

**Run it** (from `interview-trainer/`):

| Command | What it does |
|---|---|
| `npm install` | install all workspaces |
| `npm run dev` | web on :5173 (reloads on content edits) + API on :8787 |
| `npm run build` then `npm run start` | production build served by the API on :8787 |
| `npm test` | 1,664 unit/content/docs tests (vitest) |
| `npm run coverage` | same, with the coverage ratchet (fails if coverage drops) |
| `npm run typecheck` | all workspaces + the e2e folder |
| `npm run e2e` | 378 Playwright tests at phone size; set `E2E_PORT` to a free port if anything else runs |
| `npm run content:check` / `npm run content:lint` | validate the course / measure answer-option bias |
| `npm run build:standalone` | the single-file build in `dist/` (fragment for hosts + full document for file://) |

AI grading needs `ANTHROPIC_API_KEY` in `.env` (see `.env.example`); without it open questions fall back to a
self-check. Outside this sandbox, Playwright needs `npx playwright install chromium` once.

**Hosted preview.** A private claude.ai artifact (owner-only):
<https://claude.ai/artifact/5EC7quCKQzLvjfxoTBFx4M>. To update it: `npm run build:standalone`, then publish
`dist/interview-trainer.html` to that URL with capabilities `{"sample": {}, "downloads": true}` (grading and the tutor
use the viewer's own Claude account through `sample`; exports use `downloads`).

**Where to read next.** [README](../README.md) (features and authoring),
[architecture](architecture.md), [testing](testing.md), [content guide](content-guide.md),
[contributing](contributing.md), [design system](design-system.md), [engagement](engagement.md),
[learning features](learning-features.md), [accessibility](accessibility.md), and the
[research and audit archive](research/README.md).

---

## 2. What is built

### 2.1 Course and content

- **307 questions, 13 units, 34 lessons.** The 8 original units have 3 lessons each (the third is "advanced"); the 5 newer
  units (Data & Persistence, Messaging & Events, System Design, Testing & Quality, Security & Observability) have 2.
- **By type:** single 128, multi 36, open 34, match 22, code 20, estimate 20, spot 18, fill 15, order 14.
- **Authoring:** YAML in `content/` (one file per unit), validated by the zod schema in `shared/src/schema.ts` plus
  cross-field checks; the web build turns it into a virtual module; question ids are derived from lesson id + position.
- **Machine-checked integrity** (all in `npm test`): every code question's reference solution passes its tests and its
  starter fails; every open answer and follow-up model answer satisfies its own offline rubric; every lesson mixes 3+
  types and ends with an open question that has a "missed" and a "covered" follow-up; option balance ratchet in
  `content/balance.test.ts` (correct answer strictly longest in at most 30% overall, it was 83%; no style feature or
  fixed attacker beats chance by much, see P1-1); British spelling;
  README counts match the course; docs examples and paths are valid (`content/docs.test.ts`).
- **Human-style review done:** two fact-check passes over all new content (1 hard error fixed, ~15 claims softened), a
  fact-check of the 458 rewritten answer options (0 accidental second answers, 9 fixes), and a fact-check of the 354
  options restyled for P1-1 (0 errors, 9 tightened). Residual uncertainty: P1-7.

### 2.2 Lessons and question types

Nine types, all tap-first so they work on a phone: `single`, `multi`, `fill` (tap tokens into blanks), `order` (tap
lines into order; `kind: steps` for plain sentences), `spot` (tap the buggy line), `estimate` (number within a relative
tolerance; tolerant parsing of "1,234", "18,2", "5k", "2M", typographic minus), `match` (connect pairs),
`code` (write a JavaScript function, run visible + hidden tests in a sandboxed Web Worker with a timeout; Kotlin
equivalent shown afterwards) and `open` (AI-graded against a 2-6 point rubric, offline self-check fallback).

Lesson mechanics: Check, then feedback with one verdict (icon + word + rail) and an explanation; misses come back at
the end (max 2 retries); XP 10 first try / 4 on retry / +10 perfect lesson; hints that cost 3 XP; optional confidence
rating ("How sure are you?", off by default) that feeds spaced repetition; interviewer **follow-ups** on open questions,
chosen by which rubric points the learner missed or covered, with the question passing on the average of both
answers; in-app confirm dialog when quitting. Options are shuffled per attempt. Code: `web/src/screens/Lesson.tsx`,
`web/src/components/`, `shared/src/session.ts`, `shared/src/interview.ts`, `shared/src/code-runner.ts`.

### 2.3 Habit loop and progress

Designed from `research/engagement-spec.md`; the rules and edge cases are in [engagement.md](engagement.md).

- **Today**: one tap starts 8 interleaved questions (due reviews, weak spots, new material); the first sessions use
  only "gentle" formats (no open/code). Home becomes a calm "Done for today" state once the goal is met.
- **Daily goal ring** (capped display "8/8 + bonus"), **forgiving streak** (freezes earned every 7 days, max 2; chosen
  rest days; longest streak and total days never reset), **ranks** Associate → Fellow, celebrations scaled to real
  milestones (respecting reduced motion).
- **Spaced repetition** (SM-2-lite cards per question, keyed with a prompt hash so content edits do not corrupt
  history) and **Review due**; one definition of "due" everywhere (`shared/src/due.ts`).
- **Readiness and mastery**: per-topic mastery, an honest readiness score hidden until 25% of the course is seen,
  "weakest topic" only once 3 topics are started; the onboarding **role** sets a track: its units come first on Home, in Today's new material and in "Up next", and weigh more in readiness, Mixed practice and Mock (P2-6).
- **Onboarding** as an inline card (role, interview date with countdown plan, practice time), deferrable.
- **Tomorrow bridge**: the summary says what is scheduled for tomorrow and offers a calendar reminder once.
- Code: `shared/src/srs.ts`, `shared/src/streak.ts`, `shared/src/goal.ts`, `shared/src/rank.ts`,
  `shared/src/mastery.ts`, `shared/src/daily.ts`, `shared/src/plan.ts`, `web/src/screens/Home.tsx`,
  `web/src/screens/Summary.tsx`, `web/src/screens/ProgressScreen.tsx`.

### 2.4 Practice modes and learning tools

Mixed practice; Review due; **daily challenge** (one deterministic question per date, +15 XP, its own streak;
`shared/src/challenge.ts`); **timed mock interview** at `#/mock` (10/20 questions, no feedback until the end, flag and
revisit, auto-submit, scorecard by topic, history of 10; `shared/src/mock.ts`, `web/src/screens/Mock.tsx`);
**Library** at `#/library` (full-text search, filters, bookmarks, private notes, practise one question;
`web/src/screens/Library.tsx`); **27 achievements** with a trophy case (`shared/src/achievements.ts`); **share card**
(canvas PNG of numbers the learner already sees; `shared/src/share.ts`); **reminders** (an `.ics` calendar file and a
best-effort once-a-day notification; `shared/src/reminders.ts`). Details: [learning-features.md](learning-features.md).

### 2.5 AI

- **Grading** (`server/src/grader.ts`): the model judges each rubric point (covered or not) through structured output;
  the score is computed from those verdicts. The learner's answer is fenced as untrusted data (prompt-injection hygiene,
  `shared/src/grading-prompt.ts`); implausible or junk verdicts are rejected and fall back to self-check
  (`shared/src/verdict.ts`). Default model `claude-opus-5-5` at low effort, configurable with `GRADER_MODEL`.
- **Ask why tutor** (`server/src/tutor.ts`, `POST /api/explain`): a short chat about the current question; question,
  answer key and explanation are looked up server-side, never trusted from the client; `EXPLAIN_MODEL` optional.
- **Hosted build**: both go through the viewer's own Claude account via the artifact `sample` capability
  (`web/src/lib/sample-grader.ts`), with timeouts so a hung call never blocks the lesson.
- **Verified only with fakes** (no API key in the build sandbox): see P1-3.

### 2.6 Data and safety

- `Progress` v2 in `localStorage` (`shared/src/progress.ts`; additive fields in `shared/src/extras.ts` and
  `shared/src/prefs.ts`; lossless, idempotent migration from v1 in `shared/src/migrate.ts`).
- Export / import / merge with a checksum (`shared/src/backup.ts`), multi-tab sync, `storage.persist()` request.
- **Never loses data silently** (`web/src/lib/store.ts`, `shared/src/storage.ts`): failed writes are retried and the
  learner is told; newer-schema, corrupt or foreign values are parked as rotating backups (monotonic sequence, safe
  against a clock set back) before anything is overwritten, also when they appear in another tab; a "Recovered data"
  section in Settings restores or exports them (`web/src/components/DataSafety.tsx`).

### 2.7 Offline, PWA and builds

Service worker (built by `web/vite-sw-plugin.ts`, logic in `web/src/lib/sw-logic.ts`): version-keyed precache, only the
real app shell is cached as the offline shell, old caches removed, unobtrusive "Update ready" notice. The server returns
real 404s for missing assets so a deploy cannot poison the cache. Single-file build (`scripts/build-standalone.mjs`)
emits a fragment for artifact hosts and a full document for file://. File offers go through one helper
(`web/src/lib/files.ts`) that uses the host's `downloads` capability when hosted.

### 2.8 Design and accessibility

The "drafting sheet" identity replaced the original Duolingo-like look: paper/ink neutrals, cobalt / teal / vermilion /
ochre semantics, IBM Plex Sans (subset, embedded, offline), Lucide line icons (`web/src/components/icons.tsx`), flat 6px
radii. WCAG AA contrast for 228 token pairings in light and dark is enforced by `scripts/contrast.mjs` in `npm test`,
and axe runs on every screen in both themes in e2e with no exceptions. Keyboard operation of every question type, focus
management, modal key ownership, reading settings (text size, easier font), 44px targets, no sideways scroll at
320px, reduced motion. See [design-system.md](design-system.md) and [accessibility.md](accessibility.md).

### 2.9 Server

Express (`server/src/app.ts`): `GET /api/health`, `POST /api/grade`, `POST /api/explain`, static web build with SPA
fallback for navigations only; zod-validated bodies; per-IP rate limits (set `TRUST_PROXY` behind a proxy); CSP and
security headers; gzip; immutable caching for hashed assets; 405 with `Allow`; request timeouts; upstream errors never
leaked.

### 2.10 Quality bar at handoff

1,664 unit tests (coverage 99.7% statements / 98.0% branches; thresholds in the coverage config are a ratchet), 378
e2e tests including axe in light and dark, all hand-made mutants killed in the last round, typecheck clean, both builds
green, `npm test` green from a fresh clone. One known flake, present before P2-6: `a11y-behaviour.spec.ts` "the code editor is not a keyboard trap" fails about 1 run in 12 under parallel load (not yet root-caused; it passes on re-run). Seen on 2026-11-13 in a fresh container whose Playwright 1.56 had to be pointed at a newer pre-installed Chromium (a throwaway config with `launchOptions.executablePath`, not committed): `engagement.spec.ts` "several tabs" failed every time and `share-card.spec.ts` "text-only share" about 1 run in 6, both also on the code before P2-4a, so most likely the browser build; both passed in the earlier container. 90 committed screenshots in `e2e/screenshots` show every screen.

---

## 3. How it was built (to calibrate trust)

1. MVP: content schema, 5 question types, lesson engine, AI grading, PWA, hosted single-file build.
2. Spot-the-bug, estimate, match; then code-with-tests (browser sandbox, JavaScript, since running untrusted Kotlin
   on a server needs real isolation) and interviewer follow-ups.
3. Research by parallel agents: engagement spec, design audit, monetization, competitor teardown (all in
   [research](research/README.md)).
4. Engagement system, then two feature batches, three content-expansion passes and two fact-check passes, all in
   separate git worktrees, each merged only after the integrator re-ran unit and e2e tests on the merged tree.
5. Visual redesign; coverage and docs pass (found 5 real bugs).
6. Two audit rounds (UX walkthrough + adversarial QA/security, by agents that were not allowed to change code), each
   followed by fixers and re-verification. Round 1 found 2 data-loss bugs, unreadable match items and the
   "correct answer is the longest option" tell; round 2 found a multi-tab overwrite and new answer-style tells.

Everything above was verified in this sandbox: headless Chromium at phone size, fake AI clients, fake host
capabilities. Real-world checks that were **not** possible here are listed as P1-3.

---

## 4. Pending work

Ids are stable so commits and sessions can refer to them. Audit ids in brackets point at
[research](research/README.md) reports, which describe the reproduction in detail.

### P1: do next

**P1-1 Remove the remaining answer-style tells in choice questions** [N-S1-2]: **done** (2026-11-05).
- `scripts/content-lint.ts` now measures asides (`(`, `;`, `:` outside code spans), "or", causal clauses and absolute
  words for correct options vs distractors, plus three attackers fixed in advance: the audit's combined rule, the same
  rule reversed (catches over-correction), and a leave-one-question-out naive Bayes bag of words.
  `content/balance.test.ts` ratchets all of them on the real course (limits in the "Balanced options" section of
  [content-guide.md](content-guide.md)); `npm run content:lint -- --unit <ids>` and `-- --offenders` help authors.
- 354 options in 139 questions were rewritten by four writers in parallel, then reviewed by three independent
  fact-checkers (150 questions: 0 wrong facts, 0 second defensible answers, 9 loose wordings tightened). A script
  confirmed no answer key, option id, prompt or other field changed.
- Before → after (single questions, chance 25%): "pick the option with ( ; :" 54% → 19%; "pick the one with 'or'" 40%
  → 25%; "avoid the causal one" 37% → 23%; combined rule 64% → 15% (reversed 23%); bag of words 55% → 32% (the
  audit's own classifier reported 58.6%); the answer is the shortest option 12% → 20%.
- Left as is: per unit the bag of words still reaches 44-50% in spring, ddd, complexity and data (6 to 14 single
  questions each, so a couple of questions move it 10 points; the ratchet bounds the whole course at 40%), and in data
  the answer is never the shortest option (0 of 6). Worth a look when those units next get new questions.

**P1-2 Content clean-ups the audits listed** [S2-25, N-S2-13, UX-22]: **done** (2026-11-05).
- The duplicate rate-limiter question in `scale-advanced` (position 9) is now an open question on hedged requests for
  tail latency; the system-design one stays.
- Overlaps resolved: `spring-advanced/3` is now about `@Version` and retrying an optimistic-locking failure outside
  the transaction (`data-sql/7` keeps the SQL-level lost update); `data-jpa/7` is now Hibernate second-level cache
  staleness (`scale-advanced/6` keeps the cache-aside order); `testing-strategy/3` is now `@JsonTest`
  (`spring-advanced/8` keeps `@WebMvcTest`).
- Order questions: lines are unique once trimmed, the pool shows them without indentation (placed lines show it), and
  `coroutines-flow/5` and `scale-resilience/7` had a second valid order, now pinned down by their prompts.
- Multi-select: 2, 3 and 4 correct answers of 5 (11 / 12 / 11 questions, plus two 4 of 6), explanations rewritten.
- Guards in `content/content.test.ts`: no repeated question, order lines unique once trimmed, no correct-answer count on
  more than half of the `multi` questions. A fact-check of the 30 changed questions found the two order errors above and
  three smaller issues, all fixed.
- Learners keep their cards except for the questions whose prompt changed (the six above that were rewritten), which
  start fresh, as intended when the question itself changes.

**P1-3 Verify what could not be verified in the sandbox**
- Real AI grading and the tutor with `ANTHROPIC_API_KEY`: prompt quality, latency, cost per call, refusals, the
  implausible-verdict fallback rate. Build a small eval (about 60 human-labelled answers across rubrics) comparing
  Haiku, Sonnet and Opus at low effort; [MONETIZATION.md](MONETIZATION.md) has the cost model and suggests the tiers.
- The hosted page in a real claude.ai viewer: the `sample` consent flow, the code-runner Web Worker inside the
  sandboxed frame (it may be refused; the UI then says "Could not start the code runner"), `downloads` saves of JSON
  and PNG, and that `.ics` is still refused by the viewer's extension allowlist (the button is hidden there).
- The phone OS keyboard with the code editor (only emulated so far).
- *Size:* S-M.

**P1-4 Upgrade zod (security advisory)**: **done** (2026-11-06).
- zod 3.25 → 4.6.7 in `shared` and `server` (one copy, also used by the Anthropic SDK, whose peer range allows 4);
  `npm audit` reports 0 vulnerabilities. The grader now imports plain `zod` instead of `zod/v4`; `.finite()` is gone
  (zod 4 numbers reject `Infinity` and `NaN`, pinned in `shared/src/schema.test.ts`); no other code change was needed.
- A server test now pins the structured-output JSON schema the grader sends (`server/src/clients.test.ts`), the one path
  the fakes did not cover.
- Cost: the web bundle grew about 70 kB (929 → 999 kB of JavaScript; single-file build 1.02 → 1.09 MB), because the
  browser bundle includes the zod schemas although it only uses them as types. Marking the schemas pure did not remove
  them (zod 4's core has module-level side effects); the fix is to keep schema modules out of the web's import graph,
  folded into P3-5.

**P1-5 Resume a lesson after quitting or pressing Back** [S2-15, N-18]: **done** (2026-11-07).
- The session is saved after every question that was answered and continued from, in its own storage key
  (`interview-trainer.resume.v1`), apart from `Progress`. Home shows a Resume card (with a Discard that asks first);
  opening the same route (`#/lesson/<id>`, `#/daily`, `#/mix`, `#/review`, `#/warmup`, `#/topic/<id>`) carries on from
  it. It is forgotten on finish, discard, reset or import-replace, after 48 hours, and when the course changed under it
  (every saved question carries the hash of what it asks). Details: "Resuming an unfinished session" in
  [learning-features.md](learning-features.md) and the architecture notes.
- **Deviation from the plan:** the browser's Back button is not intercepted with a "really leave?" question. Everything is
  saved, so leaving costs nothing, and the X no longer asks either (the old "Quit and lose progress" dialog would now be
  untrue). Only the question on screen is asked again. The dialog tests moved to the Reset-progress dialog.
- Known limits, deliberate: one saved session at a time (starting something else and answering a question replaces it);
  a half-done interviewer follow-up is not kept (the main question is asked again); a hint revealed on the question on
  screen is not kept; one-question sessions and the mock interview are never saved.
- Quality: 14 mutants of `shared/src/resume.ts` and 8 of the wiring were killed by the new unit and e2e tests (one
  equivalent mutant remains: with a saved session `LessonScreen` takes its queue from `initial`, so the picked
  `questions` are unused).

**P1-6 Decide and enforce the XP / goal farming rule** [S2-11]: **done** (2026-11-07). Decision (yours): repeats earn
reduced XP; I also made the goal count distinct questions, as proposed.
- A question already answered earlier today (its review card was updated today and matches the wording,
  `alreadyDoneToday` in `shared/src/goal.ts`; no new stored data) pays at most `XP_REPEAT = 2` for a correct answer, does
  not count for the daily goal again, and scales the perfect-lesson bonus by the share of new questions. The next day it
  pays in full. Lifetime counters still count it as practice. The mock interview keeps its own credit and is untouched.
- The app says so: a note on the repeated question and a line on the summary. Eight rounds of one Library question now
  give 10 + 7 x 2 = 24 XP and a goal credit of 1 (was 80 XP and 8/8). Details: "Doing the same question again" in
  [learning-features.md](learning-features.md).
- Side fix found by a flaky test: finishing a session and re-entering the same route before React rendered the summary
  reused the finished screen (the hash handler re-reads `location.hash`, so a visit to `/done` and a return looked like
  "no change"). Session screens are now keyed by a navigation counter (`useVisit` in `web/src/App.tsx`); a deterministic
  e2e test covers it, and the old flake (about 1 in 60) is gone in 240 runs.
- Not changed, deliberately: badges such as "N perfect lessons" and "N questions answered" can still be advanced by
  repeating (lifetime counters count real practice); it only fools the learner. Say if you want those to count distinct
  questions too.

**P1-7 Re-check version-sensitive facts against official docs**: **done** (2026-11-07).
- Four agents with web access checked 262 claims in all 13 units against official sources (Kotlin 2.4.21 and
  kotlinx.coroutines 1.11.0; JDK 25 LTS, Kafka 4.2, Hibernate 7.4, Flyway 13; Spring Boot 4.1.2, Framework 7.0.10,
  Micrometer 1.17; OWASP, the MCP and provider docs). Result: 210 confirmed, 38 softened or qualified with a version,
  12 fixed, **no answer key changed**. Every claim, verdict and source is in the four records listed in
  [research/README.md](research/README.md) (`p1-7-*.md`).
- The changes that matter: virtual-thread pinning on `synchronized` is now stated as JDK 21 to 23 (JEP 491 removed it in
  JDK 24, so `jvm-runtime/5` now says "On JDK 21"; its review cards restart); `NESTED` propagation does not work with
  `JpaTransactionManager`; `@MockBean` was removed in Boot 4.0; Testcontainers no longer needs `replace = NONE` since
  Boot 3.4; the Kafka consumer log in `msg-kafka/4` was invented and is now the real client wording; `read_committed` is
  not Kafka's default; HashMap treeification wording; Kotlin's own `ArrayDeque` does support index access.
- Not verified, because the source was unreachable or only a secondary summary was found (none changes an answer):
  the default of `spring.jpa.open-in-view` in Boot 4.x, the Hibernate 7 wording on EAGER and N+1 in JPQL, the
  "propagate trace context even for unsampled requests" point in `observability-incidents/9`, RFC 8725 and the SRE
  burn-rate figures (arithmetic checked), and whether `suspend` functions returning `Result` were legal at exactly Kotlin
  1.5.0. docs.spring.io, docs.micrometer.io, owasp.org, docs.junit.org and several others were blocked in the sandbox
  (the agents used source code on GitHub, Maven Central and domain-limited search instead): re-check those few with
  direct access.
- This is a snapshot: Spring, Kotlin, the JDK and above all the AI content move. Re-run the check (the method is in the
  records) before a release, and prefer wording that names a version over wording that says "currently".

### P2: product improvements

**P2-1 Home for returning users** [N-12, UX-15]: **done** (2026-11-08).
- Today stays the one solid primary action. Daily challenge, Review due and Progress are full rows (they carry live
  numbers); Mixed, Mock and Library share one compact three-up row (`.mode-tile`, wraps at larger text sizes, keeps full
  accessible names). Only the unit being worked in is open (`currentUnitId` in `web/src/lib/home-copy.ts`); every other
  unit is a one-line native `<details>` with its name and tally, with "Expand all" / "Collapse all". A returning
  learner's Home went from about 4,900px to about 2,200px; a newcomer sees one open unit instead of thirteen.
- Tests: `e2e/home-returning.spec.ts` (12 tests: layout, tap targets, one-row tiles, keyboard toggling, 320px, XL text,
  every stage of the learner) and the pure `currentUnitId`; 9 mutants of the Home code are caught.
- Not done: remembering which units a learner opened between visits (they reset to "the current unit"), and a
  "Show all units" view with progress bars per unit (Expand all is the simple version).

**P2-2 Spread the first reviews** [N-4 remainder]: **done** (2026-11-09).
- `applyToCards` (`shared/src/srs.ts`) keeps at most `FIRST_REVIEW_DAY_CAP` = 10 first reviews due on any one day and slides
  the extras by up to `FIRST_REVIEW_MAX_SLIDE` = 2 days (`spreadFirstReview`, pure and deterministic): three lessons on day 1
  (25 cards) fall due as 10 / 10 / 5 instead of 25 on day 2. One lesson a day is untouched. Only a card's first successful
  review moves; misses stay due tomorrow, later reviews and early reviews are untouched, and an interview date's interval
  cap is respected. I chose a load-aware rule over a fixed "1, 2 or 3 days" so the common single-lesson learner keeps the
  standard 1-day first interval.
- Two knock-on changes, both for numbers that must agree: the lesson's note for a first review now says "in the next few days"
  (it cannot know where the other cards of the session will land), and the summary's forecast covers everything that is
  coming, not only that session's cards, capped to the next three days with reviews, so it matches Home tomorrow.
- Tests: `shared/src/srs-spread.test.ts` (20, including a property test), `e2e/first-reviews.spec.ts` (4); 12 mutants caught.
- Not done: the same smoothing for later intervals (a general due-date "load balancer"); the cap of 10 is a constant, not a
  setting.

**P2-3 Long pages and pacing** [N-14, N-15, N-17, UX-29, UX-34, N-8]: **done** (2026-11-08).
- **Mock results save by themselves** (UX-34): written as soon as the scorecard shows when nothing needs self-checking; with
  open answers to tick (no AI) "Save results" stays and any way of leaving also saves what is on screen, once. Learning notes
  in [learning-features.md](learning-features.md) explain why the leave listeners are registered at module load. Not
  added: a "Discard" for a saved mock (undoing a session's streak, badges and cards safely is a bigger change).
- **Settings** went from about 1,800px to about 980px: Reading, Reminders and Your data are one-line `SettingsGroup`s that
  show their state; toggle hints are shorter. (The notify toggle, the paste disclosure and the Reset section were already
  handled in the audit rounds.)
- **Progress** for a heavy user went from about 5,000px to about 2,600px: earned badges fold by group with counts and stay
  open while a recent badge is in them. (Started topics, "not started" and "Earlier mocks" were already folded.)
- **N-8 follow-ups:** decided as a definition, not a change of counting: the goal counts distinct questions and a follow-up
  belongs to its question; the goal sheet now says so. **N-17 ranks:** thresholds left as they are, because the audit's other
  option (celebrate the first promotion quietly) was already in place and changing them would re-rank existing profiles.
- Tests: `e2e/settings-groups.spec.ts` (8), `e2e/progress-heavy.spec.ts` (5), 5 new mock-saving tests in
  `e2e/mock.spec.ts`; a helper `openSettingsGroup` for specs that use the folded groups. 22 mutants caught.

**P2-4 Engagement-spec features not built** (spec in `research/engagement-spec.md`), in parts. Keep the ethics line in
[engagement.md](engagement.md).
- **P2-4a Backup reminder and the dead `sound` setting: done** (2026-11-13).
  - `shared/src/backup-nudge.ts` (pure rules) and `web/src/lib/backup-note.ts` (storage in its own key
    `interview-trainer.backup.v1`, memory fallback, `downloadBackup` / `copyBackup` that record only a real success). Home shows
    one quiet line below the practice rows after 5 practice days when no backup exists or the last is 14 days old; "Later"
    waits 7 days. Settings, Your data says "Last backup: 3 days ago" or "No backup made from this device yet". The data-safety
    banner's own download and copy buttons record too. The date is deliberately not in `Progress` (not exported, merged or
    migrated; a restored old backup cannot rewind it).
  - `Progress.settings.sound` is removed (nothing played it, no toggle, and the ethics line rules out auto-playing sound).
    `normalizeProgress` drops unknown keys, so saves and backups that still carry it load unchanged (tested).
  - `e2e/engage.ts`'s `seed` now also writes "just backed up" unless a spec passes its own note, so the reminder does not
    appear in specs about something else.
  - Tests: `shared/src/backup-nudge.test.ts` (12 incl. a property test), `web/src/lib/backup-note.test.ts` (7),
    `e2e/backup-nudge.spec.ts` (11); 11 mutants caught. Not covered by a test (storage is deliberately blocked in that
    scenario, so the note cannot be read back): that the data-safety banner's buttons record.
- **Still open in P2-4:** a **weekly recap** card; **teach-back** prompts (explain a concept in your own words, AI- or
  self-graded); a **transfer code / QR** to move progress to another device without a server (export already works; this is
  the convenient path). Next suggested: the weekly recap (pure derivation from `days`, no new stored data).

**P2-5 Competitor gaps not built** (from [COMPETITORS.md](COMPETITORS.md)): flashcards mode, difficulty adaptation
(questions carry `difficulty` but selection ignores it), voice answers for open questions, localisation (copy is British
English, number/date formatting is already locale-aware), and the **architecture board** question type (drag and connect
components, validated against a reference topology) that was deferred from the first day; the schema is a
discriminated union, so a new type is one schema entry, one grader, one component (see "adding a question type" in
[contributing.md](contributing.md)).

**P2-6 Role-targeted tracks**: **done** (2026-11-10).
- `shared/src/track.ts` is the one definition: `ROLE_FOCUS` lists each role's unit ids (backend: kotlin, coroutines, spring,
  jvm, data, messaging, testing; architect: ddd, scale, complexity, ai, system-design, security-observability; generalist and
  no role: none), `trackOrder` puts the focus units first with each group in course order, `nextLessonFor` finds the next
  undone lesson in that order, and `roleWeights` (`FOCUS_WEIGHT` = 1.5) replaces the old hand-written `ROLE_WEIGHTS`.
  Unit and lesson numbers stay the course's ("Unit 05", "5.2"), so they mean the same thing in Home, Library and lesson titles.
- Where the role now acts: Home lists the track first under "Your track · <role>" and the rest under "Other topics" (every
  unit and lesson stays one tap away); the first-run hero, "Up next", the summary's "Next lesson" and the open unit follow
  the track; Today's *new* questions come from the track first (due cards and weak spots are untouched, and it spills into
  the other units once the track is all seen); Mixed practice and Mock draw from focus units 1.5x as often; the Mock topic
  list groups "Your track" first with a one-line note; Settings names the track. Readiness gets the longer lists too, so
  the data, messaging, testing and system-design units now count for the roles they belong to.
- Nothing is stored beyond the role the plan already saved, so changing the role in the plan (or leaving the card empty)
  reorders everything at once; a generalist sees exactly what they saw before.
- Tests: `shared/src/track.test.ts` (16, including a property test and a check that every track unit exists in the real
  course), `e2e/tracks.spec.ts` (11); 15 mutants caught.
- Not done: the role does not lock or hide anything (a deliberate choice: a track is an order, not a gate); no per-role
  question sets or company tracks (not claimed, see COMPETITORS.md item 13); the focus lists are a judgement call, listed
  in one place so they are easy to change.

**P2-7 Kotlin for code questions**: challenges are JavaScript because they run in a browser sandbox. Real Kotlin
needs a server-side runner with container isolation (no network, CPU/memory/time limits, per-run sandbox), e.g. a
separate service behind the same run-code interface (`web/src/lib/run-code.ts`). Treat it as a security project.

**P2-8 Monetization** (nothing implemented). From [MONETIZATION.md](MONETIZATION.md): freemium "Pro" (free keeps all
non-AI lessons, streaks and review plus a small daily AI allowance; Pro adds stronger-model grading, AI mock
interviews, sync and extra tracks); price points about $9.99/month, $69/year, a $19 30-day "interview sprint" pass, a
capped lifetime founders edition. Cheapest first experiment: a licence-key paywall via a merchant of record (Paddle or
Lemon Squeezy) with no accounts, an AI quota enforced server-side, three randomised annual prices, launched with a
Show HN. Needs first: privacy-respecting analytics to measure retention and conversion, a server-side quota per
licence, and an entitlement check in the client. Accounts and cloud sync come later. Competitor prices in that doc are
unverified.

### P3: engineering hygiene

- **P3-1 Leftover hardening** [S2-13, S2-17, S2-19, S2-27]: the code worker can still `import()` same-origin URLs
  (harmless under the CSP, but tighten); unknown routes such as `#//` render Home (by design, could show "not found");
  import accepts a backup without a checksum (warn); keyword stuffing can pre-tick the offline self-check rubric (the
  learner only fools themselves; consider ignoring answers that are keyword lists).
- **P3-2 CI**: there is none. Add a workflow running `npm ci`, `npm run typecheck`, `npm run coverage`,
  `npm run content:check`, `npm run build`, `npm run build:standalone` and `npm run e2e` (install Chromium first).
- **P3-3 Screenshots in git**: 88 PNGs (~12 MB) regenerated by every e2e run bloat history. Keep a curated set, move the
  rest to CI artifacts or Git LFS, and make the suite write elsewhere by default.
- **P3-4 CSS layout**: styles are split across `web/src/styles.css`, `web/src/features-a.css`, `web/src/safety.css` and
  `web/src/ux2.css` (one file per work batch). Reorganise by design-system layer and component.
- **P3-5 Bundle size**: the single-file build is about 1 MB (course, font and app inline). Consider splitting content
  per unit (lazy import) or compressing the course payload. The web bundle also carries the zod schemas (about 70 kB since zod 4) although it uses
  them only as types: move `z.*` definitions out of the modules the web imports (e.g. a schema module imported only by
  the server, the content loader and tests), then check the bundle no longer contains zod.
- **P3-6 Flakes seen once**: e2e timeouts under heavy parallel load (several suites on one machine) and one unit failure
  during a coverage run that did not reproduce in 7+ runs and was not captured. If either reappears, record it (see the
  flaky-test policy in [testing.md](testing.md)).
- **P3-7 Main's merge commit message** says "10 units, ~330 questions"; the true numbers are 13 units and 307
  questions. Cosmetic; mention it in the next release notes.
- **P3-8 Docs drift**: README and the engagement doc carried stale "not built yet" lists until this handoff; keep the
  backlog here and link to it rather than repeating it.
- **P3-9 Move to its own repository**: `git subtree split -P interview-trainer -b trainer-only`, push to a new repo,
  then remove the folder here. Nothing outside `interview-trainer/` belongs to the app.

---

## 5. Gotchas for the next session

- **Parallel e2e runs need different ports.** The Playwright config reuses an existing server on the port, so two runs
  on the same port silently test the wrong build. Use `E2E_PORT=<free port> npx playwright test`.
- **e2e rewrites committed screenshots.** Commit them only when the UI changed on purpose; otherwise
  `git checkout e2e/screenshots`.
- **Ratchets are intentional.** Coverage thresholds, the option-balance ratchet, the contrast script and the README
  counts test fail loudly by design. Add tests or fix content; never lower a threshold to get green.
- **Changing `Progress`.** Prefer additive optional fields with defaults (`shared/src/extras.ts`,
  `shared/src/prefs.ts`) and update `normalizeProgress` and `mergeProgress`; bump the version only with a migration and
  a fixture test (see the `Progress` section of [architecture.md](architecture.md)).
- **Release**: bump the service-worker cache version (`web/vite-sw-plugin.ts`) so clients pick up the new shell, then
  rebuild and republish the hosted page as in section 1.
- **zod**: version 4 everywhere (shared schema, server, the SDK's structured-output helper). Import from `zod`.
- **AI calls cost money**; tests always use fakes. The e2e server is started with AI off.

## 6. Definition of done for any change

From [contributing.md](contributing.md): `npm run typecheck`, `npm run coverage`, `npm run content:check`,
`npm run build`, `npm run build:standalone` and `npm run e2e` all pass; new logic has unit tests, new behaviour has e2e
tests; docs updated; this handoff updated when an item here is finished or a new one is found.
