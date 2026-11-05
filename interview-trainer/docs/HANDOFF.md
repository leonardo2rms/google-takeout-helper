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
| `npm test` | 1,492 unit/content/docs tests (vitest) |
| `npm run coverage` | same, with the coverage ratchet (fails if coverage drops) |
| `npm run typecheck` | all workspaces + the e2e folder |
| `npm run e2e` | 302 Playwright tests at phone size; set `E2E_PORT` to a free port if anything else runs |
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
  `content/balance.test.ts` (correct answer strictly longest in at most 30% overall; it was 83%); British spelling;
  README counts match the course; docs examples and paths are valid (`content/docs.test.ts`).
- **Human-style review done:** two fact-check passes over all new content (1 hard error fixed, ~15 claims softened), a
  fact-check of the 458 rewritten answer options (0 accidental second answers, 9 fixes). Residual uncertainty: P1-7.

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
  "weakest topic" only once 3 topics are started; the onboarding **role** only weights readiness (see P2-6).
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

1,492 unit tests (coverage 99.7% statements / 97.6% branches; thresholds in the coverage config are a ratchet), 302
e2e tests including axe in light and dark, all hand-made mutants killed in the last round, typecheck clean, both builds
green, `npm test` green from a fresh clone. 88 committed screenshots in `e2e/screenshots` show every screen.

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

**P1-1 Remove the remaining answer-style tells in choice questions** [N-S1-2]
- *Why:* the training value of choice questions depends on not being gameable. After rebalancing lengths, the round-2
  audit measured: an option containing `(`, `;` or `:` is correct 53% of the time (chance 25%); options containing
  "or" 40%; 57% of distractors now carry a causal clause ("because ...", up from 14%), so "because" marks wrong
  answers; a hand-written rule scores 63.5% and a bag-of-words classifier 58.6%.
- *Where:* `scripts/content-lint.ts` (measure), `content/balance.test.ts` (ratchet), `content/` YAML, the
  "Balanced options" section of [content-guide.md](content-guide.md).
- *Done when:* the lint reports these features per unit; the ratchet bounds each (e.g. correct-given-feature within 25%
  ± 10 points, causal-clause share similar for answers and distractors) and a simple leave-one-out classifier stays near
  chance; offending options rewritten without changing answer keys; a fact-check pass confirms no distractor became
  defensible; full test suite green. *Size:* M-L (content-heavy).

**P1-2 Content clean-ups the audits listed** [S2-25, N-S2-13, round-1 content notes]
- Duplicate open question "Design a rate limiter for a public API..." in `content/06-scale.yaml` (lesson
  `scale-advanced`, question 9) and `content/11-system-design.yaml` (lesson `sd-estimation-traffic`, question 9):
  replace one.
- Near-duplicate topics: `spring-advanced/3` vs `data-sql/7` (lost update), `data-jpa/7` vs `scale-advanced/6`
  (cache-aside race), `spring-advanced/8` vs `testing-strategy/3` (test slices): differentiate or merge.
- Four `order` questions have lines that are identical once trimmed (`coroutines-flow/5`, `scale-resilience/7`,
  `complexity-reading/8`, `complexity-advanced/7`); they rely on indentation, which the round-1 UX audit also flagged
  (UX-22). Rewrite them so lines are unique without indentation, then indentation can be hidden in the pool.
- 34 of 36 multi-select questions are "3 correct of 5": vary the number of correct answers (changes keys, so
  re-fact-check).
- *Size:* M.

**P1-3 Verify what could not be verified in the sandbox**
- Real AI grading and the tutor with `ANTHROPIC_API_KEY`: prompt quality, latency, cost per call, refusals, the
  implausible-verdict fallback rate. Build a small eval (about 60 human-labelled answers across rubrics) comparing
  Haiku, Sonnet and Opus at low effort; [MONETIZATION.md](MONETIZATION.md) has the cost model and suggests the tiers.
- The hosted page in a real claude.ai viewer: the `sample` consent flow, the code-runner Web Worker inside the
  sandboxed frame (it may be refused; the UI then says "Could not start the code runner"), `downloads` saves of JSON
  and PNG, and that `.ics` is still refused by the viewer's extension allowlist (the button is hidden there).
- The phone OS keyboard with the code editor (only emulated so far).
- *Size:* S-M.

**P1-4 Upgrade zod (security advisory)**
- `npm audit` reports one high-severity advisory: zod below 4.6.6 (memory exhaustion validating oversized constrained
  collections). The shared schema uses zod 3 (`shared/src/schema.ts`); the server grader already imports `zod/v4` for
  the Anthropic SDK helper. Exposure is limited (32 kB body limit, schemas bounded) but it should be fixed: migrate to
  zod 4 (watch `.default()` and transform typing, `z.number().finite()`), keep all tests green. *Size:* M.

**P1-5 Resume a lesson after quitting or pressing Back** [S2-15, N-18]
- Quitting (confirmed) or the browser Back button discards an in-progress lesson; lessons are 7-10 questions, long on
  a phone. Persist the in-progress session (questions, answers, attempts, XP so far) in `Progress` or a separate key,
  offer "Resume" on Home, and intercept Back inside a lesson (history state) to ask before leaving.
  Files: `web/src/screens/Lesson.tsx`, `shared/src/session.ts`, `web/src/lib/store.ts`. *Size:* M.

**P1-6 Decide and enforce the XP / goal farming rule** [S2-11]
- Repeating one question (Library "practise this one", challenge) earns XP and goal credit each time: 8 repeats give 80
  XP and a full goal. Product decision needed (e.g. a question counts once per day toward the goal and earns reduced XP
  after the first correct answer of the day). Logic in `shared/src/goal.ts` / `shared/src/session.ts`. *Size:* S.

**P1-7 Re-check version-sensitive facts against official docs**
- Checked from knowledge only during the fact-check passes: `@MockBean` vs `@MockitoBean` (Spring Boot 3.4+),
  `flatMapLatest` still `@ExperimentalCoroutinesApi`, `NESTED` propagation with JPA, the `@DslMarker` semantics wording,
  the `@ConfigurationProperties` relaxed-binding example (`APP_MAXRETRIES`), the Kotlin 1.7 exhaustive-`when` change,
  `spring.threads.virtual.enabled`, JDK 21 virtual-thread pinning, Flyway checksum validation, Micrometer tag rules, the
  `-Xlog:safepoint` sample line, Loom carrier-pool sizing. Grep the content for each and confirm or soften.
  *Size:* S.

### P2: product improvements

**P2-1 Home for returning users** [N-12, UX-15]: after a few lessons Home is a stack of equally weighted rows (Today,
Daily challenge, Review, Mixed, Progress, Mock, Library) and the curriculum starts far down. Give it a clear hierarchy
(one primary action, secondary practice modes, the curriculum reachable in one tap; "Show all units").

**P2-2 Spread the first reviews** [N-4 remainder]: three lessons on day 1 leave about 25 cards due on day 2. Spread
first-review dates over 1-3 days (this changes intervals asserted in the SRS tests; update them deliberately).

**P2-3 Long pages and pacing** [N-14, N-15, N-17, UX-29, UX-34, N-8]: Settings is ~1,760px (shorten copy, group
sections); the Progress page for heavy users is ~5,000px (group badges, fold mock history further); rank thresholds are
too low (Engineer after 2 lessons); mock results need saving explicitly (autosave once self-assessment is done); the
summary's "of 8 today" count ignores follow-up answers (decide whether follow-ups count).

**P2-4 Engagement-spec features not built** (spec in `research/engagement-spec.md`): a **weekly recap** card;
**teach-back** prompts (explain a concept in your own words, AI- or self-graded); a **transfer code / QR** to move
progress to another device without a server (export already works; this is the convenient path); a **weekly backup
nudge**; optional **sound** (a `sound` setting already exists in `Progress.settings` but nothing plays it and there is no
toggle: either implement or remove it). Keep the ethics line in [engagement.md](engagement.md).

**P2-5 Competitor gaps not built** (from [COMPETITORS.md](COMPETITORS.md)): flashcards mode, difficulty adaptation
(questions carry `difficulty` but selection ignores it), voice answers for open questions, localisation (copy is British
English, number/date formatting is already locale-aware), and the **architecture board** question type (drag and connect
components, validated against a reference topology) that was deferred from the first day; the schema is a
discriminated union, so a new type is one schema entry, one grader, one component (see "adding a question type" in
[contributing.md](contributing.md)).

**P2-6 Role-targeted tracks**: onboarding asks for a role (`backend-kotlin`, `staff-architect`, `generalist`) but it
only weights the readiness score (`shared/src/mastery.ts`). Use it to order units, pick Today's new material and
suggest mock topics.

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
  per unit (lazy import) or compressing the course payload.
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
- **zod versions**: the schema is zod 3, the grader's structured-output helper uses `zod/v4` (P1-4).
- **AI calls cost money**; tests always use fakes. The e2e server is started with AI off.

## 6. Definition of done for any change

From [contributing.md](contributing.md): `npm run typecheck`, `npm run coverage`, `npm run content:check`,
`npm run build`, `npm run build:standalone` and `npm run e2e` all pass; new logic has unit tests, new behaviour has e2e
tests; docs updated; this handoff updated when an item here is finished or a new one is found.
