# Testing

What is tested, how, and what you must do before you call a change finished. For the architecture the tests protect see [architecture.md](architecture.md);
for the full definition of done see [contributing.md](contributing.md).

## The pyramid used here

```
            /\            e2e (Playwright, ~190 tests, 12 files): the real built UI at phone size, AI off.
           /  \           Slow (minutes), few, valuable: wiring, layout, accessibility, flows across screens.
          /----\
         / HTTP \         server routes against a real Express instance on a random port, fake grader/tutor.
        /--------\
       / content  \       the course itself: schema, rubrics, code solutions, hints, docs examples.
      /------------\
     / unit + props \     shared/, server/ internals and web/src/lib: ~1100 fast tests, 5 to 10 seconds,
    /________________\    plus fast-check property tests for the data-integrity code.
```

Principles:

- **Put logic where it can be unit tested.** Rules live in `shared/` (pure functions of state, input and `today`); `web/src/lib` holds the thin platform glue; components are thin on purpose. That is why there are no component tests with a DOM library: the components are exercised by e2e, and everything they decide is decided by tested functions.
- **Test behaviour and invariants, not implementation.** Prefer "a clock that moves backwards never lowers a streak" to "function X calls function Y".
- **Time and randomness are inputs.** Pass `today`, a seed, a fake clock. Never `sleep`.
- **No network, ever, in unit tests.** The Anthropic client is replaced by a fake object with the same `messages` surface; `fetch` is stubbed in `web/src/lib` tests.
- **A failing test is information.** Never weaken, skip or delete a test to get green; fix the code or, if the expectation was wrong, say why in the commit message.

## Running each layer

All commands run from `interview-trainer/`.

| Layer | Command | Notes |
|---|---|---|
| Everything fast (unit, props, HTTP, content, docs) | `npm test` | `vitest run`; about 10 s |
| Watch mode while developing | `npx vitest` | re-runs affected files |
| One package or file | `npx vitest run shared` / `npx vitest run server/src/routes.test.ts` | path filters |
| One test by name | `npx vitest run -t "streak"` | substring of the test name |
| Content only | `npx vitest run content` and `npm run content:check` | the second prints counts per unit and type |
| Type checking (all packages and e2e) | `npm run typecheck` | web test files are type checked too |
| Coverage with thresholds | `npm run coverage` | prints a summary and per-file table, writes `coverage/lcov.info` (git-ignored); fails below the ratchet |
| Production build | `npm run build` | runs `tsc` then `vite build`; also validates the YAML |
| Single-file build | `npm run build:standalone` | writes `dist/interview-trainer.html` (fragment) and `dist/interview-trainer.standalone.html` (complete document) |
| End to end | `npm run e2e` | builds, serves with AI off, drives headless Chromium. First time on a machine: `npx playwright install chromium` |

Where the unit tests live:

| Area | Files | What they cover |
|---|---|---|
| `shared/src/*.test.ts` | one per module (`srs`, `streak`, `day`, `migrate`, `backup`, `mastery`, `events`, `daily`, `mock`, ...) | the learning rules with exact examples |
| `shared/src/properties-*.test.ts` | `properties-progress`, `properties-logic` | fast-check properties (below) |
| `shared/src/schema.test.ts`, `load-content.test.ts` | the content schema, every cross-field rule, loading from a directory | |
| `server/src/*.test.ts` | `app` and `explain` (grading and tutor endpoints), `routes` (parsing, limits, headers, static serving, rate-limit windows, error hygiene), `clients` (the Anthropic wrappers) | HTTP via `fetch` against `app.listen(0)` |
| `web/src/lib/*.test.ts` | `store` and `store-safety` (fake `localStorage`, quota errors and recovery, unreadable data, blocked storage, safety copies, multi-tab events), `sw-logic` and `sw-register` (fake `caches`/`fetch`: what may be cached, all-or-nothing install, update notice), `sample-timeout`, `evaluate*`, `run-code` (fake Worker), `reminders` (fake `Notification`), `hooks` (a11y/today with stubbed React), `share-image` (recording canvas), API clients, `highlight`, `format`, `toasts`, `mock-marking` | the platform glue |
| `content/*.test.ts` | the course and the docs | see below |

### Property-based tests

`fast-check` generates hundreds of inputs per property and shrinks failures to a minimal case. Used where the input space is large and the invariant is clear:

- **Progress data** (`properties-progress.test.ts`): `normalizeProgress` is total and idempotent over arbitrary junk; valid data passes through unchanged; v1 to v2 keeps every v1 field and is idempotent; `mergeProgress` is commutative and associative for the fields it treats symmetrically, never loses anything, and merging with yourself is the identity; backups round-trip and arbitrary text never throws.
- **Logic** (`properties-logic.test.ts`): day arithmetic round trips; day keys sort like time in several time zones across DST; streak settlement matches its prediction over random practice histories including a backwards clock; scheduled cards stay within their bounds, intervals never shrink for repeated good answers, `hard <= good <= easy`; `parseNumber` reads back what it is shown; `deepEqual` agrees with canonical JSON; daily/mock/challenge selection is deterministic, duplicate-free and respects its caps.
- Generators live in `shared/src/arbitraries.ts` (`rawProgressArb` produces damaged profiles, `progressArb` produces valid ones). When you add a `Progress` field, extend them.

When a property fails, fast-check prints `{ seed, path }` and a counterexample. Replay it with `fc.assert(property, { seed, path })`, write the counterexample as an ordinary example test, fix the bug, keep both.
Do not "fix" a failing property by narrowing the generator unless the narrowed input is genuinely outside the documented contract (and say why in a comment, as `arbitraries.ts` does for duplicate mock records).

### Mutation checking by hand

Coverage says code ran, not that a test would notice if it were wrong. For the functions whose bugs would hurt most, flip an operator and confirm a test fails:

```text
srs.ts     base.due > now            ->  >=        (early review lengthens the interval)
srs.ts     isDue: due <= today       ->  <
streak.ts  advanceStreak gap <= 0    ->  < 0       (same day / clock moved backwards)
streak.ts  freezesNeeded > p.freezes ->  >=        (freeze budget off by one)
session.ts attempt <= MAX_RETRIES    ->  <
grade.ts   estimate: <= tolerance    ->  < (no epsilon)
grade.ts   spot: exact selection     ->  drop the size check
day.ts     weekday offset            ->  +3 instead of +4
migrate.ts days kept: >= DAYS_KEPT   ->  >
backup.ts  merge xp: max             ->  min
code-runner.ts deepEqual key count   ->  dropped
```

All of the above were killed by the suite when this was last done. Do this for anything new and critical: edit, run `npm test`, expect red, revert (`git checkout -- file`).

## End-to-end tests

`npm run e2e` runs Playwright (`playwright.config.ts`): `e2e/*.spec.ts`, Chromium at **390 x 844, device scale factor 2, touch, reduced motion** (so there are no fade-ins and screenshots show the settled state). The `webServer` is `npm run build && npm run start` with the AI keys forced to empty strings, so open questions always take the offline self-assessment path: runs are deterministic and never spend credits.

**Ports and workers.** The server port comes from `E2E_PORT` (default 4173). `fullyParallel` is on with 4 workers. Several runs on one machine (other worktrees, agents, CI shards) must not share a port:

```bash
E2E_PORT=4182 npx playwright test --workers=2
E2E_PORT=4182 npx playwright test e2e/a11y.spec.ts e2e/a11y-behaviour.spec.ts
E2E_PORT=4182 npx playwright test -g "every lesson"
```

`reuseExistingServer` is on outside CI, so a server left over on the same port is reused (which may serve an old build: stop it or pick another port). The `webServer` command runs `npm run build` into `web/dist` of the current checkout, so two runs *in the same worktree* race on that folder: give each run its own worktree, or run one at a time.

**How the specs are written.**

- State is seeded by writing `localStorage` blobs (`e2e/engage.ts`), so every test starts from an exact profile instead of clicking through onboarding.
- Time is controlled with `page.clock` (a review that becomes due tomorrow, a mock that times out), never with real waiting.
- Correct answers are read from the YAML (`e2e/solve.ts`, `answerCorrectly`) using only what a learner can tap, so tests stay in sync with the content. `question-types.spec.ts` plays **every question type** and **every lesson in the course** perfectly, checks the XP, the feedback, that wrong answers come back, and that nothing scrolls sideways on a phone.
- Network failures and the AI endpoints are faked in the browser (`page.route`), never reached.
- Robustness (QA audit regressions, each fails without its fix): `data-safety.spec.ts` (failed writes, blocked storage, unreadable data and Recovered data), `service-worker.spec.ts` (404 for missing assets, version-keyed pre-cache, offline, update notice), `standalone.spec.ts` (builds the single file, opens it from `file://`, a hanging `sample` host), `session-credit.spec.ts` (summary XP, challenge across midnight), `mock-clock.spec.ts` (timer vs wall clock), `routes-robustness.spec.ts`.
- Accessibility: `a11y.spec.ts` runs axe over every screen and state (fails on serious/critical issues; colour contrast is the one tracked exception, see [accessibility.md](accessibility.md)); `a11y-behaviour.spec.ts` covers what axe cannot see.
- Layout guards: `layout.spec.ts` renders every match question at 390 and 320px (empty, paired, checked) and fails if any word is split across lines, an item overflows or is absurdly tall (the redesign once turned items with inline code into single letters). `confirm.spec.ts` covers the in-app confirmation dialog (focus trap, Escape, focus restore, no native dialog even where `confirm()` is blocked). `first-run.spec.ts` covers the first-run Home (one next step visible without scrolling, everything still reachable) and the numbers on Home.
- Prefer selectors a learner could use: roles, labels and visible text. Reach for CSS classes or DOM structure only when there is no accessible alternative (a few helpers in `e2e/solve.ts` still do), because those break on redesigns.

**Screenshots** are written to `e2e/screenshots/` by `question-types.spec.ts` and the feature specs (`<type>-1-answered.png`, `-2-correct.png`, `-3-wrong.png`, `badge-toast.png`, ...). They are **committed on purpose**:

- they are a reviewable visual record: a UI change shows up as a binary diff in the pull request, so reviewers can see what changed without running the app;
- they document the product for people who have not set it up (the README and docs point at them);
- a redesign can be reviewed screen by screen against the "before".

They are deterministic (fixed viewport, scale factor, reduced motion, seeded state, `page.clock`), so an unchanged UI produces unchanged files. Commit new screenshots only when the UI intentionally changed; if an unrelated test rewrites them, that is a determinism bug to fix, not a diff to merge. The set is large (about 9 MB), so avoid churn.

## Content tests: keeping the course honest

`content/content.test.ts` and `content/hints.test.ts` run on the real YAML:

| Check | Why |
|---|---|
| Loads, ids unique, `validateCourse` clean (inside `loadCourse`) | structure and cross-field rules (answers exist, blanks match, spot lines are real, matches unambiguous) |
| Every lesson mixes >= 3 question types and has an `open` question | variety and a speaking exercise in every lesson |
| **Every `code` solution passes all its tests and the starter does not** (and fails cleanly, no fatal error) | an answer key that is not executable is a bug waiting to happen; a pre-solved starter is no exercise |
| Every code question has a Kotlin equivalent and a hidden test | transfer to the real interview language; performance/edge cases that cannot be pasted |
| **Open questions' model answers satisfy their own keyword rubric**, follow-ups included | offline learners would otherwise be told the perfect answer is incomplete |
| A stock "I do not know" scores below the pass mark on every rubric | keywords like `the` or `data` would make the self-check meaningless |
| Follow-up coverage: every `point` exists, and a question with follow-ups has both a `missed` and a `covered` one | no dead branches in the interviewer's choice |
| Hints are short, differ from the explanation and do not contain the literal answer; every question type has some | a hint that gives the answer is not a hint |
| `docs.test.ts`: YAML examples in the content guide validate; `npm run` scripts and repository paths mentioned in the docs exist | documentation that cannot rot silently |

These run in `npm test`, so a content PR gets the same gate as code. They cannot prove an answer is *true*: that is the fact-checking checklist in [content-guide.md](content-guide.md#fact-checking-checklist).

## Flaky-test policy

A flaky test is a bug in the test or in the code, and it erodes trust in every other test. The rules:

1. **No retries to hide it.** `playwright.config.ts` has no `retries`; do not add one. A test must pass or fail for a reason.
2. **Never fix with a sleep.** Wait on an observable condition (a role/text appearing, `expect(...).toBeVisible()`, `page.clock` ticks). Unit tests use fake timers and injected clocks.
3. **Reproduce first.** Run the test in a loop (`npx playwright test -g "name" --repeat-each=20`, `npx vitest run --retry=0 file` a few times), and for property tests replay the printed `{ seed, path }`.
4. **Find the cause**: shared mutable state between tests (a module-level store: use `vi.resetModules()` as `store.test.ts` does), real time or randomness leaking in, port collisions (use `E2E_PORT`), order dependence, animation (e2e runs with reduced motion), a race against `page.clock`.
5. **If it cannot be fixed the same day**, quarantine it: `test.fixme('reason + issue link', ...)` (Playwright) or `it.skip` with a comment naming the cause and an owner, in the same PR that notices it, and open an issue. A quarantined test is a debt with a name, not a silent skip, and it must not stay for more than a couple of weeks.
6. **Never merge on "re-ran and it passed".** If a failure was environmental (network, browser download), say so in the PR.

## Coverage and the ratchet

`npm run coverage` runs the whole suite under `@vitest/coverage-v8` and measures `shared/src/**`, `server/src/**` and `web/src/lib/**`. Excluded, deliberately: test files and test helpers (`__fixtures__`, `fixtures.ts`, `arbitraries.ts`), barrel `index.ts` files, `server/src/main.ts` (process entry point), `*.worker.ts` (needs a real Worker; covered by e2e), and all of `web/src/components` and `web/src/screens` (UI; covered by e2e, not by line counts).

Thresholds (global and per package, in `vitest.config.ts`) are a **ratchet** set slightly below what the suite achieves:

| Package | Statements | Branches | Functions | Lines | Achieved when set |
|---|---|---|---|---|---|
| `shared/src` | 99 | 96 | 99 | 99 | 99.7 / 97.0 / 99.8 / 100 |
| `server/src` | 99 | 96 | 99 | 99 | 100 / 97.2 / 100 / 100 |
| `web/src/lib` | 98 | 97 | 99 | 98 | 99.8 / 98.7 / 100 / 99.7 |

Starting point (before the coverage work): shared 97.8 / 94.1 branches, server 93.5 / 75.0, web/lib 9.7 / 16.3.

- A run below any threshold fails `npm run coverage` (and should fail CI).
- **Raise** the numbers whenever coverage goes up for good; **never lower** them to make a run pass. Add the missing test.
- Coverage is a floor, not a goal. The meaningful gate is the mutation check above and review: a test that executes a line without asserting on it is padding.
- The HTML/LCOV report (`coverage/`) is for finding untested branches: open `coverage/lcov.info` in your editor's coverage view, or read the per-file table printed by the command.

## Before you push

Run, in this order (details in [contributing.md](contributing.md#definition-of-done)):

```bash
npm test && npm run typecheck && npm run build && npm run content:check && npm run coverage
npm run e2e        # whenever UI, content, the service worker or the server routes changed
```
