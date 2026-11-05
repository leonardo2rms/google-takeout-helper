# Contributing

How to work on the Interview Trainer without breaking it. Companion docs: [architecture.md](architecture.md) (how it is built),
[testing.md](testing.md) (how it is verified), [content-guide.md](content-guide.md) (how to write questions).

## Setup

You need Node 22 (the server uses `--env-file-if-exists`, added in 22.9) and npm. Everything below runs from the `interview-trainer/` folder.

```bash
npm install                       # installs all workspaces (shared, server, web)
cp .env.example .env              # optional: add ANTHROPIC_API_KEY to try AI grading and "Ask why"
npm run dev                       # web on http://localhost:5173, API on :8787 (open the web URL)
npx playwright install chromium   # once per machine, only needed for npm run e2e
```

The app works fully without an API key (open questions fall back to self-assessment); you only need one to see the AI paths with a real model.
The e2e suite never uses a key. Layout of the repository: see the "Layout" section of the [README](../README.md).

Useful loops:

| I am changing | Run |
|---|---|
| a rule in `shared/` | `npx vitest shared` (watch mode) |
| a route or the grader/tutor | `npx vitest server` |
| a screen | `npm run dev`, phone-sized window, then the relevant e2e spec |
| a question | `npm run dev` + `npm run content:check` + `npx vitest run content` |

## Branches and commits

- **One topic per branch**, named by kind: `feat/<topic>`, `fix/<topic>`, `content/<topic>`, `chore/<topic>`, `docs/<topic>`. Branch from the latest integrated branch, not from a stale one. Integration happens by merge (`Merge feat/x: one-line summary`); do not push straight to the main line.
- **Commit often, in logical steps** that each build and pass the tests: "add the schema", "add grading", "add the UI", "add e2e", "document". Do not mix a refactor with a behaviour change.
- **Commit messages**: a short imperative subject that says *what and why it matters* ("Fix parseNumber for European-style numbers (1.234,5)", not "fixes"), a blank line, then a body for anything non-obvious: the failure that was observed, the cause, the alternative you rejected. Reference the file or rule, not the line.
- When a commit was written with AI assistance, end it with the trailer lines the tooling provides (`Co-Authored-By: ...`, `Claude-Session: ...`).
- A bug fix includes its **regression test in the same commit** and says in the message how it was found.
- Do not commit: `.env`, `coverage/`, `dist/`, `web/dist`, `e2e/.results`, `playwright-report` (already in `.gitignore`).
- Screenshots in `e2e/screenshots/` are committed on purpose, but only update them when the UI intentionally changed ([testing.md](testing.md#end-to-end-tests)).

## Definition of done

A change is done when **all** of these hold. Run them in this order; the first four take under a minute.

```bash
npm test                 # unit, property, HTTP, content and docs tests
npm run typecheck        # shared, server, web (including tests) and e2e
npm run build            # tsc + vite build; also validates every YAML file
npm run content:check    # loads the course, prints counts per unit and type
npm run coverage         # same tests with thresholds; must not fall below the ratchet
npm run e2e              # when the UI, content, service worker or server routes changed
```

- [ ] Tests exist for the behaviour you added or fixed, and fail without your change (revert the fix once and see it go red).
- [ ] No test was weakened, skipped or deleted to get green. A property-test failure was understood, not narrowed away.
- [ ] No new `any`, no `@ts-ignore`; types do the exhaustive checking (see the new-type list below).
- [ ] Rules are in `shared/` with `today`/seed passed in; no `Date.now()`, `Math.random()` or storage access in `shared/`.
- [ ] Progress changes follow [the migration rules](architecture.md#the-progress-model-and-its-migrations): additive, validated, merged, covered by the arbitraries.
- [ ] Anything learners read was fact-checked ([checklist](content-guide.md#fact-checking-checklist)) and works on a 390 px screen; interactive things are keyboard-operable and labelled ([accessibility.md](accessibility.md)).
- [ ] Docs updated where behaviour, commands or contracts changed. `content/docs.test.ts` fails if a documented `npm run` script or path no longer exists.
- [ ] If you touched the service worker (`web/src/sw/sw.ts`, `web/src/lib/sw-logic.ts`) or `web/vite-sw-plugin.ts`, run `npm run e2e -- e2e/service-worker.spec.ts`; the cache version needs no bump ([Releasing](#releasing)).
- [ ] Upstream (model/API) errors still never reach the client; user-controlled text still goes through the fenced prompt builders.

## Adding a question type end to end

The question model is a discriminated union on `type`, so most of the work is TypeScript telling you where you forgot something. Call the new type `X`. The order below is the order that keeps the build green at each step.

1. **Schema** (`shared/src/schema.ts`): add `XQuestion = z.object({ type: z.literal('x'), ...common, ... })` with tight limits (`min`/`max`, `.default()`s), add it to `RawQuestion`, export the type (`type XQ = Extract<Question, { type: 'x' }>`), and add a `case 'x'` to `validateCourse` for the rules zod cannot express (answers exist, no ambiguity). Add the type to the `TYPES` list in `shared/src/achievements.ts` (the "every format" badge says nine in its description; update it).
2. **Grading** (`shared/src/grade.ts`): a pure `gradeX(q, answer): boolean` (or a coverage score), exported through `shared/src/index.ts`. Make it total: wrong shapes, `null`, `NaN` and empty input return false, never throw.
3. **Everything the compiler now flags** (each is an exhaustive `switch` or a `Record<Question['type'], ...>`):
   - `shared/src/explain.ts` `describeQuestion` (what the tutor is told the question and answer were);
   - `shared/src/challenge.ts` `CHALLENGE_WEIGHTS` (0 excludes it from the daily challenge); `shared/src/library.ts` `TYPE_LABEL`;
   - `web/src/lib/evaluate.ts` (`Answer` union and the `evaluate` case) and `web/src/lib/explain.ts` `describeLearnerAnswer`; `web/src/lib/mock-marking.ts` if "answered" needs a special rule;
   - the labels in `web/src/screens/Lesson.tsx` and `web/src/components/MockRun.tsx`.
4. **UI**: a component in `web/src/components/<Name>Question.tsx` that reports an `Answer` (or `null` while incomplete), takes `checked`/`seed` like its siblings, is keyboard-operable with labels, and renders correct/wrong states without relying on colour; mount it in `Lesson.tsx` and `MockRun.tsx`; show the right answer in `Feedback.tsx` when wrong.
5. **Validation / content**: write at least three real questions in a lesson; extend `content/content.test.ts` if the type has its own invariant (as `code` and `open` do); `npm run content:check`.
6. **Tests**: unit tests for `gradeX` and the schema/`validateCourse` rules (`shared/src/schema.test.ts`, `logic.test.ts`); add the type to `web/src/lib/evaluate-all-types.test.ts` (it plays the key of every question in the course through `evaluate`); e2e: teach `e2e/solve.ts` (`KICKER`, `answerCorrectly`, and `answerWrongly`) to play it, so `question-types.spec.ts` covers it and `a11y.spec.ts` runs axe over it.
7. **Docs**: a section and a complete example in [content-guide.md](content-guide.md) (it is validated by a test), the question-type table in the README, a note in [architecture.md](architecture.md) if it changes a contract, and a new screenshot set from the e2e run.

## Changing the API contract

`shared/src/api.ts` and `shared/src/explain.ts` define the wire types for both ends. When you change a request or response: update the zod schema first, then `server/src/app.ts`, then the clients in `web/src/lib` (`api.ts`, `explain.ts`, `sample-grader.ts`), then the tests in `server/src/*.test.ts` (including the failure statuses) and the contract section of [architecture.md](architecture.md#grading-contract). Keep these invariants: the client never sends a rubric or a correct answer, the server answers from its own copy of the course, every error body is generic, and a missing server degrades to self-assessment.

## Releasing

There is no CI-driven release in the repository; a release is "build, then publish the output". Before building, check the [definition of done](#definition-of-done).

**Server + static (the normal deployment).** One Node process serves the API and the built web app.

```bash
npm install
npm run build        # -> web/dist (static files; fails on invalid content)
PORT=8787 ANTHROPIC_API_KEY=... npm start   # serves web/dist and /api on one port
curl -s localhost:8787/api/health           # {"ok":true,"ai":true,...}
```

- `npm start` runs the TypeScript directly with `tsx`; there is no separate compile step for the server. It loads `content/*.yaml` once at start-up, so **restart the process to publish content changes** to the rubric lookups.
- Environment: see the table in [architecture.md](architecture.md#build-variants). Use `/api/health` for the load balancer check; `ai:false` means no credentials were found.
- Put it behind HTTPS (service workers and notifications require a secure context except on localhost) and ideally behind a reverse proxy that serves both the app and `/api` from **one origin**. The server deliberately sends **no CORS headers**; if you host `web/dist` on a different origin from the API you must add CORS or a proxy rule, and build the web app with `VITE_API_URL` pointing at the API.
- **Rate limits are per `req.ip`.** By default Express's `trust proxy` is off and `X-Forwarded-For` is ignored, which is right for a directly exposed server. Behind a reverse proxy every learner would share the proxy's address and one bucket (30 gradings and 20 tutor questions per minute), so set `TRUST_PROXY=1` (the number of proxies in front, or an Express `trust proxy` value) and make the proxy forward the client address. Expired rate-limit windows are pruned as the table grows.
- The server already sends `Cache-Control: no-cache` for `sw.js`, `index.html` and the manifest, `immutable` for `/assets/*`, a CSP and gzip. A CDN in front must honour those: it must not cache `sw.js` or `/` for long, and must not rewrite a missing `/assets/*` to `index.html` (answer 404; the service worker refuses to cache it, but the page would still be blank until the next load). `PORT=` (empty) means the default 8787; an invalid `GRADER_EFFORT` stops the server at start-up.
- A **static-only** deployment (`web/dist` on any file host) is valid: the app runs offline-first and simply has no AI (the health request fails and the UI hides it).
- Secrets live only in the server environment (`ANTHROPIC_API_KEY`/`ANTHROPIC_AUTH_TOKEN`); nothing about them reaches the browser, and error details stay in the server log.

**Single-file build.** `npm run build:standalone` writes two files (about 0.9 MB each, no server, no service worker):

- `dist/interview-trainer.html` is a **fragment** for hosts that wrap one snippet in their own page (a claude.ai artifact). It has no doctype or `<html>`; it starts with `<meta charset>` and a viewport meta, which are harmless inside a host page and make the bytes right when something serves the snippet as is.
- `dist/interview-trainer.standalone.html` is a **complete document** (doctype, `lang`, charset, viewport, theme colours). Use it for opening from disk (`file://`) or any static host. The fragment opened directly renders in quirks mode, reads UTF-8 as Latin-1 ("Â·") and lays out 980 px wide on a phone.

AI works only when the host provides `window.claude` with the `sample` capability (a claude.ai artifact does); a `sample` call that does not answer within 25 s falls back to the offline self-check. Elsewhere it is a plain offline trainer. Progress is stored per origin; in a sandboxed frame `localStorage` throws, the progress then lives in memory for the tab, and the app says so and offers *Download backup* / *Copy backup text* / the text to select by hand. Do not rely on `window.confirm` in the UI: sandboxed hosts block it without `allow-modals`.

**Service worker versions.** There is nothing to bump. `web/vite-sw-plugin.ts` derives the cache name from a hash of every file in the build, so each deploy that changes any byte installs a new worker, which pre-caches the new files, then replaces the old cache and takes over. Open pages show "A new version is ready" and reload when the learner chooses.

- After a release, verify in the browser: DevTools, Application, Service Workers shows the worker activated, Cache Storage has exactly one `interview-trainer-<12 hex>` cache, and offline mode still loads the app.
- To force every client to refetch (a bad deploy), ship any change to a shipped file; there is no separate lever.
- Never cache `/api/` responses in the worker; grading must reach the server or fail fast.
- Never serve `index.html` for a missing static file. `e2e/service-worker.spec.ts` and `server/src/static.test.ts` guard this.

**Data-format releases.** If `Progress` changed, ship with the migration tests green and the arbitraries updated. Old app versions must keep refusing newer data (`future-version`) rather than overwriting it; never delete or rewrite the previous storage key. Mention the change in the release notes.

**Rolling back** a build is safe for learners: the storage key is unchanged for additive changes, and v1 data is never deleted. Publishing the old build again is enough for the worker, since its files hash to the old version and clients treat it as a new worker. An old build that meets data written by a newer one parks that data (it is never overwritten before a copy exists) and says so; the data comes back through Settings, Recovered data.
