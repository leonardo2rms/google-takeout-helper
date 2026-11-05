> **Archived research, snapshot.** Round-1 adversarial QA and security audit (storage, multi-tab, clocks, XSS, sandbox, server, PWA, single-file build, content integrity). All S0 and S1 findings were then fixed except the content tells, which moved to round 2.
>
> Written on 2026-11-04/05 while this app was being built, by an agent working from the brief described in [the handoff](../HANDOFF.md). It is kept as written so later decisions can be traced back to it. Paths such as `$S/...`, `$QA/...`, `$QA2/...` or `/tmp/claude-0/...` point at screenshots and scripts in that session's scratch space and are **not** in the repository. For what has since been built or fixed, the handoff is the source of truth.

# Interview Trainer: adversarial QA and review report

Branch under test: `claude/engineer-interview-training-82crnr` merged into the worktree (HEAD = merge). Production bundle served by `npm run start` on :4194 (AI off), Chromium 1194 via the repo's Playwright 1.56.1, phone viewport 390x844 unless noted.
Scratch dir (all repro scripts, no repo file touched): `/tmp/claude-0/-home-user-google-takeout-helper/959a0803-75f1-5c6e-8ab6-3e44ad11da40/scratchpad/audit-qa/` (referred to below as `$QA`).
Run any `tNN_*.mjs` with `node $QA/<file>` while a server is up on :4194 (`cd interview-trainer && ANTHROPIC_API_KEY= PORT=4194 npm run start`). `.ts/.mts` scripts run with `cd interview-trainer/server && npx tsx $QA/<file>`. `course.json` is a dump of the course made by `dump.ts`.

Confidence: **H** = reproduced here (script given), **C** = read in code and consistent with a repro of a neighbouring behaviour, **L** = code reading only.

Counts: **S0: 2, S1: 10, S2: 27.** Top 5 at the end of S1.

Note on the build: `npm run build` fails at this commit (finding S1-1), so I built with `cd web && npx vite build` (same bundle, skips `tsc`). Everything below was tested on that bundle.

---

## S0: data loss / security

### S0-1  Newer-version or corrupt progress is destroyed when the backup copy cannot be written; the backup key is overwritten by a second corruption the same day
- Area: persistence (`shared/src/storage.ts`, `web/src/lib/store.ts`). Confidence H (unit-level), C (browser).
- Repro: `cd interview-trainer/server && npx tsx $QA/unit_storage.ts`.
  Storage holds `{"version":3,...}` (written by a newer build); `setItem` for the `...backup-YYYYMMDD` key throws (quota, or disabled). `loadProgress` correctly sets `protectedRaw`, but `saveProgress` ignores the failed backup write and still overwrites `KEY_V2`.
  Output: `backedUp = false ; main key now holds xp 10 ; backup exists = false ; newer-version data survives anywhere? false`.
  Part B of the same script: two different unreadable values on the same day, `backup-20261105` ends up holding only the second one (first is lost).
- Observed: the only copy of the newer/corrupt data is gone. Expected: never overwrite a protected value unless the backup write succeeded (keep the in-memory profile and retry, or refuse to write and tell the learner).
- Root cause: `shared/src/storage.ts:75-77` (`backedUp = safeSet(...)`, then unconditional `safeSet(storage, KEY_V2, raw)`); `web/src/lib/store.ts:64-65` clears `protectedRaw` after the first commit whatever happened; `backupKey(today)` has no uniqueness (`storage.ts:7`).
- Fix: in `saveProgress`, `if (protectedRaw !== null && !safeSet(backup)) return { raw: protectedRaw, backedUp: false, skipped: true }` and keep `protectedRaw` in the store until a backup succeeds; make the backup key unique (`-HHMMSS` or a counter) and never replace an existing one.
- Existing test? `storage`/`backup` tests only cover the successful-backup path; a "backup write fails" case would have caught it.

### S0-2  After one failed storage write (quota) the next write throws away every unsaved session
- Area: persistence / multi-write. Confidence H.
- Repro: `node $QA/t3_quota.mjs` (patches `Storage.prototype.setItem` to throw `QuotaExceededError` between q1 and q3).
  Output: q1 stored xp 10; q2 played with quota exceeded (in-memory shows 20 XP, stored still 10); quota cleared; q3 played; **stored xp = 20, sessions = 2, cards = 2**. Three questions answered, two persisted: q2 vanished although the UI had shown it.
- Observed vs expected: docs/architecture.md says "A quota error ... leaves the in-memory state intact; the next successful write persists it". In reality the in-memory state is replaced by the stale stored copy.
- Root cause: `web/src/lib/store.ts:61-66` `commit()` sets `lastRaw = saved.raw` even though `saveProgress` ignored the failure (`storage.ts:77`). The next `updateProgress` runs `syncFromStorage()` (`store.ts:49-55`), sees `storage.getItem(KEY_V2) !== lastRaw`, and "adopts" the older stored value, discarding the in-memory profile, then applies the new change on top of it.
- Fix: `saveProgress` should return whether the main write succeeded; on failure keep `lastRaw` = the raw actually in storage and mark the state `dirty`, so `syncFromStorage` only adopts when the stored value differs from the last value we *read or successfully wrote*. Also surface a visible warning (see S1-5).
- Related amplifier (S2-19): an imported backup can legally contain hundreds of thousands of `missed` ids, i.e. a way to fill the 5 MB quota.
- Existing test? `store.test.ts` has "quota error" cases but none that performs a second write after the failed one.

(No S0 security issue found: see "What I attacked and found sound" at the end.)

---

## S1: wrong behaviour

### S1-1  Branch head does not build, typecheck, or pass its own tests; `npm run e2e` cannot start
- Area: repo health. Confidence H.
- Repro (from `interview-trainer/`): `npm run build` -> `web/src/lib/toasts.test.ts(12,72): error TS2353: ... 'emoji' does not exist in type 'AchievementDef'` (tsc includes test files); `npm run typecheck` same; `npx vitest run` -> **9 failed / 1185 passed**: `toasts.test.ts` (1, expects `emoji`, code now has `icon`), `share-image.test.ts` (7, fake canvas lacks `strokeRect` that `share-image.ts:84` now calls), `schema.test.ts` "unit colour defaults to green" (1, `color` is now optional with no default, `schema.ts:193`).
- Impact: Playwright's `webServer` runs `npm run build && npm run start` (`playwright.config.ts`), so `npm run e2e` fails before any spec runs; CI would be red.
- Fix: update the three stale tests to the new design; add `npm run typecheck` and `npm test` to the pre-merge gate.

### S1-2  Service worker + SPA fallback: after a deploy the next load can be a blank page, and the poisoned HTML-as-JS stays in the cache
- Area: PWA update flow. Confidence H.
- Repro: `node $QA/sw_prep.mjs` (copies `web/dist` to `$QA/sw/v1` and `v2`, v2 has a new hashed JS filename; the old hashed file does not exist in v2), start `npx tsx $QA/sw_server.ts` (port 4300, `staticDir` is a symlink I flip), then `node $QA/t7_sw_update.mjs`.
  Visit v1 once (SW installs; its cache holds only `/`, manifest, icon, font, **not the JS/CSS**), deploy v2, reload:
  `1st reload after deploy  build= undefined | root children= 0 | errs= ["Failed to load module script: Expected a JavaScript-or-Wasm module script but the server responded with a MIME type of "text/html" ..."]`, and the SW cache now contains `assets/index-<oldhash>.js [text/html]`. It self-heals on the following reload (index revalidated in background).
  Second scenario (visited twice before deploy): first reload after deploy still runs v1, second runs v2: the update takes two reloads and there is no "new version available" prompt.
- Root causes: (a) `server/src/app.ts:101-103` answers **every** non-`/api/` GET, including `/assets/missing.js`, `/.git/config`, `*.map`, with `index.html` and status 200; (b) `web/public/sw.js:19-31` is stale-while-revalidate for everything, caches any `res.ok` regardless of content type, and only pre-caches `/`, manifest, icon (`sw.js:6`) so hashed assets are cached lazily and the shell is served stale; (c) `CACHE` is a hand-bumped constant (`sw.js:3`), old hashed bundles are never purged.
- Fix: server returns 404 for paths with a file extension or under `/assets/`; SW: navigation requests network-first with cache fallback, hashed assets cache-first but only store responses whose content-type matches the request destination, precache the built asset list (emit a manifest at build time and derive `CACHE` from it), `skipWaiting` + a small "Update available, reload" toast.
- Existing test? None (e2e never deploys twice); `routes.test.ts` asserts the SPA fallback but not that missing assets 404.

### S1-3  Hosted single-file build: a hanging `sample` call leaves the learner stuck on "Grading..." with "I don't know" disabled
- Area: standalone build, grading and Ask why. Confidence H.
- Repro: `node $QA/t8_standalone.mjs` (variants of the built page with a fake `window.claude`). `hang` variant: after 12 s `NO FEEDBACK ... button text="Grading..." skipDisabled=true`. Quit (the X) is the only exit, and see S1-4 for when Quit does not work either.
- Root cause: `web/src/lib/sample-grader.ts:40` and `web/src/lib/explain.ts:30` await `sample(...)` with no timeout; the server path has `AbortSignal.timeout(60_000)` (`web/src/lib/api.ts:17`) but the sample path has no equivalent; `Lesson.tsx` disables skip while `phase === 'grading'`.
- Fix: wrap sample calls in `Promise.race` with the same 60 s (better 20 s) timeout, fall through to self-assessment; allow "I don't know"/Cancel while grading.
- Existing test? `sample-grader.test.ts` covers reject and garbage, not a never-settling promise.

### S1-4  `window.confirm` is blocked in sandboxed hosts: Quit lesson, Quit mock and Reset progress silently do nothing
- Area: hosted build. Confidence H.
- Repro: `node $QA/t18c.mjs` (page served over http, standalone build inside `<iframe sandbox="allow-scripts">`): `still inside lesson after clicking Quit: true`; Chromium logs "Ignored call to 'confirm()'. The document is sandboxed, and the 'allow-modals' keyword is not set."
- Root cause: `web/src/screens/Lesson.tsx:180`, `web/src/components/MockRun.tsx:128`, `web/src/components/SettingsSheet.tsx:171` use `window.confirm` (returns false when modals are not allowed). Combined with S1-3, a learner can be trapped in a lesson.
- Fix: use an in-page confirmation (the app already has `Sheet`), or treat a blocked `confirm` as "proceed" for non-destructive quits.
- Existing test? None (e2e answers dialogs natively).

### S1-5  No warning when progress cannot be saved (localStorage blocked or write fails): the learner thinks streak/XP are kept
- Area: persistence UX, hosted build in particular. Confidence H.
- Repro: same sandboxed-iframe run (`t18c.mjs`): `localStorage in sandboxed frame: throws SecurityError`; after one question Home shows "10 XP, streak 1"; after reload `0 XP` with no notice (`warning about unsaved progress shown? false`). Same silent behaviour when a write fails (S0-2).
- Root cause: `store.ts:20-27` falls back to a per-tab `Map` and `storage.ts:41-47` swallows write errors; nothing in `web/src` tells the learner (`grep` finds no such text).
- Fix: expose a `persistence: 'ok' | 'memory' | 'failing'` flag from the store; show a one-line banner on Home and in Summary ("Progress is not being saved on this device. Download a backup.").
- Also: `requestPersistence()` (`store.ts:131`, `void navigator.storage?.persist?.()`) is wrapped in try/catch but the call returns a rejected promise in sandboxed contexts: an unhandled rejection / `pageerror` ("Failed to execute 'persist' on 'StorageManager'") was observed in `t18c.mjs`. Add `.catch(() => {})`.

### S1-6  Corrupt or future-version progress is parked in a hidden key with no way back; the learner just sees a fresh 0-XP profile and the next answer replaces it
- Area: persistence UX. Confidence H.
- Repro: `node $QA/t4_storage_matrix.mjs` (matrix of stored values). For `{"version":3,...}`, truncated JSON, `[]`, `5`, `{"xp":5}`: the app renders "0/150 XP", the first answered question writes a fresh v2 profile and the old value ends up only in `interview-trainer.progress.backup-20261105`. There is no screen, message, or import path that mentions or restores that key (`grep -rn "backupKey\|progress.backup" web/src` is empty).
- Why it matters: this is exactly what happens when an older (stale, see S1-2) build meets data written by a newer one, or when a tab writes a truncated value. The data is "safe" only to someone who opens devtools.
- Fix: on `problem` show a blocking notice with "Download the unreadable data" and "Start fresh"; do not write until the learner chooses; add "Restore from automatic backup" in Settings listing `backup-*` keys.
- Existing test? `migrate.test.ts` proves the backup is made; nothing tests that the user is told.

### S1-7  The single-file artifact is not a valid standalone document: quirks mode, wrong encoding (mojibake), no viewport
- Area: single-file build (`npm run build:standalone`), opened directly (the README pitches "try the app from a phone with nothing installed"). Confidence H.
- Repro: `node $QA/inline.mjs` produces `$QA/interview-trainer.html` exactly as `scripts/build-standalone.mjs` would; `node $QA/t10_raw.mjs` opens it via `file://`: `compatMode = BackCompat`, `characterSet = windows-1252`, no viewport meta, layout width 980 on a 390px device; Progress screen text reads `READINESS Â· 0 TO 100` (UTF-8 middle dot decoded as Latin-1). The same bytes wrapped in `<!doctype html><meta charset=utf-8><meta name=viewport ...>` render correctly.
- Root cause: `scripts/build-standalone.mjs:29` emits `<title>...` with no doctype, charset, or viewport (fine inside an artifact host that wraps it, wrong everywhere else).
- Fix: emit `<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">` (hosts that wrap fragments will ignore or drop the duplicates).
- Existing test? None; no test opens the standalone output.

### S1-8  Summary shows "+20 XP" for a one-question session that is credited +10
- Area: Summary / App.finish. Confidence H.
- Repro: `#/q/<any single id>` (Library "Practise"), answer correctly, Continue. Summary tile: `XP EARNED +20`; Home afterwards: `10 XP` (also `t15_farm.mjs`: 8 sessions, stored xp 80, each summary said +20).
- Root cause: `web/src/App.tsx:120` subtracts `XP_PERFECT_BONUS` from what is *stored* for `kind` sessions (single/challenge), but `Summary.tsx:123` prints `summary.xp` (the unadjusted engine value) and `finished` carries no adjusted figure. For the daily challenge the +15 bonus is also not in the tile.
- Fix: put the credited XP into `Finished` and display that.
- Existing test? e2e checks the tile only for full lessons.

### S1-9  Daily challenge crossing midnight is recorded against the wrong day (new day's challenge is consumed)
- Area: clock handling. Confidence H.
- Repro: `node $QA/t16_midnight.mjs` (Playwright clock at 23:59:20, open `#/challenge`, skip until the last attempt, fast-forward past midnight, Continue). Result: `challenge records: {"2026-11-05":"fail"}`; the new day's *different* question is then shown as plain practice ("replay"), Home says "Daily challenge: done" and the +15 XP chance is gone.
- Root cause: `web/src/App.tsx:115` computes `today` inside `finish()` (at the end of the session) and `App.tsx:132` passes it to `recordChallenge`, but the question was chosen for the day the session started (`Practice` mount, `App.tsx:233`). Same hazard for goal/streak credit (acceptable) but not for the challenge.
- Fix: capture `today` when the `Practice` screen mounts and pass it to `finish`.
- Existing test? `challenge.test.ts` is pure; no midnight-spanning session test.

### S1-10  Content: the right answer is the longest option in 86% of single-choice questions, and a position-free "pick the longest" strategy beats chance 3.4x
- Area: content integrity. Confidence H.
- Repro: `cd interview-trainer/server && npx tsx $QA/content2.ts`: `single: "pick longest" is right 110 / 128 (86%, chance ~25%); mean len ratio answer/avg-distractor 2.35`; `multi`: 34 of 36 questions are exactly "3 correct of 5" and 2 are "4 of 6" (the count is guessable); `content1.ts`: absolute words (`always|never|only|all|none|must|cannot`) appear in 87 wrong options but 22 right ones; many distractors are strawmen ("Never retry anything", "Only test manually", "Fine if you delete the chat afterwards").
- Impact: readiness and mastery numbers (the app's headline "honest estimate") are inflated by test-wiseness; answers are also shuffled per attempt (good) but length is a positional-free tell.
- Fix: authoring rule + content test: fail when the answer is the unique longest option in more than ~35% of singles, and when `max(len)/min(len) > 2`; rewrite distractors to be plausible and similar in length; vary multi counts (2 to 4 correct).
- Existing test? `content.test.ts` has no distractor-quality checks.

---

### Top 5 (by impact): S0-2, S0-1, S1-2, S1-6, S1-5 (with S1-1 as the immediate blocker for CI).

---

## S2: minor

Server and API
- **S2-1 No security headers on app responses.** `curl -si :4194/` shows no CSP, `X-Content-Type-Options`, `Referrer-Policy`, `X-Frame-Options`/`frame-ancestors` (only Express' default 404 page carries `default-src 'none'; nosniff`). `server/src/app.ts` never sets them. Add a small header middleware (CSP `default-src 'self'; style-src 'self' 'unsafe-inline'; worker-src blob:` since the code runner is an inline blob worker). H.
- **S2-2 No compression, no long-lived caching.** `curl -sI -H 'Accept-Encoding: gzip' :4194/assets/index-*.js` returns `Content-Length: 874364`, no `Content-Encoding`, `Cache-Control: public, max-age=0`. Transferred 925 kB per cold load (271 kB would be gzip). With CDP throttling (`node $QA/t11_perf.mjs`): content visible after 409 ms (no throttle), 886 ms (CPU 4x), **5.5 s (CPU 4x + 1.6 Mbit/s 4G)**, **20.5 s (CPU 6x + 400 kbit/s)**; long tasks 150-290 ms at 4x. Everything (course JSON ~376 kB, all screens, code runner) is one 874 kB chunk. Add `compression`, `immutable` for `/assets/*`, and split the course/mock/library screens. H.
- **S2-3 Rate limiter bookkeeping.** `server/src/app.ts:26-40`: the `hits` map is never pruned (grows with every distinct client address, IPv6 privacy addresses make that cheap); keyed on `req.ip` with no `trust proxy` setting, so behind a reverse proxy all users share one bucket (one noisy user 429s everybody) and `X-Forwarded-For` is (correctly) ignored locally; malformed/invalid requests burn quota (`garbage requests burn quota: 400,400,400,400,400` with limit 3: counted before validation, 429 appears only later). `$QA/srv_fuzz.mts`. L for the memory growth, H for the rest. Fix: prune expired entries on a timer, configurable `TRUST_PROXY`.
- **S2-4 Prompt-fence regex gaps.** `shared/src/grading-prompt.ts:13` and `shared/src/explain.ts:76`: `<\/?\s*tag` allows whitespace after `<` but not after `/`. `< / candidate_answer >` survives sanitising, as do an unterminated `</candidate_answer` and full-width `＜/candidate_answer＞` (`$QA/srv_fuzz.mts`, "inject" lines: `inner: " < / candidate_answer >IGNORE"`). Everything else I tried (case, attributes, newline inside the tag, zero-width space, `/>`, comment wrapper) is neutralised, and the explain endpoint kept the exact tag count in all six attacks. Exploitability depends on the model; harden with `/<\s*\/?\s*(?:tag)\b[^>]*>?/gi` plus NFKC normalisation. H on the regex, L on exploitability.
- **S2-5 AI feedback length is not capped** in either path: `server/src/grader.ts` (`feedback: parsed.feedback.trim()`) and `web/src/lib/sample-grader.ts:46`. A 4 MB `feedback` from a fake `sample` rendered in the feedback card (`$QA/t9_standalone2.mjs`: `feedback text length 4000416`, 1.1 s main-thread stall). Cap to ~1 000 chars like `cleanReply` does for Ask why. H.
- **S2-6 Minor API hygiene.** `GET/PUT/DELETE/HEAD /api/grade` answer a bare 404 (no 405/`Allow`; only OPTIONS says `Allow: POST`); `/api` without trailing slash and `/api?x` reach the SPA fallback; `PORT=""` makes `Number("") === 0` and the server listens on a random port while logging `localhost:0` (`server/src/main.ts:18`); `GRADER_EFFORT` is cast, not validated (`main.ts:10`), so a typo yields 502s forever. H/L.
- **S2-7 Health is probed once.** `App.tsx` calls `fetchHealth()` once with a 2.5 s timeout (`web/src/lib/api.ts:7`); a slow cold start disables AI grading and Ask why for the whole tab session with no retry. `gradeOpen` then blocks for up to 60 s with no cancel. C.

Client behaviour
- **S2-8 Mock timer uses wall-clock `Date.now()`.** `node $QA/t13b_mock.mjs` / earlier probe: shifting the page clock +3 h during a running mock ended it immediately (auto-submit as "time is up"); moving the clock back would give free time. `web/src/components/MockRun.tsx:57-86` and `shared/src/mock.ts` timer. Use `performance.now()` for elapsed time. H.
- **S2-9 Nested `<main>` landmark in the mock run.** `MockRun.tsx` renders `<main className="lesson-body">` inside the app's `<main id="main">` (`document.querySelectorAll('main').length === 2`, `main main` exists, `t13b_mock.mjs`). Also constant ids repeat when several questions stay mounted (`id="estimate-input"` in `EstimateQuestion.tsx`, `id="editor-keys-hint"` in `CodeQuestion.tsx`). H/C.
- **S2-10 Double-tap on Continue skips the session summary.** `t12_ui.mjs`: `dblclick` on Continue ends on `#/` (the second tap hits the Summary's primary button, which sits at the same spot). Summary carries rank-up/streak/goal celebrations and the review forecast. Ignore a second `next()` for ~400 ms or render Summary's actions after a short delay. H.
- **S2-11 Daily goal and XP are farmable with one question.** `t15_farm.mjs`: answering the same single question 8 times gives `days[today] = {xp:80, answered:8, firstTry:8, target:8}`, the 8/8 ring and the first-goal milestone. `shared/src/progress.ts:39` documents `answered` as "Distinct questions resolved that day"; `shared/src/goal.ts:73` (`recordDay`) just adds per session. H. (Card scheduling is protected, as documented; XP/rank/goal/`stats.firstTry` are not.)
- **S2-12 Code questions can be "passed" without solving.** The learner's code and the harness share one realm and the first worker message wins. `t6b_code_cheat.mjs`: `function f(){ postMessage({ok:true,results:[]}); return 0 }` and `JSON.stringify = ()=>"x"; Object.is = ()=>true; return 0` both give a green "correct" verdict (also hidden tests, also in mock mode). `web/src/lib/code-runner.worker.ts:25`, `web/src/lib/run-code.ts:27` (`worker.onmessage = finish(e.data)`), `shared/src/code-runner.ts` (`deepEqual` uses the tamperable `Array.prototype.every`/`Object.is`). Fix: freeze intrinsics before running learner code, run tests in a second closure created before the learner code, accept only a message carrying a per-run nonce. H.
- **S2-13 The worker "no network" hardening is incomplete.** `t6_code.mjs`: `import('http://localhost:4194/api/health?from=worker-import')` from learner code issued a real request (seen in `page.on('request')`), `WebSocketStream` is still reachable (prototype walk). The FORBIDDEN list (`code-runner.worker.ts:9`) cannot block `import()`. It is documented as best effort, so S2. Infinite loops (3 s timeout), recursion overflow, 2^N string growth and a 3-second array memory bomb were all contained; the page stayed alive and the next run worked. H.
- **S2-14 Estimate parsing quirks.** `$QA/est.ts`: "1,234" is 1234 but "1.234" is 1.234 (a de/es/it/pt learner typing "1.536" for `jvm-memory/7`, "3.850" for `ai-advanced/5`, "11.574" for `sd-estimation-traffic/1` is silently read as a decimal and graded wrong, with no hint); `k` suffix works but `M`, `m`, `kb`, `ms`, `1e3`, unicode minus U+2212, full-width and Arabic-Indic digits, `1'000` all give "Enter a number". `shared/src/grade.ts:36-52`. Tolerances themselves are sane (no tol>10%, exact-integer questions use 0). H.
- **S2-15 Browser Back/Forward in a lesson drops the lesson without a prompt** (Quit has a confirm, Back does not): `t14_kbd.mjs` "Back from lesson goes to: (empty hash)"; Forward restarts it fresh. No `beforeunload` either. By design the lesson has no persisted state, but the asymmetry loses work. H.
- **S2-16 Unsaved note is discarded by Continue.** `QuestionTools.tsx` keeps the draft in local state and writes it only on "Save note"; pressing Continue unmounts the feedback card and the draft is gone with no prompt. Code reading only (not driven in the browser). L.
- **S2-17 Routing polish.** `t2_routes.mjs`: `#/library/`, `#/mock/`, `#/progress/extra`, `#//` all silently render Home (no 404); `NotFound` always says "Lesson not found / does not point to a lesson" even for `#/topic/x` and `#/q/x`; `#/lesson/kotlin-null-types?x=1` is "not found". No console errors for any of the 35 hashes tried. H.
- **S2-18 Settings: "Replace my progress" and "Reset progress" take no safety copy, and Replace overwrites device prefs.** `store.ts:119` `replaceProgress = updateProgress(() => p)`: the current profile is not written to a backup key first (only unreadable values are), and the imported `prefs` (reading size, reminders) replace this device's although README/architecture say prefs are "kept local on import/merge" (merge does keep them: `import_fuzz.mts` last line). Reset also leaves the `...progress.v1` key, which would resurrect old data if the v2 key ever goes missing. C/H.
- **S2-19 Import limits are generous.** `import_fuzz.mts`: a 2 MB file with 200 000 arbitrary `missed` ids is accepted (`missed` is unbounded and not validated against the course), 100 000 lesson ids are rejected only by the 5 MB text cap; a file with the checksum field removed can set any XP (`xp=123456789` accepted; documented as "not a security feature"). Combined with S0-2 this can fill localStorage. Cap `missed` to the number of real question ids and drop unknown lesson ids on normalise. H.
- **S2-20 Radio/checkbox widgets lack arrow-key behaviour**: `ChoiceQuestion.tsx` uses `role="radio"` buttons without roving tabindex/arrow keys; Settings/Ask-why sheets set `aria-modal` but leave `#main` neither `inert` nor `aria-hidden` (`t14_kbd.mjs`). Focus trap, Escape and focus restore all work. C/H.
- **S2-21 Stale result under the editor.** `CodeQuestion.tsx:44-49`: edits made while "Run tests" is running leave results for the old source on screen. L.
- **S2-22 Challenge date maths is O(days since 2024-01-01) per uncached day** (`shared/src/challenge.ts:49-57`): 0.5 s for a year-2999 clock, 17 s for 30 consecutive days there (`chal_time.ts`). Only matters with a badly wrong device clock. H.

PWA
- **S2-23 Manifest/icon.** Only `/icon.svg` with `purpose: "any maskable"` (SVG, one icon for both purposes) and `<link rel="apple-touch-icon" href="/icon.svg">`; iOS ignores SVG touch icons and Chrome's installability prefers 192/512 PNGs. `start_url`/`register('/sw.js')` are root-absolute, so a sub-path deployment breaks. C.
- **S2-24 SW cache grows with every deploy** (old hashed bundles are never evicted, ~0.9 MB each; see S1-2). H.

Content and docs
- **S2-25 Duplicate question.** `content/06-scale.yaml:439` (`scale-advanced/9`) and `content/11-system-design.yaml:123` (`sd-estimation-traffic/9`) are the same open question ("Design a rate limiter for a public API that runs on 20 instances behind a load balancer.") with near-identical model answers and follow-ups; both land in Mixed/Mock/Library. H (`content1.ts`). Also `complexity-reading/1` and `/4` share the identical prompt "What is the time complexity?" and type `single`, so their card hashes (`type|prompt`, `shared/src/hash.ts:21`) collide: reordering them inside the lesson would silently attach each card to the other question, which is the case the hash was meant to prevent. Include code/options in the hash.
- **S2-26 README and docs are stale.** README line 14 says "146 questions / 8 units / 16 lessons"; the build serves 307 questions / 13 units / 34 lessons (`GET :4194` log, `content1.ts` counts: single 128, multi 36, fill 15, order 14, open 34, match 22, code 20, spot 18, estimate 20). `docs/MONETIZATION.md` repeats 146/16 open. `content/docs.test.ts` checks code blocks and paths but not these figures. H.
- **S2-27 Rubric keyword stuffing.** `heuristicCoverage` passes 86 of 86 open parts when the answer is just the first keyword of each key point with no sentence (`content1.ts`: "keyword-stuffing ... fully passes 86 of 86"). The offline path only pre-ticks a checklist the learner can edit, so low impact; the existing test only tries "I do not know". Also 4 code questions' trivial stubs pass many tests (`complexity-advanced/6`: `return true` passes 4/7). H/L.

---

## What I attacked and found sound (no action needed)
- XSS: every place user text is rendered goes through React text nodes (`Inline`, notes, chat bubbles, library, toasts); the only `dangerouslySetInnerHTML` is the static icon map (`icons.tsx:110`). HTML/`onerror` payloads in AI feedback, Ask-why replies, notes, hash routes, import files rendered inert (`t9_standalone2.mjs`, `t12_ui.mjs`, `t2_routes.mjs`). File names are fixed (`backupFileName(day)`).
- Prototype pollution: `__proto__`/`constructor` keys in stored data and in imports (top level, lessons, cards, notes, bookmarks, calibration, passedByType, challenge, unitScores, achievements, milestones) never changed `Object.prototype` (`t5_proto_multitab.mjs`, `import_fuzz.mts`); duplicate JSON keys are caught by the checksum.
- Static serving: no path traversal (`..%2f`, `%2e%2e`, dot-directories all fall to `index.html`, never the file system; express.static ignores dotfiles). Error bodies never leak upstream detail; bad JSON, wrong content-type, oversize (413), arrays, deep nesting, lone surrogates, NUL, extra fields and the `__proto__`/`constructor`/`toString` ids all produce the documented 400/404/413. `X-Forwarded-For` is ignored (limit hit at the 4th request with a different spoofed header each time).
- Multi-tab: A and B alternating writes ended with xp 30, 3 sessions, 3 cards (`t5_proto_multitab.mjs`); another tab clearing storage mid-lesson does not crash the lesson.
- Deep links, unknown ids and hash garbage never produced a page error. Offline reload works (HTTP cache kept the bundle in my runs even after `Network.clearBrowserCache`; I could not make it fail).
- Memory: 60 one-question sessions: heap 5.6 -> 7.3 MB, DOM nodes 1118 -> 1208, listeners 194 -> 202 (`t17_mem.mjs`). Library search over 307 questions: 150-370 ms including typing under 6x CPU throttle.
- Streak/freeze/clock logic: not re-derived (covered by property tests); the only clock defects I could reproduce are S1-9, S2-8, S2-22.
- Keyboard: Settings sheet trap holds (Tab never leaves, Shift+Tab from the container stays in, Escape closes and restores focus to the opener); lesson focus moves to the prompt, then the verdict.

## Scripts index (`$QA/`)
`lib.mjs`/`lib2.mjs` helpers; `t1*_offline` offline reload; `t2_routes` hash fuzz; `t3_quota` S0-2; `t4_storage_matrix` corrupt storage; `t5_proto_multitab`; `t6_code`, `t6b_code_cheat`; `t7_sw_update` (+ `sw_prep.mjs`, `sw_server.ts`); `t8_standalone`, `t9_standalone2`, `t10_raw`, `t18c` hosted-build behaviour (+ `inline.mjs` to assemble the single file); `t11_perf`; `t12_ui`; `t13b_mock`; `t14_kbd`; `t15_farm`; `t16_midnight`; `t17_mem`; server `srv_fuzz.mts`; data `unit_storage.ts`, `import_fuzz.mts`, `est.ts`, `chal_time.ts`; content `content1.ts`, `content2.ts`.
Servers I started (:4194, :4300) have been stopped.
