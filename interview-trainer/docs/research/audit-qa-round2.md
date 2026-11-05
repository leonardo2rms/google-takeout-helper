> **Archived research, snapshot.** Round-2 QA re-audit: status of every round-1 finding, new findings N-S1-1..N-S2-13 and the hand-mutation results. Status after the round-2 fixes is in the handoff.
>
> Written on 2026-11-04/05 while this app was being built, by an agent working from the brief described in [the handoff](../HANDOFF.md). It is kept as written so later decisions can be traced back to it. Paths such as `$S/...`, `$QA/...`, `$QA2/...` or `/tmp/claude-0/...` point at screenshots and scripts in that session's scratch space and are **not** in the repository. For what has since been built or fixed, the handoff is the source of truth.

# Interview Trainer: second-round adversarial QA and review

Build under test: branch `claude/engineer-interview-training-82crnr` (HEAD `3fdb5f2`) merged into the worktree; `npm install && npm run build` OK, `npm run typecheck` OK, `npm run coverage` OK (66 files, 1382 tests, 99.7% stmts / 97.3% branches), `npm run content:lint` OK (ratchet passes). Production bundle served by `npm run start` on :4194 (AI off). Chromium 1194 via the repo's Playwright 1.56.1 (the `/node-tools` path from the brief does not exist here), phone viewport 390x844.
Scratch dir (all scripts, no repository file touched): `$QA2 = /tmp/claude-0/-home-user-google-takeout-helper/959a0803-75f1-5c6e-8ab6-3e44ad11da40/scratchpad/audit-qa2/`. Round-1 scripts re-run from `$QA2/r1/` (paths patched, `course.json` regenerated from the new content). Helpers: `lib.mjs`; run a browser script with `node $QA2/<file>` while a server is on :4194; `.ts/.mts` scripts run with `cd interview-trainer/server && npx tsx $QA2/<file>` (or `bash $QA2/runts.sh <file in r1/>`).
Confidence: **H** reproduced here, **C** read in code plus a reproduced neighbouring behaviour, **L** code reading only.
Caveat: Playwright's `context.setOffline` does not affect service-worker fetches, so offline tests killed the server instead. The e2e suite (`npm run e2e`) was not run; mutation survivors below may be killed there.

**Counts of new findings: S0: 0, S1: 2, S2: 13.** Round 1: all S0 and 9 of 10 S1 fixed, S1-10 only partly (see N-S1-2); 13 of 27 S2 fixed, 14 still reproduce.

---

## (a) Status of round-1 findings

| ID | Finding | Status | Evidence |
|---|---|---|---|
| S0-1 | Unreadable/newer data destroyed when backup write fails; backup key overwritten | **Fixed** | `r1/unit_storage.ts` part A: newer-version data survives, `saveProgress` returns `skipped`. Part B still prints the second value only because that script's fake storage has no `key()`/`length` (see N-S2-3); with real `localStorage` copies get distinct keys (`storage_unit.mts` case 2). |
| S0-2 | After one failed write the next write drops unsaved sessions | **Fixed** | `r1/t3_quota.mjs`: stored xp 30 after quota cleared, 3 sessions, banner "not being saved" while failing. `t_restore.mjs` case 4: retry flushes after 4 s, banner clears; `pagehide` flush works. |
| S1-1 | Build/typecheck/tests fail | **Fixed** | build, typecheck, 1382 tests green. |
| S1-2 | SW + SPA fallback poisons cache, blank page after deploy | **Fixed** | Real 404 for missing assets (`srv1.sh`), content-type gated cache, versioned cache, all-or-nothing install, old caches deleted on activate. `t_sw.mjs`/`t_sw2.mjs`/`sw_unit.mts`: stale tab keeps working after deploy, reload shows v2, offline unknown route serves the shell, HTML-as-JS never stored. New residual: N-S2-5. |
| S1-3 | Hosted `sample` hang leaves "Grading..." forever | **Fixed** | `t_standalone.mjs`: hang falls back to self-check after 25.5 s; `use` that hangs/throws/returns null, rejects, null/string/wrong-length verdicts all fall back. Residual N-S2-6. |
| S1-4 | `window.confirm` blocked in sandboxed hosts | **Fixed** | In-app `ConfirmDialog` (role `alertdialog`); Quit, Reset, Replace, Submit, Quit mock all use it. New issues N-S2-1, N-S2-9. |
| S1-5 | No warning when progress cannot be saved | **Fixed** | `t_blocked.mjs`: getter throws / setItem throws / getItem throws all show the banner and the session keeps working in memory. |
| S1-6 | Corrupt/future-version progress parked with no way back | **Fixed** | `t_restore.mjs`: banner + Settings "Recovered data" (download, copy, restore merge/replace for readable copies); reset keeps a `before-` safety copy; restore then reload persists; merge x2 does not double. Newer-schema copy offers Download/Copy only (correct). |
| S1-7 | Single-file artifact not a valid document | **Fixed** | `dist/interview-trainer.standalone.html`: `CSS1Compat`, UTF-8, no mojibake; fragment inside a host page also standards mode. The bare fragment opened directly is still `BackCompat`; by design (documented). |
| S1-8 | Summary "+20 XP" for a one-question session | **Fixed** | `t_s18.mjs`: tile "+10", stored xp 10. |
| S1-9 | Challenge crossing midnight consumes new day | **Fixed** | `r1/t16_midnight.mjs`: result recorded for 2026-11-04, day 2 challenge still offered. |
| S1-10 | Correct option is the longest (86%) | **Partly fixed** | Longest now 29% (chance 25%). But new/remaining tells exist, see N-S1-2. |
| S2-1 | No security headers | Fixed | CSP, nosniff, Referrer-Policy, X-Frame-Options on every response. |
| S2-2 | No compression/long cache | Fixed | gzip; hashed assets `immutable`; `index.html`/`sw.js` `no-cache`; 304 on If-None-Match; Range works. |
| S2-3 | Rate limiter bookkeeping | Fixed (code) | pruning + `trustProxy` option. |
| S2-4 | Prompt-fence regex gaps | Fixed | `tagPattern` (whitespace after `/`, unterminated, NFKC). |
| S2-5 | AI feedback length uncapped | Fixed (code) | `cleanFeedback` + `FEEDBACK_MAX_CHARS` on both paths. |
| S2-6 | API hygiene | Fixed | 405+`Allow`, JSON 404 under `/api`, empty `PORT`, `GRADER_EFFORT` validated. |
| S2-7 | Health probed once | Fixed (code) | `fetchHealthWithRetry`. |
| S2-8 | Mock timer on wall clock | Fixed | `monotonicNow()`; `mock-clock.spec.ts`; my mutant M11 killed. |
| S2-9 | Nested `<main>`, repeated ids | **Half fixed** | Single `<main>` (`t13b`); `id="estimate-input"` and `id="editor-keys-hint"` still constant (`EstimateQuestion.tsx:21`, `CodeQuestion.tsx:77,88`). |
| S2-10 | Double-tap Continue skips summary | Fixed | `t_dbl.mjs`: lands on `#/done` with summary. |
| S2-11 | Goal/XP farmable with one question | **Still reproduces** | `r1/t15_farm.mjs`: 8 repeats = 80 XP, 8/8 goal. |
| S2-12 | Code questions passable by `postMessage` spoof | **Still reproduces** | `r1/t6b_code_cheat.mjs`: both tricks give "good". |
| S2-13 | Worker no-network hardening incomplete | **Still reproduces** | `r1/t6_code.mjs`: `import()` request to `/api/health?from=worker-import`. |
| S2-14 | Estimate parsing quirks | **Still reproduces** | `r1/est.ts`: "1.234" = 1.234, "2M", "1e3", U+2212 minus, full-width digits null. |
| S2-15 | Back in lesson drops it silently | **Still reproduces** | `r1/t14_kbd.mjs`. |
| S2-16 | Unsaved note discarded by Continue | Still (L) | `QuestionTools.tsx:20-28` unchanged. |
| S2-17 | Routing polish | Half fixed | `#/library/`, `#/mock/`, `#/lesson/x/`, `?x=1` now work; `#//`, `#/progress/extra` still render Home. |
| S2-18 | Replace/Reset take no safety copy; Replace overwrites prefs | Fixed | `keepSafetyCopy()`; prefs stay device-local (`import_fuzz` shows merge keeps local prefs; `replaceProgress` keeps `cur.prefs`). |
| S2-19 | Import limits generous | Half fixed | `missed` capped (5000 accepted, more clipped), huge lessons rejected; missing checksum still accepts any XP (documented). |
| S2-20 | Radios without arrows; `#main` not inert behind sheets | **Still reproduces** | `t14_kbd`: `inert:false, aria-hidden:null`; confirm dialog the same (N-S2-9). |
| S2-21 | Stale test result under the editor | Still (L) | `CodeQuestion.tsx:48-49` no staleness guard. |
| S2-22 | Challenge date maths O(days) | Fixed | `r1/chal_time.ts`: 30 uncached days around 2999 = 0.6 s (was 17 s). |
| S2-23 | Manifest/icon | Fixed | 192/512/maskable PNGs + apple-touch-icon. |
| S2-24 | SW cache grows per deploy | Fixed | `activate` deletes other versions (`t_sw.mjs`: one cache after v1 to v2). |
| S2-25 | Duplicate question rate limiter | **Still reproduces** | `content/06-scale.yaml` and `content/11-system-design.yaml` both contain it. |
| S2-26 | README stale counts | **Still reproduces** | README line 14 still "146 questions / 8 units / 16 lessons" (app: 307 / 13 / 34). |
| S2-27 | Rubric keyword stuffing | Not re-run | unchanged code path. |

---

## (b) New findings

### S0
None. Re-attacked: path traversal (`%2e%2e`, `..%2f`, `%5c`, double-encoded, `..;`, NUL, overlong UTF-8: all 404 or the shell, nothing outside `web/dist`; `/assets/%2e%2e%2fsw.js` resolves to `dist/sw.js`, inside the root), prototype pollution via imports, XSS in feedback (rendered as text), gzip bomb (413), 1 MB JSON (413), chunked/slow bodies (no crash).

### S1

**N-S1-1  A foreign, newer-schema or corrupt value appearing in the main key while a tab is open throws away that tab's in-memory profile**
- Repro: `node $QA2/t_store1.mjs`. Tab A has 20 XP (2 questions). Another page of the same origin writes `{"version":3,...}` (or truncated JSON) to `interview-trainer.progress.v2`. A answers a third question.
- Observed: stored xp **10** (both variants). A's 20 XP exist nowhere in A's output; the foreign value is parked (`backup-YYYYMMDD`), but the main key now holds an empty profile plus one answer. A newer tab that then syncs adopts that near-empty profile too. Recovery needs the parked copy and a build that can read it (the old tab's Recovered-data list offers only Download/Copy for it).
- Expected: A still holds a complete, readable profile, so it should keep it, park the foreign value, and write its own profile (or at least merge), exactly as it already does for `dirty` unsaved changes.
- Same cause for a cleared key (`localStorage.clear()` in another tab: next write persists only the new answer; xp 10 instead of 30). Debatable when the user cleared on purpose, but it also happens on browser eviction.
- Why it matters: this is the "two tabs on different versions" case that happens on every deploy once a schema bump exists (the new tab writes v3, the old tab then overwrites it).
- Root cause: `web/src/lib/store.ts:147-158` `syncFromStorage()` calls `adopt(loadProgress(...))`, which sets `current` to the empty fallback profile when `res.problem`; only `dirty` changes are merged back (`:157`).
- Fix: in `syncFromStorage`, when the loaded result has `protectedRaw !== null` (or `raw === null`) and `current` has data, keep `current`, set `protectedRaw = raw` (so `saveProgress` parks it first) and mark `dirty` so the next commit writes `current`.
- Test gap: `store.test.ts` covers "other tab wrote a readable value" and "dirty then other tab wrote", not "other tab wrote an unreadable value". Confidence H.

**N-S1-2  Answer-style tells remain, and the length rebalance added a new one ("because" now marks distractors)**
- Repro: `node $QA2/content_tells.mjs` (NEW) and `OLD=1 node $QA2/content_tells.mjs` (commit `98b6505`, before balancing); `node $QA2/content_rule.mjs`. 128 single-choice questions, all with 4 options.
- Length is fixed: correct option rank by length (longest..shortest) 29% / 40% / 20% / 11% (was 86/5/5/3); mean length ratio correct/distractor 1.04 (was 2.27). The correct option is the shortest only 11% (chance 25%), so "never the shortest" is a mild tell (median ratio p10 0.86).
- Tells that the lint does not measure (`scripts/content-lint.ts` checks length, absolute words, overlap, specificity only):

| Tell | Correct | Distractors | Effect |
|---|---|---|---|
| contains `(`, `;` or `:` | 38% paren, 48% `;`/`:` | 12%, 18% | when exactly one option has `(`: correct 34 of 38; exactly one `;`/`:`: correct 31 of 36. Picking "the option with ( ; :" is right **53%** (chance 25%); before balancing 57%. |
| contains the word "or" | 30 of 128 | 10 of 384 | lift 9x (18x before). "Pick the option with 'or'" is right 40%. |
| causal clause (`because`, `since`, `so that`, `which`) | 31% | **57%** (was 14%) | **new**: the only option with a causal clause is a distractor in 19 of 22 cases; "avoid the causal one" is right 37% (was 25%, neutral). Likely from padding distractors to match length. |
| contains an absolute word (`always`, `never`, `only`, `all`, `any`...) | 21% | 17% | when exactly one option has one it is a distractor 36 of 50. |

  - Hand-built combined rule (paren/`;`/`:` +2, "or" +2, causal -1.5, absolute -1, shortest -1, random tie-break): **63.5%** on the new content (69.0% before), chance 25%.
  - Bag-of-words leave-one-out classifier on option text: **58.6%** (was 44.5%), chance 25%: the rebalance made lexical leakage worse even though length is fixed.
  - Multi-select: the 3 longest options are exactly the right set in 57% of 5-option questions (was 73%; chance 10%), 1 of 36 exact.
- Expected: no cheap surface feature should beat ~35% on 4 options.
- Fix: add these four features (punctuation marker, " or ", causal clause, absolute word) as per-option rates and per-question "unique marker" counts to the lint with a ratchet like the length one; in the content, give distractors the same `( )`/`;`/"or" constructions and give correct options causal clauses at the same rate; strip the "or ... or" enumerations from correct options.
- Confidence H (measurement; the rules are fixed before the measurement, not tuned). Severity S1 because this is the same class as round-1 S1-10 and a learner can score ~2.5x chance without knowing the material.

### S2

**N-S2-1  Enter pressed while focus is on the confirm dialog itself drives the lesson behind it**
- Repro: `node $QA2/t_confirm.mjs` step 1. Answer two questions, press Check on the third, open "Quit lesson", click the dialog title (focus moves to the `tabIndex=-1` container), press Enter.
- Observed: the lesson advances (`next()`/`check()`) behind the still-open dialog ("lesson advanced behind dialog? true"). At the last question this finishes the session and credits XP while a "Quit and lose progress" question is on screen.
- Root cause: `web/src/screens/Lesson.tsx:160` skips only `closest('[role="dialog"]')`; the confirm is `role="alertdialog"` (`ConfirmDialog.tsx`). Fix: also skip `[role="alertdialog"]` (or stop propagation in `useModal` for Enter while it is on top). Confidence H.

**N-S2-2  Quit after Check but before Continue skips the confirmation and silently discards the XP shown on screen**
- Repro: `$QA2/dbg.mjs`: open `#/lesson/kotlin-null-types`, answer and press Check (header shows XP +10), tap the X: straight to Home, no dialog, nothing saved.
- Root cause: `Lesson.tsx:~186` `Object.keys(session.attempts).length === 0` and `submit()` (the only place attempts are recorded) runs on Continue. Fix: also treat a checked question (`phase !== 'answer'`) as "something to lose". Confidence H.

**N-S2-3  Parked-copy rotation sorts by key, so with a clock set back the copy just written is the first one deleted (reported as success)**
- Repro: `npx tsx $QA2/storage_unit.mts` case 1: 8 unreadable copies exist with later dates (e.g. `backup-20270101`..), clock now 2026-11-05, main key holds `{"version":3,...}`. Output: `backedUp reported = true | main key overwritten = true | copy of newer-version data survives = false`.
- Same pattern: the "Recovered" banner is suppressed when the new key sorts below the stored ack watermark (`store.ts` `recovered: parkedKeys.filter(k => k > ack)`). Needs 8 (unreadable) or 3 (safety) existing copies from the future, so rare.
- Related: after 30 distinct unreadable values on one calendar day (even though rotation freed slots) `parkRaw` returns null (`storage.ts:152` uses highest index, not a count), so saving is refused until tomorrow (`case 5`). And `parkRaw` on a storage without `key()/length` overwrites the single slot (affects only non-standard `StorageLike`).
- Root cause: `storage.ts:141-160` ranks by key text, not creation order. Fix: after writing, verify the new key is not in `all.slice(MAX)`; delete the oldest by an explicit counter/timestamp stored with the copy, never the one just written. Confidence H (unit level).

**N-S2-4  A same-version profile with extra or malformed fields is normalised and rewritten at load with no copy**
- Repro: `npx tsx $QA2/storage_unit2.mts`: `{version:2,...valid..., futureThing:{...}}` loads, `res.changed`, KEY_V2 is rewritten without `futureThing`, no backup. A malformed `streak:"x"` is reset the same way; the original text is nowhere.
- Matters for rolling deploys that add a field without bumping `version`, and for hand-edited files. Root cause: `storage.ts:78-81`. Fix: when `res.changed` and the dropped keys include anything not produced by the normaliser, `parkRaw(raw, 'unreadable')` first. Confidence H.

**N-S2-5  Network-first navigation stores any 200 text/html (captive portal, proxy error page) as the offline shell**
- Repro: `npx tsx $QA2/sw_unit.mts` case 1: a Wi-Fi hotspot answering a navigation with `200 text/html` "CAPTIVE PORTAL" replaces `/` in the cache; the next offline start shows the portal page instead of the app until an online navigation succeeds. (Also case 3: content type is trusted, the body is not inspected.)
- Root cause: `web/src/lib/sw-logic.ts:115-117` stores `res.clone()` of whatever answered a navigation; `isStorable` checks only status and type. Fix: store the shell only when the body contains the app root marker (for example `<div id="root">` and a script from `/assets/`), or only when the final URL is the app's own shell. Redirecting portals are not affected (opaque redirect, status 0). Confidence H (unit), C (browser).

**N-S2-6  Junk-but-well-shaped AI verdicts become a confident grade**
- Repro: `node $QA2/t_standalone.mjs`, variant `junk-feedback-obj`: `points` of the right length with `covered:'yes'` gives "AI grading against a rubric ... INCORRECT 0/3" for a model-answer-quality response. Hang (25.5 s), reject, null, string, wrong length, no `window.claude` all fall back correctly.
- Root cause: `web/src/lib/sample-grader.ts:57` `p?.covered === true` with no `typeof covered === 'boolean'` check for every point. Fix: reject (fall back to self-check) unless every entry has a boolean `covered`. Confidence H.

**N-S2-7  A wrong device clock set forward erases challenge history; the stale test shows the boundary is untested**
- Repro: `npx tsx $QA2/chal_unit.mts`: four challenge days recorded, then one challenge finished with the clock at 2028-06-01: `challenge` keys become `['2028-06-01']`; after the clock is corrected the streak is 0 and `done` is 1.
- Root cause: `shared/src/challenge.ts:93` prunes by distance from the day being recorded. Fix: prune relative to the real latest day only when `day` is not more than a few days past the newest entry, or keep entries and cap by count. Mutant M14 (`>=` to `>`) also survives. Confidence H.

**N-S2-8  Server odds and ends**
- No request/header timeouts are configured (`server/src/main.ts`): a body dribbled one byte per 500 ms stayed open for 12 s and is held until Node's default 300 s (`$QA2/srv2.mjs`); 300 half-open sockets did not slow `/api/health`.
- `POST/PUT/DELETE/PATCH` to `/`, `/sw.js`, `/assets/x.js` answer Express's default HTML 404 (not 405 and not the app's plain 404); `OPTIONS /` is 200. `GET /api/health` with a malformed JSON body answers 400 because `express.json` runs first (`app.ts:~92`). `GET /assets` redirects 301 to `/assets/` which is then 404. Cosmetic. Confidence H (`srv1.sh`).
- Verified fine: HEAD (200, no body), `Range` with gzip (206, uncompressed, correct `Content-Range`), `If-None-Match` 304, `Vary: Accept-Encoding`, `sw.js` `no-cache`, hashed assets `immutable`.

**N-S2-9  ConfirmDialog accessibility details**
- `#main` is not `inert`/`aria-hidden` behind the dialog and there is no scroll lock (`t_confirm.mjs` step 2: `inert:false, aria-hidden:null`). `aria-modal` covers most screen readers.
- When a mock ends by time-out while "Quit"/"Submit" is open, the dialog closes correctly (`t_mockconfirm.mjs`) but focus lands on `<body>`; move it to the results heading.
- Verified fine: Escape closes only the top layer (confirm over Settings sheet: two Escapes), a hash change closes both, focus returns to the opener, Tab/Shift+Tab stay inside, concurrent confirm answers the first with "no".

**N-S2-10  Test gaps found by hand mutation** (unit suite only, e2e not run): see section (d). Six survivors: label thresholds at exactly 30% coverage and score 20, challenge retention boundary, best-topic 50% boundary, navigation `status <= 500`, retry backoff cap.

**N-S2-11  Update notice is not shown for the first update after a hard reload** (L): `sw-register.ts` sets `hadController = container.controller !== null` once; a Ctrl+Shift+R load has no controller, so the first `controllerchange` is treated as "first install" and no notice appears.

**N-S2-12  Standalone fragment**: the bare fragment (`interview-trainer.html`) opened directly is quirks mode (`compatMode: BackCompat`). Documented as intended, but anyone double-clicking the wrong file gets different layout; consider naming (`.fragment.html`) or a one-line `<!doctype html>` guard comment in README. Confidence H.

**N-S2-13  Carried-over docs/content**: README counts (S2-26) and the duplicate rate-limiter question (S2-25) are unchanged; `order lines duplicate when trimmed` in four questions (`r1/content2.ts`: `coroutines-flow/5`, `scale-resilience/7`, `complexity-reading/8`, `complexity-advanced/7`) and one fill bank with a duplicate token.

---

## (c) What I attacked and found sound
- Storage: quota exhaustion mid-write, second write after failure, retry/backoff and flush on `pagehide`, getter/setItem/getItem throwing, two tabs alternating (xp 40/40), unsaved-then-other-tab merge, newer-schema at load (kept untouched until a copy exists, banner shown), restore (replace/merge) then reload, merge twice is idempotent, safety copies on reset and replace, restore button absent for unreadable copies.
- Service worker: stale tab after deploy (keeps working, notice appears, reload shows new build), one cache per version, atomic install, activation only deletes own-prefix caches, offline unknown routes and missing assets, HTML-as-JS never cached.
- Server: 404 vs shell rules, traversal variants, 405 + `Allow` on `/api/*`, oversize/gzip-bomb/bad-encoding bodies, compression with HEAD/Range/INM.
- `settleChallenge` midnight, DST-safe day arithmetic, mock timer (monotonic, pause, backwards clock), readiness thresholds (`enough` at exactly 25%: killed by test), gentle-format boundary (`< 3` sessions: killed), share card (zero values hide rows; no weakest topic; huge streak is the only overflow: a 6-digit streak at 220 px would overrun the card, only reachable with an edited import).
- Standalone: doc/fragment with and without `window.claude`, `sample` hang/reject/null/string/wrong length.

## (d) Hand mutation (copy of the repo in `$QA2/mut`, `python3 $QA2/mutate.py`, unit suite 11 s per run)
24 mutants over 15 functions: **18 killed, 6 survived**.
Survivors:
- M5b `labelFor` `coverage < 0.3` to `<= 0.3` (`mastery.ts`); M5c `score < 20` to `<= 20`: no test at the exact boundary values.
- M14 `recordChallenge` retention `>= CHALLENGE_KEPT` to `>`: boundary day untested.
- M15 `buildShareCard` best-topic `>= 50` to `> 50`: boundary untested.
- M18 `handleFetch` navigation `status < 500` to `<= 500`: no test with exactly 500 (a 500 shell should fall back to the cached shell).
- M20 retry back-off cap `Math.min(delay*2, 60000)` removed: no test that the delay stops growing.
Killed (representative): saveProgress proceeding after failed backup (M1), parkRaw dedupe/rotation/day cap (M2, M3, M22), settleChallenge bonus (M4), `isStorable` status and content-type (M6, M6b), merge of unsaved changes (M7), clearing `protectedRaw` early (M8), half-filled cache (M9), `/assets/` 404 rule (M10), monotonic clock (M11), pause arithmetic (M12), 405 on `/api/grade` (M17), activate deleting foreign caches (M19), `dirty` flag (M21), sample timeout cleanup and verdict shape (M23, M24).
Test quality: the new tests are named and written as behaviours (for example "does not overwrite newer-version data while its copy cannot be written, and keeps playing in memory"), not implementation details. Gaps are the untested units: `DataSafety.tsx`, `ConfirmDialog.tsx`, `useModal.ts` have no unit tests (e2e only; `web/src/lib/**` is the only web code in coverage scope), and `store.ts` lines 104-123 and 215 are uncovered.

## (e) Scripts index (`$QA2/`)
`t_store1.mjs` N-S1-1; `content_tells.mjs`, `content_rule.mjs`, `dump_old.ts`, `course_old.json` N-S1-2; `t_confirm.mjs`, `dbg.mjs`, `t_mockconfirm.mjs` N-S2-1/2/9; `storage_unit.mts`, `storage_unit2.mts` N-S2-3/4; `sw_unit.mts`, `swprep.mts`, `swsrv.ts`, `t_sw.mjs`, `t_sw2.mjs` SW; `t_standalone.mjs` N-S2-6 and S1-3/7; `chal_unit.mts` N-S2-7; `srv1.sh`, `srv2.mjs` server; `t_restore.mjs`, `t_blocked.mjs` storage UI; `mutate.py`, `mut_setup.sh`, `mut.log` mutation; `r1/` round-1 scripts re-run.
Servers started (:4194, :4300) have been stopped.
