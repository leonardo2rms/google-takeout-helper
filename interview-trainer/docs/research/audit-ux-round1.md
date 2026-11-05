> **Archived research, snapshot.** Round-1 UX audit (first-time-user walkthrough at 390/320/1280px, light and dark). All P0 and P1 findings were then fixed; P2 leftovers are listed in the handoff.
>
> Written on 2026-11-04/05 while this app was being built, by an agent working from the brief described in [the handoff](../HANDOFF.md). It is kept as written so later decisions can be traced back to it. Paths such as `$S/...`, `$QA/...`, `$QA2/...` or `/tmp/claude-0/...` point at screenshots and scripts in that session's scratch space and are **not** in the repository. For what has since been built or fixed, the handoff is the source of truth.

# UX audit: Interview Trainer (first-time user, phone first)

Method: fresh localStorage, Chromium via Playwright at 390x844 (also 320x640 and 1280x800), light and dark. I played 4 lessons on day 1 (1.1 with deliberate mistakes, 1.2, 1.3, 2.3), moved the clock to the next day, then used Review, the daily challenge, a mock interview, Library (search, save, note), Progress, Settings, the share card, the confidence setting and the disabled Ask why. I also covered all 9 question types incl. the code editor and open follow-ups. I touched no repo file.
Screenshots: `/tmp/claude-0/-home-user-google-takeout-helper/959a0803-75f1-5c6e-8ab6-3e44ad11da40/scratchpad/audit-ux/` (call it `$S`). Scripts: `.../scratchpad/ux-work/`.

Counts: **P0: 2, P1: 12, P2: 22**.

---------------------------------------------------------------

## P0 - broken

### UX-01 Match questions with inline code in an item break text character by character
- **Screen/state**: Lesson, Match the pairs (e.g. 1.2 "Match each scope function", item `Receiver is `this`; returns the receiver`). At 390, 320 and 1280.
- **Seen**: The right column wraps `Rec / eive / r is`, the inline `this` chip becomes a vertical `t h i s`, `; returns the lambda result` becomes a ragged tower. After checking, a paired row reads `Re ce iv er is it ; retur ns the recei ver`. At 1280 (608px column) it still shows `Receive r is` and `thi s`.
- **Why**: The item cannot be read, so the learner is guessing from fragments on a core question type. Answer feedback is fine; the question itself is not. Likely every match item that mixes prose and inline code (content has many).
- **Shot**: `$S/s6-L2-match-a.png`, `$S/s6-L2-match-c-fb.png`, `$S/91-m320-light-match.png`, `$S/92-d1280-light-match-wrong-fb.png`
- **Fix**: The item body is a flex row whose text nodes and `<code>` become separate flex items that shrink. Make the label `display:block` (or `display:inline` content in a single `<span class="m-text">`) with `min-width:0; overflow-wrap:anywhere` only on code, `white-space:normal` on text, `flex:1 1 0`. At <480px stack the two columns (left items as headers, right items below) or switch to a single-column "tap the match" list so each item gets full width.
- **Effort**: S

### UX-02 `npm run build` fails (type error in a test file), so the documented setup does not produce a build
- **State**: `npm run build` -> `tsc -p . && vite build` stops at `web/src/lib/toasts.test.ts(12,72): 'emoji' does not exist in type 'AchievementDef'` (icons replaced emoji, the test was not updated). `npm run start` then serves nothing new; I had to run `npx vite build` directly.
- **Why**: A first contributor or CI cannot ship. It also hides whether the shipped bundle matches the code.
- **Fix**: Update `toasts.test.ts` to use `icon: ...`, and add `npm run typecheck` to CI before deploy.
- **Effort**: S

---------------------------------------------------------------

## P1 - hurts the experience

### UX-03 First run: the "Make it yours" form pushes "Start your first lesson" ~1,100px down
- **Seen**: Home on a brand-new profile is a 5-field form (3 radios, date, goal, time, 3 buttons) taking 2 screens. The first lesson card is at y~1140 CSS px; the headline CTA is invisible in the first 10 seconds. "Four optional taps" is wrong (it is five fields). Three competing buttons: "Save and warm up (3 questions)" (ink), "Save" (outline), "Skip for now" (text). "Save" vs "Save and warm up" is ambiguous, and "Save" later means bookmark in lessons.
- **Why**: A new user cannot answer "what do I do?" without scrolling. Skipping works (1 tap) but is the least visible option.
- **Shot**: `$S/01-home-first-run.png`, `$S/01b-crop.png`, `$S/100-onboarding-filled.png`
- **Fix**: Put the hero "Start your first lesson" ABOVE the onboarding card and collapse onboarding to a one-line card "Set a goal and interview date (optional)" that expands on tap. Inside: one primary "Start warm-up", one text button "Not now". Rename `Save` -> `Save plan`. Native `mm/dd/yyyy` and `--:-- --` placeholders are fine but add `Optional` hints.
- **Effort**: M

### UX-04 First-run Home offers 6 equal cards, and two of them are wrong for a new user
- **Seen**: After the hero: Daily challenge (day 1 was a System Design unit-11 question before any lesson), Review due (disabled), Mixed practice ("weighted toward your weak spots" when there are none), Progress ("Readiness 0 · Early estimate"), Mock interview, Library. Opening `#/mix` as a new user starts with a typed open question ("Explain the transactional outbox pattern", unit 10). Two cards (Today and Mixed practice) share the solid ink icon tile so they look equally important.
- **Why**: Dense, flat hierarchy; a newcomer can wander into the hardest content first.
- **Shot**: `$S/02-home-after-skip.png`, `$S/103-route-__mix.png`
- **Fix**: Until 1 lesson is done, show only hero + Library link; reveal Daily challenge / Mixed / Mock / Progress after lesson 1 (or tuck them under a "More practice" disclosure). Daily challenge: pick from units the learner has seen once any exists. Mixed practice for a new user: start with single-choice, no open questions. Only the hero keeps the solid tile; others use outline tiles.
- **Effort**: M

### UX-05 The verdict is stated 2-3 times in the same screen
- **Seen**: Wrong single-choice: option tag `INCORRECT`, panel `INCORRECT`, heading `Not quite`. Right: tag `CORRECT`, panel `CORRECT`, heading `Correct`. Open: `SELF-CHECK · 3/3 POINTS` / `Check your answer` / `3/3 key points · self-assessed`. Summary: `Daily goal met` heading + `Daily goal met.` ring caption + a third `Daily goal met` line (L2 summary); `Perfect lesson` in the toast and again in the panel.
- **Why**: Noise on the screen that matters most; the design doc says verdicts should read like code-review marks, not stutter.
- **Shot**: `$S/06-q1-feedback-wrong.png`, `$S/s4-03-fill-c-fb.png`, `$S/s4-07-open-c-fb.png`, `$S/s6-L2-summary.png`
- **Fix**: Keep the marker on the option (icon + `Your answer`/`Answer`), keep the panel verdict word, and make the heading the first sentence of the explanation or remove "Not quite"/"Correct" (no heading when it equals the verdict). For open: one line `3/3 points (self-assessed)`. Summary: drop the third goal line and suppress a toast that repeats the panel.
- **Effort**: S

### UX-06 Disabled "Ask why" costs ~170px in every feedback panel and explains infrastructure
- **Seen**: A greyed `Ask why` row plus "Needs the AI tutor, which is off while you are offline or no AI is set up." appears after every question, right/wrong. The icon slot is blank so the label looks indented.
- **Why**: Dead-end control, repeated ~300 times; users learn to ignore the whole panel footer. "no AI is set up" is developer language.
- **Shot**: `$S/06-q1-feedback-wrong.png`
- **Fix**: When `explain` is undefined, render nothing in lessons, and show the explanation once in Settings > "AI features" (and the Home footer already says "AI grading off"). If you must keep it, a single muted line `Ask why needs AI (off)` with an `info` icon, no second sentence.
- **Effort**: S

### UX-07 The achievement toast covers the content it announces and follows the learner into the next screen
- **Seen**: On the lesson summary the toast sits on the result heading (`Another pass needed` / `Clean run` clipped under it). It stays on screen into the next lesson and covers the kicker and the first prompt line. Challenge summary: both "RESULT" label and heading hidden.
- **Why**: It hides the one headline the learner came for and can hide the question text. The text also runs together: "First lesson, In your own words. You can see all of them on the Progress page." (capital `In`, list punctuation).
- **Shot**: `$S/s4-summary.png`, `$S/s6-L4-summary.png`, `$S/s6-L3-spot-a.png`, `$S/32-challenge-after.png`
- **Fix**: Toast to the bottom above the bar (`bottom: calc(env(safe-area-inset-bottom) + 72px)`) or put new badges inline on the summary and skip the toast on `#/done`. Auto-dismiss at 4s and clear on route change. Copy: `New badge: First lesson` / `2 new badges: First lesson and In your own words` (names as bold chips).
- **Effort**: S

### UX-08 Progress numbers contradict what the learner just did (demotivating, and inconsistent)
- **Seen**: After 4 lessons (3 at 100%) Home says `Readiness 0` (day 2) then `Readiness 1`; Progress shows Kotlin Essentials 6% with `25/25 seen`, and `Your weakest topic: Testing & Quality (0%)` picked arbitrarily among 11 zeros. Home `Today ... 5 due` vs `Review due 30+ ready` (day 2), `24 ready` after reviewing 10. Summary of a lesson finished (all questions eventually right) says `Another pass needed` at 57%; a review at 60% also says it.
- **Why**: The motivational loop depends on credible progress. A perfect streak that yields "1/100" feels broken; "30+ due" after 4 lessons feels like debt, against the stated "calm" rules.
- **Shot**: `$S/20-day2-home.png`, `$S/60-progress-light-p1.png`, `$S/s4-summary.png`, `$S/23-review-summary.png`
- **Fix**: (1) Hide the readiness score until >=25% of the course is seen; show `Readiness: not enough data yet (4 of 34 lessons)`. (2) Show strength next to "last result" (`6% recall · you got 25/25 right`) or start new cards at a higher floor. (3) One definition of "due": Today's `5 due` should read `5 of 24 due`; Review card `24 ready (do 10 now)`. (4) Result headline by outcome: `Lesson complete` + accuracy, reserve `Another pass needed` for not passing; for reviews use `Review done`. (5) Weakest topic: ignore topics with <3 seen.
- **Effort**: M

### UX-09 Code editor: the symbol bar eats half the screen and test results hide under the bottom bar
- **Seen**: 22 symbol keys wrap into 4 rows (~400px at 390 wide) with a lone `,` on row 4. With the OS keyboard open there is almost no editor left. After `Run tests`, results appear below the fold, partly behind the fixed bar (`expected {"1":["a","c"],"2":["bb","d...` is cut), with no auto-scroll. The language (JavaScript) is not labelled, in a Kotlin lesson whose prompt says "like Kotlin's groupBy".
- **Shot**: `$S/s6-L2-code-a.png`, `$S/s6-L2-code-b-wrongrun.png`
- **Fix**: One horizontally scrollable row of keys (`overflow-x:auto; flex-wrap:nowrap; scroll-snap-type:x proximity`) pinned right above the keyboard via `position:sticky; bottom: 0`; call `scrollIntoViewGently(results)` after a run (same helper Lesson uses); add a `JavaScript` tag on the editor like the code blocks' `KOTLIN` tag.
- **Effort**: M

### UX-10 Mock interview: the "Submit your answers?" confirmation is clipped by the bottom bar
- **Seen**: Tapping `Finish early` opens an inline panel under the question grid; its `Submit` button is half hidden behind the fixed bar and the page does not scroll to it. The bar still offers Flag / Back / Skip. The 1-10 navigator wraps to two rows, the second row partly under the bar. The timer chip sticks out past the 16px gutter.
- **Shot**: `$S/44-mock-finish-confirm.png`, `$S/41-mock-run-q1.png`
- **Fix**: Show the confirm in the existing bottom `Sheet` (focus-trapped, own buttons) and disable the bar while it is open; or `scrollIntoView({block:'center'})`. Grid: `grid-template-columns: repeat(auto-fill, minmax(44px, 1fr))` and put it in a collapsible "Questions" row.
- **Effort**: S

### UX-11 Library results cannot be told apart
- **Seen**: Cards show only the prompt: `What does this print?` appears again and again with no code, lesson or answer hint; 307 questions. A search for "suspend" returns `Which problem does this code have in a coroutine-based service?` with no code.
- **Shot**: `$S/50-library.png`, `$S/52-library-search.png`
- **Fix**: Show 2 lines of the question's code (mono, clipped, `line-clamp:2`) or `1.1 Null safety & types` as the secondary label; highlight the search term. Sort identical prompts by lesson.
- **Effort**: S

### UX-12 Open question self-check: the thing you must compare against is collapsed
- **Seen**: The panel says "compare with the model answer and tick the points you covered" but `Model answer` is a closed disclosure, and the header already shows a green check and `SELF-CHECK · 3/3 POINTS` before you have compared. The rubric points are listed first, so reading them and ticking is possible without ever writing a thought (in the mock results, keyword matching pre-ticked 1/4).
- **Shot**: `$S/s4-07-open-c-fb.png`
- **Fix**: Open the model answer by default in self-check mode; use a neutral (ink) marker until the learner finishes ticking, then colour it; label the rubric `Which points did you make?`.
- **Effort**: S

### UX-13 Summary actions are ambiguous and the "Next lesson" primary is wrong after non-lesson sessions
- **Seen**: Two buttons `Next lesson ->` (ink) and `Continue` (outline) under every summary, including review, daily challenge and mock. `Continue` goes home but is not named so. `STREAK 1 d` (the `d` floats apart), `34 of 8 questions today`, `Next review: tomorrow (7)` (unexplained count), `Daily goal met.`
- **Shot**: `$S/23-review-summary.png`, `$S/s6-L4-summary.png`
- **Fix**: Buttons `Next lesson` / `Back to Today`; after review/challenge make the primary `Back home`. Format `1 day`; cap the ring text at `8/8` and show `+26 bonus` below; `Next review: 7 cards tomorrow`.
- **Effort**: S

### UX-14 Quitting a lesson discards it behind a browser `confirm()`
- **Seen**: Tapping X shows the native `Quit this lesson? Progress in it will be lost.` dialog (no app styling, in a PWA it looks foreign) and the lesson is not resumable.
- **Fix**: Use the existing `Sheet` with `Keep going` (primary) and `Quit and lose progress` (text, vermilion); better, persist the in-progress session in sessionStorage and resume.
- **Effort**: M

---------------------------------------------------------------

## P2 - polish

| id | screen | seen | fix | effort |
|---|---|---|---|---|
| UX-15 | Home (any day) | The lesson list, the main curriculum, starts ~1,700px down; `Up next: ...` is a bare blue underlined link in a type style used nowhere else, and it wraps under `4 / 34 LESSONS`. `$S/20-day2-home.png` | Make "Up next" a normal `.card` row (number, title, Start) directly below the practice cards; collapse units 4+ behind "Show all units". | M |
| UX-16 | Home | `Daily challenge` card and `Review due` card touch (about 1px gap vs 8-9px elsewhere). `$S/02-home-after-skip.png` | `.challenge + .practice { margin-top: var(--s2) }` or give `.card` consistent `margin-block`. | S |
| UX-17 | Home, day 2 | After the daily goal is met the hero still says `Today · 8 questions · Start` with a full ring; `Daily challenge: passed` card squeezes title into 4 lines beside `Practise it again`. `$S/33-home-after-challenge.png` | Hero -> `Bonus round` once goal met; in the passed card move `Practise it again` below the sub-line or drop it. | S |
| UX-18 | Lesson, feedback | XP in the top bar stays at the old value while the panel says correct; it only ticks after `Continue`. | Update XP (and flash `+10`) on Check, the moment of the reward. | S |
| UX-19 | Lesson | Hint button reads `Hint (-3 XP)`: a price tag on help. | Show `Hint` and put the cost in the revealed box (`-3 XP`), or make the first hint free. | S |
| UX-20 | Lesson, multi | A partly right answer is marked `CORRECT` on the option and `INCORRECT` on the panel with no count. `$S/s4-02-multi-c-fb.png` | `Partial: 1 of 3 right, 2 missed` using the existing partial style (ochre, dashed rail). | S |
| UX-21 | Lesson | Triple instruction on multi: kicker `SELECT ALL THAT APPLY`, prompt `Select ALL statements...`, helper `Select all that apply`. | Drop the helper line when kicker says it; same for `Tap the lines in the right order` under `PUT IN THE RIGHT ORDER`. | S |
| UX-22 | Lesson, order | Option lines keep their code indentation so the structure is leaked; wrapped lines in feedback get one wavy underline across two lines. `$S/s4-06-order-c-fb.png` | Left-align pool tiles (trim indent) and render indent only after placement; underline per line box (`text-decoration-skip-ink`, `box-decoration-break: clone`). | S |
| UX-23 | Lesson, spot/estimate | Code lines clip (`listOf("a`, `muta...`, `// collecto`) and scroll sideways with no cue. (`.spot.code` is 487 wide in 356.) | Right-edge fade (`mask-image: linear-gradient(to right,#000 90%,transparent)`) or `white-space: pre-wrap` for spot lines. | S |
| UX-24 | Lesson | The screen never says which lesson/unit or that it is the daily challenge/review/mix; header has X, bar, XP only. | Put `1.1 Null safety` (or `Daily challenge`, `Review`) in the progress bar's aria-label AND as tiny mono text above the kicker. | S |
| UX-25 | Lesson / nav | First question after a hash navigation shows a heavy blue focus box on the prompt (and on the Progress `h1` in dark). `$S/23-review-first.png`, `$S/94-progress-dark.png` | Programmatic focus on `tabindex=-1` headings should not draw a ring: `[tabindex="-1"]:focus:not(:focus-visible){outline:none}`; verify with touch (it may only show after keyboard use). | S |
| UX-26 | Open question | `wifi-off` icon + mono `AI grading off. You self-check against the model answer.` reads as "you are offline". | Use a `pencil`/`check-square` icon and plain text: `Self-check: you compare with the model answer.` | S |
| UX-27 | Summary/Progress | Badge dates are ISO (`UNLOCKED 2026-11-05`); XP uses hard-coded `toLocaleString('en-US')`; the date input shows `mm/dd/yyyy`. | `Intl.DateTimeFormat(undefined,{day:'numeric',month:'short'})` and `toLocaleString()`. | S |
| UX-28 | Copy | Mixed British/American: `practise`, `behaviour`, `uninitialised` vs `initializer`, `Customize`-style words. `Select ALL` capitalised. | Pick one (British is dominant); add to content check. | S |
| UX-29 | Settings | 1,870px tall wall of 3-4 line descriptions. `Notify me when I open the app` says in its own help text "this will not arrive on its own"; `Interviewer's tip` explains the hash rule. `$S/70-settings-s1.png`, `$S/70-settings-s2.png` | Group into 4 sections with one-line helpers; delete or hide the "notify on open" toggle until it can work; move rationale to a "How this works" link. | M |
| UX-30 | Settings | `Copy backup text` and `Import backup file` are borderless ghost buttons, `Paste backup text instead` is a heading with no affordance, `Reset progress` is a small underlined link. | Same outlined secondary style for all three data actions; make "paste" a disclosure with a chevron; reset as a vermilion text button with a confirm sheet. | S |
| UX-31 | Settings | Reading size `S M L XL` radio buttons give no preview. | Render the letters in their actual size or add a sample line. | S |
| UX-32 | Library | Note displays as `Your note Remember: suspend...` (label inline with text). Filter chips (`Saved (1)`, `Never seen`) wrap awkwardly. `$S/56-library-after-note.png` | `<b>Your note</b>` on its own line (block); give chips `flex-wrap` with equal basis or horizontal scroll. | S |
| UX-33 | Progress | 13 topic rows of mostly `0%` and 27 badge cards (18 dashed "LOCKED") produce a 9,300px page. `$S/60-progress-light-full.png` | Show only started topics, with "10 more topics not started" disclosure; badges: unlocked first, locked ones as a compact 2-col grid. | M |
| UX-34 | Mock | Results screen uses a big sans `33%` while lesson summaries use a mono readout; results must be saved with `Save results` or they are lost on back. `$S/45-mock-results.png` | Same `.readout` component; autosave and offer `Discard`. | S |
| UX-35 | Mock setup | Four bullet paragraphs of rules (XP caps, half-goal) before Start. | Keep two bullets (timer, no feedback); move XP rules into a `How scoring works` disclosure. | S |
| UX-36 | Share card | The card shares `Readiness 1`, `Strongest topic 6%`, `Weakest topic 0%`: unflattering numbers nobody would post, and "Weakest topic" is not a thing to share. `$S/80-share-image.png` | Share streak, days practised, lessons done and rank only; disable the card until streak >= 3 or readiness >= 20. | S |
| UX-37 | Errors | `#/q/zzz` and `#/topic/zzz` both say `Lesson not found`. | Parameterise the noun (`Question not found`, `Topic not found`). | S |
| UX-38 | Desktop (1280) | The 608px column is left-aligned content in a flat grey field: 340px of nothing each side; lessons use the same column. `$S/10-home-after1-d1280-light.png` | At >=1024px: Home two-column (left: hero + practice cards, right: lesson list), lesson/feedback side-by-side (question left, feedback right, no fixed bottom bar). Minimum: raise the column to 720px and centre. | L |

(Touch targets: I measured every interactive element on Home, Lesson, Library and Progress at 390, 320 and 1280; all are at least 44x44. No horizontal page scroll at any of the three sizes. No console errors in any run.)

---------------------------------------------------------------

## What is already good - keep it

1. **The visual identity is real.** Cool paper + ink, hairline rules, mono labels, plex sans prompts; verdicts as a rail, a word and an icon (strike-through on wrong options, dashed for "the answer"). Colour is never the only signal. This does not look like a generic AI template; the Home list of icon-tile cards is the one place it drifts there.
2. **Wrong-answer feedback is excellent in content**: your choice struck through, correct one dashed with `ANSWER`, a short explanation, correct solution for order questions, a coloured stripe on the bar. Wrong questions come back as `TRY AGAIN` in the same session.
3. **Dark mode is clean.** Navy ground, inverted ink buttons, readable tints; no defect found on Home, feedback, Progress, Library, sheets.
4. **Tone** is factual and non-manipulative (rest days, freezes, "ranks never go down", no hearts, no guilt). Streak and goal sheets are well explained and honest.
5. **Touch and layout discipline**: 44px targets everywhere, no overflow at 320, long identifiers in notes wrap correctly, estimate input uses `inputmode=decimal`, code editor disables autocapitalise/spellcheck.
6. **Mock interview design** (flag, navigator, no feedback, resume-safe timer, per-topic results, "Practise" links) is a strong feature; interviewer follow-ups on open questions are a delight.
7. **Spaced review hints** (`You'll see this again tomorrow.`, `Next review: tomorrow (7)`) show the point of the loop at the right moment.
8. **404 / empty states** exist and are calm (`Nothing to review`, library empty message).

## Does it still look "AI generated"?
Mostly no: the drafting-sheet system is specific. Residual tells: (a) the stack of same-shaped icon cards on Home, (b) self-justifying microcopy ("costs nothing", "never gates anything", "decided by the date and your session count") that explains the ethics instead of just behaving well, (c) uniform 3-line helper text under every control in Settings, (d) badge names such as "Back of the envelope" / "Every format" fine, but the badge list is a uniform dashed-card grid with no hierarchy.

## Motivational loop: what is missing
- A visible "after this lesson you will have X" and an end-of-lesson "what you learned" list (2-3 concepts with link to Library) - the summary shows numbers only.
- A weak-spot recap on the summary ("revisit: Elvis operator") with a one-tap `Practise now` (the app knows the missed questions).
- Credible progress for good work (see UX-08): the first-week curve must go up.
- A small, honest peak moment at lesson completion (the summary is the quietest screen in the app even for a 100% perfect lesson; reduced-motion respected tick or ring fill animation would be enough).
- A plan view when an interview date is set (`26 days to interview` is one line on Home; a "this week" strip would make the date matter).

---------------------------------------------------------------

## Top 10 recommended changes (in order)

1. UX-01 Fix match items with inline code (P0, S) - unreadable questions.
2. UX-03 + UX-04 Reshape first-run Home: hero first, onboarding collapsed, other cards hidden until lesson 1 done (P1, M).
3. UX-08 Make the numbers credible: hide Readiness until enough data, fix the Today vs Review due mismatch, rename "Another pass needed" (P1, M).
4. UX-05 Kill the repeated verdict words in feedback, summary and open-answer panels (P1, S).
5. UX-06 Remove the permanent disabled "Ask why" block when AI is off (P1, S).
6. UX-07 Move or retire the achievement toast so it never covers headline or prompt (P1, S).
7. UX-09 Code editor: single scrolling key row, auto-scroll to test results, language tag (P1, M).
8. UX-02 Repair `npm run build` (test type error) and gate CI on it (P0, S).
9. UX-10 + UX-13 Mock submit confirm in a sheet; summary buttons `Next lesson` / `Back to Today` and `1 day` formatting (P1, S).
10. UX-11 + UX-12 Library cards show code/lesson; open self-check shows the model answer by default (P1, S).

Runner-up: UX-38 desktop two-column layout (L), UX-29 Settings trimming (M).
