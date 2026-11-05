> **Archived research, snapshot.** Round-2 UX re-audit after the round-1 fixes: verification table plus new findings N-1..N-18. Status after the round-2 fixes is in the handoff.
>
> Written on 2026-11-04/05 while this app was being built, by an agent working from the brief described in [the handoff](../HANDOFF.md). It is kept as written so later decisions can be traced back to it. Paths such as `$S/...`, `$QA/...`, `$QA2/...` or `/tmp/claude-0/...` point at screenshots and scripts in that session's scratch space and are **not** in the repository. For what has since been built or fixed, the handoff is the source of truth.

# UX audit round 2: Interview Trainer (after the fix round)

Build: merged `claude/engineer-interview-training-82crnr` (3fdb5f2), `npm run build` passes, served at :4193, AI grading off.
Method: Playwright (repo's Playwright, Chromium 1194) at 390x844, 320x640 and 1280x800, light and dark. Journeys: brand-new user through 3 lessons (1.1, 1.2, 1.3) with deliberate mistakes; next day with a moved clock (Today, Review, Daily challenge, Library, Mock, Settings, Progress, Share card); a power user (a seeded year of history: 150 days, all 34 lessons, 307 cards, 10 mocks, 27 badges, 30 saved, notes, interview date in 19 days); data-safety states (corrupt, newer-version, recovered). I covered all 9 question types at all five viewport/theme combinations. No horizontal overflow at any size, no console errors in any run. I touched no repo file.
Screenshots: `/tmp/claude-0/-home-user-google-takeout-helper/959a0803-75f1-5c6e-8ab6-3e44ad11da40/scratchpad/audit-ux2/` (`$S`). Scripts: `$S/work/`.

Counts of NEW findings: **P0: 0, P1: 4, P2: 14**.

Not verified (be aware): the OS soft keyboard with the code editor key row (headless), the service-worker "new version" banner together with the other banners (each tested alone), real notifications, real file download/import through the OS picker.

---

## (a) Round-1 P0/P1 status

| id | topic | state | evidence |
|---|---|---|---|
| UX-01 P0 | Match items with inline code | **Fixed** | Readable at 320 and 1280; items wrap as phrases, `this`/`it` chips stay inline. `$S/d1-small-light-match.png`, `$S/d1-desk-light-match.png`; wrong-answer review `$S/c1L2-match-wrong.png` |
| UX-02 P0 | `npm run build` fails | **Fixed** | build completes (only the chunk-size warning) |
| UX-03 | First-run form pushes first lesson down | **Fixed** | Hero "Start your first lesson" is the first card at 390 and 320; plan is a collapsed dashed card; expanded form has `Save plan` / `Not now`. `$S/a1-home-first-fold.png`, `$S/k1-onboarding-filled-320.png` |
| UX-04 | Six equal cards on first run | **Fixed** (one residual, see N-12) | Only hero + "More ways to practise" on day 0; daily modes appear after lesson 1, all six after lesson 3. `$S/c1-home-after1-fold.png` |
| UX-05 | Verdict repeated 2-3 times | **Fixed in lessons, partly in summary** | Wrong single: one `INCORRECT` word + struck option + dashed `ANSWER` (`$S/f1T-single-wrong.png`); open: `3 of 3 points ticked`. Summary still says "Daily goal met" and "Goal met." (see N-9) |
| UX-06 | Disabled "Ask why" block | **Fixed (reduced)** | Now one muted line `Ask why needs AI, which is off.` inside the panel. Acceptable, still a little developer-voiced |
| UX-07 | Achievement toast covers content | **Fixed** | Badges are an inline card on the summary (`3 new badges ... See all badges`), no toast over the headline. `$S/b2-summary-L1.png` |
| UX-08 | Progress numbers contradict | **Partly fixed** | Readiness hidden until ~25% seen (`Not enough data yet · 8% of the course seen`); headline by outcome (`Clean run`, `Lesson complete`, `Review done`); Today and Review agree up to 30. Still: due counts disagree at scale (N-3), topic strength 5% after three clean lessons, "weakest topic" with one topic (N-10) |
| UX-09 | Code editor key row, results, language | **Fixed** (keyboard untested) | One horizontally scrolling key row, `JAVASCRIPT RUNS IN YOUR BROWSER` tag, results appear in the feedback panel. `$S/d1-phone-light-code.png`, `$S/c1L2-code.png` |
| UX-10 | Mock submit confirm clipped | **Fixed** | In-app dialog `Submit your answers?` with `Keep going` / `Submit`; timer chip inside the gutter. `$S/g2-mock-finish-confirm.png` (focus issue: N-13) |
| UX-11 | Library cards identical | **Fixed** | Code excerpt, topic, lesson `1.1 Null safety & types`, status chip, `Practise this one`. `$S/j1-phone-light-library.png` |
| UX-12 | Open self-check collapsed | **Fixed** | Model answer open by default, neutral `Self-check` header, `Which points did you make?`. `$S/b2-open.png` |
| UX-13 | Summary actions ambiguous | **Mostly fixed** | `1 day`, `Back home`, `Next lesson`, `Next review: 7 questions tomorrow`. Primary swaps (after a lesson `Next lesson` is primary, after Today/Review/Challenge `Back home`): fine, but see N-9 |
| UX-14 | Quit via `confirm()` | **Fixed** | In-app dialog, `Keep going` focused first, focus trapped (2 buttons cycle), Esc closes and focus returns to the X; no dialog when nothing was answered. `$S/c1-quit-dialog.png`. Lesson is still not resumable (by design, now stated) |

Also re-checked: UX-25 focus ring (no ring after tap navigation; only visible after scripted hash navigation), UX-18 XP now shown `+10` at Check, UX-36 share card (streak, rank, lessons, days; no unflattering numbers) `$S/l1-share.png`, UX-30 Reset now has a confirm dialog (`Keep my progress` focused, Esc returns focus to the Reset link).
Round-1 P2s still open: UX-17 (see N-2), UX-29 Settings length (N-14), UX-27 ISO dates (N-11), UX-33 Progress length for a heavy user (N-15), UX-38 desktop (now one centred 720px column with borders, no two-column, acceptable).

---

## (b) NEW findings

### P1

**N-1 Data-safety banner swallows a small screen and a lesson, and its wording is confusing for a new user**
- Screen/state: corrupt or unreadable saved data (`{garbage`) at 320x640 and 390; Home and Lesson.
- Seen: a red 7-line notice ("Saved progress could not be read, so nothing new is being saved. The old data is left untouched. What you do now is kept in this tab only, and we keep trying to set a copy of the old data aside. Download a backup so nothing is lost.") plus three buttons takes ~270px at 390 and the first screen at 320, pushing the hero "Start your first lesson" below the fold. The same banner sits above the lesson progress bar, so the question starts ~330px down.
- Why it matters: this is exactly the state where a user is anxious; the copy is long and internal ("set a copy aside", "kept in this tab"), and "Download a backup" of what? There is nothing to download for a new profile. It also breaks the first-run guarantee that the hero is on screen.
- Shot: `$S/i1-A-corrupt-home-320.png`, `$S/i1-A-corrupt-lesson.png`
- Fix: one-line collapsed bar ("Your saved progress could not be read. Nothing new is saved. Details") that expands on tap; hide inside lessons (show a small icon in the lesson header instead); name the real consequence ("close this tab and today's work is gone"); show `Download backup` only when there is something to export. Effort: S-M.
- (Positive: the post-recovery path works; `Recovered data` section in Settings lists the parked copy with Download/Copy. `$S/i2-settings-recovered.png`.)

**N-2 Goal-met state is not reflected: the hero still sells "Today", the numbers disagree between Summary and Home, and the passed-challenge card is cramped**
- Screen/state: day 2 after the daily goal is met (8/8 and then 19/8).
- Seen: Home hero stays `Today · 8 questions · 5 of 9 due, 2 weak spots, 1 new · Start` (only on day 1 after the lessons did it read `Bonus round`). The ring on Home says `19/8`; the same session's summary says `8/8` and `8 of 8 questions today` with `Goal met, plus 11 bonus questions` underneath. The `Daily challenge: passed` card breaks its title over two lines, with `Practise it again` as unstyled text on the right. This is UX-17 not fixed.
- Why: the learner finished their goal and Home still looks like the morning. Two different numbers (8 vs 19) for "questions today" look like a bug, and an unaffordant text button is easy to miss.
- Shot: `$S/f1-home-after-challenge.png`, `$S/f1-summary-challenge.png`, `$S/f1-summary-review.png`
- Fix: after the goal, hero = `Done for today. 19 questions.` with secondary `Keep going`; make the summary ring show the real number (`19 of 8`) or only the sentence; give `Practise it again` a button/chevron and put it under the sub-line. Effort: S-M.

**N-3 "Due" is counted three different ways for an experienced learner**
- Screen/state: power user (73 due).
- Seen: Home `Today ... 6 of 73 due`, `Review due ... 30+ ready · 10 at a time`, Progress topic rows add up to 62 due; below 30 the Review card says `18 ready · 10 at a time`, then after one review `9 ready to review` (different phrase for the same card).
- Why: this was the heart of UX-08 and a returning heavy user sees it first. The "30+" cap hides that the backlog is 73 and growing; the learner cannot judge if the habit is sustainable.
- Shot: `$S/j1-phone-light-home.png`
- Fix: one definition and one formatter: `73 due · 10 per session`, same in Today, Review and Progress; use the same phrase at every count. Effort: S.

**N-4 The habit loop has no bridge to tomorrow at the moment it matters**
- Screen/state: summary after the first lesson, and day 2.
- Seen: the first summary ends with `Next review: 7 questions tomorrow`, but nothing asks when they will practise or offers the reminder; the reminder and `.ics` live only in Settings > Reminders (and `Notify me when I open the app` says itself that it cannot wake the app). The onboarding card that asks "When will you usually practise?" is collapsed and optional; most users never open it. After 3 lessons on day 1 the learner has 25 cards due on day 2 vs a Today session of 8 (5 due), i.e. an immediate backlog on the very first return.
- Why: the app is calm and honest, but a reason to return tomorrow is only implied. Without a prompt the realistic tomorrow is "forgot".
- Fix: on the first-ever summary add one quiet row `Practise again tomorrow? Add to calendar (.ics)` (uses the existing calendar export), and phrase tomorrow concretely (`Tomorrow: 7 reviews, about 4 minutes`). Cap day-2 backlog (spread the first-day cards over 3 days) so lesson 3 on day 1 does not create 25 due. Effort: S for the prompt, M for spreading.

### P2

| id | screen/state | seen | why | fix | effort |
|---|---|---|---|---|---|
| N-5 | Home, Today subline after a flawless lesson | `2 weak spots, 6 new` appears after a 100% lesson (day 1) and stays all day | "weak spots" is untrue and a mild guilt cue | Say `2 to reinforce` or omit when nothing was missed | S |
| N-6 | Progress/Home, readiness label | Power user: `Readiness 70 · Interview-ready` while weakest topic is 47% and mocks fell from 85% to 40% | strong claim from self-reported recall; contradicts the app's own disclaimer | Rename band (`Solid`, `Getting there`), mention weakest topic next to the score | S |
| N-7 | Progress, early | `Your weakest topic: Kotlin Essentials (5%)` when it is the only topic started; Kotlin at 5% after three lessons with 100/89/... % | deflating despite the explainer ("a perfect first lesson starts at a few percent") | Hide weakest-topic card until 3 topics started; show `3 lessons, 25 questions` beside the strength | S |
| N-8 | Summary | follow-up question is not counted: 8 answers but `7 of 8 questions today`; `Perfect lesson / Every question right on the first try` card repeats the `Clean run 100% first-try accuracy` above | numbers differ from what the learner did; stutter remains | Count follow-ups toward the goal; drop the card text when the heading already says it | S |
| N-9 | Summary after Today | `Daily goal met / Anything more today is a bonus.` and the ring caption `Goal met.` say it twice | UX-05 leftover | Keep one | S |
| N-10 | Home day 0 (lesson list) | `UP NEXT` tag on 1.1 sits next to a hero that already says "Start your first lesson"; later the Up-next card sits below Library (~700px down at 390) and competes with the hero "Today" | two primary paths, both labelled start | On day 0 hide the tag; once all six cards show, move Up next directly under the hero | S |
| N-11 | Dates | Badges `Nov 4, 2026` but Mock history `2026-11-03`, Recovered data `2026-11-04` | UX-27 only half done | Use the same formatter everywhere | S |
| N-12 | Home after lesson 3 | all six cards are again the same icon-tile rows (Today, Daily, Review, Mixed, Progress, Mock, Library) | flat hierarchy, generic look returns after the first-run fix | Give Today the only solid tile (done), turn Mixed/Mock/Library into a compact 3-up row | M |
| N-13 | Mock finish dialog | focus lands on `Submit` (the consequential action) for `10 questions have no answer yet and will count as zero` | Enter submits a blank mock | Focus `Keep going` when unanswered > 0 | S |
| N-14 | Settings | Still ~1,760px; `Notify me when I open the app` explains in its own help text that it will not work when closed; `Paste backup text instead` is a bare heading; `Reset progress` is a small underlined link (confirm is good) | UX-29/30 leftovers | Remove or gate the notify toggle; style paste as a disclosure; make reset a text button in a Danger section | M |
| N-15 | Progress, heavy user | 5,260px at 390: 13 topic rows, 10 mock rows, 27 unlocked badges | long scroll for the person who uses it most | Collapse mocks to the last 3 + trend; group badges by group with counts | M |
| N-16 | Library card | Bookmark/edit icons top-right AND `Saved` / `Edit note` buttons below; note text starts with a leading space | duplicated controls | Keep one set | S |
| N-17 | Ranks | `Engineer` reached after 2 lessons (164 XP), celebrated with a banner on the summary | ranks lose meaning (Associate to Engineer is 150 XP, Senior is 500) | Raise first thresholds or celebrate silently for the first rank | S |
| N-18 | Lesson | Banner/limit: a lesson cannot be resumed after quit (now clearly stated, but 9-13 question lessons are long on a phone) | losing progress in 10 minutes is a real exit | Save in-progress session to sessionStorage and offer `Resume` | M |

---

## (c) Top 5 to do next

1. **N-2 Goal-met Home and consistent "questions today"** (P1, S-M): the screen the habit returns to must acknowledge what was done.
2. **N-3 One definition of "due"** with the same words everywhere (P1, S).
3. **N-4 Bridge to tomorrow**: calendar/reminder row on the first summary, concrete "tomorrow" preview, spread the day-1 backlog (P1, S-M).
4. **N-1 Make the data-safety banner small and plain** (and hidden inside lessons) (P1, S-M).
5. **N-6/N-7 Honest labels at the extremes**: no "Interview-ready" from recall alone; no "weakest topic" from one topic; drop the "weak spots" wording on a flawless day (P2, S).

---

## (d) What is good and must be kept

- First run is now excellent: one hero, optional plan collapsed, six modes revealed as you earn them (`$S/a1-home-first-fold.png`).
- Match, code, order, spot and estimate questions are readable at 320 and 1280, light and dark; no overflow anywhere.
- Feedback: single verdict, struck user answer + dashed correct answer, short explanation; open self-check shows the model answer first; XP `+10` at Check.
- In-app confirm dialogs (quit lesson, submit mock, reset) with correct default focus, trap and Esc/focus-return; share sheet returns focus to its button.
- Honest numbers: readiness is hidden until enough data and says why; share card shows only flattering-or-neutral facts; summary headline by outcome (`Clean run`, `Lesson complete`, `Review done`).
- Data safety: corrupt/newer data is parked, never destroyed, and recoverable in Settings.
- Forgiving habit mechanics: rest days, freezes, "ranks never go down", `Bonus round`, no guilt copy.
- Library cards with code, topic and lesson; Mock interview setup and results; reading-size previews; calendar `.ics`.

## Habit loop, honestly

Reason to return tomorrow: yes, but implicit. What pulls: due reviews (`Next review: N tomorrow` on every summary), streak with freezes, daily challenge streak, interview date plan (`19 days to interview, 16 days to spare`), rank bar. What is missing: an explicit ask to schedule tomorrow (N-4), a concrete preview of tomorrow, a weak-spot recap on the summary ("revisit: ..." with one tap), and a smaller first-return backlog. Manipulation check: none of the dark patterns (no hearts, no loss framing, no guilt, no push by default). The one variable-reward element, the "Worth remembering" tip about 1 session in 6 with +5 XP, is disclosed with its odds in Settings; it is mild but it is a variable-ratio reward, so keep it off by default for new users or say so on the card. "Weak spots" wording on a perfect day (N-5) and the `19/8` ring are the only guilt-adjacent or confusing signals.
