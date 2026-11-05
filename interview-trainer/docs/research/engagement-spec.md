> **Archived research, snapshot.** Behavioural-science spec for the habit loop (spaced repetition, goals, forgiving streaks, readiness, onboarding, ethics line). Most of its recommended batch is built; see docs/engagement.md for what the code does.
>
> Written on 2026-11-04/05 while this app was being built, by an agent working from the brief described in [the handoff](../HANDOFF.md). It is kept as written so later decisions can be traced back to it. Paths such as `$S/...`, `$QA/...`, `$QA2/...` or `/tmp/claude-0/...` point at screenshots and scripts in that session's scratch space and are **not** in the repository. For what has since been built or fixed, the handoff is the source of truth.

# Interview Trainer: making it habit-forming (and actually effective)

Status: spec only. No repo changes were made. Grounded in: `shared/src/{progress,session,schema,grade}.ts`,
`web/src/{App.tsx,screens/*,lib/store.ts}`, `web/public/sw.js`, `e2e/question-types.spec.ts`, and the screenshots in `e2e/screenshots/`.

## 0. Reading of the brief, and the line we draw

"Addictive" is read as: **a loop that is easy to start, rewarding to finish, and that brings you back because the app
knows what you are about to forget.** The strongest retention lever for a learning app is not confetti, it is a
*due queue that is always the right next thing to do* (spaced retrieval). Gamification is the wrapper, not the engine.

The loop we are building:

```
Cue (home shows ONE "Today" card; optional calendar/notification the user chose)
  -> Action (<= 5 min: 8 interleaved questions; one tap to start)
  -> Variable-but-benign reward (progress ring fills, rank bar moves, occasional tip/bonus, celebration scaled to the achievement)
  -> Investment (cards scheduled, mastery map grows, streak/days accumulate; all user-owned, exportable)
  -> next Cue (tomorrow "N ready to review", countdown to the interview date)
```

### Where the ethical line is (explicit)

| We DO | We do NOT |
|---|---|
| Streaks that forgive: earned freezes, user-chosen rest days, "longest streak" and "days practised overall" that never reset | Streaks that can only be saved by paying/watching ads/waiting, or that reset to 0 without recourse |
| Neutral, factual reminder copy ("6 questions are ready to review") that the user opted into, max 1/day, quiet hours | Guilt copy ("Your streak is in danger!", sad mascot, "you'll lose everything"), red badge counters, repeated nags |
| Variable reward that is **only ever additive**, never gating, never costs anything, and whose odds are disclosed in Settings (and can be turned off) | Loot-box style mechanics, fake "limited time" offers, countdowns that create urgency, hearts/lives that block practising |
| Real deadline math the user entered (interview date) shown calmly; the plan silently re-computes when behind | Fake scarcity, fake social proof, fake leaderboards/"other learners", invented user counts |
| Loss-framing only in the neutral form "your freeze will cover yesterday" (positive: something protected you) | "Streak at risk" banners, countdown-to-midnight timers, loss-aversion upsells |
| Timed mock interviews, **opt-in**, with no XP penalty | Time pressure in normal lessons |
| Celebrations scaled to real milestones; reduced-motion and sound-off respected; every effect has a toggle | Constant confetti (devalues rewards), auto-playing sound, haptics that cannot be disabled |
| Everything local; Export/Import JSON so the user owns their data | Anything that needs a server, accounts, or pretends other people exist |

Rule of thumb used in review: *if the screen would still be defensible read aloud to the user, ship it.*

## 1. Ground truth (what exists today)

- `Progress` v1: `{version:1, xp, streak, lastDay, lessons{bestAccuracy,completions,lastDay}, missed[]}`; key `interview-trainer.progress.v1`; `load()` shallow-merges over `emptyProgress()`.
- `nextStreak`/`liveStreak`: hard reset after a 1-day gap; `liveStreak` shows 0 after one missed day. No forgiveness.
- `recordResult` is the single write path for a finished session (`App.finish`). It is pure and unit-tested: **new logic extends this path; do not scatter writes.**
- `missed[]` is a set of question ids; `Review mistakes` consumes it. There is no memory of *correct* answers, so nothing mastered is ever revisited.
- Question ids are positional (`<lessonId>/<n>`): reordering/inserting questions in YAML silently re-points any per-question state. Cards must therefore carry a prompt hash (see 2.2).
- Content has no `tags`/`difficulty` values in use; **topic = unit id** (`kotlin, coroutines, spring, jvm, ddd, scale, complexity, ai`). Lesson id = `qid.split('/')[0]`.
- `Practice` (mix/review) picks questions once on mount with `Math.random()`. `Lesson` runs `Session` and calls `onFinish(summarize(s))`.
- Existing e2e constraints that the new work must not break: specs `page.goto('/#/lesson/<id>')` directly on an empty profile and assert `.summary h1` equals `Lesson complete!`. **Therefore: no onboarding modal/redirect, no overlay that blocks the first lesson, celebrations never replace the `h1`, and Playwright runs under `reducedMotion: 'reduce'`.**
- Offline/PWA: `sw.js` cache `interview-trainer-v1` caches the shell; the course is in the bundle. New work adds **zero runtime dependencies** (confetti, sound, share image are small canvas/WebAudio code) so the single-file build stays small. Bump the SW cache name when shipping.

---

## 2. Ranked feature list (16)

Ranking = (learning impact + retention impact) / effort, with dependency order respected. Effort: S <= 1 day, M = 2-3 days, L = 4+ days.
Field definitions below are exact; the consolidated v2 type is in section 3. "Additive" fields can be added without a version bump (see 3.4).

### 1. Spaced repetition for everything (missed AND mastered) + "Due" queue - **M**
- **Principle:** spacing effect + retrieval practice (the two best-evidenced techniques in learning science). Missed-only review (today) fixes failures but lets mastered material decay, the exact thing that bites in interviews 3 weeks later.
- **Data (v2, in batch):** `cards: Record<QuestionId, Card>`
  ```ts
  interface Card { h: string;        // fnv1a-32 hex of prompt (+type); mismatch => card dropped
                   ease: number;     // 1.3..2.8, start 2.3
                   interval: number; // days, 0 = new, cap 120
                   due: Day;         // YYYY-MM-DD local
                   reps: number;     // consecutive successful reviews
                   lapses: number;   seen: number;  firstTry: number;  last: Day; }
  ```
  `missed[]` is **kept** (back-compat, existing tests, "Review mistakes" semantics) and still maintained by `recordResult`.
- **Scheduling (SM-2-lite, deterministic):** grade from the session: missed at any point -> `again` (due tomorrow, ease -0.2, lapses+1, reps 0; "right after seeing the feedback" is not recall); first-try correct -> `good` (reps0 ->1d, reps1 ->3d, else `round(interval*ease)`); `good` with confidence "certain" -> `easy` (x1.3, ease +0.1); `good` with confidence "guess" -> `hard` (interval x1.2, ease -0.15). **Early reviews do not inflate intervals** (card not yet due: only `seen/last` update; failures still apply), so replaying a lesson ten times cannot game the schedule. If an interview date is set, interval is capped at `floor(daysUntil/2)` (min 1) so every card gets >= 2 more looks before the date.
- **UI:** Home "Review" card becomes `Review due` with count (`due` + `missed` union, de-duplicated). `/review` = due queue first (most overdue ratio first, interleaved across units), then missed. Summary shows "Next review: tomorrow (5), in 3 days (4)" so the investment is visible. Lesson feedback chip: "You'll see this again in 3 days".
- **Edge cases:** content edited/removed (hash mismatch / unknown id -> card dropped on load via `pruneCards`); large backlog after a break (show "Today's 8 are ready", never "47 overdue"; ordering handles the rest; counts capped at "30+"); clock set backwards (`today < card.last`: treat as same day, never produce negative intervals); card `due` far in the future from clock skew (cap: if `due - today > MAX_INTERVAL` clamp on load); timezone travel (day keys are local calendar days, shift of +-1 day only changes `due` by a day; harmless).
- **Tests:** unit: `scheduleCard` table (again/hard/good/easy x reps 0/1/n), early-review no-op, cap 120, deadline cap, ease bounds 1.3..2.8, determinism, `dueQuestions` ordering + interleaving + prunes unknown/hash-mismatched ids. e2e: answer a lesson wrong then right -> clock `+1 day` -> Home shows `Review due` count = missed-in-lesson; review them -> count 0; state survives reload.

### 2. Daily goal + progress ring + per-day log + XP/activity chart - **M**
- **Principle:** goal-gradient + implementation of a small, pre-committed target (proximal goals); endowed progress (the ring is visibly part-full after the first answer, and the onboarding warm-up counts); self-monitoring.
- **Data (v2):** `goal: { target: number }` (questions/day; presets `light 8, regular 16, serious 24`, default 8 so one Daily Practice meets it), `days: Record<Day, DayLog>`
  ```ts
  interface DayLog { xp: number; answered: number; firstTry: number; target: number; } // target snapshot so history isn't rewritten when the goal changes
  ```
  Goal unit = **distinct questions resolved** (not XP: a lesson is ~90 XP, which makes XP goals lumpy; not minutes: needs timers). `days` pruned to the last 400 days (<30 KB).
- **UI:** Home header ring (SVG circle, `data-testid="goal-ring"`, aria `role="progressbar"` "5 of 8 questions today"); tap -> bottom sheet with 14-day bar chart (SVG, reuses unit colours), goal presets, "days practised: 41", "this week: 62 questions". Summary screen shows the ring animating from before to after.
- **Edge cases:** midnight rollover mid-session (day is taken at *finish* time via `dayKey(new Date())`, consistent with today; a session crossing midnight counts at finish); goal changed mid-day (applies to today and future, past keeps snapshot); first day back after >=7 idle days: "comeback goal" = half target for that one day (neutral copy "Easy restart: 4 today"); empty state: ring shows 0/8 with sentence "Eight questions is about five minutes"; DST/timezone: all keys are local calendar days; day arithmetic done in UTC (as `dayNumber` already does).
- **Tests:** unit: `recordDay` accumulates across two sessions in a day; target snapshot; prune at 400; `goalToday` fraction clamps to 1, `met` boundary; comeback rule. e2e: finish a lesson -> ring shows `N/8` complete; with `page.clock.setSystemTime` to next day ring resets to `0/8` and chart shows yesterday's bar.

### 3. Forgiving streak: earned freezes, rest days, "never-reset" counters - **M**
- **Principle:** habit formation needs continuity cues but punishment-driven streaks cause abandonment after the first break ("what-the-hell effect"). We keep the motivation of a streak and remove the cliff.
- **Rules:**
  1. A *practice day* = >= 1 resolved question (not necessarily goal met; the goal ring is a separate, softer target).
  2. Earn **1 freeze per 7-day streak milestone** (7, 14, 21...), hold at most `MAX_FREEZES = 2`. Free, automatic, never purchasable.
  3. A gap of missed days is *covered* by (a) the user's **rest days** (weekdays they chose, e.g. Sat+Sun) and then (b) freezes, one per remaining missed day. Freezes are consumed **only if they fully cover the gap**; if they can't, the streak resets to 1 and **no freeze is spent**.
  4. `longestStreak` and `stats.practiceDays` (lifetime days practised) never reset and are shown beside the streak, so a lapse is "fresh start", not "everything lost".
  5. Settlement is **lazy and idempotent**: nothing is written on app open; `streakStatus(p, today)` *predicts* the state for display and `advanceStreak` applies it on the next practice. (No timers, no background work, no double-spend on reload.)
- **Data (v2):** `streak`, `lastDay` (existing, same meaning), `longestStreak: number`, `freezes: number (0..2)`, `profile.restDays: number[]` (0=Sun..6=Sat), `stats.practiceDays: number`.
- **UI:** Home chip `🔥 12` plus `🧊 1`; tap -> sheet explaining the rules in two sentences. If a freeze will cover yesterday: chip subtext "🧊 covered yesterday". Lapsed state copy: "Fresh start. Longest: 23 days · 41 days practised". Settings: rest-day weekday toggles.
- **Edge cases:** clock moved backwards or `lastDay` in the future (gap <= 0 -> hold; next practice rewrites `lastDay = today`, self-heals); travelling across timezones (+-1 day jumps are absorbed by a freeze or rest day); DST (UTC day-number arithmetic); month/leap-year boundaries (already covered by `dayNumber`); migration: existing streaks carry over, `longestStreak = streak`; rest day that equals today (still counts normally if you practise); freeze earned at the same time the streak increments inside one `applySession` (grant after increment, cap 2).
- **Tests:** unit (table-driven): gap 0/1/2/3 x freezes 0/1/2 x rest-day masks; freeze not spent on failed cover; grant at 7/14 and cap at 2; idempotent `streakStatus`; backwards clock; leap-day Feb 28 -> Mar 1; year rollover. e2e: seed v2 blob with `streak 6, lastDay = today-2, freezes 1` (via `addInitScript`) -> Home shows 6 + "covered yesterday"; finish a lesson -> the freeze is consumed (streak 7), and reaching 7 earns one back, so the chip shows `🔥 7 🧊 1`.

### 4. Daily Practice: one-tap, interleaved, "Today" card - **M**
- **Principle:** implementation intentions / cue-action simplicity (one obvious default), interleaving (mixing topics improves discrimination and transfer, which is exactly what interviews test), desirable difficulty.
- **Composition (size 8):** up to 5 **due** cards (most overdue ratio, round-robin across units), 2 **weak-spot** questions (feature 6), the rest **fresh** (next unseen questions from the first incomplete lesson); backfill order due -> weak -> fresh -> random seen. Final order passes through `interleave()` so no two adjacent questions share a unit when avoidable. Seed = day number, so a reload gives the same session.
- **Data:** none required (derived). Optional additive: `stats.dailySessions: number`.
- **UI:** Home hero card replaces the "Mixed practice" position: "Today · 8 questions · ~5 min · 5 due, 2 weak spots, 1 new" with a single primary button. `Mixed practice` and `Review` stay as secondary rows. New route `#/daily`. If no cards exist yet (new user) the hero is "Start your first lesson" (no fake "due" items).
- **Edge cases:** pool smaller than 8 (shrink, min 3); everything due already done today -> hero becomes "Bonus round" (fresh/weak only, no XP penalty); content < size; user opens `#/daily` twice -> same set (deterministic), finishing it twice is allowed; every resolved question counts toward the ring (simple rule), and the SRS no-inflate rule protects the schedule from over-practising.
- **Tests:** unit: composition counts for (0 due), (3 due), (50 due); interleave property: adjacent same-unit count minimal; determinism for equal inputs; never duplicates; respects size. e2e: seed 12 due cards -> `#/daily` shows 8 questions mixed from >= 3 units (assert unit chips) and finishing updates the ring.

### 5. Topic mastery map + Interview Readiness score - **M**
- **Principle:** mastery learning + accurate self-assessment (calibrated feedback on progress is itself motivating; users stop guessing what to study).
- **Definitions (pure, in `mastery.ts`):** `retention(card, today) = card.due >= today ? 1 : 0.5 ** ((today-due)/max(1,interval))`; `strength = min(1, interval/21) * retention` (unseen = 0; `MASTERED_INTERVAL = 21`). Unit mastery = mean strength over **all** questions of the unit (unseen count as 0, so the score cannot be inflated by only doing easy lessons). Readiness = weighted mean over units (weights from role profile, default 1) x 100, with `coverage = seen/total` and a label: `<30% coverage -> "Early estimate"`, `<60 -> "Building"`, `<80 -> "Solid"`, else `"Interview-ready"`.
- **Honesty rules:** copy says "an estimate of how much of *this course* you'd recall today", never "chance of passing"; show coverage next to the score.
- **Data:** none (derived). Milestones `unit:<id>:80` stored in `milestones` (feature 8/10).
- **UI:** new "Progress" tab/sheet from Home: readiness number + label, 8 horizontal bars (unit colour), each bar tappable -> "Practise <topic>" (weak-spot session limited to that unit). Home shows a compact `Readiness 42 · Early estimate` line.
- **Edge cases:** empty (all zeros, label "Early estimate", no scary red); unit with 0 questions (excluded from the mean); content growth lowers the score slightly (explain via coverage; acceptable and honest); backlog: overdue decay is capped at `retention >= 0.1`.
- **Tests:** unit: strength curve at boundaries; unseen = 0; mean/weights; label thresholds; monotonic (answering good never lowers a unit's strength on that day); role weights normalise. e2e: seed cards -> Progress sheet shows expected score within +-1 (snapshot via `page.clock` fixed time).

### 6. Weak-spot targeting - **S**
- **Principle:** deliberate practice (spend time where performance is weakest); error-focused feedback.
- **Logic:** `weakSpots(course, p, today, n)`: score each question `w = (1 - strength) + 0.3 * min(lapses,3)/3 + (missed? 0.3:0)`; take the weakest unit (lowest unit strength among units with >= 1 seen question) and return top-n by `w` (ties: seeded). Mixed practice stops being uniform-random: `pickMix` samples with weight `1 + 2*(1 - strength)`.
- **Data:** none.
- **UI:** "Your weakest topic: Scale & resilience (31%) - Practise" row in the Progress sheet and inside the Today card; Mixed practice subtitle "weighted toward your weak spots".
- **Edge cases:** no seen cards -> falls back to uniform; single unit seen; all strengths equal.
- **Tests:** unit: weakest unit chosen; weights bias distribution over 1000 seeded draws (statistical bound with fixed seed); fallback uniform. e2e: covered via Daily Practice spec.

### 7. Onboarding: target role, interview date, countdown plan, goal, "when will you practise?" - **M**
- **Principle:** implementation intentions ("When X, I will Y" doubles follow-through); commitment/consistency; deadline-driven goal gradient.
- **Data (v2):** `profile: { onboarded: boolean; role: RoleId | null; interviewDate: Day | null; restDays: number[]; practiceTime: string | null /* 'HH:mm' */ }`, `RoleId = 'backend-kotlin' | 'staff-architect' | 'generalist'` (each maps unit id -> weight, e.g. staff-architect weights `ddd, scale, ai` x1.5).
- **Flow (non-blocking!):** an inline card on Home when `!profile.onboarded` (never a route, never a modal, so deep links and e2e still work): 4 taps: role, date (optional, native `<input type=date>`), daily goal preset, usual time + (later, feature 13) calendar. A 3-question warm-up counts toward day one (the ring starts at 3/8, disclosed: "your warm-up counted"). Dismissing marks `onboarded` with defaults; Settings can reopen it.
- **Plan (`planFor`):** `questionsRemaining = unseen + due-now`; `daysLeft = daysBetween(today, interviewDate)`; `perDay = ceil(questionsRemaining / max(1, daysLeft - 2))`; `onTrack = goal.target >= perDay`. Home countdown chip "23 days to interview" and a calm line: "At 16/day you'll have seen everything with 4 days to spare" or "To see everything by then: ~22/day, or focus on weak spots". Recomputes silently; no alarm colours.
- **Edge cases:** date in the past -> treated as unset with neutral "Set a new date?"; date today/tomorrow -> "Final review mode": plan switches to due + weak spots only and the SRS deadline cap kicks in; invalid/typed garbage (`isDay` validation); changing date later reschedules nothing (the cap is evaluated at scheduling time only); no date -> no countdown shown at all (no nagging to set one).
- **Tests:** unit: `planFor` table (no date, past, tomorrow, 90 days, everything seen); `isDay` rejects `2026-02-30`; role weights. e2e: new profile sees the card; completing it persists and shows the chip; **a direct `#/lesson/...` visit with no onboarding still works** (guards the existing suite).

### 8. Celebration moments + milestone cards (confetti / haptics / optional sound) - **S/M**
- **Principle:** immediate reinforcement + peak-end rule (end sessions on a high); *variable reward done benignly* (varied copy, an occasional bonus/tip). Scaled to milestone importance to avoid habituation.
- **Tiers:** micro (correct answer: 10 ms haptic tick, optional soft tone); session end (perfect lesson or goal met: confetti burst ~1.2 s); milestone (rank-up, streak 7/30/100, first mastered unit, goal-met-first-time, readiness label change: a full-width "milestone card" on the Summary above the tiles, with a stat and a one-line meaning, never replacing the `h1`). Maximum **one** large celebration per session (priority: rank-up > streak > unit mastery > goal > perfect).
- **Benign variable reward:** "Interviewer's tip": with probability 1/6 per finished session (deterministic: `fnv(day + sessionsTotal) % 6 === 0`, so reload doesn't re-roll) the Summary shows a short insight (the explanation of a question the user has mastered, reworded as "Worth remembering") and +5 XP. Odds are printed in Settings; toggle to disable. It never gates anything.
- **Data (v2):** `settings: { haptics: boolean (true); sound: boolean (false); confetti: boolean (true); tips: boolean (true); confidence: boolean (false) }`, `milestones: Record<string, Day>` (dedupes milestone cards: `'rank:3'`, `'streak:7'`, `'unit:ddd:80'`, `'goal:first'`), `stats: { sessions, answered, firstTry, perfect, practiceDays }`.
- **Implementation:** canvas confetti ~60 lines, `pointer-events:none`, aria-hidden, skipped when `matchMedia('(prefers-reduced-motion: reduce)')` or `settings.confetti=false` (then a static badge pulse); `navigator.vibrate?.(10)` guarded; WebAudio oscillator (no audio assets) created only after a user gesture.
- **Weekly recap card (Mondays, local):** "Last week: 54 questions, 4 days, readiness 38 -> 45": rendered from `days` + a stored `lastRecapWeek: Day` (additive). Shown once, dismissable.
- **Edge cases:** reduced motion; vibrate unsupported (iOS Safari): silently no-op; autoplay policy; very fast consecutive sessions (rate-limit large celebration to 1 per 2 minutes); celebration firing twice on re-render (guard via `milestones` write + `useRef`).
- **Tests:** unit: `pickCelebration(events)` priority and "max one"; tip determinism; odds exactly 1/6 over `0..5999` seeds; milestone dedupe. e2e: finishing a perfect lesson shows `[data-testid=celebration]` when motion allowed; under `reducedMotion: 'reduce'` no `canvas` is mounted and `.summary h1` is unchanged.

### 9. Ranks with senior-engineer titles - **S**
- **Principle:** identity-based motivation ("I'm becoming a Staff engineer"), clear next-step goal gradient; levels are cumulative so they never go down (no loss).
- **Ladder (`RANKS`, XP floors):** Associate 0, Engineer 150, Senior 500, Senior II 1000, Staff 2000, Senior Staff 3500, Principal 5500, Distinguished 8500, Fellow 12000. Pure `rankFor(xp)` returns `{index,title,floor,next,fraction}`.
- **Data:** none (derived from `xp`; rank-up detected by comparing `rankFor(before)` and `rankFor(after)` inside `applySession`).
- **UI:** title under the app name on Home with a thin progress bar to the next rank; rank-up milestone card.
- **Edge cases:** XP can only increase (reset clears all); migration from v1 immediately shows the correct rank without firing rank-up celebrations (migration seeds `milestones['rank:N']` for the current rank and below).
- **Tests:** unit: boundaries at each floor +-1; fraction at max rank = 1, `next = null`. e2e: seed `xp: 499` then finish a lesson -> rank-up card "Senior".

### 10. Achievements / badges - **M**
- **Principle:** goal setting with explicit sub-goals; collection/completion motive; recognition. Chosen to reward *learning behaviours*, not time spent.
- **Set (~20, pure predicates over Progress + course, stored in `milestones`):** First lesson; All 9 question types seen; 3 perfect lessons; 10-question first-try run; Comeback (practised after >= 7 idle days; reframes a lapse positively); Calibrated (>= 80% accuracy when "certain" over 30 ratings); Unit mastered (>= 80%) per unit x8; 7/30/100 practice days (lifetime, not consecutive); First mock interview; First teach-back; "Resilience architect" (scale unit >= 80); 1000 questions answered.
- **Data:** `milestones` (already in batch); badge definitions live in code (`ACHIEVEMENTS: {id,title,emoji,test:(p,ctx)=>boolean}[]`), so adding one needs no migration.
- **UI:** "Badges" grid in the Progress sheet (locked ones shown dimmed with hint), unlock toast on Summary merged into the milestone slot (max one big, others as small list).
- **Edge cases:** retroactive unlock on migration (computed once silently, no 20-toast avalanche); badge for time-of-day or "streak of N consecutive days only" is intentionally excluded (would reintroduce streak anxiety).
- **Tests:** unit: each predicate positive/negative with fixtures; `evaluateAchievements` idempotent (second run returns []); migration silent unlock. e2e: complete a perfect lesson -> "First perfect" badge appears in Progress sheet.

### 11. Confidence ratings + calibration - **M**
- **Principle:** metacognition; the **hypercorrection effect** (errors made with high confidence are corrected best when surfaced); detects "lucky guesses" that shouldn't earn long intervals.
- **Flow:** optional 3-chip row above Check on `single/multi/estimate/fill/order/match/spot` ("Guess / Think so / Certain"), skippable (tap Check without choosing = neutral). Off by default; Daily Practice and Mock mode prompt "Turn on confidence ratings?" once after 3 sessions. Not on `open`/`code` (the grader is the signal).
- **Effects:** certain+wrong -> card `lapses+1` and **priority flag** (due earlier in the queue; feedback copy "Worth a closer look: you were sure"); guess+correct -> grade `hard`; certain+correct -> grade `easy`.
- **Data:** `LessonResult.confidence?: Record<QuestionId, 1|2|3>` (input only); persisted aggregate `calibration: { n: number[3]; right: number[3] }` (counts per confidence level; additive).
- **UI:** Progress sheet: "When you said Certain you were right 86% of the time (n=44)". Needs `Session` to carry `confidence` (new optional map, `submit(s, correct, conf?)` default undefined so current tests are untouched).
- **Edge cases:** too few ratings (n < 10 -> "not enough data yet"); setting turned off mid-lesson (read at question start); double tap; keyboard Enter semantics: chips are `<button>`s so existing Enter handling (`HTMLButtonElement` guard in Lesson.tsx) still applies.
- **Tests:** unit: grade mapping matrix; calibration counts; `submit` backward compatible. e2e: enable setting, pick Certain, answer wrong -> feedback contains the closer-look note; card due tomorrow.

### 12. Daily challenge (deterministic by date) - **S**
- **Principle:** a single shared, low-stakes daily ritual (Wordle-style cue) without needing a server: the same question for everyone on a given local date, because the pick is `fnv1a(day) % pool.length`. Users can compare notes in their own chats; we never fake other users.
- **Data (additive):** `challenge: Record<Day, 'pass' | 'fail'>` (pruned to 60 days), counts as a practice day.
- **UI:** a slim card under the Today hero: "Today's question · Estimate". One question, no timer, +15 XP on pass. After completion shows a small "Share result" (text: `Interview Trainer 2026-11-04 ✓`, no spoilers). Pool = all question types except `match`, weighted to `open/code/estimate/spot` for senior-level depth.
- **Edge cases:** date shared across timezones is local, not global (documented, "same question per calendar date"); content changes shift the mapping (acceptable); already done today -> shows result, no retry XP.
- **Tests:** unit: determinism, distribution over 365 days covers all types, same-day stability. e2e: `page.clock` fixed date -> same question text on reload; finishing marks done.

### 13. Reminders the user chooses (calendar file first, notifications best-effort) - **M**
- **Principle:** implementation intention + external cue. Honest about platform limits: no server means no push. Reliable = a recurring calendar event; best-effort = Notification API.
- **Primary:** "Add daily reminder to calendar" generates an `.ics` (RRULE:FREQ=DAILY at `profile.practiceTime`, title "Interview Trainer · 5 minutes", URL = current location) as a Blob download. Works offline on every platform.
- **Secondary:** `Notification.requestPermission()` only from an explicit button after >= 3 practice days; `serviceWorker.showNotification` when the page is open/backgrounded and a due-count crossing is detected on `visibilitychange`, plus Periodic Background Sync where supported (Chrome installed PWA). SW cannot read `localStorage`, so mirror `{due, ready}` into IndexedDB on each save. At most one per day, user-set quiet hours, copy limited to neutral text ("5 questions are ready to review").
- **Data (additive):** `profile.practiceTime`, `settings.notifications: boolean`, `notify: { lastDay: Day | null }`.
- **Edge cases:** permission denied/blocked (hide button, show calendar option only); iOS requires installed PWA (explain, don't nag); clock/timezone (calendar uses floating local time); user disables -> `unregister periodicSync`.
- **Tests:** unit: `.ics` text for fixed inputs (escape, CRLF, DTSTART floating); `shouldNotify` (once/day, quiet hours, due>0). e2e: download event contains `BEGIN:VEVENT`; notification path mocked with `context.grantPermissions(['notifications'])`.

### 14. Timed mock interview mode - **L**
- **Principle:** transfer-appropriate practice and the testing effect under realistic conditions; exposure reduces anxiety. Strictly opt-in.
- **Format:** 3 presets (15 / 30 / 45 min), N questions balanced per role weights, **per-question soft timer** shown, no feedback until the end (like an interview), `open` questions use the grader (offline: self-check at the end), code questions keep the worker timeout. A mock never changes the streak or XP negatively; XP = 5 per question attempted.
- **Report:** per-topic accuracy, time per question vs target, list "revisit these" (feeds cards as `again`), and "one habit to change" (e.g. fastest wrong answers => slow down).
- **Data (additive):** `mocks: MockRecord[]` (max 20) `{ day, minutes, answered, firstTry, byUnit: Record<string,[ok,n]>, secondsPerQ: number }`.
- **Edge cases:** backgrounded tab (timer based on timestamps, pauses on `visibilitychange` with a visible "paused"); quit mid-run (discard, no penalty); timer vs reduced-motion (no ticking animation); a11y (time announced politely every 5 min only).
- **Tests:** unit: report aggregation; timer maths with injected clock; pause/resume. e2e: 15-min preset with `page.clock` fast-forward expires remaining questions as skipped and shows the report.

### 15. Explain-your-answer / teach-back - **M**
- **Principle:** self-explanation and the protégé effect (explaining to someone else exposes gaps); generation effect.
- **Flow:** after a *correct first-try* answer on conceptual `single/multi` questions the user rated "Guess" (or every Nth session if confidence is off), an optional prompt "Explain why in one sentence (or skip)". Reuses the open-question grader with the question's `explanation` as model answer and auto-derived key points; offline = reveal the explanation and self-rate "Matched / Partly / Missed". A "Teach it back" mini-card at the end of a **mastered unit** asks the learner to explain the topic to a junior colleague in 3 sentences (open question, self-rated offline).
- **Data (additive):** `teachBacks: Record<QuestionId, { day: Day; rating: 0 | 1 | 2 }>` (max 200), a 'Missed' rating downgrades the card to `hard`.
- **Edge cases:** skipping must be one tap and costs nothing; grader offline; text length cap; unsaved draft lost on navigation (acceptable).
- **Tests:** unit: rating->grade mapping, prune. e2e: guess + correct -> prompt appears, skip works, self-rate path saves.

### 16. Shareable progress card (image) - **M**
- **Principle:** identity expression and commitment device (voluntary, user-initiated, never automatic); no fake social proof.
- **Implementation:** render a 1080x1350 canvas (rank, readiness, practice days, best streak, unit bars; **no** personal data, no name, user chooses which stats via checkboxes) -> `canvas.toBlob` -> `navigator.share({files})` if `canShare`, else download PNG. Works in the single-file build (no network).
- **Data:** none (additive `settings.shareStats?: string[]`).
- **Edge cases:** `canShare` false on desktop -> download; font loading (use system font stack, wait `document.fonts.ready`); dark mode irrelevant (card has its own palette); readiness "Early estimate" label baked in (no overselling).
- **Tests:** unit: `shareCardModel(p, course, today)` pure model (what's drawn); e2e: click share with `navigator.share` stubbed returns a PNG blob of the expected size.

---

## 3. RECOMMENDED BATCH (build in this order)

Nine features, one engineer, ~2.5 weeks. Everything extends the existing `recordResult` write path, so each step ships independently behind green tests.

| # | Step | Needs | Effort |
|---|---|---|---|
| 0 | Foundation: `Progress` v2, `migrateProgress`, `day.ts`, store v2 + export/import + multi-tab sync | none | S |
| 1 | Spaced repetition cards + Review due queue (feature 1) | 0 | M |
| 2 | Daily goal ring + day log + 14-day chart (feature 2) | 0 | M |
| 3 | Forgiving streak (freezes, rest days) (feature 3) | 0, 2 (`days`, `stats`) | M |
| 4 | Mastery map + Interview Readiness (feature 5) | 1 | M |
| 5 | Weak-spot targeting (feature 6) | 1, 4 | S |
| 6 | Daily Practice "Today" card (feature 4) | 1, 5 | M |
| 7 | Ranks (feature 9) | 0 | S |
| 8 | Celebrations + milestone cards + benign tip (feature 8) | 2, 3, 4, 7 (consumes events) | M |
| 9 | Onboarding-lite: role, interview date, goal, rest days, countdown (feature 7) | 2, 3, 4 | M |

Next batch (not now): achievements (10), daily challenge (12), confidence (11), reminders (13); later: mock interview (14), teach-back (15), share card (16).

Why this set: steps 1+4+5+6 are the *learning* payload (spacing, interleaving, weak-spot targeting, honest mastery). 2+3+7+8 are the *habit* payload (small daily target, forgiving continuity, identity, rewards). 9 turns on the countdown/commitment device. All derive from one `applySession` so they stay consistent.

### 3.1 `Progress` v2 (exact)

```ts
// shared/src/progress.ts
export type Day = string;                 // 'YYYY-MM-DD', local calendar day
export type QuestionId = string;          // '<lessonId>/<n>'
export type Confidence = 1 | 2 | 3;       // guess | think so | certain (used from the next batch on)
export type RoleId = 'backend-kotlin' | 'staff-architect' | 'generalist';

export interface LessonRecord { bestAccuracy: number; completions: number; lastDay: Day }   // unchanged

export interface Card {
  h: string;          // fnv1a-32 hex of `${q.type}|${q.prompt}`; mismatch => dropped
  ease: number;       // 1.3 .. 2.8
  interval: number;   // days; 0 = learning
  due: Day;
  reps: number;
  lapses: number;
  seen: number;
  firstTry: number;
  last: Day;
}

export interface DayLog { xp: number; answered: number; firstTry: number; target: number }

export interface Progress {
  version: 2;
  // --- v1 fields, same meaning ---
  xp: number;
  streak: number;                         // current consecutive "kept" days (practised / rest / freeze-covered)
  lastDay: Day | null;                    // last practice day
  lessons: Record<string, LessonRecord>;
  missed: QuestionId[];
  // --- v2 ---
  longestStreak: number;
  freezes: number;                        // 0..MAX_FREEZES (2)
  cards: Record<QuestionId, Card>;
  days: Record<Day, DayLog>;              // pruned to last 400 days
  goal: { target: number };               // questions per day: 8 | 16 | 24 (any 1..100 accepted)
  profile: {
    onboarded: boolean;
    role: RoleId | null;
    interviewDate: Day | null;
    restDays: number[];                   // 0=Sun..6=Sat
    practiceTime: string | null;          // 'HH:mm' (used by reminders later)
  };
  settings: { haptics: boolean; sound: boolean; confetti: boolean; tips: boolean; confidence: boolean };
  stats: { sessions: number; answered: number; firstTry: number; perfect: number; practiceDays: number };
  milestones: Record<string, Day>;        // 'rank:3', 'streak:7', 'unit:ddd:80', 'goal:first', later: badge ids
}

export const MAX_FREEZES = 2;
export const FREEZE_EVERY = 7;
export const MAX_INTERVAL = 120;
export const MASTERED_INTERVAL = 21;
export const DAYS_KEPT = 400;

export const emptyProgress(): Progress;   // all defaults: goal 8, settings {haptics:true,sound:false,confetti:true,tips:true,confidence:false}, restDays []
```

### 3.2 Migration

```ts
export interface MigrationContext {
  today: Day;
  questions: readonly Question[];         // allQuestions(course): for seeding cards + hashes
}

/** Accepts anything found in storage (v1, v2, garbage). Never throws. Pure. */
export function migrateProgress(raw: unknown, ctx: MigrationContext): { progress: Progress; from: 0 | 1 | 2; changed: boolean };
/** Fills missing additive fields and clamps/validates v2 (bad Day strings, freezes>2, negative xp, interval>120). */
export function normalizeProgress(p: Partial<Progress>, ctx: MigrationContext): Progress;
/** Drops cards whose id is gone or whose hash no longer matches; trims days/milestones. */
export function pruneProgress(p: Progress, ctx: MigrationContext): Progress;
```

v1 -> v2 rules:
1. Copy `xp, streak, lastDay, lessons, missed` verbatim (validate types; `isDay` on days). `longestStreak = streak`. `freezes = 1` (a one-time welcome gift, announced once in a Summary card, key `milestones['gift:freeze']`).
2. **Seed cards so the due queue is useful on day one, without an avalanche:** for every question whose lesson has `completions > 0`: if id is in `missed` -> `{ease 2.1, interval 1, due today, lapses 1, reps 0}`; else `{ease 2.3, interval 3, reps 1, due today + 1 + (fnv(id) % 7)}` so reviews spread over the next week. `h` from the current prompt, `seen: 1`.
3. `days = {}`, `stats.practiceDays = lastDay ? streak : 0` (lower bound, honest), `stats.sessions = sum(completions)`, `goal.target = 8`, `profile.onboarded = false`, everything else default.
4. `milestones`: seed `rank:<current index>` and all lower so migration never celebrates retroactively.
5. Unknown `version > 2` or unparsable: return `emptyProgress()` with `changed:false` and **the store must not overwrite the stored value**; before first write it copies the raw string to `interview-trainer.progress.backup-<yyyymmdd>`.

Storage (`web/src/lib/store.ts`):
- New key `interview-trainer.progress.v2`. Load order: v2 -> else migrate v1 and write v2 -> else empty. **Never delete v1** (rollback safe for one release; remove in a later one).
- `window.addEventListener('storage', ...)` to reload when another tab/PWA window writes (currently two tabs overwrite each other).
- `navigator.storage?.persist?.()` once after the first completed session (Safari/ITP evicts script-writable storage of un-installed sites after ~7 idle days: this is the single biggest threat to a local-only streak). Home shows a one-time, dismissible "Add to Home Screen to keep your progress safe on iPhone" tip only on iOS Safari not in standalone mode.
- `exportProgress(): string` / `importProgress(json): Progress | Error` (runs `normalizeProgress`, confirms before replacing). Buttons next to "Reset progress".
- Existing `Reset progress` clears v2 (and nothing else).

### 3.3 New pure functions in `shared/` (signatures)

```ts
// shared/src/day.ts  (move dayKey/dayNumber here; re-export from progress.ts so imports don't break)
export const isDay: (x: unknown) => x is Day;                         // regex + real calendar date (rejects 2026-02-30)
export function dayKey(d: Date): Day;                                 // local
export function dayNumber(k: Day): number;                            // UTC-based, DST-safe
export function addDays(k: Day, n: number): Day;
export function daysBetween(from: Day, to: Day): number;              // to - from (can be negative)
export function weekday(k: Day): number;                              // 0=Sun..6=Sat, UTC-based

// shared/src/hash.ts
export function fnv1a(s: string): number;                             // 32-bit, unsigned
export const questionHash: (q: Question) => string;                   // fnv1a(`${q.type}|${q.prompt}`).toString(16)

// shared/src/srs.ts
export type Grade = 'again' | 'hard' | 'good' | 'easy';
export type Outcome = 'first' | 'retry' | 'failed';                   // first-try right | right after retry | retries exhausted
export function outcomesOf(s: SessionSummary): Record<QuestionId, Outcome>;   // first = correct \ missed; retry = recovered; failed = missed \ recovered
export function gradeFor(o: Outcome, c?: Confidence): Grade;          // retry|failed -> again; first: 3->easy, 1->hard, else good
export function scheduleCard(card: Card | undefined, grade: Grade, today: Day, opts: { hash: string; deadline?: Day | null }): Card;
export function retention(card: Card, today: Day): number;            // 0.1..1
export function strength(card: Card | undefined, today: Day): number; // 0..1, unseen = 0
export function isDue(card: Card, today: Day): boolean;               // due <= today
export function dueQuestions(p: Progress, questions: readonly Question[], today: Day): Question[]; // most overdue first, interleaved by unit
export function applyToCards(cards: Progress['cards'], outcomes: Record<QuestionId, Outcome>, questions: readonly Question[], today: Day, opts: { deadline?: Day | null; confidence?: Record<QuestionId, Confidence> }): Progress['cards'];

// shared/src/mastery.ts
export interface TopicStat { unitId: string; title: string; strength: number; seen: number; total: number; due: number; mastered: number }
export function topicStats(course: Course, p: Progress, today: Day): TopicStat[];
export interface Readiness { score: number /*0-100*/; coverage: number /*0-1*/; label: 'early' | 'building' | 'solid' | 'ready'; weakestUnitId: string | null }
export function readiness(course: Course, p: Progress, today: Day, weights?: Record<string, number>): Readiness;
export const ROLE_WEIGHTS: Record<RoleId, Record<string, number>>;    // keyed by unit id: kotlin, coroutines, spring, jvm, ddd, scale, complexity, ai
export function weakSpots(course: Course, p: Progress, today: Day, n: number, seed?: number): Question[];
export function pickMix(course: Course, p: Progress, today: Day, n: number, seed: number): Question[];  // weighted by 1 + 2*(1 - strength)

// shared/src/daily.ts
export interface DailyPlan { questions: Question[]; mix: { due: number; weak: number; fresh: number; filler: number }; minutes: number }
export function buildDailySession(course: Course, p: Progress, today: Day, opts?: { size?: number }): DailyPlan;  // default size 8, min 3
export function interleave<T>(items: readonly T[], keyOf: (t: T) => string, seed?: number): T[];

// shared/src/streak.ts
export type StreakState = 'new' | 'done-today' | 'open' | 'saved-by-freeze' | 'lapsed';
export interface StreakStatus { streak: number; state: StreakState; freezesNeeded: number; restDaysCovered: number }
export function streakStatus(p: Progress, today: Day): StreakStatus;          // pure prediction, replaces liveStreak (keep liveStreak as a wrapper returning .streak)
export function advanceStreak(p: Progress, today: Day): Pick<Progress, 'streak' | 'longestStreak' | 'freezes' | 'lastDay'> & { freezeUsed: number; freezeEarned: boolean };

// shared/src/goal.ts
export const GOAL_PRESETS: { light: 8; regular: 16; serious: 24 };
export function goalToday(p: Progress, today: Day): { target: number; done: number; fraction: number; met: boolean; comeback: boolean };
export function lastDays(p: Progress, today: Day, n?: number): { day: Day; xp: number; answered: number; met: boolean }[];  // oldest first, zero-filled

// shared/src/rank.ts
export const RANKS: readonly { title: string; xp: number }[];
export function rankFor(xp: number): { index: number; title: string; floor: number; next: number | null; fraction: number };

// shared/src/plan.ts
export function planFor(course: Course, p: Progress, today: Day): { daysLeft: number | null; remaining: number; perDay: number | null; onTrack: boolean | null; mode: 'none' | 'steady' | 'final-review' };

// shared/src/events.ts
export type SessionEvent =
  | { type: 'goal-met' } | { type: 'streak'; value: number; milestone: boolean }
  | { type: 'freeze-earned' } | { type: 'freeze-used'; count: number }
  | { type: 'rank-up'; title: string } | { type: 'unit-mastered'; unitId: string }
  | { type: 'readiness'; label: Readiness['label'] } | { type: 'tip'; questionId: QuestionId; bonusXp: number };
export function applySession(
  p: Progress,
  r: LessonResult & { outcomes?: Record<QuestionId, Outcome>; confidence?: Record<QuestionId, Confidence>; answered?: number },
  ctx: { today: Day; course: Course },
): { progress: Progress; events: SessionEvent[] };
export function pickCelebration(events: readonly SessionEvent[]): SessionEvent | null;  // priority rank-up > streak > unit-mastered > goal-met > readiness > tip
export function tipFor(p: Progress, today: Day): { show: boolean };                     // fnv(today + sessions) % 6 === 0 && settings.tips
```

`recordResult(p, r, today)` keeps its **current signature and behaviour** (v1 unit tests stay green); internally it becomes a thin wrapper:
`applySession` calls the old logic for `lessons/xp/missed`, then streak, day log, cards, stats, milestones, events. `App.finish` switches to `applySession` and passes `events` to `Summary` (`Finished.events`). `Summary` keeps its `h1`, tiles and buttons; milestone/celebration/next-review/ring are *added* sections.

`LessonResult` gains optional fields only: `outcomes`, `confidence`, `answered` (default `Object.keys(outcomes).length`). `outcomesOf(summary)` is used by `App.finish` so `session.ts` does not need to change in this batch.

### 3.4 Versioning policy
Bump `version` only for non-additive changes. Later additive fields (`challenge`, `mocks`, `teachBacks`, `calibration`, `notify`, `lastRecapWeek`) are optional in the type and get defaults in `normalizeProgress`; no migration function needed.

### 3.5 UI wiring in the batch
- `Home.tsx`: header = title + rank (bar) + streak chip (`data-testid=streak-chip`) + goal ring (`goal-ring`); hero Today card (`daily-card`); rows: Review due (`due-card`), Mixed practice (weighted), Progress sheet (readiness + topic bars + 14-day chart); countdown chip when `interviewDate`. Onboarding card inline at top while `!onboarded`.
- `App.tsx`: routes `#/daily`, `#/progress`; `Practice` uses `dueQuestions`/`pickMix` (seeded by `dayNumber` + a salt) instead of `Math.random()`.
- `Summary.tsx`: add ring-before/after, milestone card, "next review" line, tip.
- `web/public/sw.js`: bump cache to `interview-trainer-v2`.
- Mobile: all new UI is single-column, 44 px targets, bottom sheets instead of new pages, no horizontal overflow (existing `expectNoHorizontalOverflow` helper to be called on Home and Progress sheet).

### 3.6 Test plan for the batch
Unit (vitest, in `shared/src/*.test.ts`, pure and time-injected, no `Date.now()` in shared):
- `day.test.ts`: leap day, year rollover, DST weeks (Mar/Nov in a DST zone via fixed day keys), `isDay` rejects.
- `srs.test.ts`, `streak.test.ts`, `goal.test.ts`, `mastery.test.ts`, `daily.test.ts`, `rank.test.ts`, `plan.test.ts`, `events.test.ts` as described per feature.
- `migrate.test.ts`: v1 fixture (real shape from the current store), v1 with unknown question ids, garbage strings/`null`/arrays, version 3, v2 with bad fields; no throw; cards spread across 7 days; no retroactive celebrations; idempotent (`migrate(migrate(x))` equals `migrate(x)`).
- Regression: the existing `logic.test.ts` "streaks & progress" cases continue to pass unmodified (`nextStreak`, `liveStreak`, `recordResult`). Add a test asserting `recordResult`'s old outputs equal `applySession`'s for the same input on the v1-visible fields.
- Property-style: for 500 random sessions on a seeded RNG, invariants hold (xp monotonic, `0 <= freezes <= 2`, `longestStreak >= streak`, intervals `<= 120`, `ease` bounds, due >= today).
- A content test: `dailyChallenge`/`buildDailySession` against the real course (each unit reachable, no duplicate ids).
E2E (Playwright, new `e2e/engagement.spec.ts`; use `page.clock.install({ time })`, `context.addInitScript` to seed storage; config adds `reducedMotion: 'reduce'` globally):
1. v1 blob in `localStorage` -> Home shows same xp/streak, no console errors, v1 key untouched, v2 key written.
2. Fresh profile -> deep link to a lesson still works (guards the old suite); finishing shows `Lesson complete!`, ring `n/8`, `Review due` tomorrow after `clock.setSystemTime(+1 day)`.
3. Streak freeze: seeded gap of 2 days with 1 freeze -> streak preserved and message; gap of 3 with 1 freeze -> resets to 1 and freeze kept.
4. Daily Practice: seeded due cards -> 8 mixed questions, finishing drains the due count and moves the ring.
5. Onboarding card: set date 10 days ahead -> countdown chip; dismiss -> never reappears after reload.
6. Export then Import round-trip restores state; importing garbage shows an error and changes nothing.
7. Two tabs: write in tab A, tab B updates (storage event).
8. Overflow: `expectNoHorizontalOverflow` on Home, Progress sheet at 390x844.
Keep screenshots for Home/Progress/Summary under `e2e/screenshots/` like the existing suite.

### 3.7 Definition of done for the batch
`npm test`, `npm run typecheck`, `npm run e2e` green; `build:standalone` output still a single file with no new network requests; Lighthouse PWA unchanged offline; v1 -> v2 upgrade verified manually from a real v1 profile; copy reviewed against the table in section 0.

---

## 4. What we will NOT do (and why)

1. **Leaderboards, leagues, friends, "X people practised today", public profiles**: need a server, and faking them is deception. The deterministic daily challenge gives a shared ritual without pretending anyone is there.
2. **Hearts/lives or any mechanic that blocks practising**: punishes the learners who need practice most; zero learning benefit.
3. **Paid or ad-gated streak repair, paid freezes, "limited-time" offers, countdown-to-midnight timers, "your streak is in danger" banners, sad-mascot guilt, red badge counters**: manipulative loss framing and fake urgency. Freezes are earned only; copy is factual.
4. **Notification spam**: no server push means no push; reminders are opt-in, max 1/day, neutral text, quiet hours, and the calendar `.ics` is the reliable path.
5. **Loot-box variance**: the only variable reward is a small additive bonus/tip with disclosed odds, deterministic per day, with a toggle. Nothing is gated behind it.
6. **Pretending readiness predicts hiring**: the score is "how much of this course you'd recall today" with coverage shown; no "% chance to pass".
7. **Accounts, cloud sync, analytics, telemetry**: out of scope (local-only). Mitigated with Export/Import, `storage.persist()` and the iOS Add-to-Home-Screen tip.
8. **XP penalties, decay of XP/rank, or streak-only badges**: XP and ranks only go up; badges reward learning, not unbroken attendance.
9. **Blocking onboarding or paywall-style modals**: breaks deep links, the existing e2e suite, and trust. Onboarding is an inline, skippable card.
10. **New heavy dependencies** (confetti/chart libraries, date libraries): bundle size and the single-file build matter; everything here is small canvas/SVG/pure TS.
