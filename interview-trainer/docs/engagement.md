# Engagement: how the habit loop is built, and where we draw the line

The loop: **a cue** (the Today card, a countdown the learner chose) -> **a five-minute action** (8 interleaved questions) ->
**a benign reward** (the ring fills, the rank bar moves, a milestone is named once) -> **investment** (cards scheduled, topics
mastered, days practised, all exportable) -> **the next cue** ("N due").

The engine is the due queue, not the confetti: spaced retrieval is the best-evidenced way to keep material from decaying
before an interview. Gamification is the wrapper.

## Where the code lives

| Concern | File (`shared/src/`) | Notes |
|---|---|---|
| Data model, constants | `progress.ts` | `Progress` v2, `Card`, `DayLog`, limits |
| Days | `day.ts` | UTC day numbers: DST, leap days and month ends are safe. Only `dayKey(Date)` touches the local timezone |
| Scheduling | `srs.ts` | SM-2-lite, early reviews do not inflate, deadline cap, review queue, forecast |
| Streak | `streak.ts` | `streakStatus` predicts, `advanceStreak` settles on the next practice (lazy, idempotent) |
| Goal | `goal.ts` | targets are snapshotted per day, comeback day, 14-day bars |
| Mastery | `mastery.ts` | unit strength, readiness, weak spots, weighted mixed practice |
| Today | `daily.ts` | composition + `interleave` |
| Plan | `plan.ts` | interview-date maths |
| One write path | `events.ts` | `applySession(progress, result, {today, course})` returns the next progress **and** the events (rank-up, streak milestone, ...) |
| Storage rules | `migrate.ts`, `storage.ts`, `backup.ts` | v1 -> v2, normalisation, load/save, export/import/merge |

All of it is pure: `today` is always passed in, there is no `Date.now()` in `shared/`. The browser side is `web/src/lib/store.ts`
(a single store shared by every component, synced across tabs).

## Rules we hold ourselves to

- No hearts, energy or anything that blocks practising. No paid, ad-gated or time-limited streak repair. No "streak in danger"
  banners, countdown-to-midnight timers, sad mascots or red badge counters. Freezes are earned (every 7 days in a row) and capped at 2.
- Rest days are the learner's choice and free. A lapse is "fresh start": the longest streak and the days practised overall never reset.
- Copy is factual ("6 questions are due for review"), never guilt. The backlog is stated honestly ("73 due · 10 per session") and Today still asks for 8. "Due" always means a card whose due date has arrived (`dueOverview`).
- The only variable reward is additive, disclosed and switchable: about 1 session in 6 shows a "Worth remembering" tip with +5 XP,
  decided by `fnv1a(date|sessions) % 6`, so a reload cannot re-roll it. It never gates anything.
- Readiness means "how much of this course you would recall today", with coverage shown. It never claims a chance of passing.
- No accounts, no analytics, no fake users or leaderboards. The learner owns the data: export and import it.
- Celebrations are scaled (at most one big one per session, at most one burst per two minutes), respect `prefers-reduced-motion`
  and have a switch. Onboarding is an inline card, never a modal, so deep links keep working.

## Edge cases we decided on

- **Midnight**: the day is taken when a session *finishes*. A session that crosses midnight counts for the new day.
- **Clock moves backwards**: the streak holds, `lastDay` is re-anchored on the next practice, card intervals never go negative.
- **Content edited**: question ids are positional, so a card stores a hash of the question; a mismatch drops the card.
  `missed` entries for unknown ids are kept (harmless, and the migration stays lossless).
- **Replaying a lesson**: counts towards the ring and practice (so it is never wasted) but cannot lengthen a card's interval.
- **A week away**: the next day's goal is halved once. Cards that are very overdue lose strength gradually (retention is floored at 10%).
- **Interview date**: past dates are ignored (with a "Set a new date?" prompt), today or tomorrow switches Today to due cards and weak spots, and card intervals are capped at half the time left.

## Not built yet (specified)

Weekly recap, teach-back, a transfer code / QR, a weekly backup nudge and sound are specified in
[the engagement spec](research/engagement-spec.md) but not built; the [handoff](HANDOFF.md) (P2-4) has the details.
Everything else in the recommended batch is built, including reminders, the share card, achievements, confidence
ratings, the daily challenge and mock interviews (see [learning features](learning-features.md)). The `Progress` type
has room: later additive fields are optional and get defaults in `normalizeProgress`, so they need no migration.

## Reminders

A reminder is a cue the learner chose, not a lever we pull. Rules: opt-in only (nothing is requested until the toggle is used), the time is theirs, at most one local notification a day,
nothing on rest days or after practising or when nothing is due, copy that states a fact ("5 questions are due for review"), no streak talk, one tap to turn it all off, and honesty about the limits
(no push server: the calendar event is the reliable channel). The `.ics` generator and the notification rule are pure functions in `shared/src/reminders.ts` with an injected clock.

## Share card

Share only what the learner already sees, never anything inferred or personal (no interview date, role, practice time or question content), and make it optional and local: the image is drawn in the browser
and handed to the system share sheet or saved; nothing is uploaded and there is no referral tracking. The copy never claims a result ("readiness is an estimate of recall").

## Tomorrow bridge

After each of the first three sessions the summary says what is next and when ("Next review: 7 questions tomorrow, about 4 min"). Once, in those early sessions, it also offers a calendar entry (time picker, `.ics`) with a plain "No thanks". The offer is marked as shown the moment it appears (`prefs.reminders.nudged`) so it never returns, is not shown when a reminder already exists, and says nothing about streaks or loss.
