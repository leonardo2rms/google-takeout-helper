# Research and audit archive

Everything that was investigated while building Interview Trainer, kept as written. These are **snapshots**: they describe
the app at the time they were written and are not updated. For what was built afterwards and what is still open, read
[the handoff](../HANDOFF.md) first.

| File | What it is | Produced | Status now |
|---|---|---|---|
| [engagement-spec.md](engagement-spec.md) | Habit-loop spec grounded in learning science: 16 ranked features (spaced repetition, goal ring, forgiving streak, readiness, onboarding, ranks, celebrations, achievements, confidence, daily challenge, reminders, mock interview, teach-back, share card...), the `Progress` v2 schema and migration, and the "will NOT do" ethics list. | Before the engagement work | Recommended batch built; weekly recap, teach-back, transfer code/QR, sound and backup nudge are not. See handoff. |
| [design-spec.md](design-spec.md) | Critique of the original Duolingo-like UI (palette almost identical to Duolingo's, 39/56 contrast pairings failing) and the "drafting sheet / blueprint" identity: tokens, type, icons, motion, per-component notes. | Before the redesign | Implemented; the shipped system is in [design-system.md](../design-system.md). |
| [design-contrast-before.md](design-contrast-before.md) | Measured contrast of the original palette, light and dark. | Design audit | Historical. |
| [design-contrast-after.md](design-contrast-after.md) | Measured contrast of the proposed palette. | Design audit | Enforced in `npm test` by the contrast script. |
| [design-colour-blindness.md](design-colour-blindness.md) | Colour-distance check of old vs new verdict colours under protan/deutan/tritan simulation. | Design audit | Historical; verdicts now also use icon + word + border. |
| [../COMPETITORS.md](../COMPETITORS.md) | Feature-level teardown of Duolingo, Brilliant, LeetCode, NeetCode, Exercism, Codewars, HackerRank, Educative, AlgoExpert, interviewing.io, Exponent, Anki and others; feature matrix and ranked gap list. Vendor sites were unreachable, so it relies on search summaries. | Research phase | Gaps 1-10 mostly built; remaining ones are in the handoff. |
| [../MONETIZATION.md](../MONETIZATION.md) | Pricing comparables, 8-10 monetization options, unit economics of AI grading per model, paywall boundary, first experiment, go-to-market. Competitor prices unverified. | Research phase | Nothing implemented yet; next steps in the handoff. |
| [audit-ux-round1.md](audit-ux-round1.md) | First-time-user UX audit, findings UX-01..UX-38. | After the redesign | P0/P1 fixed; leftovers in the handoff. |
| [audit-qa-round1.md](audit-qa-round1.md) | Adversarial QA/security audit, findings S0-1..S2-27 with reproduction scripts (scripts not in repo). | After the redesign | S0/S1 fixed (content tells: handoff P1-1, done); leftovers in the handoff. |
| [audit-ux-round2.md](audit-ux-round2.md) | UX re-audit: round-1 verification and new findings N-1..N-18. | After round-1 fixes | P1s fixed; leftovers in the handoff. |
| [audit-qa-round2.md](audit-qa-round2.md) | QA re-audit: round-1 status, new findings N-S1-1..N-S2-13, hand-mutation results. | After round-1 fixes | Code findings and the content tells (P1-1) fixed; a few S2s open (handoff). |
| [p1-7-kotlin-coroutines.md](p1-7-kotlin-coroutines.md) | Claim-by-claim check of `01-kotlin` and `02-coroutines` against the Kotlin and kotlinx.coroutines docs and source (43 claims). | 2026-11-07 | Done; fixes merged. |
| [p1-7-spring-testing-security.md](p1-7-spring-testing-security.md) | The same for `03-spring`, `12-testing`, `13-security-observability` (48 claims). | 2026-11-07 | Done; fixes merged. |
| [p1-7-jvm-data-messaging-scale.md](p1-7-jvm-data-messaging-scale.md) | The same for `04-jvm`, `09-data`, `10-messaging`, `06-scale` (52 claims; JEP 491 and the Kafka log). | 2026-11-07 | Done; fixes merged. |
| [p1-7-ai-ddd-design-complexity.md](p1-7-ai-ddd-design-complexity.md) | The same for `08-ai`, `05-ddd`, `11-system-design`, `07-complexity` (119 claims). | 2026-11-07 | Done; fixes merged. |

The finding ids (UX-nn, N-nn, S1-n, S2-nn, N-S1-n, N-S2-nn) are referenced from the handoff and from commit messages, so
`git log --grep=N-S2-6` finds the fix for a given finding.
