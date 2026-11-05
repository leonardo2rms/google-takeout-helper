> **Status: research draft.** Based on web-search summaries (vendor sites were not reachable); matrix cells marked `?` are unconfirmed.

# Interview Trainer: feature-level competitive teardown and gap analysis

Date of research: 2026-11-04. Scope: features only. We copy ideas, never content, code or branding.

## 0. Method and what could NOT be verified (read this first)

- **WebFetch was blocked** for the vendor sites I tried (blog.duolingo.com, brilliant.org, leetcode.com all failed DNS resolution in this sandbox: they are outside the session's allowed domains). All findings therefore come from **WebSearch result summaries**, not from reading the vendor pages. Consequences:
  - Many claims are second-hand (review blogs, comparison sites, forum posts). Those are tagged **(3P)**. Claims backed by an official domain appearing in search results (Duolingo blog, HackerRank help centre, Exponent, Codewars docs, Exercism, JetBrains blog, LeetCode forum, Quizlet help) are tagged **(official-ish)**, but I saw them through a summary only.
  - Cells in the matrix marked `?` are inferred or from general product knowledge and **not confirmed in this research**.
- **Not verified at all** (no usable result): Duolingo Practice Hub details beyond one blog line; Duolingo reduced-motion/dyslexia options; LeetCode Daily Challenge/Playground/"Ask Leet" specifics and any "AI study plan"; Anki image occlusion; Quizlet "Memory Score"; AlgoExpert progress tracking; Baeldung quizzes/video/code review; Mimo/Sololearn streak mechanics and AI features (sources conflict); NeetCode AI features; Hello Interview whiteboard review; Educative standalone skill assessments; Brilliant Koji details beyond "AI tutor, Premium".
- **Prices and tier splits are volatile and sources conflict**; I did not rely on any price. Several sources (e.g. a DEV.to "LeetCode 2.0 vs HackerRank 5" benchmark, a Duolingo "DuoHacker" script) looked unreliable or off-limits and were discarded.
- "Aced" (Exponent rebrand) was seen on Exponent's own blog with no date; treated as current but unconfirmed.

## 1. Competitor feature notes (features, not marketing)

### Duolingo (consumer gamified learning; the UX benchmark)
- Mechanics: short bite-sized lessons on a guided path; instant right/wrong feedback; many exercise formats. Practice tab with a **Mistakes** review so errors "won't trip you up next time" (official blog, search summary). 
- Retention loop: day **streak**, **Streak Freeze** bought with in-app currency (gems) and applied proactively (official blog + fan wiki), **Friend Streak** shared with friends (3P), **Friends Quests** weekly shared goals, **Leagues** weekly leaderboards (official blog), daily goal, notifications.
- Pacing/monetisation: an **energy** system limits free practice (3P; least settled); ads on free tier.
- AI: **Explain My Answer** (tap after an exercise to chat about why you were right/wrong; launched in Max 2023, reported free for all from Jan 2026, 3P/official mix); **Roleplay** open conversation with end-of-chat feedback; **Video Call** in the top tier. Duolingo says it reviews AI explanations for correctness.
- Accessibility: works with VoiceOver per one video; blind-user report of unlabeled elements and inaccessible exercises; no confirmed reduced-motion setting (3P). 
- Sources: https://blog.duolingo.com/tips-for-maintaining-streak , https://blog.duolingo.com/friends-social-features/ , https://blog.duolingo.com/ko/guide-to-duolingo-practice-hub , https://blog.duolingo.com/duolingo-max , https://duolingo.fandom.com/wiki/Shop/Streak_freeze , https://www.marketbeat.com/instant-alerts/duolingo-q4-earnings-call-highlights-2026-02-26/ , https://duolingoguides.com/is-duolingo-accessible-features-for-all-users/ , https://inviocean.com/learn/language-sounds-the-blind-experience-with-duolingo/

### Brilliant (interactive STEM; closest in "learn by doing" feel)
- Interactive problem-solving lessons, daily streaks and progress tracking, personalised recommendations and a starting path chosen from interests and skill level, "jump ahead" and personalised practice, an AI tutor (Koji) in Premium, free tier limited to a small number of daily lessons, 40+ courses (official help pages via search; the rest 3P).
- Sources: https://brilliant.org/help/pricing-and-plans/what-s-the-difference-between-free-and-premium , https://brilliant.org/premium/ , https://brilliant.org/help/using-brilliant/how-do-i-get-started-on-brilliant/

### Mimo / Sololearn (mobile coding)
- Mimo: structured learning paths, mobile-first coding exercises, portfolio-style projects, mostly Pro-gated; Mimo's own page says no AI features in free/Pro (conflicts with 3P claims). Sololearn: in-app mobile code editor, points/badges/leaderboards, an AI assistant ("Kodie") per one 3P source. Streak mechanics not confirmed.
- Sources: https://mimo.org/blog/mimo-vs-sololearn , https://similarweb.com/app/google/com.sololearn/vs/com.getmimo , https://marlvel.ai/apps/sololearn-learn-to-code

### Anki and Quizlet (spaced repetition benchmark)
- Anki: **FSRS** scheduler (since 23.10), per-preset **desired retention** (0.70 to 0.99), **optimizer** that fits parameters to your own review history, "reschedule cards on change", **True Retention** stats table (24.11+), deck presets, tags/flags/search, import/export, local-first with optional sync (3P forum posts plus the FSRS docs mirror).
- Quizlet: **Learn** mode is adaptive and spaced (a session completes only after each term is answered correctly twice; Write/Spell merged into Learn), **Test** auto-generates a fresh practice test from a set (paywalled beyond one), **Match** is a timed pairing game (Quizlet help centre, official-ish).
- Sources: https://forums.ankiweb.net/t/fsrs-desired-retention/53895 , https://forums.ankiweb.net/t/anki-decks-settings/53775 , https://docsearch.algolia.com/mcp/docs/repo/open-spaced-repetition/fsrs4anki , https://help.quizlet.com/hc/en-au/articles/360030841732-Studying-on-Quizlet

### LeetCode
- Company-tagged problem lists and frequency sorting (Premium; the most-cited reason to pay), **Study Plans** with a daily unlock (00:00 UTC), deadlines, completion badges and a weekly leaderboard; later a Premium weekly practice plan with a **"My Plan" calendar**; mock interview/assessment modes (Premium, details unverified); an "Ask Leet" AI companion with customisable instructions (3P changelog summary); in-browser judge. Daily challenge, hints, notes, discussion/solutions tabs are well-known but not confirmed in this research (`?`).
- Sources: https://leetcode.com/discuss/study-guide/1422121/introducing-new-feature-study-plan , https://leetcode.com/discuss/post/3482910/feature-updates-plan-your-coding-journey-to-achieve-more/ , https://leetcode.com/discuss/post/5736503/ , https://prachub.com/resources/leetcode-pricing , https://www.teamrora.com/post/leetcode-premium-review

### NeetCode
- Curated roadmap (NeetCode 150 = Blind 75 + 75), suggested study order, 410+ free video explanations, courses incl. system design, OOD, behavioural; criticised for needing supplementation with mock interviews/communication practice (3P).
- Sources: https://neetcode.io/practice/practice/neetcode150 , https://www.educative.io/blog/is-neetcode-150-enough-to-crack-tech-interviews , https://www.lodely.com/blog/neetcode-review

### Exercism
- Free, track-based (Kotlin ~88 practice exercises), **concept exercises** vs **practice exercises**, **human mentoring** with volunteer mentors (hundreds per track), "Insiders" supporters tier, Jiki beginner product (official-ish via search).
- Sources: https://exercism.org/tracks/kotlin , https://exercism.org/docs/mentoring , https://exercism.org/tracks/kotlin/build

### Codewars
- Kata with **kyu/dan rank** (earned only by solving) separate from **honor** (activity, voting, translations); **solutions gated until you solve or give up** (giving up forfeits rank progress); privileges unlock with honor (create kata, translate); clans; community translation into more languages (Codewars docs; some pages TBD).
- Sources: https://docs.codewars.com/gamification , https://docs.codewars.com/concepts/kata/solutions/

### HackerRank
- **Prep Kits**: practice with an **AI Tutor** giving step-by-step guidance, mock tests modelled on real assessments, and two AI mock interviews (60-min coding, 60-min system design with whiteboard and section-based feedback); AI interviewer asks follow-ups and gives end-of-session feedback; free accounts get one use each of two mocks, rest need credits; free timed **skill certifications** shareable publicly (official help centre, Feb 2026, plus 3P).
- Sources: https://hackerrank-community-knowledge-base.help.usepylon.com/articles/1723224478-introduction-to-prep-kits , https://hackerrank-community-knowledge-base.help.usepylon.com/articles/8988753946 , https://hackerrank-community-knowledge-base.help.usepylon.com/articles/8813699799

### Educative
- Text-based interactive courses with in-browser coding (no setup), skill paths, AI-driven personalised roadmaps and "Explain with AI" (3P), AI mock interviews (3P: launched 2026 as "Fenzo"; per-pattern and system-design mocks); **no offline mode** (3P).
- Sources: https://www.educative.io/unlimited?aff=BPgn , https://todaytesting.com/educative-review/ , https://www.educative.io/mock-interview?aff=VpRR

### AlgoExpert
- ~200 questions in 15 categories, 2-part video explanations, in-browser multi-language judge, timed assessments, peer mock-interview pairing, SystemsExpert (modules, ~13 design questions, quiz). Progress tracking and spaced repetition reported missing (3P; unverified).
- Sources: https://teamrora.com/post/algoexpert-review , https://www.techinterview.org/bootcamp_directory/p/algoexpert/

### interviewing.io / Pramp / Exponent (now "Aced")
- interviewing.io: live **anonymous** mocks with real engineers, performance-based blind matching, written feedback, recordings with annotations (3P). Pramp: free-ish peer-to-peer mocks, now under Exponent (3P, free credit policy disputed).
- Exponent/Aced: peer matching with turn-taking interviewer/interviewee and **rubric-based feedback**; **AI mock interviewer** scoring against real-interview criteria; **"Ace" AI coach** that builds a prep study guide for a role/company, researches questions, explains lessons; role tracks (SWE, system design, ML, AI/GenAI); human coaching (official site + 3P).
- Sources: https://www.tryexponent.com/blog/exponent-is-becoming-aced , https://www.tryexponent.com/pages/business?src=nav , https://www.tryexponent.com/practice/data-structures-and-algorithms-mock-interviews , https://www.techinterview.org/post/3233474681/mock-interview-platforms/ , https://www.lodely.com/blog/pramp-vs-interviewing-io

### Baeldung and JetBrains Academy / Hyperskill (Kotlin/Spring)
- Baeldung: long-form tutorials, Kotlin+Spring track split into Basics/Advanced, paid courses (Learn Spring, Spring Security, REST with Spring Boot, Spring Data JPA), Pro tier (ad-free, dark mode, bundled IDE licence). Quizzes/video not confirmed.
- Hyperskill/JetBrains Academy: project-based **Kotlin Backend Developer (Spring Boot)** track, theory lessons plus tasks checked by tests, 10 free submissions/day, **in-IDE** courses via plugin (standalone Hyperskill plugin since Dec 2025), shareable achievements.
- Sources: https://www.baeldung.com/kotlin/spring-series , https://www.baeldung.com/kotlin/category/spring , https://hyperskill.org/tracks/142 , https://blog.jetbrains.com/education/2025/12/09/hyperskill-gets-its-own-plugin-inside-jetbrains-ides/ , https://blog.jetbrains.com/education/2025/12/11/jetbrains-academy-plugin-2025-11-is-now-available/

### Newer AI-tutor / AI-mock-interview products
- **Hello Interview**: AI mock interviews plus human-led mocks (claims 10k+ human mocks); detailed strengths/improvements feedback; whiteboard review unconfirmed. https://www.hellointerview.com/live
- **Codemia**: "LeetCode for system design": ~120 problems, whiteboard/diagram tool, AI feedback on scalability/fault tolerance, OOD challenges; no spaced repetition found. https://codemia.io , https://designgurus.io/answers/detail/what-is-codemia
- **Mockingly.ai**: AI interviewer with hard follow-ups, interactive canvas, post-interview analysis, free tier (3P). https://prachub.com/resources/mockingly-ai-review-2026-ai-system-design-mocks-vs-real-interview-questions
- (Also covered above: HackerRank AI mock interviewer, Educative AI mocks, Exponent Ace, Duolingo Explain My Answer, Brilliant Koji.)
- Pattern: the market has converged on (a) AI interviewer with follow-ups, (b) rubric-scored feedback, (c) system-design canvas. **None of the sources describe a combined bite-sized, spaced, offline, senior-JVM-focused trainer.**

## 2. Feature matrix

Legend: Yes / Partial / No. `?` = not confirmed in this research. "Us" = Interview Trainer today. Anki column also stands for Quizlet where noted. Exponent = Exponent/Aced.

| Feature | Us | Duolingo | Anki/Quizlet | LeetCode | Educative | HackerRank | Exponent/Aced |
|---|---|---|---|---|---|---|---|
| Short gamified lessons, instant feedback | Yes | Yes | Partial | No | Partial | No | No |
| Many question formats (6+) | Yes (9) | Yes | Partial | No | Partial | Partial | No |
| Run code against tests in browser | Yes | No | No | Yes | Yes | Yes | Partial ? |
| Open answers graded by AI vs rubric | Yes | Partial (Roleplay) | No | No | Partial | Yes | Yes |
| Adaptive interviewer follow-ups | Yes | No | No | No | Partial ? | Yes | Yes |
| Estimate / numeric, match, spot-the-bug | Yes | Partial | No | No | Partial ? | No | No |
| Senior JVM/Kotlin/Spring/DDD/resilience content | Yes | No | No | No | Partial | No | Partial |
| AI-assisted-engineering content | Yes | No | No | No | Partial | Partial | Partial |
| Mistakes review queue | Yes | Yes | Yes (inherent) | No | No | No | No |
| True spaced-repetition scheduler | No | Partial ? | Yes | No | No | No | No |
| Daily streak | Yes | Yes | Partial ? | Yes | Partial ? | Partial ? | No |
| Streak freeze / repair | No | Yes | No | Partial ? | No | No | No |
| Daily goal / daily challenge | No | Yes | Partial (daily limits) | Yes ? | No | Partial ? | No |
| XP / stars / levels | Yes (XP, stars) | Yes | No | Partial (badges) | No | Partial (badges, certs) | No |
| Guided path / roadmap | Yes (fixed path) | Yes | No | Yes (study plans) | Yes | Yes (prep kits) | Yes |
| Interview-date / target-role plan | No | No | No | Partial (My Plan) | Partial (AI roadmap) ? | Partial | Yes (Ace guide) |
| Topic mastery / readiness score | No | Partial | Partial (retention stats) | Partial | Partial ? | Partial (certs) | Partial ? |
| Company / role-targeted tracks | No | No | No | Yes (Premium) | Partial | Partial | Yes |
| Timed mock interview / mock loop | No | No | Partial (Quizlet Test) | Partial ? | Yes (AI mocks) | Yes | Yes |
| AI "explain my answer" on a wrong answer | No (static explanation) | Yes | No | Partial ? (Ask Leet) | Yes (3P) | Yes (AI Tutor) | Partial (Ace) |
| Hints (progressive) | No | No | No | Yes ? | Partial ? | Yes (AI Tutor steps) | No |
| Bookmarks / notes | No | No | Yes (flags, tags) | Yes ? | Yes ? | No | No |
| Search across items | No | No | Yes | Yes | Yes | Yes | Yes |
| Progress export/import without account | No | No | Yes | No | No | No | No |
| Cross-device sync | No (by design) | Yes (account) | Yes (account) | Yes (account) | Yes (account) | Yes (account) | Yes (account) |
| Works fully offline | Yes | Partial ? | Yes | No | No (3P) | No | No |
| No account, no data leaves device | Yes | No | Yes (local) | No | No | No | No |
| Social / leagues / friends | No (by design) | Yes | Partial (shared decks) | Partial | Partial ? | Partial | Yes (peer mocks) |
| Human or peer mock interviews | No | No | No | No | No | No | Yes |
| System-design practice | No (deferred) | No | No | Partial ? | Yes | Yes | Yes |
| Shareable result / certificate | No | Yes | No | Yes (badges) | Partial ? | Yes (certs) | No |
| Dark mode | Yes | Yes | Yes | Yes | Yes ? | Yes ? | Yes ? |
| Reduced motion / font options / screen-reader audit | Partial (dark mode, 44px targets, aria labels; no reduced-motion rule found in CSS) | Partial | Yes (flexible fonts) ? | Partial | Partial | Partial | Partial |
| Localisation | No | Yes | Yes | Partial | No | Yes ? | No |

Our current state, from the repo: `Progress` holds `xp`, `streak`, `lastDay`, `lessons{bestAccuracy, completions, lastDay}` and `missed[]` only; **no per-question history, timestamps or attempts**, which is the main blocker for SRS and mastery. Questions already carry `difficulty` (easy/medium/hard) and `tags[]` in the schema, which is an unused enabler for mastery, adaptive difficulty and role tracks. `web/src/styles.css` has a dark-mode media query but no `prefers-reduced-motion` rule.

## 3. Gap list (ranked by importance to a senior engineer x feasibility with no backend, offline-first)

Scoring: Importance (I) 1-5 for a senior engineer with an interview date; Feasibility (F) 1-5 local-only. Rank by I x F, ties by importance.

### 1. Spaced-repetition review queue (Must, effort M) [I5 x F5]
Competitor basis: Anki FSRS/Quizlet Learn; Duolingo Mistakes. Our "missed" list is a plain set with no scheduling, so a question fixed once never returns, which is the biggest retention hole.
Sketch: Bump `Progress` to `version: 2` with `cards: Record<questionId, {due: dayKey, interval: days, ease/stability, lapses, last: dayKey}>` and a migration that seeds cards from `missed` (due today). Add `shared/src/srs.ts` (pure, tested like `logic.test.ts`): start with a Leitner/SM-2-lite ladder (1, 3, 7, 16, 35 days; wrong answer resets to 1), leave an interface so FSRS can replace it later. Replace the "Review mistakes" card on `Home.tsx` with "Due today (N)"; the review session reuses `createSession`; every answered question (lesson or review) updates its card. Cap daily due at ~15 and show a forecast.

### 2. Interview-date study plan with daily goal (Must, effort M) [I5 x F4]
Competitor basis: LeetCode study plans / My Plan, Exponent Ace study guide, Duolingo daily goal.
Sketch: Onboarding sheet on first run (and in settings): interview date, minutes/day, focus units. `shared/src/plan.ts` turns (remaining lessons + due cards + days left) into a daily target of lessons/questions, spreading weaker topics earlier and tapering new content in the last 3 days for review only. Store `plan: {date, perDay, focus[]}` in Progress; Home shows "Today: 1 lesson + 8 reviews" with a progress ring and "on track / behind" copy. Daily goal also gives the streak a concrete meaning (see gap 11).

### 3. Topic mastery and readiness score (Must, effort M) [I5 x F4]
Competitor basis: partial everywhere; Anki retention stats; HackerRank certs.
Sketch: Add `shared/src/mastery.ts` computing per-tag and per-unit mastery from card state (gap 1) weighted by difficulty and recency decay, and an overall "readiness %" with a confidence band (few answers = low confidence, say so). New `Progress` tab/screen on `Home` with a radar or bar list per unit, weakest-three call-to-action ("Drill: Coroutines cancellation"), and a "practice weak spots" session builder that samples by low mastery. Needs consistent `tags` across the 146 questions (content audit, S).

### 4. Progress export/import and no-server device transfer (Must, effort S-M) [I4 x F5]
Competitor basis: Anki import/export (others require accounts, which we deliberately lack). Also protects the user against losing everything when Safari/Chrome evicts `localStorage` or on a new phone.
Sketch: `shared/src/backup.ts` serialises `Progress` (versioned, validated with zod, checksum) to JSON; Settings gets Export (download/share-sheet file), Import (file picker, merge-by-max for XP/streak and per-card newest `last`, not blind overwrite) and "Copy transfer code" (base64url + deflate via `CompressionStream`, fits a QR if under ~1.5 kB; QR render via a tiny local encoder, no network). Also call `navigator.storage.persist()` and show a gentle "back up" nudge weekly. Single-file build uses the same code.

### 5. Timed mock interview / mock loop (Should-leaning-Must, effort M) [I5 x F3]
Competitor basis: HackerRank (60-min AI mocks), Educative, Exponent, Pramp. We have the pieces (open questions with follow-ups, code questions, estimates) but no simulation of pressure.
Sketch: New "Mock loop" mode: pick a role profile (e.g. Senior Backend/Kotlin, 45 min), `shared/src/mock.ts` assembles 1 code + 1 open (with follow-ups) + 2 to 3 quick items + 1 estimate under a timer; no per-item feedback and no retries until the end; then a scorecard (per-rubric-point results, time per item, "hire / lean hire / lean no" style bands from score), plus gap 3 mastery update and gap 1 cards for misses. Timer state persisted so a reload does not reset it. AI-graded parts degrade to self-assessment offline as today.

### 6. "Ask why" / AI tutor on a wrong answer (Should, effort M) [I4 x F3]
Competitor basis: Duolingo Explain My Answer, HackerRank AI Tutor, Educative Explain with AI, Exponent Ace.
Sketch: On the `Feedback` component add "Explain differently" / "Why not option B?" which calls the existing grading transport (server `/api/grade`-style endpoint, or the standalone `sample` capability) with a new tutor prompt in `shared/src/grading-prompt.ts` (question, learner answer, correct answer, existing explanation fenced as untrusted). Offline or no key: hide the button and show the static explanation (graceful degradation is already our pattern). Cap 1-2 turns per question, cache replies in `localStorage` keyed by question id + answer, and never score from this chat.

### 7. Hints that cost XP (Should, effort S) [I3 x F5]
Competitor basis: LeetCode hints, HackerRank AI Tutor steps.
Sketch: Add optional `hints: string[]` (1-3, progressive) to the question schema (zod, content check); a "Hint (-3 XP)" button on `Lesson.tsx` reveals the next one; `session.ts` records `hintsUsed` and caps first-try XP, and a hinted correct answer counts as "shaky" for the SRS interval (gap 1). Author hints for code, spot and estimate questions first (highest value).

### 8. Bookmarks, personal notes and search (Should, effort S-M) [I4 x F4]
Competitor basis: Anki flags/tags/search, LeetCode notes/favourites (`?`).
Sketch: `bookmarks: string[]` and `notes: Record<qid,string>` in Progress; star and note icons on the feedback card; a "Saved" list and a search box on Home using a prebuilt in-memory index over prompt, tags, explanation (simple tokenised match, no library needed for 146 items; revisit if content grows past ~2k). Search results open a single-question practice session or reader view. Notes should be included in export (gap 4).

### 9. Accessibility pass (Should, effort S) [I4 x F5]
Competitor basis: baseline expectation; Duolingo's own gaps (unlabelled elements) show the risk.
Sketch: Add `@media (prefers-reduced-motion: reduce)` to `styles.css` (disable shake/confetti/transitions), a Settings panel for text size (scale the 17px base), a system/dyslexia-friendly font toggle (bundle an open-licensed font only if its licence allows redistribution; otherwise system fonts with wider spacing), high-contrast theme, and `aria-live` announcements for correct/incorrect feedback. Audit the tap-based types (order, match, spot, fill) with VoiceOver/TalkBack and add keyboard alternatives; extend the Playwright suite with axe checks.

### 10. Daily challenge (Should, effort S) [I3 x F5]
Competitor basis: LeetCode daily problem, Duolingo daily goal.
Sketch: Deterministic pick by `dayKey` hash over the course (no server), one "hard" mixed item (prefer code or open) with a distinct card on Home and a calendar heat-map of completed days (stored as a `Set` of day keys). Same question for every user on a given day is a free side effect and allows sharing the result without a backend.

### 11. Difficulty adaptation (Should, effort M) [I3 x F4]
Competitor basis: Brilliant personalised practice/jump-ahead, Quizlet Learn adaptive.
Sketch: Use existing `difficulty` and mastery (gap 3): in mixed/weak-spot sessions, choose next item harder after 2 consecutive first-try hits and easier after 2 misses; offer "Test out" on a unit (5 hard questions, pass to mark lessons as skipped with stars). Pure function in `session.ts`/new `adaptive.ts`, tested with seeded random.

### 12. Streak freeze and weekend mode (Could, effort S) [I2 x F5]
Competitor basis: Duolingo Streak Freeze.
Sketch: Earn one freeze per N XP (cap 2) rather than buying; `nextStreak`/`liveStreak` in `progress.ts` consume a freeze when exactly one day is missed. Local-only means it is trivially cheatable, which is acceptable for a self-motivation tool. Keep gentle (no guilt notifications).

### 13. Role-targeted tracks (Should, effort M, content cost) [I4 x F3]
Competitor basis: LeetCode/Exponent company and role tracks. We must **not** claim company-specific "real questions" (proprietary, unverifiable).
Sketch: Add `tracks` (ordered tag filters, e.g. "Spring Backend", "Kotlin/Coroutines deep dive", "Staff/Architecture", "AI-augmented engineer") as a small YAML file referencing lessons/tags; the plan (gap 2) and mock (gap 5) take a track as input. Company flavour limited to generic public-themes like "high-scale payments" without implying insider knowledge.

### 14. Flashcard / concept-card mode (Could, effort S-M) [I3 x F4]
Competitor basis: Anki, Quizlet.
Sketch: Derive cards from open-question key points and explanation first sentences, or add optional `cards: [{front, back}]` per lesson; reuse the SRS engine with a "know / unsure / don't know" self-grade. Good for commute-style micro sessions and for definitions (e.g. JMM guarantees).

### 15. Shareable result card and achievements (Could, effort S-M) [I2 x F4]
Competitor basis: Duolingo achievements, HackerRank shareable certs, Hyperskill achievements.
Sketch: Render a PNG on a `<canvas>` (readiness %, streak, top unit) and use `navigator.share` / download; badges as pure derived predicates over Progress (7-day streak, first perfect unit, 100 reviews). Do not add public leaderboards.

### 16. Code editor niceties (Could, effort S) [I3 x F4]
Competitor basis: LeetCode/HackerRank run history and custom tests.
Sketch: In `CodeQuestion.tsx` keep the last N runs in memory with pass counts, a "custom input" box that runs the learner's function in the existing worker (`code-runner.worker.ts`) and shows output only, and a "reset to starter" button. Keep sandbox limits (timeouts) unchanged.

### 17. Voice answers (Could, effort M) [I2 x F2]
Competitor basis: Duolingo Roleplay/Video Call; AI mock interviewers.
Sketch: `SpeechRecognition` for open questions as a dictation fallback (availability and offline support vary by browser/OS; feature-detect and hide when absent), then the usual grading. Do not store audio. Useful for practising talking through trade-offs.

### 18. System-design / architecture board (Could now, Should later, effort L) [I4 x F2]
Competitor basis: Codemia, Educative, HackerRank, Mockingly, Exponent. We deliberately deferred it.
Sketch: Interim M-effort stand-in using existing machinery: structured "design review" open questions (requirements, API, data model, bottlenecks, failure modes) graded by rubric, with an estimation step; later a minimal box-and-arrow canvas serialised to text for the AI grader. The canvas is a large mobile-UX investment and AI diagram grading is unproven, so keep deferred.

### 19. Localisation (Could, effort L) [I1 x F3]
Competitor basis: Duolingo, Quizlet. Low value: target is English-language interviews. Only extract UI strings into a catalogue if a second language is requested; do not translate technical content.

### 20. Discussion / community solutions (Won't, see section 5)
Codewars/LeetCode value here comes from a community; without a backend it does not exist. We already show a model answer and rubric after answering.

### Ranked top 10 for the roadmap
1 SRS review queue, 2 interview-date plan + daily goal, 3 mastery/readiness, 4 export/import, 5 timed mock loop, 6 AI "ask why" tutor, 7 hints costing XP, 8 bookmarks/notes/search, 9 accessibility pass, 10 daily challenge. (Role tracks and difficulty adaptation follow.)

Sequencing note: 1 -> 3 -> 2 -> 5 share one data change (per-question card history in `Progress` v2); do that migration once, with export (4) shipped in the same release so users can protect their data before the schema changes.

## 4. What we already do better than most competitors (keep and promote)

1. **Question-format breadth in one phone-first flow**: 9 types including spot-the-bug, numeric estimation, match and run-code, all graded instantly. Duolingo has breadth but not for engineering; LeetCode/HackerRank have only code.
2. **Open questions graded per rubric point with adaptive follow-ups** (covered vs missed point chooses the probe), explainable ✓/✗ per point, untrusted-input fencing, and graceful degrade to self-assessment. Closest rivals (HackerRank, Exponent) are paid, online and account-bound.
3. **Senior-level, JVM-specific content**: Kotlin, coroutines, Spring internals, JVM, DDD, resilience, complexity estimation, AI-assisted engineering. Baeldung/Hyperskill teach it as long-form; nobody offers it as drills.
4. **Fully offline, no account, no data leaves the device** (PWA plus single-file build). Educative, LeetCode, HackerRank, Exponent need a network and an account. This is a differentiator for privacy-conscious engineers and for flights/commutes.
5. **Mistake recovery inside the lesson** (misses return, max 2 retries) plus a review queue, a combination few interview sites have.
6. **Estimation questions** (back-of-envelope with tolerance) which are a staple of senior interviews and absent elsewhere in bite-sized form.
7. **Tested, content-as-YAML pipeline** (schema validation, e2e that plays every lesson): makes adding content cheap and safe.
8. **Prompt-injection-aware grading**: a credible, rare trait for an AI-skills trainer; consider surfacing it as a teaching point.

## 5. What NOT to copy

- **Anything needing a backend or accounts**: leagues, friend streaks, friend quests, global leaderboards, community discussion/solutions, human or peer mock matching, server-side sync, push notification infrastructure. (Our no-backend stance is the product; use export/QR instead.)
- **Proprietary content**: LeetCode company-tagged question lists, AlgoExpert/NeetCode/Educative/Exponent course text, videos and rubrics, HackerRank assessments, interviewing.io question sets. Write our own questions; do not claim "asked at Company X". Do not scrape or paraphrase paid explanations.
- **Trademarks and branding**: Duolingo's owl, "Duo", gems, "Leagues" and its character-driven notification voice; Brilliant, Codewars "kata/kyu" naming, "Streak Freeze" as a branded term, Koji, Ace, Fenzo. Use our own names and art.
- **Dark patterns**: guilt-trip notifications and mascot shaming, loss-aversion streak anxiety, energy/heart limits that block learning to push upsells, countdown-timer paywalls, hidden auto-renewing trials, forfeiting progress for peeking (Codewars give-up), "streak society" social pressure. If we add streak freezes, make them earned and quiet; never gate practice.
- **Unverified AI claims**: do not promise "readiness" as a hiring prediction. Label it a study-progress estimate with a confidence note.
- **Pay-to-grade AI**: keep the self-assessment fallback always available so the core never depends on a key or network.

## 6. Source list (all seen through WebSearch summaries; vendor pages not directly fetched)

Duolingo: https://blog.duolingo.com/tips-for-maintaining-streak , https://blog.duolingo.com/friends-social-features/ , https://blog.duolingo.com/ko/guide-to-duolingo-practice-hub , https://blog.duolingo.com/duolingo-max , https://www.marketbeat.com/instant-alerts/duolingo-q4-earnings-call-highlights-2026-02-26/ , https://duolingo.fandom.com/wiki/Shop/Streak_freeze , https://duolingoguides.com/is-duolingo-accessible-features-for-all-users/ , https://techcrunch.com/2023/03/14/duolingo-launches-new-subscription-tier-with-access-to-ai-tutor-powered-by-gpt-4
Brilliant: https://brilliant.org/help/pricing-and-plans/what-s-the-difference-between-free-and-premium , https://brilliant.org/premium/
Mimo/Sololearn: https://mimo.org/blog/mimo-vs-sololearn , https://similarweb.com/app/google/com.sololearn/vs/com.getmimo
Anki/Quizlet: https://forums.ankiweb.net/t/fsrs-desired-retention/53895 , https://forums.ankiweb.net/t/anki-decks-settings/53775 , https://help.quizlet.com/hc/en-au/articles/360030841732-Studying-on-Quizlet
LeetCode: https://leetcode.com/discuss/study-guide/1422121/introducing-new-feature-study-plan , https://leetcode.com/discuss/post/3482910/feature-updates-plan-your-coding-journey-to-achieve-more/ , https://prachub.com/resources/leetcode-pricing
NeetCode: https://www.educative.io/blog/is-neetcode-150-enough-to-crack-tech-interviews , https://www.lodely.com/blog/neetcode-review
Exercism: https://exercism.org/tracks/kotlin , https://exercism.org/docs/mentoring
Codewars: https://docs.codewars.com/gamification , https://docs.codewars.com/concepts/kata/solutions/
HackerRank: https://hackerrank-community-knowledge-base.help.usepylon.com/articles/1723224478-introduction-to-prep-kits , https://hackerrank-community-knowledge-base.help.usepylon.com/articles/8988753946 , https://hackerrank-community-knowledge-base.help.usepylon.com/articles/8813699799
Educative: https://www.educative.io/unlimited?aff=BPgn , https://todaytesting.com/educative-review/
AlgoExpert: https://teamrora.com/post/algoexpert-review , https://www.techinterview.org/bootcamp_directory/p/algoexpert/
interviewing.io/Pramp/Exponent: https://www.tryexponent.com/blog/exponent-is-becoming-aced , https://www.tryexponent.com/pages/business?src=nav , https://www.tryexponent.com/practice/data-structures-and-algorithms-mock-interviews , https://www.techinterview.org/post/3233474681/mock-interview-platforms/
Baeldung/JetBrains: https://www.baeldung.com/kotlin/spring-series , https://hyperskill.org/tracks/142 , https://blog.jetbrains.com/education/2025/12/09/hyperskill-gets-its-own-plugin-inside-jetbrains-ides/
New AI products: https://www.hellointerview.com/live , https://codemia.io , https://designgurus.io/answers/detail/what-is-codemia , https://prachub.com/resources/mockingly-ai-review-2026-ai-system-design-mocks-vs-real-interview-questions
Our repo (read directly): interview-trainer/README.md, shared/src/progress.ts, shared/src/session.ts, shared/src/interview.ts, shared/src/schema.ts, web/src/screens/Home.tsx, web/src/styles.css, content/*.yaml.
