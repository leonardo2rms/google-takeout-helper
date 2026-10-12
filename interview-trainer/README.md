# Interview Trainer

Duolingo-style practice for **senior engineer interviews**: Kotlin, Spring, the JVM, DDD, scaling/resilience
and working with AI. Short lessons, instant feedback, XP and streaks, and a "review mistakes" loop, built to be
comfortable on a phone.

> Lives in its own folder so it can be lifted into a dedicated repo later:
> `git subtree split -P interview-trainer -b trainer-only`, or just copy the directory. Nothing outside it is touched.

## What's in the box

| | |
|---|---|
| **116 questions / 8 units / 16 lessons** | Kotlin · Coroutines · Spring · JVM · DDD · Scale & resilience · Algorithms & complexity · AI-assisted engineering |
| **Question types** | `single`, `multi`, `open` (AI-graded), `fill` (tap tokens into blanks), `order` (tap lines into order) |
| **Lesson mechanics** | Check → feedback with explanation → continue. Misses return at the end of the lesson (max 2 retries). Perfect-lesson bonus. |
| **Progress** | XP, day streak, stars per lesson, "Review mistakes" queue, "Mixed practice". Stored in `localStorage`. |
| **Offline** | PWA: the whole course ships in the JS bundle and an app-shell service worker caches it. Works with no network. |

## Run it

```bash
cd interview-trainer
npm install

npm run dev          # web on :5173 (reloads when content YAML changes) + API on :8787
# or production-style, single process:
npm run build && npm start     # http://localhost:8787
```

To open it on your phone, `npm run dev` binds to the network (`vite --host`): use `http://<your-laptop-ip>:5173`.

```bash
npm test                # unit tests: grading, sessions, streaks, API, prompt building, all content
npm run typecheck
npm run content:check   # validates every YAML file and prints counts
```

### AI grading (optional)

```bash
cp .env.example .env     # put ANTHROPIC_API_KEY in it
```

With a key, open questions are graded by Claude against the question's rubric (`GRADER_MODEL`, default
`claude-opus-5-5`, effort `low` for snappy grading; try `claude-haiku-5-5` for cheap and fast). Without a key, or
when you're offline, the app **degrades gracefully**: the server answers 503, a keyword heuristic pre-ticks the
rubric, and *you* confirm which points you covered by comparing with the model answer.

How the grader is built (`server/src/grader.ts`):
- The model judges **each rubric point** (covered / not) rather than inventing a score; the score is computed
  from those verdicts, so grading is consistent and explainable (the UI shows ✓/✗ per point).
- Structured output (JSON schema) means no brittle parsing; a refusal, malformed output or wrong number of verdicts
  is an error, and the client falls back to self-assessment.
- The candidate's answer is treated as **untrusted**: it's fenced in `<candidate_answer>` tags, the system prompt
  says to ignore instructions inside it, and any attempt to close the tag is stripped. (Fitting, for an AI-skills
  trainer.)
- Rate-limited (30/min/IP by default), 32 kB body limit, and upstream errors are never leaked to the client.

### Single-file build (no server, no install)

```bash
npm run build:standalone    # -> dist/interview-trainer.html (one self-contained page, ~400 kB)
```

Everything (course, logic, styles) is inlined, with no service worker and no API server. Hosts that provide
a `sample` capability (a claude.ai artifact does) grade open questions through the viewer's own Claude account;
anywhere else it falls back to self-assessment. Handy for trying the app from a phone with nothing installed.

## Layout

```
interview-trainer/
  content/          the course: one YAML file per unit (edit these to add questions)
  shared/           schema (zod), grading, lesson-session engine, streak/progress logic. Pure TS, fully tested
  server/           Express API: /api/health, /api/grade (+ serves web/dist in production)
  web/              React + Vite PWA
  scripts/          content:check, build-standalone
```

Content is plain YAML on purpose: it is the most portable part of this project (any backend, a Kotlin/Spring
migration, a CMS import, whatever you move to later). The Vite plugin turns it into a validated virtual module at
build time; the server loads the same files to look up rubrics.

## Adding content

Drop a question into any lesson in `content/*.yaml`, or add a new `NN-name.yaml` for a new unit (files are ordered
by name). Question ids are derived automatically (`<lesson-id>/<n>`). `npm run content:check` and `npm test`
validate the structure and cross-field rules (answers exist, blanks match, rubric sanity).

```yaml
# single / multi
- type: single                      # or: multi (use `answers: [a, c]`)
  prompt: "What is the time complexity?"
  code: { lang: kotlin, source: "for (x in xs) for (y in xs) use(x, y)" }   # optional snippet
  options: [{ id: a, text: "O(n²)" }, { id: b, text: "O(n)" }]
  answer: a
  explanation: "Shown after checking. `backticks` render as inline code."

# short coding challenge: tap tokens into blanks  ([[n]] = blank n; bank needs >= 1 distractor)
- type: fill
  prompt: "Return the length or 0"
  template: "val len = name[[1]]length [[2]] 0"
  bank: ["?.", "?:", "!!", "?"]
  answers: ["?.", "?:"]
  explanation: "..."

# short coding challenge: tap lines into order (indentation is part of the line; lines must be unique)
- type: order
  prompt: "Arrange the function"
  lines: ["fun f(): Int {", "    val x = 1", "    return x", "}"]
  explanation: "..."

# open: AI-graded against the rubric (format: code gives a monospace box)
- type: open
  prompt: "Explain X"
  modelAnswer: "A strong answer..."          # shown after answering (acts as the explanation)
  keyPoints:                                  # 2-6 rubric points
    - point: "Mentions Y"
      keywords: ["y", "why"]                  # any-of; ONLY used by the offline fallback
```

Authoring rules of thumb (enforced where possible by tests):
- **Option order is shuffled at runtime**, so don't write "all of the above" / "both A and B" options.
- Every lesson should mix at least 3 question types and include one `open` question.
- A model answer must satisfy its own `keywords` rubric (a test checks this), otherwise offline users would be told
  the perfect answer is incomplete.

## Design notes

- **Why TypeScript, not Kotlin/Spring?** Fastest path to a mobile-first PWA that could be built, run and tested end
  to end in one environment. The valuable assets are the YAML course and the grading contract
  (`shared/src/api.ts`); a Spring Boot API exposing `GET /api/health` and `POST /api/grade` is a drop-in
  replacement for `server/`.
- **Code challenges are tap-based** (fill / order) rather than free typing because they work on a phone and can be
  graded deterministically without a sandbox. Free-form code answers use `open` with `format: code`, graded by AI.
- The course, including correct answers, is in the client bundle, which is what makes instant feedback and offline
  mode possible. That's fine for a self-training tool, but not for anything proctored or competitive.

## Roadmap / ideas

- **Architecture board** (drag & connect components, validate against a reference topology): the question schema is a
  discriminated union (`shared/src/schema.ts`), so it is one new type plus one component. Deliberately left out for now.
- Spaced repetition on the missed pile (currently random order).
- Per-topic skill meter; timed "mock interview" mode; export/import progress.
- Real code execution for coding challenges (sandboxed Kotlin runner) instead of AI grading.
