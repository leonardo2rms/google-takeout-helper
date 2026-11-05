# Content authoring guide

The course lives in `content/*.yaml`: one file per unit, ordered by file name (`01-kotlin.yaml`, `02-coroutines.yaml`, ...). Everything else in this repository exists to
teach these files well, so the standard here is higher than for code: **a wrong answer key is the worst bug this app can have**, because the learner trusts it.

Every YAML example in this guide is validated by `content/docs.test.ts`, so they are safe to copy: a `yaml` block that starts with `- type:` is checked as a list of questions
(open questions must satisfy their own rubric, code questions must have a passing solution), one that starts with `id:` as a whole unit, and a block marked `yaml skeleton` is an illustration that is not checked.
Run `npm run content:check` after every edit; it loads and validates the whole course and prints counts.

## The shape

```yaml skeleton
id: kotlin                    # unit id: lowercase letters, digits, dashes
title: Kotlin Essentials
emoji: "🧩"
color: "#7f52ff"              # any CSS colour; default green
description: Null safety, idioms and the language features seniors are expected to know cold.
lessons:
  - id: kotlin-null-types     # lesson id: same rule, unique across the whole course
    title: Null safety & types
    questions:                # at least 3; ids are derived: kotlin-null-types/1, /2, ...
      - type: single
        # ...
```

Rules enforced by the schema, `validateCourse` and `content/content.test.ts`:

- Unit and lesson ids are slugs and unique course-wide. Question ids are **derived** (`<lesson-id>/<position>`); never write them.
- A lesson has at least 3 questions, mixes **at least 3 question types**, and includes **at least one `open` question**.
- Optional on every question: `hint` (at most 220 characters, never the answer; revealing it costs XP and caps the review quality), `difficulty` (`easy`, `medium` default, `hard`), `tags` (extra search words for the Library), `code` (a snippet shown under the prompt: `{ lang, source }`).
- Prose is lightly formatted: `` `backticks` `` render as inline code. Keep prompts short; they are read on a phone.
- The unit ids `kotlin`, `coroutines`, `spring`, `jvm`, `ddd`, `scale`, `ai` are referenced by the role weights in `shared/src/mastery.ts`; renaming one changes the "Readiness" weighting.

## The nine question types

Below, the first block of each type is a complete, valid question. Constraints are listed under it.

### `single` - one correct option

```yaml
- type: single
  difficulty: easy
  prompt: Which scope function returns the receiver itself and exposes it as `this`?
  options:
    - { id: a, text: "`apply`" }
    - { id: b, text: "`also`" }
    - { id: c, text: "`let`" }
    - { id: d, text: "`run`" }
  answer: a
  hint: "Two scope functions return the receiver; only one of them calls it `this`."
  explanation: "`apply` configures the receiver (`this`) and returns it. `also` returns the receiver too but exposes it as `it`. `let` and `run` return the lambda's result."
  tags: [kotlin, scope-functions]
```

2 to 6 options with unique ids; `answer` is one id. Option order is **shuffled at runtime**, so never write "all of the above" or refer to an option by letter in the explanation.

### `multi` - select all that apply

```yaml
- type: multi
  prompt: Which of these HTTP methods are idempotent according to RFC 9110?
  options:
    - { id: a, text: "GET" }
    - { id: b, text: "POST" }
    - { id: c, text: "PUT" }
    - { id: d, text: "PATCH" }
    - { id: e, text: "DELETE" }
  answers: [a, c, e]
  explanation: "GET, PUT and DELETE are idempotent: repeating the request leaves the same server state. POST is not, and PATCH is not guaranteed to be (it depends on the patch format), so retries of either need an idempotency key."
```

3 to 7 options, at least 2 correct, all-or-nothing grading (the learner must pick exactly the right set).

### `fill` - tap tokens into the blanks of a snippet

```yaml
- type: fill
  prompt: Return the length of `name`, or 0 when it is null.
  template: "val len = name[[1]]length [[2]] 0"
  bank: ["?.", "?:", "!!", "?"]
  answers: ["?.", "?:"]
  explanation: "`name?.length` is null when `name` is null; the Elvis operator `?:` supplies the fallback 0."
```

Blanks are `[[1]]`, `[[2]]`, ... each used exactly once; `answers[i]` fills blank `i+1`. The `bank` must contain every answer **plus at least one distractor**. A token can be used twice if it appears twice in the bank. Set `lang:` for non-Kotlin snippets.

### `order` - tap lines into the right order

```yaml
- type: order
  kind: steps
  prompt: Put the states a circuit breaker goes through after a dependency starts failing in order.
  lines:
    - "Closed: calls pass through and failures are counted"
    - "Open: calls fail fast without reaching the dependency"
    - "Half-open: a few trial calls are let through"
    - "Closed again: the trial calls succeeded"
  explanation: "After enough failures the breaker opens; after a cool-down it lets trial calls through (half-open); if they succeed it closes again, otherwise it re-opens."
```

3 to 10 lines. `kind: code` (the default) renders monospace lines where **indentation is part of the line**; `kind: steps` renders plain sentences. All lines must be unique, including indentation, otherwise the order would be ambiguous. The canonical order is the order you write them.

### `spot` - tap the buggy line

```yaml
- type: spot
  prompt: Tap the line that causes an N+1 query problem.
  source: |
    val orders = orderRepository.findAll()
    val totals = orders.map { order ->
        order.items.sumOf { it.price }
    }
    println(totals)
  answers: [3]
  explanation: "`order.items` is a lazily loaded association, so line 3 issues one extra query per order. Fetch the items with a join (`@EntityGraph` or a `JOIN FETCH` query) instead."
```

3 to 20 lines, `answers` are **1-based line numbers** (1 to 4 of them, unique, never a blank line); the learner must select exactly those lines. Make the bug unambiguous: if two lines could reasonably be blamed, rewrite the snippet.

### `estimate` - type a number

```yaml
- type: estimate
  prompt: A service handles 2,000 requests per second and each takes 40 ms on average. How many requests are in flight on average?
  answer: 80
  tolerance: 0
  unit: requests
  explanation: "Little's law: L = lambda x W = 2,000 req/s x 0.040 s = 80 requests in flight."
```

`tolerance` is **relative** (0.05 = within 5%, default; 0 = exact; at most 0.5). `answer` must not be 0. The input accepts `1,234`, `1 234`, `1.234,5`, `18,2`, `18.2%` and `5k`. Prefer a tolerance that matches the *reasoning* (powers of two and back-of-the-envelope figures deserve 10 to 20%).

### `match` - connect pairs

```yaml
- type: match
  prompt: Match each resilience pattern to the problem it solves.
  pairs:
    - { left: "Bulkhead", right: "A slow dependency exhausts the shared thread pool" }
    - { left: "Circuit breaker", right: "Calls keep hammering a dependency that is already failing" }
    - { left: "Idempotency key", right: "A retried write must not be applied twice" }
  explanation: "Bulkheads isolate resource pools; circuit breakers fail fast while a dependency is unhealthy; idempotency keys make retries safe."
```

3 to 6 pairs; every left text and every right text must be unique (otherwise matches are ambiguous). Right-hand texts are shuffled.

### `code` - write a function, run against tests

```yaml
- type: code
  difficulty: medium
  prompt: Write `firstUnique(s)` that returns the index of the first character that appears exactly once in `s`, or -1 if there is none. It must run in O(n).
  starter: |
    function firstUnique(s) {
      // your code
    }
  entry: firstUnique
  tests:
    - { args: ["leetcode"], expect: 0 }
    - { args: ["loveleetcode"], expect: 2 }
    - { args: ["aabb"], expect: -1 }
    - { args: [""], expect: -1 }
    - { args: [42], throws: "string" }
    - { args: { js: "['a'.repeat(200000) + 'b']" }, expect: 200000, hidden: true }
  solution: |
    function firstUnique(s) {
      if (typeof s !== 'string') throw new TypeError('s must be a string');
      const counts = new Map();
      for (const c of s) counts.set(c, (counts.get(c) ?? 0) + 1);
      for (let i = 0; i < s.length; i++) if (counts.get(s[i]) === 1) return i;
      return -1;
    }
  kotlin: |
    fun firstUnique(s: String): Int {
        val counts = s.groupingBy { it }.eachCount()
        return s.indexOfFirst { counts[it] == 1 }
    }
  explanation: "Count occurrences in one pass, then scan for the first character whose count is 1. Two passes of O(n) beat a nested loop at O(n squared)."
```

2 to 12 tests, at least one visible. `args` is a JSON array (cloned for every call) or `{ js: "expression" }` evaluated in the sandbox (use it for big inputs). Each test has `expect` (deep-equal) or `throws: "text"` (the error message must contain the text; `""` accepts any error). Hidden tests report only pass/fail, so put performance and edge cases there. The `starter` and `solution` must both define `entry` (`function name`, `const name`, ...). Optional `timeoutMs` (300 to 10 000, default 3000).
`content/content.test.ts` additionally requires, for every code question: **the solution passes every test, the starter does not (and fails cleanly), a Kotlin equivalent exists, and at least one hidden test exists.** The challenge is JavaScript because it runs offline in the browser; the Kotlin version is for transfer to the real interview.

### `open` - explain in your own words (AI-graded, with follow-ups)

```yaml
- type: open
  prompt: Why should a payment API accept an idempotency key, and how would you implement it?
  modelAnswer: >
    Clients retry on timeouts, so without protection a retried POST can charge twice. The client sends a unique idempotency key with the request.
    The server stores the key together with the stored response in the same transaction as the side effect, protected by a unique constraint,
    and returns the stored response when the key is seen again. Keys expire after a retention window, and a reused key with a different payload is rejected.
  keyPoints:
    - point: "Retries (timeouts, network errors) can duplicate a non-idempotent operation"
      keywords: ["retry", "retries", "twice", "duplicate"]
    - point: "The client generates a unique key per logical operation and sends it with the request"
      keywords: ["unique key", "idempotency key", "client generates", "header"]
    - point: "The server stores the key and the result atomically with the side effect, enforced by a unique constraint"
      keywords: ["unique constraint", "same transaction", "atomic", "stores the key"]
  followUps:
    - when: missed
      point: 3
      prompt: Two retries of the same request arrive at the same moment. How do you make sure only one is processed?
      modelAnswer: "Insert the key under a unique constraint inside the transaction that performs the side effect. The second insert fails (or waits on the first), and the server then returns the stored result instead of repeating the work."
      keyPoints:
        - { point: "A unique constraint (or lock) on the key serialises the two requests", keywords: ["unique constraint", "lock", "unique index"] }
        - { point: "The loser returns the stored result rather than failing or repeating the work", keywords: ["stored result", "stored response", "return the same"] }
    - when: covered
      point: 2
      prompt: What should happen if a client reuses a key with a different request body?
      modelAnswer: "Reject it with a conflict or unprocessable-entity error: the key identifies one logical operation, so a different payload is a client bug that must not silently return the old result. Store a fingerprint (hash) of the payload with the key so the server can detect the mismatch."
      keyPoints:
        - { point: "The request is rejected (409 or 422), not answered with the stored result", keywords: ["409", "422", "reject", "conflict"] }
        - { point: "Store a fingerprint of the payload to detect the mismatch", keywords: ["fingerprint", "hash", "compare the payload"] }
```

An `explanation` is optional for open questions (the model answer is shown instead). 2 to 6 `keyPoints`; `format: code` gives a monospace answer box; `maxChars` (default 1200) caps the answer. See [Writing rubrics](#writing-rubrics-and-follow-ups).

## Writing good distractors

A wrong option should be *the answer a learner who has a specific misunderstanding would pick*. Write each distractor from a real misconception, not from filler.

- Start from the explanation: name the misconception behind each wrong option and say it in the explanation ("`also` returns the receiver too, but as `it`"). If you cannot name one, the option is filler; cut it.
- Keep options **parallel** in length, grammar and specificity. The longest, most hedged option is usually the right one in careless questions; do not let that pattern form.
- No joke options, no "none/all of the above", no double negatives, no options that are true but irrelevant to the prompt.
- Avoid absolute words only in wrong options ("always", "never"); mix them across right and wrong. A correct option may say "never" or "only" when that is the fact; a distractor may hedge ("usually", "often").
- A distractor must be **definitely wrong**, not "wrong in most cases". If an expert could argue for it, fix the question.
- For `fill`, distractors should be tokens that are *syntactically plausible* in the blank (`!!` for `?.`), not unrelated words. For `order`, make it possible to get the order wrong in an instructive way (a plausible but incorrect dependency order). For `match`, make every right-hand text plausible for every left item at a glance, differing in the detail that matters.
- Randomisation hides position, so the answer ids (`a`, `b`, ...) carry no meaning; do not reference them in explanations.

### Balanced options

Learners (and interviewers who read the app's questions) learn quickly that the right answer is "the long, careful one". When that holds, the readiness numbers measure test-taking instead of knowledge. An audit of the first version of the course found the correct option was the strictly longest in 86% of single-choice questions (chance for four options is 25%), and that distractors were short strawmen ("Never retry anything", "Only test manually"). Both are now guarded by a test, so write for balance from the start:

- **Same size, same detail.** All options of a question get a similar length (within a factor of about 1.5) and a similar level of specificity: if the answer names a flag, a number or a mechanism, a distractor names one too. Do not let the answer be the only option with a reason clause ("..., because ..."), a code span, a number or a hedge.
- **Move the detail into the `explanation`.** If the correct option needs two lines of conditions, state the essence in the option and put the caveats in the explanation, where every learner sees them.
- **Make distractors real misconceptions** that an engineer could hold and that the explanation then names and refutes ("a `+=` in a loop is fine because the JIT turns it into one `StringBuilder`"). Obviously silly options teach nothing and make the answer guessable by elimination.
- **Vary which option is longest.** Of four options, the answer should be the longest about a quarter of the time, in the middle about half of the time and the shortest about a quarter of the time. After writing a question, look at which option is longest; if it is the answer, lengthen a distractor or trim the answer.
- **Short answers are fine** (`O(n log n)`, `Dispatchers.IO`): then give every option the same shape (`O(n), since ...`) or the same kind of name instead of padding only the wrong ones.
- **Same vocabulary.** Every option should reuse words from the prompt; an option that shares nothing with the question is easy to rule out.
- **Mix absolutes and hedges** across right and wrong options (see above), and in `multi` questions do not let the correct set be exactly the longest options.
- **Same style.** A second audit found that after lengths were balanced the *style* still gave answers away: correct options carried asides in brackets, after a colon or a semicolon and "X or Y" enumerations, while distractors had been padded with "because ..." clauses. Picking "the option with a bracket" scored 54% and a simple word-counting classifier 64% (chance 25%). Write asides, "or" and causal clauses into right and wrong options at the same rate, state a misconception directly instead of justifying it with "because", and do not reuse stock words that lean one way ("trade-off", "depends" only in answers; "immediately", "automatically" only in distractors). Backtick code and complexities (`O(n log n)`): what is inside a code span is not counted as style.

`npm run content:lint` prints the baseline per unit: how often the answer is the strictly longest option and among the two longest, the mean correlation between option length and correctness, absolute words in answers versus distractors, answers that are the only hedged or the most detailed option, options that share no vocabulary with the prompt, the longest-to-shortest ratio, and how often a `multi` answer set is the longest options. Chance for four options is 25% (longest) and 50% (two longest), with a correlation of 0. It then prints the style tells: for asides (`(`, `;`, `:` outside code spans), the word "or", causal clauses (because, since, so that, which) and absolute words, how many correct options and distractors carry each, plus two attackers whose rules were fixed before measuring: the round-2 audit's combined rule (asides and "or" up; causal, absolute and the shortest option down) and a naive Bayes bag-of-words classifier trained on every other question (leave one question out). `npm run content:lint -- --unit spring,jvm` shows the detail for some units and `npm run content:lint -- --offenders` lists the questions where exactly one option differs from the rest on a feature.

`content/balance.test.ts` turns the headline numbers into a ratchet that fails with the list of offending question ids:

| Measure | Limit |
| --- | --- |
| Answer is the strictly longest option, per unit | at most 35% of its single-choice questions (rounded up to a whole question, e.g. 3 of 8), and never above 45% |
| Answer is the strictly longest option, whole course | at most 30% |
| Answer is among the two longest, whole course | at most 70% |
| Mean correlation of option length and correctness | at most 0.3 |
| `multi` answer set equals the longest options | at most 25% of `multi` questions |
| A style feature among correct options vs distractors (rate ratio, "lift"), single and `multi` separately, once the feature is on 15+ options | between 0.55 and 1.6 (for a single, P(correct \| feature) within 25% ± 10 points) |
| "Pick the option with the feature" / "avoid it", each feature, single | at most 35% |
| Combined style rule, single | at most 35% |
| Bag-of-words classifier, leave one question out, single | at most 40% |

Ties do not count as "longest". Never raise a limit to make a build pass: shorten the answer or enrich the distractors in the listed questions, or rewrite the style of the offending options. Changing option text does not affect cards or progress, but never change *which* option is correct or the number of options when you balance a question (see [Changing existing questions](#changing-existing-questions)).

## Writing rubrics and follow-ups

Open questions are graded **per rubric point**: the model decides, for each key point, whether the answer shows it; the score is `covered / total` and the pass mark is 60%. So:

- Make each key point **one independently checkable idea** (a claim a reader can tick), phrased as what a good answer *contains*. Avoid points that overlap or that are implied by another.
- 2 to 6 points; 3 or 4 is usual. Order them roughly as a strong answer would.
- `keywords` are **only** for the offline fallback (a keyword match pre-ticks the self-assessment checklist; the AI ignores them). Give 2 to 5 any-of terms, including common synonyms and plurals. Two tests keep you honest: the **model answer must satisfy its own keywords** (otherwise offline learners are told the perfect answer is incomplete), and a stock "I do not know" must **not** pass (keywords that are too generic, like `the` or `data`, fail it).
- The model answer is shown after answering and must stand alone as a good answer, not a list of keywords.
- **Follow-ups** play the interviewer. The app picks the first `missed` follow-up whose `point` the learner did not cover; if there is none, the first `covered` one whose point they did. So write them in pairs: a `missed` probe that teaches the gap ("Two retries arrive at once. How do you...?") and a `covered` one that pushes deeper ("What if the key is reused with a different body?"). `point` is the 1-based index of the main question's rubric point (the test suite fails if it points past the end). A follow-up has its own prompt, model answer and 2 to 4 key points, and is graded like an open question; the main question passes on the mean of both answers.
- Never hide the key in the question: a good follow-up does not repeat the model answer of the first.

## Difficulty, tags, hints

- `difficulty` is `easy`, `medium` (default) or `hard`. Today it is metadata (carried onto follow-ups); set it honestly so it can drive ordering later. Calibrate against a *senior* interview: `easy` is something a candidate must not miss, `hard` is something that separates strong from adequate.
- `tags` are free words matched by the Library search (an exact tag match scores higher than a text match): use the technology and the concept (`[kotlin, scope-functions]`).
- `hint` is a nudge toward the *reasoning*, never the answer; a test fails if a hint contains the (long enough) literal answer or repeats the explanation. Add hints where the first step is not obvious.

## Fact-checking checklist

Do this for every question before you commit. It is the step that most often finds a problem; recent fact-check passes fixed items about `min.insync.replicas`, cache-aside races, safepoint log formats, gRPC deadlines and test-double terminology.

1. **Try to disprove your own answer key.** Write the strongest counter-argument an expert interviewer would raise. Is there a version, configuration or context where a "wrong" option is correct, or the "right" one is not? If so, add the qualifying words to the prompt ("by default", "in Spring Boot 3", "on the JVM") or change the question.
2. **Check every distractor is wrong and every correct option is right**, one at a time, not just the key. In a `multi`, check the options you left *out* of `answers`.
3. **Run the code.** Anything with code (`code`, `spot`, `fill`, `order`, snippets in `code:`) must compile/run as written. `code` questions are executed by the tests; for Kotlin or Java snippets, paste them into a scratch project (or the Kotlin playground) and check the behaviour you claim, including the printed output.
4. **Version-sensitive claims get a version or a softer verb.** Defaults change (JVM garbage collector, `spring.threads.virtual`, Kafka settings, Java language features, Spring Boot properties). Write "since JDK 21", "by default in Spring Boot 3.x", or avoid the claim; prefer principles over numbers that rot. Name the source (JLS, RFC, official docs) in your review notes.
5. **Numbers**: re-derive them, state units, and make the `estimate` tolerance match the precision of the reasoning.
6. **Wording that can be read two ways is a bug.** Read the prompt as a hostile candidate would. If you needed to look something up to be sure, the question needs a qualifier.
7. **Security content is defensive only.** Teach how to recognise, prevent, detect and respond (validation, authz, least privilege, secrets handling, safe defaults). No working exploits, payloads or step-by-step attack instructions; a short name or a vulnerable snippet for a `spot` question is fine, a weaponised one is not. The same applies to prompt-injection items: teach the defence (untrusted data in delimiters, least privilege tools, output checks), not a recipe.
8. **Names and tone**: use the standard names (Little's law, CAP, idempotency); no jokes at the expense of the learner; explanations say *why*, not only *what*.
9. **Rubric check**: read your `modelAnswer` as the grader would; does it cover every key point, and nothing the rubric does not mention?
10. Run `npm run content:check && npm run content:lint && npx vitest run content`.

## Changing existing questions

Spaced-repetition cards are tied to the question's **id (position)** and a **hash of its type and prompt**:

- Rewriting a prompt drops learners' cards for that question (they see it as new). That is the right outcome when the meaning changed, so do rewrite when the *question* changes; do not churn prompts for cosmetic reasons.
- Fixing an option, the explanation or a typo does not affect cards.
- **Do not insert, delete or reorder questions inside an existing lesson** unless you accept that every card after the change is dropped (the hash no longer matches the id). Append new questions at the end of a lesson instead, or add a new lesson.
- Do not rename a lesson id: it carries completion records and stars.

## Adding a unit or a lesson

**A lesson in an existing unit:** append to its `lessons:` list in the unit file.

```yaml
id: demo
title: Demo unit
emoji: "🧪"
color: "#1cb0f6"
description: A tiny unit used to show the file layout.
lessons:
  - id: demo-basics
    title: Basics
    questions:
      - type: single
        prompt: Which keyword declares a read-only local variable in Kotlin?
        options:
          - { id: a, text: "`val`" }
          - { id: b, text: "`var`" }
          - { id: c, text: "`const`" }
        answer: a
        explanation: "`val` declares a read-only reference. `var` is mutable, and `const` only applies to top-level or object `val`s that are compile-time constants."
      - type: multi
        prompt: Which of these are valid ways to avoid a `NullPointerException` in Kotlin?
        options:
          - { id: a, text: "Declare the type as nullable and use `?.`" }
          - { id: b, text: "Use `!!` everywhere" }
          - { id: c, text: "Provide a default with `?:`" }
          - { id: d, text: "Use `requireNotNull` with a message" }
        answers: [a, c, d]
        explanation: "`?.`, `?:` and `requireNotNull` handle absence explicitly; `!!` just moves the exception to the call site."
      - type: open
        prompt: When would you choose `lateinit` over a nullable type?
        modelAnswer: "When a non-null property cannot be given a value in the constructor but is guaranteed to be set before use, for example by a framework or a test setup method, so that callers are spared null checks."
        keyPoints:
          - { point: "The value is not available at construction time", keywords: ["constructor", "not available", "later"] }
          - { point: "It is guaranteed to be initialised before it is read (framework or setup)", keywords: ["framework", "dependency injection", "setup", "before use"] }
```

**A new unit:** add `NN-name.yaml` (the number sets the order; files sort by name), with the fields shown above. Then:

1. `npm run content:check` - validates the schema and the cross-field rules and prints the counts.
2. `npx vitest run content` - the content tests: open rubrics, code solutions, hints, mix of types.
3. If the unit should weigh into a target role, add its id to `ROLE_WEIGHTS` in `shared/src/mastery.ts`.
4. `npm run dev` and play it: read every question on a phone-sized window, and read each explanation after a deliberately wrong answer.
5. `npm run e2e` (the suite derives its expectations from the YAML; it plays every lesson perfectly). Update screenshots only if the UI changed.
6. Update the question and lesson counts in the README table if they are mentioned there.

When the API server is running, restart it after editing YAML: it reads the course once at start-up to look rubrics up by id. The dev web server reloads on its own.
