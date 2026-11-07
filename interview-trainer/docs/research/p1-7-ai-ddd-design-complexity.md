# P1-7: fact-check of the AI, DDD, system design and complexity units

Date: 2026-11-07. Scope: `content/08-ai.yaml`, `content/05-ddd.yaml`, `content/11-system-design.yaml`, `content/07-complexity.yaml`.
Method: every concrete or version-sensitive claim was checked against an official or primary source fetched on 2026-11-07 (provider documentation, the JDK source and Javadoc, the OWASP project, RFCs, the original papers or author texts). Question ids are `<lesson-id>/<position>`.
Verdicts: confirmed (no change), fixed (wrong or ambiguous text corrected), softened (true only in some cases or unsourced detail hedged or removed), key changed (answer key altered; none were needed), could not verify.

The AI unit already avoids naming models and quoting limits, prices or TTLs as facts (the one price question uses explicitly made-up prices), so the changes there are mostly explanations that now carry the caveat the provider documentation states.

## Sources

| Key | Source |
| --- | --- |
| S1 | Anthropic prompt caching: https://platform.claude.com/docs/en/build-with-claude/prompt-caching |
| S2 | OpenAI prompt caching: https://developers.openai.com/api/docs/guides/prompt-caching |
| S3 | Anthropic structured outputs: https://platform.claude.com/docs/en/build-with-claude/structured-outputs |
| S4 | Anthropic model migration guide (sampling parameters, temperature 0): https://platform.claude.com/docs/en/models/opus-5-5/migration-guide |
| S5 | OWASP Top 10 for LLM Applications 2025: https://genai.owasp.org/llm-top-10/ and https://genai.owasp.org/llmrisk/llm01-prompt-injection/ |
| S6 | Zheng et al., "Judging LLM-as-a-Judge with MT-Bench and Chatbot Arena": https://arxiv.org/abs/2306.05685 |
| S7 | Simon Willison, "The lethal trifecta": https://simonwillison.net/2025/Jun/16/the-lethal-trifecta/ |
| S8 | Anthropic, "Contextual Retrieval" (BM25 plus embeddings, reranking): https://www.anthropic.com/news/contextual-retrieval |
| S9 | Liu et al., "Lost in the Middle": https://arxiv.org/abs/2307.03172 |
| S10 | Anthropic pricing: https://platform.claude.com/docs/en/about-claude/pricing |
| S11 | Claude Code memory (CLAUDE.md): https://code.claude.com/docs/en/memory |
| S12 | Kotlin all-open and no-arg plugins: https://kotlinlang.org/docs/all-open-plugin.html and https://kotlinlang.org/docs/no-arg-plugin.html |
| S13 | JDK 21 `HashMap.java`: https://raw.githubusercontent.com/openjdk/jdk/jdk-21-ga/src/java.base/share/classes/java/util/HashMap.java ; JEP 180: https://openjdk.org/jeps/180 |
| S14 | JDK 21 Javadoc: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/ (HashMap, ArrayList, ArrayDeque, PriorityQueue, TreeMap, Collections, Arrays) |
| S15 | Kotlin `ArrayDeque`: https://kotlinlang.org/api/core/kotlin-stdlib/kotlin.collections/-array-deque/ ; `require`/`check`: https://kotlinlang.org/api/core/kotlin-stdlib/kotlin/check.html |
| S16 | Kafka `KafkaConsumer` Javadoc: https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html |
| S17 | Vernon, "Effective Aggregate Design" parts 1 to 3: https://www.dddcommunity.org/wp-content/uploads/files/pdf_articles/Vernon_2011_1.pdf (and `_2`, `_3`) |
| S18 | Microsoft, domain events vs integration events: https://learn.microsoft.com/en-us/dotnet/architecture/microservices/microservice-ddd-cqrs-patterns/domain-events-design-implementation |
| S19 | Richardson, Saga pattern: https://microservices.io/patterns/data/saga.html |
| S20 | RFC 9111 (HTTP caching): https://datatracker.ietf.org/doc/html/rfc9111 ; RFC 6585 (429, Retry-After): https://datatracker.ietf.org/doc/html/rfc6585 |
| S21 | PACELC: https://en.wikipedia.org/wiki/PACELC_design_principle (secondary, summarises Abadi 2012); Token bucket: https://en.wikipedia.org/wiki/Token_bucket ; Consistent hashing: https://en.wikipedia.org/wiki/Consistent_hashing |
| S22 | Fowler, MonolithFirst: https://martinfowler.com/bliki/MonolithFirst.html |

## 08-ai.yaml

| Question | Claim | Verdict | Source | Change |
| --- | --- | --- | --- | --- |
| ai-coding/1 | String-built SQL is injection; parameterised queries are the fix | confirmed | S5 (LLM05 Improper Output Handling, LLM01 context); standard secure-coding guidance | none |
| ai-coding/2 | Tight feedback loops (tests, compiler errors) help assistants correct themselves; huge diffs are unreviewable | confirmed | general engineering practice, no versioned claim | none |
| ai-coding/3 | Models produce plausible, unverified API names; compile and check docs; naming a version reduces but cannot rule out hallucination | confirmed | S9 for the general weakness; `chunked(n)` exists in the Kotlin stdlib (https://kotlinlang.org/api/core/kotlin-stdlib/kotlin.collections/chunked.html) | none; "Kotlin 2.0" is only an example version and a real release |
| ai-coding/4 | Long contexts degrade; start fresh with a summary and keep standing rules in project instructions | confirmed | S9 (performance degrades with position and length of context) | none |
| ai-coding/4 | "CLAUDE.md-style project instructions" exists as a mechanism | confirmed | S11 | none |
| ai-coding/5 | Tests written by the same assistant share the code's assumptions; coverage and green runs do not prove the requirement | confirmed | testing principle, no versioned claim | none |
| ai-coding/6 | Pasting customer data into a public tool is a disclosure regardless of later deletion | confirmed | data-protection principle (no product-specific claim) | none |
| ai-coding/7 | Java to Kotlin traps: default-final classes need Spring/JPA compiler plugins, platform types, data class equality, SAM | confirmed | S12 (`kotlin-spring` opens Spring-annotated classes; `kotlin-jpa` adds no-arg constructors for JPA) | none |
| ai-llm-apps/1 | RAG grounds the model in retrieved documents; reduces but does not eliminate hallucination | confirmed | S8 (retrieval quality drives answers; failures remain) | none |
| ai-llm-apps/2 | Vector stores do approximate nearest-neighbour search; production systems combine with BM25 and filters | confirmed | S8 ("embedding models ... can miss crucial exact matches", BM25 fixes that) | none |
| ai-llm-apps/3 | Mitigations: untrusted-data separation, least privilege, human approval for risky actions, output validation; the system prompt alone is not a defence | confirmed | S5 LLM01:2025 mitigation list (constrain behaviour, validate output formats, least privilege, human approval, segregate external content) and its statement that it is unclear if fool-proof prevention exists | explanation now cites OWASP LLM01 (2025 Top 10) |
| ai-llm-apps/4 | Compare prompts on an eval set; judge models need calibration against humans | confirmed | S6 (judges agree with humans over 80% but have biases) | none |
| ai-llm-apps/5 | Prompt caching works on identical prefixes; static content first; timestamps in the prefix break it | confirmed | S1 ("100 percent identical prompt segments" up to the breakpoint; order tools, system, messages), S2 ("timestamps ... place those at the end") | none to the key |
| ai-llm-apps/5 | Cache entries persist (implicit) | softened | S1 (5-minute default lifetime, 1-hour option), S2 (about 5 to 10 minutes inactive, or up to 24 hours depending on model and settings) | explanation now says entries expire after a provider-specific time (minutes by default); no numbers quoted |
| ai-llm-apps/5 | The 20,000-token document is above the minimum cacheable length | confirmed | S1 (minimums between 512 and 4,096 tokens depending on model), S2 (1,024) | none |
| ai-llm-apps/6 | Schema-constrained output removes most format errors; validate semantically and keep a fallback | confirmed | S3 (constrained decoding guarantees schema compliance) | none to the key |
| ai-llm-apps/6 | Constrained output can still fail | softened | S3 (a refusal, `stop_reason: "refusal"`, or a response cut off at `max_tokens` may not match the schema) | explanation adds the refusal and truncation cases |
| ai-llm-apps/7 | Fake the model behind an interface in unit tests; measure quality in a separate eval suite | confirmed | engineering practice | none |
| ai-llm-apps/8 | Definitions of RAG, embeddings, prompt caching ("lower cost and latency"), eval set, prompt injection | confirmed | S1 (caching "to cut costs and latency"), S2, S8 | none |
| ai-llm-apps/9 | Chunking with overlap helps retrieval; `overlap >= size` makes the step non-positive | confirmed | S8 (chunking, contextual retrieval); code is executed by the content tests | none |
| ai-llm-apps/10 | Guardrails for a refund bot: server-side authorisation, caps, human approval, schema validation, audit, adversarial tests | confirmed | S5 (privilege control, human approval, validate outputs, red-teaming) | none |
| ai-advanced/1 | The agent loop and its limits belong in orchestration code, not the prompt or temperature | confirmed | S5 LLM06 Excessive Agency and LLM10 Unbounded Consumption (limits and controls outside the model) | none |
| ai-advanced/2 | Injection is dangerous when untrusted content, private data and an exfiltration channel meet | confirmed | S7 (the "lethal trifecta") | none |
| ai-advanced/3 | Hybrid BM25 plus vector search, re-ranking, structure-aware chunks with titles, recall@k measured separately; noisy context hurts | confirmed | S8 (BM25 plus embeddings fewer failed retrievals; reranking improves further; context prepended to chunks), S9 | none |
| ai-advanced/3 | Raising the temperature or stuffing the whole collection are not quality practices | confirmed | S9, general | none |
| ai-advanced/4 | LLM-judge practice: calibrate to human labels, swap order for pairwise comparisons, rubrics, deterministic checks where possible; same model as sole judge is a bias | confirmed | S6 (position, verbosity and self-enhancement biases; swapping positions as mitigation) | none to the key |
| ai-advanced/4 | Judges "tend to prefer the first (or last) option, longer answers, and their own family" | softened | S6 names the three biases but reports them as observed tendencies with limited data | explanation now names position, verbosity and self-enhancement bias as study findings instead of stating tendencies as certain |
| ai-advanced/5 | Arithmetic: 3,000 x $1/1M + 500 x $5/1M = $0.0055; x 1,000,000 x 0.7 = $3,850 | confirmed | recomputed; prices are explicitly made up | none |
| ai-advanced/5 | "Output tokens are priced several times higher than input tokens" | softened | S10 (the listed output price is a multiple of the input price; varies by provider and model) | now "typically ... several times ... (check the current price list)" and "can dominate" |
| ai-advanced/6 | Temperature 0 does not guarantee identical output; "most providers ignore temperature 0" is false | confirmed | S4 ("temperature = 0 ... never guaranteed identical outputs") | none to the key |
| ai-advanced/6 | Temperature is always a settable parameter | softened | S4 (current models reject `temperature`, `top_p`, `top_k` with a 400 error) | explanation says some current models reject a temperature setting and cites provider documentation; no model named |
| ai-advanced/7 | Pipeline order: constrain, parse with bounded retry, validate semantics and permissions, then side effect with idempotency key | confirmed | S3, S5 (validate output before acting) | none |
| ai-advanced/8 | Brace matching must respect JSON strings and escapes; greedy to-the-last-brace breaks | confirmed | executed by the content tests (solution passes, starter fails) | none |
| ai-advanced/9 | RAG debugging: split retrieval from generation, recall@k and MRR, hybrid, re-ranking, groundedness checks | confirmed | S8 | none |

## 05-ddd.yaml

| Question | Claim | Verdict | Source | Change |
| --- | --- | --- | --- | --- |
| ddd-tactical/1 | Money is a value object; customers, orders, accounts are entities | confirmed | Evans, *Domain-Driven Design* (2003), ch. 5; Vernon | none |
| ddd-tactical/2 | Reference other aggregates by identity; modify one aggregate per transaction; changes through the root; repository per root; small aggregates | confirmed | S17 ("Rule: Reference Other Aggregates By Identity", "Design Small Aggregates", one aggregate per transaction) | none to the key |
| ddd-tactical/2 | "One aggregate per transaction" is a rule | softened | S17: "a rule of thumb and should be the goal in most cases", with a section "Reasons To Break the Rules" | explanation labels both as Vernon's rules of thumb and notes the documented exceptions |
| ddd-tactical/3 | Anemic domain model (Fowler) | confirmed | https://martinfowler.com/bliki/AnemicDomainModel.html (name and definition) | none |
| ddd-tactical/4 | `require` throws `IllegalArgumentException`, `check` throws `IllegalStateException`; `assert` needs `-ea` | confirmed | S15 | none |
| ddd-tactical/5 | Application services orchestrate the use case and own the transaction | confirmed | Evans ch. 4 (application layer); S18 | none |
| ddd-tactical/6 | Repository interface belongs to the domain layer (dependency inversion) | confirmed | Evans ch. 6; hexagonal architecture (Cockburn) | none |
| ddd-tactical/7 | Mutating `items` and `status` from a service bypasses the root's invariants | confirmed | S17 (all changes through the root) | none |
| ddd-tactical/8 | Value object is immutable, compared by value, returns a new instance | confirmed | Evans ch. 5 | none |
| ddd-tactical/8 | "Keep money in integer cents" | softened | ISO 4217 minor units differ by currency (0, 2 or 3 digits) | explanation says integer minor units (cents here) and that the digit count depends on the currency |
| ddd-tactical/9 | Value object definition and benefits (invariants at construction, primitive obsession) | confirmed | Evans ch. 5; Fowler | none |
| ddd-strategic/1 | A bounded context owns its own model; share only an identifier | confirmed | Evans ch. 14 | none |
| ddd-strategic/2 | Anti-Corruption Layer translates between models | confirmed | Evans ch. 14 | none |
| ddd-strategic/3 | Context-mapping patterns: Shared Kernel, Conformist, Open Host Service, Published Language, Customer/Supplier, Partnership, ACL, Separate Ways; no "Singleton Kernel" | confirmed | Evans, *DDD Reference* (2015) and ch. 14 | none |
| ddd-strategic/4 | Integration events are a published, versioned contract; domain events are internal | confirmed | S18 ("integration events ... other microservices, Bounded Contexts or external applications") | none |
| ddd-strategic/5 | Outbox solves the dual-write problem; relay delivers at least once; `acks=all` cannot help before publish | confirmed | https://microservices.io/patterns/data/transactional-outbox.html ; S16 (at-least-once with commit after processing) | none |
| ddd-strategic/6 | Start from bounded contexts and business capabilities | confirmed | Evans; Richardson | none |
| ddd-strategic/6 | "Starting coarser and splitting later is cheaper than merging prematurely split services" | softened | S22 (monolith first; "almost all" successful stories, which is an observation, not a law) | now "usually cheaper" |
| ddd-strategic/7 | Match of ACL, Shared Kernel, Conformist, Open Host Service | confirmed | Evans ch. 14 | none |
| ddd-strategic/8 | One service per table is wrong; an aggregate is the consistency boundary and is not split across services | confirmed | S17, S22, Evans | none |
| ddd-strategic/8 follow-ups | Outbox plus idempotent consumer, eventual consistency, saga compensation; aggregates as small as invariants allow | confirmed | S17, S19 | none |
| ddd-advanced/1 | Command, query, domain event (past tense, internal), integration event (public contract) | confirmed | Evans, Young (CQRS), S18 | none |
| ddd-advanced/2 | Fix event-sourced history with a compensating event, never edit stored events | confirmed | Young, "CQRS Documents"; Fowler, Event Sourcing | none |
| ddd-advanced/3 | Read-your-writes in CQRS with asynchronous projections: return the result, pending state, version token | confirmed | CQRS literature (Young; Microsoft CQRS pattern docs) | none |
| ddd-advanced/4 | Orchestration vs choreography; compensations are semantic undo; sagas lack isolation, countermeasures such as semantic locks | confirmed | S19 ("Lack of isolation (the I in ACID) ... countermeasures", choreography vs orchestration definitions) | none |
| ddd-advanced/5 | Sending internal domain events straight to a broker and publishing inside a DB transaction are both problems; use integration events and an outbox | confirmed | S18, S19 | none |
| ddd-advanced/6 | Specification pattern; `&&` short-circuits | confirmed | Evans ch. 9; Kotlin language spec | none |
| ddd-advanced/7 | Modular monolith: private tables, public API or events, boundaries verified by ArchUnit or Spring Modulith | confirmed | https://docs.spring.io/spring-modulith/reference/verification.html (module verification) ; ArchUnit | none |
| ddd-advanced/8 | Snapshot arithmetic: 2,300 - 2,000 = 300 | confirmed | recomputed | none |
| ddd-advanced/9 | Event sourcing trade-offs: upcasting, snapshots, eventual consistency, GDPR and crypto-shredding | confirmed | Young; Fowler | none |
| ddd-advanced/9 | "An audit table or CDC gives that far more cheaply" | softened | judgement, not a measured fact | now "usually ... more cheaply" |
| ddd-advanced/9 follow-ups | Upcasters, additive changes, key-per-subject deletion | confirmed | Young; event-store practice | none |

## 11-system-design.yaml

| Question | Claim | Verdict | Source | Change |
| --- | --- | --- | --- | --- |
| sd-estimation-traffic/1 | 10^9 / 86,400 = 11,574 req/s; 1M per day is about 12 req/s | confirmed | recomputed (11.57) | none |
| sd-estimation-traffic/1 | Peak is "often 2 to 5 times the average" | softened | no authoritative source for that range; ratios vary by product | now "often several times the average, more for spiky products; measure your own ratio" |
| sd-estimation-traffic/2 | 6,000 / (500 x 0.6) = 20 instances | confirmed | recomputed | none |
| sd-estimation-traffic/3 | Token bucket: capacity bounds the burst, refill rate bounds the average; idle client has a full bucket (10), not 300 tokens | confirmed | S21 token bucket ("average rate up to the rate tokens are added, burstiness determined by the depth") | none |
| sd-estimation-traffic/4 | Fixed window allows up to 2x the limit around a boundary; sliding window log is exact; sliding window counter weights the previous window; token bucket bursts; leaky bucket drains at constant rate | confirmed | S21 (token vs leaky bucket comparison); sliding-window variants are standard definitions, not re-fetched | none |
| sd-estimation-traffic/5 | Token bucket refill: refill, record time, spend a token; update the clock on rejection too | confirmed | S21; code is a standard lazy-refill implementation | none |
| sd-estimation-traffic/6 | Sliding window with half-open window `(t - w, t]`, only allowed requests counted | confirmed | executed by the content tests | none |
| sd-estimation-traffic/7 | Least outstanding requests adapts to variable request cost; round robin assumes equal cost; IP hash gives stickiness, not balance | confirmed | concept-level and vendor-neutral (also known as least connections); no vendor page fetched | none |
| sd-estimation-traffic/8 | `Cache-Control: public` lets shared caches store a personalised response; `max-age` is in seconds; CDNs can cache JSON; `Content-Length` is not required | confirmed | S20 RFC 9111 (`public`, `private`, delta-seconds) | none |
| sd-estimation-traffic/9 | Rate limiter: shared atomic state (Redis INCR or Lua), 429 with `Retry-After`, fail open or closed per endpoint | confirmed | S20 RFC 6585 (429 "MAY include a Retry-After header") | none |
| sd-data-failure/1 | PACELC: if Partition choose A or C, Else choose L or C | confirmed | S21 (Abadi 2012, via secondary summary) | none |
| sd-data-failure/1 | Option "replicated writes trade latency against consistency on every request" | softened | S21: the ELC trade-off applies in the absence of partitions, not literally on every request | option now "... replicated systems trade latency against consistency in normal operation" |
| sd-data-failure/2 | `hash % N`, N from 4 to 5: 80% of keys move; consistent hashing moves about 1/5 | confirmed | recomputed (4 of every 20 hash values keep their node); S21 consistent hashing ("1/n fraction" relocates) | none |
| sd-data-failure/3 | Ring lookup by binary search; `TreeMap.ceilingEntry`; virtual nodes | confirmed | S14 (TreeMap log(n)); executed by the content tests | none |
| sd-data-failure/4 | A hot key lives on one shard; more shards do not help; replicate the key or use a local near cache | confirmed | standard sharding practice; consistent with S21 (mapping is per key) | none |
| sd-data-failure/5 | Read-your-writes by routing the writer's reads to the primary or an up-to-date replica | confirmed | Kleppmann, *Designing Data-Intensive Applications*, ch. 5 (replication lag) | none |
| sd-data-failure/6 | Cache-aside, stampede, write-through, write-behind, TTL semantics | confirmed | standard caching definitions; no versioned claim | none |
| sd-data-failure/7 | `commitSync()` commits the offsets returned by the last `poll()`, so committing before processing gives at-most-once for the batch; commit after processing gives at-least-once; order only within a partition | confirmed | S16 ("Commit offsets returned on the last poll()"; at-least-once description) | none |
| sd-data-failure/8 | Read-your-writes, monotonic reads, linearizability, eventual consistency, R + W > N | confirmed | Kleppmann ch. 5 and 9; Dynamo paper (quorum) | none |
| sd-data-failure/9 | Read-heavy scaling order: measure, cache, replicas, shard; follow-ups on lag and shard keys | confirmed | Kleppmann ch. 5 to 6 | none |

## 07-complexity.yaml

| Question | Claim | Verdict | Source | Change |
| --- | --- | --- | --- | --- |
| complexity-reading/1 | Nested loops over pairs: n(n-1)/2 comparisons, O(n^2) | confirmed | analysis | none |
| complexity-reading/2 | `HashSet` pass is O(n) time, O(n) space | confirmed | S14 (HashMap constant-time basic operations) | none |
| complexity-reading/2 | "Sorting first gives O(n log n) time with O(1) extra space" | softened | in-place heapsort yes, but library sorts (TimSort for objects, dual-pivot quicksort for primitives) are not O(1) auxiliary memory in general | explanation now says O(1) with an in-place sort such as heapsort, and that library sorts may use more |
| complexity-reading/3 | Doubling loop is O(log n) | confirmed | analysis | none |
| complexity-reading/4 | n iterations x log n halvings is O(n log n) | confirmed | analysis | none |
| complexity-reading/5 | Naive `fib` is O(2^n) (tight bound about 1.618^n); memoisation gives O(n) | confirmed | analysis | none |
| complexity-reading/6 | `a.filter { it in b }` with `b` a List is O(n m); a set makes it O(n + m) | confirmed | analysis; S14 (list contains is linear) | none |
| complexity-reading/7 | Repeated `s += x` is O(n^2); the JIT does not merge appends across iterations; `buildString` is amortised O(1) per append | confirmed | known HotSpot and Kotlin compiler behaviour (string concatenation is optimised within a single expression only); not re-fetched | none |
| complexity-reading/8 | Two-sum with a hash map is O(n) expected | confirmed | analysis | none |
| complexity-reading/9 | n log2 n for n = 10^6 is about 19.9 million; n^2 / 2 is 5 x 10^11 | confirmed | recomputed | none |
| complexity-reading/10 | Stack-based bracket matching is O(n); repeated pair deletion is O(n^2) | confirmed | analysis; executed by the content tests | none |
| complexity-reading/11 | `firstDuplicate` with `HashSet.add` returning false | confirmed | S14 (`Set.add` contract) | none |
| complexity-collections/1 | `ArrayList` gives O(1) indexed access; `LinkedList.get(i)` is O(n) | confirmed | S14 (ArrayList: "linear time (roughly speaking)" for others, constant for get) | none |
| complexity-collections/1 | "`ArrayDeque`, by index" is not an O(1) option | fixed | S15: Kotlin's `kotlin.collections.ArrayDeque` implements `MutableList` and "supports efficient get/set operations by index"; in Kotlin code a bare `ArrayDeque` is that class, so the old distractor was arguably correct. Only `java.util.ArrayDeque` has no index access (S14) | option now "java.util.ArrayDeque, by index"; explanation notes the Kotlin class. Answer key unchanged (ArrayList). Also "slower ... for nearly everything" softened to "for most workloads" |
| complexity-collections/2 | Java 8 `HashMap` treeifies colliding bins: threshold 8, table at least 64 | confirmed | S13: `TREEIFY_THRESHOLD = 8`, `MIN_TREEIFY_CAPACITY = 64`, `UNTREEIFY_THRESHOLD = 6`; JEP 180 (Java 8) | none to the key |
| complexity-collections/2 | "a bucket with 8+ entries ... becomes a red-black tree" | fixed | S13: `putVal` calls `treeifyBin` when `binCount >= TREEIFY_THRESHOLD - 1`, i.e. when a bin that already has 8 nodes receives a 9th ("adding an element to a bin with at least this many nodes"); when the table has fewer than 64 slots it resizes instead | explanation now says a bucket that already holds 8 entries and receives another, if the table has at least 64 slots (otherwise it resizes); cites JEP 180 |
| complexity-collections/2 | O(log n) lookups for Comparable keys | confirmed | S14 HashMap ("when keys are Comparable, this class may use comparison order among keys to help break ties") | none |
| complexity-collections/3 | `ArrayList.add(x)` amortised O(1); `add(0, x)` O(n) | confirmed | S14 ArrayList ("add operation runs in amortized constant time") | none |
| complexity-collections/4 | `TreeMap.get` and `Collections.binarySearch` on an `ArrayList` are O(log n); `PriorityQueue.peek` O(1); `HashMap.get` expected O(1); `ArrayDeque.addFirst` amortised O(1) | confirmed | S14: TreeMap "guaranteed log(n)", `binarySearch` "log(n) time for a random access list", PriorityQueue "constant time for the retrieval methods (peek, element, size)", ArrayDeque "most operations run in amortized constant time" | none |
| complexity-collections/5 | `removeAt(0)` in a loop is O(n^2); use an `ArrayDeque` | confirmed | S14, S15 | none |
| complexity-collections/6 | `groupingBy{}.eachCount()` and `sortedWith(comparator)` | confirmed | Kotlin stdlib API (`Grouping.eachCount`, `Iterable.sortedWith`) | none |
| complexity-collections/7 | `x !in out` on a list is linear; `distinct()` uses a hash set | confirmed | S14 | none |
| complexity-collections/8 | HashMap expected O(1); LinkedHashMap insertion or access order and `removeEldestEntry` for LRU; TreeMap sorted O(log n) with navigation methods; ConcurrentHashMap for concurrency | confirmed | S14 | none |
| complexity-advanced/1 | Sliding window with two monotonic pointers is O(n) (amortised) | confirmed | analysis | none |
| complexity-advanced/2 | Longest substring with at most k distinct characters in O(n) | confirmed | executed by the content tests | none |
| complexity-advanced/3 | log2(10^9) is about 29.9, so 30 halvings (2^30 = 1,073,741,824) | confirmed | recomputed | none |
| complexity-advanced/4 | Koko binary search: monotonic predicate, O(n log max); Kotlin `piles.max()` returns a non-null Int | confirmed | executed by the content tests; Kotlin stdlib `max()` (non-null since Kotlin 1.7, throws on empty) | none |
| complexity-advanced/5 | Technique matches: sliding window, binary search on the answer, union-find (near-constant amortised with path compression and union by size), Kahn's algorithm, trie, min-heap of size k | confirmed | standard algorithm texts (CLRS, Sedgewick) | none |
| complexity-advanced/6 | Kahn's algorithm is O(n + edges) and iterative; recursive DFS can overflow the stack | confirmed | executed by the content tests | none |
| complexity-advanced/7 | Coin-change DP is O(amount x coins) | confirmed | analysis; executed by the content tests | none |
| complexity-advanced/8 | k largest with a min-heap of size k: O(n log k) time, O(k) space | confirmed | S14 (PriorityQueue O(log n) offer and poll) | none |
| complexity-advanced/9 | Top 10 of a huge log: hash-map count, min-heap, hash-partition shards, count-min sketch overestimates only | confirmed | Cormode and Muthukrishnan, "An improved data stream summary: the count-min sketch" (2005) | none |
| complexity-advanced/9 follow-ups | Count-min sketch rows, minimum of counters, overestimation; hash partition keeps one key's count in one shard | confirmed | same | none |

## Could not verify directly

- Evans (book, 2003; *DDD Reference*, 2015), Young and Fowler on CQRS and event sourcing, Kleppmann, CLRS, the Dynamo paper, the count-min sketch paper and the HotSpot string-concatenation behaviour were checked from knowledge of the standard texts, not re-fetched; no claim in these units is version-dependent.
- PACELC is cited through a secondary summary of Abadi (2012); the original paper was not fetched.
- "Least outstanding requests" (sd-estimation-traffic/7) was not checked against a vendor page; the concept (route to the instance with the fewest in-flight requests) is vendor-neutral.
- OpenAI temperature and determinism documentation was not fetched; the claim was verified from Anthropic's documentation (S4), and the question is provider-neutral.
- Network note: direct page fetches of several hosts failed until the session's allowlist was extended; pages were fetched with `curl` through the session proxy and read as text. Nothing was taken from search snippets alone except the OWASP list ordering, which was re-checked against the OWASP page itself (S5).

## Summary

No answer key was wrong. Two option texts changed (complexity-collections/1 distractor made unambiguous for Kotlin; sd-data-failure/1 "on every request" removed). Everything else is explanation or rubric wording that now carries the caveat the source states, or drops an unsourced figure.
