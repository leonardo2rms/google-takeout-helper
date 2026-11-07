# P1-7: version-sensitive claims in JVM, data, messaging and scale

Fact-check of `content/04-jvm.yaml`, `09-data.yaml`, `10-messaging.yaml` and `06-scale.yaml` against official sources. Date of the check: 2026-11-07.

## Versions current at the time of the check

| Product | Version | Where it was read |
| --- | --- | --- |
| JDK | 25 is the current LTS (GA 2025-09-16); Oracle's downloads page lists JDK 27 as the newest feature release | Oracle Java SE support roadmap and downloads page; openjdk.org/projects/jdk/25 |
| Apache Kafka | 4.2.2 (2026-09-29) on the downloads page; KRaft only since 4.0; the search snapshot also mentioned 4.3/4.4 pages, which could not be reconciled | kafka.apache.org downloads and 4.0 / 4.2 release announcements |
| Hibernate ORM | 7.4.15.Final (2026-11-06) is the latest stable | hibernate.org/orm/releases |
| Flyway | 13.12.1 (2026-11-05); engine release notes list 13.12.0 (2026-10-30) | documentation.red-gate.com release notes; Redgate Flyway Community page |
| PostgreSQL | docs for 18 are "current" | postgresql.org/docs/current |

## How the sources were reached (limits of this check)

The session's network allowed only a few hosts. `openjdk.org`, `docs.oracle.com`, `kafka.apache.org`, `postgresql.org`, `documentation.red-gate.com`, `hibernate.org` and `docs.spring.io` could not be fetched directly (DNS refused). Two routes were used instead:

1. **Primary sources on the code host** (fetched in full and grepped): OpenJDK source at the `jdk-21-ga`, `jdk-23-ga`, `jdk-24-ga` and `jdk-25-ga` tags; Apache Kafka source (trunk) for config defaults and log messages; Flyway source; the PostgreSQL documentation sources; Resilience4j, HikariCP, grpc.io and RabbitMQ website sources.
2. **The web-search tool restricted to the official domains**, which returns the text of the official pages (JEP 122/363/444/474/490/491, Oracle GC tuning guide, Kafka docs and KIPs, Redgate Flyway docs, Hibernate and Spring docs, MySQL manual, Liquibase docs). Rows marked "search" rest on this route only.

Kafka source read was `trunk` (a later head than 4.2.2); every default quoted is the same in the 4.x documentation the search returned. Nothing was answered from memory; where neither route confirmed a point it is marked "could not verify".

## Changes that matter most

- **Virtual-thread pinning (jvm-runtime/5): the old answer is only right on JDK 21 to 23.** JEP 491 shipped in JDK 24, so on JDK 24 and the JDK 25 LTS a virtual thread blocked in `synchronized` no longer pins its carrier. The answer key was **not** changed (a is still correct for JDK 21), but the prompt now says "On JDK 21 (before JEP 491 arrived in JDK 24)" and the explanation states what holds on 21, on 24+ and what pinning still means today (native frames and foreign calls, class initialisers; JFR event `jdk.VirtualThreadPinned`; `jdk.tracePinnedThreads` removed). The prompt change drops learners' cards for this one question, which is intended.
- **Kafka poll-timeout log (msg-kafka/4)** replaced by the client's real wording (the old lines were invented and carried a "412 s" figure that Kafka does not log).
- **Exactly-once (msg-patterns/2):** "consumers read only committed data" is true only with `isolation.level=read_committed`; the default is `read_uncommitted`.
- **Lost-update option d (data-sql/7):** "default level (READ COMMITTED)" is PostgreSQL's default; MySQL's is REPEATABLE READ. Qualified, key unchanged.
- No answer key was changed in any of the four files.
- **Flyway (data-jpa/5):** the answer was right; the explanation now states the default `validateOnMigrate=true`, what `repair` does and the repeatable-migration exception.

## Claims checked

Verdicts: **confirmed** (source agrees, no edit), **fixed** (wrong or misleading, edited), **softened** (true only from a version or in some contexts, qualifier added), **key changed** (none), **could not verify**.

### 04-jvm.yaml

| Question id | Claim | Verdict | Source | Change |
| --- | --- | --- | --- | --- |
| jvm-memory/2 | Since Java 8 class metadata lives in Metaspace in native memory, PermGen is gone | confirmed | JEP 122 (search); `globals.hpp` jdk-25-ga | none |
| jvm-memory/2 | Metaspace is limited only by `-XX:MaxMetaspaceSize` | softened | `MaxMetaspaceSize` default `max_uintx`, `CompressedClassSpaceSize` default 1 GB (`globals.hpp`, jdk-25-ga) | added "no cap, apart from a 1 GB compressed class space" |
| jvm-memory/3 | G1 humongous objects (at least half a region) are allocated directly in old-generation regions | confirmed | Oracle GC tuning guide, G1 chapter (search); `g1CollectedHeap.cpp` threshold `GrainWords / 2` | none |
| jvm-memory/3 | Old-generation work is not always a full stop-the-world collection (G1 mixed pauses, concurrent ZGC/Shenandoah) | confirmed | Oracle GC tuning guide (search) | none |
| jvm-memory/4 | ZGC keeps pauses around or below a millisecond independent of heap size | confirmed | Oracle GC tuning guide, ZGC chapter; OpenJDK ZGC wiki (search) | none |
| jvm-memory/4 | CMS removed in Java 14 | confirmed | JEP 363 (search) | none |
| jvm-memory/4 | G1 is the default collector | softened | JEP 248 (JDK 9); `os::is_server_class_machine` in `os.cpp` (at least 2 processors and about 1.8 GB, otherwise Serial) | "default since Java 9"; added ZGC generational-by-default since JDK 23 (JEP 474) and non-generational removed in JDK 24 (JEP 490) |
| jvm-memory/6, /7 | `-XX:MaxRAMPercentage` is a share of the container limit; heap headroom for non-heap memory | confirmed | `gc_globals.hpp` jdk-25-ga (`MaxRAMPercentage` default 25.0, "MaxRAM * MaxRAMPercentage / 100") | none |
| jvm-memory/8 | Serial for tiny heaps and small containers | softened | `os.cpp` jdk-25-ga | added that the JVM picks Serial itself below 2 processors or about 1.8 GB |
| jvm-memory/9 | GC log shape `Pause Full (G1 Compaction Pause) 3968M->3951M(4096M)` | confirmed | cause string "G1 Compaction Pause" in `gcCause.cpp` jdk-21-ga and jdk-25-ga | none |
| jvm-runtime/4 | Virtual threads are final in Java 21 and win for I/O-bound workloads | confirmed | JEP 444 (search) | explanation now says the `synchronized` hazard applied to JDK 21 to 23 |
| jvm-runtime/5 | Virtual thread blocked in `synchronized` pins its carrier, remedy `ReentrantLock` | **fixed (version scope)** | JEP 491 (JDK 24, Closed/Delivered); `PinnedThreadPrinter.java` exists at jdk-23-ga and is gone at jdk-24-ga; `VirtualThread.java` at jdk-25-ga has no `tracePinnedThreads` | prompt now "On JDK 21 (before JEP 491 arrived in JDK 24)"; explanation covers JDK 21-23, JDK 24+ (no pinning for `synchronized` / `Object.wait()`), what still pins, and the replacement diagnostics. Key unchanged |
| jvm-runtime/3 | `ReentrantLock` is non-fair by default; a monitor has one wait set | confirmed | Java SE API (not version-sensitive; no lookup) | none |
| jvm-advanced/1 | C2 escape analysis and scalar replacement are an optimisation, not a guarantee | confirmed | not version-sensitive (no lookup) | none |
| jvm-advanced/3 | Parent-first class loading; classes unload with their loader; Metaspace has no default cap | softened | JVMS 5.3 (not version-sensitive); `globals.hpp` jdk-25-ga | same compressed class space qualifier as above |
| jvm-advanced/4 | `-Xlog:safepoint` line format `Time since last / Reaching safepoint / At safepoint / Total`, and time-to-safepoint versus operation reading | softened | `safepoint.cpp` at jdk-21-ga (has `Cleanup`), jdk-24-ga (the exact sample fields), jdk-25-ga (adds `Leaving safepoint` and `Threads: runnable, total`); operation name `G1CollectForAllocation` exists in `g1VMOperations.hpp` jdk-21-ga and jdk-25-ga; sample arithmetic 1812004300 + 19800100 = 1831804400 checks | interpretation confirmed; explanation now names the field set per JDK. The sample matches JDK 24 |
| jvm-advanced/8 | Carrier pool is a `ForkJoinPool` sized by default to the number of processors | confirmed | `VirtualThread.java`: `parallelism = availableProcessors()`, `maxPoolSize = max(parallelism, 256)` at jdk-21-ga and jdk-25-ga; JEP 444 (search) | added both property names and defaults to the explanation |
| jvm-advanced/8 | `ThreadLocal` works in virtual threads; use a `Semaphore` to limit a resource; do not pool virtual threads | confirmed | JEP 444 (search) | none |
| jvm-advanced/6 | JFR is low-overhead and meant for production; async-profiler avoids safepoint bias | confirmed | not version-sensitive (no lookup) | none |

### 09-data.yaml

| Question id | Claim | Verdict | Source | Change |
| --- | --- | --- | --- | --- |
| data-sql/2 | Function on the column blocks the index, function on the constant does not; expression index fixes it | confirmed | not version-sensitive (no lookup) | none |
| data-sql/5 | Standard anomalies; PostgreSQL REPEATABLE READ is snapshot isolation and prevents phantoms | confirmed | `mvcc.sgml` (PostgreSQL docs source): "Repeatable Read implementation does not allow phantom reads" | none |
| data-sql/6 | Write skew possible under snapshot isolation / REPEATABLE READ; SERIALIZABLE with retries (SSI) prevents it | confirmed | PostgreSQL 18 docs, transaction isolation (search); `mvcc.sgml` | none |
| data-sql/7 | Option d "default level (READ COMMITTED)" does not prevent a lost update | softened | `config.sgml`: `default_transaction_isolation` default is "read committed" (PostgreSQL only; MySQL InnoDB defaults to REPEATABLE READ) | option text now "READ COMMITTED in PostgreSQL" |
| data-sql/8 | `FOR UPDATE SKIP LOCKED` (PostgreSQL, MySQL 8) skips locked rows; `NOWAIT` errors | softened | PostgreSQL 9.5 release notes and current `SELECT` docs; MySQL 8.0.1 blog and 8.0 manual (search); `select.sgml` | explanation now "PostgreSQL 9.5 or later, MySQL 8.0.1 or later" |
| data-jpa/1 | Open-in-view is on by default in Spring Boot; lazy collection outside the transaction throws `LazyInitializationException` | confirmed | Spring Boot reference "SQL Databases" and `JpaWebConfiguration` (`matchIfMissing=true`) (search); no Boot 4.0 page found, so the 4.x default is not explicitly confirmed | none |
| data-jpa/2 | Dirty checking at flush; `readOnly = true` stops flushing | confirmed | Spring Data JPA "Transactionality" (flush mode MANUAL for read-only with Hibernate); Spring `HibernateJpaDialect` Javadoc (search) | none |
| data-jpa/3 | Keyset pagination properties | confirmed | not version-sensitive (no lookup) | none |
| data-jpa/5 | Editing an applied versioned migration fails validation on checksum mismatch; `repair` exists | **softened** | Flyway source `MigrationInfoImpl.java` (versioned migration with different checksum gives `CHECKSUM_MISMATCH`; outdated repeatable migrations are re-applied), `FlywayModel.java` (`validateOnMigrate` default `true`); Redgate docs for validate and repair (search): repair realigns checksums in the schema history and does not change SQL | explanation now says `validateOnMigrate` is on by default so `migrate` validates first, what `repair` does and when it is acceptable, and that repeatable migrations are re-applied. Flyway 13.12. The Community/Teams split was not part of the claim; the sources list validate as available in Community, and Teams-only features are undo and dry runs, so no edition wording was added |
| data-jpa/5 | Liquibase does the same with changeset checksums | confirmed | Liquibase docs on changeset checksums, `MD5SUM` column (search) | none |
| data-jpa/7 | Second-level cache (application side) does not see an outside SQL `UPDATE`; `@Version` conflict surfaces on write | confirmed (by design) | Hibernate ORM user guide, caching chapter (search) describes Hibernate-managed regions and read-write strategy; no verbatim sentence about outside writes was found, the answer follows from the cache living in the application | none |
| data-jpa/8 | 40 pools x 25 = 1,000 connections against `max_connections` 500; process per connection; PgBouncer | confirmed | not version-sensitive arithmetic; PostgreSQL process-per-connection model (no lookup) | none |

### 10-messaging.yaml

| Question id | Claim | Verdict | Source | Change |
| --- | --- | --- | --- | --- |
| msg-kafka/1 | A partition is read by at most one consumer in a group | softened | Kafka 4.2 release notes: share groups (KIP-932) production-ready, several consumers per partition (search) | "consumer group" qualifier; share groups named in msg-kafka/8 |
| msg-kafka/2 | Ordering per partition and per key | confirmed | not version-sensitive (no lookup) | none |
| msg-kafka/3 | Default partitioner uses murmur2 and treats keyless records differently | softened | `BuiltInPartitioner.java` trunk (adaptive sticky partitioning, KIP-794) and `Utils.toPositive(Utils.murmur2(...)) % numPartitions` | wording now "producer hashes keys with murmur2 and spreads keyless records in sticky batches (KIP-794)" |
| msg-kafka/4 | `max.poll.interval.ms` exceeded causes the member to leave; heartbeats on a separate thread governed by `session.timeout.ms` | confirmed | `ConsumerConfig.java` trunk: `max.poll.interval.ms` 300000, `session.timeout.ms` 45000, `max.poll.records` 500 | none to the key |
| msg-kafka/4 | The log excerpt | **fixed** | `AbstractCoordinator.java` (`handlePollTimeoutExpiry` message, "sending LeaveGroup request ... due to"), `ConsumerCoordinator.java` ("Offset commit failed on partition {} at offset {}: {}"), `Errors.java` ("Specified group generation id is not valid.") | log lines replaced with the real client wording; the "412 s" figure removed from the log and the explanation |
| msg-kafka/5 | Classic eager rebalance steps; cooperative protocol moves only some partitions | softened | `ConsumerConfig.java` (`partition.assignment.strategy` default Range then CooperativeSticky; `group.protocol` default classic); KIP-848 GA in Kafka 4.0, opt in with `group.protocol=consumer`, coordinator computes assignment (search) | explanation mentions KIP-848 |
| msg-kafka/6 | Committing before processing is at-most-once | confirmed | not version-sensitive (no lookup) | none |
| msg-kafka/7 | `acks=all`, RF 3 with `min.insync.replicas=2`, unclean leader election disabled reduce loss of acknowledged writes | confirmed | `ProducerConfig.java` (`acks` default `all`); `ServerLogConfigs.java` (`min.insync.replicas` default 1, so it must be set) | none |
| msg-kafka/8 | Extra instances beyond the partition count sit idle | softened | share groups (above) | "consumer group" qualifier and a share-groups remark |
| msg-patterns/2 | Exactly-once scope: idempotent producer, transactions, committed reads | **fixed** | `ProducerConfig.java`: `enable.idempotence` default true; KIP-679 (default since 3.0, effective from 3.1.1 after a bug fix, search); `ConsumerConfig.java`: `isolation.level` default `read_uncommitted` | explanation now says the idempotent producer is on by default since 3.0 and that committed-only reads need `isolation.level=read_committed` |
| msg-patterns/4 | Queue versus log; "RabbitMQ also offers stream-style queues" | softened | RabbitMQ Streams guide ("append-only log"); Kafka 4.2 share groups (search) | wording "streams, an append-only log type" and share groups added |
| msg-patterns/5 | Schema evolution: additive fields with defaults are safe | confirmed | not version-sensitive (no lookup) | none |
| msg-patterns/7 | `INSERT ... ON CONFLICT DO NOTHING` is PostgreSQL syntax | confirmed | already flagged as PostgreSQL syntax in the explanation (no lookup) | none |

### 06-scale.yaml

| Question id | Claim | Verdict | Source | Change |
| --- | --- | --- | --- | --- |
| scale-load/2, /8 | HikariCP log "Connection is not available, request timed out after 30000ms (total=10, active=10, idle=0, waiting=187)"; pool of 10 | confirmed | `HikariPool.java` (exact message format with totals), `HikariConfig.java` (`connectionTimeout` 30 s, default pool size 10) | explanation now states the two defaults |
| scale-resilience/1 | Circuit breaker OPEN to HALF-OPEN after the wait | softened | `CircuitBreakerConfig.java` (wait 60 s, 10 permitted half-open calls, `automaticTransitionFromOpenToHalfOpenEnabled=false`); `CircuitBreaker.java` Javadoc | explanation adds the Resilience4j defaults and that the switch happens on the next call unless automatic transition is enabled |
| scale-resilience/5 | Bulkhead isolates resources per dependency | confirmed | not version-sensitive (no lookup) | none |
| scale-resilience/11, 13 | Transactional outbox; Debezium reads the transaction log in commit order | confirmed | Debezium PostgreSQL and outbox router docs (search); PostgreSQL `logicaldecoding.sgml`: "Concurrent transactions are decoded in commit order" | none |
| scale-advanced/5 | gRPC deadlines propagate as remaining time; "in some languages through the call context" | softened | grpc.io "Deadlines" guide: automatic propagation supported by some implementations, enabled by default in Java and Go, needs enabling in C++ | wording made precise |
| scale-advanced/6 | Cache-aside: update the database then delete the entry; a rare race remains | confirmed | pattern analysis, not version-sensitive (no lookup) | none |
| scale-advanced/1 | Token bucket in a Redis Lua script | confirmed | not version-sensitive (no lookup) | none |
| scale-advanced/9 | Tail latency 1 - 0.99^50 is about 40%; hedged requests at about p95 | confirmed | arithmetic (0.395); technique from the literature, not version-sensitive | none |

### Read but not version-sensitive (no lookup)

Little's law, p99 arithmetic (1 - 0.99^20 = 18.2%), retry multiplication (27), backoff and percentile code, B-tree fan-out, queue drain arithmetic, token bucket, lost update and write skew reasoning, keyset pagination code, expand-and-contract migrations, saga and outbox ordering, happens-before and safe publication, JIT warm-up and JMH, class loading rules, thread dump and deadlock reading. These were read for correctness and left alone.

## Counts

- Rows in the tables: 52 (19 in 04-jvm, 12 in 09-data, 13 in 10-messaging, 8 in 06-scale). Of these, 14 are version-independent claims read for correctness with no lookup; 38 rest on a named source.
- Confirmed with no edit: 34 (14 of them are the no-lookup rows)
- Softened or qualified with a version or context: 15
- Fixed: 3 (jvm-runtime/5 version scope, msg-kafka/4 log lines, msg-patterns/2 committed-read wording)
- Answer keys changed: 0
- Could not verify: 2 points, neither changes an answer. The Spring Boot 4.x default for `spring.jpa.open-in-view` (the page found describes the 3.x-era behaviour), and a verbatim Hibernate documentation sentence on writes made outside Hibernate (the answer follows from the cache design). Also unresolved: whether Kafka 4.3 or 4.4 exist (the download-page snapshots disagreed); nothing in the text depends on it.
