# Commit study notes

This branch builds a small distributed log pipeline and then fixes the review comments one commit at a time. A producer accepts log events over HTTP and writes them to Kafka. A consumer reads Kafka and stores the events in Postgres. Kafka, Redis, and Postgres run locally in Docker.

Each note below is one commit. It says what was wrong, why that matters when more than one machine is involved, and what we changed.

## 1. `ded23f1` — Scaffold the log producer and consumer for local development

**Issue.** There was no runnable pipeline. The database username and password had also been typed into a file that would have been saved in git.

**Fix.** Add the two Spring Boot apps, Docker Compose for Kafka, Redis, and Postgres, and a parent Maven build. Keep the real database password in a local `.env` file that git ignores. The repo only has `.env.example`, which shows the names of the settings and uses placeholders.

## 2. `8053e56` — Commit Kafka offsets only after a log event is saved

**Issue.** The consumer turned off Kafka's automatic offset commit and chose manual acknowledgment, but it never called `acknowledge()`. Kafka therefore had no record of how far the consumer had read. After a restart it would start again at the beginning of the topic, and the lag would grow forever.

**Fix.** Acknowledge the record only after the database insert succeeds. If the insert fails, the offset stays uncommitted, so Kafka will deliver that record again.

## 3. `b90ea3d` — Use the Spring Boot 3 Resilience4j starter

**Issue.** The producer depended on `resilience4j-spring-boot2`. That library is built for Spring Boot 2. On Boot 3 its auto-configuration does not load.

**Fix.** This commit switched the dependency to the Boot 3 starter. A later commit, `f201e0c`, removes Resilience4j completely because no code was using a circuit breaker.

## 4. `f7ee0af` — Send failed log events to a dead-letter topic

**Issue.** A bad message was either dropped with `printStackTrace()`, or retried a few times and then skipped. In both cases the event was gone, and nothing recorded why.

**Fix.** Use Spring Kafka's error handler with a dead-letter topic, `log-events.DLT`. Bad JSON is not retried. Other failures get a short exponential backoff, then the record is copied to the dead-letter topic. The original offset can then move forward, so one poison message does not block the partition.

## 5. `a8702db` — Keep the first stored log event when the same id arrives again

**Issue.** Saving through JPA did a merge: if the id already existed, the new message overwrote the old row. A replay, which is normal in Kafka, could change another organization's log.

**Fix.** Insert with `INSERT ... ON CONFLICT (id) DO NOTHING`. The first write wins. A duplicate delivery is acknowledged and skipped. A later commit, `c6d7fe8`, changes the conflict key so the id is unique per organization, not globally.

## 6. `8e710d7` — Store log event times as UTC instants

**Issue.** `LocalDateTime.now()` uses the time zone of whichever machine is running. Two hosts in different zones cannot be ordered, and a daylight-saving change can duplicate or skip an hour.

**Fix.** Use `Instant` on the producer and the consumer, and `TIMESTAMP WITH TIME ZONE` in Postgres. The producer fills in the current time only when the client did not send one.

## 7. `cab69bb` — Build executable Spring Boot jars

**Issue.** The parent pom does not inherit `spring-boot-starter-parent`, so Maven did not run the `repackage` goal. `mvn package` produced a plain jar that `java -jar` cannot run. Spring also needs Java parameter names to bind constructor arguments.

**Fix.** Bind `repackage` in the parent's `pluginManagement`, and turn on `maven-compiler-plugin` parameter names.

## 8. `a277a31` — Publish log events from the producer

**Issue.** The producer module did not publish anything. There was no HTTP endpoint, no Kafka template, and no topic settings, so the pipeline could not be tried end to end.

**Fix.** Add `POST /logs`. Require an organization id, a level, and a message. Publish to `log-events` with the organization id as the Kafka key, so one organization's events stay in order on a partition. Wait for Kafka to accept the record (`acks: all`) and turn on producer idempotence so a retried send is not written twice inside Kafka. Return 202 when the broker accepts the record.

## 9. `959f47a` — Log the application package at DEBUG

**Issue.** The logging config named `com.example.logprocessor`. This project lives in `com.system.logprocessor`, so the DEBUG level never applied.

**Fix.** Log `com.system.logprocessor` at DEBUG.

## 10. `a21507b` — Drop the unused Boot 2 Prometheus setting

**Issue.** `management.metrics.export.prometheus.enabled` is a Spring Boot 2 property. Boot 3 ignores it. Prometheus export is already on because the registry is on the classpath.

**Fix.** Remove the unused block.

## 11. `92f7c43` — Remove the unused circuit breaker settings

**Issue.** The consumer's YAML configured a Resilience4j circuit breaker, but the consumer did not depend on Resilience4j and did not wrap the database write. The settings did nothing.

**Fix.** Remove them.

## 12. `c33f8ae` — Remove unused consumer dependencies

**Issue.** The consumer depended on Redis, Spring Retry, and Spring Aspects, and none of its code used them. The Redis starter still added a health check, so a stopped Redis would mark the consumer unhealthy.

**Fix.** Remove those dependencies from the consumer. Redis stays in Docker Compose for later use. It is not part of this consumer.

## 13. `76f6af0` — Use the Spring Cloud release train for Boot 3.2

**Issue.** Spring Cloud `2022.0.x` targets Boot 3.0 and 3.1. This project uses Boot 3.2, which needs the `2023.0.x` release train. Nothing used Spring Cloud yet, but the compatibility check would fail as soon as a Cloud starter was added.

**Fix.** Import Spring Cloud `2023.0.0`.

## 14. `67bea5f` — Hide health details from anonymous callers

**Issue.** `show-details: always` put database and disk details on a public `/actuator/health` URL.

**Fix.** Use `when-authorized`, so anonymous callers see only UP or DOWN.

## 15. `befdfa2` — Create the log table with Flyway

**Issue.** `ddl-auto: update` lets Hibernate change the schema at startup. That is risky for a shared database: it will not drop or reshape columns safely, and it does not give you a reviewed history of schema changes.

**Fix.** Add a Flyway migration that creates `log_events` and an index on `(organization_id, timestamp)`. Set `ddl-auto: validate`, so Hibernate checks that the Java model matches the database and does not change it.

## 16. `ff8a240` — Let containers reach Kafka and wait for Zookeeper

**Issue.** Kafka advertised only `localhost:9092`. That address works from the host, but a container that tries to use it connects to itself, not to Kafka. Compose also started Kafka without waiting until Zookeeper was ready. Topic auto-creation would have made `log-events` with one partition.

**Fix.** Add an internal listener at `kafka:29092` for containers, and keep `localhost:9092` for the host. Wait until Zookeeper is healthy before starting Kafka. Create `log-events` explicitly with 3 partitions and replication factor 1.

## 17. `62773b9` — Test the consumer and take the topic and group from configuration

**Issue.** The only check was `mvn compile`. The topic name and consumer group were also hardcoded in Java and copied again in YAML.

**Fix.** Read the topic and group from configuration. Add a test that starts embedded Kafka and a Postgres container, sends an event, and waits until it is stored. Add another test that sends malformed JSON and checks that it lands on the dead-letter topic.

## 18. `4df9941` — Inject the consumer's dependencies through its constructor

**Issue.** The consumer used field injection. The class did not show what it needs, and a test cannot pass those dependencies in without starting Spring.

**Fix.** Take the repository, the JSON mapper, and the metrics registry in the constructor and keep them `final`.

## 19. `662302d` — Remove the obsolete Compose version field

**Issue.** `version: '3.8'` is ignored by Compose v2 and prints a warning.

**Fix.** Remove the field.

## 20. `818013e` — Point the project at this repository and include the Apache 2.0 license it declares

**Issue.** The parent pom pointed at a placeholder project URL, and it declared the Apache 2.0 license without including the license text.

**Fix.** Set the URL to this GitHub repository and add the Apache 2.0 `LICENSE` file.

## 21. `5d6a41a` — Give the dead-letter topic the same partitions as log-events

**Issue.** `log-events` has 3 partitions. The dead-letter topic was being created with 1. The error handler keeps the source partition number, so a failure on partition 1 or 2 had nowhere to go. That partition would retry forever and stop making progress.

**Fix.** Create `log-events.DLT` with the same 3 partitions, both from the Kafka init container and from a Spring `NewTopic` bean. The malformed-message test now sends to partition 2 and checks that the dead-letter record is on partition 2.

## 22. `d48d82e` — Reject a null log event immediately and skip retries for constraint violations

**Issue.** The JSON literal `null` deserialized into a null object and then threw a null pointer error, which was retried. A database constraint violation was also retried, even though the same row will fail the same way every time.

**Fix.** Treat a null payload as an invalid event and send it straight to the dead-letter topic. Mark `DataIntegrityViolationException` as not retryable, so a constraint failure is dead-lettered instead of retried.

## 23. `2c9a567` — Reject log input the database cannot store, and require a message

**Issue.** The producer accepted values the database cannot store: an organization id, level, or source longer than 255 characters, or a level outside the known set. The message was required in the database and optional in the producer, so the two sides disagreed.

**Fix.** Reject those requests with 400 before they are published. Require a message on both sides. This commit also added a second Flyway migration to make `message` NOT NULL. Commit `d7b220f` later folds that change into the first migration, because nothing had been deployed yet.

## 24. `f3b18da` — Return 413 when a log event is too large for Kafka

**Issue.** A record bigger than Kafka's max request size came back as 503, which means "try again later." The record will never fit, so retrying does not help.

**Fix.** Detect `RecordTooLargeException` and return 413 Payload Too Large. The test sets a tiny max request size so a normal event is rejected with 413.

## 25. `f201e0c` — Remove unused producer dependencies

**Issue.** The producer still depended on Redis, Resilience4j, and Spring AOP. No producer code used them. The Redis starter would also fail the producer's health check when Redis was down.

**Fix.** Remove those dependencies. The producer tests no longer need to turn off Redis auto-configuration.

## 26. `396ff3f` — Exercise dead-lettering from a partition other than zero

**Issue.** The dead-letter test only sent to partition 0. That would still pass if the dead-letter topic had a single partition, which was the bug in commit `5d6a41a`.

**Fix.** The test from `5d6a41a` is the one that sends to partition 2. This commit is that test coverage standing on its own in the history: a failure on a non-zero partition must be dead-lettered on that same partition.

## 27. `6fcc149` — Let Testcontainers negotiate the Docker API version

**Issue.** The test pinned Docker API version 1.44 in a properties file. The Spring Boot dependency set also pulled an older Testcontainers, which spoke an API this Docker Desktop rejects.

**Fix.** Import the Testcontainers 1.21.4 bill of materials before the Spring Boot bill of materials, so Maven uses the newer client. Delete the pinned API version and let the client negotiate with the Docker daemon.

## 28. `050d316` — Keep retrying valid events while the database is unavailable

**Issue.** Anything that was not on the "do not retry" list got about 30 seconds of backoff and was then copied to the dead-letter topic. That included Postgres being down. Valid events were then acknowledged and never stored. Nothing in this project reads the dead-letter topic back into the database, so those events were lost.

**Fix.** If the failure is a transient database or connection error, retry every 5 seconds with no attempt limit. The partition waits until Postgres is back. Bad records still go to the dead-letter topic. The test pauses the Postgres container for 40 seconds, checks that the event is not dead-lettered, unpauses, and checks that the row is stored. Pausing keeps the published port stable. Stopping and starting the container can change that port and break the app's JDBC URL.

## 29. `c20b63b` — Stop Kafka from creating topics on first use

**Issue.** Kafka still auto-created topics. A typo in a topic name, or a consumer that started before the init container, would silently create a topic with one partition. That brings back the stuck-partition bug.

**Fix.** Set `KAFKA_AUTO_CREATE_TOPICS_ENABLE` to false. The init container is the only thing that creates `log-events` and `log-events.DLT`.

## 30. `d7b220f` — Require the log message in the initial migration

**Issue.** Commit `2c9a567` added a second migration only to make `message` NOT NULL. No deployed database had run the first migration yet, so a second file was extra history.

**Fix.** Put `message TEXT NOT NULL` in `V1__create_log_events.sql` and delete V2. If a local database already applied the old migrations, it must be recreated (`docker compose down -v`) because Flyway checksums the files it has already run.

## 31. `f369d91` — Give each log partition its own listener thread

**Issue.** The consumer had 3 partitions and one listener thread. A slow insert, or a retry while the database is down, on one partition also stalled the other two.

**Fix.** Set the listener concurrency to the partition count, so each partition can be processed on its own thread.

## 32. `95e5529` — Bound how long the producer waits for Kafka

**Issue.** The HTTP handler waited 10 seconds for the send future, but the Kafka client first blocks for metadata for up to `max.block.ms`, which defaults to 60 seconds. When the broker is down, each request holds a Tomcat thread for about a minute before returning 503.

**Fix.** Set `max.block.ms` and `request.timeout.ms` to 5 seconds, and `delivery.timeout.ms` to 10 seconds. The delivery timeout has to be at least the request timeout plus linger time.

## 33. `852190d` — Send a dead-letter record to a partition that exists

**Issue.** The dead-letter partition was copied from the source record. If someone later raised the source partition count without raising the dead-letter count, recovery would ask for a partition that does not exist, and that source partition would retry forever.

**Fix.** If the source partition number is inside the dead-letter topic, keep it. Otherwise pass `-1` and let Kafka choose a partition. A unit test checks both cases.

## 34. `9c88b68` — Read the dead-letter topic without joining a consumer group

**Issue.** The test created a new consumer group, subscribed, and polled once for a short time. That one poll had to join the group, get an assignment, and fetch. It often came back empty, and the test only passed because Awaitility kept retrying. On a slow machine it can hit the timeout.

**Fix.** Assign the dead-letter partitions directly, seek to the beginning, and poll. There is no group join.

## 35. `6eda487` — Reject log events whose fields are longer than the columns

**Issue.** The producer rejects an organization id, level, or source longer than 255 characters. Another producer could still publish those records. The consumer then failed the insert with a constraint violation and dead-lettered the record, but the `log.events.rejected` metric did not count it.

**Fix.** Check those lengths in the consumer and reject them as invalid, which increments the metric and still dead-letters the record. The test sends a 256-character organization id and expects it on the dead-letter topic and not in the database.

## 36. `d1e3f20` — Return 500 when a log event cannot be serialized

**Issue.** If turning the event into JSON failed, the producer returned 400. The event was already built by the server after validation, so this is a bug in the server, not a bad client request.

**Fix.** Return 500.

## 37. `9b4b762` — Accept a client-supplied event id so a retry is not stored twice

**Issue.** A timeout does not prove Kafka never got the record. The producer may still deliver it. The server minted a new UUID on every request, so a client that retried after a 503 stored a second event. The database deduplicates on id, so it could not see that the two requests were the same event.

**Fix.** Let the client send an optional `id`. If it does, that id is the event id. A retry with the same id is one row. If the client omits it, the server still generates a UUID.

## 38. `bb86fbf` — Accept a lowercase log level and store it in uppercase

**Issue.** The level check was an exact match. `info` was rejected with 400, while `INFO` was accepted. Clients and libraries do not all use the same case.

**Fix.** Trim the level and compare it in uppercase. Store the uppercase value. `info` is accepted and saved as `INFO`.

## 39. `6d44fef` — Reject a client event id that is not a UUID

**Issue.** After clients could choose the id, two organizations that both sent `evt-1` collided. The id was the only primary key, so the second insert did nothing. That second organization had already received 202 Accepted, and its event was then dropped with no error.

**Fix.** This commit required the client id to be a UUID and returned 400 otherwise. That stops accidental collisions on short ids such as `evt-1`. It does not stop one organization from copying a UUID it has seen from another organization. Commit `c6d7fe8` is the fix that closes that gap, and it removes this UUID rule.

## 40. `a355179` — Classify connection failures by SQLState only

**Issue.** The consumer treated a database error as "temporary, keep retrying" when the exception type or the SQLState said the connection failed. It also matched English text in the driver message, such as "Connection refused." That text can change with the driver version or the locale, so a real outage might stop matching, or an unrelated error might match.

**Fix.** Keep the exception types and the SQLState checks (`08` for connection failures, and `57P01` for the server shutting the connection down). Drop the message-text checks.

## 41. `c6d7fe8` — Store the same event id separately for each organization

**Issue.** A UUID rule only makes collisions unlikely. It does not make them impossible. If organization B reuses an id it saw from organization A, the insert matches the existing primary key and organization A's row causes organization B's event to be skipped. The producer had already returned 202.

**Fix.** Make the primary key `(organization_id, id)`. The insert uses `ON CONFLICT (organization_id, id) DO NOTHING`. The same id is stored once per organization, and a replay from the same organization is still skipped. The Java model uses an `@IdClass` so both columns are the identity. The consumer test sends `evt-1` for `org-1` and `org-2` and checks that both rows are stored. Client ids no longer have to be UUIDs. `evt-1` is accepted again.

If Postgres was created from an earlier version of this branch, recreate it with `docker compose down -v` before starting the consumer. Flyway will not edit a primary key that was already created, and the first migration file has changed.
