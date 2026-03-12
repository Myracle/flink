# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Flink — open source stream processing framework. Version 2.3-SNAPSHOT. Java 11/17/21 supported; default source is Java 11, default target is Java 17. Uses Maven (mvnw wrapper, requires ≥3.8.6).

## Build Commands

```bash
# Fast build (skip tests and QA checks)
./mvnw clean package -DskipTests -Dfast

# Build specific module (with dependencies)
./mvnw clean package -DskipTests -Dfast -pl flink-runtime -am

# Compile only (useful for IDE development)
./mvnw test-compile -Dflink.markBundledAsOptional=false -Dfast

# Code formatting check / apply
./mvnw spotless:check
./mvnw spotless:apply

# Run single test class
./mvnw test -pl flink-core -Dtest=ConfigurationTest

# Run single test method
./mvnw test -pl flink-core -Dtest=ConfigurationTest#testGetString

# Run integration tests (convention: *ITCase suffix)
./mvnw verify -pl flink-tests -Dtest=SomeITCase -DfailIfNoTests=false

# Run end-to-end tests
flink-end-to-end-tests/run-single-test.sh test-scripts/test_batch_wordcount.sh
```

## Code Style

- **Formatter**: google-java-format (AOSP style) via Spotless. Max line length: 100.
- **Scala**: scalafmt 3.4.3, max column 100.
- **Checkstyle**: config at `tools/maven/checkstyle.xml` (v10.18.2). Suppressions at `tools/maven/suppressions.xml`.
- **No star imports**. No `org.mockito` or `org.powermock` in production code.
- **Disallowed imports**: `org.apache.commons.lang` (use `commons-lang3`), `org.codehaus.jackson` (use shaded jackson).
- **License**: All source files require Apache 2.0 header (managed by Spotless/RAT).
- **EditorConfig**: `.editorconfig` — UTF-8, LF line endings, 4-space indent for Java/Python.

## Architecture

### Dependency Hierarchy (bottom-up)

```
flink-annotations / flink-metrics-core
        └── flink-core-api  ◄── flink-datastream-api
                └── flink-core
              ┌─────┴──────────────┐
         flink-rpc          flink-table-common
              └── flink-runtime    flink-table-planner
                    └── flink-streaming-java
                          └── flink-clients
                        ┌───────┴────────┐
                   flink-yarn     flink-kubernetes
```

### Core Modules

- `flink-core-api` / `flink-core` — Configuration (`ConfigOption<T>`), type system, filesystem abstraction, memory management, plugin system, serialization
- `flink-rpc` — 3 sub-modules: `rpc-core` (interfaces) → `rpc-akka` (Akka impl) → `rpc-akka-loader` (classloader isolation)
- `flink-runtime` — JobManager (scheduling, failure recovery), TaskManager (execution), ResourceManager (resource allocation), Dispatcher (job submission), shuffle, checkpointing
- `flink-streaming-java` — DataStream API, operators, windowing, StreamGraph construction
- `flink-table` — 15+ sub-modules: `table-common` (types, UDF) → `sql-parser` (Calcite SQL) → `table-planner` (optimizer, codegen) → `table-runtime` (execution) → `sql-gateway` / `sql-client`
- `flink-connectors` — `connector-base`, `connector-files`, `connector-datagen`. Most connectors externalized to separate repos (flink-connector-kafka, etc.)
- `flink-state-backends` — `rocksdb`, `forst`, `changelog`, `heap-spillable`, `common`

### Key Patterns

- **API stability annotations**: `@Public`, `@PublicEvolving`, `@Internal`, `@Experimental` control compatibility guarantees
- **Shading**: Critical deps (Netty, Guava, Jackson, ASM, ZooKeeper) are shaded via `flink-shaded-*` to avoid classpath conflicts
- **Classloader isolation**: Connectors/formats loaded via plugin system with parent-first or child-first strategy

## Testing

- **JUnit 5** (primary), JUnit 4 via vintage engine. AssertJ for assertions. Mockito for mocking.
- **Unit tests** (`*Test`): `src/test/java` in each module. Fork count: 4, memory: 768m.
- **Integration tests** (`*ITCase`): Fork count: 2, memory: 1536m.
- **Architecture tests**: `flink-architecture-tests` module using ArchUnit.
- **End-to-end tests**: `flink-end-to-end-tests` with shell scripts in `test-scripts/`.
- **Testcontainers** (1.21.4): Docker-based integration tests.
- Test exclusion tags: `FailsInGHAContainerWithRootUser`, `FailsWithAdaptiveScheduler`, `FailsOnJava11`.

## Maven Profiles

| Profile | Purpose |
|---------|---------|
| `fast` / `-Dfast` | Skip QA checks (spotless, checkstyle, RAT, javadoc) |
| `java11-target` | Compile for Java 11 |
| `java17-target` | Compile for Java 17 (default) |
| `java21-target` | Compile for Java 21 |
| `scala-2.12` | Scala 2.12 (active by default) |
| `github-actions` | Exclude tests that fail in GitHub Actions containers |
