# CLAUDE.md - Flink Data Sampling FLIP Development

## Project Context
This is the Apache Flink open-source project. We are developing FLIP-XXX: Runtime Data Sampling.
The complete design document is at: ./FLIP-data-sample.md
MUST read this file before any implementation work.

## Key Architecture Reference
- FlameGraph implementation (our architecture reference pattern):
    - REST Handler: flink-runtime/src/main/java/org/apache/flink/runtime/rest/handler/job/JobVertexFlameGraphHandler.java
    - Tracker: flink-runtime/src/main/java/org/apache/flink/runtime/webmonitor/stats/VertexThreadInfoTracker.java
    - Coordinator: flink-runtime/src/main/java/org/apache/flink/runtime/webmonitor/stats/TaskStatsRequestCoordinator.java
    - TM-side Service: flink-runtime/src/main/java/org/apache/flink/runtime/taskexecutor/ThreadInfoSampleService.java
- Output pattern reference:
    - RecordWriterOutput: flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/io/RecordWriterOutput.java
    - CountingOutput: flink-streaming-java/src/main/java/org/apache/flink/streaming/api/operators/CountingOutput.java
    - OperatorChain: flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/tasks/OperatorChain.java

## Build Commands
- Full build (skip tests): `mvn clean install -DskipTests -Dfast`
- Build specific module: `mvn clean install -DskipTests -pl flink-runtime`
- Run specific test: `mvn test -pl flink-runtime -Dtest=TestClassName`
- Run single test method: `mvn test -pl flink-runtime -Dtest=TestClassName#methodName`
- Checkstyle: `mvn checkstyle:check -pl flink-runtime`

## Coding Style & Conventions
- Follow Apache Flink coding guidelines: https://flink.apache.org/how-to-contribute/code-style-and-quality-preamble/
- Use `@Internal` annotation for all new classes (this FLIP introduces no public API)
- All new public methods must have Javadoc
- Use `javax.annotation.Nullable` for nullable fields
- Follow existing naming patterns (e.g., `XxxHandler`, `XxxTracker`, `XxxCoordinator`)
- Imports: static imports last, no wildcard imports
- Line width: 100 characters max
- Use `Preconditions.checkArgument()` / `checkNotNull()` for parameter validation
- Tab: 4 spaces (Flink standard)

## Git Conventions
- Branch naming: feature/FLINK-XXXXX-data-sampling-<component>
- Commit message format: [FLINK-XXXXX][runtime] Add SamplingRecordWriterOutput for data sampling

## Important Patterns to Follow
- When implementing REST handler, follow JobVertexFlameGraphHandler pattern exactly
- When implementing Tracker, follow VertexThreadInfoTracker pattern
- Configuration options go in RestOptions.java, following rest.flamegraph.* naming pattern
- All serializable classes must have serialVersionUID
- Thread safety: volatile for cross-thread flags, ReentrantLock for guarded sections

## Testing Patterns
- Unit tests in same module under src/test/java
- Integration tests use MiniCluster: see FlameGraph-related tests for examples
- Use `@ExtendWith(TestLoggerExtension.class)` for test classes
- Mock with Mockito, assertions with AssertJ

## Do NOT
- Do NOT modify existing public API classes without explicit instruction
- Do NOT add new dependencies without discussion
- Do NOT change checkpointing/network stack code paths
- Do NOT use System.out.println (use SLF4J logging)
- Do NOT create files outside the designated module unless instructed
