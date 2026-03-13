# FLIP-XXX: Runtime Data Sampling — Implementation Review Context

## 1. Overview

This document summarizes the implementation of FLIP-XXX (Runtime Data Sampling for Job Vertices), intended for Flink technical reviewers. It covers the FLIP design intent, the mapping from design to code, key implementation decisions and deviations, thread safety model, and known limitations.

### Feature Summary

On-demand runtime data sampling at the output of any job vertex (operator chain). Users can dynamically inspect intermediate data flowing through any vertex in a running job via REST API, without restarting the job or modifying the topology. Records are displayed via `toString()` for readable types.

### Design Principles

- **Reuse FlameGraph architecture pattern** (Lazy Trigger + Polling, Tracker/Coordinator/Handler layering)
- **Forward-first**: record forwarding is never blocked; sampling is best-effort
- **Round-scoped**: sampling is active only during a bounded window (default 3s), not continuously
- **Default disabled**: opt-in via `rest.data-sampling.enabled=true`
- **Partial success**: unlike FlameGraph's all-or-nothing, individual TM failures don't discard results from healthy TMs

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Layer 1: REST API                                              │
│  JobVertexDataSampleHandler                                     │
│  GET /jobs/:jobid/vertices/:vertexid/data-sample                │
└──────────────────────┬──────────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────────┐
│  Layer 2: Tracker (JM-side)                                     │
│  VertexDataSampleTracker — lazy trigger + Guava cache           │
└──────────────────────┬──────────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────────┐
│  Layer 3: Coordinator (JM-side)                                 │
│  DataSampleRequestCoordinator — fan-out RPC, partial success,   │
│  assemble stats, apply total records cap (5000)                 │
└──────────────────────┬──────────────────────────────────────────┘
                       │ RPC (TaskExecutorDataSampleGateway)
┌──────────────────────▼──────────────────────────────────────────┐
│  Layer 4: TaskManager Execution                                 │
│  TaskExecutor.requestDataSamples() → StreamTask                 │
│  → SamplingRecordWriterOutput + BoundedSampleBuffer             │
└──────────────────────┬──────────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────────┐
│  Layer 5: Data Model + Serialization                            │
│  SamplingConfig / DataSampleRequest / SampledRecord /           │
│  SamplingRoundResult / TaskDataSampleResponse /                 │
│  VertexDataSampleStats / DataSampleResponseBody                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. File Inventory

### Production Code

| Layer | File | Description |
|-------|------|-------------|
| Config | `flink-core/.../configuration/RestOptions.java` (lines 289-348) | 7 new config options under `rest.data-sampling.*` |
| REST | `flink-runtime/.../rest/handler/job/JobVertexDataSampleHandler.java` | GET handler with terminal-state pre-check, subtask/maxRecords post-filtering |
| REST | `flink-runtime/.../rest/messages/JobVertexDataSampleHeaders.java` | REST endpoint metadata |
| REST | `flink-runtime/.../rest/messages/JobVertexDataSampleParameters.java` | Path + query parameters |
| REST | `flink-runtime/.../rest/messages/DataSampleResponseBody.java` | JSON response body with nested `SubtaskSampleResponseBody`, `SampledRecordResponseBody`, `FailedSubtaskResponseBody` |
| REST | `flink-runtime/.../rest/messages/SubtaskIndexQueryParameter.java` | Query parameter class |
| REST | `flink-runtime/.../rest/messages/MaxRecordsQueryParameter.java` | Query parameter class |
| Tracker | `flink-runtime/.../sampling/VertexDataSampleTracker.java` | Lazy trigger + Guava Cache + pending deduplication |
| Tracker | `flink-runtime/.../sampling/VertexDataSampleTrackerBuilder.java` | Builder pattern for tracker construction |
| Coordinator | `flink-runtime/.../sampling/DataSampleRequestCoordinator.java` | Fan-out RPC, partial success assembly, timeout, ghost ID dedup |
| TM Gateway | `flink-runtime/.../taskexecutor/TaskExecutorDataSampleGateway.java` | RPC gateway interface (mixin pattern, like `TaskExecutorThreadInfoGateway`) |
| TM Execution | `flink-runtime/.../taskexecutor/TaskExecutor.java` (lines 658-721) | `requestDataSamples()` — fan-out to tasks with graceful per-task error handling |
| Task | `flink-runtime/.../streaming/runtime/tasks/StreamTask.java` (lines 1737-1815) | `requestDataSamples()` — mailbox-based start/collect with timer |
| Hot Path | `flink-runtime/.../streaming/runtime/io/SamplingRecordWriterOutput.java` | `RecordWriterOutput` subclass, volatile gate, rate limiter, toString conversion |
| Buffer | `flink-runtime/.../sampling/BoundedSampleBuffer.java` | Lock-guarded ArrayList with capacity limit |
| Installation | `flink-runtime/.../streaming/runtime/tasks/OperatorChain.java` (lines 560-574) | Conditional wrapping in `createChainOutputs()` |
| Registration | `flink-runtime/.../webmonitor/WebMonitorEndpoint.java` (lines 311-332, 978-999) | Tracker initialization + handler registration |
| Interface | `flink-runtime/.../sampling/DataSampleableTask.java` | Interface implemented by StreamTask |
| Data Models | `flink-runtime/.../sampling/` — `SampledRecord`, `SubtaskDataSample`, `SamplingRoundResult`, `TaskDataSampleResponse`, `VertexDataSampleStats`, `DataSampleRequest`, `SamplingConfig`, `SampleStatus`, `SampleErrorCode`, `FailedSubtaskInfo`, `SamplingRoundState`, `SampleBuffer` | Serializable domain objects |

### Test Code

| File | Type | Coverage |
|------|------|----------|
| `flink-runtime/.../sampling/DataSampleRequestCoordinatorTest.java` | Unit | Coordinator: complete/partial/failed/timeout/concurrent rounds/empty executions |
| `flink-runtime/.../rest/messages/DataSampleResponseBodyTest.java` | Unit | JSON serialization, factory methods, null field omission |
| `flink-runtime/.../rest/handler/job/JobVertexDataSampleHandlerTest.java` | Unit | Handler: running/finished/mixed vertices, subtask filter, maxRecords, staleness |
| `flink-runtime/.../streaming/runtime/io/SamplingRecordWriterOutputTest.java` | Unit | Hot path: basic sampling, roundId mismatch, disableSampling, buffer full, multiple rounds |
| `flink-runtime/.../sampling/BoundedSampleBufferTest.java` | Unit | Buffer: add/drain, capacity, reset, zero capacity, unmodifiable result |
| `flink-tests/.../test/sampling/DataSamplingITCase.java` | Integration | E2E: MiniCluster, REST client, COMPLETE status, multi-subtask, subtask filter, maxRecords, invalid vertex |

---

## 4. FLIP Conformance Matrix

### Fully Implemented

| FLIP Requirement | Implementation |
|------------------|----------------|
| REST endpoint `GET /jobs/:jobid/vertices/:vertexid/data-sample` | `JobVertexDataSampleHeaders` + `JobVertexDataSampleHandler` |
| Status enum: PENDING, COMPLETE, PARTIAL, FAILED, NO_DATA, DISABLED | `SampleStatus` enum, handler factory methods (`waiting()`, `terminated()`, `disabled()`) |
| Error code enum: TIMEOUT, TM_UNREACHABLE, TOO_MANY_CONCURRENT_ROUNDS, TASK_TERMINATED, INTERNAL_ERROR | `SampleErrorCode` enum |
| Lazy Trigger + Polling (FlameGraph pattern) | `VertexDataSampleTracker.getJobVertexStats()` — cache check → trigger if empty/stale → return cached |
| Partial success (COMPLETE/PARTIAL/FAILED all complete normally) | `DataSampleRequestCoordinator.checkAndComplete()` — three-way status resolution, future always completes normally |
| `@Nullable Long endTimestamp`, `@Nullable Integer roundId` | `DataSampleResponseBody` uses `Long`/`Integer` wrapper types with `@JsonInclude(NON_NULL)` |
| Forward-first hot path | `SamplingRecordWriterOutput.collectAndCheckIfChained()` — `super.collect()` before sampling |
| Volatile gate + round-scoped lifecycle | `volatile boolean samplingEnabled`, `startRound()`/`completeRoundAndCollect()` lifecycle |
| Rate limiting (per-second counter) | `checkRateLimit()` with wall-clock second reset |
| `toString()` override detection (cached reflection) | `hasCustomToString()` via `ConcurrentHashMap<Class<?>, Boolean>` |
| Binary data fallback | `"[Binary data: N bytes]"` for `byte[]` |
| Exception isolation for `toString()` | `try/catch` in `convertToString()`, DEBUG log |
| Record length truncation | `data.substring(0, maxRecordLength)` with `truncated` flag |
| Side output support | `sampleSideOutputRecord()` with `OutputTag.getId()` |
| BoundedSampleBuffer with fill-and-stop | `BoundedSampleBuffer.tryAdd()` returns `false` when full |
| Installation in OperatorChain | `createChainOutputs()` conditionally creates `SamplingRecordWriterOutput` |
| Configuration options (7 options) | `RestOptions.DATA_SAMPLING_*` with FLIP-specified defaults |
| SamplingConfig range validation | `[1, 10000]` for maxSampleRate, `[1MB, 50MB]` for maxResponseBytes |
| Max 5 concurrent rounds (hardcoded) | `DataSampleRequestCoordinator.MAX_CONCURRENT_ROUNDS = 5` |
| Max 5000 total records (hardcoded) | `DataSampleRequestCoordinator.MAX_TOTAL_RECORDS = 5000` |
| Proportional fair truncation | `applyFairTruncation()` — ascending sort by count, redistribute surplus |
| Ghost ID deduplication | `recentRequestIds` deque (10 entries) |
| Query parameters: `subtaskIndex`, `maxRecords` (post-filters) | `SubtaskIndexQueryParameter`, `MaxRecordsQueryParameter`, applied in `DataSampleResponseBody.fromStats()` |
| Disabled mode returns DISABLED | `DisabledJobVertexDataSampleHandler` registered when `rest.data-sampling.enabled=false` |
| Terminated vertex returns FAILED + TASK_TERMINATED | `JobVertexDataSampleHandler.isTerminated()` pre-check |
| Stale indicator | `stale = (now >= endTime + refreshInterval)` in handler |
| Drop counters (droppedByContention, droppedByRateLimit) | Tracked per output, aggregated in `SamplingRoundResult`, propagated to REST response |
| `DataSampleableTask` interface | Implemented by `StreamTask` |
| `TaskExecutorDataSampleGateway` (mixin pattern) | Separate gateway interface, like `TaskExecutorThreadInfoGateway` |
| Guava Cache with TTL expiration | `CacheBuilder.expireAfterAccess()` in `VertexDataSampleTrackerBuilder` |
| Pending request deduplication | `VertexDataSampleTracker.pendingStats` set |

### Intentional Deviations from FLIP

| FLIP Design | Implementation | Rationale |
|-------------|----------------|-----------|
| `completeRoundAndCollect()` on ScheduledExecutor thread | On mailbox thread (via mailbox action submitted from timer callback) | Simpler single-thread model: both write and read happen on mailbox thread, eliminating concurrent buffer access. Timer callback only sets `samplingEnabled = false` (volatile write) |
| `DataSampleRequestCoordinator extends TaskStatsRequestCoordinator` | Standalone class (does NOT extend) | Base class assumes all-or-nothing semantics; `handleFailedResponse()` discards all results. Standalone implementation enables native partial success. |
| `ScheduledExecutorService.schedule()` for sampling window | `systemTimerService.registerTimer()` (processing time timer) | Reuses Flink's existing timer infrastructure on StreamTask. Timer registration happens inside the startRound mailbox action so the window starts accurately after sampling begins. |
| `BoundedSampleBuffer` accessed from multiple threads | Single-writer (mailbox thread only) with lock as safety net | `startRound()`, `collect()`, `completeRoundAndCollect()` all run on mailbox thread. Lock remains for `drainAndClear()` as defense-in-depth. |
| `maxResponseBytes` byte accounting (Appendix E) | Not yet implemented (TODO in coordinator) | Deferred to follow-up. `maxTotalRecords` (5000) cap provides an effective bound. |

### Not Yet Implemented (Future Work per FLIP)

| Feature | Status |
|---------|--------|
| Schema-Aware Formatting for BinaryRowData | Separate FLIP (FLIP-SQL-data-sampling.md) |
| WebUI "Data Sample" tab | Not included in this PR (Phase 3 per FLIP) |
| Per-vertex enable/disable | Listed as Future Work in FLIP |
| Data masking / PII redaction | Listed as Future Work in FLIP |
| `maxResponseBytes` byte accounting | TODO in code; `maxTotalRecords` cap provides effective bound |

---

## 5. Thread Safety Model

### Three-Thread Interaction on TM Side

```
Thread              Operations                        Sync Mechanism
─────────────────── ───────────────────────────────── ──────────────────────────
Mailbox thread      collect() → sampleRecord()        Single-writer for buffer
                    startRound() → buffer.reset()     writes, rate limiter, drop
                    completeRoundAndCollect()          counters (all plain fields)
                    → buffer.drainAndClear()

Timer thread        disableSampling()                 volatile write to
                    (fires after sampling window)     samplingEnabled flag

RPC handler thread  requestDataSamples()              AtomicReference CAS for
                    (initial entry point)             samplingState transition
                    Submits startRound as mailbox      (IDLE → SAMPLING)
                    action, does NOT directly
                    access buffer
```

### Key Invariants

1. **`samplingEnabled`** is `volatile` — only cross-thread shared mutable state in the hot path. Timer thread writes `false`; mailbox thread reads.
2. **`samplingState`** uses `AtomicReference<SamplingRoundState>` with `compareAndSet(IDLE, SAMPLING)` — prevents concurrent RPCs from starting overlapping rounds.
3. **Rate limiter and drop counters** are plain `int`/`long` — mailbox-thread-only access, no synchronization needed.
4. **`BoundedSampleBuffer`** uses `ReentrantLock` as defense-in-depth; in practice, `tryLock()` always succeeds on the mailbox thread (no contention).
5. **`completeRoundAndCollect(roundId)`** checks `roundId` against `currentRoundId` — prevents stale callbacks from draining a newer round's data.
6. **Timer registration happens inside the startRound mailbox action** — ensures the sampling window starts only after `startRound()` has enabled sampling, not when the RPC arrives.

### JM-Side Synchronization

- `VertexDataSampleTracker`: all cache operations under `synchronized(lock)`
- `DataSampleRequestCoordinator`: all pending-round operations under `synchronized(lock)`

---

## 6. Request Lifecycle (End-to-End)

```
1. WebUI/Client → GET /jobs/:jid/vertices/:vid/data-sample

2. JobVertexDataSampleHandler:
   a. Pre-check: is vertex/subtask terminated? → return FAILED + TASK_TERMINATED
   b. VertexDataSampleTracker.getJobVertexStats(jobId, vertex):
      - Cache hit + fresh → return cached VertexDataSampleStats
      - Cache miss or stale → triggerSamplingInternal() (async), return cached or empty
   c. Empty → return PENDING (first request, waiting for round to complete)
   d. Present → DataSampleResponseBody.fromStats(stats, stale, subtaskFilter, maxRecords)

3. triggerSamplingInternal():
   a. Check pendingStats deduplication
   b. Resolve ResourceManagerGateway → get TM locations for vertex's executions
   c. DataSampleRequestCoordinator.triggerDataSampleRequest(executionsWithGateways, window, capacity)

4. Coordinator:
   a. Check shutdown, concurrent round limit
   b. Assign roundId, create PendingSamplingRound
   c. For each TM group: gatewayFuture → gateway.requestDataSamples(attemptIds, request, timeout)
   d. Schedule global timeout via delayedExecutor

5. TaskExecutor.requestDataSamples():
   a. Look up each task via taskSlotTable
   b. Cast to DataSampleableTask → task.requestDataSamples(roundId, window, capacity)
   c. Wrap each future with .exceptionally() for graceful per-task failure
   d. CompletableFuture.allOf() → assemble TaskDataSampleResponse

6. StreamTask.requestDataSamples():
   a. AtomicReference CAS: IDLE → SAMPLING (reject if already SAMPLING)
   b. Mailbox action: startRound() on each SamplingRecordWriterOutput + registerTimer
   c. During window: collect() → sampleRecord() (forward-first, rate-limited)
   d. Timer fires: disableSampling() (volatile), then mailbox action: completeRoundAndCollect()
   e. Results aggregated, samplingState → IDLE, resultFuture completed

7. Coordinator receives per-TM responses:
   a. handleSuccessfulResponse() / handleFailedResponse() → track in PendingSamplingRound
   b. checkAndComplete(): all groups resolved → assembleStats()
   c. Status: all success → COMPLETE, mixed → PARTIAL, all failed → FAILED
   d. Apply maxTotalRecords cap (5000) with proportional fair truncation
   e. Cache result in VertexDataSampleTracker

8. Next poll: returns cached result (with stale=true if refreshInterval exceeded)
```

---

## 7. Configuration Summary

| Key | Type | Default | FLIP Range |
|-----|------|---------|------------|
| `rest.data-sampling.enabled` | Boolean | `false` | — |
| `rest.data-sampling.max-sample-rate` | Integer | `100` | [1, 10000] (validated) |
| `rest.data-sampling.max-record-length` | Integer | `10000` | > 0 (validated) |
| `rest.data-sampling.sampling-window` | Duration | `3s` | [1s, 30s] (FLIP range; runtime validated via SamplingConfig) |
| `rest.data-sampling.timeout` | Duration | `30s` | > 0 |
| `rest.data-sampling.cache-ttl` | Duration | `5min` | > 0 |
| `rest.data-sampling.max-response-bytes` | Integer | `10485760` (10MB) | [1MB, 50MB] (validated) |

Hardcoded internal limits:
- Max concurrent rounds per JM: 5
- Max total records per response: 5000
- Ghost ID buffer size: 10
- Per-subtask buffer capacity: `min(ceil(maxSampleRate * samplingWindow / 1000), 1000)`

---

## 8. Key Design Decisions for Review

### 8.1 Standalone Coordinator (Not Extending TaskStatsRequestCoordinator)

**FLIP rationale**: The base class's `handleFailedResponse()` discards ALL collected results — incompatible with partial success. A unified `checkAndComplete()` path resolves all three states (COMPLETE/PARTIAL/FAILED) consistently.

**Review focus**: Verify the three-set disjoint invariant (`remainingGroups`, `successResults`, `failedSubtaskInfos`); verify that PARTIAL/FAILED complete the future normally (not exceptionally) for cacheability.

### 8.2 Mailbox-Only Buffer Access (Deviation from FLIP's Three-Thread Model)

**FLIP design**: `completeRoundAndCollect()` runs on ScheduledExecutor thread, acquires lock to read buffer.

**Implementation**: Both `startRound()` and `completeRoundAndCollect()` run on mailbox thread. Timer thread only sets `volatile samplingEnabled = false`. This eliminates concurrent buffer access entirely.

**Trade-off**: Timer callback → mailbox submission adds a mailbox queue delay for result collection, but this is bounded and acceptable (same pattern as processing time timer callbacks in Flink).

**Review focus**: Verify that the timer thread's `disableSampling()` call is safe (only volatile write, no buffer access). Verify mailbox action submission from timer callback follows Flink conventions.

### 8.3 Timer Registration Inside Mailbox Action

**Problem solved**: If timer is registered on the RPC thread but `startRound()` hasn't executed yet (still in mailbox queue), the timer could fire before sampling starts, producing a shorter effective window.

**Solution**: Timer registration is inside the same mailbox action as `startRound()`, so the window starts only after sampling is enabled.

**Review focus**: Verify that `systemTimerService.registerTimer()` is safe to call from mailbox thread. Verify no timer registration leak if task shuts down between RPC and mailbox execution.

### 8.4 Per-Task Error Handling in TaskExecutor

**Problem solved**: `CompletableFuture.allOf()` fails entirely if any future completes exceptionally — `thenApply()` is skipped.

**Solution**: Each per-task future is wrapped with `.exceptionally(t -> SamplingRoundResult.failed(...))` before `allOf()`, ensuring all futures complete normally.

**Review focus**: Verify that `join()` in `thenApply()` is safe (all futures guaranteed normal completion).

### 8.5 AtomicReference for Sampling State Transition

**Problem solved**: Two concurrent RPCs could both read `samplingState == IDLE`, both proceed to start overlapping rounds.

**Solution**: `AtomicReference<SamplingRoundState>` with `compareAndSet(IDLE, SAMPLING)` — only one RPC wins; the other returns FAILED immediately.

**Review focus**: Verify state transitions: `IDLE → SAMPLING` (in requestDataSamples), `SAMPLING → IDLE` (in completeRoundAndCollect callback).

---

## 9. Hot Path Performance Impact

| State | Per-Record Overhead | Notes |
|-------|-------------------|-------|
| **Disabled** (`enabled=false`) | Zero | `SamplingRecordWriterOutput` not instantiated; plain `RecordWriterOutput` used |
| **Enabled, idle** (`samplingEnabled=false`) | ~1ns | Single volatile read in `collectAndCheckIfChained()` |
| **Active sampling** | ~50-100ns + `toString()` | Rate-limited to `max-sample-rate` per second; `toString()` is user-defined and dominates |
| **Between rounds** | Same as idle | `samplingEnabled` is `false` after round completes |

Key optimizations:
- Single `System.currentTimeMillis()` per sampled record (shared between rate limiter and SampledRecord timestamp)
- `ConcurrentHashMap` cache for `hasCustomToString()` reflection check
- `tryLock()` (non-blocking) for buffer writes
- `forward-first`: sampling runs after record forwarding, never blocks the data path

---

## 10. Test Coverage Summary

### Unit Tests (40 tests)

- **DataSampleRequestCoordinatorTest** (9 tests): complete flow, partial success, all-failed, timeout, concurrent round limit, empty executions, shutdown, ghost ID dedup
- **DataSampleResponseBodyTest** (8 tests): JSON serialization, factory methods (terminated/disabled/waiting), null field omission, fromStats conversion
- **JobVertexDataSampleHandlerTest** (7 tests): running vertex, finished vertex, mixed subtasks, waiting (no stats), subtask filter, maxRecords filter, staleness detection
- **SamplingRecordWriterOutputTest** (9 tests): basic sampling, no data, roundId mismatch, disableSampling, buffer full drops, inactive before startRound, round isolation, timestamps, multiple rounds
- **BoundedSampleBufferTest** (7 tests): add/drain, empty drain, capacity enforcement, drain clears, reset changes capacity, zero capacity, unmodifiable result

### Integration Tests (5 tests)

- **DataSamplingITCase**: MiniCluster with `parallelism=2`, REST client polling, COMPLETE status verification, multi-subtask data validation, subtask filter, maxRecords filter, invalid vertex 404

### Not Covered (Known Gaps)

- `VertexDataSampleTracker` integration test (tracker → coordinator → mock TM)
- `StreamTask.requestDataSamples()` unit test (requires mailbox + timer service setup)
- Multi-output (side output) scenario in IT case
- JM failover during active round
- Task cancellation during active round (resource leak verification)
- High-parallelism truncation (`maxTotalRecords` cap) in IT case

---

## 11. Known Limitations and TODOs

1. **`maxResponseBytes` byte accounting not implemented**: The `max-response-bytes` config is validated but not enforced at the coordinator level. The `maxTotalRecords` (5000) cap provides an effective bound. Marked as TODO in `DataSampleRequestCoordinator`.

2. **Cache TTL uses `expireAfterAccess`**: Each polling request refreshes the TTL. Under continuous WebUI polling, entries may live longer than expected. This is consistent with FlameGraph's behavior.

3. **No per-vertex enable/disable**: Sampling is a global switch. Source vertices that expose raw PII cannot be individually excluded. Listed as Future Work in FLIP.

4. **WebUI not included**: This PR covers Phases 1-2 (backend). Phase 3 (WebUI) is separate.

5. **`VertexDataSampleStats.endTimestamp` is primitive `long`**: The FLIP defines `@Nullable Long`, but the internal class uses primitive. The conversion to `@Nullable Long` happens at the REST boundary in `DataSampleResponseBody.fromStats()` (maps `0` to `null`).

---

## 12. Files Modified in Existing Codebase

The following pre-existing files were modified (not new files):

| File | Change | Impact |
|------|--------|--------|
| `RestOptions.java` | Added 7 config options | Additive only |
| `OperatorChain.java` | Conditional `SamplingRecordWriterOutput` instantiation in `createChainOutputs()` | Gated by `rest.data-sampling.enabled`; zero change when disabled |
| `StreamTask.java` | Implement `DataSampleableTask`, add `requestDataSamples()` + `samplingState` field | New interface implementation; no change to existing methods |
| `TaskExecutor.java` | Add `requestDataSamples()` method | New RPC handler; no change to existing methods |
| `TaskExecutorGateway.java` | Extend `TaskExecutorDataSampleGateway` | Interface addition |
| `ResourceManagerGateway.java` | Add `requestTaskExecutorDataSampleGateway()` | New method for TM gateway resolution |
| `ResourceManager.java` | Implement gateway resolution | New method |
| `WebMonitorEndpoint.java` | Handler registration + tracker initialization | Follows existing pattern (FlameGraph) |
| `Task.java` | Add `getDataSampleableTask()` accessor | Accessor only |

No changes to: checkpointing, network stack, serialization framework, or any existing public API classes.
