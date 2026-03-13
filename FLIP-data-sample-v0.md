
# FLIP-XXX: Runtime Data Sampling for Job Vertices with WebUI Visualization

## Summary

Support on-demand runtime data sampling at the output of any job vertex (operator chain), with REST API trigger and Flink WebUI visualization. Users can dynamically inspect intermediate data flowing through any vertex in a running job without restarting the job or modifying the topology.

## Status

- **Status**: Draft
- **Type**: Feature
- **JIRA**: (TBD)
- **Authors**: (TBD)

---

## Motivation

Debugging data issues in Apache Flink streaming jobs is a long-standing operational pain point. When users need to inspect intermediate data at runtime — for example, verifying transformations, checking data quality, or debugging unexpected output — all current approaches require either **job modification and restart** (add `print()` sink, `executeAndCollect()`, log statements) or **external infrastructure** (Kafka sink, external storage).

The Flink community already has a proven architecture for on-demand dynamic sampling — the **FlameGraph** feature ([FLINK-13550](https://issues.apache.org/jira/browse/FLINK-13550)). FlameGraph dynamically samples thread stack traces via REST API → JM coordination → TM-side sampling → WebUI rendering, all without restarting the job.

**Proposal**: Introduce a native **runtime data sampling** capability that reuses the FlameGraph architecture pattern, allowing users to dynamically sample records at the output of any job vertex and visualize them in the Flink WebUI.

---

## Goals

1. Support on-demand data sampling at the output side of any job vertex (operator chain) in a running job.
2. Provide a REST API to trigger and retrieve sampling results.
3. Visualize sampled records in the Flink WebUI using `toString()` for readable types. **Schema-Aware Formatting** for Flink Table/SQL internal binary formats (e.g., `BinaryRowData`) is designed separately — see [FLIP-SQL-data-sampling.md](./FLIP-SQL-data-sampling.md).
4. Ensure **minimal overhead**: zero additional code path when disabled; minimal overhead (single `volatile` check per record) when enabled but idle; bounded overhead during active sampling rounds.
5. Provide safety mechanisms: configurable sample size limits, TTL-based auto cleanup, and memory usage caps.
6. Reuse the proven FlameGraph architecture pattern for minimal code invasiveness.

## Non-Goals (Initial Scope)

- Data masking / PII redaction
- Binary data visualization (a fallback message will be shown for raw `byte[]`)
- Persistent storage of sampled data
- Sampling at operator input side (only output side)
- Sampling within a chained operator (only at chain output level)

---

## Public Interfaces

> **Stability note**: All Java classes introduced in this FLIP are annotated `@Internal` — they are implementation details subject to change without notice. The REST API is documented but follows Flink's existing convention of no strict cross-version stability guarantees.

### New Configuration Options

All new configuration options are added to `RestOptions.java`, following the established `rest.flamegraph.*` naming pattern:

| Configuration Key | Type | Default | Description |
|---|---|---|---|
| `rest.data-sampling.enabled` | Boolean | `false` | Master switch. When `false`, no sampling infrastructure is initialized. |
| `rest.data-sampling.max-sample-rate` | Integer | `100` | Maximum records sampled per subtask per second (approximate per-second counter, upper bound). Range: 1–10000. Per-round buffer capacity derived as `max-sample-rate × sampling-window` (capped at 1000). |
| `rest.data-sampling.max-record-length` | Integer | `10000` | Maximum character length for `toString()` per record. Longer strings are truncated. |
| `rest.data-sampling.cache-ttl` | Duration | `5 min` | TTL for cached sampling results. Expired entries cleaned up lazily via Guava Cache `expireAfterWrite`. |
| `rest.data-sampling.sampling-window` | Duration | `3 s` | Duration of each sampling round's capture window. Range: 1s–30s. |
| `rest.data-sampling.timeout` | Duration | `30 s` | Overall timeout for a complete sampling round (same timeout pattern as FlameGraph's `requestThreadInfoSamples()`). |
| `rest.data-sampling.max-response-bytes` | Integer | `10485760` | Hard upper bound (bytes) on the final JSON REST response body. Default: 10 MB. Range: 1 MB – 50 MB. Must not exceed `rest.server.max-content-length`. |

Design notes:
- **Default disabled** (unlike FlameGraph) because sampling intercepts the data hot path and may expose sensitive data.
- **`max-sample-rate`** protects against expensive `toString()` calls. Default 100/s/subtask is conservative to prioritize production cluster protection. Named `max-` to clarify it is an upper bound, not a precise rate.
- **Internal caps**: Total records across all subtasks per response capped at 5000 (hardcoded). At most 5 concurrent active sampling rounds per JM (hardcoded). These can be exposed as configs in future iterations if needed.
- **Cache key**: `(jobId, vertexId)`. Query parameters (`subtaskIndex`, `maxRecords`) are post-filters on the cached result — they do not trigger new rounds or invalidate cache.

### New REST API Endpoint

```
GET /jobs/:jobid/vertices/:vertexid/data-sample
```

Follows the same **Lazy Trigger + Polling** pattern as FlameGraph — a `GET` that returns sampling results and **may initiate a sampling round if necessary** (same semantics as FlameGraph's `GET /flamegraph`).

**Query Parameters**:

| Parameter | Type | Required | Description |
|---|---|---|---|
| `subtaskIndex` | Integer | No | Filter response to a specific subtask (post-filter only; all subtasks are always sampled). |
| `maxRecords` | Integer | No | Limit returned records per subtask in the response (post-filter on cached result). |

**Response** (`DataSampleResponseBody`):

```json
{
    "status": "COMPLETE",
    "endTimestamp": 1700000000000,
    "roundId": 42,
    "stale": false,
    "totalRecordCount": 3,
    "totalTruncated": false,
    "droppedByContention": 0,
    "droppedByRateLimit": 5,
    "errorCode": null,
    "errorMessage": null,
    "failedSubtasks": [],
    "samples": [
        {
            "subtaskIndex": 0,
            "sampleCount": 2,
            "truncated": false,
            "records": [
                {
                    "sampleTimestamp": 1700000000100,
                    "recordTimestamp": 1700000000050,
                    "data": "Row(name=Alice, age=30, city=Beijing)",
                    "dataType": "org.apache.flink.types.Row",
                    "truncated": false
                }
            ]
        }
    ]
}
```

**Status Enum**: `PENDING` | `COMPLETE` | `PARTIAL` | `FAILED` | `NO_DATA` | `DISABLED`

**Error Code Enum** (only when `status ∈ {FAILED, PARTIAL}`): `TIMEOUT` | `TM_UNREACHABLE` | `TOO_MANY_CONCURRENT_ROUNDS` | `TASK_TERMINATED` | `INTERNAL_ERROR`

**Status Enum Semantics**:

| Status | `endTimestamp` | Meaning |
|---|---|---|
| `PENDING` | `null` | Sampling triggered, waiting for first results |
| `COMPLETE` | `> 0` | Sampling round finished, all subtasks returned data |
| `PARTIAL` | `> 0` | Some subtasks succeeded, at least one failed — partial data is displayed with failed subtasks listed. Unlike FlameGraph's all-or-nothing model, partial data has full debugging value. |
| `NO_DATA` | `> 0` | Sampling round finished, no records captured (e.g., no throughput) |
| `DISABLED` | `null` | Feature disabled via configuration |
| `FAILED` | `null` | All subtasks/TMs failed or admission rejected (see `errorCode`) |

**Behavior** (Lazy Trigger + Auto-Polling, same as FlameGraph):
1. **First request**: Triggers a new sampling round asynchronously, typically returns `PENDING`.
2. **While round in progress**: Returns previous cached result (with `stale=true`) or `PENDING`.
3. **Round completed**: Returns `COMPLETE`, `PARTIAL`, or `NO_DATA`.
4. **Continuous polling**: WebUI polls every refresh cycle (default 3s). Backend returns cache until TTL expires, then triggers a new round transparently.

### Sampling Window Semantics — Round-Scoped Model

A critical difference from FlameGraph: FlameGraph's JM-initiated thread sampling is **naturally bounded**. Data sampling on the hot path is **not** — if left enabled, `SamplingOutput` continuously runs `toString()` on every record. Therefore, data sampling uses a **round-scoped** lifecycle:

1. **Trigger**: REST request + cache miss → Coordinator starts a new round with unique `roundId`.
2. **Single RPC — TM-side autonomous sampling**: Coordinator sends `requestDataSamples(roundId, samplingWindow, maxBufferCapacity)` to relevant TMs. Each TM autonomously: clears buffer → enables → captures for `sampling-window` → disables → returns via `CompletableFuture`. Same pattern as FlameGraph's `ThreadInfoSampleService`.
3. **Fill-and-stop**: Captures records until window expires or buffer reaches capacity, whichever first.
4. **Auto-disable**: `SamplingOutput` sets `samplingEnabled = false` autonomously.
5. **Cache**: Coordinator assembles results, caches with TTL. Subsequent polls return cache.
6. **New round**: Cache TTL expires + new request → repeat from step 1.

**Key properties**:
- `samplingEnabled = true` lasts at most `sampling-window` (default 3s), not indefinitely.
- Multiple WebUI clients share the same round via cache.
- At most 5 concurrent rounds per JM (hardcoded, with `rejectedRounds` metric for observability).

### Sampling Lifecycle State Machine

```
                       ┌──────────────────────────────┐
    (feature disabled) │         NOT_INSTALLED         │
                       │  SamplingRecordWriterOutput   │
                       │  not created. Zero overhead   │
                       └──────────────────────────────┘
                                     │ rest.data-sampling.enabled=true
                                     ▼
                       ┌──────────────────────────────┐
    (initial state)    │           IDLE                │
                       │ samplingEnabled=false         │
                       │ ~0 overhead (volatile check)  │
                       └──────────────────────────────┘
                                     │ REST request + cache miss/expired
                                     ▼
                       ┌──────────────────────────────┐
    (bounded window)   │    CAPTURING(roundId)         │
                       │ samplingEnabled=true          │
                       │ buffer filling (≤ window)     │
                       └──────────────────────────────┘
                            │                    │
              window expires │                    │ buffer full
                            ▼                    ▼
                       ┌──────────────────────────────┐
    (round finished)   │    COMPLETE(roundId)          │
                       │ samplingEnabled=false (auto)  │
                       │ buffer frozen, ready to read  │
                       └──────────────────────────────┘
                                     │ Coordinator collects + caches
                                     ▼
                       ┌──────────────────────────────┐
    (serving cache)    │     IDLE (cache valid)        │── polling returns
                       │ samplingEnabled=false         │   cached result
                       └──────────────────────────────┘
                                     │ cache TTL expires + new request
                                     ▼
                              new CAPTURING(roundId+1)
```

### New Java Classes

#### Data Models

```java
@Internal
public enum SampleStatus {
    PENDING, COMPLETE, PARTIAL, FAILED, NO_DATA, DISABLED
}

@Internal
public class SampledRecord implements Serializable {
    private final long sampleTimestamp;
    @Nullable private final Long recordTimestamp;
    private final String data;
    private final String dataType;
    private final boolean truncated;
    @Nullable private final String sideOutputName;
}

@Internal
public class SubtaskDataSample implements Serializable {
    private final int subtaskIndex;
    private final List<SampledRecord> records;
    private final int sampleCount;
    private final boolean truncated;
}

@Internal
public class DataSampleResponseBody implements ResponseBody {
    private final SampleStatus status;
    @Nullable private final Long endTimestamp;
    @Nullable private final Integer roundId;
    private final boolean stale;
    private final List<SubtaskDataSample> samples;
    private final int totalRecordCount;
    private final boolean totalTruncated;
    private final long droppedByContention;
    private final long droppedByRateLimit;
    @Nullable private final SampleErrorCode errorCode;
    @Nullable private final String errorMessage;
    private final List<FailedSubtaskInfo> failedSubtasks;
}

@Internal
public class FailedSubtaskInfo implements Serializable {
    private final int subtaskIndex;
    private final SampleErrorCode errorCode;
    private final String errorMessage;
}

@Internal
public enum SampleErrorCode {
    TIMEOUT, TM_UNREACHABLE, TOO_MANY_CONCURRENT_ROUNDS, TASK_TERMINATED, INTERNAL_ERROR
}

/** TM-local behavior config, immutable after construction. Set once at task startup. */
@Internal
public class SamplingConfig implements Serializable {
    private final int sampleRate;
    private final int maxRecordLength;
    private final long maxResponseBytes;
}

/** Per-round RPC parameters sent from JM to TM. Same pattern as ThreadInfoSamplesRequest. */
@Internal
public class DataSampleRequest implements Serializable {
    private final int roundId;
    private final Duration samplingWindow;
    private final int maxBufferCapacity;
}

/** Task-level sampling result, one per subtask. */
@Internal
public class SamplingRoundResult implements Serializable {
    private final int roundId;
    private final List<SampledRecord> records;
    private final long droppedByContention;
    private final long droppedByRateLimit;
    private final SampleStatus status;
}

/** TM-level aggregation, one per TM. */
@Internal
public class TaskDataSampleResponse implements Serializable {
    private final Map<ExecutionAttemptID, SamplingRoundResult> results;
}

/** JM-level assembled statistics, cached by VertexDataSampleTracker. */
@Internal
public class VertexDataSampleStats implements Statistics {
    private final SampleStatus status;
    private final long endTimestamp;
    private final int roundId;
    private final List<SubtaskDataSample> samples;
    private final int totalRecordCount;
    private final boolean totalTruncated;
    private final long droppedByContention;
    private final long droppedByRateLimit;
    private final List<FailedSubtaskInfo> failedSubtasks;
    @Nullable private final SampleErrorCode errorCode;
    @Nullable private final String errorMessage;
}
```

#### REST Endpoint Classes

```java
/** GET endpoint, same pattern as FlameGraph. */
@Internal
public class JobVertexDataSampleHeaders
        implements RuntimeMessageHeaders<
            EmptyRequestBody, DataSampleResponseBody, JobVertexDataSampleParameters> {
    // URL: /jobs/:jobid/vertices/:vertexid/data-sample
}

@Internal
public class JobVertexDataSampleParameters extends JobVertexMessageParameters {
    public final SubtaskIndexQueryParameter subtaskIndexQueryParameter;
    public final MaxRecordsQueryParameter maxRecordsQueryParameter;
}

/** Same pattern as JobVertexFlameGraphHandler: pre-checks terminal state, provides disabledHandler(). */
@Internal
public class JobVertexDataSampleHandler
        extends AbstractJobVertexHandler<DataSampleResponseBody, JobVertexDataSampleParameters> {
    // 1. Pre-check: is vertex/subtask terminated?
    // 2. Delegate to VertexDataSampleTracker for lazy-trigger + cache lookup
    // 3. Map to DataSampleResponseBody
}
```

#### TM-Side Interfaces

```java
/**
 * Interface for tasks supporting runtime data sampling. StreamTask implements this interface.
 * In Flink 2.x, BATCH mode jobs also execute via StreamTask subclasses, so SamplingOutput
 * is installed on BATCH tasks as well (best-effort — task may terminate before window expires).
 */
@Internal
public interface DataSampleableTask {
    CompletableFuture<SamplingRoundResult> requestDataSamples(
            int roundId, Duration samplingWindow, int maxBufferCapacity);
    SamplingRoundState getSamplingState();
}

/**
 * RPC gateway mixed into TaskExecutorGateway, same pattern as TaskExecutorThreadInfoGateway.
 */
@Internal
public interface TaskExecutorDataSampleGateway extends RpcGateway {
    CompletableFuture<TaskDataSampleResponse> requestDataSamples(
            Collection<ExecutionAttemptID> taskExecutionAttemptIds,
            DataSampleRequest request, @RpcTimeout Duration timeout);
}
```

---

## Proposed Changes

### Architecture Overview

The architecture follows the **proven FlameGraph pattern** with 5 layers:

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 1: REST API                                          │
│  JobVertexDataSampleHandler (GET /data-sample)              │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│  Layer 2: Tracker (JM-side)                                 │
│  VertexDataSampleTracker — lazy trigger + Guava cache       │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│  Layer 3: Coordinator (JM-side)                             │
│  DataSampleRequestCoordinator — fan-out RPC, collect,       │
│  assemble, apply total records cap                          │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│  Layer 4: TaskManager Execution                             │
│  TaskExecutor + SamplingRecordWriterOutput                  │
│  (RecordWriterOutput subclass) + BoundedSampleBuffer        │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│  Layer 5: Data Model + Serialization                        │
│  SamplingConfig / DataSampleRequest / SampledRecord /       │
│  SamplingRoundResult / TaskDataSampleResponse /             │
│  VertexDataSampleStats / DataSampleResponseBody             │
└─────────────────────────────────────────────────────────────┘
```

Key difference from FlameGraph: FlameGraph uses `ThreadInfoSampleService` with recursive scheduled sampling, while data sampling uses `SamplingRecordWriterOutput` (a `RecordWriterOutput` subclass) with a `BoundedSampleBuffer`.

### Core Component: `SamplingRecordWriterOutput` (TM-side)

A `RecordWriterOutput` subclass that extends the network output with sampling capability. Uses **inheritance** (`extends RecordWriterOutput`) for type compatibility with `OperatorChain.streamOutputs[]` (typed `RecordWriterOutput<?>[]`).

```java
@Internal
public class SamplingRecordWriterOutput<OUT> extends RecordWriterOutput<OUT> {

    private volatile boolean samplingEnabled;
    private final BoundedSampleBuffer<SampledRecord> sampleBuffer;
    // Reflection-based detection: caches whether a class has overridden toString().
    // Uses ConcurrentHashMap<Class<?>, Boolean> (or ClassValue) for thread-safe caching.
    private static boolean hasCustomToString(Class<?> clazz) { /* cached reflection check */ }

    @Override
    public boolean collectAndCheckIfChained(StreamRecord<OUT> record) {
        boolean result = super.collectAndCheckIfChained(record);  // forward-first
        if (samplingEnabled) {
            sampleRecord(record);
        }
        return result;
    }

    @Override
    public <X> boolean collectAndCheckIfChained(OutputTag<X> outputTag, StreamRecord<X> record) {
        boolean result = super.collectAndCheckIfChained(outputTag, record);  // forward-first
        if (samplingEnabled) {
            sampleSideOutputRecord(outputTag, record);
        }
        return result;
    }

    private String convertToString(Object value) {
        if (value instanceof byte[]) {
            return "[Binary data: " + ((byte[]) value).length + " bytes]";
        }
        // Schema-Aware Formatting for RowData — see FLIP-SQL-data-sampling.md
        if (!hasCustomToString(value.getClass())) {
            return "[" + value.getClass().getSimpleName()
                    + ": toString() not overridden — consider enabling Schema-Aware Formatting]";
        }
        String str = value.toString();
        return truncateIfNeeded(str, maxRecordLength);
    }
    // All other Output methods delegate directly to super
}
```

**Key design points**:
- **Forward-first**: `super.collectAndCheckIfChained()` executes before `sampleRecord()` — record forwarding is not blocked. Sampling runs after forwarding on the same operator thread, adding bounded CPU overhead controlled by `max-sample-rate` + `sampling-window`.
- **BoundedSampleBuffer**: Fixed-capacity buffer with fill-and-stop semantics. Internally an `ArrayList` with capacity guard.
- **Unreadable toString() detection**: Uses reflection (`Class.getMethod("toString").getDeclaringClass() != Object.class`) cached via `ConcurrentHashMap` or `ClassValue` to detect classes without a custom `toString()` override. This is more precise than regex-based detection, which suffers from false positives (e.g., `user@deadbeef` matches the `ClassName@hashcode` pattern).
- **Exception isolation**: `toString()` failures caught at DEBUG level — never block record forwarding.
- **Throttling**: Per-second counter approximation (counter resets at wall-clock second boundaries), bounded by `max-sample-rate`. Simpler and cheaper than token-bucket.
- **Drop counters**: `droppedByContention` (tryLock failed) and `droppedByRateLimit` (rate limit exceeded) included in response for observability.
- **Installation**: Wraps the `RecordWriterOutput`(s) in `OperatorChain.createChainOutputs()` — the chain's tail end (network output boundary). Captures records **after** all chained operators have processed them. `mainOperatorOutput` is NOT wrapped — it points to the chain entry, not exit.
- **Side outputs**: Each side output `RecordWriterOutput` is independently wrapped. `OutputTag` info recorded in `SampledRecord.sideOutputName`.
- **Thread safety**: See Appendix A for the full three-thread model. Summary: `samplingEnabled` is `volatile`; buffer uses single-writer (mailbox thread) model; `ReentrantLock` guards the read path; `startRound()` runs on mailbox thread to avoid clear-vs-write races.

**Installation pseudocode**:
```java
// In OperatorChain.createChainOutputs()
private void createChainOutputs(...) {
    for (int i = 0; i < outputsInOrder.size(); ++i) {
        RecordWriterOutput<?> recordWriterOutput;
        if (dataSamplingEnabled) {
            recordWriterOutput = closer.register(
                new SamplingRecordWriterOutput<>(
                    recordWriterDelegate.getRecordWriter(i),
                    outSerializer, sideOutputTag, supportsUnalignedCheckpoints,
                    samplingConfig));
        } else {
            recordWriterOutput = createStreamOutput(...);
        }
        this.streamOutputs[i] = recordWriterOutput;  // type-safe: extends RecordWriterOutput
        recordWriterOutputs.put(output.getDataSetId(), recordWriterOutput);
    }
}
```

### JM-Side: Tracker + Coordinator

**`VertexDataSampleTracker`** — implements `VertexStatsTracker<VertexDataSampleStats>`:
- Guava Cache with TTL-based expiration
- Lazy trigger: only initiates sampling when cache is empty or expired
- Pending request deduplication
- Groups executions by TM location for batched RPC

**`DataSampleRequestCoordinator`** — a **standalone coordinator** (does NOT extend `TaskStatsRequestCoordinator`) with **native partial success semantics**:

Standalone implementation because the base class assumes all-or-nothing semantics — `handleFailedResponse()` discards the entire pending request, which is incompatible with partial success. Data sampling results from individual subtasks are independently valuable (unlike FlameGraph where partial thread samples are meaningless).

Core behavior:
- Fan-out single RPC to all relevant TMs (TM-side autonomous sampling lifecycle)
- Collect responses, assemble `VertexDataSampleStats`
- **Partial success**: When individual TMs fail, continue collecting from remaining TMs. Uses separate `successGroups` and `failedGroups` maps with a unified `checkAndComplete()` resolution path.
- Apply internal `maxTotalRecords` cap (5000, proportional fair distribution across subtasks)
- Apply `maxResponseBytes` hard limit (proportional fair truncation if exceeded)
- Self-managed timeout and ghost ID deduplication (following base class patterns)

**Partial success status logic**:
- All TM groups succeeded → `COMPLETE` (or `NO_DATA` if all buffers empty)
- Some succeeded, some failed → `PARTIAL` (with `failedSubtasks` list)
- All failed → `FAILED`
- `PARTIAL` completes the future **normally** (not exceptionally) so the result is cacheable.
- `FAILED` also completes the future **normally** with a `FAILED` status — not `completeExceptionally()`. This ensures the result is cacheable and avoids triggering redundant retry rounds during the cache TTL window.

**`PendingDataSampleRequest`** tracks resolution via three disjoint sets: `remainingGroups`, `successGroups`, `failedGroups`. A group moves from `remainingGroups` to exactly one of the other two — no group can be both succeeded and failed.

### TM-Side: TaskExecutor Enhancement

```java
// TaskExecutor.requestDataSamples() — same pattern as requestThreadInfoSamples()
for each attemptId in taskExecutionAttemptIds:
    task = taskSlotTable.getTask(attemptId)
    if task instanceof DataSampleableTask:
        futures.add(task.requestDataSamples(roundId, samplingWindow, maxBufferCapacity))
return CompletableFuture.allOf(futures).thenApply(ignored -> assembleResponse(futures))
```

**`StreamTask.requestDataSamples()`**:
```java
public CompletableFuture<SamplingRoundResult> requestDataSamples(
        int roundId, Duration samplingWindow, int maxBufferCapacity) {
    CompletableFuture<SamplingRoundResult> resultFuture = new CompletableFuture<>();

    // Step 1: startRound() on mailbox thread (same thread as collect(), no race)
    mainMailboxExecutor.execute(
        () -> samplingOutput.startRound(roundId, maxBufferCapacity),
        "Start data sampling round " + roundId);

    // Step 2: Schedule result collection after sampling window
    scheduledExecutor.schedule(
        () -> {
            SamplingRoundResult result = samplingOutput.completeRoundAndCollect(roundId);
            resultFuture.complete(result);
        },
        samplingWindow.toMillis(), TimeUnit.MILLISECONDS);

    return resultFuture;
}
```

### WebUI: New "Data Sample" Tab

Follows the same **automatic periodic polling** pattern as FlameGraph:
- Subscribes to `jobLocalService.jobWithVertexChanges()`, driven by global `StatusService.refresh$` (default 3s).
- Uses `[ngSwitch]` on `status` enum for rendering (not `endTimestamp` sentinels as FlameGraph).

UI features:
- **Auto-refresh**: Live-updating data without manual Refresh button.
- **Freshness indicator**: "Measurement: N ago" same as FlameGraph.
- **Subtask selector**: Dropdown to filter by subtask or show all.
- **Record table**: Columns — #, Record Timestamp, Sample Time, Data, Type.
- **Truncation indicators**: Orange tags for truncated records/subtasks.
- **Status display**: `PENDING` → "Waiting for samples...", `NO_DATA` → "No data captured", `PARTIAL` → yellow warning banner listing failed subtask indices, `DISABLED` → "Sampling disabled", `FAILED(*)` → error with retry hint.
- **Stale indicator**: Blue "Refreshing..." badge when returning cached result while new round is in progress.

### Performance Overhead Control

| State | Overhead |
|---|---|
| **Disabled** | Zero: `SamplingRecordWriterOutput` not instantiated |
| **Enabled, idle** | Minimal: single `volatile` check per record, no allocation |
| **Active sampling** | Bounded: ≤ `max-sample-rate` × `toString()` cost per subtask per second, for at most `sampling-window` |
| **Between rounds** | Same as idle: `samplingEnabled = false` |

Additional safety mechanisms:
- Per-subtask buffer limit (capped at 1000), internal total cap (5000 records), record length limit, byte-level response cap, TTL cache eviction
- Forward-first design, exception isolation, round-scoped auto-disable, global concurrent round limit (5)
- Default disabled, no persistent storage, REST auth/authz inherited

> ⚠️ **Security Warning**: Sampled data may contain PII or sensitive business data. Source vertices are particularly sensitive — they capture raw data before any filtering/masking. Per-vertex sampling control is listed as Future Work.

### Backpressure Behavior

Under severe backpressure, `output.collect()` may block for seconds. If blocking exceeds `sampling-window`, sampling auto-disables → round may produce few or zero records (`NO_DATA`). Under moderate backpressure, some records are sampled between episodes. This is expected behavior — users should check backpressure status when sampling yields few results.

### MultipleInputStreamTask Compatibility

`MultipleInputStreamTask` uses the same `OperatorChain` infrastructure. `SamplingOutput` is installed on each `RecordWriterOutput` in `createChainOutputs()` — no special handling needed.

---

## Correctness and Overhead Contract

| State | Overhead | Correctness Guarantee |
|---|---|---|
| **Disabled** | Zero | REST returns `DISABLED` |
| **Enabled, idle** | ~54ns/record (volatile check + virtual dispatch) | No data captured |
| **Enabled, active** | Bounded by `max-sample-rate` × `toString()` cost | Fresh data from current round |
| **Between rounds** | Same as idle | Cached result until TTL |

> **Consistency boundary**: Sampled data is **best-effort observational data** and is **not checkpoint-consistent**. This is a diagnostic tool, not a data extraction mechanism.

**Benchmark targets** (to be validated):
- Disabled: < 1% throughput difference vs. baseline
- Enabled-idle: < 2% throughput difference
- Active sampling (default config): < 3% throughput impact
- Max active duration: at most `sampling-window`
- Memory: < 10MB per subtask with default config

## Compatibility, Deprecation, and Migration Plan

- **No breaking changes**: Purely additive — new REST endpoints, configurations, and classes.
- **No deprecations, no migration required**: Default disabled.
- **Binary compatibility**: All new classes are `@Internal`. REST API follows Flink's convention — documented but without strict cross-version stability.
- **WebUI compatibility**: New tab added alongside existing tabs.
- **Documentation deliverables**: Configuration reference (auto-generated), REST API documentation, release notes entry.

## Test Plan

### Functional Tests (FLIP scope)

1. **E2E sampling flow**: DataStream job, trigger sampling via REST, verify `COMPLETE` response with correct records.
2. **Table/SQL job**: Verify `BinaryRowData` records display with Schema-Aware format (deferred to [FLIP-SQL-data-sampling.md](./FLIP-SQL-data-sampling.md)).
3. **Multi-TM scenario**: Verify fan-out RPC and result assembly across multiple TaskManagers.
4. **Disabled mode**: No `SamplingOutput` instantiated, correct `DISABLED` response.
5. **Partial success E2E**: Kill one TM during active round → `PARTIAL` status with data from surviving TMs and `failedSubtasks` listing the killed TM's subtasks.
6. **BATCH mode**: Verify sampling works on BATCH tasks; task completion before window → early round completion; already terminated task → `TASK_TERMINATED`.
7. **Installation scope**: Chain `Source → Map → Filter → NetworkOutput`, verify exactly one sampled record per input record (not 3) — confirming `SamplingOutput` captures final output after all chained transformations.
8. **Low-throughput / NO_DATA**: Verify `NO_DATA` status when no records captured during window.
9. **Concurrent round limit**: Verify limit enforcement and `TOO_MANY_CONCURRENT_ROUNDS` error.
10. **Auto-disable**: Verify `samplingEnabled = false` after `sampling-window` expires.
11. **Stale indicator**: Verify `stale=true` when returning cached result while new round triggers.
12. **High-parallelism truncation**: Verify `totalTruncated` and proportional fair strategy.
13. **JM failover**: Rounds in progress are abandoned; new requests start fresh.
14. **Task cancellation**: No resource leak during active sampling round.
15. **Unreadable toString() degradation**: Custom POJO without `toString()` override → degradation message displayed.

### Performance Benchmarks

1. Three-state overhead: disabled / enabled-idle / active — verify targets.
2. Bounded memory consumption validation.
3. Throttling effectiveness under high-throughput workloads.
4. Round lifecycle: active duration bounded by `sampling-window`.

> See **Appendix C** for the full implementation-level test checklist.

## Performance Benchmark Results

### Methodology

- **Topology**: `NumberSequenceSource → rebalance → Map → DiscardingSink`, operator chaining disabled.
- **Map function**: ~1μs CPU load per record (input-dependent XOR loop with volatile sink to prevent JIT elimination), simulating the lightest realistic ETL workload.
- **Environment**: macOS, standalone single-TM cluster, parallelism=4, TaskManager memory=4096m.
- **Metrics**: Flink REST API aggregated `numRecordsOutPerSecond` (sum across all subtasks).
- **Protocol**: 90s JIT warmup → 6 collection rounds at 60s intervals → discard round 1 (Meter window warmup) → average rounds 2-6.

### Three-State Throughput Results

| Scenario | Configuration | Avg Throughput (records/s) | Relative to Baseline |
|----------|---------------|--------------------------|---------------------|
| **A. Baseline** | `data-sampling.enabled: false` | 1,201,794 | 100% |
| **B. Enabled-Idle** | `data-sampling.enabled: true`, no API calls | 1,182,556 | **98.40% (-1.60%)** |
| **C. Active Sampling** | `data-sampling.enabled: true`, continuous sampling | 1,172,342 | **97.55% (-2.45%)** |

- Subtask skew: 0% across all scenarios (rebalance ensures uniform distribution).
- Inter-round variance: < 1% (stable and reproducible).

### Per-Record Absolute Overhead

| Comparison | Extra Latency per Record | Breakdown |
|------------|-------------------------|-----------|
| A → B (Idle) | ~54ns | Virtual method dispatch (`super` delegation) + `volatile` read |
| A → C (Active) | ~84ns | Idle overhead + rate limiter check (~30ns marginal) |
| B → C (Sampling increment) | ~30ns | Rate limiter branch; `toString()` called only 100 times/s/subtask |

### Interpretation

The idle overhead (~54ns) is higher than a single `volatile` read (~1ns) because `SamplingRecordWriterOutput` overrides `collectAndCheckIfChained()` and delegates via `super`, adding one virtual dispatch layer. This is a fixed per-record cost independent of the processing workload:

| Workload Type | Per-Record Processing Cost | Idle Overhead (54ns) as % |
|---------------|--------------------------|--------------------------|
| Lightest ETL (~1μs, this benchmark) | ~3.3μs | 1.6% |
| Typical ETL (parsing, filtering) | ~10μs | 0.54% |
| Stateful computation (windows, joins) | ~100μs | 0.05% |
| Production workloads | ≥1ms | <0.01% |

### Conclusion

All targets met: Enabled-Idle < 2%, Active < 3%. The active sampling increment (B→C) is only 0.86%, confirming that the rate limiter, toString time budget, and bounded buffer effectively contain the sampling overhead. For typical production workloads (≥10μs/record), the total impact is < 0.5%.

## Open Questions

1. **Default `max-sample-rate` value**: The current default (100/s/subtask) prioritizes production safety over debugging convenience. Should this be higher (e.g., 500) for better out-of-box experience, or is the conservative default appropriate given that `toString()` cost is unbounded for user-defined types?

2. **Per-vertex enable/disable**: Currently, sampling is a global master switch. Should we support per-vertex configuration (e.g., disabling sampling on source vertices that expose raw PII) in the initial version, or defer to Future Work?

3. **`max-sample-rate` throttling granularity**: The current design uses approximate per-second counters (wall-clock reset). Is this sufficient, or should we use a more precise mechanism (e.g., token bucket) despite the higher overhead?

4. **Cache TTL vs. freshness expectation**: Default `cache-ttl` is 5 minutes. For rapidly changing data streams, users may expect fresher samples. Should we expose a "force refresh" parameter in the REST API, or rely on TTL expiration?

5. **Security model**: Sampled data may contain PII or sensitive business data. Should we require explicit `rest.data-sampling.enabled=true` opt-in (current design), or additionally require authentication/authorization beyond Flink's existing REST auth?

---

## Rejected Alternatives

### 1. Use OperatorEvent / CoordinationRequest Instead of RPC
`OperatorEvent` requires every operator to implement `OperatorEventHandler` — high invasiveness. The RPC approach (FlameGraph pattern) operates at `TaskExecutor` level, decoupled from operator implementation.

### 2. Implement as a Special Sink (Like CollectSink)
Modifying topology at runtime is not supported without restart. The Output decorator pattern is non-invasive.

### 3. Record Raw Bytes Instead of toString()
Serialized bytes are not human-readable. For `BinaryRowData`, the correct solution is Schema-Aware Formatting (see [FLIP-SQL-data-sampling.md](./FLIP-SQL-data-sampling.md)), not raw byte exposure.

### 4. Continuous Sampling with Configurable Rate
Continuous sampling has persistent overhead even when unused. On-demand round-scoped sampling (FlameGraph pattern) has near-zero cost outside the sampling window.

### 5. Per-Poll Enable/Disable
Toggling `samplingEnabled` on every WebUI poll creates unnecessary RPC overhead and race conditions. Round-scoped lifecycle avoids both per-poll churn and indefinite always-on sampling.

### 6. Always-On Circular Buffer (TTL-Controlled)
`samplingEnabled = true` could persist for the entire `cache-ttl` (default 5min), creating persistent `toString()` overhead on the hot path. Round-scoped model limits active sampling to a short window (default 3s).

### 7. Two-Phase RPC (start → wait → collect)
Split sampling into `startSamplingRound()` then `collectSamplingResults()`. Rejected because: (a) incompatible with `TaskStatsRequestCoordinator` state machine which assumes single RPC per TM group; (b) orphaned state risk if TM crashes between phases; (c) inter-phase timer complexity; (d) FlameGraph already demonstrates the correct pattern — TM-side autonomous sampling within a single `CompletableFuture`.

### 8. Task-level Interceptor Instead of Output Decorator
Intercepting at `processInput()` level captures **input**, not output. The `RecordWriterOutput` wrapper naturally sits at the chain's tail end, captures fully-transformed records, and handles side outputs. It follows the established Output decorator pattern (`CountingOutput`, `RecordWriterOutput`).

## Implementation Plan

| Phase | Content | Target |
|---|---|---|
| **Phase 1**: Core Infrastructure | `SamplingOutput`, `BoundedSampleBuffer`, data models, TM-side RPC | 2 weeks |
| **Phase 2**: JM Coordination + REST | `DataSampleRequestCoordinator`, `VertexDataSampleTracker`, REST handler | 2 weeks |
| **Phase 3**: WebUI | Data Sample component with auto-polling, subtask selector, status handling | 1.5 weeks |
| **Phase 4**: Testing + Docs | E2E tests, performance benchmarks, documentation | 1 week |

## Future Work

1. **Schema-Aware Formatting for Table/SQL jobs**: Structured display of `BinaryRowData` via `RowType`-based field extraction. Complete design in [FLIP-SQL-data-sampling.md](./FLIP-SQL-data-sampling.md). This will require `flink-runtime` to add `flink-table-common` as a `provided`-scope dependency (`flink-table-common` is already available at runtime via `flink-table-api-java-uber.jar`).
2. **Data masking / PII redaction**: Configurable field-level masking rules.
3. **Sampling within a chained operator**: Target a specific operator inside a chain.
4. **Input-side sampling**: Sample at operator input for before/after comparison.
5. **Record filtering**: Server-side filter predicates (e.g., `age > 30`).

## References

- [FLINK-13550](https://issues.apache.org/jira/browse/FLINK-13550): FlameGraph — proven architecture for on-demand dynamic sampling
- [`CountingOutput.java`](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/streaming/api/operators/CountingOutput.java): Output decorator pattern reference
- [`TaskStatsRequestCoordinator.java`](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/webmonitor/stats/TaskStatsRequestCoordinator.java): Base class for distributed request coordination
- [Flink REST API Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/rest_api/)

---

## Appendix A: Thread Safety Model

Three threads interact with `SamplingOutput`:

| Thread | Role | Operations | Synchronization |
|---|---|---|---|
| **Mailbox thread** | Data producer | `collect()` → `sampleRecord()` → buffer write; `startRound()` → buffer clear + enable | Single-writer: both `collect()` and `startRound()` run on mailbox thread, so buffer-write and buffer-clear never race. |
| **ScheduledExecutorService thread** | Result collector | `completeRoundAndCollect()` → read buffer + disable | Acquires `ReentrantLock` to guard buffer read. |
| **RPC handler thread** | Request dispatcher | Submits `startRound()` to mailbox + schedules `completeRoundAndCollect()` | Does NOT directly access the buffer. |

**Key invariants**:
- `samplingEnabled` is `volatile` for cross-thread visibility.
- `BoundedSampleBuffer` uses a single-writer model — only the mailbox thread writes. The write path uses `tryLock()` to avoid blocking the hot path (contention → sample dropped, counted in `droppedByContention`).
- `completeRoundAndCollect()` sets `samplingEnabled = false` **before** acquiring the lock, so the mailbox thread skips `tryLock()` entirely during buffer read — eliminating contention during the read phase.
- **Delayed callback race guard**: `completeRoundAndCollect(roundId)` compares passed `roundId` with `currentRoundId`. On mismatch (new round already started), returns empty `NO_DATA` result, preventing round N from reading round N+1's data.
- **JMM guarantees**: All configuration fields (`samplingConfig`, `sampleBuffer`) are declared `final` — JLS §17.5 guarantees visibility to any thread obtaining a reference to the fully-constructed object.

## Appendix B: Type Compatibility Analysis

**The core type challenge**: `OperatorChain.streamOutputs` is typed `RecordWriterOutput<?>[]`. The chosen approach is `SamplingRecordWriterOutput extends RecordWriterOutput`:

- **Type compatibility**: IS-A `RecordWriterOutput`, stores in `streamOutputs[]` with zero changes.
- **Zero delegation overhead for checkpoint path**: `broadcastEvent()`, `alignedBarrierTimeout()`, `abortCheckpoint()`, `flush()` are inherited, not delegated.
- **`OutputWithChainingCheck` compliance**: Inherited from `RecordWriterOutput`. Overridden `collectAndCheckIfChained()` preserves return value semantics.
- **Minimal code change**: Only `createChainOutputs()` changes.

Callers of `RecordWriterOutput`-specific methods on `streamOutputs` elements:

| Caller | Method | Notes |
|---|---|---|
| `OperatorChain.broadcastEvent()` | `broadcastEvent(event, isPriorityEvent)` | Inherited from super |
| `OperatorChain.alignedBarrierTimeout()` | `alignedBarrierTimeout(checkpointId)` | Inherited from super |
| `OperatorChain.abortCheckpoint()` | `abortCheckpoint(checkpointId, cause)` | Inherited from super |
| `OperatorChain.flushOutputs()` | `flush()` | Inherited from super |
| `StreamIterationHead.init()` | `(RecordWriterOutput<OUT>[]) getStreamOutputs()` | Type-safe: extends RecordWriterOutput |
| `createOutputCollector()` | Cast to `RecordWriterOutput<T>` | Type-safe |

**Rejected alternatives**: Composition-based wrapping (requires changing `streamOutputs` type), SamplingHook injection (violates SRP), changing `streamOutputs` to interface array (risky checkpoint subsystem refactoring).

## Appendix C: Implementation Test Checklist

These tests verify implementation-level details and should be included in PR, not in FLIP evaluation:

1. **Thread safety**: Verify `startRound()` on mailbox thread doesn't race with `collect()` buffer writes. Verify `droppedByContention` increments on lock contention. Verify `completeRoundAndCollect()` lock acquisition.
2. **Delayed callback roundId consistency**: Simulate callback for round N after round N+1 starts → verify `NO_DATA` returned.
3. **`hasCustomToString()` correctness**: Returns `false` for `Object`, `BinaryRowData` (no override); returns `true` for `Row`, `String`, custom POJOs with `toString()` override. Verify caching behavior across repeated calls.
4. **Serialization**: Verify `SamplingConfig` and `DataSampleRequest` survive `ObjectOutputStream`/`ObjectInputStream` round-trip.
5. **Configuration validation**: Negative/zero values throw `IllegalArgumentException`.
6. **Byte Accounting**: Verify `maxResponseBytes` truncation with proportional fair strategy.
7. **Partial success coordinator**: Verify three-set disjoint invariant; verify `completeAsPartial()` completes future normally (cacheable); verify last response as success + prior failures → `PARTIAL` (not `COMPLETE`); verify global timeout marks remaining groups as failed.
8. **Concurrent round admission isolation**: When `TOO_MANY_CONCURRENT_ROUNDS` returned, verify no TM has `samplingEnabled = true` for rejected request.
9. **Stale roundId semantics**: When `stale=true`, verify `roundId` matches old cached round.
10. **PARTIAL + stale WebUI rendering**: Verify both indicators display simultaneously.
11. **Concurrent clients**: Verify round sharing and deduplication.
12. **TM reconnect → partial success**: If other TMs succeed → `PARTIAL` (not `FAILED`).
13. **Schema-Aware tests**: Deferred to [FLIP-SQL-data-sampling.md](./FLIP-SQL-data-sampling.md).

## Appendix D: Coordinator Partial Success Detailed Design

The base class `TaskStatsRequestCoordinator` uses `handleFailedResponse()` as a terminal operation: it clears all collected results and completes the future exceptionally. In a 3-TM scenario where TM-B fails first and TM-C succeeds last, overriding `handleFailedResponse()` to remove failed groups creates a semantic conflict — `isComplete()` (= `pendingTasks.isEmpty()`) returns `true` during `handleSuccessfulResponse()`, triggering `completePromiseAndDiscard()` → `assembleCompleteStats()` instead of `completePromiseAsPartial()`, producing an incorrect `COMPLETE` status.

`DataSampleRequestCoordinator` eliminates this via a unified `resolveGroup()` → `checkAndComplete()` path:

```java
private void checkAndComplete(int requestId, PendingDataSampleRequest pending) {
    if (!pending.isAllGroupsResolved()) return;

    pendingRequests.remove(requestId);
    if (pending.hasAnySuccess() && pending.hasAnyFailure()) {
        pending.completeAsPartial();
    } else if (pending.hasAnySuccess()) {
        pending.completeAsSuccess();
    } else {
        pending.completeAsFailed();
    }
}
```

**Timeout group identification**: Global timeout marks **all groups still in `remainingGroups`** as failed with `TIMEOUT`. Whatever remains after per-TM successes/failures is exactly the set of non-responding TMs.

## Appendix E: Byte Accounting Details

- **Measurement**: Against final JSON UTF-8 serialized byte count of complete `DataSampleResponseBody`.
- **Estimation**: Conservative `recordJsonBytes ≈ data.length() * 4 + 200` (worst-case UTF-8 + JSON overhead). For ASCII-dominated data, actual utilization is ~25% of budget.
- **Implementation optimization**: Two-phase estimation — fast `*2` factor first; if > 80% threshold, compute actual UTF-8 length for precision.
- **Truncation**: After `maxTotalRecords` cap, estimate total bytes. If exceeded, iteratively remove last record from subtask with most records. Final serialization check confirms compliance.
