# FLIP Data Sampling — Performance Analysis

## Three-State Overhead Model

| State | Target | Implementation | Overhead |
|---|---|---|---|
| **Disabled** | Zero | `RecordWriterOutput` used directly; `SamplingRecordWriterOutput` not instantiated | **0** |
| **Enabled-Idle** | ~0 | Single `volatile` read of `samplingEnabled` per record | **~1ns/record** (x86 TSO: no CPU fence) |
| **Active Sampling** | < 3% throughput | Rate-limited `toString()` with time budget | See below |

## Enabled-Idle: Per-Record Cost

```java
boolean result = super.collectAndCheckIfChained(record);  // baseline (inlined by C2)
if (samplingEnabled) { ... }                               // ~1ns volatile read
```

Baseline `pushToRecordWriter` typically costs 200-5000ns (serialization + buffer). The 1ns volatile read is **< 0.1%** overhead.

## Active Sampling: Defense-in-Depth Rate Control

Three independent limiters protect the mailbox thread during active sampling:

| Layer | Mechanism | Config | Default |
|---|---|---|---|
| **Rate limiter** | Per-second counter | `rest.data-sampling.max-sample-rate` | 100/s |
| **toString time budget** | Cumulative `nanoTime` per second | `rest.data-sampling.tostring-budget-ms` | 50ms/s |
| **Buffer capacity** | Bounded buffer, drop on full | Derived from sampling window | ~300 |

At the default 100 samples/s, 99.99%+ of records hit only the volatile read. The remaining ~0.01% execute `sampleRecord()`.

## Per-Sample Operation Costs

| Operation | Cost | Notes |
|---|---|---|
| `System.currentTimeMillis()` | ~25ns | Linux vDSO; one call per sample |
| Rate limit check | ~2ns | Integer compare + branch |
| `System.nanoTime()` (budget check) | ~25ns | Monotonic; two calls per sample |
| `ClassValue.get()` (toString check) | ~10-15ns | JVM-native per-class cache with WeakReference |
| `value.toString()` | **user-dependent** | 5ns (String) to 10μs+ (complex POJO) |
| `new SampledRecord(...)` | ~20-30ns | Short-lived TLAB allocation |
| `BoundedSampleBuffer.tryAdd()` | ~2ns | Lock-free; plain ArrayList bounds check |
| **Total (excl. toString)** | ~100ns | Fixed overhead per sampled record |

The `toString()` time budget (default 50ms/s) caps the worst case: even if `toString()` costs 10ms per call, at most 5 calls/s execute before the budget cuts off further sampling for that second.

## Key Design Decisions

**Why `toString()` on the hot path instead of deferred?**
Flink operators may reuse record objects (`ReusingDeserializationDelegate`). Storing a reference and calling `toString()` later would read mutated data. Immediate `toString()` is the only correct approach.

**Why no lock in `BoundedSampleBuffer`?**
All buffer operations (`tryAdd`, `drainAndClear`, `reset`) run on the mailbox thread. The lock was defense-in-depth but adds ~15-30ns CAS overhead per sample with zero contention. Removed in favor of clear thread-model documentation.

**Why `ClassValue` instead of `ConcurrentHashMap` for toString reflection check?**
`ClassValue` is JVM-native (~10ns vs ~40ns), uses `WeakReference` so unloaded UDF classes are GC'd (no metaspace leak), and is shared `static` across all output instances.

**Why `nanoTime` for the time budget?**
`nanoTime()` is monotonic (immune to NTP clock adjustments), suitable for measuring elapsed time. `currentTimeMillis()` is used only for the rate-limiter's 1-second window where wall-clock semantics are appropriate.

## Quantitative Summary

| Scenario | Per-Record Overhead | Throughput Impact |
|---|---|---|
| Disabled | 0 | 0% |
| Enabled-Idle | ~1ns | < 0.1% |
| Active, default config (100/s) | ~1ns (non-sampled) / ~100ns + toString (sampled) | < 0.5% |
| Active, aggressive (10000/s, complex types) | ~1ns / ~100ns + toString (capped by time budget) | 1-3% |

## JM-Side Memory Protection

The `DataSampleRequestCoordinator` applies a two-phase truncation on the JM side:

1. **Flatten limit** (`FLATTEN_LIMIT = 10000`): Stops collecting records from TM responses once 10,000 total records are accumulated, preventing unbounded memory growth with high parallelism.
2. **Fair truncation** (`MAX_TOTAL_RECORDS = 5000`): Proportional distribution across subtasks — no subtask monopolizes the response.

Worst-case JM memory: 10,000 records × 10KB max = ~100MB, bounded and short-lived.
