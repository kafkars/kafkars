# Architecture

`kafkars` is one native Kafka client with a deterministic semantic core, one
runtime-neutral execution engine, and a curated Rust facade.

```text
kafka-client-core ----------------------+
    deterministic policy                |
                                         v
kafka-wire -----------> kafka-driver -> kafka-client-engine ---> kafkars
    protocol bytes        RPC and I/O      integration owner      Rust facade
         |
         +---- kafka-wire-records --------+
```

The engine is the join point. The core never depends on networking, generated
protocol values, an async runtime, or a public adapter.

## Ownership

### Time

The public operation boundary captures one absolute deadline. Core decides
semantic expiry before driver admission. Once the driver accepts a request,
the driver owns transport deadline settlement. Retries reuse the original
deadline; no layer starts a replacement timeout.

Background consumer work captures its own explicit internal attempt deadline.
Application observation does not start or extend Fetch work.

### Bytes

Admission reserves retained bytes before accepting caller ownership. Canonical
records, encoded batches, queued work, and terminal values stay
charged until their exact owner releases them. No unbounded queue is a progress
mechanism.

### Completion

Every accepted operation reserves terminal capacity before admission and
reaches exactly one terminal state. Notification is separate from result
ownership: a stalled observer may backpressure new work, but cannot make the
reactor lose a terminal value.

### Cancellation

Dropping an observer abandons observation only. Cancellation is an explicit
stage-fenced transition. Work rejected before transport is `NotSent`; work
that may have crossed the transport boundary is `PossiblySent`. Certainty may
stay the same or weaken, never improve.

## Crates

### `kafka-client-core`

Owns deterministic policy:

- operation identities, deadlines, and terminal decisions;
- producer admission, batching, flush, close, retry, identity, and sequences;
- direct and group consumer assignment, checkpoints, and processing liveness;
- concrete admin state machines;
- transaction epochs, fencing, and abort-required outcomes.

It owns no socket, clock, thread, driver call, protocol DTO, callback, or
runtime integration.

### `kafka-client-engine`

Owns execution:

- the unique embedded `kafka-driver` reactor;
- monotonic clock capture and core-time mapping;
- bounded ingress, retained storage, terminal registries, and notifier workers;
- `kafka-wire` request and response adaptation;
- `kafka-wire-records` batch materialization and compression scheduling;
- concrete producer, consumer, admin, and transaction hosts;
- shutdown, recovery, and worker finalization.

The engine shares mechanisms across domains, not domain policy. Each Kafka
operation retains a concrete owner, call lane, response adapter, recovery path,
and completion type. Generic executors may not erase those capabilities.

### `kafkars`

Owns stable Rust vocabulary, inert builders, named runtime-neutral futures,
and blocking observation. Generated wire values, driver routes, reactor types,
and engine identities do not cross the facade.

### `kafka-client-sim`

Owns virtual time, deterministic effect execution, and fault scenarios over
core machines. It does not emulate transport policy already owned by the
driver.

### `kafka-client-guardrails`

Executes repository policy. `contracts/invariants.toml` is the normative
semantic registry and must name live Rust test evidence. `guardrails.toml`
governs source shape, dependency direction, capability imports, test mirrors,
and deliberate size exceptions.

## Domain boundaries

### Producer

Core owns admission, batching, cancellation, retry eligibility, idempotent
identity, and partition sequences. The engine owns record retention,
materialization, compression, topic-view lookup, and tracked Produce calls.
The driver remains the sole topology and route authority.

Keyed automatic partitioning uses Java-compatible positive Murmur2 over the
logical partition domain. Null keys use bounded sticky state. Explicit
partitions bypass lookup. Idempotence and `acks=all` are guarantees, not
downgrade switches.

### Consumer

Direct assignment and hosted group membership reuse mechanisms but not policy.
Assignment epochs fence delivery, seek, pause, resume, and checkpoint commit.
Fetch retains bounded decoded batches and advances only through core-authorized
facts. Group heartbeats do not substitute for application-processing liveness.

Classic and consumer-protocol membership retain separate concrete machines.
The driver owns coordinator routing; the client owns no coordinator cache.

### Admin

Each admin API is a concrete deterministic machine and bounded engine owner.
Request order, signed broker codes, delivery certainty, route evidence, and
retained bytes survive normalization. Destructive calls do not gain an
automatic retry merely because another admin call is safe to repeat.

### Transactions

A transactional producer is unique, non-cloneable ownership. Core owns the
linear lifecycle and consequence of every accepted operation. The engine owns
the concrete `InitProducerId`, `AddPartitionsToTxn`, Produce,
`AddOffsetsToTxn`, `TxnOffsetCommit`, and `EndTxn` executions. Uncertain
delivery fences later admission rather than manufacturing success.

## Threads and callbacks

One engine host thread owns one driver reactor. Logical shards are fairness and
ownership boundaries, not extra reactors. Dedicated bounded notifier workers
run Rust wakers away from network I/O. Compression uses explicit workers where
configured. No application callback runs on a reactor thread.

## Shutdown and recovery

Shutdown closes admission before draining. Concrete owners continue until
accepted work settles or the unique driver is destroyed. Only after driver
destruction may recovery conservatively reclaim unresolved calls. Every worker
is joined before the retained shutdown result is published.

## Source contract

- `unsafe` is forbidden throughout the repository.
- No async runtime dependency enters the deterministic core.
- Every Rust file begins with a `//!` module contract.
- `lib.rs` and `mod.rs` are declarative facades.
- Unit tests live in sibling `*_test.rs` files.
- New capability edges and size exceptions require explicit policy changes.
- Repository-shape changes run `cargo test -p kafka-client-guardrails`.
