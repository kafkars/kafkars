# Share consumer design

This document defines the first Rust share-group consumer for kafkars. It is
the normative ownership design for the source implementation, not a broker
support claim. The protocol behavior follows Apache Kafka KIP-932 while the
ownership model follows the repository-wide rules in
[ARCHITECTURE.md](ARCHITECTURE.md).

## Scope and compatibility

The first implementation is a third consumer domain. It is not a mode on the
direct-assignment or ordinary group `ConsumerBuilder` because a share group
owns record acquisitions and acknowledgements rather than partition positions
and committed offsets.

The first milestone deliberately selects protocol v1:

- `ShareGroupHeartbeat` v1 for membership and assignment;
- `ShareFetch` v1 for acquisition and broker-local share sessions;
- `ShareAcknowledge` v1 for explicit acknowledgement;
- Kafka 4.1 through 4.3 as the initial qualification range;
- no fallback to Kafka 4.0's incompatible early-access protocol.

The pinned wire crate also contains ShareFetch and ShareAcknowledge v2. Renew,
acquire-mode selection, and other v2 behavior remain a later additive milestone.
The v1 implementation must not send v2-only fields or silently negotiate a
different ownership policy.

## Public Rust surface

The facade will expose a separate builder and unique consumer:

```rust,ignore
let mut consumer = client
    .share_consumer("workers")
    .subscribe(["jobs"])
    .build()?;

let batch = consumer.recv().await?.ok_or(Closed)?;

for record in batch.records() {
    process(record)?;
}

let acknowledgement = batch.accept_all()?;
consumer
    .try_acknowledge(acknowledgement, Duration::from_secs(5))?
    .await?;

consumer.try_close()?.await?;
```

Mixed outcomes consume the same batch capability:

```rust,ignore
let decisions = batch
    .records()
    .map(|record| {
        let disposition = match process(record) {
            Ok(()) => ShareDisposition::Accept,
            Err(ProcessError::Retryable) => ShareDisposition::Release,
            Err(ProcessError::Permanent) => ShareDisposition::Reject,
        };
        record.decision(disposition)
    })
    .collect::<Vec<_>>();

let acknowledgement = batch.into_acknowledgement(decisions)?;
consumer.try_acknowledge(acknowledgement, timeout)?.await?;
```

The names above are the implemented Rust source-preview spelling. These
ownership decisions are fixed:

- `ShareConsumer` is unique, non-cloneable, and `Send`.
- `ShareConsumerBatch` is the linear capability for exact acquired records.
- Consuming a batch creates one `ShareAcknowledgement`.
- `Accept`, `Release`, and `Reject` are the only public v1 dispositions.
- Dropping a batch sends no network acknowledgement. It releases local payload
  bytes, and the broker acquisition lock expires normally.
- A pre-admission acknowledgement rejection returns the exact capability and
  may report `RetrySafe`.
- Once transport accepts an acknowledgement, an ambiguous terminal remains
  `PossiblySent` and is never blindly resubmitted.
- A named runtime-neutral future and blocking observation share one terminal;
  there is no callback API.

The share consumer exposes no checkpoint, offset commit, seek, static
membership, ordinary consumer-group revocation, or transaction offset-transfer
surface. Wire `Gap` acknowledgement values are an internal range-encoding
detail, never a public disposition.

## Machines and owners

```text
ShareConsumer
    |
    +-- ShareMembershipMachine -- ShareGroupHeartbeat -- group coordinator
    |             |
    |             +-- current fetchable partition set
    |
    +-- ShareSessionMachine per broker -- ShareFetch / ShareAcknowledge
    |
    +-- AcquisitionLedger
          acquired -> staged -> acknowledging -> accepted/released/rejected
                         +---------------------> expired/session-lost
```

The deterministic core owns policy and semantic time. The engine owns the
clock, retained bytes, generated protocol adaptation, concrete driver calls,
record decoding, and hosted execution. The driver remains the sole coordinator,
leader-route, connection, and protocol-negotiation authority. The facade owns
only curated Rust types and operation boundaries. The simulator executes core
effects under virtual time without emulating a broker.

### Membership

`ShareMembershipMachine` owns one stable consumer-generated member ID for the
consumer lifetime, the broker member epoch, the exact assignment, the next
heartbeat deadline, and one in-flight heartbeat attempt. Join uses member epoch
zero, steady heartbeats use the last accepted epoch, and leave uses the
protocol's leave epoch. Assignment changes stop new acquisition from removed
partitions but do not destroy outstanding acknowledgement capabilities.

Coordinator rejection may request driver-owned route invalidation and a
core-owned bounded retry under the original attempt deadline. A replacement
uses a fresh attempt identity and cannot overlap the invalidation or prior call.
Unknown, stale, regressing, or malformed membership facts do not mutate live
ownership. Initial fatal membership failure remains observable after accepted
construction.

### Broker-local sessions

Each broker route owns one `ShareSessionMachine`: broker identity, member
identity, share-session epoch, the exact included and forgotten partitions, and
at most one tracked ShareFetch or ShareAcknowledge call per permitted lane. The
engine retains the driver's opaque causal route receipt beside each terminal;
core does not manufacture route or socket generations before asynchronous
driver resolution. No second topology or coordinator cache is introduced.

Session loss prevents old-session acquisitions from being replayed into a new
session. Re-establishment starts from explicit empty session authority and adds
only partitions still assigned by membership. Current-leader hints and route
invalidation reuse the existing driver-owned mechanism.

### Acquisition ledger

The ledger owns every broker-acquired offset exactly once. An entry retains:

- group and stable member identity;
- broker identity plus the engine-owned opaque route receipt for its terminal;
- share-session epoch;
- topic UUID, partition, offset range, and delivery count;
- one local acquisition generation;
- retained payload and ledger charges;
- conservative lock boundary;
- delivery, acknowledgement, and terminal phase.

The local lock boundary is derived from the ShareFetch submission time plus the
broker-reported acquisition-lock duration. It is a conservative local fence,
not proof of broker state. Exact broker `INVALID_RECORD_STATE` remains
authoritative.

Overlapping or duplicate acquired ranges, invalid delivery counts, malformed
record ranges, stale session facts, and unbudgeted response bytes fail closed.
Assignment removal does not invalidate an existing ledger entry. Expiry or
session loss prevents a local acknowledgement from claiming authority it no
longer owns.

## Ownership table

| Concern | Owner | Rule |
| --- | --- | --- |
| Public time | Core | Acknowledge and close deadlines are captured at the public call boundary and never reset. |
| Background time | Core | Heartbeat, fetch, session, lock, and recovery deadlines are explicit internal schedules. |
| Protocol clock | Engine | The engine samples the clock and turns elapsed facts into core inputs; it does not invent policy. |
| Acquisition lock | Core ledger | Local expiry is conservative; the broker's exact response remains authoritative. |
| Bytes | Engine | Response, decoding, ledger, delivery, completion, and close-recovery capacity are reserved before ShareFetch admission. |
| Completion | Core and engine | Every accepted acknowledgement or close reserves exactly one terminal before any driver call. |
| Batch cancellation | Core | Dropping a batch sends nothing and releases only local delivery bytes. |
| Observer cancellation | Core | Dropping a future abandons observation, not accepted work. |
| Assignment | Membership | Removal stops future acquisition while existing exact acknowledgements remain independently fenced. |
| Session | Core and engine | Core fences broker, member, member epoch, session epoch, assignment, and acquisition generations; the engine separately owns each opaque driver route receipt. |

Emergency capacity is reserved before acquisition. It is for deterministic
local settlement and bounded close ownership; it is not permission to send an
unrequested acknowledgement from `Drop`.

## Acknowledgement normalization

A `ShareAcknowledgement` consumes one batch and contains one disposition for
every acquired record. The core validates topic, partition, offset, acquisition
generation, session fence, and lock boundary before admission. Caller order is
not wire authority; normalization sorts exact offsets, rejects duplicates and
omissions, then compresses contiguous equal outcomes. Internal `Gap` values may
span offsets not represented in an adjacent disposition range, but the public
API never accepts `Gap` as a caller decision.

Acknowledgement settlement is per partition and preserves every signed broker
code, current-leader hint, route receipt, and delivery certainty. A successful
partition retires only its exact ledger entries. A definitely-unsent
pre-admission or driver rejection returns exact retry ownership where safe. A
possibly-sent terminal does not recreate the consumed capability, strengthen
certainty, or manufacture success.

## Close and shutdown

Close first stops membership admission and future ShareFetch work. It then
settles accepted acknowledgement calls, sends the explicit leave heartbeat,
and boundedly resolves still-owned acquisitions according to their actual
state. Dropped batches are not retroactively acknowledged. Records still owned
by the broker may become available through lock expiry or broker-side member
departure behavior.

Close uses one public absolute deadline and reports partial or ambiguous
outcomes rather than extending the deadline or claiming all work was released.
Client shutdown closes share-consumer admission before draining the same owners.
Fallback recovery may reclaim bytes and publish conservative failures only
after the unique driver owner is gone.

## Implementation checkpoints

Each checkpoint is one small PGP-signed, bodyless Conventional Commit by
`zsumz`, with focused tests and source-shape guardrails.

1. `docs(consumer): define share consumer ownership`
   - this design, planned `SHR-*` invariants, and the protocol boundary;
2. `feat(consumer): add share group membership`
   - stable member identity, heartbeat v1 join/steady/leave, assignment,
     rediscovery, simulation, and startup failure;
3. `feat(consumer): acquire shared records`
   - per-broker sessions, ShareFetch v1, bounded ledger, decoding, delivery
     counts, and public batch/record types;
4. `feat(consumer): acknowledge shared records`
   - dispositions, normalization, ShareAcknowledge v1, partition terminals,
     and delivery certainty;
5. `fix(consumer): recover share sessions`
   - leader and connection movement, session/member fences, lock expiry, close,
     and shutdown;
6. `test(consumer): qualify share group delivery`
   - Kafka 4.1, 4.2, and 4.3 plaintext plus existing security lanes.

## Completion evidence

The source is not implemented or broker-compatible merely because this design
exists. Completion requires the canonical local gate and archived real-broker
evidence showing:

- two share consumers process records from one partition;
- Accept prevents normal redelivery;
- Release causes redelivery with a higher delivery count;
- Reject prevents normal redelivery;
- an unacknowledged record returns after acquisition-lock expiry;
- assignment changes preserve outstanding acknowledgement ownership;
- member shutdown releases unfinished broker work without false success;
- session loss and leader movement recover without duplicate local ownership;
- bounded close retains truthful partial and ambiguous outcomes;
- Kafka 4.1 through 4.3 plaintext and existing security cells pass.

Implicit acknowledgement, acknowledgement callbacks, piggyback optimization,
Renew, v2 acquire modes, foreign bindings, and Kafka 4.0 compatibility are not
part of this milestone.
