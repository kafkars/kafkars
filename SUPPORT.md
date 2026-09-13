# Support and compatibility

This document states the public boundary of the experimental Rust client for
Apache Kafka®. It is intentionally narrower than the amount of code in the
repository.

## Release status

- Workspace version: `0.0.2-rc.2`
- Publication: enabled for `kafkars`, `kafka-client-core`, and
  `kafka-client-engine`; disabled for simulation and guardrails
- Stability: no semantic-versioning or source-compatibility promise
- Supported releases: no production release; `0.0.2-rc.2` is a
  release-candidate source preview
- Intended audience: design review, source and registry integration evaluation,
  and contribution

This release candidate is not production-supported and is not recommended for
production traffic. A supported release requires complete passing archived
real-broker evidence and separate release authorization.

## Runtime surface

The qualification column below describes configured release-tier scenario
scope, not a result. Only an archived passing verdict for the exact client
commit and cell is eligible evidence for a compatibility claim.

| Area | Source status | Qualification status |
| --- | --- | --- |
| Rust facade | Concrete runtime-neutral builders, futures, blocking observation, and error vocabulary | Unit-tested; no stable API promise |
| Producer | Immediate `try_send`, bounded FIFO waiting `send`, partitioning, batching, retry, cancellation, complete acknowledgement metadata, flush, shared or explicitly independent execution ownership, and close paths | Configured exact method selection for immediate and waiting sends, round-trip, explicit timestamp receipt and broker fidelity, UUID-bound ordinary admission with complete topic, identity, optional leader-epoch, coordinate, timestamp, and nullable serialized-size receipts, Java-compatible automatic keyed routing through the public receipt, independent broker placement, and a direct consumer, readiness/flush, independent sibling-close and replacement owners, null/empty, ordering, explicit partition-routing, batch, stage-aware cancellation on both retained delivery and waiting-send observers, every public compression mode, broker-restart, and rolling-restart scenarios, plus client metrics and shutdown isolation |
| Direct consumer | Assignment, complete broker Fetch policy and immutable UUID-qualified Fetch evidence, bounded Fetch-call and retained-delivery capacity, borrowed-to-owned batch and record transfer, checkpoint, seek, events, immutable read isolation, shared-client one-shot or explicitly independent execution ownership, and close paths | Configured beginning/end/exact positioning, round-trip through both waiting `recv` and immediate `try_take_batch`, Fetch topic identity, offset window, positive byte charge, and checkpoint joined to independent broker facts, owned-record transfer through both the owned-batch chain and direct `into_owned_records` with exact post-terminal source retention and ordinary producer settlement, retained failure observation through both waiting `next_event` and immediate `try_take_event` under independently applied topic authorization, explicit timestamp recovery, seek, pause/resume, incremental and multi-partition assignment, cursor continuity, replacement, dual independent cursors over the same broker records, record fidelity, and read-committed visibility with every non-default Fetch and capacity value after an independently verified aborted transaction |
| Classic group consumer | Dynamic and static membership, caller-ordered multi-topic subscription, waiting `recv` and immediate `try_take_batch`, complete broker Fetch policy, bounded Fetch-call and retained-delivery capacity, complete session, rebalance, heartbeat, and rejoin timing, explicit processing and membership-start deadlines plus individual or aggregate seek/close operation configuration, fail-closed, earliest, and latest missing-offset policy, range and cooperative-sticky assignment, assignment events, fetch, processing acknowledgement, full and processed-prefix checkpoint commit, seek, and close paths | Configured assignment transitions through both waiting `next_event` and immediate `try_take_event`, round-trip through both batch observers and the compatibility `into_checkpoint` full-batch conversion, processing-window renewal through public acknowledgement, ordered partial checkpoint with independently proven prefix offset and exact replacement-member suffix recovery, every public membership-timing value, broker-reported cooperative-sticky selection, exact two-topic assignment and record delivery, seek replay under every non-default Fetch, capacity, and shared runtime value through exact aggregate `operation_config`, pause/resume, correlated fail-closed missing-offset failure, earliest and latest offset reset, read-committed, shutdown, static-member retention and administrative removal, record fidelity, membership ownership, offset resume, broker restart, and non-default-timing recovery while every broker is disrupted in turn |
| KIP-848 consumer group | Caller-ordered multi-topic subscription, waiting `recv` and immediate `try_take_batch`, complete broker Fetch policy, bounded Fetch-call and retained-delivery capacity, explicit processing and membership-start deadlines plus individual or aggregate seek/close operation configuration, fail-closed, earliest, and latest missing-offset policy, topic UUID resolution, heartbeat, assignment translation, reconciliation, fetch, processing acknowledgement, full and processed-prefix checkpoint commit, and owned-topic acknowledgement | Configured assignment transitions through both waiting `next_event` and immediate `try_take_event`, round-trip through both batch observers and the canonical `checkpoint` full-batch conversion, processing-window renewal through public acknowledgement, ordered partial checkpoint with independently proven prefix offset and exact replacement-member suffix recovery, exact two-topic assignment and record delivery, seek replay under every non-default Fetch, capacity, and shared runtime value through the exact individual setters, pause/resume, correlated fail-closed missing-offset failure, earliest and latest offset reset, read-committed, shutdown, record fidelity, membership ownership, offset resume, and session recovery in applicable Kafka 4.x cells |
| Share-group consumer | Caller-ordered multi-topic subscription, complete broker long-poll, byte, record, acquisition-range, and attempt-timeout policy, explicit membership-start and close durations, optional rack identity, Share heartbeat membership, broker-local acquisition sessions, delivery counts, linear batches, all-record `accept_all`, and explicit record-ordered Accept, Release, or Reject acknowledgement | Configured exact two-topic assignment and record acquisition, every non-default Fetch and runtime value through exact bounded public batches, exact three-acquisition `accept_all`, exact broker-reported rack identity and requested authorization bitfields through singleton and plural Admin descriptions, lifecycle, record fidelity, mixed release/reject, batch drop, membership ownership, close uncertainty, leader recovery, and session recovery in applicable Kafka 4.x cells |
| Admin | Broad concrete request-specific core, engine, and facade paths including exact-broker routes, broker unregistration, and dynamic metadata-quorum voter changes | Configured automatic topic creation and partition expansion plus exact three-broker manually placed topic creation and partition expansion, with validate-only, batch, delete, list, and failure lifecycles, including caller-ordered name-based plural deletion with mixed outcomes and independent before/after metadata, plus caller-ordered topic-ID description with requested authorization bitfields and deletion with independent UUID and topology fencing; metadata-backed topic description plus explicit single-page and cursor-followed `DescribeTopicPartitions` with exact response limits, page boundaries, returned cursors, and an independently confirmed aggregate, alongside caller-ordered detailed name-based plural topic descriptions with mixed outcomes and requested authorization bitfields; singleton and caller-ordered plural selected topic-configuration descriptions plus exact incremental Set/Delete/Append/Subtract mutations through topic-specific and generic-resource surfaces, caller-ordered legacy full-snapshot replacement and exact default restoration through both topic and generic-resource surfaces without an expected wire value, Kafka 4.1+ generic configuration-resource discovery, and dedicated client-metrics resource discovery with exact pinned-CLI state, plus caller-ordered offset queries including exact earliest, latest, maximum-timestamp, and caller-timestamp selection with returned timestamps joined to independent record and watermark evidence, plus paired ReadUncommitted and ReadCommitted singleton selection and a read-uncommitted caller-ordered batch over committed records; singleton and caller-ordered plural record deletion with explicit and high-watermark boundaries plus independent before/after ranges; caller-ordered partition reassignment with replication-factor change and exact converged replica/ISR metadata, followed by selected and all-active listing against pinned CLI state; selected and cluster-wide preferred leader elections that restore separately disrupted leaders after exact full-ISR recovery; cluster identity with requested authorization metadata, feature and canonical metadata-quorum discovery, reversible stopped-broker unregistration with exact immediate remaining membership and same-cluster restoration, Kafka 4.3.1 validation-only finalized-feature updates with caller-ordered outcomes and exact unchanged pinned-CLI state and epoch, authenticated delegation-token create/describe/renew/explicit-zero-delay-expire with secret-free independent absence, and all seven modern Streams-group description, stable-offset, offset-mutation, and deletion methods with caller-ordered public results plus independent final absence; exact active partition producer state, selected-partition broker log directories and exact per-broker replica placements plus caller-ordered two-directory replica alteration with exact settled targets and no future copy, canonical cluster transaction listing plus exact state, signed producer-ID, duration, and Kafka 4.3 transactional-ID-pattern filtering against a stable unfiltered baseline, caller-ordered exact transaction descriptions, active-transaction force termination, and broker-derived single-partition transaction abort with pre-cleanup pinned CLI evidence, plus consumer-group and generic group discovery and caller-ordered dedicated classic plus detailed mixed classic/KIP-848 descriptions with exact requested authorization bitfields and broker-selected assignors; singleton and caller-ordered plural active Share-group state and assignment descriptions with requested authorization bitfields, singleton and caller-ordered plural selected Share-group offset listings, Share-group offset alteration/deletion, and caller-ordered Share-group deletion with independent CLI queries; consumer-group offset list/alter/delete, singleton and caller-ordered plural empty-group deletion, and caller-ordered static-member removal after retained offline-member proof, all with independent before/after group state; caller-ordered ACL lifecycle; named-user producer/consumer byte-rate quota replacement, paired strict and non-strict description, validation without mutation, and removal; and named-user SCRAM-SHA-256/512 credential upsert, description, and deletion with independent state queries |
| Transactions | Initialization, begin, UUID-bound per-record and homogeneous batch produce, fresh topic-identity validation, offset transfer, commit, abort, fencing, and close paths | Configured commit/abort through both individual `send` and homogeneous `send_batch`, caller-ordered UUID-bound admission and pre-commit validation against independent topic IDs, multi-record boundaries, successive transactions, fencing, and offset transfer for classic and KIP-848 groups |
| Simulation | Virtual-time execution of deterministic core effects | Development evidence, not broker emulation |
| Foreign bindings | Not included | No ABI or compatibility promise |

An API is implemented only when the public Rust facade reaches a concrete
engine owner and deterministic terminal path. An RFC statement, invariant,
guardrail, fixture, simulation, benchmark description, or exported vocabulary
alone is design evidence and must not be represented as broker support.

### Child-handle ownership

- `Client::producer` selects one clone-shared producer lifecycle. Every handle
  built through that path shares admission, flush, and close state.
  `Client::independent_producer` instead starts a private execution and close
  owner from the same configuration for each successful build.
- `Client::assigned_consumer` admits one directly assigned consumer for the
  shared client lifetime. Each successful
  `Client::independent_assigned_consumer` build starts a private execution owner
  with its own assignment and cursor set.
- Independent owners are not included in the originating client's metrics or
  shutdown. Close them explicitly; dropping their final handle requests private
  engine shutdown.
- The pinned Testlab protocol selects this path explicitly through the
  `independent_handles` capability and verifies later sibling, replacement, or
  dual-cursor operations rather than inferring ownership from construction.
- Group, Share, Admin, and transactional handles retain the ownership contracts
  stated by their public builders and operations.

### Producer admission methods

`Producer::try_send` attempts immediate bounded admission and returns the exact
caller-owned record on rejection. `Producer::send` instead transfers the record
to a bounded FIFO waiting operation under the configured waiting-record and
waiting-byte limits, without requiring an application retry loop. The pinned
Testlab protocol retains which method a scenario selected, and its dedicated
waiting-send scenario requires the exact public terminal and an independently
observed Kafka record. Its cancellation scenario carries the same selection
through the command, invokes either `Delivery::cancel` or `Send::cancel` twice
on the retained observer, and preserves stage uncertainty through terminal and
broker truth. This remains configured qualification scope until archived
evidence passes for an exact client commit.

### Transaction staging methods

`Transaction::send` stages one record and returns its sole public observer.
`Transaction::send_batch` admits one nonempty caller-ordered record set sharing
a topic and explicit partition, then returns one batch acknowledgment with its
exact offset range. The pinned Testlab protocol retains which method was
selected. Dedicated commit and abort scenarios require one exact batch command,
expand the public offset range into one staged terminal per record, and compare
the whole outcome with independent read-committed broker truth. This remains
configured qualification scope until archived evidence passes for an exact
client commit.

### Transaction topic identities

`Record::expected_topic_uuid` makes a nonzero broker-issued topic identity part
of record admission. A UUID-bound transaction must become quiescent and wait
for `Transaction::validate_for_commit` after its final send; any later accepted
send or offset transfer invalidates that seal. The pinned Testlab scenario
resolves two topics through public Admin, independently observes the same IDs,
binds every record, and requires the caller-ordered IDs to survive the fresh
validation and commit. This remains configured qualification scope until
archived evidence passes for an exact client commit.

### Producer receipt metadata

`RecordMetadata` retains the public topic, pre-attempt topic UUID when one was
required, partition, offset, timestamp, optional leader epoch, and exact
serialized key/value lengths. The pinned Testlab scenario resolves the topic
through public Admin and independent Kafka tooling before two ordinary sends,
then checks every receipt field against scenario and broker evidence. It
requires `None` for a null key or value and `Some(0)` for present empty bytes.
This remains configured qualification scope until archived evidence passes for
an exact client commit.

### Direct-consumer batch observers

`AssignedConsumer::recv` waits for one retained background Fetch delivery.
`AssignedConsumer::try_take_batch` instead takes one already-authorized batch
only when it is immediately available. The pinned Testlab protocol retains
which observer a scenario selected, repeatedly invokes the immediate method
within the scenario bound, and joins its returned record to independent broker
truth. This remains configured qualification scope until archived evidence
passes for an exact client commit.

### Owned direct-consumer record transfer

`RecordBatch::into_owned` preserves one bounded Fetch lease while
`OwnedConsumerBatch::into_records` yields non-clone record owners.
`RecordBatch::into_owned_records` reaches the same linear record owners
directly. The pinned Testlab commands retain which conversion was selected and
dedicated scenarios require both public paths.
`OwnedConsumerRecord::try_into_record` transfers timestamp, nullable bytes, and
ordered headers into a producer record and returns a `RetainedSourceRecord`
that keeps the exact source coordinates and bytes readable. Each scenario sends
that record to a distinct topic, independently observes both records, and
re-reads the retained source only after the destination terminal.
This remains configured qualification scope until archived evidence passes for
an exact client commit.

### Direct-consumer Fetch evidence

`RecordBatch::evidence` retains the broker-issued topic UUID, requested and next
offsets, log-start, last-stable, and high-watermark bounds, and exact retained
byte charge for one Fetch lease. The batch checkpoint reports the same progress.
The pinned Testlab scenario compares these public facts with prior independent
topic-ID and partition-watermark snapshots and the exact broker record. This
remains configured qualification scope until archived evidence passes for an
exact client commit.

### Direct-consumer event observers

`AssignedConsumer::next_event` waits for one already-retained failure event.
`AssignedConsumer::try_take_event` instead takes such an event only when it is
immediately available. The pinned Testlab protocol retains the selected public
method and the complete public position or Fetch fence and failure kind without
sending the expected result to the adapter. Its broker-policy scenario applies
and independently observes a topic READ deny, requires both methods to expose
the exact `PositionResolutionFailed(Broker(29))` result, removes that policy,
and joins restored direct-consumer progress to an independent broker record.
This remains configured qualification scope until archived evidence passes for
an exact client commit.

### Admin group-listing filters

The pinned Testlab protocol selects state and group-type filters through both
`ListConsumerGroupsBuilder` and `ListGroupsBuilder`, and protocol-type filters
through the generic `ListGroupsBuilder`. The exact caller-provided filter sets
must survive into the Testlab command, and the public group identity, type,
protocol, and state must agree with independent broker observations. A separate
unfiltered consumer-group listing remains in the release pack as a control.
This remains configured qualification scope until archived evidence passes for
an exact client commit.

### Admin group-description authorization metadata

The pinned Testlab protocol selects `include_authorized_operations(true)` on
`DescribeClassicGroupsBuilder`, `DescribeConsumerGroupsBuilder`,
`DescribeShareGroupBuilder`, and `DescribeShareGroupsBuilder`. The exact option
must survive into each command, and every successful public description must
retain Kafka's authorization bitfield. Immediate independent Kafka CLI queries
continue to establish each live group identity and member count without
substituting for the option-specific public result. Paired Streams-group
lifecycles select authorization metadata, full topology description, and stable
offset reads both true and false across the singular and plural builders.
`DescribeConsumerGroupsBuilder` additionally runs as paired singleton calls
with the bitfield excluded then included over unchanged live membership.
This remains configured qualification scope until archived evidence passes for
an exact client commit.

### Admin manual topic replica placement

The pinned Testlab protocol carries every caller-ordered partition index and
broker list into `NewTopic::with_replica_assignments`. On three-broker release
cells, immediate independent metadata must expose exactly those topic
partitions, preserve each replica order, select a leader from its replicas, and
report the complete replica set in sync. Automatic topic placement continues to
make no claim about broker-selected replica topology. This remains configured
qualification scope until archived evidence passes for an exact client commit.

### Admin manual partition replica placement

The pinned Testlab protocol carries one caller-ordered broker list per newly
added partition into `NewPartitions::with_replica_assignments`. The exact prior
count remains scenario-only and maps those lists to partition indices. On
three-broker release cells, immediate independent metadata must expose the
complete expanded topic, preserve each requested replica order, select a leader
from its replicas, and report the complete replica set in sync. Automatic
partition placement continues to make no claim about broker-selected topology.
This remains configured qualification scope until archived evidence passes for
an exact client commit.

### Admin client-quota filter strictness and validation without mutation

The pinned Testlab protocol selects paired
`DescribeClientQuotasBuilder::strict(true)` and `strict(false)` calls for one
exact named-user byte rate. Both public values must match immediate independent
Kafka CLI queries. The fixture contains only a simple user entity, so it does
not claim strict-versus-nonstrict result divergence for composite entities.

The pinned Testlab protocol selects
`AlterClientQuotasBuilder::validate_only(true)` with an exact proposed
named-user byte-rate replacement. The required current rate remains
scenario-only, the adapter must emit the distinct validation completion, and an
immediate Kafka CLI query must prove that exact prior rate remained unchanged.
This remains configured qualification scope until archived evidence passes for
an exact client commit.

### Admin explicit delegation-token expiration

The pinned authenticated Testlab lifecycle selects
`ExpireDelegationTokenBuilder::expire_after(Duration::ZERO)` rather than
omitting the period and relying on Kafka's `-1` immediate-expiry sentinel. The
zero-millisecond intent crosses the secret-free command, and the immediately
following sanitized Kafka CLI query must report no live token for the exact
owner. This remains configured qualification scope until archived evidence
passes for an exact client commit.

### Admin topic-description authorization metadata

The pinned Testlab protocol selects `include_authorized_operations(true)` on
both `DescribeTopicsBuilder` and `DescribeTopicsByIdBuilder`. The exact option
must survive into each command, and every successful public description must
retain Kafka's authorization bitfield. Immediate metadata or pinned Kafka topic
CLI observations independently establish each topic identity and partition
topology without substituting for the option-specific public result. This
remains configured qualification scope until archived evidence passes for an
exact client commit.

### Admin all-topic listing metadata and internal filtering

The pinned Testlab protocol selects
`ListTopicsBuilder::include_authorized_operations(true)` alongside paired
`include_internal(false)` and `include_internal(true)` calls. Both options must
survive into each command, and each byte-sorted public result retains every
detailed success or normalized resource error. Every success must carry Kafka's
authorization bitfield, while immediate metadata independently proves the exact
error-free partition topology of each included expectation. A classic group
commit materializes canonical `__consumer_offsets`; metadata must prove it
exists around both public calls, which first omit it, then include it with the
internal marker. This does not claim exhaustive listing, independently verified
topic IDs, replica topology, or internal-marker correctness beyond that
canonical topic. This remains configured qualification scope until archived
evidence passes for an exact client commit.

### Admin cluster-description options and fenced brokers

The pinned Testlab protocol selects
`DescribeClusterBuilder::include_authorized_operations(true)` and paired
`include_fenced_brokers(false)` / `include_fenced_brokers(true)` calls. The
exact options must survive into each command and the public descriptions must
retain Kafka's authorization bitfield. In the disposable plaintext
three-broker cell, Testlab gracefully stops broker 3; the disabled call must
omit it, while the enabled call must include it with the public fenced marker.
Both results are checked against an immediate independent two-broker metadata
snapshot before a restart restores full membership. This proves one
graceful-stop fencing path and remains configured qualification scope until
archived evidence passes for an exact client commit.

### Admin offset isolation and record-timestamp selection

The pinned Testlab protocol retains exact `ListOffsetsBuilder::read_isolation`
selection through paired singleton calls. A `ReadUncommitted` earliest query
must equal the immediate independent low watermark, while a `ReadCommitted`
latest query must equal the high watermark over the same acknowledged records.
A separate caller-ordered batch selects `ReadUncommitted` once for the complete
public call and requires every ordered earliest/latest result to equal
contiguous immediate independent watermarks. This proves both singleton option
paths and the batch option path on committed data; unresolved-transaction
last-stable-offset differentiation remains outside configured qualification.

`OffsetSpec::max_timestamp` selects the record carrying the greatest timestamp.
Its pinned Testlab scenario places that record before a later record with a
lower timestamp, then requires one unique greatest independent record, the same
public offset and `ListOffsetsResultInfo::timestamp_ms()`, and immediate
independent watermarks. This distinguishes the result from both boundary
selectors.

`OffsetSpec::for_timestamp` selects the earliest record whose timestamp is at
least the caller's nonnegative Unix epoch millisecond value. The pinned Testlab
scenario retains that exact selector and timestamp in its command, compares the
public offset and `ListOffsetsResultInfo::timestamp_ms()` with one independent
broker record, and bounds the selected offset with immediate independent
watermarks. An earlier record with a lower timestamp distinguishes this path
from both `OffsetSpec::earliest` and `OffsetSpec::latest`. Both paths remain
configured qualification scope until archived evidence passes for an exact
client commit.

### Admin configuration description metadata

The pinned Testlab protocol selects `include_synonyms(true)` and
`include_documentation(true)` through both `DescribeConfigsBuilder` and
`DescribeConfigResourcesBuilder`. Every successful selected entry retains the
public `ConfigEntry` value, read-only flag, signed source, sensitive flag,
ordered synonyms with their values and signed sources, optional configuration
type, and optional documentation. Requested synonyms must include the effective
value, requested documentation must include a type and nonempty text, and
immediate independent configuration reads remain the value authority. This
remains configured qualification scope until archived evidence passes for an
exact client commit.

### Admin transaction-listing filters

The pinned Testlab protocol selects every public `ListTransactionsBuilder`
filter: caller-ordered state and signed producer-ID sets, minimum running
duration, and Kafka-owned transactional-ID regular expression. Every filtered
action names an earlier nonempty unfiltered public listing from the same client.
Its result must be canonical, match the scenario exactly, preserve producer IDs,
and strictly narrow that baseline while an immediate unfiltered pinned CLI
snapshot proves the full transaction set did not change. State, producer-ID,
and duration selection remain in the cross-version transaction scenario. The
pattern scenario is confined to Kafka 4.3 gating packs that negotiate
ListTransactions v2. This remains configured qualification scope until archived
evidence passes for an exact client commit.

### Admin configuration mutation methods

The pinned Testlab protocol selects `ConfigAlteration::set`, `delete`, `append`,
and `subtract` through both public incremental Admin builders. The list-valued
Append and Subtract commands carry only their exact operand, Delete carries no
value, and independent configuration reads must prove every distinct final
state after an exact named baseline. Both public legacy replacement builders
also select `LegacyTopicConfigEntry::restore_default` without receiving the
expected broker default. This remains configured qualification scope until
archived evidence passes for an exact client commit.

## Kafka broker versions

No Kafka broker version is release-supported in this preview. The protocol
adapters negotiate bounded per-request version windows, but that is not a
substitute for end-to-end qualification.

[Testlab](https://github.com/kafkars/testlab) is the external authority for
Kafka and Docker setup, broker versions, security modes, topologies,
real-broker scenarios, repetitions, independent broker-visible observations,
evidence sealing, aggregation, and deterministic verdicts. Kafkars owns the
GitHub triggers, selects pull-request or release qualification, archives the
returned evidence, and applies the required gate.

Only archived passing evidence from the applicable gating Testlab run is
eligible evidence for a support claim, which additionally requires explicit
release authorization. A Testlab `Failed` verdict blocks the claim and may
identify a client defect. An `Invalid` verdict means the run did not constitute
valid qualification and also blocks release. Compatibility reports must cite
the exact client commit, broker distribution and version, topology, security
configuration, scenario scope, and archived gating verdict without widening
that evidence into a production-support claim.

### Configured release-tier cells

The release tier pinned by this repository at Testlab revision
`37ee6d75e2e9507392f1a7377bbfd050a1183fb4` defines the following gating cells.
This table records configuration only. The archived qualification artifact is
the authority for whether any cell passed, failed, or was invalid.

PR qualification executes one pack pass. The pinned Testlab release workflow
runs at most eight cells concurrently, retains each cell's evidence for 90 days,
and requires all expected cells and repetitions against identical packaged
candidate checksums before sealing the complete release aggregate. A missing,
partial, failed, or invalid gating cell cannot become a passing release.

| Cell ID | Broker | Topology and security | Configured pack | Attempts |
| --- | --- | --- | --- | ---: |
| `apache-kafka-4-3-1-plaintext` | Apache Kafka 4.3.1 | Single broker, plaintext | `kafkars-share-release` | 3 |
| `apache-kafka-4-2-1-plaintext` | Apache Kafka 4.2.1 | Single broker, plaintext | `kafkars-share-release` | 1 |
| `apache-kafka-4-1-2-plaintext` | Apache Kafka 4.1.2 | Single broker, plaintext | `kafkars-share-release` | 1 |
| `apache-kafka-4-0-2-plaintext` | Apache Kafka 4.0.2 | Single broker, plaintext | `kafkars-release` | 1 |
| `apache-kafka-3-9-2-plaintext` | Apache Kafka 3.9.2 | Single broker, plaintext | `kafkars-classic` | 1 |
| `apache-kafka-3-8-1-plaintext` | Apache Kafka 3.8.1 | Single broker, plaintext | `kafkars-classic` | 1 |
| `apache-kafka-3-7-2-plaintext` | Apache Kafka 3.7.2 | Single broker, plaintext | `kafkars-classic` | 1 |
| `apache-kafka-4-3-1-three-tls` | Apache Kafka 4.3.1 | Three brokers, custom-root TLS without SASL | `kafkars-three-broker-security` | 1 |
| `apache-kafka-4-3-1-three-sasl-plain` | Apache Kafka 4.3.1 | Three brokers, plaintext with SASL/PLAIN | `kafkars-three-broker-security` | 1 |
| `apache-kafka-4-3-1-three-scram-sha-256` | Apache Kafka 4.3.1 | Three brokers, plaintext with SCRAM-SHA-256 | `kafkars-three-broker-security` | 1 |
| `apache-kafka-4-3-1-three-scram-sha-512` | Apache Kafka 4.3.1 | Three brokers, plaintext with SCRAM-SHA-512 | `kafkars-three-broker-security` | 1 |
| `apache-kafka-4-3-1-three-sasl-plain-tls` | Apache Kafka 4.3.1 | Three brokers, custom-root TLS with SASL/PLAIN | `kafkars-three-broker-security` | 1 |
| `apache-kafka-4-3-1-three-scram-sha-256-tls` | Apache Kafka 4.3.1 | Three brokers, custom-root TLS with SCRAM-SHA-256 | `kafkars-three-broker-security` | 1 |
| `apache-kafka-4-3-1-three-scram-sha-512-tls` | Apache Kafka 4.3.1 | Three brokers, custom-root TLS with SCRAM-SHA-512 | `kafkars-three-broker-security` | 1 |
| `apache-kafka-4-3-1-three-plaintext` | Apache Kafka 4.3.1 | Three brokers, plaintext without SASL | `kafkars-three-broker-share` | 1 |
| `apache-kafka-4-3-1-broker-role-failover` | Apache Kafka 4.3.1 | Three brokers, plaintext with controlled broker-role failover | `kafkars-broker-role-failover` | 1 |
| `apache-kafka-4-3-1-broker-policy` | Apache Kafka 4.3.1 | Single broker, SASL/PLAIN with authorizer controls | `kafkars-broker-policy` | 1 |
| `apache-kafka-4-3-1-network-faults` | Apache Kafka 4.3.1 | Single broker, plaintext through a controlled network proxy | `kafkars-network-faults` | 1 |
| `protocol-adversary` | Scripted Kafka endpoint | External protocol adversary | `kafkars-protocol-adversary` | 1 |
| `apache-kafka-4-3-1-delegation-token-sasl-plain-tls` | Apache Kafka 4.3.1 | Three brokers, custom-root TLS with SASL/PLAIN and an internal authenticated observer | `kafkars-delegation-token` | 1 |
| `apache-kafka-4-3-1-streams-group-admin` | Apache Kafka 4.3.1 | Three brokers, plaintext with modern Streams coordination and pinned CLI observation | `kafkars-streams-group` | 1 |

## Transport and authentication

| Configuration | Code path | Real-broker qualification |
| --- | --- | --- |
| Plain TCP without SASL | Present and the default | Configured for single-broker Kafka 3.7.2, 3.8.1, 3.9.2, 4.0.2, 4.1.2, 4.2.1, and 4.3.1 plus two three-broker Kafka 4.3.1 cells, including dedicated modern Streams-group Admin coverage; consult the exact archived verdict |
| TLS with platform roots | Present | Not configured in the release tier |
| TLS with a custom PEM root bundle | Present | Configured without SASL for one three-broker Kafka 4.3.1 cell; consult the exact archived verdict |
| SASL/PLAIN over plain TCP | Present | Configured for one three-broker Kafka 4.3.1 cell; consult the exact archived verdict |
| SASL/PLAIN over custom-root TLS | Present | Configured for one three-broker Kafka 4.3.1 cell; consult the exact archived verdict |
| SCRAM-SHA-256 over plain TCP | Present | Configured for one three-broker Kafka 4.3.1 cell; consult the exact archived verdict |
| SCRAM-SHA-256 over custom-root TLS | Present | Configured for one three-broker Kafka 4.3.1 cell; consult the exact archived verdict |
| SCRAM-SHA-512 over plain TCP | Present | Configured for one three-broker Kafka 4.3.1 cell; consult the exact archived verdict |
| SCRAM-SHA-512 over custom-root TLS | Present | Configured for one three-broker Kafka 4.3.1 cell; consult the exact archived verdict |
| Mutual TLS client certificates | Not exposed | Unsupported |
| SASL/OAUTHBEARER | Not exposed | Unsupported |
| SASL/GSSAPI or Kerberos | Not exposed | Unsupported |

Credentials are retained with redacted diagnostics and zeroized on final
release, but operational secret storage and rotation remain the embedding
application's responsibility.

## Reviewed-pair integration audit

The current reviewed driver/wire pair closes the three previously documented
integration omissions. These are implementation claims, not broker-version
qualification claims.

| Contract | Reviewed-pair status | Local evidence |
| --- | --- | --- |
| Exact broker identity and routing | Driver `TopicView` broker identities are projected into tracked `Route::Broker` calls for aggregate Fetch and exact-broker Admin operations | `exact_broker_submission_reaches_the_selected_loopback_broker` plus route-specific submission tests |
| Kafka protocol topic UUIDs | Nonzero driver topic UUIDs are retained as exact bytes, translated through producer receipts and UUID-bound transaction validation, and used by KIP-848 assignment, reconciliation, and owned-topic acknowledgement | `receipt_retains_the_topic_uuid_proven_before_produce`, `exact_complete_topic_uuid_snapshot_validates`, `live_topic_view_retains_broker_issued_topic_identity`, `resolved_topic_uuids_translate_assignments_and_owned_partitions`, and KIP-848 reconciliation tests |
| Configured client ID | The validated facade value is passed into the driver builder and encoded in Kafka request headers | `generated_request_and_response_complete_through_a_loopback_broker` verifies the configured header value |

Local topic identities remain client ownership keys and are deliberately
distinct from Kafka protocol topic UUIDs.

## Retained integration limits

### Controller maintenance operations

The public `unregister_broker`, `add_raft_voter`, and `remove_raft_voter` paths
reach concrete bounded engine terminals. The pinned release tier qualifies only
broker unregistration, in the disposable plaintext Kafka 4.3.1 three-broker
cell: Testlab stops the selected broker, submits one public unregistration,
independently observes the exact remaining broker set, restarts the broker, and
requires the original cluster identity and complete broker set to return.

Raft voter addition and removal require a Kafka dynamic-quorum deployment and
exact voter directory identities. The current Testlab release tier uses a
static controller quorum, so those two methods are implemented but not
real-broker-qualified for the first stable cut. Unit, protocol, and loopback
coverage must not be widened into a compatibility claim.

### Multi-topic subscriptions

The public classic and KIP-848 builders retain every caller-supplied topic.
The pinned Testlab scenarios require both public assignments and one exact
independently observed record from each of two topics.
The public Share builder likewise retains the caller-ordered topic list. Its
scenario requires an assignment spanning both topics and one exact publicly
accepted, independently observed record from each.
This remains configured qualification scope until archived evidence passes for
an exact client commit.

### Share rack identity

The public Share builder retains an optional caller-supplied rack ID. The
pinned singleton and plural Share-group description scenarios require the
public Admin facade to report the exact broker-returned rack ID for each active
member. This remains configured qualification scope until archived evidence
passes for an exact client commit.

### Share Fetch and runtime policy

The public Share builder retains long-poll, minimum-byte, maximum-byte,
maximum-record, acquisition-range, and attempt-timeout policy plus explicit
membership-start and close durations. The pinned scenarios supply every value
at a non-default setting, prove one-record delivery and acquisition-range
boundaries through retained public batches, and join every record to independent
broker evidence. This remains configured qualification scope until archived
evidence passes for an exact client commit.

### Share acknowledgement methods

`ShareConsumerBatch::accept_all` converts every retained record to Accept in
one linear operation. `ShareConsumerBatch::into_acknowledgement` instead keeps
caller-selected record-order decisions. The pinned Testlab command retains the
exact method: one configured batch accepts three independently matched records
through `accept_all`, while lifecycle scenarios preserve explicit mixed Accept,
Release, and Reject behavior. This remains configured qualification scope until
archived evidence passes for an exact client commit.

### Classic membership timing

The public classic builder retains explicit session and rebalance timeouts,
heartbeat interval and attempt timeout, and rejoin backoff and attempt timeout.
The pinned Testlab scope runs all six at non-default values in a single-broker
configured scenario and while disrupting every broker in a three-broker
cluster. This remains configured qualification scope until archived evidence
passes for an exact client commit.

### Group runtime deadlines

The public classic and KIP-848 builders retain caller-selected processing and
membership-start deadlines plus seek and close durations through individual
setters or `GroupConsumerOperationConfig`. The pinned classic scenario selects
`ConsumerBuilder::operation_config`; the KIP-848 scenario retains the individual
setters. Both set every duration to a non-default value, establish live
membership, replay one exact independently observed record through public seek,
and close the same member.
The pinned classic round trip converts its full retained batch through
`ConsumerBatch::into_checkpoint`; the paired KIP-848 round trip retains
canonical `ConsumerBatch::checkpoint`. Exact Testlab command evidence rejects
substitution between those public spellings.
The paired membership-ownership scenarios likewise retain exact
`Consumer::next_event` and `Consumer::try_take_event` selections while public
assignment transitions settle for both classic and KIP-848 membership.
Dedicated scenarios additionally retain a batch longer than the original
processing window, call public `Consumer::acknowledge` midway with its
assignment-fenced checkpoint, and commit the same independently observed record
inside the renewed window for both protocols. Paired partial-checkpoint
scenarios receive two records in one retained batch, mark only the first through
`CheckpointBuilder::mark_processed`, independently prove offset 1, and require
a replacement member to receive the exact suffix before offset 2 for both
protocols.
This remains configured qualification scope until archived evidence passes for
an exact client commit.

### Consumer Fetch and capacity policy

The public assigned, classic, and KIP-848 configuration paths retain broker
long-poll, minimum, response, per-partition, and attempt-timeout policy plus
independent in-flight Fetch, buffered-batch, retained-byte, and decoded-batch
ceilings. The pinned Testlab scenarios supply every value at a non-default
setting, deliver through each public consumer path, and join the exact record to
independent broker evidence. This remains configured qualification scope until
archived evidence passes for an exact client commit.

### Multi-member group progress

Local unit, invariant, simulation, and loopback evidence does not establish
real-broker support. Consult archived Testlab evidence for the exact group
scenarios a gating run covered; do not infer KIP-848 fetch, commit, or concurrent
multi-member progress from narrower evidence.

### Fetch leader movement

Normal broker Fetch resolves the broker-issued topic UUID and leader epoch,
then uses the exact Fetch v16 topic-ID route. KIP-951 current-leader hints for
`NOT_LEADER_OR_FOLLOWER` and `FENCED_LEADER_EPOCH` invalidate the old route and
replace the same offset under its original deadline; absent or stale hints fall
back to bounded metadata refresh, and `UNKNOWN_LEADER_EPOCH` retries the
established broker route without carrying the rejected epoch. The qualification
matrix includes in-process direct and classic-group recovery across broker
leader movement. This is source and qualification-scenario coverage, not a
production-support promise; any compatibility statement remains limited to
exact archived passing cells and separate support authorization.

### Foreign interfaces

This cut is Rust-only. It includes no C header, stable C ABI, or Java, Python,
Node.js, Go, or .NET binding. A future binding is a separate compatibility and
release surface.

## Project naming

The product, source repository, package, and public Rust library are `kafkars`.
`kafka-client-core` and `kafka-client-engine` are implementation dependencies,
not separate public product identities. `zsumz` is the maintainer and signing
identity.

For build setup, see `README.md`. For security-sensitive reports, follow
`SECURITY.md` rather than opening a public issue.
