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
| Producer | Immediate `try_send`, bounded FIFO waiting `send`, partitioning, batching, retry, cancellation, flush, shared or explicitly independent execution ownership, and close paths | Configured exact method selection for immediate and waiting sends, round-trip, explicit timestamp receipt and broker fidelity, Java-compatible automatic keyed routing through the public receipt, independent broker placement, and a direct consumer, readiness/flush, independent sibling-close and replacement owners, null/empty, ordering, explicit partition-routing, batch, stage-aware cancellation on both retained delivery and waiting-send observers, every public compression mode, broker-restart, and rolling-restart scenarios, plus client metrics and shutdown isolation |
| Direct consumer | Assignment, complete broker Fetch policy, bounded Fetch-call and retained-delivery capacity, checkpoint, seek, events, immutable read isolation, shared-client one-shot or explicitly independent execution ownership, and close paths | Configured beginning/end/exact positioning, round-trip through both waiting `recv` and immediate `try_take_batch`, retained failure observation through both waiting `next_event` and immediate `try_take_event` under independently applied topic authorization, explicit timestamp recovery, seek, pause/resume, incremental and multi-partition assignment, cursor continuity, replacement, dual independent cursors over the same broker records, record fidelity, and read-committed visibility with every non-default Fetch and capacity value after an independently verified aborted transaction |
| Classic group consumer | Dynamic and static membership, caller-ordered multi-topic subscription, waiting `recv` and immediate `try_take_batch`, complete broker Fetch policy, bounded Fetch-call and retained-delivery capacity, complete session, rebalance, heartbeat, and rejoin timing, explicit processing, membership-start, seek, and close durations, fail-closed, earliest, and latest missing-offset policy, range and cooperative-sticky assignment, assignment events, fetch, processing acknowledgement, checkpoint commit, seek, and close paths | Configured round-trip through both batch observers, processing-window renewal through public acknowledgement, every public membership-timing value, broker-reported cooperative-sticky selection, exact two-topic assignment and record delivery, seek replay under every non-default Fetch, capacity, and shared runtime value, pause/resume, correlated fail-closed missing-offset failure, earliest and latest offset reset, read-committed, shutdown, static-member retention and administrative removal, record fidelity, membership ownership, offset resume, broker restart, and non-default-timing recovery while every broker is disrupted in turn |
| KIP-848 consumer group | Caller-ordered multi-topic subscription, waiting `recv` and immediate `try_take_batch`, complete broker Fetch policy, bounded Fetch-call and retained-delivery capacity, explicit processing, membership-start, seek, and close durations, fail-closed, earliest, and latest missing-offset policy, topic UUID resolution, heartbeat, assignment translation, reconciliation, fetch, processing acknowledgement, checkpoint commit, and owned-topic acknowledgement | Configured round-trip through both batch observers, processing-window renewal through public acknowledgement, exact two-topic assignment and record delivery, seek replay under every non-default Fetch, capacity, and shared runtime value, pause/resume, correlated fail-closed missing-offset failure, earliest and latest offset reset, read-committed, shutdown, record fidelity, membership ownership, offset resume, and session recovery in applicable Kafka 4.x cells |
| Share-group consumer | Caller-ordered multi-topic subscription, complete broker long-poll, byte, record, acquisition-range, and attempt-timeout policy, explicit membership-start and close durations, optional rack identity, Share heartbeat membership, broker-local acquisition sessions, delivery counts, linear batches, and explicit Accept, Release, or Reject acknowledgement | Configured exact two-topic assignment and record acquisition, every non-default Fetch and runtime value through exact bounded public batches, exact broker-reported rack identity through singleton and plural Admin descriptions, lifecycle, record fidelity, mixed release/reject, batch drop, membership ownership, close uncertainty, leader recovery, and session recovery in applicable Kafka 4.x cells |
| Admin | Broad concrete request-specific core, engine, and facade paths including exact-broker routes, broker unregistration, and dynamic metadata-quorum voter changes | Configured topic create/validate/batch/partition/delete/list lifecycles and failures, including caller-ordered name-based plural deletion with mixed outcomes and independent before/after metadata, plus caller-ordered topic-ID description and deletion with independent UUID and topology fencing; singleton topic description plus caller-ordered detailed name-based plural topic descriptions with mixed outcomes; singleton and caller-ordered plural selected topic-configuration descriptions and mutations, caller-ordered generic topic-resource descriptions and incremental mutations, caller-ordered legacy full-snapshot replacement through both topic and generic-resource surfaces, Kafka 4.1+ generic configuration-resource discovery, and dedicated client-metrics resource discovery with exact pinned-CLI state, plus caller-ordered offset queries; singleton and caller-ordered plural record deletion with explicit and high-watermark boundaries plus independent before/after ranges; caller-ordered partition reassignment with replication-factor change and exact converged replica/ISR metadata, followed by selected and all-active listing against pinned CLI state; selected and cluster-wide preferred leader elections that restore separately disrupted leaders after exact full-ISR recovery; cluster identity, feature and canonical metadata-quorum discovery, reversible stopped-broker unregistration with exact immediate remaining membership and same-cluster restoration, Kafka 4.3.1 validation-only finalized-feature updates with caller-ordered outcomes and exact unchanged pinned-CLI state and epoch, authenticated delegation-token create/describe/renew/expire with secret-free independent absence, and all seven modern Streams-group description, stable-offset, offset-mutation, and deletion methods with caller-ordered public results plus independent final absence; exact active partition producer state, selected-partition broker log directories and exact per-broker replica placements plus caller-ordered two-directory replica alteration with exact settled targets and no future copy, canonical cluster transaction listing, caller-ordered exact transaction descriptions, active-transaction force termination, and broker-derived single-partition transaction abort with pre-cleanup pinned CLI evidence, plus consumer-group and generic group discovery and caller-ordered detailed mixed classic/KIP-848 descriptions including broker-selected assignors; singleton and caller-ordered plural active Share-group state and assignment descriptions, singleton and caller-ordered plural selected Share-group offset listings, Share-group offset alteration/deletion, and caller-ordered Share-group deletion with independent CLI queries; consumer-group offset list/alter/delete, singleton and caller-ordered plural empty-group deletion, and caller-ordered static-member removal after retained offline-member proof, all with independent before/after group state; caller-ordered ACL lifecycle; named-user producer/consumer byte-rate quota replacement, description, and removal; and named-user SCRAM-SHA-256/512 credential upsert, description, and deletion with independent state queries |
| Transactions | Initialization, begin, per-record and homogeneous batch produce, offset transfer, commit, abort, fencing, and close paths | Configured commit/abort through both individual `send` and homogeneous `send_batch`, multi-record boundaries, successive transactions, fencing, and offset transfer for classic and KIP-848 groups |
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

### Direct-consumer batch observers

`AssignedConsumer::recv` waits for one retained background Fetch delivery.
`AssignedConsumer::try_take_batch` instead takes one already-authorized batch
only when it is immediately available. The pinned Testlab protocol retains
which observer a scenario selected, repeatedly invokes the immediate method
within the scenario bound, and joins its returned record to independent broker
truth. This remains configured qualification scope until archived evidence
passes for an exact client commit.

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
`0b9a73051423ce59e60b7bc972c2320a610fb54d` defines the following gating cells.
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
| Kafka protocol topic UUIDs | Nonzero driver topic UUIDs are retained as exact bytes, then translated through KIP-848 assignment, reconciliation, and owned-topic acknowledgement | `live_topic_view_retains_broker_issued_topic_identity`, `resolved_topic_uuids_translate_assignments_and_owned_partitions`, and KIP-848 reconciliation tests |
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

### Classic membership timing

The public classic builder retains explicit session and rebalance timeouts,
heartbeat interval and attempt timeout, and rejoin backoff and attempt timeout.
The pinned Testlab scope runs all six at non-default values in a single-broker
configured scenario and while disrupting every broker in a three-broker
cluster. This remains configured qualification scope until archived evidence
passes for an exact client commit.

### Group runtime deadlines

The public classic and KIP-848 builders retain caller-selected processing,
membership-start, seek, and close durations. The pinned scenarios set every
duration to a non-default value, establish live membership, replay one exact
independently observed record through public seek, and close the same member.
Dedicated scenarios additionally retain a batch longer than the original
processing window, call public `Consumer::acknowledge` midway with its
assignment-fenced checkpoint, and commit the same independently observed record
inside the renewed window for both protocols.
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
