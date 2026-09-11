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
| Producer | Bounded admission, partitioning, batching, retry, cancellation, flush, and close paths | Configured round-trip, readiness/flush, null/empty, ordering, partition-routing, batch, cancellation, every public compression mode, broker-restart, and rolling-restart scenarios, plus client metrics and shutdown isolation |
| Direct consumer | Assignment, fetch, checkpoint, seek, events, and close paths | Configured beginning/end/exact positioning, round-trip, seek, pause/resume, incremental and multi-partition assignment, cursor continuity, replacement, and record-fidelity scenarios |
| Classic group consumer | Membership, assignment events, fetch, checkpoint commit, seek, and close paths | Configured round-trip, seek, pause/resume, offset reset, read-committed, shutdown, record fidelity, membership ownership, offset resume, broker restart, and session recovery |
| KIP-848 consumer group | Topic UUID resolution, heartbeat, assignment translation, reconciliation, fetch, checkpoint commit, and owned-topic acknowledgement | Configured round-trip, seek, pause/resume, offset reset, read-committed, shutdown, record fidelity, membership ownership, offset resume, and session recovery in applicable Kafka 4.x cells |
| Share-group consumer | Share heartbeat membership, broker-local acquisition sessions, delivery counts, linear batches, and explicit Accept, Release, or Reject acknowledgement | Configured lifecycle, record fidelity, mixed release/reject, batch drop, maximum-record fetch, membership ownership, close uncertainty, leader recovery, and session recovery in applicable Kafka 4.x cells |
| Admin | Broad concrete request-specific core, engine, and facade paths including exact-broker routes | Configured topic create/validate/batch/partition/delete/describe/list lifecycles and failures; topic configuration and caller-ordered offset queries; cluster, consumer-group, and generic group discovery; consumer-group offset list/alter/delete plus group deletion; and caller-ordered ACL lifecycle with independent state queries |
| Transactions | Initialization, begin, produce, offset transfer, commit, abort, fencing, and close paths | Configured commit/abort, multi-record boundaries, successive transactions, fencing, and offset transfer for classic and KIP-848 groups |
| Simulation | Virtual-time execution of deterministic core effects | Development evidence, not broker emulation |
| Foreign bindings | Not included | No ABI or compatibility promise |

An API is implemented only when the public Rust facade reaches a concrete
engine owner and deterministic terminal path. An RFC statement, invariant,
guardrail, fixture, simulation, benchmark description, or exported vocabulary
alone is design evidence and must not be represented as broker support.

### Child-handle ownership

- One `Client` owns one clone-shared producer lifecycle. Every producer handle
  built from that client shares admission, flush, and close state. Use another
  client for an independently closable producer.
- One `Client` admits one directly assigned consumer for its lifetime. Use
  another client for an independent direct-consumer cursor set.
- Group, Share, Admin, and transactional handles retain the ownership contracts
  stated by their public builders and operations.

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
`0341a91df302a5c5b1c2180aa53fd20066694da3` defines the following gating cells.
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
| `apache-kafka-4-3-1-three-plaintext` | Apache Kafka 4.3.1 | Three brokers, plaintext without SASL | `kafkars-three-broker-share` | 1 |
| `apache-kafka-4-3-1-broker-role-failover` | Apache Kafka 4.3.1 | Three brokers, plaintext with controlled broker-role failover | `kafkars-broker-role-failover` | 1 |
| `apache-kafka-4-3-1-broker-policy` | Apache Kafka 4.3.1 | Single broker, SASL/PLAIN with authorizer controls | `kafkars-broker-policy` | 1 |
| `apache-kafka-4-3-1-network-faults` | Apache Kafka 4.3.1 | Single broker, plaintext through a controlled network proxy | `kafkars-network-faults` | 1 |
| `protocol-adversary` | Scripted Kafka endpoint | External protocol adversary | `kafkars-protocol-adversary` | 1 |

## Transport and authentication

| Configuration | Code path | Real-broker qualification |
| --- | --- | --- |
| Plain TCP without SASL | Present and the default | Configured for single-broker Kafka 3.7.2, 3.8.1, 3.9.2, 4.0.2, 4.1.2, 4.2.1, and 4.3.1 plus one three-broker Kafka 4.3.1 cell; consult the exact archived verdict |
| TLS with platform roots | Present | Not configured in the release tier |
| TLS with a custom PEM root bundle | Present | Configured without SASL for one three-broker Kafka 4.3.1 cell; consult the exact archived verdict |
| SASL/PLAIN over plain TCP | Present | Configured for one three-broker Kafka 4.3.1 cell; consult the exact archived verdict |
| SASL/PLAIN over custom-root TLS | Present | Not configured in the release tier |
| SCRAM-SHA-256 over plain TCP | Present | Configured for one three-broker Kafka 4.3.1 cell; consult the exact archived verdict |
| SCRAM-SHA-256 over custom-root TLS | Present | Not configured in the release tier |
| SCRAM-SHA-512 over plain TCP | Present | Configured for one three-broker Kafka 4.3.1 cell; consult the exact archived verdict |
| SCRAM-SHA-512 over custom-root TLS | Present | Not configured in the release tier |
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
