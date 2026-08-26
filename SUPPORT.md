# Support and compatibility

This document states the public boundary of the experimental Rust client for
Apache Kafka®. It is intentionally narrower than the amount of code in the
repository.

## Release status

- Workspace version: `0.0.2-rc.1`
- Publication: enabled for `kafkars`, `kafka-client-core`, and
  `kafka-client-engine`; disabled for simulation and guardrails
- Stability: no semantic-versioning or source-compatibility promise
- Supported releases: no production release; `0.0.2-rc.1` is a
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
| Producer | Bounded admission, partitioning, batching, retry, cancellation, flush, and close paths | Configured round-trip, partition-routing, batch, broker-restart, and rolling-restart scenarios |
| Direct consumer | Assignment, fetch, checkpoint, seek, events, and close paths | Configured assigned-consumer round-trip scenario |
| Classic group consumer | Membership, assignment events, fetch, checkpoint commit, seek, and close paths | Configured classic-group round-trip and broker-restart scenarios |
| KIP-848 consumer group | Topic UUID resolution, heartbeat, assignment translation, reconciliation, fetch, checkpoint commit, and owned-topic acknowledgement | Configured consumer-protocol group round-trip scenario in applicable Kafka 4.x cells |
| Share-group consumer | Share heartbeat membership, broker-local acquisition sessions, delivery counts, linear batches, and explicit Accept, Release, or Reject acknowledgement | Configured lifecycle, membership-ownership, close-uncertainty, leader-recovery, and session-recovery scenarios in their applicable Kafka 4.x cells |
| Admin | Broad concrete request-specific core, engine, and facade paths including exact-broker routes | Configured create-topic, create-partitions, describe-topic, list-topics, list-offsets, and list-consumer-group-offsets scenarios |
| Transactions | Initialization, begin, produce, offset transfer, commit, abort, fencing, and close paths | Configured commit-and-abort and fencing scenarios |
| Simulation | Virtual-time execution of deterministic core effects | Development evidence, not broker emulation |
| Foreign bindings | Not included | No ABI or compatibility promise |

An API is implemented only when the public Rust facade reaches a concrete
engine owner and deterministic terminal path. An RFC statement, invariant,
guardrail, fixture, simulation, benchmark description, or exported vocabulary
alone is design evidence and must not be represented as broker support.

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
`54ad151fd151e352e1fde8851b214493042878ef` defines the following gating cells.
This table records configuration only. The archived qualification artifact is
the authority for whether any cell passed, failed, or was invalid.

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
