# Support and compatibility

This document states the public boundary of the experimental Rust client for
Apache Kafka®. It is intentionally narrower than the amount of code in the
repository.

## Release status

- Workspace version: `0.0.1`
- Publication: enabled for `kafkars`, `kafka-client-core`, and
  `kafka-client-engine`; disabled for simulation and guardrails
- Stability: no semantic-versioning or source-compatibility promise
- Supported releases: no production release; `0.0.1` is a source preview
- Intended audience: design review, source evaluation, and contribution

The current source is not a public beta and is not recommended for production
traffic. A supported release requires complete passing archived real-broker
evidence and separate release authorization.

## Runtime surface

| Area | Source status | Qualification status |
| --- | --- | --- |
| Rust facade | Concrete runtime-neutral builders, futures, blocking observation, and error vocabulary | Unit-tested; no stable API promise |
| Producer | Bounded admission, partitioning, batching, retry, cancellation, flush, and close paths | Eligible for Testlab qualification; no archived passing gating evidence |
| Direct consumer | Assignment, fetch, checkpoint, seek, events, and close paths | Eligible for Testlab qualification; no archived passing gating evidence |
| Classic group consumer | Membership, assignment events, fetch, checkpoint commit, seek, and close paths | Eligible for Testlab qualification; no archived passing gating evidence |
| KIP-848 consumer group | Topic UUID resolution, heartbeat, assignment translation, reconciliation, fetch, checkpoint commit, and owned-topic acknowledgement | Eligible for Testlab qualification; no archived passing gating evidence |
| Admin | Broad concrete request-specific core, engine, and facade paths including exact-broker routes | Eligible for Testlab qualification; no archived passing gating evidence |
| Transactions | Initialization, begin, produce, offset transfer, commit, abort, fencing, and close paths | Eligible for Testlab qualification; no archived passing gating evidence |
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

Only archived passing evidence from the applicable gating Testlab run can
establish a support claim. A Testlab `Failed` verdict blocks the claim and may
identify a client defect. An `Invalid` verdict means the run did not constitute
valid qualification and also blocks release. Until passing evidence exists,
compatibility reports should include the exact broker distribution and version
without describing either as supported.

## Transport and authentication

| Configuration | Code path | Real-broker qualification |
| --- | --- | --- |
| Plain TCP without SASL | Present and the default | No archived passing Testlab release evidence |
| TLS with platform roots | Present | No archived passing Testlab release evidence |
| TLS with a custom PEM root bundle | Present | No archived passing Testlab release evidence |
| SASL/PLAIN over plain TCP or custom-root TLS | Present | No archived passing Testlab release evidence |
| SCRAM-SHA-256 over plain TCP or custom-root TLS | Present | No archived passing Testlab release evidence |
| SCRAM-SHA-512 over plain TCP or custom-root TLS | Present | No archived passing Testlab release evidence |
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
production support promise without archived passing cells.

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
