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
| Producer | Bounded admission, partitioning, batching, retry, cancellation, flush, and close paths | PR and nightly scenarios defined; no archived passing qualification cell |
| Direct consumer | Assignment, fetch, checkpoint, seek, events, and close paths | PR and nightly scenarios defined; no archived passing qualification cell |
| Classic group consumer | Membership, assignment events, fetch, checkpoint commit, seek, and close paths | PR and nightly scenarios defined; no archived passing qualification cell |
| KIP-848 consumer group | Topic UUID resolution, heartbeat, assignment translation, reconciliation, fetch, checkpoint commit, and owned-topic acknowledgement | Multi-member redistribution, explicit member close, client shutdown, and committed-offset resume scenario defined; no archived passing qualification cell |
| Admin | Broad concrete request-specific core, engine, and facade paths including exact-broker routes | PR and nightly scenarios defined; no archived passing qualification cell |
| Transactions | Initialization, begin, produce, offset transfer, commit, abort, fencing, and close paths | PR and nightly scenarios defined; no archived passing qualification cell |
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

The qualification target matrix is explicit even though no cell has archived
passing evidence yet:

<!-- qualification-evidence:begin -->
| Kafka | Pull-request profile | Scheduled profile | Current evidence status |
| --- | --- | --- | --- |
| 4.3.1 | Full plaintext plus three security-smoke cells | Full over eight matrix security modes | Not yet qualified |
| 4.2.1 | Compatibility smoke, plaintext | Full, plaintext | Not yet qualified |
| 4.1.2 | Compatibility smoke, plaintext | Full, plaintext | Not yet qualified |
| 4.0.2 | Compatibility smoke, plaintext | Full, plaintext | Not yet qualified |
| 3.9.2 | Classic, plaintext, gating | Classic, plaintext, advisory | Not yet qualified |
| 3.8.1 | Classic, plaintext, gating | Classic, plaintext, advisory | Not yet qualified |
| 3.7.2 | Classic, plaintext, gating | Classic, plaintext, advisory | Not yet qualified |
<!-- qualification-evidence:end -->

The Kafka 3.x targets are legacy lanes, not maintained upstream releases. Their
scheduled results are advisory, but all three are mandatory pull-request
gates. Every future qualified cell must be generated from archived
qualification evidence rather than maintained as a prose promise. Until that
evidence exists, compatibility reports should include the exact broker
distribution and version.

`.github/workflows/qualification.yml` runs ten explicit pull-request cells and
fourteen explicit scheduled cells. It has no release profile. The pull-request
gate downloads the cell artifacts and archives an evidence-generated aggregate
containing `compatibility.json`, `COMPATIBILITY.md`, and `SUPPORT.md`.
Incomplete sets, mixed crate graphs, mutable image references, failed runners,
and failed gating cells cannot produce a qualified aggregate.

## Transport and authentication

| Configuration | Code path | Real-broker qualification |
| --- | --- | --- |
| Plain TCP without SASL | Present and the default | Targeted; no archived passing evidence |
| TLS with platform roots | Present | Explicitly unqualified by the self-signed test matrix |
| TLS with a custom PEM root bundle | Present | Targeted with hostname-rejection checks; no archived passing evidence |
| SASL/PLAIN over plain TCP or custom-root TLS | Present | Targeted with wrong-secret checks; no archived passing evidence |
| SCRAM-SHA-256 over plain TCP or custom-root TLS | Present | Targeted with wrong-secret checks; no archived passing evidence |
| SCRAM-SHA-512 over plain TCP or custom-root TLS | Present | Targeted with wrong-secret checks; no archived passing evidence |
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

The broker matrix is scoped to cover initial classic cooperative assignment,
classic member shutdown/resume, and KIP-848 redistribution across separate
client hosts. The KIP-848 scenario fetches and commits on both members, shuts
one client down, then requires the survivor to resume both partitions at the
committed offsets. Concurrent multi-member progress within one client host is
not yet qualified.

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
