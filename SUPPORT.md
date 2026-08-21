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
traffic. A supported release requires a real-broker matrix and separate
release authorization.

## Runtime surface

| Area | Source status | Qualification status |
| --- | --- | --- |
| Rust facade | Concrete runtime-neutral builders, futures, blocking observation, and error vocabulary | Unit-tested; no stable API promise |
| Producer | Bounded admission, partitioning, batching, retry, cancellation, flush, and close paths | No maintained real-broker matrix |
| Direct consumer | Assignment, fetch, checkpoint, seek, events, and close paths | No maintained real-broker matrix |
| Classic group consumer | Membership, assignment events, fetch, checkpoint commit, seek, and close paths | No maintained real-broker matrix |
| KIP-848 consumer group | Topic UUID resolution, heartbeat, assignment translation, reconciliation, and owned-topic acknowledgement | Integrated and unit-tested; no maintained real-broker matrix |
| Admin | Broad concrete request-specific core, engine, and facade paths including exact-broker routes | Integrated and unit-tested; no maintained real-broker matrix |
| Transactions | Initialization, begin, produce, offset transfer, commit, abort, fencing, and close paths | No maintained real-broker matrix |
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

The qualification target matrix is explicit even though no cell has passed a
release qualification yet:

<!-- qualification-evidence:begin -->
| Kafka version | Intended lane | Current evidence status |
| --- | --- | --- |
| 4.3.1 | Primary/current required gate | Not yet qualified |
| 4.2.1 | Supported compatibility | Not yet qualified |
| 4.1.2 | Supported compatibility | Not yet qualified |
| 3.9.2 | Optional legacy compatibility, non-gating | Not yet qualified |
<!-- qualification-evidence:end -->

Kafka 3.9.2 is a legacy lane, not a maintained upstream release. Every future
qualified cell must be generated from archived qualification evidence rather
than maintained as a prose promise. Until that evidence exists, compatibility
reports should include the exact broker distribution and version.

## Transport and authentication

| Configuration | Code path | Real-broker qualification |
| --- | --- | --- |
| Plain TCP without SASL | Present and the default | Not release-qualified |
| TLS with platform roots | Present | Not release-qualified |
| TLS with a custom PEM root bundle | Present | Not release-qualified |
| SASL/PLAIN over plain TCP or TLS | Present | Not release-qualified |
| SCRAM-SHA-256 over plain TCP or TLS | Present | Not release-qualified |
| SCRAM-SHA-512 over plain TCP or TLS | Present | Not release-qualified |
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
