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
| KIP-848 consumer group | Concrete count-only behavior | Partial; paths requiring Kafka topic IDs fail closed |
| Admin | Broad concrete request-specific core, engine, and facade paths | Exact-broker limitation below; no maintained real-broker matrix |
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

Before a beta or production-supported release, the project intends to qualify
maintained Kafka 3.x and current Kafka 4.x releases across produce and consume,
group coordination, administration, transactions, reconnects, retries,
deadlines, and shutdown. Until that evidence exists, compatibility reports
should include the exact broker distribution and version.

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

## Known integration limits

### Exact-broker operations

The pinned `kafka-driver` does not expose broker IDs or an exact-broker route.
Operations that require exact-broker aggregation or fetch therefore fail
closed. A safe individual name-routed request is used only when it preserves
the operation's ordering, correlation, deadline, and delivery contract. The
client does not invent a broker cache or silently downgrade routing.

### KIP-848 topic IDs

The pinned driver projection does not expose Kafka topic IDs. KIP-848 behavior
that requires those IDs fails closed; count-only behavior that does not require
them remains available. Local client topic identities are ownership keys and
must not be confused with Kafka protocol topic IDs.

### Client ID propagation

`ClientBuilder::client_id` validates and retains the configured value, and
`Client::client_id` returns it. The pinned driver cannot currently propagate
that value into Kafka request headers. Do not rely on broker logs, quotas, or
metrics seeing this configured client ID.

### Foreign interfaces

This cut is Rust-only. It includes no C header, stable C ABI, or Java, Python,
Node.js, Go, or .NET binding. A future binding is a separate compatibility and
release surface.

## Project naming

The product, package, and public Rust library are `kafkars`; `kafka-client` is
the repository. `kafka-client-core` and `kafka-client-engine` are published
implementation dependencies, not separate public product identities. `zsumz`
is the maintainer and signing identity.

For build setup, see `README.md`. For security-sensitive reports, follow
`SECURITY.md` rather than opening a public issue.
