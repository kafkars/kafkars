# kafkars

`kafkars` is an experimental Rust client for Apache Kafka® built around a
deterministic semantic core and a runtime-neutral execution engine. The project
treats deadlines, retained bytes, operation completion, cancellation, and
delivery certainty as explicit ownership rather than incidental runtime
behavior.

Version `0.0.1` is a source preview, not a beta or production release. The
user-facing `kafkars` crate and its `kafka-client-core` and
`kafka-client-engine` implementation dependencies are publishable;
`kafka-client-sim` and `kafka-client-guardrails` remain unpublished. There is
no stable API promise or qualified Kafka broker matrix yet.

## What is here

The Rust workspace contains:

- `kafka-client-core`, deterministic producer, consumer, admin, and
  transaction policy with no networking or async-runtime dependency;
- `kafka-client-engine`, the bounded integration owner for one embedded
  `kafka-driver` reactor;
- `kafkars`, the user-facing Rust facade;
- `kafka-client-sim`, virtual-time execution of core state machines; and
- `kafka-client-guardrails`, executable repository and architecture policy.

The source contains concrete Rust paths for producing, direct and group
consumption, administration, transactions, metrics, security, and shutdown.
That does not mean every path is broker-qualified. Contracts, simulations,
fixtures, and benchmark descriptions are design evidence; they are not a
claim of interoperability with a real Kafka deployment. See [support and
compatibility](SUPPORT.md) for the current boundary.

No C ABI or Java, Python, Node.js, Go, or .NET binding is included in this
preview. A future foreign interface would require its own versioned contract
and qualification; this repository makes no foreign-ABI compatibility
promise.

## Use the source-preview crate

```toml
[dependencies]
kafkars = "0.0.1"
```

The public Rust crate name is also `kafkars`:

```rust
use kafkars::{Client, ClientBuilder};
```

This version is for API evaluation. It is not a claim of production readiness
or broker compatibility beyond the exact evidence described below.

## Build from a clean clone

The workspace intentionally uses exact, path-based sibling checkouts of
`kafka-driver` and `kafka-wire`. Rust `1.88.0`, Git, and Bash are required.
After cloning this repository, run:

```sh
./scripts/bootstrap-siblings
./scripts/check-dependency-provenance
cargo test --locked --workspace --all-features
```

`bootstrap-siblings` reads the two reviewed commits from
`dependencies/sibling-revisions.env` without evaluating the file, then creates
`../kafka-driver` and `../kafka-protocol`. It never changes an existing sibling
checkout. If either path already exists at a different revision or has
unreviewed state, the provenance check fails closed and leaves it for you to
resolve.

The complete local gate is:

```sh
./scripts/check
```

Generate the Rust API documentation with:

```sh
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --all-features --no-deps
```

## Important limitations

- No Kafka version or security combination is release-supported yet because
  this cut has no maintained real-broker qualification matrix.
- Exact-broker aggregation and fetch behavior fail closed when the reviewed
  driver cannot provide broker identity or an exact-broker route. A safe
  individual name-routed request is used only where it preserves the operation
  contract.
- KIP-848 paths that require Kafka topic IDs fail closed. Count-only behavior
  that does not require those IDs remains available.
- `ClientBuilder::client_id` retains validated configuration, but the pinned
  driver does not yet put it into Kafka request headers.
- The C ABI and language bindings are intentionally deferred.

## Project identity

`kafkars` is the project, source repository, package, and public Rust crate
name. The repository is available at
[github.com/kafkars/kafkars](https://github.com/kafkars/kafkars).
`kafka-client-core` and `kafka-client-engine` retain implementation-oriented
package identities; most users should depend only on `kafkars`. `zsumz` is the
maintainer and signing identity, not a second client implementation.

Read [ARCHITECTURE.md](ARCHITECTURE.md) before changing ownership boundaries.
Contributions are described in [CONTRIBUTING.md](CONTRIBUTING.md), private
vulnerability reporting in [SECURITY.md](SECURITY.md), and the exact support
status in [SUPPORT.md](SUPPORT.md). The source is licensed under
[Apache-2.0](LICENSE).

## Trademarks

KAFKA is a registered trademark of The Apache Software Foundation and has been
licensed for use by kafkars. kafkars has no affiliation with and is not
endorsed by The Apache Software Foundation. See the
[Apache Kafka trademark policy](https://kafka.apache.org/community/trademark/).
