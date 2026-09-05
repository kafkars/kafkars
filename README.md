<p align="center">
  <img src="crates/kafkars/assets/kafkars-logo.svg" alt="kafkars — native Rust client for Apache Kafka®" width="780">
</p>

# kafkars

`kafkars` is a deterministic, runtime-neutral Rust client for Apache Kafka®.
It treats deadlines, retained bytes, operation completion, cancellation, and
delivery certainty as explicit ownership rather than incidental runtime
behavior.

## Status

Version `0.0.2-rc.2` is a release-candidate source preview for API and registry
integration evaluation. It is not a production-supported release or general
broker-compatibility claim.

The source includes concrete producer, direct, group, and share-group consumer,
admin, transaction, metrics, security, and shutdown APIs. There is no stable
API promise. External [Testlab](https://github.com/kafkars/testlab) is the Kafka
real-broker qualification authority; this repository chooses pull-request or
release qualification, retains the resulting evidence, and applies the required
gate. The pinned release tier defines single-broker plaintext cells for Apache
Kafka 3.7.2, 3.8.1, 3.9.2, 4.0.2, 4.1.2, 4.2.1, and 4.3.1, plus Apache Kafka
4.3.1 three-broker plaintext, custom-root TLS, SASL/PLAIN, and
SCRAM-SHA-256/512 cells. A configured cell or running workflow is not a
qualification result; only archived passing evidence for the exact client
commit and cell is eligible for a compatibility claim. This preview is
Rust-only; a future foreign interface will require its own versioned contract
and qualification.
See [support and compatibility](SUPPORT.md) for the exact boundary.

## Use

```toml
[dependencies]
kafkars = "=0.0.2-rc.2"
```

```rust
use kafkars::{Client, Result};

fn client() -> Result<Client> {
    Client::builder()
        .bootstrap_servers(["localhost:9092"])
        .client_id("orders-api")
        .build()
}
```

The crate root is deliberately limited to `Client`, `Producer`, `Consumer`,
`Admin`, `Error`, and `Result`. Supporting vocabulary lives under the owning
`admin`, `client`, `consumer`, `error`, `metrics`, `producer`, `security`,
`topic`, and `transaction` modules. Client construction remains
`Client::builder()`; its concrete builder is `client::ClientBuilder`.

Compile-checked examples cover the
[producer](crates/kafkars/examples/producer.rs),
[consumer](crates/kafkars/examples/consumer.rs),
[admin](crates/kafkars/examples/admin.rs), and
[transaction](crates/kafkars/examples/transaction.rs) surfaces.

## Design

```text
kafka-client-core ----------------------+
    deterministic policy                |
                                         v
kafka-wire -----------> kafka-driver -> kafka-client-engine ---> kafkars
    protocol bytes        RPC and I/O      integration owner      Rust facade
```

The core owns semantic time, retained-byte accounting, cancellation, and
terminal decisions without owning networking or an async runtime. Each client
owns one private native reactor thread through the engine. Runtime-neutral means
that Kafkars embeds no async executor; it does not mean threadless. The engine
also owns bounded execution, protocol adaptation, shutdown, and recovery.
`kafkars` exposes the curated public Rust API.

`kafka-client-sim` supplies virtual-time execution, and
`kafka-client-guardrails` enforces repository and architecture policy. Most
applications should depend only on `kafkars`.

## Validate

Rust `1.88.0`, Git, and Bash are required. From a clean clone:

```sh
./scripts/check
```

Cargo resolves exact published `kafka-driver 0.1.0-rc.5` and `kafka-wire
0.1.0-rc.3` packages from crates.io. `Cargo.lock` binds their registry sources
and checksums. For the root and engine manifest edges, the guardrails reject
local paths, Git dependencies, alternate registries, aliases, and manifest
`[patch]` or `[replace]` overrides.

## Known limitations

- This RC makes no production-support claim for any Kafka version or security
  combination.
- Mutual TLS, SASL/OAUTHBEARER, and SASL/GSSAPI are not exposed.
- This cut is Rust-only and has no stable foreign ABI or language bindings.

The reviewed driver integration now projects exact broker routes, Kafka topic
UUIDs, and configured client IDs. These contracts have loopback integration
evidence. Any broker-compatibility claim for them must remain limited to exact
archived passing Testlab cells; configured or local evidence is insufficient.

## License

Apache-2.0.

Read [ARCHITECTURE.md](ARCHITECTURE.md) before changing ownership boundaries.
See [CONTRIBUTING.md](CONTRIBUTING.md) for participation and
[SECURITY.md](SECURITY.md) for private vulnerability reporting.

## Trademarks

Apache Kafka and the Kafka logo are trademarks of The Apache Software
Foundation. kafkars has no affiliation with and is not
endorsed by The Apache Software Foundation. See the
[Apache Kafka trademark policy](https://kafka.apache.org/community/trademark/).
