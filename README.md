<p align="center">
  <img src="crates/kafkars/assets/kafkars-logo.svg" alt="kafkars — native Rust client for Apache Kafka®" width="780">
</p>

# kafkars

A native Rust client for Apache Kafka® with producers, direct and group
consumers, share groups, transactions, and admin APIs.

- **Runtime-neutral:** no async executor required; each client owns a native I/O thread.
- **Bounded:** explicit deadlines, memory limits, cancellation, and delivery outcomes.
- **Deterministic core:** client policy can be tested with virtual time.

**Status:** `0.0.2-rc.2` is an experimental release candidate with no stable API
or production-support promise. See [support and compatibility](SUPPORT.md).

## Quick start

Requires Rust 1.88 or later.

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

Examples: [producer](crates/kafkars/examples/producer.rs) ·
[consumer](crates/kafkars/examples/consumer.rs) ·
[admin](crates/kafkars/examples/admin.rs) ·
[transactions](crates/kafkars/examples/transaction.rs).

## Documentation

- [Architecture](ARCHITECTURE.md) — core, driver, engine, and public API.
- [Support and compatibility](SUPPORT.md) — API status and release boundaries.
- [Qualification coverage](docs/QUALIFICATION.md) — detailed real-broker scenarios.
- [Share consumers](SHARE_CONSUMER.md) — acquisition and acknowledgement semantics.
- [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md).

Broker compatibility requires archived passing
[Testlab](https://github.com/kafkars/testlab) evidence for the exact commit and
broker configuration. Mutual TLS, SASL/OAUTHBEARER, SASL/GSSAPI, and foreign
language bindings are not available in this preview.

## Development

With Rust 1.88.0, Git, and Bash installed:

```sh
./scripts/check
```

## License

[Apache-2.0](LICENSE).

Apache Kafka and the Kafka logo are trademarks of The Apache Software
Foundation. kafkars has no affiliation with and is not endorsed by The Apache
Software Foundation. See the
[Apache Kafka trademark policy](https://kafka.apache.org/community/trademark/).
