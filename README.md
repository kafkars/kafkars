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
gate. Record-fidelity coverage carries an explicit producer timestamp through
the public delivery receipt, an independent Kafka observation, and the public
assigned-consumer record. Automatic keyed-routing coverage separately omits an
explicit partition and binds Kafkars's public receipt, independent broker
placement, and direct consumer result to a Java-compatible partition oracle.
Producer-admission coverage selects both immediate `Producer::try_send` and
bounded FIFO `Producer::send`; the waiting-send scenario retains that exact
public method through the Testlab command and independently observes its record
in Kafka. Cancellation coverage carries the same selection through the command,
invokes the matching public observer twice, and preserves stage uncertainty
through terminal and independent broker truth.
Direct-consumer delivery coverage selects both waiting
`AssignedConsumer::recv` and immediate `AssignedConsumer::try_take_batch`;
Testlab retains the exact observer choice and joins the returned batch to an
independently observed Kafka record.
Explicit child-ownership coverage builds two private producers from one client
configuration, closes one without stopping its sibling, replaces a closed
producer, and proves two private directly assigned consumers retain separate
cursors over the same records.
Multi-topic group coverage passes two caller-ordered topics through the public
builder, observes both assignments, and commits one independently matched
record from each topic under both classic and KIP-848 membership.
Multi-topic Share coverage passes two caller-ordered topics through the public
builder, observes assignments for both topics, and accepts one
independently matched exact record from each.
Share rack-identity coverage also retains the optional public builder value and
requires exact broker-reported rack IDs through singleton and plural public
Admin descriptions.
Complete Share-configuration coverage passes non-default long-poll, byte,
record, acquisition-range, attempt-timeout, membership-start, and close values
through the public builder, then requires exact public batches and independent
broker observations.
Classic membership-timing coverage passes non-default session, rebalance,
heartbeat, and rejoin values through the public builder, then requires live
single-broker operation and recovery while each broker is disrupted in turn.
Shared group-runtime coverage passes non-default processing, membership-start,
seek, and close deadlines through both classic and KIP-848 builders and
requires exact public seek replay plus orderly close.
Consumer Fetch-policy coverage passes every broker-request and retained-delivery
capacity through the public assigned, classic, and KIP-848 builders at
non-default values, then requires exact public delivery joined to independent
broker observations.
The pinned release tier defines single-broker plaintext cells for Apache
Kafka 3.7.2, 3.8.1, 3.9.2, 4.0.2, 4.1.2, 4.2.1, and 4.3.1, plus Apache Kafka
4.3.1 three-broker plaintext, custom-root TLS, SASL/PLAIN, and
SCRAM-SHA-256/512 cells, including every SASL mechanism over TLS and a dedicated
authenticated delegation-token lifecycle cell, plus a dedicated modern
Streams-group Admin lifecycle cell. A configured cell or running workflow is
not a qualification result; only archived passing evidence for the exact
client commit and cell is eligible for a compatibility claim. This preview is
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
owns one private native reactor thread through the engine. Explicit independent
producer and directly assigned consumer builders start an additional private
engine from the same client configuration, giving each successful build its own
close and cursor owner. Runtime-neutral means that Kafkars embeds no async
executor; it does not mean threadless. The engine also owns bounded execution,
protocol adaptation, shutdown, and recovery.
`kafkars` exposes the curated public Rust API.

`kafka-client-sim` supplies virtual-time execution, and
`kafka-client-guardrails` enforces repository and architecture policy. Most
applications should depend only on `kafkars`.

## Validate

Rust `1.88.0`, Git, and Bash are required. From a clean clone:

```sh
./scripts/check
```

Cargo resolves exact published `kafka-driver 0.1.0-rc.6` and `kafka-wire
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
