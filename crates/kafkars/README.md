<p align="center">
  <img src="https://raw.githubusercontent.com/kafkars/kafkars/main/crates/kafkars/assets/kafkars-logo.svg" alt="kafkars — native Rust client for Apache Kafka®" width="780">
</p>

# kafkars

`kafkars` is an experimental, deterministic, runtime-neutral Rust client for
Apache Kafka®. Version `0.0.2-rc.1` is a release-candidate source preview for
API and registry integration evaluation, not a production-supported release or
broker-compatibility claim.

```toml
[dependencies]
kafkars = "=0.0.2-rc.1"
```

```rust
use kafkars::{Client, ClientBuilder};
```

See the [source repository](https://github.com/kafkars/kafkars) for the
architecture, exact limitations, qualification evidence, and contribution
guide.

Apache Kafka and the Kafka logo are trademarks of The Apache Software
Foundation. kafkars has no affiliation with and is not
endorsed by The Apache Software Foundation. See the
[Apache Kafka trademark policy](https://kafka.apache.org/community/trademark/).
