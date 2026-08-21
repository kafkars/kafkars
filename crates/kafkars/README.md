# kafkars

`kafkars` is an experimental, deterministic, runtime-neutral Rust client for
Apache Kafka®. Version `0.0.1` is a source preview for API evaluation, not a
beta, production release, or broker-compatibility claim.

```toml
[dependencies]
kafkars = "0.0.1"
```

```rust
use kafkars::{Client, ClientBuilder};
```

See the [source repository](https://github.com/kafkars/kafkars) for the
architecture, exact limitations, qualification evidence, and contribution
guide.

KAFKA is a registered trademark of The Apache Software Foundation and has been
licensed for use by kafkars. kafkars has no affiliation with and is not
endorsed by The Apache Software Foundation. See the
[Apache Kafka trademark policy](https://kafka.apache.org/community/trademark/).
