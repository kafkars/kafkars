# kafka-client-core

`kafka-client-core` is the deterministic policy layer used by the experimental
`kafkars` Rust client. It owns deadlines, admission, retained-byte accounting,
terminal decisions, cancellation, and domain state transitions without owning
networking or an async runtime.

This implementation crate is packaged separately so it can be published before
`kafkars` and satisfy the facade's crates.io dependency. Its API is not an
independent stability promise; application code should normally depend on
`kafkars` instead.

KAFKA is a registered trademark of The Apache Software Foundation and has been
licensed for use by kafkars. kafkars has no affiliation with and is not
endorsed by The Apache Software Foundation. See the
[Apache Kafka trademark policy](https://kafka.apache.org/community/trademark/).
