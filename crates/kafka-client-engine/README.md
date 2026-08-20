# kafka-client-engine

`kafka-client-engine` is the runtime-neutral execution layer used by the
experimental `kafkars` Rust client. It owns bounded integration with
`kafka-driver`, protocol adaptation, completion, shutdown, and recovery.

This implementation crate is packaged separately so it can be published before
`kafkars` and satisfy the facade's crates.io dependency. Its API is not an
independent stability promise; application code should normally depend on
`kafkars` instead.

KAFKA is a registered trademark of The Apache Software Foundation and has been
licensed for use by kafkars. kafkars has no affiliation with and is not
endorsed by The Apache Software Foundation. See the
[Apache Kafka trademark policy](https://kafka.apache.org/community/trademark/).
