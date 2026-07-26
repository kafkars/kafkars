//! Tests for facade-owned client construction and configuration views.

use crate::{Client, Compression, ErrorKind, ReadIsolation};

#[test]
fn client_retains_facade_configuration_across_clones() {
    let client = Client::builder()
        .bootstrap_servers(["broker-a:9092", "broker-b:9092"])
        .client_id("orders-api")
        .build();
    let Ok(client) = client else {
        panic!("valid client configuration should build");
    };
    let clone = client.clone();

    assert_eq!(client.client_id(), Some("orders-api"));
    assert_eq!(
        clone.bootstrap_servers(),
        &["broker-a:9092".to_owned(), "broker-b:9092".to_owned()]
    );
}

#[test]
fn client_builder_accepts_each_closed_producer_compression_choice() {
    for compression in [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ] {
        let client = Client::builder()
            .bootstrap_servers(["127.0.0.1:1"])
            .producer_compression(compression)
            .build();
        assert!(client.is_ok());
    }
}

#[test]
fn client_builder_accepts_each_closed_assigned_read_isolation_choice() {
    for read_isolation in [ReadIsolation::ReadUncommitted, ReadIsolation::ReadCommitted] {
        let client = Client::builder()
            .bootstrap_servers(["127.0.0.1:1"])
            .assigned_consumer_read_isolation(read_isolation)
            .build();
        assert!(client.is_ok());
    }
}

#[test]
fn empty_bootstrap_set_is_rejected_in_the_facade() {
    let result = Client::builder().build();
    let Err(error) = result else {
        panic!("an empty bootstrap set should be rejected");
    };

    assert_eq!(error.kind(), ErrorKind::Configuration);
}
