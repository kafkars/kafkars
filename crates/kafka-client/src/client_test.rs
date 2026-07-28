//! Tests for facade-owned client construction and configuration views.

use std::time::Duration;

use crate::{Client, Compression, DeliveryStatus, ErrorKind, ProducerLimits, ReadIsolation};

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
fn client_builder_translates_explicit_producer_limits_before_startup() {
    let limits = ProducerLimits::new(4_096, 4, 3, 2_048, 2, 1_024, Duration::from_millis(7));
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .producer_limits(limits)
        .build();

    assert!(client.is_ok());
}

#[test]
fn empty_bootstrap_set_is_rejected_in_the_facade() {
    let result = Client::builder().build();
    let Err(error) = result else {
        panic!("an empty bootstrap set should be rejected");
    };

    assert_eq!(error.kind(), ErrorKind::Configuration);
}

#[test]
fn readiness_after_shutdown_is_immediately_definitely_unsent() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let shutdown = client.shutdown();

    let error = client
        .ready()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("shutdown must fence readiness admission"));
    assert_eq!(error.kind(), ErrorKind::State);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    shutdown
        .wait()
        .unwrap_or_else(|error| panic!("finish client shutdown: {error}"));
}
