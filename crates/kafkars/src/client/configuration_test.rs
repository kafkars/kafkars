//! Typed producer configuration selection and startup validation scenarios.

use std::time::Duration;

use crate::{Client, Compression, ErrorKind, ProducerConfig, ProducerLimits, ProducerRetryConfig};

#[test]
fn complete_and_convenience_selection_views_are_exact() {
    let retry = ProducerRetryConfig::new(5, Duration::from_millis(250));
    let limits = ProducerLimits::default().with_linger(Duration::from_millis(20));
    let complete = ProducerConfig::default()
        .with_delivery_timeout(Duration::from_secs(45))
        .with_compression(Compression::Lz4)
        .with_retry(retry)
        .with_limits(limits);
    let builder = Client::builder().producer_config(complete);

    assert_eq!(builder.selected_producer_config(), complete);
    assert_eq!(
        builder.selected_producer_delivery_timeout(),
        Duration::from_secs(45)
    );
    assert_eq!(builder.selected_producer_compression(), Compression::Lz4);
    assert_eq!(builder.selected_producer_retry(), retry);
    assert_eq!(builder.selected_producer_limits(), limits);

    let convenience = Client::builder()
        .producer_delivery_timeout(Duration::from_secs(60))
        .producer_compression(Compression::Zstd)
        .producer_retry(7, Duration::from_millis(400))
        .producer_limits(limits);
    assert_eq!(
        convenience.selected_producer_config(),
        ProducerConfig::new(
            Duration::from_secs(60),
            Compression::Zstd,
            ProducerRetryConfig::new(7, Duration::from_millis(400)),
            limits,
        )
    );
}

#[test]
fn invalid_typed_policy_rejects_before_host_start() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .producer_config(ProducerConfig::default().with_delivery_timeout(Duration::from_secs(45)))
        .build()
        .unwrap_or_else(|error| panic!("valid producer config must start: {error}"));
    assert_eq!(
        client.producer().selected_delivery_timeout(),
        Duration::from_secs(45)
    );

    let result = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .producer_config(ProducerConfig::default().with_delivery_timeout(Duration::ZERO))
        .build();
    let Err(error) = result else {
        panic!("zero producer delivery timeout must reject");
    };

    assert_eq!(error.kind(), ErrorKind::Configuration);
}
