//! Consumer-capacity validation evidence.

use super::{ConsumerLimitsError, EngineConsumerLimits, validate_consumer_fetch_envelope};

#[test]
fn default_limits_compile_exactly() {
    let limits = EngineConsumerLimits::default()
        .validate()
        .unwrap_or_else(|error| panic!("default consumer limits rejected: {error:?}"));

    assert_eq!(limits.in_flight_fetches(), 8);
    assert_eq!(limits.buffered_batches(), 8);
    assert_eq!(limits.buffered_bytes(), 8 * 1024 * 1024);
    assert_eq!(limits.max_batch_bytes(), 1024 * 1024);
    assert_eq!(
        validate_consumer_fetch_envelope(limits, 1024 * 1024),
        Ok(())
    );
}

#[test]
fn zero_and_incoherent_limits_are_closed() {
    let cases = [
        (
            EngineConsumerLimits::new(0, 1, 1, 1),
            ConsumerLimitsError::InFlightFetches,
        ),
        (
            EngineConsumerLimits::new(1, 0, 1, 1),
            ConsumerLimitsError::BufferedBatches,
        ),
        (
            EngineConsumerLimits::new(1, 1, 0, 1),
            ConsumerLimitsError::BufferedBytes,
        ),
        (
            EngineConsumerLimits::new(1, 1, 1, 0),
            ConsumerLimitsError::MaxBatchBytes,
        ),
        (
            EngineConsumerLimits::new(1, 1, 1, 2),
            ConsumerLimitsError::MaxBatchExceedsBufferedBytes,
        ),
    ];

    for (limits, expected) in cases {
        assert_eq!(limits.validate(), Err(expected));
    }
}

#[test]
fn hard_batch_envelope_covers_the_requested_partition_fetch_bytes() {
    let limits = EngineConsumerLimits::new(1, 1, 1024, 512)
        .validate()
        .unwrap_or_else(|error| panic!("limits rejected: {error:?}"));

    assert_eq!(
        validate_consumer_fetch_envelope(limits, 513),
        Err(ConsumerLimitsError::MaxBatchBelowPartitionFetchBytes)
    );
}
