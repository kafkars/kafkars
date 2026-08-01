//! Checked duration-normalization scenarios for engine configuration.

use std::time::Duration;

use super::{
    EngineConfig,
    validation::{EngineConfigError, duration_ticks},
};

#[test]
fn duration_ticks_are_exact_or_rejected_before_host_start() {
    assert_eq!(duration_ticks(Duration::from_nanos(7)), Ok(7));
    assert_eq!(
        duration_ticks(Duration::MAX),
        Err(EngineConfigError::DurationOverflow)
    );
}

#[test]
fn request_header_client_id_is_bounded_before_host_start() {
    let maximum = "x".repeat(i16::MAX as usize);
    assert!(
        EngineConfig::new(vec!["127.0.0.1:9092".to_owned()])
            .with_client_id(Some(maximum))
            .validate()
            .is_ok()
    );

    let oversized = "x".repeat(i16::MAX as usize + 1);
    assert_eq!(
        EngineConfig::new(vec!["127.0.0.1:9092".to_owned()])
            .with_client_id(Some(oversized))
            .validate()
            .err(),
        Some(EngineConfigError::ClientIdTooLong)
    );
}
