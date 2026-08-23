//! Validation evidence for public `ShareFetch` settings.

use std::time::Duration;

use super::{EngineShareConsumerFetchConfig, share_consumer_fetch::ShareConsumerFetchConfigError};

#[test]
fn defaults_compile_to_bounded_share_fetch_fields() {
    let validated = EngineShareConsumerFetchConfig::default()
        .validate()
        .unwrap_or_else(|error| panic!("default config: {error:?}"));

    assert_eq!(validated.max_wait_ms(), 500);
    assert_eq!(validated.min_bytes(), 1);
    assert_eq!(validated.max_bytes(), 1024 * 1024);
    assert_eq!(validated.max_records(), 500);
    assert_eq!(validated.batch_size(), 500);
    assert_eq!(validated.attempt_timeout(), Duration::from_secs(30));
}

#[test]
fn invalid_share_fetch_fields_fail_before_registration() {
    let zero_records = EngineShareConsumerFetchConfig::default();
    let zero_records = EngineShareConsumerFetchConfig::new(
        zero_records.max_wait(),
        zero_records.min_bytes(),
        zero_records.max_bytes(),
        0,
        zero_records.batch_size(),
        zero_records.attempt_timeout(),
    );
    assert_eq!(
        zero_records.validate(),
        Err(ShareConsumerFetchConfigError::MaxRecords)
    );

    let incoherent = EngineShareConsumerFetchConfig::new(
        Duration::from_millis(500),
        2,
        1,
        1,
        1,
        Duration::from_secs(1),
    );
    assert_eq!(
        incoherent.validate(),
        Err(ShareConsumerFetchConfigError::MinBytesExceedMaxBytes)
    );
}
