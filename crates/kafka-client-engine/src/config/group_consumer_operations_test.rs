//! Hosted-group operation timeout validation evidence.

use std::time::Duration;

use super::group_consumer_operations::{
    EngineGroupConsumerOperationConfig, GroupConsumerOperationConfigError,
};

#[test]
fn defaults_and_distinct_durations_validate_exactly() {
    let default = EngineGroupConsumerOperationConfig::default()
        .validate()
        .unwrap_or_else(|error| panic!("default group operations: {error:?}"));
    assert_eq!(default.seek_timeout(), Duration::from_secs(30));
    assert_eq!(default.close_timeout(), Duration::from_secs(30));

    let distinct =
        EngineGroupConsumerOperationConfig::new(Duration::from_nanos(7), Duration::from_nanos(11))
            .validate()
            .unwrap_or_else(|error| panic!("distinct group operations: {error:?}"));
    assert_eq!(distinct.seek_timeout(), Duration::from_nanos(7));
    assert_eq!(distinct.close_timeout(), Duration::from_nanos(11));
}

#[test]
fn zero_and_unrepresentable_durations_reject_independently() {
    assert_eq!(
        EngineGroupConsumerOperationConfig::new(Duration::ZERO, Duration::from_secs(1)).validate(),
        Err(GroupConsumerOperationConfigError::SeekTimeout)
    );
    assert_eq!(
        EngineGroupConsumerOperationConfig::new(Duration::MAX, Duration::from_secs(1)).validate(),
        Err(GroupConsumerOperationConfigError::SeekTimeout)
    );
    assert_eq!(
        EngineGroupConsumerOperationConfig::new(Duration::from_secs(1), Duration::ZERO).validate(),
        Err(GroupConsumerOperationConfigError::CloseTimeout)
    );
    assert_eq!(
        EngineGroupConsumerOperationConfig::new(Duration::from_secs(1), Duration::MAX).validate(),
        Err(GroupConsumerOperationConfigError::CloseTimeout)
    );
}
