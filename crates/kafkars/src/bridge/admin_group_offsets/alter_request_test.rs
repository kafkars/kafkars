//! Inert group-offset alteration bridge request scenarios.

use std::time::Duration;

use kafka_client_engine::{
    AlterConsumerGroupOffsetTarget as EngineTarget,
    AlterConsumerGroupOffsetsRequest as EngineRequest,
};

use crate::ConsumerGroupOffsetAlteration;

use super::alter_request::AlterConsumerGroupOffsetsAdminRequest;

#[test]
fn request_is_linear_sendable_and_preserves_optional_epoch_and_metadata() {
    fn assert_send<T: Send>() {}
    assert_send::<AlterConsumerGroupOffsetsAdminRequest>();

    let request = AlterConsumerGroupOffsetsAdminRequest::new(
        "payments".to_owned(),
        vec![
            ConsumerGroupOffsetAlteration::new("orders", 7, 42)
                .leader_epoch(9)
                .metadata(""),
            ConsumerGroupOffsetAlteration::new("audit", 1, 3),
        ],
    );
    assert!(format!("{request:?}").contains("AlterConsumerGroupOffsetsAdminRequest"));
    assert_eq!(
        request.into_engine(),
        EngineRequest::new(
            "payments".to_owned(),
            vec![
                EngineTarget::new("orders".to_owned(), 7, 42, Some(9), Some(String::new())),
                EngineTarget::new("audit".to_owned(), 1, 3, None, None),
            ],
        )
    );
}

#[test]
fn request_preserves_omitted_explicit_and_zero_retention_time() {
    let target = || ConsumerGroupOffsetAlteration::new("orders", 7, 42);
    let engine_target = || EngineTarget::new("orders".to_owned(), 7, 42, None, None);

    let omitted = AlterConsumerGroupOffsetsAdminRequest::new("payments".to_owned(), vec![target()]);
    assert_eq!(
        omitted.into_engine(),
        EngineRequest::new("payments".to_owned(), vec![engine_target()])
    );

    let explicit = Duration::from_millis(30_001);
    let request = AlterConsumerGroupOffsetsAdminRequest::new("payments".to_owned(), vec![target()])
        .with_retention_time(explicit);
    assert_eq!(
        request.into_engine(),
        EngineRequest::new("payments".to_owned(), vec![engine_target()])
            .with_retention_time(explicit)
    );

    let request = AlterConsumerGroupOffsetsAdminRequest::new("payments".to_owned(), vec![target()])
        .with_retention_time(Duration::ZERO);
    assert_eq!(
        request.into_engine(),
        EngineRequest::new("payments".to_owned(), vec![engine_target()])
            .with_retention_time(Duration::ZERO)
    );
}
