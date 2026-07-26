//! Inert group-offset alteration bridge request scenarios.

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
