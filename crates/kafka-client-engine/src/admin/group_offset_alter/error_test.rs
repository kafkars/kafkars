//! Stable pre-admission offset-alteration error scenarios.

use super::{
    AlterConsumerGroupOffsetTarget, AlterConsumerGroupOffsetsAdmissionError,
    AlterConsumerGroupOffsetsAdmissionErrorKind, AlterConsumerGroupOffsetsRequest,
};

#[test]
fn admission_error_preserves_every_stable_category() {
    for kind in [
        AlterConsumerGroupOffsetsAdmissionErrorKind::InvalidRequest,
        AlterConsumerGroupOffsetsAdmissionErrorKind::InvalidDeadline,
        AlterConsumerGroupOffsetsAdmissionErrorKind::Contended,
        AlterConsumerGroupOffsetsAdmissionErrorKind::Closed,
        AlterConsumerGroupOffsetsAdmissionErrorKind::Capacity,
        AlterConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes,
        AlterConsumerGroupOffsetsAdmissionErrorKind::IdentityExhausted,
        AlterConsumerGroupOffsetsAdmissionErrorKind::HostUnavailable,
    ] {
        let request = request();
        let error = AlterConsumerGroupOffsetsAdmissionError::new(kind, request.clone());
        assert_eq!(error.kind(), kind);
        assert!(error.to_string().contains("AlterConsumerGroupOffsets"));
        assert_eq!(error.into_request(), request);
    }
}

fn request() -> AlterConsumerGroupOffsetsRequest {
    AlterConsumerGroupOffsetsRequest::new(
        "workers".to_owned(),
        vec![AlterConsumerGroupOffsetTarget::new(
            "orders".to_owned(),
            0,
            91,
            Some(7),
            Some(String::new()),
        )],
    )
}
