//! Stable pre-admission offset-deletion error scenarios.

use super::{
    DeleteConsumerGroupOffsetsAdmissionError, DeleteConsumerGroupOffsetsAdmissionErrorKind,
};

#[test]
fn admission_error_preserves_every_stable_category() {
    for kind in [
        DeleteConsumerGroupOffsetsAdmissionErrorKind::InvalidRequest,
        DeleteConsumerGroupOffsetsAdmissionErrorKind::InvalidDeadline,
        DeleteConsumerGroupOffsetsAdmissionErrorKind::Contended,
        DeleteConsumerGroupOffsetsAdmissionErrorKind::Closed,
        DeleteConsumerGroupOffsetsAdmissionErrorKind::Capacity,
        DeleteConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes,
        DeleteConsumerGroupOffsetsAdmissionErrorKind::IdentityExhausted,
        DeleteConsumerGroupOffsetsAdmissionErrorKind::HostUnavailable,
    ] {
        let error = DeleteConsumerGroupOffsetsAdmissionError::new(kind);
        assert_eq!(error.kind(), kind);
        assert!(error.to_string().contains("DeleteConsumerGroupOffsets"));
    }
}
