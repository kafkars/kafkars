//! Scenarios for stable pre-admission group-offset errors.

use super::{ListConsumerGroupOffsetsAdmissionError, ListConsumerGroupOffsetsAdmissionErrorKind};

#[test]
fn admission_error_preserves_each_stable_category() {
    for kind in [
        ListConsumerGroupOffsetsAdmissionErrorKind::InvalidRequest,
        ListConsumerGroupOffsetsAdmissionErrorKind::InvalidDeadline,
        ListConsumerGroupOffsetsAdmissionErrorKind::Contended,
        ListConsumerGroupOffsetsAdmissionErrorKind::Closed,
        ListConsumerGroupOffsetsAdmissionErrorKind::Capacity,
        ListConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes,
        ListConsumerGroupOffsetsAdmissionErrorKind::IdentityExhausted,
        ListConsumerGroupOffsetsAdmissionErrorKind::HostUnavailable,
    ] {
        let error = ListConsumerGroupOffsetsAdmissionError::new(kind);
        assert_eq!(error.kind(), kind);
        assert!(error.to_string().contains("ListConsumerGroupOffsets"));
    }
}
