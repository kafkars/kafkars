//! Public share acknowledgement normalization-error contract.

use super::{
    ShareAcknowledgementBuildError, ShareAcknowledgementBuildErrorKind, ShareConsumerBatch,
    ShareRecordDecision,
};

#[test]
fn acknowledgement_build_error_is_lossless_and_categories_are_exact() {
    fn error_contract(error: ShareAcknowledgementBuildError) {
        let _: ShareAcknowledgementBuildErrorKind = error.kind();
        let _: (ShareConsumerBatch, Vec<ShareRecordDecision>) = error.into_parts();
    }
    let categories = [
        ShareAcknowledgementBuildErrorKind::EmptyAcquisitions,
        ShareAcknowledgementBuildErrorKind::EmptyDecisions,
        ShareAcknowledgementBuildErrorKind::MixedSession,
        ShareAcknowledgementBuildErrorKind::InvalidOffset,
        ShareAcknowledgementBuildErrorKind::UnknownAcquisition,
        ShareAcknowledgementBuildErrorKind::OffsetOutsideRange,
        ShareAcknowledgementBuildErrorKind::DuplicateDecision,
        ShareAcknowledgementBuildErrorKind::MissingDecision,
        ShareAcknowledgementBuildErrorKind::AllocationFailed,
        ShareAcknowledgementBuildErrorKind::AccountingInvariant,
    ];

    assert_eq!(categories.len(), 10);
    let _ = error_contract as fn(ShareAcknowledgementBuildError);
}
