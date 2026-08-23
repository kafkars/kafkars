//! Private share acknowledgement translation contract.

use kafka_client_engine::share::{
    ShareAcknowledgementBuildErrorKind as EngineBuildErrorKind,
    ShareDisposition as EngineDisposition,
};

use super::acknowledgement::{
    ShareAcknowledgementBuildErrorKind, ShareDisposition, engine_disposition,
};

#[test]
fn dispositions_and_normalization_categories_translate_exactly() {
    assert_eq!(
        engine_disposition(ShareDisposition::Accept),
        EngineDisposition::Accept
    );
    assert_eq!(
        engine_disposition(ShareDisposition::Release),
        EngineDisposition::Release
    );
    assert_eq!(
        engine_disposition(ShareDisposition::Reject),
        EngineDisposition::Reject
    );
    let mappings = [
        (
            EngineBuildErrorKind::EmptyAcquisitions,
            ShareAcknowledgementBuildErrorKind::EmptyAcquisitions,
        ),
        (
            EngineBuildErrorKind::EmptyDecisions,
            ShareAcknowledgementBuildErrorKind::EmptyDecisions,
        ),
        (
            EngineBuildErrorKind::MixedSession,
            ShareAcknowledgementBuildErrorKind::MixedSession,
        ),
        (
            EngineBuildErrorKind::InvalidOffset,
            ShareAcknowledgementBuildErrorKind::InvalidOffset,
        ),
        (
            EngineBuildErrorKind::UnknownAcquisition,
            ShareAcknowledgementBuildErrorKind::UnknownAcquisition,
        ),
        (
            EngineBuildErrorKind::OffsetOutsideRange,
            ShareAcknowledgementBuildErrorKind::OffsetOutsideRange,
        ),
        (
            EngineBuildErrorKind::DuplicateDecision,
            ShareAcknowledgementBuildErrorKind::DuplicateDecision,
        ),
        (
            EngineBuildErrorKind::MissingDecision,
            ShareAcknowledgementBuildErrorKind::MissingDecision,
        ),
        (
            EngineBuildErrorKind::AllocationFailed,
            ShareAcknowledgementBuildErrorKind::AllocationFailed,
        ),
        (
            EngineBuildErrorKind::AccountingInvariant,
            ShareAcknowledgementBuildErrorKind::AccountingInvariant,
        ),
    ];
    for (engine, bridge) in mappings {
        assert_eq!(ShareAcknowledgementBuildErrorKind::from(engine), bridge);
    }
}
