//! Exhaustive protocol-failure classification for feature mutation.

use kafka_client_core::{DeliveryStatus, UpdateFeaturesInput};

use crate::protocol::admin::update_features::UpdateFeaturesProtocolFailure;

use super::response::protocol_failure;

#[test]
fn compatibility_and_retained_capacity_remain_distinct() {
    assert_eq!(
        protocol_failure(UpdateFeaturesProtocolFailure::MissingSelectedVersion),
        UpdateFeaturesInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    assert_eq!(
        protocol_failure(UpdateFeaturesProtocolFailure::RetainedBytes {
            required: 11,
            limit: 10,
        }),
        UpdateFeaturesInput::ResponseTooLarge
    );
    assert_eq!(
        protocol_failure(UpdateFeaturesProtocolFailure::Allocation {
            field: "results",
            requested: 2,
        }),
        UpdateFeaturesInput::ResponseTooLarge
    );
}

#[test]
fn malformed_or_uncorrelatable_shapes_are_invalid_responses() {
    for failure in [
        UpdateFeaturesProtocolFailure::NegativeThrottleTime { actual: -1 },
        UpdateFeaturesProtocolFailure::TopLevelErrorWithResults,
        UpdateFeaturesProtocolFailure::SuccessDiagnostic {
            field: "error_message",
        },
        UpdateFeaturesProtocolFailure::V2ResultsPresent,
        UpdateFeaturesProtocolFailure::TooManyResults { actual: 2, max: 1 },
        UpdateFeaturesProtocolFailure::ResultCount {
            expected: 1,
            actual: 2,
        },
        UpdateFeaturesProtocolFailure::EmptyFeatureName,
        UpdateFeaturesProtocolFailure::FeatureNameTooLong { actual: 2, max: 1 },
        UpdateFeaturesProtocolFailure::ResponseTextBytesExceeded {
            required: 2,
            max: 1,
        },
        UpdateFeaturesProtocolFailure::UnexpectedFeature,
        UpdateFeaturesProtocolFailure::MissingFeature,
        UpdateFeaturesProtocolFailure::DuplicateFeature,
    ] {
        assert_eq!(
            protocol_failure(failure),
            UpdateFeaturesInput::InvalidResponse
        );
    }
}
