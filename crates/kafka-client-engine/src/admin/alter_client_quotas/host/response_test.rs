//! Exhaustive protocol-failure classification without generated wire ownership.

use kafka_client_core::{AlterClientQuotasInput, DeliveryStatus};

use crate::protocol::admin::alter_client_quotas::AlterClientQuotasResponseFailure;

use super::response::protocol_failure;

#[test]
fn compatibility_and_retained_capacity_remain_distinct() {
    assert_eq!(
        protocol_failure(AlterClientQuotasResponseFailure::UnsupportedApiVersion { actual: 2 }),
        AlterClientQuotasInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    assert_eq!(
        protocol_failure(AlterClientQuotasResponseFailure::RetainedBytes {
            required: 11,
            limit: 10,
        }),
        AlterClientQuotasInput::ResponseTooLarge
    );
}

#[test]
fn every_malformed_normalized_shape_is_an_invalid_response() {
    for failure in [
        AlterClientQuotasResponseFailure::NegativeThrottleTime { actual: -1 },
        AlterClientQuotasResponseFailure::EntryCount {
            expected: 1,
            actual: 0,
        },
        AlterClientQuotasResponseFailure::TooManyEntries { actual: 2, max: 1 },
        AlterClientQuotasResponseFailure::EmptyEntity,
        AlterClientQuotasResponseFailure::TooManyEntityComponents { actual: 2, max: 1 },
        AlterClientQuotasResponseFailure::EmptyEntityType,
        AlterClientQuotasResponseFailure::EntityTypeTooLong { actual: 2, max: 1 },
        AlterClientQuotasResponseFailure::EmptyEntityName,
        AlterClientQuotasResponseFailure::EntityNameTooLong { actual: 2, max: 1 },
        AlterClientQuotasResponseFailure::DuplicateEntityType,
        AlterClientQuotasResponseFailure::DuplicateResponseEntity,
        AlterClientQuotasResponseFailure::UnexpectedEntity,
        AlterClientQuotasResponseFailure::MissingEntity,
        AlterClientQuotasResponseFailure::InvalidRequest,
    ] {
        assert_eq!(
            protocol_failure(failure),
            AlterClientQuotasInput::InvalidResponse
        );
    }
}
