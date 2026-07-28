//! Exhaustive protocol-failure classification without generated wire ownership.

use kafka_client_core::{DeliveryStatus, DescribeClientQuotasInput};

use crate::protocol::admin::describe_client_quotas::DescribeClientQuotasResponseFailure;

use super::response::protocol_failure;

#[test]
fn compatibility_and_retained_capacity_remain_distinct() {
    assert_eq!(
        protocol_failure(DescribeClientQuotasResponseFailure::UnsupportedApiVersion { actual: 2 }),
        DescribeClientQuotasInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    assert_eq!(
        protocol_failure(DescribeClientQuotasResponseFailure::RetainedBytes {
            required: 11,
            limit: 10,
        }),
        DescribeClientQuotasInput::ResponseTooLarge
    );
}

#[test]
fn every_malformed_normalized_shape_is_an_invalid_response() {
    for failure in [
        DescribeClientQuotasResponseFailure::NegativeThrottleTime { actual: -1 },
        DescribeClientQuotasResponseFailure::MissingEntriesOnSuccess,
        DescribeClientQuotasResponseFailure::EntriesWithTopLevelError { actual: 1 },
        DescribeClientQuotasResponseFailure::TooManyEntries { actual: 2, max: 1 },
        DescribeClientQuotasResponseFailure::EmptyEntity,
        DescribeClientQuotasResponseFailure::TooManyEntityComponents { actual: 2, max: 1 },
        DescribeClientQuotasResponseFailure::EmptyEntityType,
        DescribeClientQuotasResponseFailure::EntityTypeTooLong { actual: 2, max: 1 },
        DescribeClientQuotasResponseFailure::EmptyEntityName,
        DescribeClientQuotasResponseFailure::EntityNameTooLong { actual: 2, max: 1 },
        DescribeClientQuotasResponseFailure::EmptyValues,
        DescribeClientQuotasResponseFailure::TooManyQuotaValues { actual: 2, max: 1 },
        DescribeClientQuotasResponseFailure::EmptyQuotaKey,
        DescribeClientQuotasResponseFailure::QuotaKeyTooLong { actual: 2, max: 1 },
        DescribeClientQuotasResponseFailure::NonFiniteQuotaValue,
        DescribeClientQuotasResponseFailure::DuplicateEntityType,
        DescribeClientQuotasResponseFailure::DuplicateQuotaKey,
        DescribeClientQuotasResponseFailure::DuplicateEntity,
    ] {
        assert_eq!(
            protocol_failure(failure),
            DescribeClientQuotasInput::InvalidResponse
        );
    }
}
