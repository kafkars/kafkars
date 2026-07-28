//! Exhaustive protocol-failure classification without generated wire ownership.

use kafka_client_core::{CreateAclsInput, DeliveryStatus};

use crate::protocol::admin::create_acls::CreateAclsResponseFailure;

use super::response::protocol_failure;

#[test]
fn unsupported_version_and_retained_capacity_remain_distinct() {
    assert_eq!(
        protocol_failure(CreateAclsResponseFailure::UnsupportedApiVersion { actual: 9 }),
        CreateAclsInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    assert_eq!(
        protocol_failure(CreateAclsResponseFailure::RetainedBytes {
            required: 11,
            limit: 10,
        }),
        CreateAclsInput::ResponseTooLarge
    );
    assert_eq!(
        protocol_failure(CreateAclsResponseFailure::ResultStorage),
        CreateAclsInput::ResponseTooLarge
    );
}

#[test]
fn malformed_positional_shapes_are_invalid_responses() {
    for failure in [
        CreateAclsResponseFailure::EmptyExpectedResults,
        CreateAclsResponseFailure::TooManyExpectedResults { actual: 2, max: 1 },
        CreateAclsResponseFailure::NegativeThrottleTime { actual: -1 },
        CreateAclsResponseFailure::ResultCount {
            expected: 2,
            actual: 1,
        },
    ] {
        assert_eq!(protocol_failure(failure), CreateAclsInput::InvalidResponse);
    }
}
