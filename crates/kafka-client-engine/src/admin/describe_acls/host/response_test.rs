//! Exhaustive protocol-failure classification without generated wire ownership.

use kafka_client_core::{DeliveryStatus, DescribeAclsInput};

use crate::protocol::admin::describe_acls::DescribeAclsResponseFailure;

use super::response::protocol_failure;

#[test]
fn unsupported_version_and_retained_capacity_remain_distinct() {
    assert_eq!(
        protocol_failure(DescribeAclsResponseFailure::UnsupportedApiVersion { actual: 9 }),
        DescribeAclsInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    assert_eq!(
        protocol_failure(DescribeAclsResponseFailure::RetainedBytes {
            required: 11,
            limit: 10,
        }),
        DescribeAclsInput::ResponseTooLarge
    );
}

#[test]
fn malformed_normalized_shapes_are_invalid_responses() {
    for failure in [
        DescribeAclsResponseFailure::NegativeThrottleTime { actual: -1 },
        DescribeAclsResponseFailure::ResourcesWithTopLevelError { actual: 1 },
        DescribeAclsResponseFailure::TooManyResources { actual: 2, max: 1 },
        DescribeAclsResponseFailure::EmptyResourceName,
        DescribeAclsResponseFailure::ResourceNameTooLong { actual: 2, max: 1 },
        DescribeAclsResponseFailure::EmptyResourceAcls,
        DescribeAclsResponseFailure::TooManyAcls { actual: 2, max: 1 },
        DescribeAclsResponseFailure::EmptyPrincipal,
        DescribeAclsResponseFailure::PrincipalTooLong { actual: 2, max: 1 },
        DescribeAclsResponseFailure::EmptyHost,
        DescribeAclsResponseFailure::HostTooLong { actual: 2, max: 1 },
        DescribeAclsResponseFailure::DuplicateResource,
        DescribeAclsResponseFailure::DuplicateAcl,
    ] {
        assert_eq!(
            protocol_failure(failure),
            DescribeAclsInput::InvalidResponse
        );
    }
}
