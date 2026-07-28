//! Exhaustive protocol-failure classification without generated wire ownership.

use kafka_client_core::{DeliveryStatus, DescribeUserScramCredentialsInput};

use crate::protocol::admin::describe_user_scram_credentials::DescribeUserScramCredentialsResponseFailure;

use super::response::protocol_failure;

#[test]
fn compatibility_and_retained_capacity_remain_distinct() {
    assert_eq!(
        protocol_failure(
            DescribeUserScramCredentialsResponseFailure::UnsupportedApiVersion { actual: 1 }
        ),
        DescribeUserScramCredentialsInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    assert_eq!(
        protocol_failure(DescribeUserScramCredentialsResponseFailure::RetainedBytes {
            required: 11,
            limit: 10,
        }),
        DescribeUserScramCredentialsInput::ResponseTooLarge
    );
}

#[test]
fn every_malformed_or_uncorrelatable_shape_is_an_invalid_response() {
    for failure in [
        DescribeUserScramCredentialsResponseFailure::NegativeThrottleTime { actual: -1 },
        DescribeUserScramCredentialsResponseFailure::ResultsWithTopLevelError { actual: 1 },
        DescribeUserScramCredentialsResponseFailure::TooManyResults { actual: 2, max: 1 },
        DescribeUserScramCredentialsResponseFailure::TooManyCredentialInfos { actual: 2, max: 1 },
        DescribeUserScramCredentialsResponseFailure::EmptyUser,
        DescribeUserScramCredentialsResponseFailure::UserTooLong { actual: 2, max: 1 },
        DescribeUserScramCredentialsResponseFailure::TooManyCredentialsForUser {
            actual: 2,
            max: 1,
        },
        DescribeUserScramCredentialsResponseFailure::EmptyCredentialsOnSuccess,
        DescribeUserScramCredentialsResponseFailure::CredentialsWithUserError { actual: 1 },
        DescribeUserScramCredentialsResponseFailure::InvalidMechanism { actual: 0 },
        DescribeUserScramCredentialsResponseFailure::NonPositiveIterations { actual: 0 },
        DescribeUserScramCredentialsResponseFailure::DuplicateMechanism { actual: 1 },
        DescribeUserScramCredentialsResponseFailure::EmptyUserFilter,
        DescribeUserScramCredentialsResponseFailure::TooManyRequestedUsers { actual: 2, max: 1 },
        DescribeUserScramCredentialsResponseFailure::EmptyRequestedUser,
        DescribeUserScramCredentialsResponseFailure::RequestedUserTooLong { actual: 2, max: 1 },
        DescribeUserScramCredentialsResponseFailure::DuplicateRequestedUser,
        DescribeUserScramCredentialsResponseFailure::DuplicateUser,
        DescribeUserScramCredentialsResponseFailure::MissingUser,
        DescribeUserScramCredentialsResponseFailure::UnexpectedUser,
    ] {
        assert_eq!(
            protocol_failure(failure),
            DescribeUserScramCredentialsInput::InvalidResponse
        );
    }
}
