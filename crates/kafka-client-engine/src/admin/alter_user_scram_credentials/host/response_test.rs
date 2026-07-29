//! Exhaustive protocol-failure classification without secret request ownership.

use kafka_client_core::{AlterUserScramCredentialsInput, DeliveryStatus};

use crate::protocol::admin::alter_user_scram_credentials::AlterUserScramCredentialsResponseFailure;

use super::response::protocol_failure;

#[test]
fn compatibility_and_retained_capacity_remain_distinct() {
    assert_eq!(
        protocol_failure(
            AlterUserScramCredentialsResponseFailure::UnsupportedApiVersion { actual: 1 }
        ),
        AlterUserScramCredentialsInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    assert_eq!(
        protocol_failure(AlterUserScramCredentialsResponseFailure::RetainedBytes {
            required: 11,
            limit: 10,
        }),
        AlterUserScramCredentialsInput::ResponseTooLarge
    );
}

#[test]
fn every_malformed_or_uncorrelatable_shape_is_an_invalid_response() {
    for failure in [
        AlterUserScramCredentialsResponseFailure::NegativeThrottleTime { actual: -1 },
        AlterUserScramCredentialsResponseFailure::TooManyResults { actual: 2, max: 1 },
        AlterUserScramCredentialsResponseFailure::ResultCount {
            expected: 1,
            actual: 2,
        },
        AlterUserScramCredentialsResponseFailure::EmptyUser,
        AlterUserScramCredentialsResponseFailure::UserTooLong { actual: 2, max: 1 },
        AlterUserScramCredentialsResponseFailure::EmptyAffectedUsers,
        AlterUserScramCredentialsResponseFailure::TooManyAffectedUsers { actual: 2, max: 1 },
        AlterUserScramCredentialsResponseFailure::EmptyAffectedUser,
        AlterUserScramCredentialsResponseFailure::AffectedUserTooLong { actual: 2, max: 1 },
        AlterUserScramCredentialsResponseFailure::DuplicateAffectedUser,
        AlterUserScramCredentialsResponseFailure::DuplicateUser,
        AlterUserScramCredentialsResponseFailure::MissingUser,
        AlterUserScramCredentialsResponseFailure::UnexpectedUser,
    ] {
        assert_eq!(
            protocol_failure(failure),
            AlterUserScramCredentialsInput::InvalidResponse
        );
    }
}
