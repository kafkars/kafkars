//! Request-shape and generated-materialization evidence for API-key 50.

use kafka_wire::RetainedSize;

use super::{
    DescribeUserScramCredentialsRequestFailure, DescribeUserScramCredentialsRequestRef,
    describe_user_scram_credentials_request,
    retention::{MAX_USER_BYTES, MAX_USERS, request_peak_charge},
};

#[test]
fn null_selects_all_and_selected_users_preserve_caller_order() {
    let all = describe_user_scram_credentials_request(
        DescribeUserScramCredentialsRequestRef::all(),
        1 << 20,
    )
    .unwrap_or_else(|error| panic!("valid all-user request: {error:?}"));
    assert!(all.users.is_none());

    let users = vec!["zoë".to_owned(), "alice".to_owned()];
    let selected = describe_user_scram_credentials_request(
        DescribeUserScramCredentialsRequestRef::selected(&users),
        1 << 20,
    )
    .unwrap_or_else(|error| panic!("valid filtered request: {error:?}"));
    let generated = selected
        .users
        .unwrap_or_else(|| panic!("expected selected users"));
    assert_eq!(generated[0].name.as_str(), "zoë");
    assert_eq!(generated[1].name.as_str(), "alice");
}

#[test]
fn explicit_empty_invalid_and_user_shape_is_bounded() {
    let empty = Vec::new();
    assert_eq!(
        describe_user_scram_credentials_request(
            DescribeUserScramCredentialsRequestRef::selected(&empty),
            usize::MAX,
        ),
        Err(DescribeUserScramCredentialsRequestFailure::EmptyUserFilter)
    );

    let empty_name = vec![String::new()];
    assert_eq!(
        describe_user_scram_credentials_request(
            DescribeUserScramCredentialsRequestRef::selected(&empty_name),
            usize::MAX,
        ),
        Err(DescribeUserScramCredentialsRequestFailure::EmptyUser)
    );

    let oversized = vec!["x".repeat(MAX_USER_BYTES + 1)];
    assert_eq!(
        describe_user_scram_credentials_request(
            DescribeUserScramCredentialsRequestRef::selected(&oversized),
            usize::MAX,
        ),
        Err(DescribeUserScramCredentialsRequestFailure::UserTooLong {
            actual: MAX_USER_BYTES + 1,
            max: MAX_USER_BYTES,
        })
    );

    let too_many = vec!["alice".to_owned(); MAX_USERS + 1];
    assert_eq!(
        describe_user_scram_credentials_request(
            DescribeUserScramCredentialsRequestRef::selected(&too_many),
            usize::MAX,
        ),
        Err(DescribeUserScramCredentialsRequestFailure::TooManyUsers {
            actual: MAX_USERS + 1,
            max: MAX_USERS,
        })
    );
}

#[test]
fn duplicate_users_are_rejected_without_changing_caller_order() {
    let users = vec!["alice".to_owned(), "bob".to_owned(), "alice".to_owned()];
    assert_eq!(
        describe_user_scram_credentials_request(
            DescribeUserScramCredentialsRequestRef::selected(&users),
            usize::MAX,
        ),
        Err(DescribeUserScramCredentialsRequestFailure::DuplicateUser)
    );
    assert_eq!(users, vec!["alice", "bob", "alice"]);
}

#[test]
fn retained_limit_covers_scratch_and_generated_request() {
    let users = vec!["alice".to_owned(), "bob".to_owned()];
    let source = DescribeUserScramCredentialsRequestRef::selected(&users);
    let required =
        request_peak_charge(source).unwrap_or_else(|| panic!("request charge should fit"));
    assert_eq!(
        describe_user_scram_credentials_request(source, required - 1),
        Err(DescribeUserScramCredentialsRequestFailure::RetainedBytes {
            required,
            limit: required - 1,
        })
    );
    let request = describe_user_scram_credentials_request(source, required)
        .unwrap_or_else(|error| panic!("exact charge should fit: {error:?}"));
    assert!(request.retained_size().heap_bytes() <= required);
}
