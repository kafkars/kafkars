//! Engine request ownership and core validation scenarios.

use super::{
    DescribeUserScramCredentialsAdmissionError, DescribeUserScramCredentialsAdmissionErrorKind,
    DescribeUserScramCredentialsRequest,
};

#[test]
fn request_preserves_all_user_semantics_and_explicit_caller_order() {
    let all = DescribeUserScramCredentialsRequest::new(None)
        .into_plan()
        .unwrap_or_else(|error| panic!("all-user selection: {error}"));
    assert!(all.describes_all_users());
    assert_eq!(all.users(), None);

    let selected =
        DescribeUserScramCredentialsRequest::new(Some(vec!["bob".to_owned(), "alice".to_owned()]))
            .into_plan()
            .unwrap_or_else(|error| panic!("selected users: {error}"));
    assert_eq!(
        selected.users(),
        Some(["bob".to_owned(), "alice".to_owned()].as_slice())
    );
}

#[test]
fn core_rejects_empty_present_invalid_and_duplicate_user_filters() {
    for users in [
        Vec::new(),
        vec![String::new()],
        vec!["alice".to_owned(), "alice".to_owned()],
    ] {
        assert!(
            DescribeUserScramCredentialsRequest::new(Some(users))
                .into_plan()
                .is_err()
        );
    }
}

#[test]
fn stable_request_parts_and_admission_error_remain_explicit() {
    let users = vec!["alice".to_owned()];
    assert_eq!(
        DescribeUserScramCredentialsRequest::new(Some(users.clone())).into_users(),
        Some(users)
    );
    let error = DescribeUserScramCredentialsAdmissionError::new(
        DescribeUserScramCredentialsAdmissionErrorKind::InvalidRequest,
    );
    assert_eq!(
        error.kind(),
        DescribeUserScramCredentialsAdmissionErrorKind::InvalidRequest
    );
}
