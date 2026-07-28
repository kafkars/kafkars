//! SCRAM description selection validation and caller-order scenarios.

use super::{
    DESCRIBE_USER_SCRAM_CREDENTIALS_MAX_USERS, DescribeUserScramCredentialsPlan,
    DescribeUserScramCredentialsPlanError,
};

#[test]
fn absent_selection_explicitly_describes_all_users() {
    let plan = DescribeUserScramCredentialsPlan::new(None)
        .unwrap_or_else(|error| panic!("all-user selection: {error}"));

    assert!(plan.describes_all_users());
    assert_eq!(plan.users(), None);
}

#[test]
fn named_selection_preserves_exact_caller_order() {
    let plan =
        DescribeUserScramCredentialsPlan::new(Some(vec!["zed".to_owned(), "alice".to_owned()]))
            .unwrap_or_else(|error| panic!("named selection: {error}"));

    assert!(!plan.describes_all_users());
    assert_eq!(
        plan.users().unwrap_or_else(|| panic!("named users")),
        ["zed", "alice"]
    );
}

#[test]
fn present_empty_selection_cannot_silently_mean_all_users() {
    assert_eq!(
        DescribeUserScramCredentialsPlan::new(Some(Vec::new())),
        Err(DescribeUserScramCredentialsPlanError::EmptyUserSelection)
    );
}

#[test]
fn selection_rejects_empty_long_and_duplicate_user_names() {
    assert_eq!(
        DescribeUserScramCredentialsPlan::new(Some(vec![String::new()])),
        Err(DescribeUserScramCredentialsPlanError::EmptyUserName)
    );
    assert_eq!(
        DescribeUserScramCredentialsPlan::new(Some(vec!["x".repeat(i16::MAX as usize + 1)])),
        Err(DescribeUserScramCredentialsPlanError::UserNameTooLong)
    );
    assert_eq!(
        DescribeUserScramCredentialsPlan::new(Some(vec!["alice".to_owned(), "alice".to_owned()])),
        Err(DescribeUserScramCredentialsPlanError::DuplicateUserName)
    );
}

#[test]
fn selection_has_an_explicit_user_count_bound() {
    let users = (0..=DESCRIBE_USER_SCRAM_CREDENTIALS_MAX_USERS)
        .map(|index| format!("user-{index}"))
        .collect();

    assert_eq!(
        DescribeUserScramCredentialsPlan::new(Some(users)),
        Err(DescribeUserScramCredentialsPlanError::TooManyUsers)
    );
}
