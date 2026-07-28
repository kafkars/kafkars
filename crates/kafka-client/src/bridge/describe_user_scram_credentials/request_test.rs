//! Optional public-to-engine SCRAM user-filter translation tests.

use super::DescribeUserScramCredentialsAdminRequest;

#[test]
fn absent_filter_remains_distinct_from_explicit_users() {
    let all = DescribeUserScramCredentialsAdminRequest::new().into_engine();
    let selected = DescribeUserScramCredentialsAdminRequest::new()
        .with_users(vec!["bob".to_owned(), "alice".to_owned()])
        .into_engine();

    assert_eq!(all.users(), None);
    assert_eq!(
        selected.users(),
        Some(["bob".to_owned(), "alice".to_owned()].as_slice())
    );
}

#[test]
fn malformed_explicit_filter_remains_inert_until_engine_submission() {
    let request = DescribeUserScramCredentialsAdminRequest::new()
        .with_users(vec![String::new(), String::new()])
        .into_engine();

    assert_eq!(request.users().map(<[_]>::len), Some(2));
}
