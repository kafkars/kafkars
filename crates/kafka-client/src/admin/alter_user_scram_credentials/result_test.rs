//! Public SCRAM alteration result ordering and throttle accessors.

use std::time::Duration;

use crate::admin::BatchResult;

use super::AlterUserScramCredentialsResult;

#[test]
fn result_preserves_first_occurrence_user_order_and_throttle() {
    let result = AlterUserScramCredentialsResult::new(
        Duration::from_millis(17),
        BatchResult::new(vec![
            ("bob".to_owned(), Ok(())),
            ("alice".to_owned(), Ok(())),
        ]),
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(17));
    assert_eq!(result.users().entries()[0].0, "bob");
    assert_eq!(result.users().entries()[1].0, "alice");
    assert_eq!(result.into_users().entries().len(), 2);
}
