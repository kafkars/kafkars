//! Classic transition rejection vocabulary evidence.

use super::{ClassicGroupApplyError, ClassicGroupErrorKind};

#[test]
fn apply_error_preserves_the_exact_deterministic_kind() {
    let error = ClassicGroupApplyError::new(ClassicGroupErrorKind::DeadlineElapsed);
    assert_eq!(error.kind(), ClassicGroupErrorKind::DeadlineElapsed);
}
