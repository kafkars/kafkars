//! Scalar ownership and lossless rejection evidence for assignment retirement.

use super::{AssignmentEpoch, RetireAssignment, RetireAssignmentError, RetireAssignmentErrorKind};

#[test]
fn retirement_input_and_error_recover_the_exact_optional_epoch() {
    let expected = Some(AssignmentEpoch::initial());
    let input = RetireAssignment::new(expected);
    assert_eq!(input.expected_assignment_epoch(), expected);

    let error = RetireAssignmentError::new(
        RetireAssignmentErrorKind::AssignmentEpochMismatch {
            expected,
            actual: None,
        },
        input,
    );
    assert_eq!(
        error.kind(),
        RetireAssignmentErrorKind::AssignmentEpochMismatch {
            expected,
            actual: None,
        }
    );
    assert_eq!(error.input().expected_assignment_epoch(), expected);
    assert_eq!(error.into_input().expected_assignment_epoch(), expected);
}
