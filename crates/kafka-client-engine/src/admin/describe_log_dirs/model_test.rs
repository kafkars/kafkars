//! Engine request canonicalization and plan validation scenarios.

use super::{DescribeLogDirsAdmissionErrorKind, DescribeLogDirsRequest};
use crate::admin::describe_log_dirs::DescribeLogDirsAdmissionError;

#[test]
fn request_preserves_order_and_rejects_invalid_broker_sets_in_core() {
    let plan = DescribeLogDirsRequest::new(vec![9, 2, 7])
        .canonicalize()
        .into_plan()
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    assert_eq!(plan.broker_ids(), &[9, 2, 7]);

    assert!(DescribeLogDirsRequest::new(Vec::new()).into_plan().is_err());
    assert!(
        DescribeLogDirsRequest::new(vec![1, -1])
            .into_plan()
            .is_err()
    );
    assert!(DescribeLogDirsRequest::new(vec![1, 1]).into_plan().is_err());

    let error =
        DescribeLogDirsAdmissionError::new(DescribeLogDirsAdmissionErrorKind::InvalidRequest);
    assert_eq!(
        error.kind(),
        DescribeLogDirsAdmissionErrorKind::InvalidRequest
    );
}
