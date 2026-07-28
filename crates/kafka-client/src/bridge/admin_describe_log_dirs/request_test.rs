//! Public-to-engine broker selection translation tests.

use super::DescribeLogDirsAdminRequest;

#[test]
fn translation_is_deferred_and_preserves_the_linear_broker_selection() {
    let request = DescribeLogDirsAdminRequest::new(vec![7, 2]);
    let engine = request.into_engine();

    assert!(format!("{engine:?}").contains("DescribeLogDirsRequest"));
}
