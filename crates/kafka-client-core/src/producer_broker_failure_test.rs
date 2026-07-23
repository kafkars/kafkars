//! Evidence that normalized broker facts remain semantic and lossless.

use core::num::NonZeroI16;

use crate::{ProducerBrokerFailure, ProducerBrokerFailureKind};

#[test]
fn semantic_fact_preserves_category_and_signed_code() {
    let code =
        NonZeroI16::new(-123).unwrap_or_else(|| panic!("the test broker code must be non-zero"));
    let failure = ProducerBrokerFailure::new(ProducerBrokerFailureKind::Unknown, code);

    assert_eq!(failure.kind(), ProducerBrokerFailureKind::Unknown);
    assert_eq!(failure.code(), -123);
}
