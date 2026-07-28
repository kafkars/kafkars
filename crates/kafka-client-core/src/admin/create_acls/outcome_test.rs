//! Per-binding broker error and allocation-free outcome view tests.

use core::num::NonZeroI16;

use super::{
    CreateAclBinding, CreateAclBrokerError, CreateAclResult, CreateAclsBatch, CreateAclsPlan,
};

#[test]
fn exact_signed_broker_error_and_bounded_diagnostic_remain_lossless() {
    let error = CreateAclBrokerError::new(
        NonZeroI16::new(-731).unwrap_or_else(|| panic!("nonzero")),
        Some("denied".to_owned()),
        true,
    );

    assert_eq!(error.code(), -731);
    assert_eq!(error.message(), Some("denied"));
    assert!(error.message_truncated());
    assert_eq!(error.into_parts(), (-731, Some("denied".to_owned()), true));
}

#[test]
fn outcome_iteration_zips_existing_caller_order_without_materialization() {
    let plan = CreateAclsPlan::new(vec![binding("first"), binding("second")])
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let batch = CreateAclsBatch::from_plan(
        7,
        plan,
        vec![
            CreateAclResult::Created,
            CreateAclResult::BrokerFailed(CreateAclBrokerError::new(
                NonZeroI16::new(17).unwrap_or_else(|| panic!("nonzero")),
                None,
                false,
            )),
        ],
    );

    assert_eq!(batch.throttle_time_ms(), 7);
    let mut outcomes = batch.outcomes();
    assert_eq!(
        outcomes.next().map(|(binding, _)| binding.resource_name()),
        Some("first")
    );
    assert_eq!(
        outcomes.next().map(|(binding, _)| binding.resource_name()),
        Some("second")
    );
    assert!(outcomes.next().is_none());
}

fn binding(name: &str) -> CreateAclBinding {
    CreateAclBinding::new(
        2,
        name.to_owned(),
        3,
        "User:alice".to_owned(),
        "*".to_owned(),
        3,
        3,
    )
}
