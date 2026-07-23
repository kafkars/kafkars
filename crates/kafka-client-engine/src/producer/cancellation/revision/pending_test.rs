//! Pending cancellation plans preserve mechanism-phase expectations.

use crate::producer::{
    batch_store::BatchRevisionExpectation, execution::PreparedRevisionExpectation,
};

use super::pending::PendingRevisionPlan;

#[test]
fn materialization_plan_requires_absent_prepared_bytes() {
    let plan = PendingRevisionPlan::Materialize(2);

    assert_eq!(
        plan.batch_expectation(),
        BatchRevisionExpectation::ReadyForMaterialization
    );
    assert_eq!(
        plan.prepared_expectation(),
        PreparedRevisionExpectation::Absent
    );
}

#[test]
fn submission_and_armed_plans_preserve_materialized_bytes() {
    for (plan, expected) in [
        (
            PendingRevisionPlan::Submit(1),
            PreparedRevisionExpectation::Unarmed,
        ),
        (
            PendingRevisionPlan::Armed,
            PreparedRevisionExpectation::Armed,
        ),
    ] {
        assert_eq!(
            plan.batch_expectation(),
            BatchRevisionExpectation::Materialized
        );
        assert_eq!(plan.prepared_expectation(), expected);
    }
}

#[test]
fn retry_wait_plan_requires_explicit_waiting_state_and_no_prepared_bytes() {
    let plan = PendingRevisionPlan::RetryWaiting;

    assert_eq!(
        plan.batch_expectation(),
        BatchRevisionExpectation::RetryWaiting
    );
    assert_eq!(
        plan.prepared_expectation(),
        PreparedRevisionExpectation::Absent
    );
}
