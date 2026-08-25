//! Tests for retained-byte capacity.

use crate::{ByteBudget, ByteCount, CapacityError};

#[test]
fn byte_budget_rejection_is_atomic() {
    let mut budget = ByteBudget::new(ByteCount::new(1_024));

    assert_eq!(budget.try_reserve(ByteCount::new(900)), Ok(()));
    assert_eq!(
        budget.try_reserve(ByteCount::new(200)),
        Err(CapacityError::Exhausted)
    );
    assert_eq!(budget.used(), ByteCount::new(900));
}

#[test]
fn byte_count_adapter_preserves_checked_arithmetic() {
    let seven = ByteCount::new(7);
    assert_eq!(
        seven.checked_add(ByteCount::new(5)),
        Some(ByteCount::new(12))
    );
    assert_eq!(
        seven.checked_sub(ByteCount::new(5)),
        Some(ByteCount::new(2))
    );
    assert_eq!(seven.checked_sub(ByteCount::new(8)), None);
    assert_eq!(
        ByteCount::new(u64::MAX).checked_add(ByteCount::new(1)),
        None
    );
}

#[test]
fn byte_budget_preserves_overflow_classification() {
    let mut budget = ByteBudget::new(ByteCount::new(u64::MAX));
    assert_eq!(budget.try_reserve(ByteCount::new(u64::MAX)), Ok(()));
    assert_eq!(
        budget.try_reserve(ByteCount::new(1)),
        Err(CapacityError::Overflow)
    );
    assert_eq!(budget.used(), ByteCount::new(u64::MAX));
}

#[test]
fn over_release_rejection_is_atomic() {
    let mut budget = ByteBudget::new(ByteCount::new(8));
    assert_eq!(budget.try_reserve(ByteCount::new(5)), Ok(()));
    assert_eq!(
        budget.release(ByteCount::new(6)),
        Err(CapacityError::OverRelease)
    );
    assert_eq!(budget.used(), ByteCount::new(5));
}

#[test]
fn release_plan_is_inert_until_consumed_by_commit() {
    let mut budget = ByteBudget::new(ByteCount::new(8));
    assert_eq!(budget.try_reserve(ByteCount::new(5)), Ok(()));
    let Ok(plan) = budget.plan_release(ByteCount::new(3)) else {
        panic!("three reserved bytes must have a release plan");
    };

    assert_eq!(budget.used(), ByteCount::new(5));
    budget.commit_release(plan);
    assert_eq!(budget.used(), ByteCount::new(2));
}

#[test]
fn compatibility_adapters_preserve_public_debug_shape() {
    assert_eq!(format!("{:?}", ByteCount::new(7)), "ByteCount(7)");
    assert_eq!(
        format!("{:?}", ByteBudget::new(ByteCount::new(8))),
        "ByteBudget { limit: ByteCount(8), used: ByteCount(0) }"
    );
}
